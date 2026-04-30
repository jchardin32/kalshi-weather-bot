"""
shadow_logger.py — V24.7 Shadow Edge Logger
============================================

Drop-in observability module. Logs what the bot WOULD have done at every
gate, in parallel with live trading logic. Zero behavior change.

Purpose:
  After 50+ hours of build time and zero trades, we don't know whether
  the issue is (a) thesis (no edge exists in this approach), (b) gates
  too strict (edges exist but get filtered out), or (c) market timing
  (edges exist but disappear before we poll). This module produces the
  dataset needed to answer that.

Output:
  CSV at SHADOW_LOG_PATH (default ./shadow_edges.csv) with one row per
  market evaluation. Each row records:
    - market identifiers
    - model output (prob, forecast, sigma, n_members)
    - market quote at evaluation time (yes_ask, yes_bid, no_ask, no_bid)
    - raw and shrunk edge for both YES and NO sides
    - which gate stopped each profile (or "PASSED")
    - settlement outcome (filled in later by reconcile)

Analysis later (after ~3-7 days of data):
  - For rows where shadow says "would have traded YES at 35¢" and
    settlement was YES, you collected (100 - 35) = 65¢. Sum these.
  - Bucket by gate-that-stopped-it: if "bracket_dead_zone" rows are
    profitable when settled, the dead-zone is killing alpha.
  - Bucket by mode (same_day vs next_day): is HRRR blend helping or hurting?
  - Reliability plot: bucket model_prob into deciles, check actual hit rate.

Integration:
  1. Save this file next to your main bot file.
  2. In your main file, add at the top with other imports:
        from shadow_logger import shadow_log_evaluation, shadow_settlement_update
  3. In the run() loop, AFTER `maybe_refresh_model(state)` and BEFORE the
     `for profile in profiles` loop, add ONE line:
        shadow_log_evaluation(state, profiles, tracker)
     (You can keep the live evaluate_for_profile loop exactly as-is.)
  4. In reconcile_settlements, after each settlement is processed, add:
        shadow_settlement_update(settlement_row)
     (Optional but valuable — lets us close the loop on shadow trades.)

Tuning knobs (env vars):
  SHADOW_LOG_PATH        path to CSV (default ./shadow_edges.csv)
  SHADOW_LOG_ENABLED     "0" to disable (default "1")
  SHADOW_GATELESS        "1" to also log a "no-gates" hypothetical
                         edge using a 0¢ threshold and no shrinkage
                         (default "1" — this is the whole point)
"""

from __future__ import annotations

import csv
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


SHADOW_LOG_PATH = os.environ.get("SHADOW_LOG_PATH", "./shadow_edges.csv")
SHADOW_LOG_ENABLED = os.environ.get("SHADOW_LOG_ENABLED", "1") == "1"
SHADOW_GATELESS = os.environ.get("SHADOW_GATELESS", "1") == "1"

# Mirror the constants from the main bot. Keep these in sync if you change
# them in the main file — or import them directly if you prefer.
EDGE_SHRINK_K = 8
MIN_PRICE_CENTS = 8
MAX_PRICE_CENTS = 92

_FIELDS = [
    "ts_iso",
    "ticker",
    "city",
    "market_type",
    "target_date",
    "threshold",
    "hours_to_close",
    "mode",
    # Model output
    "model_prob",
    "forecast_val",
    "sigma",
    "n_members",
    "model_age_h",
    "model_skip_reason",
    # Market state
    "yes_ask",
    "yes_bid",
    "no_ask",
    "no_bid",
    "spread",
    # Raw economics (no gates applied)
    "raw_edge_yes",         # model_prob - yes_ask  (cents)
    "raw_edge_no",          # (100-model_prob) - no_ask  (cents)
    "shrunk_edge_yes",
    "shrunk_edge_no",
    "shrink_factor",
    # Per-profile decisions: which gate stopped it (or "PASSED")
    "base_decision",
    "base_side",
    "base_edge_used",       # the edge value compared against threshold
    "base_threshold",       # the threshold it had to clear
    "aggro_decision",
    "aggro_side",
    "aggro_edge_used",
    "aggro_threshold",
    # Gateless hypothetical: would we ever have wanted this trade?
    "gateless_side",        # "yes" / "no" / "" (no edge at all)
    "gateless_edge",        # raw, unshrunk
    "gateless_price",       # what we would have paid
    # Settlement (filled later by shadow_settlement_update)
    "settled",              # "yes" / "no" / ""
    "settlement_value",     # 0 or 100
    # Notional shadow P&L if we'd taken the gateless trade at 1 contract
    # (filled later)
    "shadow_pnl_cents",
]

_lock = threading.Lock()
_writer = None
_fp = None
_initialized = False
# In-memory index: ticker -> list of row dicts pending settlement.
# Keep the last 14 days; pruned opportunistically.
_pending_by_ticker: dict[str, list[dict]] = {}


def _init_writer():
    """Open the CSV file, write header if new. Idempotent."""
    global _writer, _fp, _initialized
    if _initialized:
        return
    path = Path(SHADOW_LOG_PATH)
    new_file = not path.exists() or path.stat().st_size == 0
    _fp = path.open("a", newline="", buffering=1)  # line-buffered
    _writer = csv.DictWriter(_fp, fieldnames=_FIELDS)
    if new_file:
        _writer.writeheader()
    _initialized = True


def _safe_int(v) -> Optional[int]:
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _evaluate_gates(state, profile, raw_edge_yes_cents: float,
                    raw_edge_no_cents: float, shrunk_yes: float,
                    shrunk_no: float):
    """
    Re-implement the gate logic from evaluate_for_profile, but instead of
    returning an intent or None, return (decision_string, side, edge_used,
    threshold_used). decision_string is "PASSED" or the name of the gate
    that stopped it.

    Mirrors evaluate_for_profile in the main file. Keep in sync.
    """
    # Gate 1: model skip reason
    if state.skip_reason:
        return (f"model_skip:{state.skip_reason.split(':')[0]}", "", 0.0, 0.0)
    # Gate 2: min members
    if state.n_members < profile.min_n_members:
        return ("min_members", "", 0.0, 0.0)
    # Gate 3: missing model or quote
    if state.model_prob is None or state.yes_ask is None:
        return ("missing_model_or_quote", "", 0.0, 0.0)
    # Gate 4: price band
    if state.yes_ask < MIN_PRICE_CENTS or state.yes_ask > MAX_PRICE_CENTS:
        return ("price_band", "", 0.0, 0.0)

    # Mode-specific threshold (matches evaluate_for_profile)
    if state.market_type in ("RAIN", "SNOW"):
        edge_threshold = profile.edge_threshold_cents
    elif state.mode == "same_day":
        edge_threshold = profile.same_day_edge_threshold_cents
    else:
        edge_threshold = profile.edge_threshold_cents

    # Determine side from shrunk edge (same as live logic)
    # raw_edge in main code = model_prob - yes_ask, then shrunk.
    # Positive shrunk_yes => buy YES; negative => buy NO.
    shrunk_edge = shrunk_yes  # this is what the live bot uses for the comparison
    if abs(shrunk_edge) < edge_threshold:
        side = "yes" if shrunk_edge > 0 else "no"
        return ("below_threshold", side, shrunk_edge, edge_threshold)

    buy_yes = shrunk_edge > 0
    side = "yes" if buy_yes else "no"

    if buy_yes:
        price = state.yes_ask
    else:
        if state.no_ask is None:
            return ("no_ask_missing", "no", shrunk_edge, edge_threshold)
        if state.no_ask < MIN_PRICE_CENTS or state.no_ask > MAX_PRICE_CENTS:
            return ("no_price_band", "no", shrunk_edge, edge_threshold)
        price = state.no_ask

    # Note: we do NOT replicate the exposure caps, dedup, or kelly>=1 gates here.
    # Those depend on portfolio state / time and would force a re-implementation
    # of half the bot. If a market got past `below_threshold`, we call it PASSED
    # at the gate level — exposure/dedup are separate concerns and can be
    # post-analyzed if needed.
    _ = price
    return ("PASSED", side, shrunk_edge, edge_threshold)


def shadow_log_evaluation(state, profiles, tracker) -> None:
    """
    Log what each profile would have done at this market evaluation.
    Call once per market per cycle, after model refresh.

    state: MarketState (from main bot)
    profiles: list[StrategyProfile]
    tracker: PositionTracker (unused for now, reserved for exposure analysis)
    """
    if not SHADOW_LOG_ENABLED:
        return

    try:
        with _lock:
            _init_writer()

            yes_ask = _safe_int(state.yes_ask)
            yes_bid = _safe_int(state.yes_bid)
            no_ask = _safe_int(state.no_ask)
            no_bid = _safe_int(state.no_bid)
            spread = (yes_ask - yes_bid) if (yes_ask is not None and yes_bid is not None) else None

            model_prob = state.model_prob  # 0..100 or None

            raw_edge_yes = None
            raw_edge_no = None
            shrunk_yes = None
            shrunk_no = None
            shrink_factor = None

            if model_prob is not None and state.n_members > 0:
                n = state.n_members
                shrink_factor = n / (n + EDGE_SHRINK_K)
                if yes_ask is not None:
                    raw_edge_yes = model_prob - yes_ask
                    shrunk_yes = raw_edge_yes * shrink_factor
                if no_ask is not None:
                    raw_edge_no = (100 - model_prob) - no_ask
                    shrunk_no = raw_edge_no * shrink_factor

            # Gateless hypothetical: take the better side at the better raw edge.
            gateless_side = ""
            gateless_edge = 0.0
            gateless_price = 0
            if SHADOW_GATELESS and raw_edge_yes is not None:
                # Pick whichever side has positive raw edge; if both, take larger.
                yes_e = raw_edge_yes if raw_edge_yes is not None else -999
                no_e = raw_edge_no if raw_edge_no is not None else -999
                if yes_e > 0 and yes_e >= no_e:
                    gateless_side = "yes"
                    gateless_edge = yes_e
                    gateless_price = yes_ask or 0
                elif no_e > 0:
                    gateless_side = "no"
                    gateless_edge = no_e
                    gateless_price = no_ask or 0

            row = {
                "ts_iso": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "ticker": state.ticker,
                "city": state.city or "",
                "market_type": state.market_type or "",
                "target_date": state.target_date or state.date_str or "",
                "threshold": state.threshold if state.threshold is not None else "",
                "hours_to_close": round(state.hours_to_close(), 2),
                "mode": state.mode or "",
                "model_prob": round(model_prob, 2) if model_prob is not None else "",
                "forecast_val": state.forecast_val if state.forecast_val is not None else "",
                "sigma": state.sigma if state.sigma is not None else "",
                "n_members": state.n_members,
                "model_age_h": round(state.age_h, 2) if state.age_h is not None else "",
                "model_skip_reason": state.skip_reason or "",
                "yes_ask": yes_ask if yes_ask is not None else "",
                "yes_bid": yes_bid if yes_bid is not None else "",
                "no_ask": no_ask if no_ask is not None else "",
                "no_bid": no_bid if no_bid is not None else "",
                "spread": spread if spread is not None else "",
                "raw_edge_yes": round(raw_edge_yes, 2) if raw_edge_yes is not None else "",
                "raw_edge_no": round(raw_edge_no, 2) if raw_edge_no is not None else "",
                "shrunk_edge_yes": round(shrunk_yes, 2) if shrunk_yes is not None else "",
                "shrunk_edge_no": round(shrunk_no, 2) if shrunk_no is not None else "",
                "shrink_factor": round(shrink_factor, 3) if shrink_factor is not None else "",
                "gateless_side": gateless_side,
                "gateless_edge": round(gateless_edge, 2),
                "gateless_price": gateless_price,
                "settled": "",
                "settlement_value": "",
                "shadow_pnl_cents": "",
            }

            # Per-profile gate evaluation
            for profile in profiles:
                if shrunk_yes is None:
                    decision = ("missing_inputs", "", 0.0, 0.0)
                else:
                    decision = _evaluate_gates(
                        state, profile,
                        raw_edge_yes or 0.0,
                        raw_edge_no or 0.0,
                        shrunk_yes,
                        shrunk_no or 0.0,
                    )
                prefix = profile.name  # "base" / "aggro"
                row[f"{prefix}_decision"] = decision[0]
                row[f"{prefix}_side"] = decision[1]
                row[f"{prefix}_edge_used"] = round(decision[2], 2)
                row[f"{prefix}_threshold"] = round(decision[3], 2)

            # Make sure all expected fields exist (in case profile names change)
            for f in _FIELDS:
                row.setdefault(f, "")

            _writer.writerow(row)

            # Track for later settlement update if this row had a gateless trade
            if gateless_side:
                _pending_by_ticker.setdefault(state.ticker, []).append(row)
                _maybe_prune_pending()

    except Exception as e:
        # Never let shadow logging crash the bot
        try:
            import logging
            logging.getLogger(__name__).warning("shadow_log_evaluation failed: %s", e)
        except Exception:
            pass


def shadow_settlement_update(settlement_row: dict) -> None:
    """
    Call from reconcile_settlements with each settled row. Updates pending
    shadow rows for that ticker by appending a settlement.csv companion file
    rather than rewriting the main CSV (which would be expensive). The
    analysis script joins them on ticker.
    """
    if not SHADOW_LOG_ENABLED:
        return

    try:
        ticker = settlement_row.get("ticker") or settlement_row.get("market_ticker")
        if not ticker:
            return

        # Determine YES/NO outcome. Kalshi settlements typically expose
        # 'result' or 'yes_count'/'no_count' or revenue. Be defensive.
        result = None
        if "result" in settlement_row:
            r = str(settlement_row["result"]).lower()
            if r in ("yes", "no"):
                result = r
        if result is None:
            # Fallback: revenue-based inference if available
            rev = settlement_row.get("revenue")
            side = settlement_row.get("side") or settlement_row.get("yes_no")
            # Without clearer schema, leave as unknown
            _ = rev, side

        path = Path(SHADOW_LOG_PATH).with_suffix(".settlements.csv")
        new_file = not path.exists() or path.stat().st_size == 0
        with _lock:
            with path.open("a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=[
                    "ts_iso", "ticker", "result", "raw_settlement_row",
                ])
                if new_file:
                    w.writeheader()
                w.writerow({
                    "ts_iso": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "ticker": ticker,
                    "result": result or "",
                    "raw_settlement_row": str(settlement_row)[:500],  # truncate
                })

        # Drop pending entries for this ticker (they're settled now)
        _pending_by_ticker.pop(ticker, None)

    except Exception as e:
        try:
            import logging
            logging.getLogger(__name__).warning("shadow_settlement_update failed: %s", e)
        except Exception:
            pass


def _maybe_prune_pending():
    """Bounded memory: drop pending tickers older than 14 days."""
    if len(_pending_by_ticker) < 5000:
        return
    cutoff = time.time() - 14 * 86400
    for ticker in list(_pending_by_ticker.keys()):
        rows = _pending_by_ticker[ticker]
        rows = [r for r in rows if _row_ts_epoch(r) > cutoff]
        if rows:
            _pending_by_ticker[ticker] = rows
        else:
            _pending_by_ticker.pop(ticker, None)


def _row_ts_epoch(row: dict) -> float:
    try:
        return datetime.fromisoformat(row["ts_iso"]).timestamp()
    except Exception:
        return 0.0
