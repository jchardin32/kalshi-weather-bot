"""
discovery_broadener.py — V24.8 Universe Expansion

Purpose
=======
The shadow log on weather markets revealed extreme illiquidity (9 tradeable
quotes in 11 hours across 9,651 evaluations). Before pivoting markets, we
need real liquidity data on Kalshi's other categories. This module enables
the bot to discover and shadow-log ALL Kalshi markets — politics, economics,
sports, entertainment — alongside weather, without changing any live
trading behavior.

Design principles
=================
1. Weather trading remains untouched. The original discover_weather_markets
   function is the live-trading source of truth. Non-weather markets are
   discovered into the same registry but lack lat/threshold, so
   maybe_refresh_model returns early for them and they never produce
   trade intents.

2. Shadow logger captures everything. It already logs market quotes, sizes,
   spreads, and timestamps regardless of model output. For non-weather
   markets, model_prob will be None — that's fine. We're answering the
   question "is there liquidity here," not "is there mispricing here."

3. Budget-conscious. Kalshi has thousands of open markets. Pulling all of
   them every cycle would crush our request budget. We run broad discovery
   on a slow cadence (default every 30 min) and only refresh quote/state
   on markets we already track at the normal cadence.

4. Categorized. Every market gets tagged with its detected category
   (weather/politics/economics/sports/entertainment/other) so the shadow
   log can be sliced by category in analysis.

Integration
===========
1. Save discovery_broadener.py next to your bot file.
2. Add ONE import block at the top of your bot:
       from discovery_broadener import (
           discover_all_markets,
           classify_ticker,
           BROADEN_DISCOVERY_ENABLED,
           BROADEN_DISCOVERY_INTERVAL_S,
       )
3. In the main run() loop, find where discover_weather_markets is called
   on a cadence. Add a parallel call to discover_all_markets on its own
   slower cadence (controlled by env var). Example:
       if BROADEN_DISCOVERY_ENABLED and (now - last_broad_discovery) > BROADEN_DISCOVERY_INTERVAL_S:
           discover_all_markets(registry, budget)
           last_broad_discovery = now

   See INTEGRATION_PATCH_v24_8.md for the exact 5-line edit.

Env vars
========
BROADEN_DISCOVERY_ENABLED      "1" to enable (default "1")
BROADEN_DISCOVERY_INTERVAL_S   seconds between broad discovery runs
                                (default 1800 = 30 min)
BROADEN_MAX_MARKETS            cap on total non-weather markets tracked
                                (default 2000 — protects request budget)
BROADEN_CATEGORIES             comma-separated categories to include
                                (default "all" — alternatives: "politics,
                                 economics,sports,entertainment")
BROADEN_MIN_VOLUME             minimum 24h volume to track (default 0)
                                Set to e.g. 100 to skip dead markets.
BROADEN_HOURS_TO_CLOSE_MAX     don't track markets > N hours to close
                                (default 168 = 7 days). Reduces universe
                                to markets that might actually settle
                                during data collection.

Category classification
=======================
Kalshi uses series_ticker prefixes that loosely correspond to categories.
We classify with a mix of prefix matching and the API's `category` field.
See classify_ticker() for the exact mapping. When in doubt -> "other".
"""

from __future__ import annotations

import logging
import os
import time
from typing import Optional

# These are intentionally NOT imported at module level — they live in the
# main bot file. We accept them as parameters where needed to avoid a
# circular import.

log = logging.getLogger(__name__)


# ============================================================================
# CONFIG
# ============================================================================
def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


BROADEN_DISCOVERY_ENABLED = _env_bool("BROADEN_DISCOVERY_ENABLED", True)
BROADEN_DISCOVERY_INTERVAL_S = _env_int("BROADEN_DISCOVERY_INTERVAL_S", 1800)
BROADEN_MAX_MARKETS = _env_int("BROADEN_MAX_MARKETS", 2000)
BROADEN_MIN_VOLUME = _env_float("BROADEN_MIN_VOLUME", 0.0)
BROADEN_HOURS_TO_CLOSE_MAX = _env_float("BROADEN_HOURS_TO_CLOSE_MAX", 168.0)
BROADEN_CATEGORIES = os.getenv("BROADEN_CATEGORIES", "all").lower().strip()


# ============================================================================
# CATEGORY CLASSIFICATION
# ============================================================================
# Series-ticker prefix -> category. Based on Kalshi's actual ticker structure.
# This is a heuristic; some tickers will fall through to "other" and we
# rely on the API's category field as a backup.
_PREFIX_TO_CATEGORY: dict[str, str] = {
    # Weather (already covered, but tagged for completeness)
    "KXHIGH": "weather",
    "KXHIGHT": "weather",
    "KXLOW": "weather",
    "KXLOWT": "weather",
    "KXTEMP": "weather",
    "KXRAIN": "weather",
    "KXSNOW": "weather",
    "KXHURRICANE": "weather",
    # Politics / governance
    "KXPRES": "politics",
    "KXSENATE": "politics",
    "KXHOUSE": "politics",
    "KXGOV": "politics",
    "KXELECTION": "politics",
    "KXNOMINEE": "politics",
    "KXSCOTUS": "politics",
    "KXCONGRESS": "politics",
    # Economics / monetary policy
    "KXFED": "economics",
    "KXCPI": "economics",
    "KXJOBS": "economics",
    "KXNFP": "economics",
    "KXGDP": "economics",
    "KXPPI": "economics",
    "KXUNEMP": "economics",
    "KXRECESSION": "economics",
    "KXRATE": "economics",
    # Markets / finance
    "KXSP": "finance",
    "KXNASDAQ": "finance",
    "KXBTC": "finance",
    "KXETH": "finance",
    "KXOIL": "finance",
    "KXGOLD": "finance",
    # Sports
    "KXNFL": "sports",
    "KXNBA": "sports",
    "KXMLB": "sports",
    "KXNHL": "sports",
    "KXNCAA": "sports",
    "KXSUPER": "sports",
    "KXWORLD": "sports",
    "KXTENNIS": "sports",
    "KXGOLF": "sports",
    "KXSOCCER": "sports",
    "KXMMA": "sports",
    "KXUFC": "sports",
    # Entertainment / culture
    "KXOSCAR": "entertainment",
    "KXEMMY": "entertainment",
    "KXGRAMMY": "entertainment",
    "KXBOX": "entertainment",  # box office
    "KXMOVIE": "entertainment",
    "KXTV": "entertainment",
    # Tech / science
    "KXAI": "tech",
    "KXSPACEX": "tech",
    "KXLAUNCH": "tech",
}


def classify_ticker(ticker: str, api_category: Optional[str] = None) -> str:
    """
    Classify a ticker into a high-level category.
    Prefers our prefix mapping; falls back to Kalshi's API category field;
    finally 'other'.
    """
    if not ticker:
        return "other"
    t = ticker.upper().strip()
    # Strip "KX" prefix and try progressively shorter matches against
    # the longest known prefixes first
    for prefix in sorted(_PREFIX_TO_CATEGORY.keys(), key=len, reverse=True):
        if t.startswith(prefix):
            return _PREFIX_TO_CATEGORY[prefix]
    # Fall back to Kalshi's category if provided
    if api_category:
        c = api_category.lower().strip()
        # Map Kalshi's category names to ours where they differ
        mapping = {
            "climate": "weather",
            "world": "politics",
            "crypto": "finance",
            "economics": "economics",
            "financials": "finance",
            "science and technology": "tech",
            "companies": "finance",
        }
        return mapping.get(c, c if c else "other")
    return "other"


def _category_allowed(category: str) -> bool:
    if BROADEN_CATEGORIES == "all":
        return True
    allowed = {c.strip() for c in BROADEN_CATEGORIES.split(",") if c.strip()}
    return category in allowed


# ============================================================================
# DISCOVERY
# ============================================================================
def discover_all_markets(registry, budget,
                         api_get_func=None,
                         host: Optional[str] = None,
                         market_state_factory=None,
                         existing_count_fn=None) -> dict:
    """
    Discover all open Kalshi markets across categories. Adds non-weather
    markets to the registry as 'observation-only' MarketStates (no lat/
    threshold, so they never produce trade intents but ARE picked up by
    shadow_log_evaluation in the run loop).

    Args:
      registry: MarketRegistry from the main bot
      budget: RequestBudget from the main bot
      api_get_func: the bot's api_get function (passed in to avoid imports)
      host: Kalshi API base URL (e.g. HOST from main bot)
      market_state_factory: callable that creates MarketState. We accept
        this as a parameter rather than importing because MarketState is
        defined in the main bot file.
      existing_count_fn: optional callable returning current non-weather
        market count, used for the BROADEN_MAX_MARKETS cap.

    Returns:
      dict with stats: {discovered, by_category, total_seen, skipped_*}
    """
    if api_get_func is None or host is None or market_state_factory is None:
        log.error("discover_all_markets: required dependencies not provided")
        return {"discovered": 0, "error": "missing_deps"}

    stats = {
        "discovered": 0,
        "total_seen": 0,
        "by_category": {},
        "skipped_weather": 0,        # already covered by main discovery
        "skipped_volume": 0,
        "skipped_hours": 0,
        "skipped_category": 0,
        "skipped_cap": 0,
        "skipped_no_ticker": 0,
        "skipped_already_tracked": 0,
    }

    # Walk all open markets paginated. Kalshi's /markets endpoint returns
    # everything when no series_ticker is specified.
    cursor = None
    pages = 0
    page_cap = 50  # safety: 50 * 1000 = 50k markets max per discovery run
    new_in_session = 0

    log.info("BroadDiscovery: starting universe scan (categories=%s, max=%d, min_vol=%.0f, max_hours=%.0f)",
             BROADEN_CATEGORIES, BROADEN_MAX_MARKETS,
             BROADEN_MIN_VOLUME, BROADEN_HOURS_TO_CLOSE_MAX)

    now = time.time()

    while True:
        params = {"status": "open", "limit": 1000}
        if cursor:
            params["cursor"] = cursor

        r = api_get_func(f"{host}/markets", params=params, budget=budget)
        if not r or not r.ok:
            log.warning("BroadDiscovery: API call failed at page %d", pages)
            break

        try:
            resp = r.json()
        except ValueError:
            log.warning("BroadDiscovery: bad JSON at page %d", pages)
            break

        markets = resp.get("markets") if isinstance(resp, dict) else None
        if not markets:
            markets = []

        for m in markets:
            stats["total_seen"] += 1
            if not isinstance(m, dict):
                continue

            ticker = (m.get("ticker") or "").upper().strip()
            if not ticker:
                stats["skipped_no_ticker"] += 1
                continue

            # Already tracked by weather discovery? Don't duplicate.
            if ticker in registry.states:
                stats["skipped_already_tracked"] += 1
                continue

            # Classify
            api_cat = m.get("category")
            category = classify_ticker(ticker, api_cat)

            # Skip weather here — main bot already handles weather discovery
            # and we don't want to step on its parsing logic.
            if category == "weather":
                stats["skipped_weather"] += 1
                continue

            if not _category_allowed(category):
                stats["skipped_category"] += 1
                continue

            # Volume gate
            volume = _safe_float(m.get("volume_24h") or m.get("volume") or 0)
            if volume < BROADEN_MIN_VOLUME:
                stats["skipped_volume"] += 1
                continue

            # Time-to-close gate
            close_ts = _parse_close_ts(m)
            if close_ts is not None:
                hours_to_close = (close_ts - now) / 3600.0
                if hours_to_close <= 0:
                    continue  # already expired
                if hours_to_close > BROADEN_HOURS_TO_CLOSE_MAX:
                    stats["skipped_hours"] += 1
                    continue

            # Cap check (count of non-weather markets only)
            if existing_count_fn is not None:
                if existing_count_fn() + new_in_session >= BROADEN_MAX_MARKETS:
                    stats["skipped_cap"] += 1
                    continue

            # Build a minimal MarketState. NOTE: lat=None, threshold=None,
            # so maybe_refresh_model will return early and no model runs.
            # The shadow logger will still record the market quote each cycle.
            try:
                state = market_state_factory(
                    ticker=ticker,
                    series_ticker=m.get("series_ticker") or m.get("event_ticker"),
                    market=m,
                    city=None,
                    market_type=category.upper(),  # e.g. "POLITICS", "SPORTS"
                    target_date=None,
                    date_str=None,
                    threshold=None,
                    metar_id=None,
                    lat=None,
                    lon=None,
                    tz_name="UTC",
                    fractional=bool(m.get("fractional_trading_enabled", False)),
                )
                registry.upsert(state)
                new_in_session += 1
                stats["discovered"] += 1
                stats["by_category"][category] = (
                    stats["by_category"].get(category, 0) + 1
                )
            except Exception as e:
                log.warning("BroadDiscovery: failed to build MarketState for %s: %s",
                            ticker, e)
                continue

        cursor = resp.get("cursor") if isinstance(resp, dict) else None
        pages += 1
        if not cursor or not markets or pages >= page_cap:
            if pages >= page_cap:
                log.warning("BroadDiscovery: hit page cap (%d)", page_cap)
            break
        time.sleep(0.05)  # be polite to the API

    log.info(
        "BroadDiscovery: %d new markets across %d categories (seen=%d, "
        "weather_skipped=%d, vol_skipped=%d, hours_skipped=%d, "
        "cat_skipped=%d, cap_skipped=%d, dup_skipped=%d) in %d pages",
        stats["discovered"], len(stats["by_category"]),
        stats["total_seen"], stats["skipped_weather"],
        stats["skipped_volume"], stats["skipped_hours"],
        stats["skipped_category"], stats["skipped_cap"],
        stats["skipped_already_tracked"], pages,
    )
    if stats["by_category"]:
        cat_summary = ", ".join(
            f"{k}={v}" for k, v in sorted(
                stats["by_category"].items(), key=lambda x: -x[1]
            )
        )
        log.info("BroadDiscovery: by category: %s", cat_summary)

    return stats


# ============================================================================
# HELPERS
# ============================================================================
def _safe_float(v) -> float:
    try:
        return float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _parse_close_ts(market: dict) -> Optional[float]:
    """Extract close time as unix seconds. Tries multiple field names."""
    for field in ("close_time", "expiration_time", "expected_expiration_time"):
        v = market.get(field)
        if not v:
            continue
        # ISO 8601 string -> epoch
        try:
            from datetime import datetime
            # Kalshi uses "2025-04-30T17:00:00Z"
            if isinstance(v, str):
                v_clean = v.rstrip("Z")
                dt = datetime.fromisoformat(v_clean)
                return dt.timestamp()
            if isinstance(v, (int, float)):
                return float(v)
        except (ValueError, TypeError):
            continue
    return None
