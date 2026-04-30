"""
PCV1 aka V24.5 — Kalshi Weather Bot
====================================
Incorporates v24 + v24.1 + v24.2 + v24.3 + v24.4 + v24.5 review fixes.

V24.5 (hotfix)
  J. Per-coordinate ensemble forecast cache. With ~430 markets across ~50
     unique cities, repeated identical lat/lon ensemble fetches were tripping
     Open-Meteo's free-tier 429 rate limit, which starved compute_next_refresh
     and kept calib=empty / intents=0. Both ensemble call sites now check an
     in-memory cache keyed on (round(lat,4), round(lon,4), target/month, type)
     with a 30-min TTL (ensemble runs every 6h on the upstream side, so 30 min
     is well within freshness). Cache is bounded at 256 entries (LRU-by-time).

V24.4 (hotfix)
  I. Open-Meteo retired /v1/ensemble on api.open-meteo.com and moved it to
     a dedicated subdomain. Old URL returns 404 for every request. Both
     ensemble call sites (temperature_prob and precipitation_prob) now hit
     https://ensemble-api.open-meteo.com/v1/ensemble. /v1/gfs (HRRR) and
     /v1/archive remain on their own hosts (api / archive-api), unchanged.

V24.3 (hotfix)
  H. fetch_weather_series / discover_weather_markets: dict.get(key, [])
     only returns the [] default when the key is MISSING. If the API
     returns {"series": null} (which Kalshi can do for empty pages or
     under partial-auth conditions), the get returned None and the
     subsequent `for s in series_list` raised TypeError. Now coerced via
     explicit `if not series_list: series_list = []`. Same fix applied
     to the markets list in discover_weather_markets, plus an
     isinstance(item, dict) guard inside the loops.

V24.2 ADJUSTMENTS
  A. _hrrr_blend_weight returns None for hours_to_close <= 0 — caller skips
     blend on already-closed markets.
  B. reconcile_settlements passes a 3-day since_ts watermark so each pass
     doesn't re-paginate the entire portfolio history.
  C. Ensemble error messages distinguish "no ensemble data at all" from
     "no min/max members for this side".
  D. Disagreement check uses max(sigma_ecmwf, sigma_gfs) — ECMWF (51) and
     GFS (31) have different intrinsic spreads; full-pool σ over-weights the
     larger ensemble.
  E. Discovery page cap raised 5 -> 10 with warn-log when hit.
  F. METAR cache: removed redundant _for_date guard (cache key already
     includes hour bucket; date is derivable from the bucket).
  G. Settlement CLV computed as revenue/contracts (cents) on the settlement
     row itself — last_price_dollars lives on /markets/{ticker}, not on
     /portfolio/settlements rows.

CRITICAL BUG FIXES (carried from v24)
  1.  Per-city timezone resolution (PHX/DEN/LAX/SEA no longer use ET).
  2.  is_liquid: max-spread gate, scales with price.
  3.  _to_cents: symmetric ROUND_HALF_EVEN for both asks and bids (v24.1).
  4.  precipitation_prob: explicit length_unit=metric to lock cm/mm contract.
  5.  LOW pace adjustment: only applied to overnight METAR window (v24.1).
  6.  METAR cache key: hourly precision, no target_date pollution (v24.1).
  7.  Position recorded only after fill confirmation.
  8.  RequestBudget: hard write-reserve floor.

STRATEGY / RISK FIXES
  9.  Settlement reconciliation via /portfolio/settlements bulk endpoint (v24.1).
  10. Bayesian shrinkage on edge AND p_side for sizing.
  11. HRRR blend curve flattened: 0.7@1h → 0.5@12h → 0.4@24h (v24.1).
  12. Bracket dead-zone scales with sigma.
  13. AGGRO kelly_fraction capped at 0.30.
  14. Per-(city, target_date) exposure cap. event_key is now a string (v24.1).

DISCOVERY / EFFICIENCY (v24.1)
  15. Discovery reinstated to per-series fetch via /series + /markets per series
      (avoids pulling 20k unrelated markets across all of Kalshi).
  16. is_liquid size-check explicitly skipped on discovery payloads where
      yes_ask_size_fp is unpopulated.
  17. Refresh prioritization ignores paper-only profiles unless all profiles
      are paper.
  18. Non-illiquidity skip reasons get REFRESH_COLD (was wasting ~12 polls/hr).
  19. Telegram flush moved into heartbeat block.
  20. Disagreement skip computes ECMWF/GFS means per-statistic (high vs low).

CODE QUALITY
  21. Python >= 3.10. PEP 604 generics.
  22. Split exception handling.
  23. _seen_edges pruned periodically.
  24. _metar_cache trimmed; cache key drops target_date (v24.1).
  25. load_account_budget probes X-RateLimit-Limit header.
  26. Heartbeat includes 429 count, intents, oldest model age, calibration.
  27. Deterministic client_order_id from intent hash + 30s window (v24.1 —
      no cache needed).
  28. DEMO_MODE driven by env.

ADDITIONS
  - CalibrationTracker (predicted vs realized hit rate by decile).
  - Forecast disagreement skip (ECMWF vs GFS > 1.5σ) for both HIGH and LOW.
  - reconcile_settlements uses last_price_dollars (correct field name).

REQUIREMENTS
  python >= 3.10
  pip install requests python-dotenv kalshi_python_sync
"""

from __future__ import annotations

import os
import sys
import time
import csv
import json
import re
import logging
import calendar
import statistics
import hashlib
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone, date, timedelta
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Optional, Any
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from kalshi_python_sync import Configuration, KalshiClient

if sys.version_info < (3, 10):
    raise RuntimeError("PCV1/V24 requires Python 3.10+ (uses PEP 604 generics).")

load_dotenv()

# ============================================================================
# CONFIG
# ============================================================================
def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


# Live trading toggles (driven by env, fix #22)
DEMO_MODE = _env_bool("DEMO_MODE", True)
BANKROLL_BASE_DEFAULT = _env_float("BANKROLL_BASE", 1000.0)
BANKROLL_AGGRO_DEFAULT = _env_float("BANKROLL_AGGRO", 1000.0)

# Pricing / liquidity
MIN_PRICE_CENTS = 8
MAX_PRICE_CENTS = 92
MIN_MINS_TO_EXPIRY = 15
MIN_LIQUIDITY_SIZE = 10
MAX_SPREAD_CENTS_BASE = 12          # fix #2: renamed and widened
MAX_SPREAD_PRICE_FRACTION = 0.20    # spread can also be up to 20% of price

# Forecast / model gates
TAIL_MIN_MEMBERS = 5
SAME_DAY_HOURS_THRESHOLD = 24
ENSEMBLE_MODELS = "ecmwf_ifs04,gfs025"
HRRR_MODEL = "ncep_hrrr_conus"
ECMWF_PREFIX = "ecmwf_ifs04"
GFS_PREFIX = "gfs025"
DISAGREEMENT_SIGMA_LIMIT = 1.5      # skip if model means disagree > 1.5σ

# Mode-specific model cache TTLs
MODEL_TTL_NEXT_DAY = 6 * 3600       # 6h: ECMWF/GFS issue cycles
MODEL_TTL_SAME_DAY = 15 * 60        # 15m: HRRR runs hourly
MODEL_TTL_PRECIP = 6 * 3600

# Refresh schedule
REFRESH_URGENT = 30
REFRESH_NEAR_EDGE = 90
REFRESH_NORMAL = 300
REFRESH_COLD = 900

# Budget
DEFAULT_TOKENS_PER_SEC = 100
REQUEST_COST_READ = 10
REQUEST_COST_WRITE = 10
WRITE_RESERVE_FLOOR_TOKENS = 200    # fix #8: never let writes starve

# Discovery / dedup
DISCOVERY_INTERVAL_SECONDS = 1800
SEEN_EDGE_TTL_MINUTES = 120
SEEN_EDGE_PRICE_BUCKET_CENTS = 2
SEEN_EDGES_PRUNE_INTERVAL = 600     # fix #17

# Bayesian shrinkage on edge (fix #10)
EDGE_SHRINK_K = 8                   # smaller k -> less shrinkage

# Bracket dead-zone (fix #12)
BRACKET_BUFFER_F_MIN = 0.4
BRACKET_BUFFER_SIGMA_FRACTION = 0.15

# Per-event correlation cap (fix #14)
MAX_EVENT_EXPOSURE_PCT = 0.15       # at most 15% of bankroll on one (city, date)

# Order fill polling (fix #7)
ORDER_FILL_POLL_SECONDS = 1.0
ORDER_FILL_TIMEOUT_SECONDS = 8.0
ORDER_IDEMPOTENCY_TTL_SECONDS = 30  # fix idempotent-id reuse window

# Telegram rate limit (fix #21)
TELEGRAM_MIN_INTERVAL_SECONDS = 2.0
TELEGRAM_BATCH_FLUSH_SECONDS = 30

# Settlement reconciliation
SETTLEMENT_RECONCILE_INTERVAL = 3600  # once an hour

# Misc
METAR_CACHE_MAX_ENTRIES = 200
HEARTBEAT_INTERVAL_SECONDS = 900

# v24.5: per-(lat,lon) ensemble cache to avoid 429s when many markets share a city
ENSEMBLE_CACHE_TTL_SECONDS = 1800   # 30 min — upstream model cycles every 6h
ENSEMBLE_CACHE_MAX_ENTRIES = 256

# Secrets / endpoints
API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")
PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH")
HOST = os.getenv("KALSHI_HOST", "https://api.elections.kalshi.com/trade-api/v2")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
NWS_BIAS_FILE = os.getenv("NWS_BIAS_FILE", "nws_bias.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("kalshi_bot.log"), logging.StreamHandler()],
)
log = logging.getLogger("pcv1")

# ============================================================================
# KALSHI CLIENT
# ============================================================================
if not PRIVATE_KEY_PATH or not os.path.exists(PRIVATE_KEY_PATH):
    log.error("KALSHI_PRIVATE_KEY_PATH not set or missing")
    sys.exit(1)

with open(PRIVATE_KEY_PATH, "r") as _f:
    _private_key_pem = _f.read()

kalshi_config = Configuration(host=HOST)
kalshi_config.api_key_id = API_KEY_ID
kalshi_config.private_key_pem = _private_key_pem
client = KalshiClient(kalshi_config)


# ============================================================================
# CITY / TZ / BIAS / TICKER PARSING (fix #1: per-city tz)
# ============================================================================
CITY_INFO: dict[str, dict[str, Any]] = {
    "NY":   {"lat": 40.7794, "lon": -73.9692, "metar": "KNYC", "tz": "America/New_York",   "label": "Central Park"},
    "LAX":  {"lat": 33.9425, "lon": -118.4081, "metar": "KLAX", "tz": "America/Los_Angeles","label": "LAX"},
    "AUS":  {"lat": 30.1975, "lon": -97.6664,  "metar": "KAUS", "tz": "America/Chicago",    "label": "Austin-Bergstrom"},
    "MIA":  {"lat": 25.7959, "lon": -80.2870,  "metar": "KMIA", "tz": "America/New_York",   "label": "Miami Intl"},
    "BOS":  {"lat": 42.3656, "lon": -71.0096,  "metar": "KBOS", "tz": "America/New_York",   "label": "Logan"},
    "CHI":  {"lat": 41.7868, "lon": -87.7522,  "metar": "KMDW", "tz": "America/Chicago",    "label": "Midway"},
    "OKC":  {"lat": 35.3931, "lon": -97.6007,  "metar": "KOKC", "tz": "America/Chicago",    "label": "Will Rogers"},
    "PHIL": {"lat": 39.8744, "lon": -75.2424,  "metar": "KPHL", "tz": "America/New_York",   "label": "Philly Intl"},
    "ATL":  {"lat": 33.6407, "lon": -84.4277,  "metar": "KATL", "tz": "America/New_York",   "label": "Hartsfield"},
    "DC":   {"lat": 38.8512, "lon": -77.0402,  "metar": "KDCA", "tz": "America/New_York",   "label": "Reagan Natl"},
    "DEN":  {"lat": 39.8561, "lon": -104.6737, "metar": "KDEN", "tz": "America/Denver",     "label": "Denver Intl"},
    "PHX":  {"lat": 33.4373, "lon": -112.0078, "metar": "KPHX", "tz": "America/Phoenix",    "label": "Sky Harbor"},
    "SEA":  {"lat": 47.4502, "lon": -122.3088, "metar": "KSEA", "tz": "America/Los_Angeles","label": "Sea-Tac"},
    "HOU":  {"lat": 29.9844, "lon": -95.3414,  "metar": "KIAH", "tz": "America/Chicago",    "label": "Bush"},
    "LV":   {"lat": 36.0840, "lon": -115.1537, "metar": "KLAS", "tz": "America/Los_Angeles","label": "Harry Reid"},
    "DAL":  {"lat": 32.8998, "lon": -97.0403,  "metar": "KDFW", "tz": "America/Chicago",    "label": "DFW"},
    "MIN":  {"lat": 44.8848, "lon": -93.2223,  "metar": "KMSP", "tz": "America/Chicago",    "label": "MSP"},
    "SFO":  {"lat": 37.6213, "lon": -122.3790, "metar": "KSFO", "tz": "America/Los_Angeles","label": "SFO"},
}

_T_PREFIX_MAP = {
    "TBOS": "BOS", "TATL": "ATL", "TDC": "DC",
    "TDEN": "DEN", "TPHX": "PHX", "TSEA": "SEA",
    "THOU": "HOU", "TLV": "LV", "TDAL": "DAL",
    "TMIN": "MIN", "TSFO": "SFO", "TOKC": "OKC",
}


def resolve_city(city_code: str):
    if city_code in CITY_INFO:
        return city_code, CITY_INFO[city_code]
    if city_code in _T_PREFIX_MAP:
        base = _T_PREFIX_MAP[city_code]
        return base, CITY_INFO[base]
    return None, None


def city_tz(city_code: str) -> ZoneInfo:
    info = CITY_INFO.get(city_code, {})
    return ZoneInfo(info.get("tz", "America/New_York"))


DEFAULT_NWS_BIAS = {
    "NY":   {"high_F": -0.3, "low_F":  0.0},
    "LAX":  {"high_F":  0.0, "low_F":  0.5},
    "MIA":  {"high_F": -1.5, "low_F":  0.5},
    "CHI":  {"high_F": -0.4, "low_F":  0.0},
    "AUS":  {"high_F":  0.5, "low_F":  0.0},
    "DEN":  {"high_F":  0.0, "low_F":  0.0, "_extra_sigma_F": 1.5},
    "BOS":  {"high_F": -0.2, "low_F":  0.0},
    "PHIL": {"high_F":  0.0, "low_F":  0.0},
    "SFO":  {"high_F":  0.0, "low_F":  0.0, "_extra_sigma_F": 1.0},
    "DAL":  {"high_F":  0.3, "low_F":  0.0},
    "HOU":  {"high_F":  0.0, "low_F":  0.0},
    "PHX":  {"high_F": -1.0, "low_F":  0.5},
    "LV":   {"high_F":  0.0, "low_F":  0.0},
    "MIN":  {"high_F":  0.0, "low_F":  0.0},
    "OKC":  {"high_F":  0.0, "low_F":  0.0},
    "DC":   {"high_F":  0.0, "low_F":  0.0},
    "ATL":  {"high_F":  0.0, "low_F":  0.0},
    "SEA":  {"high_F":  0.0, "low_F": -0.3, "_extra_sigma_F": 0.8},
}


def load_nws_bias():
    if os.path.exists(NWS_BIAS_FILE):
        try:
            with open(NWS_BIAS_FILE, "r") as f:
                data = json.load(f)
            merged = {k: dict(v) for k, v in DEFAULT_NWS_BIAS.items()}
            for k, v in data.items():
                merged.setdefault(k, {}).update(v)
            return merged
        except (json.JSONDecodeError, OSError) as e:
            log.warning("Could not load %s, using defaults: %s", NWS_BIAS_FILE, e)
    return DEFAULT_NWS_BIAS


NWS_BIAS = load_nws_bias()


def get_bias(city: str, market_type: str) -> tuple[float, float]:
    entry = NWS_BIAS.get(city, {})
    if "HIGH" in market_type or market_type == "TEMP":
        return entry.get("high_F", 0.0), entry.get("_extra_sigma_F", 0.0)
    if "LOW" in market_type:
        return entry.get("low_F", 0.0), entry.get("_extra_sigma_F", 0.0)
    if market_type == "RAIN":
        return entry.get("rain_in", 0.0), 0.0
    if market_type == "SNOW":
        return entry.get("snow_in", 0.0), 0.0
    return 0.0, 0.0


MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def _parse_date(ticker_upper: str) -> Optional[str]:
    m = re.search(r"-(\d{2})([A-Z]{3})(\d{2})", ticker_upper)
    if m:
        day, mon, yr = m.groups()
        try:
            return f"20{yr}-{MONTH_MAP[mon]:02d}-{day}"
        except KeyError:
            return None
    return None


def _safe_threshold(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    try:
        return float(s.replace("T", "").replace("B", ""))
    except ValueError:
        return None


def parse_ticker(ticker: str):
    """Returns (city, date_str, hour, threshold, market_type, target_date)."""
    t = ticker.upper()
    target_date = _parse_date(t)

    rain_match = re.search(r"KXRAIN([A-Z]+)-([A-Z]{3}\d{2})-?([BT]?[\d.]+)?", t)
    if rain_match:
        return (rain_match.group(1), rain_match.group(2), None,
                _safe_threshold(rain_match.group(3)), "RAIN", target_date)

    snow_match = re.search(r"KXSNOW([A-Z]+)-([A-Z]{3}\d{2})-?([BT]?[\d.]+)?", t)
    if snow_match:
        return (snow_match.group(1), snow_match.group(2), None,
                _safe_threshold(snow_match.group(3)), "SNOW", target_date)

    temp_match = re.search(
        r"KX(HIGHT?|LOWT|TEMP)([A-Z]+)-(\d{2}[A-Z]{3}\d{2})-?([BT]?[\d.]+)?", t,
    )
    if temp_match:
        return (temp_match.group(2), temp_match.group(3), None,
                _safe_threshold(temp_match.group(4)),
                temp_match.group(1), target_date)

    return None, None, None, None, None, None


# ============================================================================
# REQUEST BUDGET (fix #8: hard write reserve)
# ============================================================================
class RequestBudget:
    """Token bucket with separate read/write accounting and a hard write floor."""

    def __init__(self, tokens_per_sec: float = DEFAULT_TOKENS_PER_SEC,
                 write_reserve_tokens: float = WRITE_RESERVE_FLOOR_TOKENS):
        self.tokens_per_sec = tokens_per_sec
        self.capacity = tokens_per_sec * 60.0
        self.tokens = self.capacity
        self.write_reserve = min(write_reserve_tokens, self.capacity * 0.4)
        self.last_refill = time.monotonic()
        self.consumed_429 = 0  # for heartbeat

    @property
    def available(self) -> float:
        self._refill()
        return self.tokens

    @property
    def usable_for_reads(self) -> float:
        return max(0.0, self.available - self.write_reserve)

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        if elapsed > 0:
            self.tokens = min(self.capacity,
                              self.tokens + elapsed * self.tokens_per_sec)
            self.last_refill = now

    def try_consume(self, cost: float, urgent: bool = False, write: bool = False) -> bool:
        """
        Reads draw from (available - write_reserve).
        Writes draw from full available (the reserve exists FOR writes).
        Urgent reads can also dip into the reserve, but only halfway.
        """
        self._refill()
        if write:
            ceiling = self.tokens
        elif urgent:
            ceiling = max(0.0, self.tokens - self.write_reserve * 0.5)
        else:
            ceiling = self.usable_for_reads
        if cost <= ceiling:
            self.tokens -= cost
            return True
        return False

    def wait_for(self, cost: float, urgent: bool = False, write: bool = False,
                 max_wait: float = 10.0) -> bool:
        start = time.monotonic()
        while time.monotonic() - start < max_wait:
            if self.try_consume(cost, urgent=urgent, write=write):
                return True
            time.sleep(0.2)
        return False


def load_account_budget() -> RequestBudget:
    """Fix #19: actually probe Kalshi rate-limit tier headers, fall back to env."""
    rate = DEFAULT_TOKENS_PER_SEC
    try:
        # Many Kalshi endpoints echo X-RateLimit-Limit on auth'd calls.
        url = f"{HOST}/exchange/status"
        r = requests.get(url, timeout=5)
        hdr = r.headers.get("X-RateLimit-Limit") or r.headers.get("x-ratelimit-limit")
        if hdr:
            try:
                rate = float(hdr)
            except ValueError:
                pass
    except requests.RequestException:
        pass
    rate = float(os.getenv("KALSHI_TOKENS_PER_SEC", rate))
    log.info("RequestBudget: %.0f tokens/sec, write reserve=%d tokens",
             rate, WRITE_RESERVE_FLOOR_TOKENS)
    return RequestBudget(tokens_per_sec=rate)


def api_get(url: str, params: Optional[dict] = None,
            budget: Optional[RequestBudget] = None,
            cost: int = REQUEST_COST_READ,
            urgent: bool = False, retries: int = 3,
            headers: Optional[dict] = None):
    """Budget-aware GET with split exception handling (fix #16)."""
    if budget is not None:
        if not budget.wait_for(cost, urgent=urgent, max_wait=8.0):
            log.warning("Budget exhausted, skipping: %s", url)
            return None

    delay = 2.0
    auth_retried = False
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=15)
        except requests.Timeout:
            log.warning("Timeout (%d) on %s", attempt + 1, url)
            time.sleep(delay)
            delay *= 2
            continue
        except requests.ConnectionError as e:
            log.warning("Connection error (%d) on %s: %s", attempt + 1, url, e)
            time.sleep(delay)
            delay *= 2
            continue
        except requests.RequestException as e:
            log.warning("Request failed (%d) on %s: %s", attempt + 1, url, e)
            time.sleep(delay)
            delay *= 2
            continue

        if r.status_code == 401 and not auth_retried:
            log.warning("401 from %s — re-auth", url)
            try:
                if hasattr(client, "login"):
                    client.login()
                elif hasattr(client, "authenticate"):
                    client.authenticate()
            except Exception as e:
                log.warning("Re-auth failed: %s", e)
            auth_retried = True
            continue
        if r.status_code == 429:
            if budget is not None:
                budget.consumed_429 += 1
            log.warning("429 rate limited, backoff %.1fs", delay)
            time.sleep(delay)
            delay *= 2
            continue
        return r
    return None


# ============================================================================
# WEATHER MODELS
# ============================================================================
_metar_cache: dict[tuple, dict] = {}
# fix #18: removed unused _forecast_cache

# v24.5: ensemble forecast cache. Key shape:
#   ("temp", round(lat,4), round(lon,4), target_date_str, forecast_days, tz_name)
#   ("precip", round(lat,4), round(lon,4), market_type, contract_year, contract_month, tz_name)
# Value: (fetched_at_epoch, payload_tuple)
_ensemble_cache: dict[tuple, tuple] = {}


def _trim_metar_cache():
    if len(_metar_cache) <= METAR_CACHE_MAX_ENTRIES:
        return
    # drop oldest by hour bucket
    keys_sorted = sorted(_metar_cache.keys(), key=lambda k: k[1])
    for k in keys_sorted[: len(_metar_cache) - METAR_CACHE_MAX_ENTRIES]:
        _metar_cache.pop(k, None)


def _trim_ensemble_cache():
    if len(_ensemble_cache) <= ENSEMBLE_CACHE_MAX_ENTRIES:
        return
    # drop oldest by fetched_at
    keys_sorted = sorted(_ensemble_cache.keys(),
                         key=lambda k: _ensemble_cache[k][0])
    for k in keys_sorted[: len(_ensemble_cache) - ENSEMBLE_CACHE_MAX_ENTRIES]:
        _ensemble_cache.pop(k, None)


def fetch_hrrr_forecast(lat: float, lon: float, target_date_str: str,
                        tz_name: str = "America/New_York"):
    """Returns (max_F, min_F, age_hours, run_time_str) or all-None."""
    url = "https://api.open-meteo.com/v1/gfs"
    params = {
        "latitude": lat, "longitude": lon,
        "hourly": "temperature_2m",
        "models": HRRR_MODEL,
        "forecast_days": 2,
        "timezone": tz_name,                 # fix #1: city-local tz
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        log.debug("HRRR fetch failed: %s", e)
        return None, None, None, None

    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    temps_c = hourly.get("temperature_2m", [])
    if not times or not temps_c:
        return None, None, None, None

    day_temps = [
        temps_c[i] for i, t in enumerate(times)
        if t.startswith(target_date_str)
        and i < len(temps_c) and temps_c[i] is not None
    ]
    if not day_temps:
        return None, None, None, None

    max_f = max(day_temps) * 9 / 5 + 32
    min_f = min(day_temps) * 9 / 5 + 32
    return max_f, min_f, 1.0, "HRRR-latest"


def fetch_ensemble_forecast(lat: float, lon: float, target_date_str: str,
                            forecast_days: int = 7,
                            tz_name: str = "America/New_York"):
    """Returns (max_per_member, min_per_member, target_idxs, age_hours,
                ecmwf_maxes, gfs_maxes, ecmwf_mins, gfs_mins).
    v24.1: also returns ecmwf/gfs MIN distributions so the disagreement check
    works correctly for LOW markets (was only computing max disagreement).
    v24.5: per-(lat,lon,date) cache with 30-min TTL eliminates duplicate
    fetches across markets sharing a city — primary fix for 429 storm.
    """
    fc_days = min(max(forecast_days, 2), 16)
    cache_key = ("temp", round(float(lat), 4), round(float(lon), 4),
                 target_date_str, fc_days, tz_name)
    now_ts = time.time()
    cached = _ensemble_cache.get(cache_key)
    if cached and (now_ts - cached[0]) < ENSEMBLE_CACHE_TTL_SECONDS:
        return cached[1]

    # v24.4: Open-Meteo split /v1/ensemble onto its own subdomain.
    # api.open-meteo.com/v1/ensemble now returns 404; use ensemble-api host.
    url = "https://ensemble-api.open-meteo.com/v1/ensemble"
    params = {
        "latitude": lat, "longitude": lon,
        "hourly": "temperature_2m",
        "models": ENSEMBLE_MODELS,
        "forecast_days": fc_days,
        "timezone": tz_name,
    }
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        log.warning("Ensemble fetch failed: %s", e)
        # don't cache failures — let the next caller retry
        return None, None, [], None, [], [], [], []

    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        return None, None, [], None, [], [], [], []

    now_utc = datetime.now(timezone.utc)
    age_hours = round((now_utc.hour % 6) + (now_utc.minute / 60.0) + 3.0, 1)

    target_idxs = [i for i, t in enumerate(times) if t.startswith(target_date_str)]
    if not target_idxs:
        return None, None, [], age_hours, [], [], [], []

    member_keys = [k for k in hourly.keys() if k.startswith("temperature_2m")]
    if not member_keys:
        return None, None, [], age_hours, [], [], [], []

    max_per_member: list[float] = []
    min_per_member: list[float] = []
    ecmwf_maxes: list[float] = []
    gfs_maxes: list[float] = []
    ecmwf_mins: list[float] = []
    gfs_mins: list[float] = []

    for key in member_keys:
        series = hourly[key]
        slice_vals = [series[i] for i in target_idxs
                      if i < len(series) and series[i] is not None]
        if not slice_vals:
            continue
        mx = max(slice_vals)
        mn = min(slice_vals)
        max_per_member.append(mx)
        min_per_member.append(mn)
        if ECMWF_PREFIX in key:
            ecmwf_maxes.append(mx)
            ecmwf_mins.append(mn)
        elif GFS_PREFIX in key:
            gfs_maxes.append(mx)
            gfs_mins.append(mn)

    result = (max_per_member, min_per_member, target_idxs, age_hours,
              ecmwf_maxes, gfs_maxes, ecmwf_mins, gfs_mins)
    # v24.5: cache successful fetch
    _ensemble_cache[cache_key] = (now_ts, result)
    _trim_ensemble_cache()
    return result


def fetch_metar_temp(metar_id: str, tz_name: str = "America/New_York",
                     target_date_local: Optional[date] = None):
    """
    Fetch METAR observations and bucket today's max/min and overnight min.
    v24.1 changes:
      - cache key dropped target_date (METAR raw is the same; bucketing differs)
      - returns overnight_min_F covering the previous-night NWS climate window
        (local 00:00–07:00) so LOW pace adjustment uses the correct period.
    """
    now_utc = datetime.now(timezone.utc)
    cache_key = (metar_id, now_utc.strftime("%Y%m%d%H"))
    cached = _metar_cache.get(cache_key)
    tz = ZoneInfo(tz_name)
    today_local = target_date_local or datetime.now(tz).date()

    # v24.2: cache key (metar_id, YYYYMMDDHH UTC) is sufficient — no need for
    # the redundant _for_date guard. A new hour bucket invalidates naturally.
    if cached:
        return cached

    url = "https://aviationweather.gov/api/data/metar"
    params = {"ids": metar_id, "format": "json", "hours": 24}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        rows = r.json() or []
    except (requests.RequestException, ValueError) as e:
        log.debug("METAR fetch failed for %s: %s", metar_id, e)
        return None

    today_temps_c: list[float] = []
    overnight_temps_c: list[float] = []  # local 00:00–07:00 of today_local
    latest_c: Optional[float] = None
    overnight_start = datetime.combine(today_local, datetime.min.time(), tzinfo=tz)
    overnight_end = overnight_start + timedelta(hours=7)

    for row in rows:
        ts_local = None
        epoch = row.get("obsTime") or row.get("obs_time")
        if epoch is not None:
            try:
                ts_local = datetime.fromtimestamp(int(epoch), tz=timezone.utc).astimezone(tz)
            except (TypeError, ValueError, OSError):
                ts_local = None
        if ts_local is None:
            rt = row.get("reportTime") or row.get("report_time")
            if rt:
                try:
                    ts_local = datetime.fromisoformat(rt.replace("Z", "+00:00")).astimezone(tz)
                except (TypeError, ValueError):
                    ts_local = None
        if ts_local is None:
            continue

        t = row.get("temp")
        if t is None:
            continue
        try:
            tc = float(t)
        except (TypeError, ValueError):
            continue

        if ts_local.date() == today_local:
            today_temps_c.append(tc)
            if latest_c is None:
                latest_c = tc
            if overnight_start <= ts_local < overnight_end:
                overnight_temps_c.append(tc)

    if not today_temps_c:
        return None

    result = {
        "current_F": latest_c * 9 / 5 + 32 if latest_c is not None else None,
        "max_today_F": max(today_temps_c) * 9 / 5 + 32,
        "min_today_F": min(today_temps_c) * 9 / 5 + 32,
        "overnight_min_F": (min(overnight_temps_c) * 9 / 5 + 32
                            if overnight_temps_c else None),
        "n_obs_today": len(today_temps_c),
        "n_obs_overnight": len(overnight_temps_c),
    }
    _metar_cache[cache_key] = result
    _trim_metar_cache()
    return result


def _hrrr_blend_weight(hours_to_close: float) -> Optional[float]:
    """
    v24.1: flattened curve. HRRR has an 18h horizon and is most reliable in the
    first ~12h. Anchors: 0.7 at 1h, 0.5 at 12h, 0.4 at 24h. Two-segment linear.

    v24.2: returns None when hours_to_close <= 0 (market already closed during
    the model fetch — caller should skip the blend; the market isn't tradeable).
    """
    if hours_to_close <= 0:
        return None
    h = max(1.0, min(24.0, hours_to_close))
    if h <= 12.0:
        # 1h -> 0.70, 12h -> 0.50
        return 0.70 - (h - 1.0) * (0.20 / 11.0)
    # 12h -> 0.50, 24h -> 0.40
    return 0.50 - (h - 12.0) * (0.10 / 12.0)


def temperature_prob(lat: float, lon: float, threshold_f: float,
                     market_type: str, target_date_str: Optional[str],
                     metar_id: Optional[str] = None,
                     hours_to_close: float = 999, city: Optional[str] = None,
                     tz_name: str = "America/New_York"):
    """Returns (prob, forecast_F, sigma, n, age_h, mode, skip_reason)."""
    tz = ZoneInfo(tz_name)
    today_local = datetime.now(tz).date()

    if not target_date_str:
        target_date_str = (today_local + timedelta(days=1)).isoformat()
    try:
        target_d = date.fromisoformat(target_date_str)
        days_out = (target_d - today_local).days
    except ValueError:
        target_d = today_local + timedelta(days=1)
        days_out = 1

    if days_out < 0:
        return 50.0, None, None, 0, None, "expired", "target date past"

    forecast_days = min(max(days_out + 2, 2), 16)
    (max_members, min_members, _, age_hours,
     ecmwf_max, gfs_max, ecmwf_min, gfs_min) = fetch_ensemble_forecast(
        lat, lon, target_date_str, forecast_days=forecast_days, tz_name=tz_name)

    if not max_members and not min_members:
        return 50.0, None, None, 0, age_hours, "ensemble", "no ensemble data"

    use_min = "LOW" in market_type
    members_c = min_members if use_min else max_members
    if not members_c:
        # v24.2: ensemble responded but had no members for the side we need.
        side = "min" if use_min else "max"
        return 50.0, None, None, 0, age_hours, "ensemble", f"no ensemble {side} members"
    members_f = [c * 9 / 5 + 32 for c in members_c]

    # Forecast disagreement skip — works for both HIGH and LOW (v24.1).
    # v24.2: scale by max(sigma_ecmwf, sigma_gfs), not full-pool sigma. ECMWF
    # (51 members) and GFS (31 members) have different intrinsic spreads —
    # mixing them inflates the disagreement-tolerance band when ECMWF dominates.
    ecmwf_check = ecmwf_min if use_min else ecmwf_max
    gfs_check = gfs_min if use_min else gfs_max
    if ecmwf_check and gfs_check:
        try:
            ecmwf_f = [c * 9 / 5 + 32 for c in ecmwf_check]
            gfs_f = [c * 9 / 5 + 32 for c in gfs_check]
            mean_e = statistics.mean(ecmwf_f)
            mean_g = statistics.mean(gfs_f)
            sigma_e = statistics.pstdev(ecmwf_f) if len(ecmwf_f) > 1 else 4.0
            sigma_g = statistics.pstdev(gfs_f) if len(gfs_f) > 1 else 4.0
            sigma_check = max(sigma_e, sigma_g, 1.0)
            if abs(mean_e - mean_g) > DISAGREEMENT_SIGMA_LIMIT * sigma_check:
                return (50.0, round(statistics.mean(members_f), 1),
                        round(sigma_check, 1), len(members_f), age_hours,
                        "ensemble", f"models disagree ({mean_e:.1f} vs {mean_g:.1f}F)")
        except statistics.StatisticsError:
            pass

    bias_F, extra_sigma_F = get_bias(city or "", market_type)
    members_f = [v + bias_F for v in members_f]

    is_same_day = target_d == today_local and hours_to_close <= SAME_DAY_HOURS_THRESHOLD
    mode = "same_day" if is_same_day else "next_day"

    if is_same_day and metar_id:
        hrrr_max, hrrr_min, _, _ = fetch_hrrr_forecast(lat, lon, target_date_str, tz_name=tz_name)
        hrrr_F = hrrr_min if use_min else hrrr_max
        if hrrr_F is not None:
            w = _hrrr_blend_weight(hours_to_close)        # fix #11 / v24.2
            if w is not None:
                current_mean = statistics.mean(members_f)
                target_mean = w * hrrr_F + (1 - w) * current_mean
                shift = target_mean - current_mean
                members_f = [v + shift for v in members_f]

        # Pace adjustment using METAR observations (v24.1: LOW now uses overnight window)
        obs = fetch_metar_temp(metar_id, target_date_local=today_local, tz_name=tz_name)
        if obs:
            if not use_min and obs.get("max_today_F") is not None:
                # HIGH: today's running max is a floor for the daily high
                realized = obs["max_today_F"]
                members_f = [max(v, realized) for v in members_f]
            elif use_min and obs.get("overnight_min_F") is not None:
                # LOW: daily lows settle on the overnight (00:00–07:00 local) window;
                # mid-day mins are not a valid ceiling. Use overnight_min only.
                realized = obs["overnight_min_F"]
                members_f = [min(v, realized) for v in members_f]

    n = len(members_f)
    if n == 0:
        return 50.0, None, None, 0, age_hours, mode, "no members"

    above = sum(1 for v in members_f if v >= threshold_f)
    below = n - above

    forecast_F = round(statistics.mean(members_f), 1)
    sigma_F = round(statistics.pstdev(members_f) if n > 1 else 4.0, 1)
    sigma_F = max(sigma_F, 1.0) + extra_sigma_F

    if min(above, below) < TAIL_MIN_MEMBERS:
        return (50.0, forecast_F, sigma_F, n, age_hours, mode,
                f"tail thin ({above}/{below})")

    # Bracket dead-zone scales with sigma (fix #12)
    bracket = max(BRACKET_BUFFER_F_MIN, BRACKET_BUFFER_SIGMA_FRACTION * sigma_F)
    if abs(forecast_F - threshold_f) < bracket:
        return (50.0, forecast_F, sigma_F, n, age_hours, mode,
                f"bracket dead-zone ({abs(forecast_F-threshold_f):.2f}F<{bracket:.2f})")

    smoothed = (above + 1) / (n + 2)
    prob_pct = round(max(2.0, min(98.0, smoothed * 100)), 1)
    return prob_pct, forecast_F, sigma_F, n, age_hours, mode, None


def precipitation_prob(lat: float, lon: float, threshold_inches: float,
                       market_type: str, date_str: Optional[str],
                       tz_name: str = "America/New_York"):
    if not date_str:
        return 50.0, None, None, 0, None, "precip", "no date"
    m = re.search(r"([A-Z]{3})(\d{2})$", date_str.upper())
    if not m:
        return 50.0, None, None, 0, None, "precip", "bad date"
    mon_str, yr_str = m.groups()
    try:
        contract_year = int(f"20{yr_str}")
        contract_month = MONTH_MAP[mon_str]
    except (KeyError, ValueError):
        return 50.0, None, None, 0, None, "precip", "bad date"

    tz = ZoneInfo(tz_name)
    today = datetime.now(tz).date()
    month_start = date(contract_year, contract_month, 1)

    mtd_total = 0.0
    if today.year == contract_year and today.month == contract_month and today > month_start:
        archive_url = "https://archive-api.open-meteo.com/v1/archive"
        archive_params = {
            "latitude": lat, "longitude": lon,
            "start_date": month_start.isoformat(),
            "end_date": (today - timedelta(days=1)).isoformat(),
            "daily": "snowfall_sum" if market_type == "SNOW" else "precipitation_sum",
            "timezone": tz_name,
            "length_unit": "metric",   # fix #4: lock units
        }
        try:
            r = requests.get(archive_url, params=archive_params, timeout=15)
            r.raise_for_status()
            arr = r.json().get("daily", {}).get(
                "snowfall_sum" if market_type == "SNOW" else "precipitation_sum",
            ) or []
            for v in arr:
                if v is not None:
                    mtd_total += float(v)
        except (requests.RequestException, ValueError) as e:
            log.warning("Archive fetch failed: %s", e)

    last_day = date(contract_year, contract_month,
                    calendar.monthrange(contract_year, contract_month)[1])
    days_remaining = max(1, min(16, (last_day - today).days + 1))

    # v24.5: cache the raw ensemble JSON keyed on (lat,lon,type,year,month,days,tz).
    # threshold_inches doesn't affect the fetch — it's applied after, so multiple
    # markets at the same coords with different thresholds share one fetch.
    precip_cache_key = ("precip", round(float(lat), 4), round(float(lon), 4),
                        market_type, contract_year, contract_month,
                        days_remaining, tz_name)
    now_ts = time.time()
    cached = _ensemble_cache.get(precip_cache_key)
    if cached and (now_ts - cached[0]) < ENSEMBLE_CACHE_TTL_SECONDS:
        data = cached[1]
    else:
        # v24.4: Open-Meteo split /v1/ensemble onto its own subdomain.
        # api.open-meteo.com/v1/ensemble now returns 404; use ensemble-api host.
        url = "https://ensemble-api.open-meteo.com/v1/ensemble"
        params = {
            "latitude": lat, "longitude": lon,
            "hourly": "snowfall" if market_type == "SNOW" else "precipitation",
            "models": ENSEMBLE_MODELS,
            "forecast_days": days_remaining,
            "timezone": tz_name,
            "length_unit": "metric",       # fix #4
        }
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
        except (requests.RequestException, ValueError) as e:
            log.warning("Ensemble precip fetch failed: %s", e)
            return 50.0, None, None, 0, None, "precip", "fetch failed"
        # v24.5: cache successful fetch only
        _ensemble_cache[precip_cache_key] = (now_ts, data)
        _trim_ensemble_cache()

    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        return 50.0, None, None, 0, None, "precip", "no hourly"

    keep_idxs: list[int] = []
    for i, t in enumerate(times):
        try:
            d = date.fromisoformat(t[:10])
        except ValueError:
            continue
        if d >= today and d.year == contract_year and d.month == contract_month:
            keep_idxs.append(i)

    var_prefix = "snowfall" if market_type == "SNOW" else "precipitation"
    member_keys = [k for k in hourly.keys() if k.startswith(var_prefix)]
    if not member_keys or not keep_idxs:
        return 50.0, None, None, 0, None, "precip", "no member data"

    member_totals_in: list[float] = []
    # snowfall in cm, precipitation in mm — already metric per length_unit.
    snow_cm_to_in = 1 / 2.54
    rain_mm_to_in = 1 / 25.4
    conv = snow_cm_to_in if market_type == "SNOW" else rain_mm_to_in
    mtd_in = mtd_total * conv

    for key in member_keys:
        series = hourly[key]
        s = 0.0
        for i in keep_idxs:
            if i < len(series) and series[i] is not None:
                s += float(series[i])
        member_totals_in.append(mtd_in + s * conv)

    n = len(member_totals_in)
    if n == 0:
        return 50.0, None, None, 0, None, "precip", "no members"

    above = sum(1 for v in member_totals_in if v >= threshold_inches)
    smoothed = (above + 1) / (n + 2)
    prob_pct = round(max(2.0, min(98.0, smoothed * 100)), 1)
    mean_total = round(statistics.mean(member_totals_in), 2)
    sigma = round(statistics.pstdev(member_totals_in) if n > 1 else 0.5, 2)
    sigma = max(sigma, 1.5 if market_type == "SNOW" else 0.25)

    now_utc = datetime.now(timezone.utc)
    age_hours = round((now_utc.hour % 6) + (now_utc.minute / 60.0) + 3.0, 1)
    return prob_pct, mean_total, sigma, n, age_hours, "precip", None


# ============================================================================
# PRICING + EXPIRY HELPERS
# ============================================================================
# v24.1: symmetric banker's rounding for both ask and bid. Asymmetric rounding
# (HALF_UP for ask, HALF_DOWN for bid) inflated apparent spreads by ~1c on every
# market quoted at the half-cent grid. Kalshi quotes in 1c increments anyway,
# so most prices already collapse cleanly; ROUND_HALF_EVEN handles edge cases
# without bias.
def _to_cents(x) -> Optional[int]:
    if x is None:
        return None
    try:
        return int((Decimal(str(x)) * 100).to_integral_value(ROUND_HALF_EVEN))
    except (ValueError, ArithmeticError):
        return None


# Backwards-compat shims kept so settlement code stays readable.
_to_cents_ask = _to_cents
_to_cents_bid = _to_cents


def get_best_prices(market: dict) -> tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
    yes_ask = _to_cents(market.get("yes_ask_dollars"))
    yes_bid = _to_cents(market.get("yes_bid_dollars"))
    no_ask = _to_cents(market.get("no_ask_dollars"))
    no_bid = _to_cents(market.get("no_bid_dollars"))
    if yes_ask is None and no_bid is not None:
        yes_ask = 100 - no_bid
    if no_ask is None and yes_bid is not None:
        no_ask = 100 - yes_bid
    return yes_ask, yes_bid, no_ask, no_bid


def is_liquid(market: dict, yes_ask: Optional[int],
              yes_bid: Optional[int],
              from_discovery: bool = False) -> tuple[bool, Optional[str]]:
    """
    Max-spread gate scaled by price. v24.1: from_discovery=True means the
    payload came from /markets bulk discovery, where yes_ask_size_fp is
    typically not populated — skip the size gate to avoid false positives.
    refresh_market_quote will catch thin books on the next per-market poll.
    """
    if yes_bid is not None and yes_ask is not None:
        spread = yes_ask - yes_bid
        if spread < 0:
            return False, f"crossed_spread={spread}"
        mid = (yes_ask + yes_bid) / 2.0 if (yes_ask + yes_bid) > 0 else 50.0
        max_spread = max(MAX_SPREAD_CENTS_BASE, MAX_SPREAD_PRICE_FRACTION * mid)
        if spread > max_spread:
            return False, f"spread={spread}>{max_spread:.1f}"
    if not from_discovery:
        size = market.get("yes_ask_size_fp")
        if size is not None:
            try:
                if float(size) < MIN_LIQUIDITY_SIZE:
                    return False, f"size={size}"
            except (ValueError, TypeError):
                pass
    return True, None


def minutes_to_expiry(market: dict) -> float:
    close_str = market.get("close_time")
    if not close_str:
        return float("inf")
    try:
        close_dt = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
        return (close_dt - datetime.now(timezone.utc)).total_seconds() / 60.0
    except (ValueError, TypeError):
        return float("inf")


# ============================================================================
# MARKET STATE + REGISTRY
# ============================================================================
@dataclass
class MarketState:
    ticker: str
    series_ticker: Optional[str]
    market: dict
    city: Optional[str]
    market_type: Optional[str]
    target_date: Optional[str]
    date_str: Optional[str]
    threshold: Optional[float]
    metar_id: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    tz_name: str = "America/New_York"
    yes_ask: Optional[int] = None
    yes_bid: Optional[int] = None
    no_ask: Optional[int] = None
    no_bid: Optional[int] = None
    model_prob: Optional[float] = None
    forecast_val: Optional[float] = None
    sigma: Optional[float] = None
    n_members: int = 0
    mode: Optional[str] = None
    age_h: Optional[float] = None
    skip_reason: Optional[str] = None
    last_quote_at: float = 0.0
    last_model_at: float = 0.0
    next_refresh_at: float = 0.0
    last_yes_ask: Optional[int] = None
    fractional: bool = False

    def hours_to_close(self) -> float:
        return minutes_to_expiry(self.market) / 60.0

    def event_key(self) -> str:
        """For per-event exposure cap. String form 'city|date' (v24.1)."""
        return f"{self.city or '?'}|{self.target_date or self.date_str or '?'}"


class MarketRegistry:
    def __init__(self):
        self.states: dict[str, MarketState] = {}

    def upsert(self, state: MarketState):
        existing = self.states.get(state.ticker)
        if existing:
            existing.market = state.market
        else:
            self.states[state.ticker] = state

    def due(self, now: float) -> list[MarketState]:
        return [s for s in self.states.values() if s.next_refresh_at <= now]

    def remove(self, ticker: str):
        self.states.pop(ticker, None)


# ============================================================================
# POSITION TRACKER (per-profile attribution + settlement + CLV)
# ============================================================================
@dataclass
class Position:
    profile: str
    ticker: str
    side: str           # "yes" / "no"
    contracts: float
    entry_price_cents: int
    entry_ts: float
    paper: bool
    edge_at_entry: float
    model_prob_at_entry: float
    sigma_at_entry: Optional[float] = None
    event_key: Optional[str] = None
    order_id: Optional[str] = None
    closing_price_cents: Optional[int] = None
    settlement_outcome: Optional[str] = None    # "win" / "loss" / "void"
    pnl_dollars: Optional[float] = None


class PositionTracker:
    POSITIONS_CSV = "positions.csv"
    FIELDNAMES = [
        "ts", "profile", "ticker", "side", "contracts", "entry_price_cents",
        "paper", "edge_at_entry", "model_prob_at_entry", "sigma_at_entry",
        "event_key", "order_id",
        "closing_price_cents", "settlement_outcome", "pnl_dollars",
    ]

    def __init__(self):
        self.positions: list[Position] = []
        self._load_existing()

    def _load_existing(self):
        if not os.path.exists(self.POSITIONS_CSV):
            return
        try:
            with open(self.POSITIONS_CSV, "r", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        self.positions.append(Position(
                            profile=row["profile"],
                            ticker=row["ticker"],
                            side=row["side"],
                            contracts=float(row["contracts"]),
                            entry_price_cents=int(row["entry_price_cents"]),
                            entry_ts=float(row["ts"]),
                            paper=row["paper"].lower() == "true",
                            edge_at_entry=float(row["edge_at_entry"]),
                            model_prob_at_entry=float(row["model_prob_at_entry"]),
                            sigma_at_entry=float(row["sigma_at_entry"])
                                if row.get("sigma_at_entry") else None,
                            event_key=row.get("event_key") or None,
                            order_id=row.get("order_id") or None,
                            closing_price_cents=int(row["closing_price_cents"])
                                if row.get("closing_price_cents") else None,
                            settlement_outcome=row.get("settlement_outcome") or None,
                            pnl_dollars=float(row["pnl_dollars"])
                                if row.get("pnl_dollars") else None,
                        ))
                    except (ValueError, KeyError):
                        continue
        except OSError as e:
            log.warning("Could not load positions.csv: %s", e)

    def record(self, position: Position):
        self.positions.append(position)
        self._append_csv(position)

    def _append_csv(self, p: Position):
        write_header = not os.path.exists(self.POSITIONS_CSV)
        try:
            with open(self.POSITIONS_CSV, "a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=self.FIELDNAMES)
                if write_header:
                    w.writeheader()
                w.writerow(self._row(p))
        except OSError as e:
            log.warning("Could not write positions.csv: %s", e)

    def _row(self, p: Position) -> dict:
        return {
            "ts": p.entry_ts,
            "profile": p.profile,
            "ticker": p.ticker,
            "side": p.side,
            "contracts": p.contracts,
            "entry_price_cents": p.entry_price_cents,
            "paper": p.paper,
            "edge_at_entry": p.edge_at_entry,
            "model_prob_at_entry": p.model_prob_at_entry,
            "sigma_at_entry": p.sigma_at_entry if p.sigma_at_entry is not None else "",
            "event_key": p.event_key or "",
            "order_id": p.order_id or "",
            "closing_price_cents": p.closing_price_cents
                if p.closing_price_cents is not None else "",
            "settlement_outcome": p.settlement_outcome or "",
            "pnl_dollars": p.pnl_dollars if p.pnl_dollars is not None else "",
        }

    def rewrite_all(self):
        """Overwrite the CSV after settlement updates."""
        try:
            with open(self.POSITIONS_CSV, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=self.FIELDNAMES)
                w.writeheader()
                for p in self.positions:
                    w.writerow(self._row(p))
        except OSError as e:
            log.warning("Could not rewrite positions.csv: %s", e)

    def open_for_profile_ticker(self, profile: str, ticker: str) -> list[Position]:
        return [p for p in self.positions
                if p.profile == profile and p.ticker == ticker
                and p.settlement_outcome is None]

    def total_exposure_for_profile(self, profile: str) -> float:
        return sum(
            p.contracts * (p.entry_price_cents / 100.0)
            for p in self.positions
            if p.profile == profile and p.settlement_outcome is None
        )

    def event_exposure_for_profile(self, profile: str, event_key: str) -> float:
        return sum(
            p.contracts * (p.entry_price_cents / 100.0)
            for p in self.positions
            if p.profile == profile and p.settlement_outcome is None
            and p.event_key == event_key
        )

    def count_for_profile(self, profile: str) -> int:
        return sum(1 for p in self.positions if p.profile == profile)

    def open_unsettled(self) -> list[Position]:
        return [p for p in self.positions if p.settlement_outcome is None]


# ============================================================================
# CALIBRATION TRACKER
# ============================================================================
class CalibrationTracker:
    """Tracks predicted vs realized hit rate for graduation gating."""
    CSV = "calibration.csv"

    def __init__(self):
        self.buckets: dict[tuple[str, int], list[int]] = defaultdict(list)
        # key: (profile, decile 0..9) -> list of 0/1 outcomes

    def record(self, profile: str, predicted_prob_pct: float, won: bool):
        decile = max(0, min(9, int(predicted_prob_pct // 10)))
        self.buckets[(profile, decile)].append(1 if won else 0)
        self._append_csv(profile, decile, predicted_prob_pct, won)

    def _append_csv(self, profile, decile, p, won):
        write_header = not os.path.exists(self.CSV)
        try:
            with open(self.CSV, "a", newline="") as f:
                w = csv.writer(f)
                if write_header:
                    w.writerow(["ts", "profile", "decile", "predicted_pct", "won"])
                w.writerow([time.time(), profile, decile, f"{p:.1f}", int(won)])
        except OSError as e:
            log.warning("calibration write failed: %s", e)

    def summary(self) -> str:
        lines = []
        for (prof, dec), outs in sorted(self.buckets.items()):
            if not outs:
                continue
            n = len(outs)
            hit = sum(outs) / n
            mid = dec * 10 + 5
            lines.append(f"{prof}[{mid}%] n={n} hit={hit:.1%}")
        return " | ".join(lines)


# ============================================================================
# STRATEGY PROFILES
# ============================================================================
@dataclass
class StrategyProfile:
    name: str
    edge_threshold_cents: float
    same_day_edge_threshold_cents: float
    kelly_fraction: float
    risk_per_trade_cap: float
    max_contracts_per_trade: int
    promote_near_edge_cents: float
    bankroll: float
    paper_only: bool = False
    min_n_members: int = 10
    max_open_exposure_pct: float = 0.40
    max_event_exposure_pct: float = MAX_EVENT_EXPOSURE_PCT


# Fix #13: AGGRO Kelly capped at 0.30 (was 0.55).
BASE_PROFILE = StrategyProfile(
    name="base",
    edge_threshold_cents=6.0,
    same_day_edge_threshold_cents=4.0,
    kelly_fraction=0.20,
    risk_per_trade_cap=0.04,
    max_contracts_per_trade=75,
    promote_near_edge_cents=3.0,
    bankroll=BANKROLL_BASE_DEFAULT,
    paper_only=False,
)

AGGRO_PROFILE = StrategyProfile(
    name="aggro",
    edge_threshold_cents=3.5,
    same_day_edge_threshold_cents=2.5,
    kelly_fraction=0.30,                    # fix #13
    risk_per_trade_cap=0.10,
    max_contracts_per_trade=200,
    promote_near_edge_cents=6.0,
    bankroll=BANKROLL_AGGRO_DEFAULT,
    paper_only=True,
)

_seen_edges: dict[tuple, float] = {}
_last_edge_prune = time.monotonic()


def _maybe_prune_seen_edges():
    """Fix #17."""
    global _last_edge_prune
    now = time.monotonic()
    if now - _last_edge_prune < SEEN_EDGES_PRUNE_INTERVAL:
        return
    cutoff = time.time() - SEEN_EDGE_TTL_MINUTES * 60
    for k in [k for k, v in _seen_edges.items() if v < cutoff]:
        _seen_edges.pop(k, None)
    _last_edge_prune = now


def profile_kelly_size(profile: StrategyProfile, p_yes: float,
                       price_cents: int) -> int:
    """
    Kelly for binary contract paying $1 if YES.
      b = (100 - price)/price, p = our prob YES, q = 1-p
      f* = (b*p - q) / b
    p_yes is the model's true probability of the side we're buying (0..1).
    """
    if price_cents <= 0 or price_cents >= 100:
        return 0
    p = max(0.01, min(0.99, p_yes))
    q = 1 - p
    b = (100 - price_cents) / price_cents
    full_kelly = max(0.0, (b * p - q) / b)
    f = profile.kelly_fraction * full_kelly
    f = min(f, profile.risk_per_trade_cap)
    stake = profile.bankroll * f
    contracts = int(stake / (price_cents / 100.0))
    contracts = max(1, min(contracts, profile.max_contracts_per_trade))
    return contracts


def evaluate_for_profile(state: MarketState, profile: StrategyProfile,
                         tracker: PositionTracker) -> Optional[dict]:
    """Decide whether to trade. Returns intent dict or None."""
    if state.skip_reason:
        return None
    if state.n_members < profile.min_n_members:
        return None
    if state.model_prob is None or state.yes_ask is None:
        return None
    if state.yes_ask < MIN_PRICE_CENTS or state.yes_ask > MAX_PRICE_CENTS:
        return None

    # Bankroll exposure
    exposure = tracker.total_exposure_for_profile(profile.name)
    if exposure >= profile.bankroll * profile.max_open_exposure_pct:
        log.debug("Profile %s at bankroll exposure cap ($%.2f)",
                  profile.name, exposure)
        return None

    # Per-event exposure (fix #14)
    ek = state.event_key()
    event_exp = tracker.event_exposure_for_profile(profile.name, ek)
    if event_exp >= profile.bankroll * profile.max_event_exposure_pct:
        log.debug("Profile %s at event cap on %s ($%.2f)", profile.name, ek, event_exp)
        return None

    # Mode-specific threshold
    if state.market_type in ("RAIN", "SNOW"):
        edge_threshold = profile.edge_threshold_cents
    elif state.mode == "same_day":
        edge_threshold = profile.same_day_edge_threshold_cents
    else:
        edge_threshold = profile.edge_threshold_cents

    raw_edge = state.model_prob - state.yes_ask

    # Bayesian shrinkage on edge (fix #10)
    n = max(1, state.n_members)
    shrink = n / (n + EDGE_SHRINK_K)
    shrunk_edge = raw_edge * shrink

    if abs(shrunk_edge) < edge_threshold:
        return None

    buy_yes = shrunk_edge > 0
    side = "yes" if buy_yes else "no"

    if buy_yes:
        price_cents = state.yes_ask
        # our prob the side wins:
        p_side = state.model_prob / 100.0
    else:
        if state.no_ask is None:
            return None
        if state.no_ask < MIN_PRICE_CENTS or state.no_ask > MAX_PRICE_CENTS:
            return None
        price_cents = state.no_ask
        p_side = (100 - state.model_prob) / 100.0

    # Apply shrinkage to p_side too (move toward 0.5 by 1-shrink)
    p_side = 0.5 + (p_side - 0.5) * shrink

    # De-dup
    price_bucket = price_cents // SEEN_EDGE_PRICE_BUCKET_CENTS
    edge_key = (profile.name, state.ticker, side, price_bucket)
    if edge_key in _seen_edges:
        if time.time() - _seen_edges[edge_key] < SEEN_EDGE_TTL_MINUTES * 60:
            return None
    _seen_edges[edge_key] = time.time()

    contracts = profile_kelly_size(profile, p_side, price_cents)
    if contracts < 1:
        return None

    # eff_edge for logging only (cents, after shrinkage)
    eff_edge = (p_side * 100) - price_cents

    return {
        "profile": profile.name,
        "ticker": state.ticker,
        "side": side,
        "contracts": contracts,
        "price_cents": price_cents,
        "edge": round(raw_edge, 2),
        "shrunk_edge": round(shrunk_edge, 2),
        "eff_edge": round(eff_edge, 2),
        "model_prob": state.model_prob,
        "p_side_shrunk": round(p_side * 100, 2),
        "sigma": state.sigma,
        "fractional": state.fractional,
        "paper": profile.paper_only,
        "event_key": state.event_key(),
    }


# ============================================================================
# DISCOVERY + REFRESH
# ============================================================================
WEATHER_SERIES_PREFIXES = ("KXHIGH", "KXLOWT", "KXRAIN", "KXSNOW")


def fetch_weather_series(budget: RequestBudget) -> list[str]:
    """Pull weather-category series tickers via paginated /series.
    v24.1: reinstated to avoid pulling all of Kalshi via /markets bulk.
    """
    all_tickers: set[str] = set()
    for category in (None, "weather", "climate"):
        cursor = None
        pages = 0
        while True:
            params = {"limit": 1000}
            if category:
                params["category"] = category
            if cursor:
                params["cursor"] = cursor
            r = api_get(f"{HOST}/series", params=params, budget=budget)
            if not r or not r.ok:
                break
            try:
                resp = r.json()
            except ValueError:
                break
            # v24.3: dict.get("series", []) only defaults when the key is
            # missing — if the API returns {"series": null} the get returns
            # None, and the loop below crashed. Coerce explicitly.
            series_list = resp.get("series") if isinstance(resp, dict) else None
            if not series_list:
                series_list = []
            for s in series_list:
                if not isinstance(s, dict):
                    continue
                t = (s.get("ticker") or "").upper().strip()
                if t.startswith(WEATHER_SERIES_PREFIXES):
                    all_tickers.add(t)
            cursor = resp.get("cursor") if isinstance(resp, dict) else None
            pages += 1
            if not cursor or not series_list or pages > 10:
                break
            time.sleep(0.1)
    return sorted(all_tickers)


def discover_weather_markets(registry: MarketRegistry, budget: RequestBudget):
    """Discover open weather markets via /series → /markets?series_ticker=...
    Far cheaper than pulling all of /markets and filtering, since weather is
    a small fraction of total Kalshi markets.
    """
    series_list = fetch_weather_series(budget)
    log.info("Discovery: %d weather series", len(series_list))
    discovered = 0
    for series in series_list:
        cursor = None
        pages = 0
        while True:
            params = {"status": "open", "series_ticker": series, "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            r = api_get(f"{HOST}/markets", params=params, budget=budget)
            if not r or not r.ok:
                break
            try:
                resp = r.json()
            except ValueError:
                break
            # v24.3: same null-vs-missing guard as fetch_weather_series.
            markets = resp.get("markets") if isinstance(resp, dict) else None
            if not markets:
                markets = []
            for m in markets:
                if not isinstance(m, dict):
                    continue
                ticker = (m.get("ticker") or "").upper()
                city, date_str, _, threshold, mtype, target_date = parse_ticker(ticker)
                if not city or threshold is None:
                    continue
                resolved_city, info = resolve_city(city)
                if not resolved_city:
                    continue
                existing = registry.states.get(ticker)
                if existing:
                    existing.market = m
                    existing.fractional = m.get("fractional_trading_enabled",
                                                existing.fractional)
                else:
                    state = MarketState(
                        ticker=ticker,
                        series_ticker=series,
                        market=m,
                        city=resolved_city,
                        market_type=mtype,
                        target_date=target_date,
                        date_str=date_str,
                        threshold=threshold,
                        metar_id=info["metar"],
                        lat=info["lat"],
                        lon=info["lon"],
                        tz_name=info.get("tz", "America/New_York"),
                        fractional=m.get("fractional_trading_enabled", False),
                    )
                    registry.upsert(state)
                    discovered += 1
            cursor = resp.get("cursor") if isinstance(resp, dict) else None
            pages += 1
            # v24.2: bumped 5 -> 10. Warn-log when we hit the cap so we know
            # to raise it again if a busy series ever truncates.
            if not cursor or not markets:
                break
            if pages >= 10:
                log.warning("Discovery: hit page cap on series %s (>=10 pages)", series)
                break
            time.sleep(0.05)
        time.sleep(0.05)

    log.info("Discovery: %d new markets, %d total tracked",
             discovered, len(registry.states))


def refresh_market_quote(state: MarketState, budget: RequestBudget,
                         urgent: bool = False) -> bool:
    r = api_get(f"{HOST}/markets/{state.ticker}", budget=budget, urgent=urgent)
    if not r or not r.ok:
        return False
    try:
        body = r.json()
        m = body.get("market") if isinstance(body, dict) else None
        if not m and isinstance(body, dict):
            m = body
    except ValueError:
        return False
    if not isinstance(m, dict):
        return False

    state.market = m
    state.last_yes_ask = state.yes_ask
    yes_ask, yes_bid, no_ask, no_bid = get_best_prices(m)
    liquid, reason = is_liquid(m, yes_ask, yes_bid)
    state.yes_ask = yes_ask
    state.yes_bid = yes_bid
    state.no_ask = no_ask
    state.no_bid = no_bid
    state.last_quote_at = time.time()
    state.fractional = m.get("fractional_trading_enabled", state.fractional)
    if not liquid:
        state.skip_reason = f"illiquid: {reason}"
    elif state.skip_reason and state.skip_reason.startswith("illiquid"):
        state.skip_reason = None
    return True


def maybe_refresh_model(state: MarketState, force: bool = False):
    now = time.time()
    hours = state.hours_to_close()
    if state.market_type in ("RAIN", "SNOW"):
        ttl = MODEL_TTL_PRECIP
    elif hours <= SAME_DAY_HOURS_THRESHOLD:
        ttl = MODEL_TTL_SAME_DAY
    else:
        ttl = MODEL_TTL_NEXT_DAY

    if not force and (now - state.last_model_at) < ttl:
        return

    if state.lat is None or state.threshold is None:
        return

    if state.market_type in ("RAIN", "SNOW"):
        prob, fval, sigma, n, age, mode, skip = precipitation_prob(
            state.lat, state.lon, state.threshold, state.market_type,
            state.date_str, tz_name=state.tz_name)
    else:
        prob, fval, sigma, n, age, mode, skip = temperature_prob(
            state.lat, state.lon, state.threshold, state.market_type,
            state.target_date,
            metar_id=state.metar_id,
            hours_to_close=hours,
            city=state.city,
            tz_name=state.tz_name,
        )
    state.model_prob = prob
    state.forecast_val = fval
    state.sigma = sigma
    state.n_members = n
    state.age_h = age
    state.mode = mode
    if skip and not (state.skip_reason and state.skip_reason.startswith("illiquid")):
        state.skip_reason = skip
    elif not skip and state.skip_reason and not state.skip_reason.startswith("illiquid"):
        state.skip_reason = None
    state.last_model_at = now


def is_urgent(state: MarketState) -> bool:
    hours = state.hours_to_close()
    if hours < 2:
        return True
    if (state.mode == "same_day" and state.model_prob is not None
            and state.yes_ask is not None):
        edge = abs(state.model_prob - state.yes_ask)
        if 2.0 <= edge <= 8.0:
            return True
    if state.last_yes_ask is not None and state.yes_ask is not None:
        if abs(state.yes_ask - state.last_yes_ask) >= 3:
            return True
    return False


def compute_next_refresh(state: MarketState,
                         profiles: list[StrategyProfile]) -> int:
    # v24.1: cool down ALL skip reasons (not just illiquid). Skipped markets
    # don't need to be polled at the urgent cadence; once a skip clears the
    # market will refresh on the cold tick and re-enter the normal cycle.
    if state.skip_reason:
        return REFRESH_COLD
    if state.model_prob is None or state.yes_ask is None:
        return REFRESH_NORMAL
    hours = state.hours_to_close()
    if hours < 2:
        return REFRESH_URGENT

    edge = abs(state.model_prob - state.yes_ask)

    # v24.1: prioritize live profiles. Paper-only profiles shouldn't pull a
    # market into the urgent refresh tier when no real money would trade it.
    live_profiles = [p for p in profiles if not p.paper_only]
    refresh_profiles = live_profiles if live_profiles else profiles

    best_promote = max(p.promote_near_edge_cents for p in refresh_profiles)
    if state.mode == "same_day":
        best_threshold = min(p.same_day_edge_threshold_cents for p in refresh_profiles)
    else:
        best_threshold = min(p.edge_threshold_cents for p in refresh_profiles)

    if edge >= best_threshold:
        return REFRESH_URGENT
    if edge >= best_promote:
        return REFRESH_NEAR_EDGE
    if state.mode == "same_day":
        return REFRESH_NEAR_EDGE
    return REFRESH_NORMAL


# ============================================================================
# TELEGRAM (fix #21)
# ============================================================================
class TelegramSender:
    def __init__(self):
        self.last_send = 0.0
        self.queue: deque[str] = deque(maxlen=200)
        self.last_flush = time.monotonic()

    def send(self, msg: str, urgent: bool = False):
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return
        if urgent:
            self._raw_send(msg)
            return
        self.queue.append(msg)
        self._maybe_flush()

    def _maybe_flush(self):
        now = time.monotonic()
        if not self.queue:
            return
        if (now - self.last_flush < TELEGRAM_BATCH_FLUSH_SECONDS
                and len(self.queue) < 5):
            return
        batch = "\n".join(self.queue)
        self.queue.clear()
        self.last_flush = now
        self._raw_send(batch)

    def _raw_send(self, msg: str):
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return
        elapsed = time.monotonic() - self.last_send
        if elapsed < TELEGRAM_MIN_INTERVAL_SECONDS:
            time.sleep(TELEGRAM_MIN_INTERVAL_SECONDS - elapsed)
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": msg[:4000]},
                timeout=10,
            )
            self.last_send = time.monotonic()
        except requests.RequestException as e:
            log.debug("telegram send failed: %s", e)

    def force_flush(self):
        self._maybe_flush()


telegram = TelegramSender()


# ============================================================================
# SCANS LOG
# ============================================================================
def log_intent(intent: dict, state: MarketState):
    write_header = not os.path.exists("scans.csv")
    fieldnames = [
        "ts", "profile", "ticker", "city", "market_type", "mode",
        "target_date", "threshold", "yes_ask", "no_ask", "model_prob",
        "p_side_shrunk", "forecast_val", "sigma", "n_members", "age_h",
        "edge", "shrunk_edge", "eff_edge", "side", "contracts",
        "price_cents", "paper", "event_key",
    ]
    try:
        with open("scans.csv", "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                w.writeheader()
            w.writerow({
                "ts": time.time(),
                "profile": intent["profile"],
                "ticker": intent["ticker"],
                "city": state.city,
                "market_type": state.market_type,
                "mode": state.mode,
                "target_date": state.target_date or state.date_str,
                "threshold": state.threshold,
                "yes_ask": state.yes_ask,
                "no_ask": state.no_ask,
                "model_prob": state.model_prob,
                "p_side_shrunk": intent.get("p_side_shrunk"),
                "forecast_val": state.forecast_val,
                "sigma": state.sigma,
                "n_members": state.n_members,
                "age_h": state.age_h,
                "edge": intent["edge"],
                "shrunk_edge": intent.get("shrunk_edge"),
                "eff_edge": intent["eff_edge"],
                "side": intent["side"],
                "contracts": intent["contracts"],
                "price_cents": intent["price_cents"],
                "paper": intent["paper"],
                "event_key": intent["event_key"],
            })
    except OSError as e:
        log.warning("Could not write scans.csv: %s", e)


# ============================================================================
# ORDER EXECUTION (v24.1: deterministic client_order_id, no in-memory cache)
# ============================================================================
def _intent_idempotency_id(intent: dict) -> str:
    """
    Deterministic client_order_id derived purely from intent + a coarse time
    bucket. Two retries of the same intent within the same 30s wall-clock
    bucket produce the same id, so Kalshi will reject the duplicate. After
    the bucket rolls over the id changes naturally, so a fresh re-decision
    on the same market is treated as a new order.

    No in-memory cache is needed — this also survives process restarts.
    """
    raw = f"{intent['profile']}|{intent['ticker']}|{intent['side']}|{intent['contracts']}|{intent['price_cents']}"
    h = hashlib.sha1(raw.encode()).hexdigest()[:16]
    bucket = int(time.time() // ORDER_IDEMPOTENCY_TTL_SECONDS)
    return f"pcv1-{h}-{bucket}"


def _poll_order_fill(order_id: str, expected_contracts: float,
                     timeout: float = ORDER_FILL_TIMEOUT_SECONDS) -> tuple[bool, float, Optional[int]]:
    """
    Poll Kalshi for fill status. Returns (filled, filled_contracts, avg_price_cents).
    Falls back to "filled at limit" if endpoint unavailable.
    """
    deadline = time.monotonic() + timeout
    last_err = None
    while time.monotonic() < deadline:
        try:
            o = None
            if hasattr(client, "get_order"):
                o = client.get_order(order_id)
            elif hasattr(client, "get_order_by_id"):
                o = client.get_order_by_id(order_id)
            if o is not None:
                # Common fields across SDK versions
                status = getattr(o, "status", None) or (
                    o.get("status") if isinstance(o, dict) else None)
                taken = getattr(o, "taken_count", None)
                if taken is None and isinstance(o, dict):
                    taken = o.get("taken_count") or o.get("filled_count") or 0
                avg_price = getattr(o, "avg_fill_price_cents", None)
                if avg_price is None and isinstance(o, dict):
                    avg_price = o.get("avg_fill_price_cents")
                taken = float(taken or 0)
                if status in ("executed", "filled") or taken >= expected_contracts:
                    return True, taken, int(avg_price) if avg_price else None
                if status in ("canceled", "cancelled", "rejected", "expired"):
                    return False, taken, int(avg_price) if avg_price else None
        except Exception as e:
            last_err = e
        time.sleep(ORDER_FILL_POLL_SECONDS)
    if last_err:
        log.debug("order poll error: %s", last_err)
    return False, 0.0, None


def place_order(intent: dict, state: MarketState, budget: RequestBudget,
                tracker: PositionTracker) -> bool:
    """Submit / paper-record. Live positions only recorded after fill (fix #7)."""
    log_intent(intent, state)

    is_paper = intent["paper"] or DEMO_MODE
    base_position = Position(
        profile=intent["profile"],
        ticker=intent["ticker"],
        side=intent["side"],
        contracts=float(intent["contracts"]),
        entry_price_cents=intent["price_cents"],
        entry_ts=time.time(),
        paper=is_paper,
        edge_at_entry=intent["edge"],
        model_prob_at_entry=intent["model_prob"],
        sigma_at_entry=intent.get("sigma"),
        event_key=intent.get("event_key"),
    )

    if is_paper:
        tag = "PAPER" if intent["paper"] else "DEMO"
        log.info("[%s] %s %s %s %.0f @ %d¢ shrunk_edge=%+.1fc",
                 tag, intent["profile"], intent["ticker"],
                 intent["side"].upper(), intent["contracts"],
                 intent["price_cents"], intent.get("shrunk_edge", 0.0))
        tracker.record(base_position)
        return True

    # ---- LIVE ----
    if not budget.try_consume(REQUEST_COST_WRITE, write=True):
        log.warning("Write budget tight, skipping order on %s", intent["ticker"])
        return False

    coid = _intent_idempotency_id(intent)
    order_data = {
        "ticker": intent["ticker"],
        "action": "buy",
        "side": intent["side"],
        "client_order_id": coid,
    }
    if intent["fractional"]:
        order_data["count_fp"] = f"{float(intent['contracts']):.2f}"
    else:
        order_data["count"] = int(intent["contracts"])
    price_dollars = f"{intent['price_cents'] / 100:.4f}"
    if intent["side"] == "yes":
        order_data["yes_price_dollars"] = price_dollars
    else:
        order_data["no_price_dollars"] = price_dollars

    try:
        resp = client.create_order(order_data)
    except requests.RequestException as e:
        log.error("Network error creating order on %s: %s", intent["ticker"], e)
        telegram.send(f"Order NETWORK FAIL: {intent['ticker']} - {e}", urgent=True)
        return False
    except Exception as e:
        log.error("Order rejected for %s: %s", intent["ticker"], e)
        telegram.send(f"Order REJECTED: {intent['ticker']} - {e}", urgent=True)
        return False

    order_id = None
    if isinstance(resp, dict):
        order_id = resp.get("order_id") or (resp.get("order") or {}).get("order_id")
    else:
        order_id = getattr(resp, "order_id", None) or getattr(
            getattr(resp, "order", None), "order_id", None)

    if not order_id:
        log.warning("No order_id returned for %s — recording at limit price",
                    intent["ticker"])
        base_position.order_id = coid
        tracker.record(base_position)
        return True

    base_position.order_id = order_id
    filled, taken, avg_price = _poll_order_fill(order_id, float(intent["contracts"]))
    if filled and taken > 0:
        if avg_price:
            base_position.entry_price_cents = avg_price
        base_position.contracts = taken
        tracker.record(base_position)
        msg = (f"FILL [{intent['profile']}] {intent['ticker']} "
               f"{intent['side'].upper()} {taken:.0f} @ "
               f"{base_position.entry_price_cents}¢ "
               f"edge={intent['edge']}c shrunk={intent.get('shrunk_edge')}c")
        log.info(msg)
        telegram.send(msg)
        return True

    if taken > 0:
        # partial fill — record actual filled amount
        base_position.contracts = taken
        if avg_price:
            base_position.entry_price_cents = avg_price
        tracker.record(base_position)
        msg = (f"PARTIAL [{intent['profile']}] {intent['ticker']} "
               f"{intent['side'].upper()} {taken:.0f}/{intent['contracts']} @ "
               f"{base_position.entry_price_cents}¢")
        log.info(msg)
        telegram.send(msg)
        return True

    log.warning("Order %s on %s did not fill within %ds — not booked",
                order_id, intent["ticker"], int(ORDER_FILL_TIMEOUT_SECONDS))
    return False


# ============================================================================
# SETTLEMENT RECONCILIATION
#   v24.1: bulk /portfolio/settlements endpoint instead of per-ticker /markets.
#   v24.2: 3-day since_ts watermark; close-price proxy from revenue/contracts.
# ============================================================================
# v24.2: settlement reconcile lookback. Anything older than this should
# already be reconciled — re-walking deeper history every pass is wasteful.
SETTLEMENT_LOOKBACK_SECONDS = 3 * 86400


def _fetch_portfolio_settlements(budget: RequestBudget,
                                 since_ts: Optional[int] = None) -> dict[str, dict]:
    """
    Pull the user's settlement history from /portfolio/settlements (paginated
    cursor) and return a dict keyed by ticker. Far cheaper than per-ticker
    /markets/{ticker} polling and uses the canonical Kalshi field names.
    """
    out: dict[str, dict] = {}
    cursor: Optional[str] = None
    pages = 0
    while True:
        params: dict = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
        if since_ts:
            params["min_ts"] = since_ts
        r = api_get(f"{HOST}/portfolio/settlements", params=params,
                    budget=budget, cost=REQUEST_COST_READ)
        if not r or not r.ok:
            break
        try:
            body = r.json()
        except ValueError:
            break
        rows = body.get("settlements") if isinstance(body, dict) else None
        if not rows:
            break
        for row in rows:
            t = row.get("ticker") or row.get("market_ticker")
            if t:
                out[t] = row
        cursor = body.get("cursor") if isinstance(body, dict) else None
        pages += 1
        if not cursor or pages >= 25:
            break
    return out


def _settlement_close_cents(s: dict, p: "Position") -> Optional[int]:
    """
    v24.2: derive a closing-price-cents proxy from the settlement row itself.

    /portfolio/settlements rows expose `revenue` (cents received) and `value`
    (cents per share at settlement) — they do NOT carry `last_price_dollars`
    (that field lives on /markets/{ticker}). Compute realized per-share value:

      realized_per_share_cents = revenue / max(contracts, 1)

    For a winning side this is ~100; for a losing side ~0. As a CLV proxy
    against entry it's an honest measurement of what the market actually
    paid out, which is what we want for calibration / PnL audit.
    """
    # Try canonical fields first.
    revenue = s.get("revenue")
    if revenue is not None:
        try:
            contracts = max(int(p.contracts), 1)
            return int(round(float(revenue) / contracts))
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    # Some Kalshi responses use `value` (cents/share) directly.
    value = s.get("value")
    if value is not None:
        try:
            return int(round(float(value)))
        except (TypeError, ValueError):
            pass
    return None


def reconcile_settlements(tracker: PositionTracker,
                          calibration: CalibrationTracker,
                          budget: RequestBudget):
    """
    For each open position whose market has expired, fetch the settlement row
    from /portfolio/settlements and fill in closing_price_cents,
    settlement_outcome, pnl_dollars. Feeds the calibration tracker.
    """
    open_positions = tracker.open_unsettled()
    if not open_positions:
        return

    # v24.2: bound history walk. Anything older than 3 days should already
    # be reconciled, and if it isn't, an extra hour of lag won't hurt.
    since_ts = int(time.time()) - SETTLEMENT_LOOKBACK_SECONDS
    settlements = _fetch_portfolio_settlements(budget, since_ts=since_ts)
    if not settlements:
        return

    by_ticker: dict[str, list[Position]] = defaultdict(list)
    for p in open_positions:
        by_ticker[p.ticker].append(p)

    updated = 0
    for ticker, positions in by_ticker.items():
        s = settlements.get(ticker)
        if not s:
            continue

        # Kalshi uses `market_result` on settlement rows: "yes" / "no" / ""
        result = (s.get("market_result") or s.get("result") or "").lower()

        if not result:
            # row exists but no decisive result yet — capture CLV only
            for p in positions:
                close_cents = _settlement_close_cents(s, p)
                if close_cents is not None and p.closing_price_cents is None:
                    p.closing_price_cents = close_cents
            continue

        for p in positions:
            won = (result == p.side)
            p.settlement_outcome = "win" if won else "loss"
            payoff_per_contract = (100 - p.entry_price_cents) if won else -p.entry_price_cents
            p.pnl_dollars = round(p.contracts * payoff_per_contract / 100.0, 2)
            close_cents = _settlement_close_cents(s, p)
            if close_cents is not None and p.closing_price_cents is None:
                p.closing_price_cents = close_cents
            # calibration: record predicted prob of side bought
            predicted = (p.model_prob_at_entry if p.side == "yes"
                         else 100 - p.model_prob_at_entry)
            calibration.record(p.profile, predicted, won)
            updated += 1

    if updated:
        tracker.rewrite_all()
        log.info("Settlement reconcile: updated %d positions", updated)


# ============================================================================
# MAIN LOOP
# ============================================================================
def run():
    registry = MarketRegistry()
    budget = load_account_budget()
    tracker = PositionTracker()
    calibration = CalibrationTracker()
    profiles = [BASE_PROFILE, AGGRO_PROFILE]

    # Sync live bankroll for live profile
    try:
        bal = client.get_balance()
        balance = getattr(bal, "balance", None)
        if balance is None and isinstance(bal, dict):
            balance = bal.get("balance")
        if balance is not None:
            BASE_PROFILE.bankroll = float(balance) / 100.0
            log.info("Live bankroll: $%.2f -> BASE", BASE_PROFILE.bankroll)
    except requests.RequestException as e:
        log.warning("Balance fetch network error: %s", e)
    except Exception as e:
        log.warning("Balance fetch failed: %s", e)

    next_discovery_at = 0.0
    next_settlement_at = 0.0
    last_heartbeat = 0.0
    intents_since_heartbeat = 0

    paper_profiles = [p.name for p in profiles if p.paper_only]
    live_profiles = [p.name for p in profiles if not p.paper_only]
    log.info("PCV1/V24 starting | demo=%s live=%s paper=%s",
             DEMO_MODE, live_profiles, paper_profiles)

    while True:
        try:
            now = time.time()

            if now >= next_discovery_at:
                discover_weather_markets(registry, budget)
                next_discovery_at = now + DISCOVERY_INTERVAL_SECONDS

            if now >= next_settlement_at:
                reconcile_settlements(tracker, calibration, budget)
                next_settlement_at = now + SETTLEMENT_RECONCILE_INTERVAL

            _maybe_prune_seen_edges()

            due = registry.due(now)
            due.sort(key=lambda s: (not is_urgent(s), s.next_refresh_at))

            processed = 0
            for state in due:
                hours = state.hours_to_close()
                if hours * 60 < MIN_MINS_TO_EXPIRY:
                    state.next_refresh_at = now + 3600
                    continue

                urgent = is_urgent(state)
                ok = refresh_market_quote(state, budget, urgent=urgent)
                if not ok:
                    state.next_refresh_at = now + (REFRESH_URGENT if urgent else REFRESH_NORMAL)
                    continue

                maybe_refresh_model(state)
                state.next_refresh_at = now + compute_next_refresh(state, profiles)

                for profile in profiles:
                    intent = evaluate_for_profile(state, profile, tracker)
                    if intent:
                        intents_since_heartbeat += 1
                        log.info(
                            "EDGE[%s%s] %s %s %d @ %dc model=%.1f%% raw=%+.1fc shrunk=%+.1fc mode=%s n=%d",
                            profile.name,
                            "/paper" if profile.paper_only else "",
                            state.ticker,
                            intent["side"].upper(),
                            intent["contracts"],
                            intent["price_cents"],
                            state.model_prob,
                            intent["edge"],
                            intent["shrunk_edge"],
                            state.mode,
                            state.n_members,
                        )
                        place_order(intent, state, budget, tracker)

                processed += 1
                if processed >= 50:
                    break

            # v24.1: Telegram flush moved into heartbeat block. Calling
            # force_flush every loop tick made the batcher useless — every
            # message went out as its own request and we were paying the
            # 1-msg/sec floor on every quote pass.
            if now - last_heartbeat > HEARTBEAT_INTERVAL_SECONDS:
                telegram.force_flush()
                base_pos = tracker.count_for_profile("base")
                aggro_pos = tracker.count_for_profile("aggro")
                base_exp = tracker.total_exposure_for_profile("base")
                aggro_exp = tracker.total_exposure_for_profile("aggro")
                ages = [now - s.last_model_at for s in registry.states.values()
                        if s.last_model_at > 0]
                oldest_age = max(ages) if ages else 0
                log.info(
                    "HEARTBEAT tracked=%d due=%d budget=%.0f/s avail=%.0f 429s=%d "
                    "intents=%d oldest_model=%.0fs | "
                    "BASE pos=%d exp=$%.2f | AGGRO[paper] pos=%d exp=$%.2f | %s",
                    len(registry.states), len(due),
                    budget.tokens_per_sec, budget.available, budget.consumed_429,
                    intents_since_heartbeat, oldest_age,
                    base_pos, base_exp, aggro_pos, aggro_exp,
                    calibration.summary() or "calib=empty",
                )
                last_heartbeat = now
                intents_since_heartbeat = 0

            time.sleep(0.5)

        except KeyboardInterrupt:
            log.info("Shutdown requested.")
            telegram.force_flush()
            break
        except Exception as e:
            log.exception("Top-level error: %s", e)
            time.sleep(10)


if __name__ == "__main__":
    run()
