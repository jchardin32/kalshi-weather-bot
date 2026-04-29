"""
PCV1 aka V24.2 — Kalshi Weather Bot
====================================
Incorporates v24 + v24.1 + v24.2 review fixes.

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

_T_PREF
