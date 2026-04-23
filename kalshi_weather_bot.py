import os
import time
import csv
import re
import logging
import requests
from datetime import datetime, timezone, timedelta
from math import erf, sqrt
from dotenv import load_dotenv
from kalshi_python_sync import Configuration, KalshiClient

load_dotenv()

# ================== CONFIG ==================
BANKROLL          = 1000
RISK_PER_TRADE    = 0.02
EDGE_THRESHOLD    = 4
DEMO_MODE         = True
SCAN_INTERVAL     = 600
MIN_MINS_TO_EXPIRY = 15
OPEN_METEO_DELAY  = 0.1
MAX_CITY_EXPOSURE = 3          # max open contracts per city
MAX_TOTAL_EXPOSURE = 8         # max open contracts overall
# ============================================

API_KEY_ID        = os.getenv("KALSHI_API_KEY_ID")
PRIVATE_KEY_PATH  = os.getenv("KALSHI_PRIVATE_KEY_PATH")
HOST              = os.getenv("KALSHI_HOST")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.FileHandler("kalshi_bot.log"), logging.StreamHandler()])
log = logging.getLogger(__name__)

with open(PRIVATE_KEY_PATH, "r") as f:
    private_key_pem = f.read()

config = Configuration(host=HOST)
config.api_key_id = API_KEY_ID
config.private_key_pem = private_key_pem
client = KalshiClient(config)

CITY_COORDS = { ... }  # (your 13 cities from before — unchanged)

CSV_PATH = "weather_scans.csv"
# (CSV_HEADERS from your version)

_forecast_cache = {}
_seen_edges = {}
_open_positions = {}  # city -> count of open contracts

def send_telegram(message):
    # (your existing function)

def parse_ticker(ticker):
    # (your existing function)

def minutes_to_expiry(market):
    # (your existing function)

def _normal_cdf(x):
    # (your existing function)

def get_model_prob(...):
    # (your existing improved normal CDF version)

def cached_model_prob(...):
    # (your existing cache version)

def log_to_csv(row):
    # (your existing function)

def calc_contracts(edge, yes_price, side):
    # (your Kelly-inspired version)

def place_order(...):
    if DEMO_MODE:
        log.info("[DEMO] Would place ...")
        return
    # live stub remains for now

# NEW: P&L / resolution tracker
def track_resolved_markets():
    try:
        resp = requests.get(f"{HOST}/markets", params={"status": "closed", "limit": 50}, timeout=10)
        for m in resp.json().get("markets", []):
            if m.get("ticker") in _seen_edges:  # only care about ones we traded
                expected = _seen_edges[m.get("ticker")]["model_prob"] / 100
                actual = 1 if m.get("yes_outcome") == "yes" else 0
                log.info("RESOLVED %s | Expected %.1f%% | Actual %s", m.get("ticker"), expected*100, "YES" if actual else "NO")
                # You can add more calibration logging here over time
    except Exception as e:
        log.warning("Resolution poll failed: %s", e)

# NEW: Exposure cap check
def can_open_position(city: str, contracts: int) -> bool:
    current_city = _open_positions.get(city, 0)
    current_total = sum(_open_positions.values())
    if current_city + contracts > MAX_CITY_EXPOSURE or current_total + contracts > MAX_TOTAL_EXPOSURE:
        log.warning("Exposure cap hit for %s — skipping", city)
        return False
    return True

# ================== MAIN LOOP ==================
log.info("🚀 Kalshi weather bot (improved with P&L tracking + exposure caps) started")

while True:
    try:
        # (your existing market fetch + loop logic, with the new checks inserted)

        # After deciding to trade:
        if net_edge >= EDGE_THRESHOLD and can_open_position(city_code, contracts):
            # ... send telegram, place_order, update _open_positions[city_code] += contracts

        # Every cycle, check for resolved markets
        if datetime.now().minute % 10 == 0:  # every 10 min
            track_resolved_markets()

        time.sleep(SCAN_INTERVAL)

    except Exception as e:
        log.exception("Top-level error: %s", e)
        time.sleep(60)
