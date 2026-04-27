import os
import time
import csv
import re
import logging
import requests
import uuid
from datetime import datetime, timezone, timedelta
from math import erf, sqrt
from dotenv import load_dotenv
from kalshi_python_sync import Configuration, KalshiClient

load_dotenv()

BANKROLL = 1000
RISK_PER_TRADE = 0.02
EDGE_THRESHOLD = 2.0
DEMO_MODE = True
SCAN_INTERVAL = 180
MIN_MINS_TO_EXPIRY = 15
SEEN_EDGE_TTL_MINUTES = 60
DYNAMIC_CITIES = True

API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")
PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH")
HOST = os.getenv("KALSHI_HOST")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.FileHandler("kalshi_bot.log"), logging.StreamHandler()])
log = logging.getLogger(__name__)

with open(PRIVATE_KEY_PATH, "r") as f:
    private_key_pem = f.read()

config = Configuration(host=HOST)
config.api_key_id = API_KEY_ID
config.private_key_pem = private_key_pem
client = KalshiClient(config)

MASTER_CITY_COORDS = { ... }   # (same list as before - keep it)

_forecast_cache = {}
_seen_edges = {}

# ... (send_telegram, parse_ticker, minutes_to_expiry, _normal_cdf, get_model_prob, cached_model_prob, log_to_csv, get_current_bankroll, place_order, fetch_weather_markets functions stay the same as last version)

print("🚀 DEBUG v4 - logging right after parse - April 27 2026")

while True:
    try:
        BANKROLL = get_current_bankroll()
        markets = fetch_weather_markets()

        active_coords = {}
        if DYNAMIC_CITIES:
            seen = set()
            for m in markets:
                ticker = m.get("ticker", "")
                city_code, _, _, _, _ = parse_ticker(ticker)
                if city_code and city_code in MASTER_CITY_COORDS and city_code not in seen:
                    seen.add(city_code)
                    active_coords[city_code] = MASTER_CITY_COORDS[city_code]
            log.info("Dynamic mode: using %d active cities", len(active_coords))
        else:
            active_coords = MASTER_CITY_COORDS.copy()

        cycle_key = datetime.now().strftime("%Y-%m-%d-%H-%M")[:13] + "0"
        _forecast_cache.clear()

        now = datetime.now()
        expired = [k for k, v in _seen_edges.items() if (now - v).total_seconds() >= SEEN_EDGE_TTL_MINUTES * 60]
        for k in expired:
            del _seen_edges[k]

        edges_found = 0
        weather_count = 0
        for m in markets:
            try:
                ticker = m.get("ticker", "")

                city_code, date_str, hour, threshold, market_type = parse_ticker(ticker)
                if not city_code or city_code not in active_coords or threshold is None:
                    continue

                weather_count += 1

                # DEBUG - this should now appear for every parsed market
                log.info("PARSED OK: %s | city=%s | threshold=%.1f", ticker, city_code, threshold)

                mins_left = minutes_to_expiry(m)
                if mins_left < MIN_MINS_TO_EXPIRY:
                    continue

                yes_price = m.get("yes_price")
                if yes_price is None:
                    continue

                lat, lon = active_coords[city_code]
                model_prob, forecast_f, sigma_f = cached_model_prob(lat, lon, hour, threshold, cycle_key)

                edge = model_prob - yes_price

                log.info("PROCESSED: %s | model=%.1f%% | forecast=%.1f°F | kalshi=%.1f¢ | edge=%.1f¢", 
                         ticker, model_prob, forecast_f or 0, yes_price, edge)

                # ... rest of the loop (log_to_csv, if edge >= threshold, etc.) stays the same

            except Exception as e:
                log.warning("Error processing %s: %s", ticker, e)
                continue

        log.info("Cycle complete — %d weather markets scanned, %d edges found. Sleeping %ds...", weather_count, edges_found, SCAN_INTERVAL)
        time.sleep(SCAN_INTERVAL)

    except Exception as e:
        log.exception("Top-level error: %s", e)
        time.sleep(60)
