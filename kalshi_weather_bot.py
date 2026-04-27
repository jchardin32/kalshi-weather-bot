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

# ================== CONFIG ==================
BANKROLL = 1000                  # Starting value — will be updated from Kalshi API
RISK_PER_TRADE = 0.02
EDGE_THRESHOLD = 3               # Now 3¢ for all markets
DEMO_MODE = False                # LIVE TRADING ENABLED
SCAN_INTERVAL = 180              # 3 minutes
MIN_MINS_TO_EXPIRY = 15
# ============================================

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

# Fetch real bankroll at startup
try:
    balance_resp = client.get_balance()
    BANKROLL = balance_resp.available_balance / 100.0
    log.info("Live bankroll loaded: $%.2f", BANKROLL)
except:
    log.warning("Could not fetch live balance — using $1000 fallback")

CITY_COORDS = { ... }  # (your 13 cities — unchanged)

_forecast_cache = {}
_seen_edges = {}

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=10)
    except:
        pass

# (parse_ticker, minutes_to_expiry, _normal_cdf, get_model_prob, cached_model_prob, log_to_csv remain the same as last version)

def place_order(ticker, side, contracts, price_cents):
    order_data = {
        "ticker": ticker,
        "action": "buy" if side == "yes" else "sell",
        "side": "yes",
        "count": contracts,
        "type": "limit",
        "yes_price" if side == "yes" else "no_price": price_cents,
        "client_order_id": str(uuid.uuid4())
    }
    try:
        result = client.create_order(order_data)
        global BANKROLL
        cost = contracts * (price_cents / 100.0)
        BANKROLL -= cost
        msg = f"✅ TRADE PLACED\n{ticker}\n{side.upper()} {contracts} @ {price_cents}¢\nNew balance: ${BANKROLL:.2f}"
        send_telegram(msg)
        log.info(msg)
        return True
    except Exception as e:
        log.error("Order failed: %s", e)
        send_telegram(f"❌ Order FAILED: {ticker} - {e}")
        return False

print("🚀 LIVE Kalshi weather bot — scanning every 3 min, 3¢ edge, real money")

while True:
    try:
        # (market fetch + loop logic from previous version with the new EDGE_THRESHOLD)
        # ... (same as last version but using EDGE_THRESHOLD = 3 for all markets)

        if abs(edge) >= EDGE_THRESHOLD:
            # ... calculate contracts, place_order, etc.
            direction = "BUY YES" if edge > 0 else "SELL YES (buy NO)"
            side = "yes" if edge > 0 else "no"
            price_cents = int(yes_price + 1) if edge > 0 else int(yes_price - 1)
            contracts = calc_contracts(edge, int(yes_price), side)
            place_order(ticker, side, contracts, price_cents)

        time.sleep(SCAN_INTERVAL)

    except Exception as e:
        log.exception("Error: %s", e)
        time.sleep(60)
