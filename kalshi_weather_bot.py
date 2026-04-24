import os
import time
import csv
import re
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from kalshi_python_sync import Configuration, KalshiClient

load_dotenv()

BANKROLL = 1000
RISK_PER_TRADE = 0.02
EDGE_THRESHOLD = 4
DEMO_MODE = True
SCAN_INTERVAL = 600
MIN_MINS_TO_EXPIRY = 15

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

CITY_COORDS = {
    "NYC": (40.7128, -74.0060), "CHI": (41.8781, -87.6298),
    "MIA": (25.7617, -80.1918), "LAX": (34.0522, -118.2437),
    "AUS": (30.2672, -97.7431), "DEN": (39.7392, -104.9903),
    "BOS": (42.3601, -71.0589), "SEA": (47.6062, -122.3321),
    "PHL": (39.9526, -75.1652), "ATL": (33.7490, -84.3880),
    "DFW": (32.7767, -96.7970), "HOU": (29.7604, -95.3698),
    "PHX": (33.4484, -112.0740),
}

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=10)
    except:
        pass

def parse_ticker(ticker):
    match = re.search(r'KX(TEMP|HIGH|LOWT)([A-Z]+)-(\d{2}[A-Z]{3}\d{2})(\d{2})?-?T?(\d+\.\d+)?', ticker.upper())
    if match:
        mtype = match.group(1)
        city = match.group(2)
        date_str = match.group(3)
        hour = int(match.group(4)) if match.group(4) else None
        threshold = float(match.group(5)) if match.group(5) else None
        return city, date_str, hour, threshold, mtype
    return None, None, None, None, None

def get_model_prob(lat, lon, target_hour, threshold_f):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {"latitude": lat, "longitude": lon, "hourly": "temperature_2m", "models": "gfs_seamless", "forecast_days": 2, "timezone": "America/New_York"}
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json()
        temps = data.get("hourly", {}).get("temperature_2m", [])
        if target_hour is not None and target_hour < len(temps):
            forecast = temps[target_hour]
            forecast_f = forecast * 9 / 5 + 32
            deviation = forecast_f - threshold_f
            prob = max(5, min(95, int(50 + deviation * 9.5)))
            return prob, round(forecast_f, 1)
    except:
        pass
    return 50, None

print("🚀 Kalshi weather bot (stable working version) started — 13 cities")

while True:
    try:
        resp = requests.get(f"{HOST}/markets", params={"status": "open", "limit": 200}, timeout=15)
        markets = resp.json().get("markets", []) if resp.ok else []

        log.info("Scanning %d markets...", len(markets))

        for m in markets:
            ticker = m.get("ticker", "")
            if not ticker.startswith("KX"):
                continue

            city_code, date_str, hour, threshold, market_type = parse_ticker(ticker)
            if not city_code or city_code not in CITY_COORDS or threshold is None:
                continue

            yes_price = m.get("yes_price") or 50
            lat, lon = CITY_COORDS[city_code]
            model_prob, forecast = get_model_prob(lat, lon, hour, threshold)
            edge = model_prob - yes_price

            with open("weather_scans.csv", "a", newline="") as f:
                writer = csv.writer(f)
                if f.tell() == 0:
                    writer.writerow(["timestamp", "ticker", "city", "threshold_f", "kalshi_yes", "model_prob", "forecast_f", "edge_cents"])
                writer.writerow([datetime.now().isoformat(), ticker, city_code, threshold, yes_price, model_prob, forecast, edge])

            if abs(edge) >= EDGE_THRESHOLD:
                direction = "BUY YES" if edge > 0 else "SELL YES (buy NO)"
                msg = f"🔥 EDGE FOUND\n{ticker} ({market_type})\n{threshold}°F | Kalshi {yes_price}¢ | Model {model_prob}% (~{forecast}°F)\nEdge {edge:+.1f}¢ → {direction}"
                send_telegram(msg)
                log.info(msg)

        log.info("Cycle complete — sleeping 10 min")
        time.sleep(SCAN_INTERVAL)

    except Exception as e:
        log.exception("Error: %s", e)
        time.sleep(60)
