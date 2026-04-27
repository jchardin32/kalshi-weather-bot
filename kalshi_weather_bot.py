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
BANKROLL = 1000
RISK_PER_TRADE = 0.02
EDGE_THRESHOLD = 2.0
DEMO_MODE = True
SCAN_INTERVAL = 180
MIN_MINS_TO_EXPIRY = 15
SEEN_EDGE_TTL_MINUTES = 60
DYNAMIC_CITIES = True
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

MASTER_CITY_COORDS = {
    "NYC": (40.7128, -74.0060), "CHI": (41.8781, -87.6298),
    "MIA": (25.7617, -80.1918), "LAX": (34.0522, -118.2437),
    "AUS": (30.2672, -97.7431), "DEN": (39.7392, -104.9903),
    "BOS": (42.3601, -71.0589), "SEA": (47.6062, -122.3321),
    "PHL": (39.9526, -75.1652), "ATL": (33.7490, -84.3880),
    "DFW": (32.7767, -96.7970), "HOU": (29.7604, -95.3698),
    "PHX": (33.4484, -112.0740),
    "STL": (38.6270, -90.1994), "IND": (39.7684, -86.1581),
    "MEM": (35.1495, -90.0490), "OKC": (35.4676, -97.5164),
    "MSP": (44.9778, -93.2650), "KC": (39.0997, -94.5786),
    "CLE": (41.4993, -81.6944), "PIT": (40.4406, -79.9959),
    "CIN": (39.1031, -84.5120), "NAS": (36.1627, -86.7816),
    "LOU": (38.2527, -85.7585), "TPA": (27.9506, -82.4572),
    "ORL": (28.5383, -81.3792), "SFO": (37.7749, -122.4194),
    "LAS": (36.1699, -115.1398),
}

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

def parse_ticker(ticker):
    match = re.search(r'KX(TEMP|HIGH|LOWT)([A-Z]+)-(\d{2}[A-Z]{3}\d{2})-?([BT]?[\d.]+)', ticker.upper())
    if match:
        mtype = match.group(1)
        city = match.group(2)
        date_str = match.group(3)
        threshold_str = match.group(4).replace('B', '').replace('T', '')
        try:
            threshold = float(threshold_str)
        except ValueError:
            return None, None, None, None, None
        hour = None
        return city, date_str, hour, threshold, mtype
    return None, None, None, None, None

def minutes_to_expiry(market):
    close_str = market.get("close_time") or market.get("expiration_time")
    if not close_str:
        return float("inf")
    try:
        close_dt = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
        now_utc = datetime.now(timezone.utc)
        return (close_dt - now_utc).total_seconds() / 60.0
    except:
        return float("inf")

def _normal_cdf(x):
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))

def get_model_prob(lat, lon, target_hour, threshold_f):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat, "longitude": lon,
        "hourly": "temperature_2m,temperature_2m_spread",
        "models": "ecmwf_ifs025",
        "forecast_days": 2,
        "timezone": "America/New_York"
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        hourly = data.get("hourly", {})
        temps = hourly.get("temperature_2m", [])
        spread = hourly.get("temperature_2m_spread", [])

        if target_hour is not None and target_hour < len(temps):
            forecast_c = temps[target_hour]
            sigma_c = spread[target_hour] if (spread and target_hour < len(spread)) else 1.11
        else:
            day_temps = temps[24:48] if len(temps) >= 48 else temps
            day_spread = spread[24:48] if (spread and len(spread) >= 48) else spread
            forecast_c = max(day_temps) if day_temps else 15.0
            sigma_c = max(day_spread) if day_spread else 1.11

        forecast_f = forecast_c * 9 / 5 + 32
        sigma_f = sigma_c * 9 / 5
        sigma_f = max(sigma_f, 4.0)
        z = (forecast_f - threshold_f) / sigma_f
        prob = _normal_cdf(z) * 100
        prob = round(max(2.0, min(98.0, prob)), 1)
        return prob, round(forecast_f, 1), round(sigma_f, 1)

    except Exception as e:
        log.warning("Open-Meteo failed: %s", e)
    return 50.0, None, None

def cached_model_prob(lat, lon, target_hour, threshold_f, cycle_key):
    cache_key = (lat, lon, target_hour, cycle_key)
    if cache_key not in _forecast_cache:
        result = get_model_prob(lat, lon, target_hour, threshold_f)
        _forecast_cache[cache_key] = result[1], result[2]
        time.sleep(0.1)
    forecast_f, sigma_f = _forecast_cache[cache_key]
    if forecast_f is None or sigma_f is None:
        return 50.0, forecast_f, sigma_f
    z = (forecast_f - threshold_f) / sigma_f
    prob = _normal_cdf(z) * 100
    prob = round(max(2.0, min(98.0, prob)), 1)
    return prob, forecast_f, sigma_f

def log_to_csv(row):
    write_header = not os.path.exists("weather_scans.csv")
    try:
        with open("weather_scans.csv", "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "ticker", "city", "threshold_f", "kalshi_yes", "model_prob", "forecast_f", "model_sigma_f", "edge_cents", "action"])
            if write_header:
                writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        log.warning("CSV write failed: %s", e)

def get_current_bankroll():
    try:
        balance = client.get_balance()
        return balance.balance / 100.0
    except Exception as e:
        log.warning("Balance fetch failed, using cached bankroll $%.2f: %s", BANKROLL, e)
        return BANKROLL

def place_order(ticker, side, contracts, price_cents):
    if DEMO_MODE:
        log.info("[DEMO] Would place %s %d @ %d¢", side.upper(), contracts, price_cents)
        return True

    order_data = {
        "ticker": ticker,
        "action": "buy" if side == "yes" else "sell",
        "side": "yes",
        "count": contracts,
        "type": "limit",
        "client_order_id": str(uuid.uuid4())
    }
    if side == "yes":
        order_data["yes_price"] = price_cents
    else:
        order_data["no_price"] = price_cents

    try:
        result = client.create_order(order_data)
        global BANKROLL
        cost = contracts * (price_cents / 100.0)
        BANKROLL -= cost
        msg = f"✅ TRADE EXECUTED\n{ticker}\n{side.upper()} {contracts} @ {price_cents}¢\nUpdated bankroll: ${BANKROLL:.2f}"
        send_telegram(msg)
        log.info(msg)
        return True
    except Exception as e:
        log.error("Order failed: %s", e)
        send_telegram(f"❌ Order FAILED: {ticker} - {e}")
        return False

def fetch_weather_markets():
    markets = []
    cursor = None
    page = 0
    while True:
        params = {"status": "open", "limit": 200}
        if cursor:
            params["cursor"] = cursor
        try:
            r = requests.get(f"{HOST}/markets", params=params, timeout=15)
            if not r.ok:
                log.warning("Market fetch failed: HTTP %d — %s", r.status_code, r.text[:200])
                break
            data = r.json()
            batch = data.get("markets", [])
            weather_batch = [m for m in batch if any(x in m.get("ticker", "") for x in ["TEMP", "HIGH", "LOWT"])]
            markets.extend(weather_batch)
            page += 1
            log.info("Page %d: %d total, %d weather", page, len(batch), len(weather_batch))
            cursor = data.get("cursor")
            if not cursor or len(batch) < 200:
                break
            time.sleep(0.5)
        except Exception as e:
            log.warning("Market fetch failed: %s", e)
            break
    log.info("Total weather markets fetched: %d", len(markets))
    return markets

print("🚀 Kalshi weather bot v7 — ECMWF, dynamic cities, fixed sigma, DEMO mode")

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

                mins_left = minutes_to_expiry(m)
                if mins_left < MIN_MINS_TO_EXPIRY:
                    continue

                yes_price = m.get("yes_price")
                if yes_price is None:
                    continue

                lat, lon = active_coords[city_code]
                model_prob, forecast_f, sigma_f = cached_model_prob(lat, lon, hour, threshold, cycle_key)

                edge = model_prob - yes_price
log.info("SCAN: %s | kalshi=%d | model=%.1f | forecast=%.1f | sigma=%.1f | edge=%.1f",
         ticker, yes_price, model_prob, forecast_f or 0, sigma_f or 0, edge)

                log_to_csv({
                    "timestamp": datetime.now().isoformat(),
                    "ticker": ticker,
                    "city": city_code,
                    "threshold_f": threshold,
                    "kalshi_yes": yes_price,
                    "model_prob": model_prob,
                    "forecast_f": forecast_f,
                    "model_sigma_f": sigma_f,
                    "edge_cents": round(edge, 2),
                    "action": "none"
                })

                if abs(edge) >= EDGE_THRESHOLD:
                    edges_found += 1
                    direction = "BUY YES" if edge > 0 else "SELL YES (buy NO)"
                    edge_key = (ticker, direction)
                    if edge_key in _seen_edges:
                        continue
                    _seen_edges[edge_key] = datetime.now()

                    side = "yes" if edge > 0 else "no"
                    price_cents = int(yes_price + 1) if edge > 0 else int(yes_price - 1)
                    max_loss = (yes_price / 100.0) if side == "yes" else (1 - yes_price / 100.0)
                    contracts = max(1, int((BANKROLL * RISK_PER_TRADE) / max_loss))

                    msg = f"🔥 EDGE FOUND\n{ticker} ({market_type})\n{threshold}°F | Kalshi {yes_price}¢ | Model {model_prob}% (~{forecast_f}°F)\nEdge {edge:+.1f}¢ → {direction}"
                    send_telegram(msg)
                    log.info(msg)

                    place_order(ticker, side, contracts, price_cents)

            except Exception as e:
                log.warning("Error processing %s: %s", ticker, e)
                continue

        log.info("Cycle complete — %d weather markets scanned, %d edges found. Sleeping %ds...", weather_count, edges_found, SCAN_INTERVAL)
        time.sleep(SCAN_INTERVAL)

    except Exception as e:
        log.exception("Top-level error: %s", e)
        time.sleep(60)
