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
BANKROLL          = 1000       # Total bankroll in dollars
RISK_PER_TRADE    = 0.02       # Fraction of bankroll risked per trade
EDGE_THRESHOLD    = 4          # Minimum net edge in cents
DEMO_MODE         = True       # Set False only when live order code is fully tested
SCAN_INTERVAL     = 600        # Seconds between full scans
MIN_MINS_TO_EXPIRY = 15        # Skip markets expiring sooner than this
OPEN_METEO_DELAY  = 0.1        # Seconds between Open-Meteo calls (rate limiting)
# ============================================

API_KEY_ID        = os.getenv("KALSHI_API_KEY_ID")
PRIVATE_KEY_PATH  = os.getenv("KALSHI_PRIVATE_KEY_PATH")
HOST              = os.getenv("KALSHI_HOST")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID")

# =================== LOGGING ===================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("kalshi_bot.log"), logging.StreamHandler()]
)
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

CSV_PATH = "weather_scans.csv"
CSV_HEADERS = ["timestamp", "ticker", "city", "threshold_f", "kalshi_yes", "kalshi_bid",
               "kalshi_ask", "spread", "model_prob", "forecast_f", "model_sigma_f",
               "edge_cents", "action"]

_forecast_cache: dict = {}
_seen_edges: dict = {}

def send_telegram(message: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message},
            timeout=10
        )
    except Exception as e:
        log.warning("Telegram send failed: %s", e)

def parse_ticker(ticker: str):
    match = re.search(r'KX(TEMP|HIGH|LOWT)([A-Z]+)-(\d{2}[A-Z]{3}\d{2})(\d{2})?-?T?(\d+\.\d+)?', ticker.upper())
    if match:
        mtype = match.group(1)
        city = match.group(2)
        date_str = match.group(3)
        hour = int(match.group(4)) if match.group(4) else None
        threshold = float(match.group(5)) if match.group(5) else None
        return city, date_str, hour, threshold, mtype
    return None, None, None, None, None

def minutes_to_expiry(market: dict) -> float:
    close_str = market.get("close_time") or market.get("expiration_time")
    if not close_str:
        return float("inf")
    try:
        close_dt = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
        now_utc = datetime.now(timezone.utc)
        return (close_dt - now_utc).total_seconds() / 60.0
    except ValueError:
        return float("inf")

def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))

def get_model_prob(lat: float, lon: float, target_hour: int | None, threshold_f: float):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat, "longitude": lon,
        "hourly": "temperature_2m,temperature_2m_spread",
        "models": "gfs_seamless",
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
            forecast_f = forecast_c * 9 / 5 + 32
            sigma_c = spread[target_hour] if (spread and target_hour < len(spread)) else 1.11
            sigma_f = sigma_c * 9 / 5

            z = (forecast_f - threshold_f) / sigma_f
            prob = _normal_cdf(z) * 100
            prob = round(max(2.0, min(98.0, prob)), 1)
            return prob, round(forecast_f, 1), round(sigma_f, 1)
    except Exception as e:
        log.warning("Open-Meteo request failed: %s", e)
    return 50.0, None, None

def cached_model_prob(lat: float, lon: float, target_hour: int | None, threshold_f: float, cycle_key: str):
    cache_key = (lat, lon, cycle_key)
    if cache_key not in _forecast_cache:
        result = get_model_prob(lat, lon, target_hour, threshold_f)
        _forecast_cache[cache_key] = result[1], result[2]
        time.sleep(OPEN_METEO_DELAY)
    forecast_f, sigma_f = _forecast_cache[cache_key]
    if forecast_f is None or sigma_f is None:
        return 50.0, forecast_f, sigma_f
    z = (forecast_f - threshold_f) / sigma_f
    prob = _normal_cdf(z) * 100
    prob = round(max(2.0, min(98.0, prob)), 1)
    return prob, forecast_f, sigma_f

def log_to_csv(row: dict) -> None:
    write_header = not os.path.exists(CSV_PATH)
    try:
        with open(CSV_PATH, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            if write_header:
                writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        log.warning("CSV write failed: %s", e)

def calc_contracts(edge: float, yes_price: int, side: str) -> int:
    max_loss_per_contract = yes_price if side == "yes" else (100 - yes_price)
    if max_loss_per_contract <= 0:
        return 1
    edge_fraction = abs(edge) / 100.0
    kelly_fraction = edge_fraction / (max_loss_per_contract / 100.0)
    half_kelly = kelly_fraction * 0.5
    risk_cap_dollars = BANKROLL * RISK_PER_TRADE
    kelly_dollars = BANKROLL * half_kelly
    risk_dollars = min(risk_cap_dollars, kelly_dollars)
    cost_per_contract = max_loss_per_contract / 100.0
    contracts = max(1, int(risk_dollars / cost_per_contract))
    return contracts

def place_order(ticker: str, side: str, contracts: int, price_cents: int) -> None:
    if DEMO_MODE:
        log.info("[DEMO] Would place %s %d × %s @ %d¢", side.upper(), contracts, ticker, price_cents)
        return
    # Live order code goes here when you're ready
    log.info("LIVE ORDER PLACED (stub)")

# ================== MAIN LOOP ==================
log.info("🚀 Kalshi weather bot (cleaned & improved) started — 13 cities")

last_daily_summary = datetime.now()

while True:
    try:
        resp = requests.get(f"{HOST}/markets", params={"status": "open", "limit": 200}, timeout=15)
        resp.raise_for_status()
        markets = resp.json().get("markets", [])

        log.info("Scanning %d markets...", len(markets))

        edges_found = 0
        cycle_key = datetime.now().strftime("%Y-%m-%d-%H")
        _forecast_cache.clear()

        for m in markets:
            try:
                ticker = m.get("ticker", "")
                if not ticker.startswith("KX"):
                    continue

                mins_left = minutes_to_expiry(m)
                if mins_left < MIN_MINS_TO_EXPIRY:
                    continue

                city_code, date_str, hour, threshold, market_type = parse_ticker(ticker)
                if not city_code or city_code not in CITY_COORDS or threshold is None:
                    continue

                yes_bid = m.get("yes_bid") or m.get("yes_price") or 50
                yes_ask = m.get("yes_ask") or m.get("yes_price") or 50
                yes_mid = (yes_bid + yes_ask) / 2
                spread = yes_ask - yes_bid

                lat, lon = CITY_COORDS[city_code]
                model_prob, forecast_f, sigma_f = cached_model_prob(lat, lon, hour, threshold, cycle_key)

                raw_edge = model_prob - yes_mid
                net_edge = abs(raw_edge) - spread / 2

                # Always log
                log_to_csv({
                    "timestamp": datetime.now().isoformat(),
                    "ticker": ticker,
                    "city": city_code,
                    "threshold_f": threshold,
                    "kalshi_yes": yes_mid,
                    "kalshi_bid": yes_bid,
                    "kalshi_ask": yes_ask,
                    "spread": spread,
                    "model_prob": model_prob,
                    "forecast_f": forecast_f,
                    "model_sigma_f": sigma_f,
                    "edge_cents": round(raw_edge, 2),
                    "action": "none"
                })

                if net_edge < EDGE_THRESHOLD:
                    continue

                direction = "BUY YES" if raw_edge > 0 else "SELL YES (buy NO)"
                dedup_key = (ticker, direction)
                if dedup_key in _seen_edges:
                    continue
                _seen_edges[dedup_key] = datetime.now()

                edges_found += 1
                side = "yes" if raw_edge > 0 else "no"
                price_cents = int(yes_ask + 1) if side == "yes" else int(yes_bid)
                contracts = calc_contracts(raw_edge, int(yes_mid), side)

                msg = (f"🔥 EDGE FOUND\n"
                       f"{ticker} ({market_type}) | {mins_left:.0f} min left\n"
                       f"Threshold {threshold}°F | Spread {spread:.1f}¢\n"
                       f"Kalshi mid {yes_mid:.1f}¢ | Model {model_prob}% ({forecast_f}°F ± {sigma_f}°F)\n"
                       f"Raw edge {raw_edge:+.1f}¢ | Net edge {net_edge:+.1f}¢\n"
                       f"Size: {contracts} contracts @ {price_cents}¢")

                log.info(msg)
                send_telegram(msg)
                place_order(ticker, side, contracts, price_cents)

            except Exception as e:
                log.warning("Error processing market %s: %s", ticker, e)
                continue

        log.info("Cycle complete — %d edges found. Sleeping %ds...", edges_found, SCAN_INTERVAL)
        time.sleep(SCAN_INTERVAL)

    except Exception as e:
        log.exception("Top-level error: %s", e)
        time.sleep(60)
