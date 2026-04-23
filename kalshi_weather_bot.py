import os
import time
import csv
import re
import uuid
import requests
from datetime import datetime
from dotenv import load_dotenv
from kalshi_python_sync import Configuration, KalshiClient

load_dotenv()

# ================== CONFIG ==================
BANKROLL = 1000          # your $1000 demo bankroll
EDGE_THRESHOLD = 4
DEMO_MODE = True         # change to False when ready for real money
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
# ============================================

API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")
PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH")
HOST = os.getenv("KALSHI_HOST")

with open(PRIVATE_KEY_PATH, "r") as f:
    private_key_pem = f.read()

config = Configuration(host=HOST)
config.api_key_id = API_KEY_ID
config.private_key_pem = private_key_pem
client = KalshiClient(config)

CITY_COORDS = {
    "NYC": (40.7128, -74.0060),
    "CHI": (41.8781, -87.6298),
    "MIA": (25.7617, -80.1918),
    "LAX": (34.0522, -118.2437),
}

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=10)
    except:
        pass

def parse_ticker(ticker):
    match = re.search(r'KXTEMP([A-Z]+)-(\d{2}[A-Z]{3}\d{2})(\d{2})-T(\d+\.\d+)', ticker.upper())
    if match:
        return match.group(1), match.group(2), int(match.group(3)), float(match.group(4))
    return None, None, None, None

def get_model_prob(lat, lon, target_hour, threshold_f):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {"latitude": lat, "longitude": lon, "hourly": "temperature_2m", "models": "gfs_seamless", "forecast_days": 2, "timezone": "America/New_York"}
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json()
        temps = data.get("hourly", {}).get("temperature_2m", [])
        if temps and target_hour < len(temps):
            forecast = temps[target_hour]
            deviation = forecast - threshold_f
            prob = max(5, min(95, int(50 + deviation * 9.5)))
            return prob, round(forecast, 1)
    except:
        pass
    return 50, None

def place_order(ticker, side, count, price_cents):
    if DEMO_MODE:
        print(f"   [DEMO] Would place {side} {count} @ {price_cents}¢")
        return
    # live order code (same as before)
    try:
        order_data = {"ticker": ticker, "action": "buy" if side == "yes" else "sell", "side": "yes", "count": count, "type": "limit", "yes_price" if side == "yes" else "no_price": price_cents, "client_order_id": str(uuid.uuid4())}
        client.create_order(order_data)
        print(f"   ✅ LIVE ORDER PLACED")
    except Exception as e:
        print(f"   Order failed: {e}")

send_telegram("🚀 Kalshi weather bot started — 24/7 on Railway\nBankroll: $" + str(BANKROLL) + " | Demo: " + str(DEMO_MODE))

print("🚀 Bot running 24/7 with Telegram alerts")

while True:
    try:
        markets_response = client.get_markets(status="open", limit=200)
        markets = markets_response.markets if hasattr(markets_response, 'markets') else []
        
        print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M')} — Scanning...")
        
        for m in markets:
            ticker = getattr(m, 'ticker', '')
            if not ticker.startswith("KXTEMP"):
                continue
            city_code, date_str, hour, threshold = parse_ticker(ticker)
            if not city_code or city_code not in CITY_COORDS:
                continue
                
            yes_price = getattr(m, 'yes_price', 50) or 50
            lat, lon = CITY_COORDS[city_code]
            
            model_prob, forecast = get_model_prob(lat, lon, hour, threshold)
            edge = model_prob - yes_price
            
            # Always log to CSV
            with open("weather_scans.csv", "a", newline="") as f:
                writer = csv.writer(f)
                if f.tell() == 0:
                    writer.writerow(["timestamp", "ticker", "city", "threshold_f", "kalshi_yes", "model_prob", "forecast_f", "edge_cents"])
                writer.writerow([datetime.now().isoformat(), ticker, city_code, threshold, yes_price, model_prob, forecast, edge])
            
            if abs(edge) >= EDGE_THRESHOLD:
                direction = "BUY YES" if edge > 0 else "SELL YES (buy NO)"
                msg = f"🔥 EDGE FOUND\n{ticker}\n{threshold}°F | Kalshi {yes_price}¢ | Model {model_prob}% (~{forecast}°F)\nEdge {edge:+.1f}¢ → {direction}"
                send_telegram(msg)
                print(f"🔥 {msg}")
                
                risk_dollars = BANKROLL * 0.01
                contract_value = yes_price / 100.0
                contracts = max(1, int(risk_dollars / contract_value))
                side = "yes" if edge > 0 else "no"
                price_cents = int(yes_price + 1) if edge > 0 else int(yes_price - 1)
                place_order(ticker, side, contracts, price_cents)
        
        print("Cycle complete — sleeping 10 min")
        time.sleep(600)
        
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(60)
