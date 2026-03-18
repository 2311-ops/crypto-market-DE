"""Test CoinGecko API connectivity."""
import requests
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("COINGECKO_API_KEY")
API_BASE = os.getenv("COINGECKO_API_BASE", "https://api.coingecko.com/api/v3")
COINS = os.getenv("COINGECKO_COINS", "bitcoin,ethereum,solana").split(",")
VS_CURRENCY = os.getenv("COINGECKO_VS_CURRENCY", "usd")
ORDER = os.getenv("COINGECKO_ORDER", "market_cap_desc")
PER_PAGE = int(os.getenv("COINGECKO_PER_PAGE", 50))

headers = {"x-cg-pro-api-key": API_KEY} if API_KEY else {}

url = f"{API_BASE}/coins/markets"
params = {
    "vs_currency": VS_CURRENCY,
    "ids": ",".join(COINS),
    "order": ORDER,
    "per_page": PER_PAGE,
    "page": 1,
    "sparkline": "false",
    "price_change_percentage": "24h",
}

print(f"Testing API with URL: {url}")
print(f"Params: {params}")
print(f"Headers: {headers}")

try:
    response = requests.get(url, params=params, headers=headers, timeout=10)
    print(f"Status Code: {response.status_code}")
    if response.status_code == 200:
        print("✅ API call successful!")
        data = response.json()
        print(f"Received {len(data)} coins")
        print(f"Sample data: {data[0] if data else 'No data'}")
    else:
        print(f"❌ API Error: {response.text[:500]}")
except Exception as e:
    print(f"❌ Request failed: {e}")