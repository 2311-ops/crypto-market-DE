"""Test CoinGecko API without API key."""
import requests

url = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    "vs_currency": "usd",
    "ids": "bitcoin",
    "order": "market_cap_desc",
    "per_page": 1,
    "page": 1,
    "sparkline": "false",
}

print(f"Testing FREE API with URL: {url}")
print(f"Params: {params}")

try:
    response = requests.get(url, params=params, timeout=10)
    print(f"Status Code: {response.status_code}")
    if response.status_code == 200:
        print("✅ FREE API call successful!")
        data = response.json()
        print(f"Received {len(data)} coins")
        if data:
            print(f"Sample: {data[0]['id']} - ${data[0]['current_price']}")
    else:
        print(f"❌ API Error: {response.text[:300]}")
except Exception as e:
    print(f"❌ Request failed: {e}")