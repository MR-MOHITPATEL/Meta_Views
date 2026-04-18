import os
import requests
from dotenv import load_dotenv

load_dotenv()
ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
API_VERSION = os.getenv("API_VERSION", "v19.0")
AD_ACCOUNT_ID = os.getenv("AD_ACCOUNT_ID")

# Get a few adsets to see their targeting
url = f"https://graph.facebook.com/{API_VERSION}/{AD_ACCOUNT_ID}/adsets"
params = {
    "access_token": ACCESS_TOKEN,
    "fields": "id,name,targeting",
    "limit": 5
}
try:
    response = requests.get(url, params=params)
    data = response.json()
    for adset in data.get('data', []):
        print(f"Adset: {adset.get('name')} ({adset.get('id')})")
        print(f"Targeting: {adset.get('targeting', {}).get('geo_locations', {})}")
        print("-" * 50)
except Exception as e:
    print(e)
