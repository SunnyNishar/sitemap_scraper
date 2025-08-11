import requests
import json
import string

# === API endpoint ===
url = "https://charts.youtube.com/youtubei/v1/browse?alt=json"

# === Common payload structure ===
base_payload = {
    "context": {
        "capabilities": {},
        "client": {
            "clientName": "WEB_MUSIC_ANALYTICS",
            "clientVersion": "2.0",
            "hl": "en-GB",
            "gl": "IN",
            "experimentIds": [],
            "experimentsToken": "",
            "theme": "MUSIC",
        },
        "request": {"internalExperimentFlags": []},
    },
    "browseId": "FEmusic_analytics_charts_home",
}

headers = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}

# === Full ISO 3166-1 alpha-2 country codes list ===
country_codes = [
    "us",
    "gb",
    "in",
    "ca",
    "au",
    "nz",
    "de",
    "fr",
    "es",
    "it",
    "br",
    "mx",
    "jp",
    "kr",
    "id",
    "ru",
    "nl",
    "se",
    "no",
    "dk",
    "fi",
    "pl",
    "pt",
    "ar",
    "cl",
    "co",
    "pe",
    "ve",
    "za",
    "ng",
    "ke",
    "eg",
    "ua",
    "my",
    "sg",
    "ph",
    "th",
    "vn",
    "tr",
    "sa",
    "ae",
    "kw",
    "qa",
    # Add more if needed
]

supported_regions = {}

print("🌍 Checking YouTube Podcasts availability by region...\n")

for code in country_codes:
    payload = base_payload.copy()
    payload["query"] = (
        f"perspective=PODCAST_SHOW"
        f"&chart_params_country_code={code}"
        f"&chart_params_chart_type=PODCAST_SHOWS_BY_WATCH_TIME"
        f"&chart_params_period_type=WEEKLY"
    )

    try:
        res = requests.post(url, headers=headers, json=payload, timeout=10)
        res.raise_for_status()
        data = res.json()

        # Quick detection: look for "contents" or "sectionListRenderer"
        if "contents" in json.dumps(data):
            supported_regions[code] = True
            print(f"✅ {code.upper()} - Supported")
        else:
            print(f"❌ {code.upper()} - No data")
    except Exception as e:
        print(f"⚠️ {code.upper()} - Error: {e}")

# === Save supported regions list ===
with open("youtube_podcasts_supported_regions.json", "w", encoding="utf-8") as f:
    json.dump(list(supported_regions.keys()), f, indent=2)

print("\n🎯 Supported regions saved to youtube_podcasts_supported_regions.json")
