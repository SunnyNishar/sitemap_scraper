import requests

# API URL
url = "https://itunes.apple.com/WebObjects/MZStoreServices.woa/ws/charts?cc=us&g=1489&name=Podcasts&limit=300"

# Fetch the data
response = requests.get(url)
response.raise_for_status()

# Parse JSON
data = response.json()

# The IDs are inside data['resultIds']
ids = data.get("resultIds", [])

# Save to a text file
with open("./documents/chart_api.txt", "w") as f:
    for pid in ids:
        f.write(f"{pid}\n")

print(f"Saved {len(ids)} IDs to podcast_ids.txt")
