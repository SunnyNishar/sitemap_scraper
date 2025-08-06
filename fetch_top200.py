import requests
import os

# === Step 1: Fetch all genre IDs and names from iTunes ===
GENRE_LOOKUP_URL = "https://itunes.apple.com/WebObjects/MZStoreServices.woa/ws/genres"

try:
    response = requests.get(GENRE_LOOKUP_URL)
    response.raise_for_status()
    genres_data = response.json()
except Exception as e:
    print("❌ Failed to fetch genres:", e)
    exit(1)


# === Step 2: Recursively collect all genre IDs (including subgenres) ===
def collect_genre_ids(genre_dict):
    ids = {}
    for genre_id, genre_info in genre_dict.items():
        ids[int(genre_id)] = genre_info.get("name", "")
        subgenres = genre_info.get("subgenres", {})
        if subgenres:
            ids.update(collect_genre_ids(subgenres))
    return ids


ALL_GENRES = collect_genre_ids(genres_data["26"].get("subgenres", {}))

print(f"🎧 Total genres + subgenres found: {len(ALL_GENRES)}")

# === Step 3: Regions ===
REGIONS = ["us", "gb", "ca", "au", "in"]

# === Step 4: Crawl each genre × region for top 200 Apple Podcast IDs ===
all_apple_ids = set()
failed_requests = []

for region in REGIONS:
    for genre_id, genre_name in ALL_GENRES.items():
        url = f"https://itunes.apple.com/{region}/rss/toppodcasts/limit=200/genre={genre_id}/json"
        print(f"📥 Fetching {region.upper()} | {genre_id} - {genre_name}")
        try:
            res = requests.get(url)
            res.raise_for_status()
            feed_data = res.json()
            entries = feed_data.get("feed", {}).get("entry", [])

            for entry in entries:
                podcast_url = entry.get("id", {}).get("label", "")
                if "id" in podcast_url:
                    parts = podcast_url.split("/id")
                    if len(parts) == 2:
                        apple_id = parts[1].split("?")[0]
                        all_apple_ids.add(apple_id)
        except Exception as e:
            print(f"❌ Error: {e}")
            failed_requests.append((region, genre_id, genre_name))

# === Step 5: Save Apple IDs to file ===
output_path = "./documents/apple_ids_from_all_genres_regions.txt"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

with open(output_path, "w") as f:
    for apple_id in sorted(all_apple_ids):
        f.write(apple_id + "\n")

print(f"\n✅ Done. Total unique Apple IDs: {len(all_apple_ids)}")
print(f"📁 Saved to: {output_path}")

# === Optional: Log failed requests
if failed_requests:
    print("\n⚠️ Failed Requests:")
    for region, genre_id, genre_name in failed_requests:
        print(f"- {region.upper()} | {genre_id} - {genre_name}")
