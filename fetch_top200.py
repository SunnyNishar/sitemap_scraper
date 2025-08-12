import requests
import os
import re

GENRE_LOOKUP_URL = "https://itunes.apple.com/WebObjects/MZStoreServices.woa/ws/genres"

try:
    response = requests.get(GENRE_LOOKUP_URL)
    response.raise_for_status()
    genres_data = response.json()
except Exception as e:
    print("❌ Failed to fetch genres:", e)
    exit(1)


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

REGIONS = [
    "us",
]

all_apple_ids = set()
failed_requests = []
unexpected_formats = []

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
                id_field = entry.get("id", "")

                # Handle known formats
                if isinstance(id_field, dict):
                    podcast_url = id_field.get("label", "")
                elif isinstance(id_field, str):
                    podcast_url = id_field
                else:
                    podcast_url = ""

                # Try to extract Apple ID
                if "id" in podcast_url:
                    match = re.search(r"/id(\d+)", podcast_url)
                    if match:
                        apple_id = match.group(1)
                        all_apple_ids.add(apple_id)
                    else:
                        unexpected_formats.append(
                            f"{region} | {genre_id} | Unexpected split format: {podcast_url}"
                        )
                else:
                    unexpected_formats.append(
                        f"{region} | {genre_id} | No 'id' in: {podcast_url}"
                    )

        except Exception as e:
            print(f"❌ Error: {e}")
            failed_requests.append((region, genre_id, genre_name))

# Save Apple IDs
output_path = "./documents/apple_ids_from_all_genres_regions.txt"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

with open(output_path, "w") as f:
    for apple_id in sorted(all_apple_ids):
        f.write(apple_id + "\n")

# Save unexpected formats
unexpected_log_path = "./documents/unexpected_id_formats.txt"
failed_requests_path = "./documents/failed_requests.txt"
with open(unexpected_log_path, "w") as f:
    for line in unexpected_formats:
        f.write(line + "\n")

# Final summary
print(f"\n✅ Done. Total unique Apple IDs: {len(all_apple_ids)}")
print(f"📁 Saved to: {output_path}")
print(f"🪵 Unexpected formats logged to: {unexpected_log_path}")
print(f"🗃 Failed requests logged to: {failed_requests_path}")

if failed_requests:
    print("\n⚠️ Failed Requests:")
    with open(failed_requests_path, "w") as f:
        for region, genre_id, genre_name in failed_requests:
            f.write(f"{region.upper()} | {genre_id} - {genre_name}\n")
