import requests
import os
import json
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

REGIONS = ["ca"]

all_apple_ids = set()
apple_ids_with_timestamps = []
unexpected_formats = []
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
                id_field = entry.get("id", "")
                podcast_url = (
                    id_field.get("label", "")
                    if isinstance(id_field, dict)
                    else id_field
                )

                id_match = re.search(r"/id(\d+)", podcast_url)
                if id_match:
                    apple_id = id_match.group(1)
                    updated_time = ""

                    if "im:releaseDate" in entry:
                        release_field = entry["im:releaseDate"]
                        updated_time = (
                            release_field.get("label", "")
                            if isinstance(release_field, dict)
                            else release_field
                        )

                    if not updated_time and "updated" in entry:
                        updated_field = entry["updated"]
                        updated_time = (
                            updated_field.get("label", "")
                            if isinstance(updated_field, dict)
                            else updated_field
                        )

                    if not updated_time and "published" in entry:
                        published_field = entry["published"]
                        updated_time = (
                            published_field.get("label", "")
                            if isinstance(published_field, dict)
                            else published_field
                        )

                    if apple_id not in all_apple_ids:
                        all_apple_ids.add(apple_id)
                        apple_ids_with_timestamps.append((apple_id, updated_time))
                else:
                    unexpected_formats.append(
                        f"{region} | {genre_id} | No ID pattern found in: {podcast_url}"
                    )

        except Exception as e:
            print(f"❌ Error: {e}")
            failed_requests.append((region, genre_id, genre_name))

# Save Apple IDs + updated times
output_path = "./documents/apple_ids_with_updated_time.txt"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

with open(output_path, "w") as f:
    for apple_id, updated_time in sorted(apple_ids_with_timestamps):
        f.write(f"{apple_id} | {updated_time}\n")

print(f"\n✅ Done. Total unique Apple IDs: {len(all_apple_ids)}")
print(f"📁 Saved to: {output_path}")
