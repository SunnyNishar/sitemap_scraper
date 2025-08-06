import requests
import os

# === Expanded Genre IDs ===
# Top-level genres and key sub-genres (official Apple genre IDs)
GENRE_IDS = [
    26,
    1301,  # Arts
    1302,  # Business
    1303,  # Comedy
    1304,  # Education
    1305,  # Kids & Family
    1307,  # Health & Fitness
    1309,  # TV & Film
    1310,  # Music
    1311,  # News
    1314,  # Religion & Spirituality
    1315,  # Science
    1316,  # Society & Culture
    1318,  # Sports
    1320,  # Technology
    1321,  # True Crime
    1323,  # History
    1324,  # Fiction
    1325,  # Leisure
    1326,  # Government
    # === Popular sub-genres ===
    1401,  # Books (Arts sub-genre)
    1402,  # Design (Arts)
    1405,  # Fashion & Beauty (Arts)
    1406,  # Food (Arts)
    1407,  # Performing Arts (Arts)
    1408,  # Visual Arts (Arts)
    1410,  # Documentary (TV & Film)
    1416,  # Music Commentary (Music)
    1420,  # Relationships (Society & Culture)
    1430,  # Mental Health (Health & Fitness)
    1440,  # Sexuality (Health & Fitness)
    1450,  # Marketing (Business)
    1460,  # Entrepreneurship (Business)
    1470,  # Investing (Business)
    1480,  # Management (Business)
    1490,  # Self-Improvement (Education)
]

# === List of Country Codes ===
REGIONS = ["us", "ca", "au", "in", "jp", "br"]

# === Store all unique Apple IDs
all_apple_ids = set()

# === Loop through each region and genre
for region in REGIONS:
    for genre_id in GENRE_IDS:
        url = f"https://itunes.apple.com/{region}/rss/toppodcasts/limit=200/genre={genre_id}/json"
        print(f"📥 Fetching: {url}")
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            entries = data.get("feed", {}).get("entry", [])

            for entry in entries:
                podcast_url = entry.get("id", {}).get("label", "")
                if "id" in podcast_url:
                    parts = podcast_url.split("/id")
                    if len(parts) == 2:
                        apple_id = parts[1].split("?")[0]
                        all_apple_ids.add(apple_id)

        except Exception as e:
            print(f"❌ Error for genre {genre_id} in region {region}: {e}")

# === Save to file
output_path = "./documents/apple_ids_from_genres_regions.txt"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

with open(output_path, "w") as f:
    for apple_id in sorted(all_apple_ids):
        f.write(apple_id + "\n")

print(f"\n✅ Done. Found {len(all_apple_ids)} unique Apple IDs.")
print(f"📁 Saved to '{output_path}'")
