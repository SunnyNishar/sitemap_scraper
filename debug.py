#!/usr/bin/env python3
import requests
import re
import mysql.connector
import datetime
import os
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===============================
# CONFIG
# ===============================
GENRE_LOOKUP_URL = "https://itunes.apple.com/WebObjects/MZStoreServices.woa/ws/genres"
LOOKUP_API_BASE = "https://itunes.apple.com/lookup"

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "podcast",
}

OUTPUT_DIR = "./documents"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_IDS = os.path.join(OUTPUT_DIR, "apple_ids_from_all_genres_regions.txt")
UNEXPECTED_LOG = os.path.join(OUTPUT_DIR, "unexpected_id_formats.txt")
FAILED_REQ_LOG = os.path.join(OUTPUT_DIR, "failed_requests.txt")
FAILED_INSERTS_LOG = os.path.join(OUTPUT_DIR, "failed_inserts.txt")
FAILED_METADATA_LOG = os.path.join(OUTPUT_DIR, "failed_metadata.txt")
CSV_OUTPUT = os.path.join(OUTPUT_DIR, "rank_comparison2.csv")

# Rate limiting settings
MAX_WORKERS = 10  # Concurrent threads for metadata fetching
REQUEST_DELAY = 0.1  # Delay between requests to avoid rate limiting


# ===============================
# FUNCTIONS
# ===============================
def collect_genre_info(genre_dict, parent_name=None, depth=0):
    ids = {}
    for genre_id, genre_info in genre_dict.items():
        name = genre_info.get("name", "")

        if depth == 0:
            ids[int(genre_id)] = (name, None)  # genre 26 itself
        elif depth == 1:
            ids[int(genre_id)] = (name, None)  # main category
        else:
            ids[int(genre_id)] = (parent_name, name)  # subcategory

        subgenres = genre_info.get("subgenres", {})
        if subgenres:
            ids.update(collect_genre_info(subgenres, name, depth + 1))
    return ids


def fetch_podcast_metadata(apple_id):
    """Fetch podcast metadata using iTunes Lookup API"""
    try:
        url = f"{LOOKUP_API_BASE}?id={apple_id}&entity=podcast"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("results") and len(data["results"]) > 0:
            result = data["results"][0]
            return {
                "title": result.get("trackName", ""),
                "img_url": result.get("artworkUrl600", result.get("artworkUrl100", "")),
                "podcast_url": result.get(
                    "trackViewUrl", f"https://podcasts.apple.com/podcast/id{apple_id}"
                ),
            }
    except Exception as e:
        return {"error": str(e)}

    return {"error": "No results found"}


def fetch_metadata_batch(apple_ids):
    """Fetch metadata for multiple apple IDs concurrently"""
    metadata = {}
    failed_metadata = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {
            executor.submit(fetch_podcast_metadata, aid): aid for aid in apple_ids
        }

        for future in as_completed(future_to_id):
            apple_id = future_to_id[future]
            try:
                result = future.result()
                if "error" in result:
                    failed_metadata.append(f"{apple_id}: {result['error']}")
                    # Use defaults for failed requests
                    metadata[apple_id] = {
                        "title": f"Podcast {apple_id}",
                        "img_url": "",
                        "podcast_url": f"https://podcasts.apple.com/podcast/id{apple_id}",
                    }
                else:
                    metadata[apple_id] = result

                time.sleep(REQUEST_DELAY)  # Rate limiting
            except Exception as e:
                failed_metadata.append(f"{apple_id}: {str(e)}")
                metadata[apple_id] = {
                    "title": f"Podcast {apple_id}",
                    "img_url": "",
                    "podcast_url": f"https://podcasts.apple.com/podcast/id{apple_id}",
                }

    return metadata, failed_metadata


def compare_ranks(db_cursor, region, category, subcategory):
    """Compare today's vs yesterday's ranks for a given genre & return changes."""
    # Fetch yesterday's ranks
    db_cursor.execute(
        """
        SELECT appleid, chart_rank, title
        FROM unique_apple_chart_old
        WHERE countryCode=%s AND category=%s AND (
            (subcategory IS NULL AND %s IS NULL) OR subcategory=%s
        )
    """,
        (region, category, subcategory, subcategory),
    )
    old_data = {
        row[0]: (row[1], row[2]) for row in db_cursor.fetchall()
    }  # appleid: (rank, title)

    # Fetch today's ranks
    db_cursor.execute(
        """
        SELECT appleid, chart_rank, title
        FROM unique_apple_chart
        WHERE countryCode=%s AND category=%s AND (
            (subcategory IS NULL AND %s IS NULL) OR subcategory=%s
        )
    """,
        (region, category, subcategory, subcategory),
    )
    new_data = {
        row[0]: (row[1], row[2]) for row in db_cursor.fetchall()
    }  # appleid: (rank, title)

    results = []
    for aid, (new_rank, title) in new_data.items():
        old_rank, _ = old_data.get(aid, (None, None))
        if old_rank is None:
            change = "NEW"
        else:
            diff = old_rank - new_rank
            change = f"+{diff}" if diff > 0 else (str(diff) if diff < 0 else "0")

        results.append((aid, title, new_rank, old_rank, change))

    for aid, (old_rank, title) in old_data.items():
        if aid not in new_data:
            results.append((aid, title, None, old_rank, "OUT"))

    return results


def sort_genres_with_podcasts_first(genres_list):
    """Sorts genres so 'Podcasts' (id 26) comes first, rest by genre_id order."""
    return sorted(genres_list, key=lambda x: (0 if x[0] == 26 else 1, x[0]))


# ===============================
# MAIN FETCH & INSERT
# ===============================
try:
    response = requests.get(GENRE_LOOKUP_URL, timeout=30)
    response.raise_for_status()
    genres_data = response.json()
except Exception as e:
    print("❌ Failed to fetch genres:", e)
    exit(1)

ALL_GENRES_INFO = collect_genre_info({"26": genres_data["26"]})
GENRE_LIST = sort_genres_with_podcasts_first(list(ALL_GENRES_INFO.items()))

REGIONS = ["us"]

db = mysql.connector.connect(**DB_CONFIG)
cursor = db.cursor()

print("♻️ Moving current snapshot to old table...")
cursor.execute("TRUNCATE TABLE unique_apple_chart_old")
cursor.execute("INSERT INTO unique_apple_chart_old SELECT * FROM unique_apple_chart")
cursor.execute("TRUNCATE TABLE unique_apple_chart")
db.commit()

insert_sql = """
INSERT INTO unique_apple_chart
(chart_rank, podcast_url, appleid, title, img_url, countryCode, countryName, category, subcategory, createdTime, updatedTime)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

all_apple_ids = set()
failed_requests, unexpected_formats, failed_inserts = [], [], []
all_failed_metadata = []

for region in REGIONS:
    for idx, (genre_id, (category_name, subcategory_name)) in enumerate(
        GENRE_LIST, start=1
    ):
        display_name = subcategory_name if subcategory_name else category_name
        print(
            f"🔥 [{idx}/{len(GENRE_LIST)}] {region.upper()} | {genre_id} - {display_name}"
        )

        # Use Charts API to get 250 results (same as chart_api_250.py)
        url = f"https://itunes.apple.com/WebObjects/MZStoreServices.woa/ws/charts?cc={region}&g={genre_id}&name=Podcasts&limit=250"

        try:
            res = requests.get(url, timeout=20)
            res.raise_for_status()
            data = res.json()

            result_ids = data.get("resultIds", [])
            if not result_ids:
                print(f"⚠️ No IDs for {region.upper()} | {genre_id} - {display_name}")
                continue

            # Convert to strings and add to global set
            apple_ids_for_genre = [str(aid) for aid in result_ids]
            all_apple_ids.update(apple_ids_for_genre)

            print(f"📊 Fetching metadata for {len(apple_ids_for_genre)} podcasts...")

            # Fetch metadata for all podcasts in this genre
            metadata, failed_meta = fetch_metadata_batch(apple_ids_for_genre)
            all_failed_metadata.extend(failed_meta)

            # Insert into database with ranking
            for rank, apple_id in enumerate(apple_ids_for_genre, start=1):
                meta = metadata.get(
                    apple_id,
                    {
                        "title": f"Podcast {apple_id}",
                        "img_url": "",
                        "podcast_url": f"https://podcasts.apple.com/podcast/id{apple_id}",
                    },
                )

                now = datetime.datetime.now()
                subcat_value = subcategory_name if subcategory_name else None

                try:
                    cursor.execute(
                        insert_sql,
                        (
                            rank,
                            meta["podcast_url"],
                            apple_id,
                            meta["title"],
                            meta["img_url"],
                            region,
                            None,
                            category_name,
                            subcat_value,
                            now,
                            now,
                        ),
                    )
                except mysql.connector.Error as err:
                    failed_inserts.append(
                        f"{region} | {genre_id} | appleid={apple_id} | {err}"
                    )

        except Exception as e:
            failed_requests.append((region, genre_id, display_name, str(e)))

db.commit()

# Save logs
with open(OUTPUT_IDS, "w") as f:
    for aid in sorted(all_apple_ids):
        f.write(aid + "\n")

with open(UNEXPECTED_LOG, "w") as f:
    f.write("\n".join(unexpected_formats))

with open(FAILED_REQ_LOG, "w") as f:
    for region, gid, gname, err in failed_requests:
        f.write(f"{region.upper()} | {gid} - {gname} | {err}\n")

with open(FAILED_INSERTS_LOG, "w") as f:
    f.write("\n".join(failed_inserts))

with open(FAILED_METADATA_LOG, "w") as f:
    f.write("\n".join(all_failed_metadata))

print(f"✅ Total unique Apple IDs collected: {len(all_apple_ids)}")
print(f"📁 Saved to: {OUTPUT_IDS}")
if failed_requests:
    print(f"⚠️ {len(failed_requests)} failed chart requests logged")
if all_failed_metadata:
    print(f"⚠️ {len(all_failed_metadata)} failed metadata requests logged")

# ===============================
# COMPARE RANKS PER GENRE IN PYTHON
# ===============================
print("📊 Generating rank comparison CSV...")
with open(CSV_OUTPUT, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(
        [
            "appleid",
            "title",
            "category",
            "subcategory",
            "current_rank",
            "old_rank",
            "movement",
        ]
    )

    for genre_id, (category_name, subcategory_name) in GENRE_LIST:
        changes = compare_ranks(cursor, "us", category_name, subcategory_name)
        for aid, title, new_rank, old_rank, change in changes:
            writer.writerow(
                [
                    aid,
                    title,
                    category_name,
                    subcategory_name,
                    new_rank,
                    old_rank,
                    change,
                ]
            )

print(f"✅ Rank comparison saved to {CSV_OUTPUT}")

cursor.close()
db.close()
print("\n✅ Done. Enhanced script completed with 250 podcasts per genre!")
