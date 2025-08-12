#!/usr/bin/env python3
import requests
import re
import mysql.connector
import datetime
import os
import csv

# ===============================
# CONFIG
# ===============================
GENRE_LOOKUP_URL = "https://itunes.apple.com/WebObjects/MZStoreServices.woa/ws/genres"

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "podcast",  # Change to your DB name
}

OUTPUT_DIR = "./documents"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_IDS = os.path.join(OUTPUT_DIR, "apple_ids_from_all_genres_regions.txt")
UNEXPECTED_LOG = os.path.join(OUTPUT_DIR, "unexpected_id_formats.txt")
FAILED_REQ_LOG = os.path.join(OUTPUT_DIR, "failed_requests.txt")
FAILED_INSERTS_LOG = os.path.join(OUTPUT_DIR, "failed_inserts.txt")


# ===============================
# FETCH GENRE LIST WITH CATEGORY/SUBCATEGORY
# ===============================
def collect_genre_info(genre_dict, parent_name=None, depth=0):
    ids = {}
    for genre_id, genre_info in genre_dict.items():
        name = genre_info.get("name", "")

        if depth == 0:
            # Genre 26 itself
            ids[int(genre_id)] = (name, None)
        elif depth == 1:
            # Immediate children of 26 are main categories (no subcategory)
            ids[int(genre_id)] = (name, None)
        else:
            # Real subcategory
            ids[int(genre_id)] = (parent_name, name)

        subgenres = genre_info.get("subgenres", {})
        if subgenres:
            ids.update(collect_genre_info(subgenres, name, depth + 1))
    return ids


try:
    response = requests.get(GENRE_LOOKUP_URL, timeout=30)
    response.raise_for_status()
    genres_data = response.json()
except Exception as e:
    print("❌ Failed to fetch genres:", e)
    exit(1)

# Only podcast subgenres (no All Podcasts 26)
ALL_GENRES_INFO = collect_genre_info({"26": genres_data["26"]})

# Sort by genre ID
GENRE_LIST = sorted(ALL_GENRES_INFO.items(), key=lambda x: x[0])

print(f"🎧 Total podcast genres (no 26): {len(GENRE_LIST)}")
print("📋 Genres to fetch in order:")
for gid, (cat, subcat) in GENRE_LIST:
    print(f" - {gid}: {cat} / {subcat}")

# ===============================
# COUNTRY CODES
# ===============================
REGIONS = ["us"]  # Add more later

# ===============================
# CONNECT TO DATABASE
# ===============================
db = mysql.connector.connect(**DB_CONFIG)
cursor = db.cursor()

print("♻️ Moving current snapshot to old table...")
cursor.execute("TRUNCATE TABLE unique_apple_chart_old")
cursor.execute("INSERT INTO unique_apple_chart_old SELECT * FROM unique_apple_chart")

cursor.execute("TRUNCATE TABLE unique_apple_chart")
db.commit()

# ===============================
# SQL INSERT TEMPLATE
# ===============================
insert_sql = """
INSERT INTO unique_apple_chart
(chart_rank, podcast_url, appleid, title, img_url, countryCode, countryName, category, subcategory, createdTime, updatedTime)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

all_apple_ids = set()
failed_requests = []
unexpected_formats = []
failed_inserts = []

# ===============================
# FETCH & INSERT
# ===============================
for region in REGIONS:
    print(f"\n🌍 Starting region: {region.upper()} — {len(GENRE_LIST)} genres")
    for idx, (genre_id, (category_name, subcategory_name)) in enumerate(
        GENRE_LIST, start=1
    ):
        display_name = subcategory_name if subcategory_name else category_name
        print(
            f"📥 [{idx}/{len(GENRE_LIST)}] {region.upper()} | {genre_id} - {display_name}"
        )
        url = f"https://itunes.apple.com/{region}/rss/toppodcasts/limit=200/genre={genre_id}/json"

        try:
            res = requests.get(url, timeout=20)
            res.raise_for_status()
            entries = res.json().get("feed", {}).get("entry", [])

            for rank, entry in enumerate(entries, start=1):
                podcast_url = entry.get("id", {}).get("label", "")
                match = re.search(r"/id(\d+)", podcast_url)
                if not match:
                    unexpected_formats.append(
                        f"{region} | {genre_id} | Unexpected format: {podcast_url}"
                    )
                    continue
                apple_id = match.group(1)
                all_apple_ids.add(apple_id)

                title = entry.get("im:name", {}).get("label", "")
                img_url = (
                    entry.get("im:image", [])[-1].get("label", "")
                    if entry.get("im:image")
                    else ""
                )
                now = datetime.datetime.now()

                try:
                    cursor.execute(
                        insert_sql,
                        (
                            rank,
                            podcast_url,
                            apple_id,
                            title,
                            img_url,
                            region,
                            None,  # countryName not fetched
                            category_name,
                            subcategory_name,
                            now,
                            now,
                        ),
                    )
                except mysql.connector.Error as err:
                    failed_inserts.append(
                        f"{region} | {genre_id} | appleid={apple_id} | {err}"
                    )

        except Exception as e:
            failed_requests.append((region, genre_id, subcategory_name, str(e)))

db.commit()

# ===============================
# SAVE LOGS
# ===============================
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

# ===============================
# COMPARE OLD vs NEW
# ===============================
print("\n📊 Rank Changes:")
compare_query = """
SELECT 
    cur.appleid,
    cur.title,
    cur.countryCode,
    cur.category,
    cur.subcategory,
    cur.chart_rank AS current_rank,
    old.chart_rank AS old_rank,
    CASE
        WHEN old.chart_rank IS NULL THEN 'NEW'
        WHEN cur.chart_rank < old.chart_rank THEN CONCAT('+', old.chart_rank - cur.chart_rank)
        WHEN cur.chart_rank > old.chart_rank THEN CONCAT('-', cur.chart_rank - old.chart_rank)
        ELSE '0'
    END AS movement
FROM unique_apple_chart cur
LEFT JOIN unique_apple_chart_old old
    ON cur.appleid = old.appleid
    AND cur.countryCode = old.countryCode
    AND cur.category = old.category
    AND cur.subcategory = old.subcategory
ORDER BY cur.countryCode, cur.category, cur.chart_rank
"""
cursor.execute(compare_query)
rows = cursor.fetchall()

csv_file = os.path.join(OUTPUT_DIR, "rank_comparison.csv")
with open(csv_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    # Write header
    writer.writerow(
        [
            "appleid",
            "title",
            "countryCode",
            "category",
            "subcategory",
            "current_rank",
            "old_rank",
            "movement",
        ]
    )
    # Write all rows
    writer.writerows(rows)

print(f"✅ Rank comparison saved to {csv_file}")

cursor.close()
db.close()
print("\n✅ Done.")
