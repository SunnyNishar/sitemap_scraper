#!/usr/bin/env python3
import requests
import mysql.connector
import datetime
import os
import csv
import time
from typing import Dict, List, Tuple

# ===============================
# CONFIG
# ===============================
GENRE_LOOKUP_URL = "https://itunes.apple.com/WebObjects/MZStoreServices.woa/ws/genres"
CHARTS_URL_TPL = "https://itunes.apple.com/WebObjects/MZStoreServices.woa/ws/charts?cc={cc}&g={genre_id}&name=Podcasts&limit={limit}"
LOOKUP_URL_TPL = "https://itunes.apple.com/lookup?id={ids}&country={country}"

# Set this to 250 to fetch 250 ranks per genre (if available from charts API)
CHART_LIMIT = 250

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "podcast",
}

OUTPUT_DIR = "./documents"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_IDS = os.path.join(OUTPUT_DIR, "apple_ids_from_all_genres_regions_charts.txt")
FAILED_REQ_LOG = os.path.join(OUTPUT_DIR, "failed_requests_charts.txt")
FAILED_INSERTS_LOG = os.path.join(OUTPUT_DIR, "failed_inserts_charts.txt")
CSV_OUTPUT = os.path.join(OUTPUT_DIR, "rank_comparison_charts.csv")

# Regions / storefronts to process (kept same behavior as testdb2: US only by default)
REGIONS = ["us"]


# ===============================
# HELPERS
# ===============================
def safe_get(d, *keys, default=""):
    cur = d
    for k in keys:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur


def collect_genre_info(
    genre_dict, parent_name=None, depth=0
) -> Dict[int, Tuple[str, str]]:
    """
    Build mapping: genre_id -> (category_name, subcategory_name or None)
    Match testdb2 behavior: at root and depth 1, subcategory is None;
    deeper levels record parent as category and current as subcategory.
    """
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


def sort_genres_with_podcasts_first(genres_list):
    """Sorts genres so 'Podcasts' (id 26) comes first, rest by genre_id order."""
    return sorted(genres_list, key=lambda x: (0 if x[0] == 26 else 1, x[0]))


def fetch_chart_ids(region: str, genre_id: int, limit: int) -> List[int]:
    """Call ws/charts and return the ranked Apple IDs for the given genre."""
    url = CHARTS_URL_TPL.format(cc=region, genre_id=genre_id, limit=limit)
    res = requests.get(url, timeout=20)
    res.raise_for_status()
    data = res.json()
    ids = data.get("resultIds", [])
    return [int(x) for x in ids]


def batched(iterable, n):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == n:
            yield batch
            batch = []
    if batch:
        yield batch


def lookup_metadata(apple_ids: List[int], country: str) -> Dict[int, dict]:
    out = {}
    missing_ids = []

    for chunk in batched(apple_ids, 100):
        ids_str = ",".join(str(x) for x in chunk)
        url = LOOKUP_URL_TPL.format(ids=ids_str, country=country)
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            j = r.json()

            found_ids = set()
            for item in j.get("results", []):
                aid = item.get("trackId") or item.get("collectionId")
                if not aid:
                    continue
                title = (
                    item.get("collectionName")
                    or item.get("trackName")
                    or item.get("collectionCensoredName")
                    or item.get("trackCensoredName")
                    or ""
                ).strip()
                artwork = item.get("artworkUrl600") or item.get("artworkUrl100") or ""
                podcast_url = (
                    item.get("trackViewUrl") or item.get("collectionViewUrl") or ""
                )
                out[int(aid)] = {
                    "title": title,
                    "artwork": artwork,
                    "podcast_url": podcast_url,
                }
                found_ids.add(int(aid))

            # Track missing ones for investigation
            for aid in chunk:
                if aid not in found_ids:
                    missing_ids.append(aid)

        except Exception as e:
            print(f"⚠️ Lookup failed for chunk {ids_str[:80]}...: {e}")
            time.sleep(0.5)

    # Save missing IDs for later debug
    if missing_ids:
        with open(os.path.join(OUTPUT_DIR, "lookup_missing_ids.txt"), "a") as f:
            for mid in missing_ids:
                f.write(f"{country},{mid}\n")

    return out


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

    return results


# ===============================
# MAIN
# ===============================
def main():
    # Fetch and build genres mapping
    try:
        response = requests.get(GENRE_LOOKUP_URL, timeout=30)
        response.raise_for_status()
        genres_data = response.json()
    except Exception as e:
        print("❌ Failed to fetch genres:", e)
        return

    # Limit to Podcasts (26) and its tree; match testdb2's collect + sorting
    all_genres_info = collect_genre_info({"26": genres_data["26"]})
    genre_list = sort_genres_with_podcasts_first(list(all_genres_info.items()))

    db = mysql.connector.connect(**DB_CONFIG)
    cursor = db.cursor()

    print("♻️ Moving current snapshot to old table...")
    cursor.execute("TRUNCATE TABLE unique_apple_chart_old")
    cursor.execute(
        "INSERT INTO unique_apple_chart_old SELECT * FROM unique_apple_chart"
    )
    cursor.execute("TRUNCATE TABLE unique_apple_chart")
    db.commit()

    insert_sql = """
    INSERT INTO unique_apple_chart
    (chart_rank, podcast_url, appleid, title, img_url, countryCode, countryName, category, subcategory, createdTime, updatedTime)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    all_apple_ids = set()
    failed_requests, failed_inserts = [], []

    now = datetime.datetime.now()

    for region in REGIONS:
        for idx, (genre_id, (category_name, subcategory_name)) in enumerate(
            genre_list, start=1
        ):
            display_name = subcategory_name if subcategory_name else category_name
            print(
                f"📥 [{idx}/{len(genre_list)}] {region.upper()} | {genre_id} - {display_name}"
            )

            # 1) Get ranked IDs from charts (up to CHART_LIMIT)
            ids_ranked = []
            try:
                ids_ranked = fetch_chart_ids(region, genre_id, CHART_LIMIT)
            except Exception as e:
                print(
                    f"❌ Charts error {region.upper()} | {genre_id} - {display_name}: {e}"
                )
                failed_requests.append((region, genre_id, display_name, str(e)))
                continue

            if not ids_ranked:
                print(
                    f"⚠️ No chart IDs for {region.upper()} | {genre_id} - {display_name}"
                )
                continue

            # 2) Enrich with Lookup metadata in batches
            meta_map = lookup_metadata(ids_ranked, region)

            # 3) Insert rows in rank order
            for rank, aid in enumerate(ids_ranked, start=1):
                all_apple_ids.add(str(aid))
                meta = meta_map.get(aid, {})
                podcast_url = meta.get(
                    "podcast_url",
                    f"https://podcasts.apple.com/{region}/podcast/id{aid}",
                )
                title = meta.get("title", "")
                img_url = meta.get("artwork", "")

                subcat_value = subcategory_name if subcategory_name else None

                try:
                    cursor.execute(
                        insert_sql,
                        (
                            rank,
                            podcast_url,
                            str(aid),
                            title,
                            img_url,
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
                        f"{region} | {genre_id} | appleid={aid} | {err}"
                    )

    db.commit()

    # Save a flat list of every encountered Apple ID (across genres)
    with open(OUTPUT_IDS, "w") as f:
        for aid in sorted(all_apple_ids, key=lambda x: int(x)):
            f.write(aid + "\n")

    with open(FAILED_REQ_LOG, "w") as f:
        for region, gid, gname, err in failed_requests:
            f.write(f"{region.upper()} | {gid} - {gname} | {err}\n")

    with open(FAILED_INSERTS_LOG, "w") as f:
        f.write("\n".join(failed_inserts))

    # ===============================
    # COMPARE RANKS PER GENRE IN PYTHON
    # ===============================
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

        for genre_id, (category_name, subcategory_name) in genre_list:
            changes = compare_ranks(cursor, REGIONS[0], category_name, subcategory_name)
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
    print("\n✅ Done.")


if __name__ == "__main__":
    main()
