#!/usr/bin/env python3
import requests
import mysql.connector
import datetime
import os
import csv
import time
import random
from typing import Dict, List, Tuple
from collections import defaultdict

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

# Expanded regions list from yt.py
REGIONS = [
    "us",
    "in",
    "gb",
    "ca",
    "fr",
    "de",
    "au",
    "it",
    "kr",
    "hk",
    "tw",
    "es",
    "ie",
    "se",
    "ch",
    "sg",
    "no",
    "at",
    "dk",
    "jp",
    "fi",
    "be",
    "nl",
    "cn",
    "id",
    "br",
    "ru",
    "mx",
    "ae",
    "sa",
    "il",
    "ar",
    "cl",
    "co",
    "nz",
    "ph",
    "pl",
    "za",
    "ua",
]

COUNTRY_NAMES = {
    "us": "United States",
    "in": "India",
    "gb": "United Kingdom",
    "ca": "Canada",
    "fr": "France",
    "de": "Germany",
    "au": "Australia",
    "it": "Italy",
    "kr": "South Korea",
    "hk": "Hong Kong",
    "tw": "Taiwan",
    "es": "Spain",
    "ie": "Ireland",
    "se": "Sweden",
    "ch": "Switzerland",
    "sg": "Singapore",
    "no": "Norway",
    "at": "Austria",
    "dk": "Denmark",
    "jp": "Japan",
    "fi": "Finland",
    "be": "Belgium",
    "nl": "Netherlands",
    "cn": "China",
    "id": "Indonesia",
    "br": "Brazil",
    "ru": "Russia",
    "mx": "Mexico",
    "ae": "United Arab Emirates",
    "sa": "Saudi Arabia",
    "il": "Israel",
    "ar": "Argentina",
    "cl": "Chile",
    "co": "Colombia",
    "nz": "New Zealand",
    "ph": "Philippines",
    "pl": "Poland",
    "za": "South Africa",
    "ua": "Ukraine",
}


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


def get_genres_for_region(all_genres_info, region):
    """Get genre list for a region. Always include genre 26 (Podcasts) for all regions."""
    # Always include all genres, including 26
    genre_list = sort_genres_with_podcasts_first(list(all_genres_info.items()))
    return genre_list


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


def safe_request(url, retries=5):
    """Perform a GET request with retries and exponential backoff."""
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            wait = (2**attempt) + random.uniform(0, 1)
            print(
                f"⚠️ Request failed (attempt {attempt + 1}/{retries}): {e} | retrying in {wait:.1f}s"
            )
            time.sleep(wait)
    raise Exception(f"❌ Failed after {retries} retries: {url}")


def lookup_metadata(
    apple_ids: List[int], country: str, missing_ids_set: set
) -> Dict[int, dict]:
    """
    Look up Apple metadata with adaptive batching:
    - Try 100 IDs per request
    - If lookup fails, retry in smaller chunks (50 each)
    """
    out = {}

    def process_chunk(chunk):
        ids_str = ",".join(str(x) for x in chunk)
        url = LOOKUP_URL_TPL.format(ids=ids_str, country=country)
        r = safe_request(url)  # will retry internally
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

        # Track missing
        for aid in chunk:
            if aid not in found_ids:
                missing_ids_set.add((country, aid))

    for chunk in batched(apple_ids, 100):
        try:
            process_chunk(chunk)
        except Exception as e:
            print(
                f"⚠️ Lookup failed for 100-size chunk (starting {chunk[0]}): {e} — retrying in halves"
            )
            with open(FAILED_REQ_LOG, "a", encoding="utf-8") as f:
                f.write(f"{country.upper()} | CHUNK START {chunk[0]} | ERROR: {e}\n")

            # fallback: split into two 50-ID chunks
            for half in batched(chunk, 50):
                try:
                    process_chunk(half)
                except Exception as e2:
                    print(f"❌ Still failed for half-chunk (starting {half[0]}): {e2}")
                    with open(FAILED_REQ_LOG, "a", encoding="utf-8") as f:
                        f.write(
                            f"{country.upper()} | HALF-CHUNK START {half[0]} | ERROR: {e2}\n"
                        )
                    time.sleep(1)

    return out


def get_last_table_name(cursor):
    """Get the most recent chart table name"""
    cursor.execute("""
        SELECT TABLE_NAME 
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = 'podcast' 
        AND TABLE_NAME LIKE 'apple_chart_%'
        ORDER BY TABLE_NAME DESC 
        LIMIT 1
    """)
    result = cursor.fetchone()
    return result[0] if result else None


def create_new_table(cursor, table_name):
    """Create a new chart table with timestamp and optimized indexes"""
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id int NOT NULL AUTO_INCREMENT,
            chart_rank int DEFAULT NULL,
            podcast_url varchar(255) DEFAULT NULL,
            appleid varchar(50) DEFAULT NULL,
            title varchar(255) DEFAULT NULL,
            img_url varchar(255) DEFAULT NULL,
            countryCode varchar(10) DEFAULT NULL,
            countryName varchar(100) DEFAULT NULL,
            category varchar(50) DEFAULT NULL,
            subcategory varchar(255) DEFAULT NULL,
            old_rank int DEFAULT NULL,
            movement varchar(10) DEFAULT NULL,
            createdTime datetime DEFAULT NULL,
            updatedTime datetime DEFAULT NULL,
            PRIMARY KEY (id),
            INDEX idx_comparison (appleid, countryCode, category, subcategory),
            INDEX idx_country (countryCode)
        ) ENGINE=InnoDB
    """)


# OPTIMIZED: Load all data at once instead of per-region
def load_all_table_data(cursor, table):
    """Load all data from table at once and return as dictionary"""
    cursor.execute(f"""
        SELECT appleid, chart_rank, title, countryCode, countryName, category, 
               COALESCE(subcategory, '') as subcategory
        FROM {table}
    """)

    data = defaultdict(dict)
    for row in cursor.fetchall():
        appleid, rank, title, country, country_name, category, subcategory = row
        key = (appleid, country, category, subcategory)
        data[key] = {"rank": rank, "title": title, "country_name": country_name}
    return data


# OPTIMIZED: Bulk comparison instead of region-by-region
def bulk_rank_comparison(cursor, last_table, current_table):
    """Compare all ranks at once using bulk operations"""
    print("📄 Loading previous rankings...")
    old_data = load_all_table_data(cursor, last_table)

    print("📄 Loading current rankings...")
    new_data = load_all_table_data(cursor, current_table)

    print("📄 Computing rank changes...")

    # Prepare bulk updates and CSV data
    bulk_updates = []
    csv_data = []

    for key, new_info in new_data.items():
        appleid, country, category, subcategory = key
        new_rank = new_info["rank"]
        title = new_info["title"]
        country_name = new_info["country_name"]

        old_info = old_data.get(key)

        if old_info is None:
            old_rank = None
            movement = "NEW"
        else:
            old_rank = old_info["rank"]
            diff = old_rank - new_rank
            movement = f"+{diff}" if diff > 0 else str(diff) if diff < 0 else "0"

        # Prepare update tuple
        subcat_for_update = subcategory if subcategory else None
        bulk_updates.append(
            (old_rank, movement, appleid, country, category, subcat_for_update)
        )

        # Prepare CSV data
        csv_data.append(
            (
                appleid,
                title,
                country,
                country_name,
                category,
                subcategory if subcategory else None,
                new_rank,
                old_rank,
                movement,
            )
        )

    # OPTIMIZED: Single bulk update instead of many small ones
    if bulk_updates:
        print(f"📄 Updating {len(bulk_updates)} records...")
        update_sql = f"""
        UPDATE {current_table}
        SET old_rank=%s, movement=%s
        WHERE appleid=%s AND countryCode=%s AND category=%s
          AND ((subcategory IS NULL AND %s IS NULL) OR subcategory=%s)
        """

        # Process in chunks to avoid memory issues
        chunk_size = 1000
        for i in range(0, len(bulk_updates), chunk_size):
            chunk = bulk_updates[i : i + chunk_size]
            # Add subcategory twice for the SQL condition
            chunk_with_double_subcat = []
            for old_rank, movement, appleid, country, category, subcategory in chunk:
                chunk_with_double_subcat.append(
                    (
                        old_rank,
                        movement,
                        appleid,
                        country,
                        category,
                        subcategory,
                        subcategory,
                    )
                )

            cursor.executemany(update_sql, chunk_with_double_subcat)

            if i % 5000 == 0:
                print(
                    f"  ✅ Processed {min(i + chunk_size, len(bulk_updates))}/{len(bulk_updates)} updates"
                )

    return csv_data


# OPTIMIZED: Batch insert with executemany
def batch_insert_podcasts(cursor, insert_sql, batch_data, failed_inserts):
    """Insert podcast data in batches for better performance"""
    chunk_size = 500
    total_inserted = 0

    for i in range(0, len(batch_data), chunk_size):
        chunk = batch_data[i : i + chunk_size]
        try:
            cursor.executemany(insert_sql, chunk)
            total_inserted += len(chunk)

            if i % 2000 == 0 and i > 0:
                print(f"  ✅ Inserted {total_inserted} records...")

        except mysql.connector.Error as err:
            # Handle individual failures if needed
            print(f"  ⚠️ Batch insert failed: {err}")
            for single_record in chunk:
                try:
                    cursor.execute(insert_sql, single_record)
                    total_inserted += 1
                except mysql.connector.Error as single_err:
                    failed_inserts.append(f"Individual insert failed: {single_err}")

    return total_inserted


# ===============================
# MAIN
# ===============================
def main():
    # Fetch and build genres mapping
    open(os.path.join(OUTPUT_DIR, "lookup_missing_ids.txt"), "w").close()
    missing_ids_set = set()
    try:
        response = requests.get(GENRE_LOOKUP_URL, timeout=30)
        response.raise_for_status()
        genres_data = response.json()
    except Exception as e:
        print("❌ Failed to fetch genres:", e)
        return

    # Limit to Podcasts (26) and its tree; match testdb2's collect + sorting
    all_genres_info = collect_genre_info({"26": genres_data["26"]})

    # Connect to database with optimized settings
    db_config_optimized = DB_CONFIG.copy()
    db_config_optimized.update(
        {
            "autocommit": False,
            "use_unicode": True,
            "charset": "utf8mb4",
        }
    )

    db = mysql.connector.connect(**db_config_optimized)
    cursor = db.cursor(buffered=True)

    # Optimize MySQL session variables for bulk operations
    try:
        cursor.execute("SET SESSION bulk_insert_buffer_size = 67108864")  # 64MB
        cursor.execute("SET SESSION myisam_sort_buffer_size = 67108864")  # 64MB
        cursor.execute(
            "SET SESSION innodb_lock_wait_timeout = 120"
        )  # Increase lock timeout
        cursor.execute("SET SESSION max_heap_table_size = 134217728")  # 128MB
        cursor.execute("SET SESSION tmp_table_size = 134217728")  # 128MB
        print("✅ Database session optimized for bulk operations")
    except mysql.connector.Error as e:
        print(f"⚠️ Some database optimizations failed (this is usually OK): {e}")
        # Continue anyway - these are optimizations, not requirements

    # Get last table name for comparison
    last_table = get_last_table_name(cursor)

    # Create new table with timestamp
    now = datetime.datetime.now()
    current_table = f"apple_chart_{now.strftime('%Y%m%d_%H%M%S')}"
    create_new_table(cursor, current_table)

    print(f"📊 Created new table: {current_table}")
    if last_table:
        print(f"📊 Will compare with: {last_table}")

    insert_sql = f"""
    INSERT INTO {current_table}
    (chart_rank, podcast_url, appleid, title, img_url, countryCode, countryName, 
     category, subcategory, createdTime, updatedTime)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    all_apple_ids = set()
    failed_requests, failed_inserts = [], []
    batch_insert_data = []

    print("📄 Starting data collection...")

    for region in REGIONS:
        print(f"\n🌍 Processing region: {region.upper()}")
        genre_list = get_genres_for_region(all_genres_info, region)

        for idx, (genre_id, (category_name, subcategory_name)) in enumerate(
            genre_list, start=1
        ):
            display_name = subcategory_name if subcategory_name else category_name
            print(
                f"🔥 [{idx}/{len(genre_list)}] {region.upper()} | {genre_id} - {display_name}"
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
            meta_map = lookup_metadata(ids_ranked, region, missing_ids_set)

            # 3) Collect data for batch insert
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
                country_name = COUNTRY_NAMES.get(region, region.upper())

                # Collect data for batch insert
                batch_insert_data.append(
                    (
                        rank,
                        podcast_url,
                        str(aid),
                        title,
                        img_url,
                        region,
                        country_name,
                        category_name,
                        subcat_value,
                        now,
                        now,
                    )
                )

        # OPTIMIZED: Batch insert per region to avoid memory issues
        if batch_insert_data:
            print(
                f"  💾 Batch inserting {len(batch_insert_data)} records for {region.upper()}..."
            )
            batch_insert_podcasts(cursor, insert_sql, batch_insert_data, failed_inserts)
            batch_insert_data = []  # Clear for next region
            db.commit()  # Commit per region

    print("💾 Final commit...")
    db.commit()

    # ===============================
    # OPTIMIZED BULK RANK COMPARISON
    # ===============================
    print("\n📄 Starting optimized rank comparison...")
    if last_table and last_table != current_table:
        csv_data = bulk_rank_comparison(cursor, last_table, current_table)

        print("💾 Committing rank updates...")
        db.commit()

        # Write CSV file
        print(f"📝 Writing {len(csv_data)} records to CSV...")
        with open(CSV_OUTPUT, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "appleid",
                    "title",
                    "country_code",
                    "country_name",
                    "category",
                    "subcategory",
                    "current_rank",
                    "old_rank",
                    "movement",
                ]
            )
            writer.writerows(csv_data)

        print(f"✅ Optimized rank comparison saved to {CSV_OUTPUT}")
    else:
        print("⚠️ No previous table found — skipping rank comparison.")

    # Save other output files
    with open(os.path.join(OUTPUT_DIR, "lookup_missing_ids.txt"), "w") as f:
        for country, mid in sorted(missing_ids_set):
            f.write(f"{country},{mid}\n")
    with open(OUTPUT_IDS, "w") as f:
        for aid in sorted(all_apple_ids, key=lambda x: int(x)):
            f.write(aid + "\n")

    with open(FAILED_REQ_LOG, "w") as f:
        for region, gid, gname, err in failed_requests:
            f.write(f"{region.upper()} | {gid} - {gname} | {err}\n")

    with open(FAILED_INSERTS_LOG, "w") as f:
        f.write("\n".join(failed_inserts))

    cursor.close()
    db.close()
    print("\n✅ Done! Optimization complete.")


if __name__ == "__main__":
    main()
