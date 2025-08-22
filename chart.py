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
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# ===============================
# CONFIG
# ===============================
GENRE_LOOKUP_URL = "https://itunes.apple.com/WebObjects/MZStoreServices.woa/ws/genres"
CHARTS_URL_TPL = "https://itunes.apple.com/WebObjects/MZStoreServices.woa/ws/charts?cc={cc}&g={genre_id}&name=Podcasts&limit={limit}"
LOOKUP_URL_TPL = "https://itunes.apple.com/lookup?id={ids}&country={country}"

# Set this to 250 to fetch 250 ranks per genre (if available from charts API)
CHART_LIMIT = 250

# Enhanced retry configuration
MAX_RETRIES = 5
BACKOFF_FACTOR = 3
RETRY_STATUS_CODES = [429, 500, 502, 503, 504]
REQUEST_TIMEOUT = (10, 30)  # (connect timeout, read timeout)
MAX_CONSECUTIVE_FAILURES = 10

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "podcast_169",
}

OUTPUT_DIR = "./documents"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_IDS = os.path.join(OUTPUT_DIR, "apple_ids_from_all_genres_regions_charts.txt")
FAILED_REQ_LOG = os.path.join(OUTPUT_DIR, "failed_requests_charts.txt")
FAILED_INSERTS_LOG = os.path.join(OUTPUT_DIR, "failed_inserts_charts.txt")
CSV_OUTPUT = os.path.join(OUTPUT_DIR, "rank_comparison_charts.csv")

REGIONS = [
    "dz",
    "ao",
    "am",
    "az",
    "bh",
    "bj",
    "bw",
    "bn",
    "bf",
    "cm",
    "cv",
    "td",
    "ci",
    "cd",
    "eg",
    "sz",
    "ga",
    "gm",
    "gh",
    "gw",
    "in",
    "iq",
    "il",
    "jo",
    "ke",
    "kw",
    "lb",
    "lr",
    "ly",
    "mg",
    "mw",
    "ml",
    "mr",
    "mu",
    "ma",
    "mz",
    "na",
    "ne",
    "ng",
    "om",
    "qa",
    "cg",
    "rw",
    "sa",
    "sn",
    "sc",
    "sl",
    "za",
    "lk",
    "tj",
    "tz",
    "tn",
    "tm",
    "ae",
    "ug",
    "ye",
    "zm",
    "zw",
    "au",
    "bt",
    "kh",
    "cn",
    "fj",
    "hk",
    "id",
    "jp",
    "kz",
    "kr",
    "kg",
    "la",
    "mo",
    "my",
    "mv",
    "fm",
    "mn",
    "mm",
    "np",
    "nz",
    "pg",
    "ph",
    "sg",
    "sb",
    "tw",
    "th",
    "to",
    "uz",
    "vu",
    "vn",
    "at",
    "by",
    "be",
    "ba",
    "bg",
    "hr",
    "cy",
    "cz",
    "dk",
    "ee",
    "fi",
    "fr",
    "ge",
    "de",
    "gr",
    "hu",
    "is",
    "ie",
    "it",
    "xk",
    "lv",
    "lt",
    "lu",
    "mt",
    "md",
    "me",
    "nl",
    "mk",
    "no",
    "pl",
    "pt",
    "ro",
    "ru",
    "rs",
    "sk",
    "si",
    "es",
    "se",
    "ch",
    "tr",
    "ua",
    "gb",
    "ai",
    "ag",
    "ar",
    "bs",
    "bb",
    "bz",
    "bm",
    "bo",
    "br",
    "vg",
    "ky",
    "cl",
    "co",
    "cr",
    "dm",
    "do",
    "ec",
    "sv",
    "gd",
    "gt",
    "gy",
    "hn",
    "jm",
    "mx",
    "ms",
    "ni",
    "pa",
    "py",
    "pe",
    "kn",
    "lc",
    "vc",
    "sr",
    "tt",
    "tc",
    "uy",
    "ve",
    "ca",
    "us",
]

COUNTRY_NAMES = {
    # Africa / Middle East
    "dz": "Algeria",
    "ao": "Angola",
    "am": "Armenia",
    "az": "Azerbaijan",
    "bh": "Bahrain",
    "bj": "Benin",
    "bw": "Botswana",
    "bn": "Brunei Darussalam",
    "bf": "Burkina Faso",
    "cm": "Cameroon",
    "cv": "Cape Verde",
    "td": "Chad",
    "ci": "Côte d'Ivoire",
    "cd": "Democratic Republic of the Congo",
    "eg": "Egypt",
    "sz": "Eswatini",
    "ga": "Gabon",
    "gm": "Gambia",
    "gh": "Ghana",
    "gw": "Guinea-Bissau",
    "iq": "Iraq",
    "il": "Israel",
    "jo": "Jordan",
    "ke": "Kenya",
    "kw": "Kuwait",
    "lb": "Lebanon",
    "lr": "Liberia",
    "ly": "Libya",
    "mg": "Madagascar",
    "mw": "Malawi",
    "ml": "Mali",
    "mr": "Mauritania",
    "mu": "Mauritius",
    "ma": "Morocco",
    "mz": "Mozambique",
    "na": "Namibia",
    "ne": "Niger",
    "ng": "Nigeria",
    "om": "Oman",
    "qa": "Qatar",
    "cg": "Republic of the Congo",
    "rw": "Rwanda",
    "sa": "Saudi Arabia",
    "sn": "Senegal",
    "sc": "Seychelles",
    "sl": "Sierra Leone",
    "za": "South Africa",
    "lk": "Sri Lanka",
    "tj": "Tajikistan",
    "tz": "Tanzania",
    "tn": "Tunisia",
    "tm": "Turkmenistan",
    "ae": "United Arab Emirates",
    "ug": "Uganda",
    "ye": "Yemen",
    "zm": "Zambia",
    "zw": "Zimbabwe",
    # Asia-Pacific
    "au": "Australia",
    "bt": "Bhutan",
    "kh": "Cambodia",
    "cn": "China",
    "fj": "Fiji",
    "hk": "Hong Kong",
    "id": "Indonesia",
    "jp": "Japan",
    "kz": "Kazakhstan",
    "kr": "South Korea",
    "kg": "Kyrgyzstan",
    "la": "Laos",
    "mo": "Macau",
    "my": "Malaysia",
    "mv": "Maldives",
    "fm": "Micronesia",
    "mn": "Mongolia",
    "mm": "Myanmar (Burma)",
    "np": "Nepal",
    "nz": "New Zealand",
    "pg": "Papua New Guinea",
    "ph": "Philippines",
    "sg": "Singapore",
    "sb": "Solomon Islands",
    "tw": "Taiwan",
    "th": "Thailand",
    "to": "Tonga",
    "uz": "Uzbekistan",
    "vu": "Vanuatu",
    "vn": "Vietnam",
    # Europe
    "at": "Austria",
    "by": "Belarus",
    "be": "Belgium",
    "ba": "Bosnia and Herzegovina",
    "bg": "Bulgaria",
    "hr": "Croatia",
    "cy": "Cyprus",
    "cz": "Czech Republic",
    "dk": "Denmark",
    "ee": "Estonia",
    "fi": "Finland",
    "fr": "France",
    "ge": "Georgia",
    "de": "Germany",
    "gr": "Greece",
    "hu": "Hungary",
    "is": "Iceland",
    "ie": "Ireland",
    "it": "Italy",
    "xk": "Kosovo",
    "lv": "Latvia",
    "lt": "Lithuania",
    "lu": "Luxembourg",
    "mt": "Malta",
    "md": "Moldova",
    "me": "Montenegro",
    "nl": "Netherlands",
    "mk": "North Macedonia",
    "no": "Norway",
    "pl": "Poland",
    "pt": "Portugal",
    "ro": "Romania",
    "ru": "Russia",
    "rs": "Serbia",
    "sk": "Slovakia",
    "si": "Slovenia",
    "es": "Spain",
    "se": "Sweden",
    "ch": "Switzerland",
    "tr": "Türkiye",
    "ua": "Ukraine",
    "gb": "United Kingdom",
    # Americas / Caribbean
    "ai": "Anguilla",
    "ag": "Antigua and Barbuda",
    "ar": "Argentina",
    "bs": "Bahamas",
    "bb": "Barbados",
    "bz": "Belize",
    "bm": "Bermuda",
    "bo": "Bolivia",
    "br": "Brazil",
    "vg": "British Virgin Islands",
    "ky": "Cayman Islands",
    "cl": "Chile",
    "co": "Colombia",
    "cr": "Costa Rica",
    "dm": "Dominica",
    "do": "Dominican Republic",
    "ec": "Ecuador",
    "sv": "El Salvador",
    "gd": "Grenada",
    "gt": "Guatemala",
    "gy": "Guyana",
    "hn": "Honduras",
    "jm": "Jamaica",
    "mx": "Mexico",
    "ms": "Montserrat",
    "ni": "Nicaragua",
    "pa": "Panama",
    "py": "Paraguay",
    "pe": "Peru",
    "kn": "Saint Kitts and Nevis",
    "lc": "Saint Lucia",
    "vc": "Saint Vincent and the Grenadines",
    "sr": "Suriname",
    "tt": "Trinidad and Tobago",
    "tc": "Turks and Caicos Islands",
    "uy": "Uruguay",
    "ve": "Venezuela",
    "ca": "Canada",
    "us": "United States",
    # South Asia (extra from your original list)
    "in": "India",
}


def create_robust_session():
    """Create a requests session with robust retry strategy and connection pooling."""
    session = requests.Session()

    # Configure retry strategy
    retry_strategy = Retry(
        total=MAX_RETRIES,
        status_forcelist=RETRY_STATUS_CODES,
        allowed_methods=["HEAD", "GET", "OPTIONS"],  # Updated parameter name
        backoff_factor=BACKOFF_FACTOR,
        raise_on_status=False,  # Don't raise exception on retry-able status codes
    )

    # Configure HTTP adapter with retry and connection pooling
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=20,
        pool_maxsize=50,
        pool_block=False,
    )

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Set default headers to appear more like a regular browser
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://itunes.apple.com/",
            "Origin": "https://itunes.apple.com",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
    )

    return session


def safe_request_with_backoff(session, url, context="", max_attempts=None):
    """
    Make a safe HTTP request with exponential backoff and comprehensive error handling.
    """
    if max_attempts is None:
        max_attempts = MAX_RETRIES

    last_exception = None

    for attempt in range(max_attempts):
        try:
            # Add jitter to prevent thundering herd
            if attempt > 0:
                jitter = random.uniform(0.5, 1.5)
                sleep_time = (BACKOFF_FACTOR**attempt) * jitter * 3
                print(
                    f"    💤 Retry {attempt + 1}/{max_attempts} for {context} - sleeping {sleep_time:.1f}s"
                )
                time.sleep(sleep_time)

            response = session.get(url, timeout=REQUEST_TIMEOUT)

            # Handle different response codes
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError as e:
                    raise requests.exceptions.JSONDecodeError(
                        f"Invalid JSON response: {e}"
                    )

            elif response.status_code == 429:
                # Rate limited - longer backoff
                retry_after = int(response.headers.get("Retry-After", 60))
                print(f"    🚦 Rate limited for {context} - waiting {retry_after}s")
                time.sleep(retry_after)
                continue

            elif response.status_code in [500, 502, 503, 504]:
                # Server errors - retry with backoff
                print(f"    ⚠️ Server error {response.status_code} for {context}")
                last_exception = requests.exceptions.HTTPError(
                    f"HTTP {response.status_code}"
                )
                continue

            elif response.status_code == 404:
                # Apple sometimes sends 404 instead of 429 (soft block)
                print(
                    f"    ⚠️ Got 404 for {context}, treating as transient error (possible throttling)"
                )
                last_exception = requests.exceptions.HTTPError("HTTP 404 (transient)")
                continue

            elif response.status_code in [400, 401, 403]:
                # Client errors - don't retry
                raise requests.exceptions.HTTPError(
                    f"HTTP {response.status_code}: {response.text[:200]}"
                )

            else:
                # Other status codes
                print(f"    ❓ Unexpected status {response.status_code} for {context}")
                last_exception = requests.exceptions.HTTPError(
                    f"HTTP {response.status_code}"
                )
                continue

        except requests.exceptions.Timeout as e:
            print(f"    ⏱️ Timeout for {context} (attempt {attempt + 1}/{max_attempts})")
            last_exception = e

        except requests.exceptions.ConnectionError as e:
            print(
                f"    🔌 Connection error for {context} (attempt {attempt + 1}/{max_attempts}): {str(e)[:100]}"
            )
            last_exception = e

        except requests.exceptions.HTTPError as e:
            print(
                f"    🚫 HTTP error for {context} (attempt {attempt + 1}/{max_attempts}): {e}"
            )
            last_exception = e

        except requests.exceptions.RequestException as e:
            print(
                f"    ❌ Request error for {context} (attempt {attempt + 1}/{max_attempts}): {e}"
            )
            last_exception = e

        except Exception as e:
            print(
                f"    💥 Unexpected error for {context} (attempt {attempt + 1}/{max_attempts}): {e}"
            )
            last_exception = e

    # All attempts failed
    print(f"    ❌ All {max_attempts} attempts failed for {context}")
    if last_exception:
        raise last_exception
    else:
        raise requests.exceptions.RequestException(
            f"All {max_attempts} attempts failed for {context}"
        )


def collect_genre_info(
    genre_dict, parent_name=None, depth=0
) -> Dict[int, Tuple[str, str]]:
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


def fetch_chart_ids(session, region: str, genre_id: int, limit: int) -> List[int]:
    """Call ws/charts and return the ranked Apple IDs for the given genre."""
    url = CHARTS_URL_TPL.format(cc=region, genre_id=genre_id, limit=limit)
    context = f"charts {region.upper()}/{genre_id}"

    try:
        data = safe_request_with_backoff(session, url, context)
        if data is None:
            return []

        ids = data.get("resultIds", [])
        return [int(x) for x in ids if str(x).isdigit()]

    except Exception as e:
        print(f"    ❌ Charts fetch failed for {context}: {e}")
        return []


def batched(iterable, n):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == n:
            yield batch
            batch = []
    if batch:
        yield batch


def lookup_metadata(
    session, apple_ids: List[int], country: str, missing_ids_set: set
) -> Dict[int, dict]:
    out = {}
    consecutive_failures = 0

    for chunk in batched(apple_ids, 100):
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            print(
                f"    ⚠️ Too many consecutive failures for {country} - skipping remaining lookups"
            )
            break

        ids_str = ",".join(str(x) for x in chunk)
        url = LOOKUP_URL_TPL.format(ids=ids_str, country=country)
        context = f"lookup {country}/{len(chunk)} IDs"

        try:
            data = safe_request_with_backoff(session, url, context)
            if data is None:
                consecutive_failures += 1
                continue

            # Reset failure counter on success
            consecutive_failures = 0

            found_ids = set()
            for item in data.get("results", []):
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
                    missing_ids_set.add((country, aid))

        except Exception as e:
            consecutive_failures += 1
            print(f"    ❌ Lookup failed for {context}: {e}")

            # Add small delay after failures to avoid overwhelming the server
            time.sleep(1)

    return out


def get_last_table_name(cursor):
    """Get the most recent chart table name"""
    cursor.execute("""
        SELECT TABLE_NAME 
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = 'podcast_169' 
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
            appleid BIGINT UNSIGNED DEFAULT NULL,
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
    print("📊 Loading previous rankings...")
    old_data = load_all_table_data(cursor, last_table)

    print("📊 Loading current rankings...")
    new_data = load_all_table_data(cursor, current_table)

    print("📊 Computing rank changes...")

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
        print(f"📊 Updating {len(bulk_updates)} records...")
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
    print(
        "🚀 Starting enhanced podcast charts collection with robust error handling..."
    )

    # Create robust HTTP session
    session = create_robust_session()

    # Fetch and build genres mapping
    open(os.path.join(OUTPUT_DIR, "lookup_missing_ids.txt"), "w").close()
    missing_ids_set = set()

    try:
        print("📡 Fetching genres...")
        data = safe_request_with_backoff(session, GENRE_LOOKUP_URL, "genres fetch")
        if not data:
            print("❌ Failed to fetch genres - exiting")
            return
        genres_data = data
    except Exception as e:
        print(f"❌ Failed to fetch genres: {e}")
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

    try:
        db = mysql.connector.connect(**db_config_optimized)
        cursor = db.cursor(buffered=True)
    except mysql.connector.Error as e:
        print(f"❌ Database connection failed: {e}")
        return

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

    try:
        create_new_table(cursor, current_table)
        print(f"📊 Created new table: {current_table}")
        if last_table:
            print(f"📊 Will compare with: {last_table}")
    except mysql.connector.Error as e:
        print(f"❌ Failed to create table: {e}")
        cursor.close()
        db.close()
        return

    insert_sql = f"""
    INSERT INTO {current_table}
    (chart_rank, podcast_url, appleid, title, img_url, countryCode, countryName, 
     category, subcategory, createdTime, updatedTime)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    all_apple_ids = set()
    failed_requests, failed_inserts = [], []
    batch_insert_data = []

    print("📊 Starting data collection...")

    total_regions = len(REGIONS)
    for region_idx, region in enumerate(REGIONS, 1):
        print(
            f"\n🌍 Processing region: {region.upper()} ({region_idx}/{total_regions})"
        )
        genre_list = get_genres_for_region(all_genres_info, region)

        for idx, (genre_id, (category_name, subcategory_name)) in enumerate(
            genre_list, start=1
        ):
            display_name = subcategory_name if subcategory_name else category_name
            print(
                f"  📥 [{idx}/{len(genre_list)}] {region.upper()} | {genre_id} - {display_name}"
            )

            # 1) Get ranked IDs from charts (up to CHART_LIMIT)
            try:
                ids_ranked = fetch_chart_ids(session, region, genre_id, CHART_LIMIT)
            except Exception as e:
                print(f"    ❌ Charts error: {e}")
                failed_requests.append((region, genre_id, display_name, str(e)))
                continue

            if not ids_ranked:
                print(f"    ⚠️ No chart IDs found")
                continue

            print(f"    📊 Found {len(ids_ranked)} chart entries")

            # 2) Enrich with Lookup metadata in batches
            try:
                meta_map = lookup_metadata(session, ids_ranked, region, missing_ids_set)
                print(f"    ✅ Retrieved metadata for {len(meta_map)} podcasts")
            except Exception as e:
                print(f"    ❌ Metadata lookup error: {e}")
                meta_map = {}

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
                        int(aid),
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
            try:
                batch_insert_podcasts(
                    cursor, insert_sql, batch_insert_data, failed_inserts
                )
                batch_insert_data = []  # Clear for next region
                db.commit()  # Commit per region
                print(f"  ✅ Successfully committed data for {region.upper()}")
            except mysql.connector.Error as e:
                print(f"  ❌ Database error for {region.upper()}: {e}")
                db.rollback()
                failed_inserts.append(f"Region {region.upper()} batch failed: {e}")
            except Exception as e:
                print(f"  ❌ Unexpected error for {region.upper()}: {e}")
                db.rollback()
                failed_inserts.append(f"Region {region.upper()} unexpected error: {e}")

    print("💾 Final commit...")
    try:
        db.commit()
        print("✅ Final commit successful")
    except mysql.connector.Error as e:
        print(f"❌ Final commit failed: {e}")
        db.rollback()

    # ===============================
    # OPTIMIZED BULK RANK COMPARISON
    # ===============================
    print("\n📊 Starting optimized rank comparison...")
    if last_table and last_table != current_table:
        try:
            csv_data = bulk_rank_comparison(cursor, last_table, current_table)

            print("💾 Committing rank updates...")
            db.commit()
            print("✅ Rank updates committed")

            # Write CSV file
            print(f"📝 Writing {len(csv_data)} records to CSV...")
            try:
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
            except Exception as e:
                print(f"❌ Failed to write CSV: {e}")
                failed_inserts.append(f"CSV write failed: {e}")

        except Exception as e:
            print(f"❌ Rank comparison failed: {e}")
            failed_inserts.append(f"Rank comparison failed: {e}")
    else:
        print("⚠️ No previous table found — skipping rank comparison.")

    # Save other output files
    try:
        print("📝 Writing output files...")

        with open(os.path.join(OUTPUT_DIR, "lookup_missing_ids.txt"), "w") as f:
            for country, mid in sorted(missing_ids_set):
                f.write(f"{country},{mid}\n")
        print(f"✅ Written {len(missing_ids_set)} missing IDs")

        with open(OUTPUT_IDS, "w") as f:
            for aid in sorted(all_apple_ids, key=lambda x: int(x)):
                f.write(aid + "\n")
        print(f"✅ Written {len(all_apple_ids)} Apple IDs")

        with open(FAILED_REQ_LOG, "w") as f:
            for region, gid, gname, err in failed_requests:
                f.write(f"{region.upper()} | {gid} - {gname} | {err}\n")
        print(f"✅ Written {len(failed_requests)} failed requests")

        with open(FAILED_INSERTS_LOG, "w") as f:
            f.write("\n".join(failed_inserts))
        print(f"✅ Written {len(failed_inserts)} failed inserts")

    except Exception as e:
        print(f"❌ Failed to write some output files: {e}")

    # Clean up
    try:
        cursor.close()
        db.close()
        session.close()
        print("✅ Cleaned up connections")
    except Exception as e:
        print(f"⚠️ Cleanup warning: {e}")

    # Print summary
    print(f"\n📈 SUMMARY:")
    print(f"  • Total Apple IDs collected: {len(all_apple_ids)}")
    print(f"  • Failed requests: {len(failed_requests)}")
    print(f"  • Failed inserts: {len(failed_inserts)}")
    print(f"  • Missing lookup IDs: {len(missing_ids_set)}")
    print(f"  • Created table: {current_table}")

    if failed_requests:
        print(f"\n⚠️ FAILED REQUESTS BY REGION:")
        failed_by_region = {}
        for region, gid, gname, err in failed_requests:
            failed_by_region[region] = failed_by_region.get(region, 0) + 1
        for region, count in sorted(failed_by_region.items()):
            print(f"  • {region.upper()}: {count} failures")

    print("\n✅ Enhanced collection complete with robust error handling!")


if __name__ == "__main__":
    main()
