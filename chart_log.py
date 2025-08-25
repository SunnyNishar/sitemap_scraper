#!/usr/bin/env python3
import requests
import mysql.connector
import datetime
import os
import csv
import time
import random
import logging
import sys
from typing import Dict, List, Tuple
from collections import defaultdict
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


# ===============================
# LOGGING SETUP
# ===============================
def setup_logging():
    """Set up comprehensive logging system (append to same daily log)."""
    # Create logs directory if it doesn't exist
    log_dir = "./logs"
    os.makedirs(log_dir, exist_ok=True)

    # Use only date so reruns/crashes on same day append to the same files
    run_date = datetime.datetime.now().strftime("%Y%m%d")
    log_file = os.path.join(log_dir, f"podcast_charts_{run_date}.log")
    error_log_file = os.path.join(log_dir, f"podcast_charts_errors_{run_date}.log")

    # Formatters
    detailed_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(funcName)-20s | Line %(lineno)-4d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    simple_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S"
    )

    # Root logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # IMPORTANT: clear existing handlers to prevent duplicate lines on rerun/import
    for h in list(logger.handlers):
        logger.removeHandler(h)

    # File handler (daily, append)
    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    logger.addHandler(console_handler)

    # Error file handler (daily, append)
    error_handler = logging.FileHandler(error_log_file, mode="a", encoding="utf-8")
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(detailed_formatter)
    logger.addHandler(error_handler)

    return logger, log_file


# Initialize logging
logger, main_log_file = setup_logging()

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
    "in": "India",
}


def create_robust_session():
    """Create a requests session with robust retry strategy and connection pooling."""
    logger.info("Creating robust HTTP session")

    session = requests.Session()

    # Configure retry strategy
    retry_strategy = Retry(
        total=MAX_RETRIES,
        status_forcelist=RETRY_STATUS_CODES,
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        backoff_factor=BACKOFF_FACTOR,
        raise_on_status=False,
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

    logger.debug(f"HTTP session configured with {MAX_RETRIES} max retries")
    return session


def safe_request_with_backoff(session, url, context="", max_attempts=None):
    """Make a safe HTTP request with exponential backoff and comprehensive error handling."""
    if max_attempts is None:
        max_attempts = MAX_RETRIES

    logger.debug(f"Starting request for {context}: {url}")
    last_exception = None

    for attempt in range(max_attempts):
        try:
            # Add jitter to prevent thundering herd
            if attempt > 0:
                jitter = random.uniform(0.5, 1.5)
                sleep_time = (BACKOFF_FACTOR**attempt) * jitter * 3
                logger.warning(
                    f"Retry {attempt + 1}/{max_attempts} for {context} - sleeping {sleep_time:.1f}s"
                )
                time.sleep(sleep_time)

            response = session.get(url, timeout=REQUEST_TIMEOUT)
            logger.debug(f"Got response {response.status_code} for {context}")

            # Handle different response codes
            if response.status_code == 200:
                try:
                    data = response.json()
                    logger.debug(f"Successfully parsed JSON response for {context}")
                    return data
                except ValueError as e:
                    logger.error(f"Invalid JSON response for {context}: {e}")
                    raise requests.exceptions.JSONDecodeError(
                        f"Invalid JSON response: {e}"
                    )

            elif response.status_code == 429:
                # Rate limited - longer backoff
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited for {context} - waiting {retry_after}s")
                time.sleep(retry_after)
                continue

            elif response.status_code in [500, 502, 503, 504]:
                # Server errors - retry with backoff
                logger.warning(f"Server error {response.status_code} for {context}")
                last_exception = requests.exceptions.HTTPError(
                    f"HTTP {response.status_code}"
                )
                continue

            elif response.status_code == 404:
                # Apple sometimes sends 404 instead of 429 (soft block)
                logger.warning(
                    f"Got 404 for {context}, treating as transient error (possible throttling)"
                )
                last_exception = requests.exceptions.HTTPError("HTTP 404 (transient)")
                continue

            elif response.status_code in [400, 401, 403]:
                # Client errors - don't retry
                logger.error(
                    f"Client error {response.status_code} for {context}: {response.text[:200]}"
                )
                raise requests.exceptions.HTTPError(
                    f"HTTP {response.status_code}: {response.text[:200]}"
                )

            else:
                # Other status codes
                logger.warning(
                    f"Unexpected status {response.status_code} for {context}"
                )
                last_exception = requests.exceptions.HTTPError(
                    f"HTTP {response.status_code}"
                )
                continue

        except requests.exceptions.Timeout as e:
            logger.warning(
                f"Timeout for {context} (attempt {attempt + 1}/{max_attempts})"
            )
            last_exception = e

        except requests.exceptions.ConnectionError as e:
            logger.warning(
                f"Connection error for {context} (attempt {attempt + 1}/{max_attempts}): {str(e)[:100]}"
            )
            last_exception = e

        except requests.exceptions.HTTPError as e:
            logger.error(
                f"HTTP error for {context} (attempt {attempt + 1}/{max_attempts}): {e}"
            )
            last_exception = e

        except requests.exceptions.RequestException as e:
            logger.error(
                f"Request error for {context} (attempt {attempt + 1}/{max_attempts}): {e}"
            )
            last_exception = e

        except Exception as e:
            logger.error(
                f"Unexpected error for {context} (attempt {attempt + 1}/{max_attempts}): {e}"
            )
            last_exception = e

    # All attempts failed
    logger.error(f"All {max_attempts} attempts failed for {context}")
    if last_exception:
        raise last_exception
    else:
        raise requests.exceptions.RequestException(
            f"All {max_attempts} attempts failed for {context}"
        )


def collect_genre_info(
    genre_dict, parent_name=None, depth=0
) -> Dict[int, Tuple[str, str]]:
    """Collect genre information recursively"""
    logger.debug(f"Collecting genre info at depth {depth}, parent: {parent_name}")
    ids = {}

    for genre_id, genre_info in genre_dict.items():
        name = genre_info.get("name", "")
        logger.debug(f"Processing genre {genre_id}: {name}")

        if depth == 0:
            ids[int(genre_id)] = (name, None)  # genre 26 itself
        elif depth == 1:
            ids[int(genre_id)] = (name, None)  # main category
        else:
            ids[int(genre_id)] = (parent_name, name)  # subcategory

        subgenres = genre_info.get("subgenres", {})
        if subgenres:
            logger.debug(f"Found {len(subgenres)} subgenres for {name}")
            ids.update(collect_genre_info(subgenres, name, depth + 1))

    logger.info(f"Collected {len(ids)} genres at depth {depth}")
    return ids


def sort_genres_with_podcasts_first(genres_list):
    """Sorts genres so 'Podcasts' (id 26) comes first, rest by genre_id order."""
    sorted_list = sorted(genres_list, key=lambda x: (0 if x[0] == 26 else 1, x[0]))
    logger.debug(f"Sorted {len(sorted_list)} genres with Podcasts first")
    return sorted_list


def get_genres_for_region(all_genres_info, region):
    """Get genre list for a region. Always include genre 26 (Podcasts) for all regions."""
    genre_list = sort_genres_with_podcasts_first(list(all_genres_info.items()))
    logger.debug(f"Generated {len(genre_list)} genres for region {region}")
    return genre_list


def fetch_chart_ids(session, region: str, genre_id: int, limit: int) -> List[int]:
    """Call ws/charts and return the ranked Apple IDs for the given genre."""
    url = CHARTS_URL_TPL.format(cc=region, genre_id=genre_id, limit=limit)
    context = f"charts {region.upper()}/{genre_id}"

    logger.debug(f"Fetching chart IDs for {context}")

    try:
        data = safe_request_with_backoff(session, url, context)
        if data is None:
            logger.warning(f"No data returned for {context}")
            return []

        ids = data.get("resultIds", [])
        valid_ids = [int(x) for x in ids if str(x).isdigit()]

        logger.info(f"Retrieved {len(valid_ids)} chart IDs for {context}")
        return valid_ids

    except Exception as e:
        logger.error(f"Charts fetch failed for {context}: {e}")
        return []


def batched(iterable, n):
    """Split iterable into batches of size n"""
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
    """Lookup metadata for Apple IDs in batches"""
    logger.debug(f"Looking up metadata for {len(apple_ids)} IDs in {country}")

    out = {}
    consecutive_failures = 0

    for chunk_idx, chunk in enumerate(batched(apple_ids, 100)):
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            logger.error(
                f"Too many consecutive failures for {country} - skipping remaining lookups"
            )
            break

        logger.debug(
            f"Processing chunk {chunk_idx + 1} with {len(chunk)} IDs for {country}"
        )

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
            results = data.get("results", [])
            logger.debug(f"Got {len(results)} results for {context}")

            for item in results:
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
                    logger.debug(f"Missing metadata for ID {aid} in {country}")

            logger.info(
                f"Successfully retrieved metadata for {len(found_ids)}/{len(chunk)} IDs"
            )

        except Exception as e:
            consecutive_failures += 1
            logger.error(f"Lookup failed for {context}: {e}")
            time.sleep(1)  # Add small delay after failures

    logger.info(f"Total metadata retrieved for {country}: {len(out)} items")
    return out


def get_last_table_name(cursor):
    """Get the most recent chart table name"""
    logger.debug("Fetching last table name")

    cursor.execute("""
        SELECT TABLE_NAME 
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = 'podcast_169' 
        AND TABLE_NAME LIKE 'apple_chart_%'
        ORDER BY TABLE_NAME DESC 
        LIMIT 1
    """)
    result = cursor.fetchone()

    if result:
        logger.info(f"Found last table: {result[0]}")
        return result[0]
    else:
        logger.info("No previous table found")
        return None


def get_previous_table_name(cursor, exclude_table):
    """Get the previous chart table name excluding a specific table (used when resuming)."""
    logger.debug(f"Fetching previous table name excluding {exclude_table}")

    cursor.execute(
        """
        SELECT TABLE_NAME 
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME LIKE 'apple_chart_%%'
          AND TABLE_NAME <> %s
        ORDER BY TABLE_NAME DESC
        LIMIT 1
    """,
        ("podcast_169", exclude_table),
    )
    row = cursor.fetchone()

    if row:
        logger.info(f"Found previous table: {row[0]}")
        return row[0]
    else:
        logger.info("No previous table found")
        return None


def create_new_table(cursor, table_name):
    """Create a new chart table with timestamp and optimized indexes"""
    logger.info(f"Creating new table: {table_name}")

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

    logger.info(f"Successfully created table {table_name}")


def load_all_table_data(cursor, table):
    """Load all data from table at once and return as dictionary"""
    logger.info(f"Loading all data from table {table}")

    cursor.execute(f"""
        SELECT appleid, chart_rank, title, countryCode, countryName, category, 
               COALESCE(subcategory, '') as subcategory
        FROM {table}
    """)

    data = defaultdict(dict)
    row_count = 0

    for row in cursor.fetchall():
        appleid, rank, title, country, country_name, category, subcategory = row
        key = (appleid, country, category, subcategory)
        data[key] = {"rank": rank, "title": title, "country_name": country_name}
        row_count += 1

    logger.info(f"Loaded {row_count} records from {table}")
    return data


def bulk_rank_comparison(cursor, last_table, current_table):
    """Compare all ranks at once using bulk operations"""
    logger.info("Starting bulk rank comparison")

    logger.info("Loading previous rankings...")
    old_data = load_all_table_data(cursor, last_table)

    logger.info("Loading current rankings...")
    new_data = load_all_table_data(cursor, current_table)

    logger.info("Computing rank changes...")

    # Prepare bulk updates and CSV data
    bulk_updates = []
    csv_data = []
    stats = {"new": 0, "up": 0, "down": 0, "same": 0}

    for key, new_info in new_data.items():
        appleid, country, category, subcategory = key
        new_rank = new_info["rank"]
        title = new_info["title"]
        country_name = new_info["country_name"]

        old_info = old_data.get(key)

        if old_info is None:
            old_rank = None
            movement = "NEW"
            stats["new"] += 1
        else:
            old_rank = old_info["rank"]
            diff = old_rank - new_rank
            if diff > 0:
                movement = f"+{diff}"
                stats["up"] += 1
            elif diff < 0:
                movement = str(diff)
                stats["down"] += 1
            else:
                movement = "0"
                stats["same"] += 1

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

    logger.info(
        f"Rank changes computed: NEW={stats['new']}, UP={stats['up']}, DOWN={stats['down']}, SAME={stats['same']}"
    )

    # OPTIMIZED: Single bulk update instead of many small ones
    if bulk_updates:
        logger.info(f"Updating {len(bulk_updates)} records...")

        update_sql = f"""
        UPDATE {current_table}
        SET old_rank=%s, movement=%s
        WHERE appleid=%s AND countryCode=%s AND category=%s
          AND ((subcategory IS NULL AND %s IS NULL) OR subcategory=%s)
        """

        # Process in chunks to avoid memory issues
        chunk_size = 1000
        total_updated = 0

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
            total_updated += len(chunk)

            if i % 5000 == 0:
                logger.info(
                    f"Processed {min(i + chunk_size, len(bulk_updates))}/{len(bulk_updates)} updates"
                )

        logger.info(f"Successfully updated {total_updated} records")

    return csv_data


def batch_insert_podcasts(cursor, insert_sql, batch_data, failed_inserts):
    """Insert podcast data in batches for better performance"""
    logger.info(f"Starting batch insert of {len(batch_data)} records")

    chunk_size = 500
    total_inserted = 0

    for i in range(0, len(batch_data), chunk_size):
        chunk = batch_data[i : i + chunk_size]
        try:
            cursor.executemany(insert_sql, chunk)
            total_inserted += len(chunk)

            if i % 2000 == 0 and i > 0:
                logger.info(f"Inserted {total_inserted} records so far...")

        except mysql.connector.Error as err:
            logger.error(f"Batch insert failed: {err}")
            # Handle individual failures if needed
            for single_record in chunk:
                try:
                    cursor.execute(insert_sql, single_record)
                    total_inserted += 1
                except mysql.connector.Error as single_err:
                    logger.error(f"Individual insert failed: {single_err}")
                    failed_inserts.append(f"Individual insert failed: {single_err}")

    logger.info(f"Batch insert completed: {total_inserted} records inserted")
    return total_inserted


# ===============================
# MAIN
# ===============================
def main():
    start_time = datetime.datetime.now()
    logger.info("=" * 80)
    logger.info(
        "Starting enhanced podcast charts collection with resumption capability"
    )
    logger.info(f"Start time: {start_time}")
    logger.info(f"Log file: {main_log_file}")
    logger.info("=" * 80)

    try:
        # Create robust HTTP session
        logger.info("Initializing HTTP session")
        session = create_robust_session()

        # Fetch and build genres mapping
        logger.info("Preparing output files")
        open(os.path.join(OUTPUT_DIR, "lookup_missing_ids.txt"), "w").close()
        missing_ids_set = set()

        try:
            logger.info("Fetching genres from iTunes API")
            data = safe_request_with_backoff(session, GENRE_LOOKUP_URL, "genres fetch")
            if not data:
                logger.error("Failed to fetch genres - exiting")
                return
            genres_data = data
            logger.info("Successfully fetched genres data")
        except Exception as e:
            logger.error(f"Failed to fetch genres: {e}")
            return

        # Limit to Podcasts (26) and its tree
        logger.info("Processing genre information")
        all_genres_info = collect_genre_info({"26": genres_data["26"]})
        logger.info(f"Processed {len(all_genres_info)} total genres")

        # Connect to database with optimized settings
        logger.info("Connecting to database")
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
            logger.info("Successfully connected to database")
        except mysql.connector.Error as e:
            logger.error(f"Database connection failed: {e}")
            return

        # Optimize MySQL session variables for bulk operations
        try:
            logger.info("Optimizing database session")
            cursor.execute("SET SESSION bulk_insert_buffer_size = 67108864")  # 64MB
            cursor.execute("SET SESSION myisam_sort_buffer_size = 67108864")  # 64MB
            cursor.execute(
                "SET SESSION innodb_lock_wait_timeout = 120"
            )  # Increase lock timeout
            cursor.execute("SET SESSION max_heap_table_size = 134217728")  # 128MB
            cursor.execute("SET SESSION tmp_table_size = 134217728")  # 128MB
            logger.info("Database session optimized for bulk operations")
        except mysql.connector.Error as e:
            logger.warning(
                f"Some database optimizations failed (this is usually OK): {e}"
            )

        # ===============================
        # TABLE RESUMPTION LOGIC
        # ===============================
        logger.info("Checking for existing tables and resumption possibility")

        last_table = get_last_table_name(cursor)  # most recent table (could be today's)
        now = datetime.datetime.now()
        today_str = now.strftime("%Y%m%d")

        reuse_existing = False
        current_table = None
        previous_table = None  # the baseline to compare against (yesterday)

        if last_table and last_table.startswith(f"apple_chart_{today_str}"):
            # Check if today's table is complete
            cursor.execute(f"SELECT COUNT(DISTINCT countryCode) FROM {last_table}")
            completed_regions = cursor.fetchone()[0]
            if completed_regions == len(REGIONS):
                logger.info(
                    f"Today's table {last_table} already exists and is complete. Exiting."
                )
                cursor.close()
                db.close()
                session.close()
                return  # <-- End the script immediately
            else:
                # If incomplete, still allow resumption
                reuse_existing = True
                current_table = last_table
                previous_table = get_previous_table_name(
                    cursor, exclude_table=current_table
                )
                logger.info(
                    f"Resuming into existing table: {current_table} "
                    f"(completed {completed_regions}/{len(REGIONS)} regions)"
                )

        if not reuse_existing:
            current_table = f"apple_chart_{now.strftime('%Y%m%d_%H%M%S')}"
            try:
                create_new_table(cursor, current_table)
                previous_table = last_table  # yesterday's (or None if first run)
                logger.info(f"Created new table: {current_table}")
                if previous_table:
                    logger.info(f"Will compare with: {previous_table}")
            except mysql.connector.Error as e:
                logger.error(f"Failed to create table: {e}")
                cursor.close()
                db.close()
                return

        db.commit()

        insert_sql = f"""
        INSERT INTO {current_table}
        (chart_rank, podcast_url, appleid, title, img_url, countryCode, countryName, 
         category, subcategory, createdTime, updatedTime)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        all_apple_ids = set()
        failed_requests, failed_inserts = [], []
        batch_insert_data = []

        # Statistics tracking
        total_charts_fetched = 0
        total_metadata_lookups = 0
        total_records_inserted = 0

        logger.info("Starting data collection phase")

        # ===============================
        # RESUME FROM LAST COMPLETED REGION
        # ===============================
        cursor.execute(f"SELECT DISTINCT countryCode FROM {current_table}")
        done_regions = {row[0] for row in cursor.fetchall()}
        start_index = 0

        if done_regions:
            last_done = None
            for r in REGIONS:  # walk in processing order
                if r in done_regions:
                    last_done = r
            if last_done:
                # wipe last region to guard against partial work before crash
                logger.info(f"Cleaning partial data for last region: {last_done}")
                cursor.execute(
                    f"DELETE FROM {current_table} WHERE countryCode = %s", (last_done,)
                )
                db.commit()
                start_index = REGIONS.index(last_done)
                logger.info(
                    f"Resuming from region {last_done.upper()} (wiped its old rows)"
                )

        total_regions = len(REGIONS)
        logger.info(
            f"Processing {len(REGIONS[start_index:])} regions starting from index {start_index}"
        )

        for region_idx, region in enumerate(
            REGIONS[start_index:], start=start_index + 1
        ):
            logger.info(
                f"Processing region: {region.upper()} ({region_idx}/{total_regions})"
            )
            genre_list = get_genres_for_region(all_genres_info, region)

            region_charts = 0
            region_metadata = 0
            region_records = 0

            for idx, (genre_id, (category_name, subcategory_name)) in enumerate(
                genre_list, start=1
            ):
                display_name = subcategory_name if subcategory_name else category_name
                logger.debug(
                    f"[{idx}/{len(genre_list)}] {region.upper()} | {genre_id} - {display_name}"
                )

                # 1) Get ranked IDs from charts
                try:
                    ids_ranked = fetch_chart_ids(session, region, genre_id, CHART_LIMIT)
                    region_charts += len(ids_ranked)
                    total_charts_fetched += len(ids_ranked)
                except Exception as e:
                    logger.error(f"Charts error for {region}/{genre_id}: {e}")
                    failed_requests.append((region, genre_id, display_name, str(e)))
                    continue

                if not ids_ranked:
                    logger.debug(f"No chart IDs found for {region}/{genre_id}")
                    continue

                logger.debug(
                    f"Found {len(ids_ranked)} chart entries for {display_name}"
                )

                # 2) Enrich with Lookup metadata in batches
                try:
                    meta_map = lookup_metadata(
                        session, ids_ranked, region, missing_ids_set
                    )
                    region_metadata += len(meta_map)
                    total_metadata_lookups += len(meta_map)
                    logger.debug(f"Retrieved metadata for {len(meta_map)} podcasts")
                except Exception as e:
                    logger.error(f"Metadata lookup error for {region}/{genre_id}: {e}")
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
                    region_records += 1

            # Batch insert per region to avoid memory issues (with idempotent logic)
            if batch_insert_data:
                logger.info(
                    f"Batch inserting {len(batch_insert_data)} records for {region.upper()}"
                )
                try:
                    # Clear any existing data for this region first (idempotent)
                    cursor.execute(
                        f"DELETE FROM {current_table} WHERE countryCode = %s", (region,)
                    )

                    inserted = batch_insert_podcasts(
                        cursor, insert_sql, batch_insert_data, failed_inserts
                    )
                    total_records_inserted += inserted
                    batch_insert_data = []  # Clear for next region
                    db.commit()  # Commit per region
                    logger.info(
                        f"Successfully committed {inserted} records for {region.upper()}"
                    )
                except mysql.connector.Error as e:
                    logger.error(f"Database error for {region.upper()}: {e}")
                    db.rollback()
                    failed_inserts.append(f"Region {region.upper()} batch failed: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error for {region.upper()}: {e}")
                    db.rollback()
                    failed_inserts.append(
                        f"Region {region.upper()} unexpected error: {e}"
                    )

            logger.info(
                f"Region {region.upper()} completed: {region_charts} charts, {region_metadata} metadata, {region_records} records"
            )

        logger.info("Performing final commit")
        try:
            db.commit()
            logger.info("Final commit successful")
        except mysql.connector.Error as e:
            logger.error(f"Final commit failed: {e}")
            db.rollback()

        # ===============================
        # CONDITIONAL RANK COMPARISON
        # ===============================
        logger.info("Checking if rank comparison should be performed")

        # Only run rank comparison if table is complete
        cursor.execute(f"SELECT COUNT(DISTINCT countryCode) FROM {current_table}")
        completed_regions = cursor.fetchone()[0]

        if completed_regions == len(REGIONS) and previous_table:
            logger.info("Starting rank comparison phase")
            try:
                logger.info(
                    f"All regions completed ({completed_regions}/{len(REGIONS)}). Comparing with {previous_table}"
                )
                csv_data = bulk_rank_comparison(cursor, previous_table, current_table)

                logger.info("Committing rank updates")
                db.commit()
                logger.info("Rank updates committed successfully")

                # Write CSV file
                logger.info(f"Writing {len(csv_data)} records to CSV")
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

                    logger.info(f"Rank comparison CSV saved to {CSV_OUTPUT}")
                except Exception as e:
                    logger.error(f"Failed to write CSV: {e}")
                    failed_inserts.append(f"CSV write failed: {e}")

            except Exception as e:
                logger.error(f"Rank comparison failed: {e}")
                failed_inserts.append(f"Rank comparison failed: {e}")
        else:
            if completed_regions != len(REGIONS):
                logger.info(
                    f"Skipping rank comparison - only {completed_regions}/{len(REGIONS)} regions completed"
                )
            elif not previous_table:
                logger.info("Skipping rank comparison - no previous table available")

        # Save other output files
        try:
            logger.info("Writing output files")

            with open(os.path.join(OUTPUT_DIR, "lookup_missing_ids.txt"), "w") as f:
                for country, mid in sorted(missing_ids_set):
                    f.write(f"{country},{mid}\n")
            logger.info(f"Written {len(missing_ids_set)} missing IDs")

            with open(OUTPUT_IDS, "w") as f:
                for aid in sorted(all_apple_ids, key=lambda x: int(x)):
                    f.write(aid + "\n")
            logger.info(f"Written {len(all_apple_ids)} Apple IDs")

            with open(FAILED_REQ_LOG, "w") as f:
                for region, gid, gname, err in failed_requests:
                    f.write(f"{region.upper()} | {gid} - {gname} | {err}\n")
            logger.info(f"Written {len(failed_requests)} failed requests")

            with open(FAILED_INSERTS_LOG, "w") as f:
                f.write("\n".join(failed_inserts))
            logger.info(f"Written {len(failed_inserts)} failed inserts")

        except Exception as e:
            logger.error(f"Failed to write some output files: {e}")

        # Calculate execution time
        end_time = datetime.datetime.now()
        execution_time = end_time - start_time

        # Print and log final summary
        logger.info("=" * 80)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Start time: {start_time}")
        logger.info(f"End time: {end_time}")
        logger.info(f"Total execution time: {execution_time}")
        logger.info(f"Total Apple IDs collected: {len(all_apple_ids)}")
        logger.info(f"Total chart entries fetched: {total_charts_fetched}")
        logger.info(f"Total metadata lookups: {total_metadata_lookups}")
        logger.info(f"Total records inserted: {total_records_inserted}")
        logger.info(f"Failed requests: {len(failed_requests)}")
        logger.info(f"Failed inserts: {len(failed_inserts)}")
        logger.info(f"Missing lookup IDs: {len(missing_ids_set)}")
        logger.info(f"Created/used table: {current_table}")

        if previous_table:
            logger.info(f"Compared with table: {previous_table}")

        if reuse_existing:
            logger.info("Mode: RESUMED from existing table")
        else:
            logger.info("Mode: FRESH start with new table")

        if failed_requests:
            logger.warning("FAILED REQUESTS BY REGION:")
            failed_by_region = {}
            for region, gid, gname, err in failed_requests:
                failed_by_region[region] = failed_by_region.get(region, 0) + 1
            for region, count in sorted(failed_by_region.items()):
                logger.warning(f"  {region.upper()}: {count} failures")

        # Clean up
        try:
            cursor.close()
            db.close()
            session.close()
            logger.info("Cleaned up database and HTTP connections")
        except Exception as e:
            logger.warning(f"Cleanup warning: {e}")

        logger.info(
            "Enhanced collection with resumption capability completed successfully!"
        )
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Fatal error in main execution: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Script interrupted by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)
