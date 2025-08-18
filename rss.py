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
    "database": "podcast2",
}

OUTPUT_DIR = "./documents"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_IDS = os.path.join(OUTPUT_DIR, "apple_ids_from_all_genres_regions.txt")
UNEXPECTED_LOG = os.path.join(OUTPUT_DIR, "unexpected_id_formats.txt")
FAILED_REQ_LOG = os.path.join(OUTPUT_DIR, "failed_requests.txt")
FAILED_INSERTS_LOG = os.path.join(OUTPUT_DIR, "failed_inserts.txt")
CSV_OUTPUT = os.path.join(OUTPUT_DIR, "rank_comparison.csv")

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
    "ci": "Côte d’Ivoire",
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


def sort_genres_with_podcasts_first(genres_list):
    return sorted(genres_list, key=lambda x: (0 if x[0] == 26 else 1, x[0]))


def get_last_table_name(cursor):
    cursor.execute("""
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = 'podcast2'
        AND TABLE_NAME LIKE 'apple_rss_%'
        ORDER BY TABLE_NAME DESC
        LIMIT 1
    """)
    result = cursor.fetchone()
    return result[0] if result else None


def create_new_table(cursor, table_name):
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
            createdTime datetime DEFAULT NULL,
            updatedTime datetime DEFAULT NULL,
            PRIMARY KEY (id)
        ) ENGINE=InnoDB
    """)


def compare_ranks(cursor, last_table, current_table, region, category, subcategory):
    if not last_table:
        cursor.execute(
            f"""
            SELECT appleid, chart_rank, title, countryCode, countryName
            FROM {current_table}
            WHERE countryCode=%s AND category=%s AND (
                (subcategory IS NULL AND %s IS NULL) OR subcategory=%s
            )
        """,
            (region, category, subcategory, subcategory),
        )
        new_data = {
            row[0]: (row[1], row[2], row[3], row[4]) for row in cursor.fetchall()
        }
        return [
            (aid, title, new_rank, None, "NEW", country_code, country_name)
            for aid, (new_rank, title, country_code, country_name) in new_data.items()
        ]

    cursor.execute(
        f"""
        SELECT appleid, chart_rank, title
        FROM {last_table}
        WHERE countryCode=%s AND category=%s AND (
            (subcategory IS NULL AND %s IS NULL) OR subcategory=%s
        )
    """,
        (region, category, subcategory, subcategory),
    )
    old_data = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

    cursor.execute(
        f"""
        SELECT appleid, chart_rank, title, countryCode, countryName
        FROM {current_table}
        WHERE countryCode=%s AND category=%s AND (
            (subcategory IS NULL AND %s IS NULL) OR subcategory=%s
        )
    """,
        (region, category, subcategory, subcategory),
    )
    new_data = {row[0]: (row[1], row[2], row[3], row[4]) for row in cursor.fetchall()}

    results = []
    for aid, (new_rank, title, country_code, country_name) in new_data.items():
        old_rank, _ = old_data.get(aid, (None, None))
        if old_rank is None:
            change = "NEW"
        else:
            diff = old_rank - new_rank
            change = f"+{diff}" if diff > 0 else (str(diff) if diff < 0 else "0")
        results.append(
            (aid, title, new_rank, old_rank, change, country_code, country_name)
        )

    return results


# ===============================
# MAIN
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

db = mysql.connector.connect(**DB_CONFIG)
cursor = db.cursor()

# Get last table name and create new one
last_table = get_last_table_name(cursor)
now = datetime.datetime.now()
current_table = f"apple_rss_{now.strftime('%Y%m%d_%H%M%S')}"
create_new_table(cursor, current_table)

print(f"📊 Created new table: {current_table}")
if last_table:
    print(f"📊 Will compare with: {last_table}")

insert_sql = f"""
INSERT INTO {current_table}
(chart_rank, podcast_url, appleid, title, img_url, countryCode, countryName, category, subcategory, createdTime, updatedTime)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

all_apple_ids = set()
failed_requests, unexpected_formats, failed_inserts = [], [], []

for region in REGIONS:
    print(f"\n🌍 Processing region: {region.upper()}")
    for idx, (genre_id, (category_name, subcategory_name)) in enumerate(
        GENRE_LIST, start=1
    ):
        display_name = subcategory_name if subcategory_name else category_name
        print(
            f"🔥 [{idx}/{len(GENRE_LIST)}] {region.upper()} | {genre_id} - {display_name}"
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
                subcat_value = subcategory_name if subcategory_name else None
                country_name = COUNTRY_NAMES.get(region, region.upper())

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
                            country_name,
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
            failed_requests.append((region, genre_id, subcategory_name, str(e)))

db.commit()

# Save outputs
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

db.commit()
# Compare and save CSV
# Compare and save CSV
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

    if last_table and last_table != current_table:
        for region in REGIONS:
            for genre_id, (category_name, subcategory_name) in GENRE_LIST:
                changes = compare_ranks(
                    cursor,
                    last_table,
                    current_table,
                    region,
                    category_name,
                    subcategory_name,
                )
                for (
                    aid,
                    title,
                    new_rank,
                    old_rank,
                    change,
                    country_code,
                    country_name,
                ) in changes:
                    writer.writerow(
                        [
                            aid,
                            title,
                            country_code,
                            country_name,
                            category_name,
                            subcategory_name,
                            new_rank,
                            old_rank,
                            change,
                        ]
                    )
    else:
        print("⚠️ No previous table found — skipping rank comparison.")

print(f"✅ Rank comparison saved to {CSV_OUTPUT}")

cursor.close()
db.close()
print("\n✅ Done.")
