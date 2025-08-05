import sqlite3
import mysql.connector
import re
import os

# === 📂 File Paths ===
SQLITE_DB_PATH = r"C:\Users\USER\Downloads\podcastindex_feeds.db\podcastindex_feeds.db"
OUTPUT_FILE = "./documents/new_ids_to_insert.txt"

# === 🛢 MySQL DB config ===
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "sitemap",
}


# === 1️⃣ Get Apple IDs from PodcastIndex SQLite DB ===
def get_apple_ids_from_sqlite(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT DISTINCT itunesId FROM podcasts WHERE itunesId IS NOT NULL AND itunesId != 0"
    )
    ids = set(str(row[0]) for row in cursor.fetchall())
    conn.close()
    return ids


# === 2️⃣ Get existing Apple IDs from MySQL DB ===
def get_existing_ids_from_mysql():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT apple_id FROM test_apple_ids"
    )  # or change to feeds table if needed
    ids = set(str(row[0]) for row in cursor.fetchall())
    cursor.close()
    conn.close()
    return ids


# === 3️⃣ Save only new, unique IDs to file (append mode without duplicates) ===
def save_new_ids_to_file(new_ids, file_path):
    existing_file_ids = set()

    # Load existing IDs in file (if any)
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            for line in f:
                match = re.search(r"\b(\d{6,15})\b", line)
                if match:
                    existing_file_ids.add(match.group(1))

    # Merge + filter empty values
    combined_ids = sorted(
        aid for aid in existing_file_ids.union(new_ids) if aid.strip()
    )

    with open(file_path, "w") as f:
        for i, aid in enumerate(combined_ids, 1):
            f.write(f"{aid}\n")

    print(f"✅ {len(new_ids)} new Apple IDs added (total in file: {len(combined_ids)})")


# === 🏁 Main ===
if __name__ == "__main__":
    print("📥 Extracting Apple IDs from PodcastIndex SQLite DB...")
    sqlite_ids = get_apple_ids_from_sqlite(SQLITE_DB_PATH)

    print("🗃 Fetching existing Apple IDs from MySQL DB...")
    mysql_ids = get_existing_ids_from_mysql()

    print("🔍 Comparing...")
    new_ids = sqlite_ids - mysql_ids
    print(f"🆕 Found {len(new_ids)} new Apple IDs")

    print("💾 Saving to output file...")
    save_new_ids_to_file(new_ids, OUTPUT_FILE)
