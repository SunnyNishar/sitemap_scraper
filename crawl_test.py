import mysql.connector
import re

# === 📁 Input/output files ===
INPUT_FILE = "./documents/all_discovered_apple_ids.txt"
OUTPUT_FILE = "./documents/new_ids_to_insert.txt"

# === 🛢 Database connection config ===
DB_CONFIG = {"host": "localhost", "user": "root", "password": "", "database": "sitemap"}


# === 📖 Step 1: Read Apple IDs from the input file ===
def read_ids_from_file(file_path):
    ids = set()
    with open(file_path, "r") as f:
        for line in f:
            match = re.search(r"\b(\d{6,15})\b", line)
            if match:
                ids.add(match.group(1))
    return ids


# === 🗃 Step 2: Read existing Apple IDs from the database ===
def read_ids_from_db():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT apple_id FROM bigint_apple_ids")
    db_ids = set(str(row[0]) for row in cursor.fetchall())
    cursor.close()
    conn.close()
    return db_ids


# === 🆕 Step 3: Compare and find new IDs ===
# === 🆕 Step 3: Compare and append new IDs to output file (no duplicates) ===
def save_new_ids_to_file(new_ids, file_path):
    existing_ids = set()

    # Read existing IDs from the file (if exists)
    try:
        with open(file_path, "r") as f:
            for line in f:
                match = re.search(r"\b(\d{6,15})\b", line)
                if match:
                    existing_ids.add(match.group(1))
    except FileNotFoundError:
        pass  # File doesn't exist yet, that's okay

    truly_new_ids = new_ids - existing_ids

    # Merge existing IDs with new IDs (avoid duplicates)
    all_ids = sorted(existing_ids.union(truly_new_ids))

    # Write all unique IDs back to the file with numbering
    with open(file_path, "w") as f:
        for i, aid in enumerate(all_ids, 1):
            f.write(f"{aid}\n")

    print(
        f"✅ {len(truly_new_ids)} new Apple IDs added (total: {len(all_ids)} unique IDs in file)"
    )


# === 🏁 Main ===
if __name__ == "__main__":
    print("📖 Reading Apple IDs from file...")
    file_ids = read_ids_from_file(INPUT_FILE)

    print("🗃 Fetching Apple IDs from database...")
    db_ids = read_ids_from_db()

    print("🔍 Comparing IDs...")
    new_ids = file_ids - db_ids

    print(f"🆕 Found {len(new_ids)} new IDs not in DB")
    save_new_ids_to_file(new_ids, OUTPUT_FILE)
