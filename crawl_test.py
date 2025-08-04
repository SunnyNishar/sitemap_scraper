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
    cursor.execute("SELECT apple_id FROM test_apple_ids")
    db_ids = set(str(row[0]) for row in cursor.fetchall())
    cursor.close()
    conn.close()
    return db_ids


# === 🆕 Step 3: Compare and find new IDs ===
def save_new_ids_to_file(new_ids, file_path):
    with open(file_path, "w") as f:
        for i, aid in enumerate(sorted(new_ids), 1):
            f.write(f"{i}. {aid}\n")
    print(f"✅ {len(new_ids)} new Apple IDs saved to '{file_path}'")


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
