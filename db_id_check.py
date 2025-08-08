import mysql.connector
import re

# === 📁 Input file ===
INPUT_FILE = "./documents/apple_chart_all_ids.txt"

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


# === 🏁 Main ===
if __name__ == "__main__":
    print("📖 Reading Apple IDs from file...")
    file_ids = read_ids_from_file(INPUT_FILE)

    print("🗃 Fetching Apple IDs from database...")
    db_ids = read_ids_from_db()

    print("🔍 Comparing IDs...")
    new_ids = file_ids - db_ids

    print(f"🆕 Found {len(new_ids)} new Apple IDs not in DB")
