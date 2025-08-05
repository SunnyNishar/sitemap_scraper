import sqlite3
import requests
import time

# === CONFIGURATION ===
DB_PATH = "C:/Users/USER/Downloads/podcastindex_feeds.db/podcastindex_feeds.db"
BATCH_SIZE = 200
DELAY_BETWEEN_BATCHES = 1  # seconds (to avoid rate-limiting)
SAVE_TO_FILE = "valid_apple_ids.txt"


# === STEP 1: Load Apple IDs from SQLite DB ===
def get_apple_ids_from_sqlite(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT DISTINCT itunesId FROM podcasts WHERE itunesId IS NOT NULL AND itunesId != 0"
    )
    ids = [str(row[0]) for row in cursor.fetchall() if row[0]]
    conn.close()
    return ids


# === STEP 2: Validate Apple IDs using iTunes Lookup API ===
def validate_apple_ids(ids):
    valid_ids = set()
    base_url = "https://itunes.apple.com/lookup?id="

    for i in range(0, len(ids), BATCH_SIZE):
        batch = ids[i : i + BATCH_SIZE]
        batch_str = ",".join(batch)

        try:
            response = requests.get(base_url + batch_str)
            response.raise_for_status()
            data = response.json()

            if "results" in data:
                for item in data["results"]:
                    if "collectionId" in item:  # Podcasts have collectionId
                        valid_ids.add(str(item["collectionId"]))

            print(f"✅ Batch {i // BATCH_SIZE + 1}: {len(valid_ids)} valid so far")

        except Exception as e:
            print(f"❌ Error in batch {i // BATCH_SIZE + 1}: {e}")

        time.sleep(DELAY_BETWEEN_BATCHES)

    return valid_ids


# === STEP 3: Save valid IDs to file ===
def save_ids_to_file(ids, filepath):
    with open(filepath, "w") as f:
        for id_ in sorted(ids):
            f.write(id_ + "\n")
    print(f"✅ Saved {len(ids)} valid Apple IDs to {filepath}")


# === MAIN ===
if __name__ == "__main__":
    print("🔄 Extracting Apple IDs from DB...")
    all_ids = get_apple_ids_from_sqlite(DB_PATH)
    print(f"🔍 Total IDs extracted: {len(all_ids)}")

    print("🚀 Validating Apple IDs via iTunes Lookup API...")
    valid_ids = validate_apple_ids(all_ids)

    save_ids_to_file(valid_ids, SAVE_TO_FILE)
