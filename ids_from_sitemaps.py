import requests
import xml.etree.ElementTree as ET
import gzip
import io
import re
import mysql.connector

# === Config ===
SITEMAP_INDEX_URL = "https://podcasts.apple.com/sitemaps_podcasts_index_podcast_1.xml"
OUTPUT_FILE = "./documents/new_ids_from_sitemaps.txt"
DB_CONFIG = {"host": "localhost", "user": "root", "password": "", "database": "sitemap"}


# === Step 1: Fetch sitemap index and extract all .gz sitemap URLs ===
def fetch_gz_sitemap_urls(index_url):
    print(f"🌐 Fetching sitemap index: {index_url}")
    try:
        response = requests.get(index_url)
        response.raise_for_status()
    except Exception as e:
        print("❌ Failed to fetch sitemap index:", e)
        return []

    gz_urls = []
    root = ET.fromstring(response.content)
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    for sitemap in root.findall("ns:sitemap", ns):
        loc = sitemap.find("ns:loc", ns)
        if loc is not None and loc.text.endswith(".xml.gz"):
            gz_urls.append(loc.text)

    print(f"✅ Found {len(gz_urls)} .gz sitemap files")
    return gz_urls


# === Step 2: Download, decompress, and parse <loc> URLs from each .gz sitemap ===
def extract_urls_from_gz(gz_url):
    urls = []
    try:
        response = requests.get(gz_url)
        response.raise_for_status()
        with gzip.open(io.BytesIO(response.content), "rt", encoding="utf-8") as f:
            root = ET.parse(f).getroot()
            ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

            for url in root.findall("ns:url", ns):
                loc = url.find("ns:loc", ns)
                if loc is not None and loc.text:
                    urls.append(loc.text)
    except Exception as e:
        print(f"⚠️ Failed to process {gz_url}: {e}")
    return urls


# === Step 3: Extract Apple IDs from URLs ===
def extract_apple_ids(urls):
    ids = set()
    for url in urls:
        match = re.search(r"id(\d{6,15})", url)
        if match:
            ids.add(match.group(1))
    return ids


# === Step 4: Get existing Apple IDs from DB ===
def get_existing_ids_from_db():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT apple_id FROM bigint_apple_ids")
    db_ids = set(str(row[0]) for row in cursor.fetchall())
    cursor.close()
    conn.close()
    return db_ids


# === Step 5: Save only new Apple IDs to file ===
def save_new_ids_to_file(new_ids, path):
    with open(path, "w") as f:
        for aid in sorted(new_ids):
            f.write(f"{aid}\n")
    print(f"💾 Saved {len(new_ids)} new Apple IDs to: {path}")


# === Main ===
if __name__ == "__main__":
    print("🚀 Starting Apple ID discovery from .gz sitemaps...\n")

    gz_sitemaps = fetch_gz_sitemap_urls(SITEMAP_INDEX_URL)
    all_urls = []
    total_ids = set()

    for i, gz_url in enumerate(gz_sitemaps, 1):
        print(f"\n📦 Processing sitemap {i}/{len(gz_sitemaps)}: {gz_url}")
        urls = extract_urls_from_gz(gz_url)
        all_urls.extend(urls)
        ids = extract_apple_ids(urls)
        total_ids.update(ids)
        print(f"  ➕ Found {len(ids)} IDs in this sitemap")

    print(f"\n🔍 Total Apple IDs extracted: {len(total_ids)}")

    db_ids = get_existing_ids_from_db()
    new_ids = total_ids - db_ids

    print(f"🆕 New Apple IDs not in DB: {len(new_ids)}")

    save_new_ids_to_file(new_ids, OUTPUT_FILE)
