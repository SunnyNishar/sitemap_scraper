import re
import mysql.connector
from lxml import etree

SITEMAP_FILE = "./dataset/sitemaps_podcasts_podcast_100_1.xml"
OUTPUT_FILE = "./documents/new_ids_to_process.txt"

DB_CONFIG = {"host": "localhost", "user": "root", "password": "", "database": "sitemap"}

print("Reading sitemap file...")
tree = etree.parse(SITEMAP_FILE)
urls = tree.xpath(
    "//xmlns:loc", namespaces={"xmlns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
)

sitemap_ids = set()
for url in urls:
    match = re.search(r"id(\d+)", url.text)
    if match:
        sitemap_ids.add(match.group(1))

print(f"✅ Extracted {len(sitemap_ids)} IDs from sitemap.")

print("Connecting to database...")
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()
cursor.execute("SELECT apple_id FROM test_apple_ids")
rows = cursor.fetchall()
existing_ids = set()
for row in rows:
    id_str = str(row[0])  # Convert to string (to match sitemap format)
    existing_ids.add(id_str)
cursor.close()
conn.close()

print(f"✅ Retrieved {len(existing_ids)} IDs from database.")

new_ids = sitemap_ids - existing_ids
print(f"☑️ Found {len(new_ids)} new IDs not in the database.")

with open(OUTPUT_FILE, "w") as f:
    for i, podcast_id in enumerate(sorted(new_ids), start=1):
        f.write(f"{podcast_id}\n")

print(f"✅ New IDs saved to '{OUTPUT_FILE}'")
