import re
from lxml import etree

SITEMAP_FILE = "./dataset/sitemaps_podcasts_podcast_100_1.xml"
OUTPUT_FILE = "./documents/extracted_ids.txt"

ids = set()
tree = etree.parse(SITEMAP_FILE)
urls = tree.xpath('//xmlns:loc', namespaces={'xmlns': 'http://www.sitemaps.org/schemas/sitemap/0.9'})
for url in urls:
    match = re.search(r'id(\d+)', url.text)
    if match:
        ids.add(match.group(1))

with open(OUTPUT_FILE, "w") as f:
    for i, podcast_id in enumerate(ids, start=1):
        f.write(f"{i}. {podcast_id}\n")

print(f"Extracted {len(ids)} IDs and saved them to {OUTPUT_FILE}.")
