import requests
import time
import hashlib

# === 🔐 PodcastIndex API credentials ===
API_KEY = "9CG39SQF9F5ZEUVMKZ2G"
API_SECRET = "F78eSkhL4vF^aF^dn^qpGDJt#8HGERAgDgNF2J6U"
USER_AGENT = "mycrawler/1.0"

# === 📁 Output file ===
OUTPUT_FILE = "./documents/all_discovered_apple_ids.txt"


def get_auth_headers():
    now = str(int(time.time()))
    message = API_KEY + API_SECRET + now
    signature = hashlib.sha1(message.encode("utf-8")).hexdigest()
    return {
        "User-Agent": USER_AGENT,
        "X-Auth-Key": API_KEY,
        "X-Auth-Date": now,
        "Authorization": signature,
    }


def fetch_from_endpoint(url):
    headers = get_auth_headers()
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            # Some endpoints return data under 'feeds', others under 'podcasts'
            return data.get("feeds") or data.get("podcasts") or []
        else:
            print(f"❌ {url} → {response.status_code} → {response.text}")
    except Exception as e:
        print(f"⚠️ Error accessing {url}: {e}")
    return []


def fetch_categories():
    """Fetch all available categories from PodcastIndex"""
    headers = get_auth_headers()
    try:
        response = requests.get(
            "https://api.podcastindex.org/api/1.0/categories/list", headers=headers
        )
        if response.status_code == 200:
            data = response.json()
            return data.get("feeds") or data.get("categories") or {}
        else:
            print(f"❌ Categories endpoint → {response.status_code} → {response.text}")
    except Exception as e:
        print(f"⚠️ Error fetching categories: {e}")
    return {}


def extract_valid_ids(feeds, master_ids):
    new_ids = set()
    suspicious_ids = []

    for feed in feeds:
        aid = feed.get("itunesId")
        if isinstance(aid, int):
            if aid > 100000 and aid not in master_ids:
                new_ids.add(aid)
            elif aid <= 100000:
                suspicious_ids.append(aid)

    # Debug: show suspicious IDs if any
    if suspicious_ids:
        print(f"    🚫 Filtered suspicious IDs: {sorted(set(suspicious_ids))}")

    master_ids.update(new_ids)
    return new_ids


def collect_apple_ids():
    master_ids = set()

    print("📦 Fetching from /recent/feeds")
    feeds = fetch_from_endpoint("https://api.podcastindex.org/api/1.0/recent/feeds")
    print(f"  → {len(extract_valid_ids(feeds, master_ids))} new IDs")

    print("🔥 Fetching from /podcasts/trending")
    feeds = fetch_from_endpoint(
        "https://api.podcastindex.org/api/1.0/podcasts/trending"
    )
    print(f"  → {len(extract_valid_ids(feeds, master_ids))} new IDs")

    print("🔍 Fetching from /search/byterm")
    search_terms = [
        "news",
        "tech",
        "history",
        "education",
        "sports",
        "startup",
        "health",
        "business",
        "comedy",
        "science",
        "marketing",
        "travel",
        "interview",
        "politics",
        "entertainment",
        "music",
    ]
    for term in search_terms:
        url = f"https://api.podcastindex.org/api/1.0/search/byterm?q={term}"
        feeds = fetch_from_endpoint(url)
        print(f"  - '{term}' → {len(extract_valid_ids(feeds, master_ids))} new")

    print(" Fetching from /podcasts/bytag")
    tags = [
        "technology",
        "history",
        "comedy",
        "news",
        "education",
        "health",
        "business",
    ]
    for tag in tags:
        url = f"https://api.podcastindex.org/api/1.0/podcasts/bytag?tag={tag}"
        feeds = fetch_from_endpoint(url)
        print(f"  - tag '{tag}' → {len(extract_valid_ids(feeds, master_ids))} new")

    print(" Fetching from /podcasts/bymedium")
    media_types = [
        "podcast",
        "music",
        "audiobook",
        "blog",
        "film",
        "video",
        "newsletter",
    ]
    for medium in media_types:
        url = f"https://api.podcastindex.org/api/1.0/podcasts/bymedium?medium={medium}"
        feeds = fetch_from_endpoint(url)
        print(
            f"  - medium '{medium}' → {len(extract_valid_ids(feeds, master_ids))} new"
        )

    # NEW: Fetch using actual categories from /categories/list
    print("📂 Fetching categories and searching by category names")
    categories = fetch_categories()

    if categories:
        category_names = []

        # Parse the categories response (structure may vary)
        if isinstance(categories, dict):
            # If it's a dict with category data
            for cat_data in categories.values():
                if isinstance(cat_data, dict) and "name" in cat_data:
                    category_names.append(cat_data["name"].lower())
                elif isinstance(cat_data, str):
                    category_names.append(cat_data.lower())
        elif isinstance(categories, list):
            # If it's a list of category objects
            for cat in categories:
                if isinstance(cat, dict) and "name" in cat:
                    category_names.append(cat["name"].lower())
                elif isinstance(cat, str):
                    category_names.append(cat.lower())

        print(f"  Found {len(category_names)} categories from API")

        # Use all 112 categories but with a reasonable sample for testing
        # You can remove the [:20] to use all categories
        sample_categories = category_names

        for i, category in enumerate(sample_categories, 1):
            # Search by category name
            url = f"https://api.podcastindex.org/api/1.0/search/byterm?q={category.replace(' ', '%20')}"
            feeds = fetch_from_endpoint(url)
            new_count = len(extract_valid_ids(feeds, master_ids))

            # Show all categories being processed
            if new_count > 0:
                print(
                    f"  - [{i}/{len(sample_categories)}] '{category}' → {new_count} new ✅"
                )
            else:
                print(f"  - [{i}/{len(sample_categories)}] '{category}' → 0 new ⚪")

            # Add small delay to be respectful to the API
            time.sleep(0.3)

    return master_ids


def save_to_file(apple_ids):
    with open(OUTPUT_FILE, "w") as f:
        for i, aid in enumerate(sorted(apple_ids), 1):
            f.write(f"{aid}\n")
    print(f"\n✅ Saved {len(apple_ids)} valid Apple IDs to {OUTPUT_FILE}")


def print_summary(apple_ids):
    print("\n📊 COLLECTION SUMMARY")
    print(f"   Total unique Apple iTunes IDs: {len(apple_ids)}")
    print(f"   ID range: {min(apple_ids)} - {max(apple_ids)}")


if __name__ == "__main__":
    print("🚀 Starting Apple ID collection from PodcastIndex API\n")
    all_ids = collect_apple_ids()
    save_to_file(all_ids)
    print_summary(all_ids)
    print("\n🎉 Collection complete!")
