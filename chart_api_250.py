import requests
import os

GENRE_LOOKUP_URL = "https://itunes.apple.com/WebObjects/MZStoreServices.woa/ws/genres"

try:
    response = requests.get(GENRE_LOOKUP_URL)
    response.raise_for_status()
    genres_data = response.json()
except Exception as e:
    print("❌ Failed to fetch genres:", e)
    exit(1)


def collect_genre_ids(genre_dict):
    ids = {}
    for genre_id, genre_info in genre_dict.items():
        ids[int(genre_id)] = genre_info.get("name", "")
        subgenres = genre_info.get("subgenres", {})
        if subgenres:
            ids.update(collect_genre_ids(subgenres))
    return ids


ALL_GENRES = collect_genre_ids(genres_data["26"].get("subgenres", {}))

print(f"🎧 Total genres + subgenres found: {len(ALL_GENRES)}")

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

all_apple_ids = set()
failed_requests = []

for region in REGIONS:
    for genre_id, genre_name in ALL_GENRES.items():
        url = f"https://itunes.apple.com/WebObjects/MZStoreServices.woa/ws/charts?cc={region}&g={genre_id}&name=Podcasts&limit=200"
        print(f"📥 Fetching {region.upper()} | {genre_id} - {genre_name}")
        try:
            res = requests.get(url, timeout=10)
            res.raise_for_status()
            data = res.json()

            ids = data.get("resultIds", [])
            if ids:
                all_apple_ids.update(map(str, ids))
            else:
                print(f"⚠️ No IDs for {region.upper()} | {genre_id} - {genre_name}")

        except Exception as e:
            print(f"❌ Error: {e}")
            failed_requests.append((region, genre_id, genre_name))


# Save Apple IDs
output_path = "./documents/chart_api_200.txt"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

with open(output_path, "w") as f:
    for apple_id in sorted(all_apple_ids):
        f.write(apple_id + "\n")

# Save failed requests
failed_requests_path = "./documents/failed_requests.txt"
with open(failed_requests_path, "w") as f:
    for region, genre_id, genre_name in failed_requests:
        f.write(f"{region.upper()} | {genre_id} - {genre_name}\n")

# Final summary
print(f"\n✅ Done. Total unique Apple IDs: {len(all_apple_ids)}")
print(f"📁 Saved to: {output_path}")
if failed_requests:
    print(f"⚠️ {len(failed_requests)} Failed requests logged to: {failed_requests_path}")
