import asyncio
import aiohttp
import time
from datetime import datetime

# ======== CONFIG =========
COUNTRIES = ["us", "in", "gb", "au", "ca"]
GENRES = ["26", "1301", "1303", "1304"]

TOTAL_REQUESTS = 5000  # total requests to attempt
BATCH_SIZE = 500  # number of requests sent at once
TIMEOUT = 5
# =========================


def build_url(country, genre):
    return f"https://itunes.apple.com/{country}/rss/toppodcasts/genre={genre}/json"


async def fetch(session, index, country, genre):
    url = build_url(country, genre)
    try:
        start = time.time()
        async with session.get(url, timeout=TIMEOUT) as resp:
            content = await resp.read()
            elapsed = time.time() - start

            return {
                "index": index,
                "status": resp.status,
                "country": country,
                "genre": genre,
                "elapsed": elapsed,
                "size": len(content),
                "headers": dict(resp.headers),
            }
    except Exception as e:
        return {
            "index": index,
            "status": "ERR",
            "country": country,
            "genre": genre,
            "elapsed": None,
            "size": 0,
            "headers": {},
            "error": str(e),
        }


async def stress_test():
    print(f"🚀 Async stress test on Top Podcasts RSS API")
    print(f"Countries: {COUNTRIES}")
    print(f"Genres: {GENRES}")
    print(f"Total: {TOTAL_REQUESTS} | Batch size: {BATCH_SIZE}\n")

    start_time = datetime.now()

    # Prepare request list
    combos = []
    idx = 0
    while len(combos) < TOTAL_REQUESTS:
        for country in COUNTRIES:
            for genre in GENRES:
                idx += 1
                combos.append((idx, country, genre))
                if len(combos) >= TOTAL_REQUESTS:
                    break
            if len(combos) >= TOTAL_REQUESTS:
                break

    connector = aiohttp.TCPConnector(limit=None)  # no connection limit
    async with aiohttp.ClientSession(connector=connector) as session:
        for i in range(0, len(combos), BATCH_SIZE):
            batch = combos[i : i + BATCH_SIZE]
            tasks = [fetch(session, idx, c, g) for idx, c, g in batch]
            results = await asyncio.gather(*tasks)

            for res in results:
                if res["status"] == "ERR":
                    print(f"[{res['index']}] ❌ Error: {res['error']}")
                    continue

                rl_limit = res["headers"].get("X-RateLimit-Limit")
                rl_rem = res["headers"].get("X-RateLimit-Remaining")
                retry_after = res["headers"].get("Retry-After")

                print(
                    f"[{res['index']}] {res['country']}:{res['genre']} "
                    f"Status: {res['status']} | Time: {res['elapsed']:.2f}s | Size: {res['size']} "
                    f"| Limit: {rl_limit} Rem: {rl_rem} Retry: {retry_after}"
                )

                # Detect throttling
                if res["status"] == 429:
                    elapsed_total = (datetime.now() - start_time).total_seconds()
                    print(
                        f"\n🚫 Rate limit hit at request #{res['index']} "
                        f"({res['country']}:{res['genre']}) after {elapsed_total:.2f}s."
                    )
                    return
                if res["size"] < 500:  # RSS feeds are usually >1KB
                    print(
                        f"\n⚠️ Suspicious small response at #{res['index']} — possible silent throttling."
                    )
                    return

    elapsed_total = (datetime.now() - start_time).total_seconds()
    print(
        f"\n✅ Finished {TOTAL_REQUESTS} requests in {elapsed_total:.2f}s without hitting a limit."
    )


if __name__ == "__main__":
    asyncio.run(stress_test())
