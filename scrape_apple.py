import asyncio
from playwright.async_api import async_playwright
import re


# Helper: extract iTunes ID from URL
def extract_itunes_id(url):
    match = re.search(r"/id(\d+)", url)
    return match.group(1) if match else None


# Main scraping logic
async def scrape_apple_charts(region="us"):
    url = f"https://podcasts.apple.com/{region}/charts"
    print(f"Fetching: {url}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle")

        # Wait until podcast links are visible
        await page.wait_for_selector("a[href*='/podcast/']")

        # Get all podcast links
        links = await page.eval_on_selector_all(
            "a[href*='/podcast/']", "els => els.map(e => e.href)"
        )
        print(f"Found {len(links)} podcast links")

        # Extract unique iTunes IDs
        ids = set()
        for link in links:
            id = extract_itunes_id(link)
            if id:
                ids.add(id)

        await browser.close()

    # Show results
    print(f"\n🎧 Total Unique iTunes IDs: {len(ids)}")
    for itunes_id in sorted(ids):
        print(itunes_id)

    # Optional: Save to file
    with open("apple_chart_ids.txt", "w") as f:
        for itunes_id in sorted(ids):
            f.write(itunes_id + "\n")


# Run the async main
asyncio.run(scrape_apple_charts())
