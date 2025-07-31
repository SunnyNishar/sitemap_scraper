import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Starting URL
start_url = "https://www.feedspot.com/widgets"

# Keep track of visited links
visited = set()

# Only crawl 1 level deep for now
def crawl(url):
    if url in visited:
        return
    visited.add(url)

    print(f"Visiting: {url}")
    try:
        response = requests.get(url, timeout=5)
        if response.status_code != 200:
            print(f"Failed to fetch {url}")
            return
    except Exception as e:
        print(f"Error: {e}")
        return

    soup = BeautifulSoup(response.text, "html.parser")

    # Find and print all links
    links = soup.find_all("a")
    for link in links:
        href = link.get("href")
        if href:
            full_url = urljoin(url, href)  # Handle relative links
            print("→ Found link:", full_url)

# Run the crawler on the start URL
crawl(start_url)
