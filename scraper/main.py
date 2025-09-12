# serpapi_social_scraper.py
import json
import logging
import os
import time

import instaloader
from dotenv import load_dotenv
from serpapi import GoogleSearch

load_dotenv()
# ---------------- CONFIG ----------------
SERPAPI_KEY = os.getenv("SERPAPI", "2s7017")
INSTAGRAM_QUERY = "site:instagram.com intitle:NYC influencer"
TIKTOK_QUERY = "site:tiktok.com intitle:NYC influencer"
MAX_PROFILES = 9
DELAY = 2  # seconds
OUTPUT_FILE = "social_nyc_serpapi.json"
MIN_FOLLOWERS = 5000
# ----------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("serpapi_social_scraper.log"),
        logging.StreamHandler(),
    ],
)

loader = instaloader.Instaloader()


def fetch_urls(query, start=0, domain="instagram"):
    search = GoogleSearch({"q": query, "api_key": SERPAPI_KEY, "start": start})
    results = search.get_dict()
    urls = []
    if "organic_results" in results:
        for r in results["organic_results"]:
            link = r.get("link")
            if link and f"{domain}.com/" in link and "/p/" not in link:
                urls.append(link)
    return urls


def get_instagram_profile(url):
    username = url.rstrip("/").split("/")[-1]
    try:
        profile = instaloader.Profile.from_username(loader.context, username)
        return {
            "username": profile.username,
            "full_name": profile.full_name,
            "followers": profile.followers,
            "type": "instagram",
        }
    except Exception as e:
        logging.warning(f"Failed to fetch Instagram {username}: {e}")
        return None


def main():
    all_profiles = []

    # Scrape Instagram
    start = 0
    while len(all_profiles) < MAX_PROFILES:
        urls = fetch_urls(INSTAGRAM_QUERY, start=start, domain="instagram")
        if not urls:
            logging.info("No more Instagram results from SerpApi.")
            break

        for url in urls:
            profile_data = get_instagram_profile(url)
            if profile_data and profile_data["followers"] >= MIN_FOLLOWERS:
                all_profiles.append(profile_data)
                logging.info(
                    f"Added Instagram {profile_data['username']} ({profile_data['followers']} followers)"
                )
                if len(all_profiles) >= MAX_PROFILES:
                    break
            time.sleep(DELAY)

        start += 10
        time.sleep(DELAY)

    # Scrape TikTok (no follower filtering here unless you add a TikTok API)
    start = 0
    while len(all_profiles) < MAX_PROFILES:
        urls = fetch_urls(TIKTOK_QUERY, start=start, domain="tiktok")
        if not urls:
            logging.info("No more TikTok results from SerpApi.")
            break

        for url in urls:
            all_profiles.append(
                {
                    "username": url.rstrip("/").split("/")[-1],
                    "profile_url": url,
                    "type": "tiktok",
                }
            )
            logging.info(f"Added TikTok {url}")
            if len(all_profiles) >= MAX_PROFILES:
                break
            time.sleep(DELAY)

        start += 10
        time.sleep(DELAY)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(all_profiles, f, indent=2)
    logging.info(f"Saved {len(all_profiles)} profiles to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
