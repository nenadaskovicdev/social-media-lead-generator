import json
import logging
import os
import random
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import instaloader
import pymongo
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import DuplicateKeyError, OperationFailure
from serpapi import GoogleSearch

load_dotenv()

# ---------------- CONFIG ----------------
SERPAPI_KEY = os.getenv("SERPAPI", "2s7017")
MONGO_URI = os.getenv(
    "MONGO_URI", "mongodb://booking:booking@127.0.0.1:27017/scraper"
)
MONGO_USERNAME = os.getenv("MONGO_USERNAME", "")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "")
DATABASE_NAME = "social_scraper"
INSTAGRAM_QUERY = "site:instagram.com intitle:NYC influencer"
TIKTOK_QUERY = "site:tiktok.com intitle:NYC influencer"
MAX_PROFILES = 9
MIN_FOLLOWERS = 5000

# Delay configuration (in seconds)
MIN_DELAY = 2
MAX_DELAY = 5
SERPAPI_DELAY = 3
FAILURE_DELAY = 10

# Request headers for bot evasion
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
]
# ----------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("social_media_scraper.log"),
        logging.StreamHandler(),
    ],
)

print(MONGO_URI, DATABASE_NAME, MONGO_USERNAME, MONGO_PASSWORD)


class MongoDBClient:
    """MongoDB client for handling database operations"""

    def __init__(
        self, uri: str, db_name: str, username: str = "", password: str = ""
    ):
        try:
            # Handle authentication if credentials are provided
            if username and password:
                # Extract host and port from URI
                if "mongodb://" in uri:
                    host_port = uri.replace("mongodb://", "").split("/")[0]
                    self.client = MongoClient(
                        f"mongodb://{username}:{password}@{host_port}/{db_name}?authSource=admin"
                    )
                else:
                    self.client = MongoClient(
                        uri, username=username, password=password
                    )
            else:
                self.client = MongoClient(uri)

            # Test connection
            self.client.admin.command("ismaster")
            self.db = self.client[db_name]
            self.setup_indexes()
            logging.info("Successfully connected to MongoDB")

        except OperationFailure as e:
            logging.error(f"MongoDB authentication failed: {e}")
            raise
        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            raise

    def setup_indexes(self):
        """Create indexes to prevent duplicates and improve query performance"""
        try:
            # Index for profiles collection
            self.db.profiles.create_index(
                [("profile_url", pymongo.ASCENDING)], unique=True
            )
            # Index for serpapi_results collection
            self.db.serpapi_results.create_index(
                [("query", pymongo.ASCENDING), ("start", pymongo.ASCENDING)],
                unique=True,
            )
            logging.info("MongoDB indexes created successfully")
        except Exception as e:
            logging.warning(f"Could not create MongoDB indexes: {e}")

    def insert_profiles(self, profiles: List[Dict]) -> int:
        """Insert multiple profiles with upsert operation to avoid duplicates"""
        if not profiles:
            return 0

        operations = []
        for profile in profiles:
            # Ensure we have a profile_url field
            if "profile_url" not in profile:
                if "username" in profile and "type" in profile:
                    profile["profile_url"] = (
                        f"https://www.{profile['type']}.com/{profile['username']}"
                    )
                else:
                    logging.warning(
                        f"Profile missing URL and username: {profile}"
                    )
                    continue

            operations.append(
                UpdateOne(
                    {"profile_url": profile["profile_url"]},
                    {"$set": profile},
                    upsert=True,
                )
            )

        try:
            result = self.db.profiles.bulk_write(operations, ordered=False)
            return result.upserted_count + result.modified_count
        except Exception as e:
            logging.error(f"Error inserting profiles: {e}")
            return 0

    def insert_serpapi_result(
        self, query: str, start: int, results: Dict
    ) -> bool:
        """Store SerpAPI results to avoid duplicate API calls"""
        try:
            document = {
                "query": query,
                "start": start,
                "results": results,
                "created_at": datetime.utcnow(),
            }
            self.db.serpapi_results.insert_one(document)
            return True
        except DuplicateKeyError:
            logging.info(
                f"SerpAPI result for query '{query}' with start {start} already exists"
            )
            return False
        except Exception as e:
            logging.error(f"Error storing SerpAPI result: {e}")
            return False

    def get_serpapi_result(self, query: str, start: int) -> Optional[Dict]:
        """Retrieve stored SerpAPI result if exists"""
        try:
            result = self.db.serpapi_results.find_one(
                {"query": query, "start": start}
            )
            return result["results"] if result else None
        except Exception as e:
            logging.error(f"Error retrieving SerpAPI result: {e}")
            return None

    def profile_exists(self, profile_url: str) -> bool:
        """Check if a profile already exists in the database"""
        try:
            return (
                self.db.profiles.count_documents({"profile_url": profile_url})
                > 0
            )
        except Exception as e:
            logging.error(f"Error checking profile existence: {e}")
            return False


class SocialMediaScraper:
    """Main scraper class for handling social media profile extraction"""

    def __init__(self):
        self.db = MongoDBClient(
            MONGO_URI, DATABASE_NAME, MONGO_USERNAME, MONGO_PASSWORD
        )
        print(MONGO_URI, DATABASE_NAME, MONGO_USERNAME, MONGO_PASSWORD)
        self.instagram_loader = instaloader.Instaloader()
        self.setup_instaloader()

    def setup_instaloader(self):
        """Configure Instaloader with random user agent for bot evasion"""
        random_user_agent = random.choice(USER_AGENTS)
        self.instagram_loader.context.user_agent = random_user_agent
        self.instagram_loader.context.delay_range = [MIN_DELAY, MAX_DELAY]

    def random_delay(
        self, min_delay: float = MIN_DELAY, max_delay: float = MAX_DELAY
    ):
        """Add a random delay between requests to avoid detection"""
        delay = random.uniform(min_delay, max_delay)
        logging.debug(f"Waiting for {delay:.2f} seconds")
        time.sleep(delay)

    def fetch_serpapi_urls(
        self, query: str, start: int = 0, domain: str = "instagram"
    ) -> List[str]:
        """
        Fetch URLs from Google Search via SerpAPI with caching
        Returns a list of profile URLs
        """
        # Check if we already have this result cached
        cached_result = self.db.get_serpapi_result(query, start)

        if cached_result:
            logging.info(
                f"Using cached SerpAPI result for {query} (start: {start})"
            )
            results = cached_result
        else:
            # Make API request if not cached
            logging.info(
                f"Fetching SerpAPI result for {query} (start: {start})"
            )

            params = {
                "q": query,
                "api_key": SERPAPI_KEY,
                "start": start,
                "num": 10,  # Number of results per page
            }

            try:
                search = GoogleSearch(params)
                results = search.get_dict()
                self.db.insert_serpapi_result(query, start, results)
                self.random_delay(
                    SERPAPI_DELAY, SERPAPI_DELAY + 1
                )  # Delay after API call
            except Exception as e:
                logging.error(f"Error fetching SerpAPI results: {e}")
                return []

        # Extract profile URLs from results
        urls = []
        if "organic_results" in results:
            for result in results["organic_results"]:
                link = result.get("link", "")
                if link and f"{domain}.com/" in link:
                    # Filter out post URLs and keep only profile URLs
                    if (
                        "/p/" not in link
                        and "/reel/" not in link
                        and "/tv/" not in link
                    ):
                        urls.append(link)

        return urls

    def extract_username_from_url(self, url: str) -> str:
        """Extract username from social media profile URL"""
        parsed_url = urlparse(url)
        path_parts = [part for part in parsed_url.path.split("/") if part]
        return path_parts[-1] if path_parts else ""

    def scrape_instagram_profile(self, url: str) -> Optional[Dict]:
        """Scrape Instagram profile data with error handling and retries"""
        username = self.extract_username_from_url(url)
        if not username:
            logging.warning(f"Could not extract username from URL: {url}")
            return None

        max_retries = 3
        for attempt in range(max_retries):
            try:
                profile = instaloader.Profile.from_username(
                    self.instagram_loader.context, username
                )

                profile_data = {
                    "username": profile.username,
                    "full_name": profile.full_name,
                    "followers": profile.followers,
                    "following": profile.followees,
                    "posts": profile.mediacount,
                    "bio": profile.biography,
                    "profile_url": f"https://www.instagram.com/{profile.username}/",
                    "type": "instagram",
                    "scraped_at": datetime.utcnow(),
                }

                logging.info(
                    f"Scraped Instagram profile: {profile.username} ({profile.followers} followers)"
                )
                return profile_data

            except instaloader.exceptions.ProfileNotExistsException:
                logging.warning(f"Instagram profile does not exist: {username}")
                return None
            except instaloader.exceptions.QueryReturnedBadRequestException:
                logging.warning(
                    f"Instagram blocked request for {username}, waiting before retry"
                )
                time.sleep(FAILURE_DELAY * (attempt + 1))
            except Exception as e:
                logging.warning(
                    f"Attempt {attempt + 1} failed for Instagram {username}: {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(FAILURE_DELAY * (attempt + 1))
                else:
                    logging.error(
                        f"All attempts failed for Instagram {username}: {e}"
                    )

        return None

    def scrape_tiktok_profile(self, url: str) -> Optional[Dict]:
        """
        Placeholder for TikTok profile scraping
        In a real implementation, you would use a TikTok API or web scraping approach
        """
        username = self.extract_username_from_url(url)
        if not username:
            return None

        # Simulate some delay as if we're making a real request
        self.random_delay()

        # In a real implementation, you would fetch actual TikTok profile data here
        profile_data = {
            "username": username,
            "profile_url": url,
            "type": "tiktok",
            "scraped_at": datetime.utcnow(),
        }

        logging.info(f"Added TikTok profile: {username}")
        return profile_data

    def scrape_profiles(
        self, platform: str, query: str, max_profiles: int
    ) -> int:
        """
        Main method to scrape profiles for a specific platform
        Returns the number of profiles successfully scraped
        """
        profiles_scraped = 0
        start = 0

        while profiles_scraped < max_profiles:
            # Fetch URLs from search results
            urls = self.fetch_serpapi_urls(query, start, platform)
            if not urls:
                logging.info(f"No more {platform} results from SerpAPI.")
                break

            # Process each URL
            for url in urls:
                if profiles_scraped >= max_profiles:
                    break

                # Check if we already have this profile
                if self.db.profile_exists(url):
                    logging.info(f"Profile already exists: {url}")
                    continue

                # Scrape profile based on platform
                if platform == "instagram":
                    profile_data = self.scrape_instagram_profile(url)
                    # Filter by minimum followers for Instagram
                    if (
                        profile_data
                        and profile_data.get("followers", 0) >= MIN_FOLLOWERS
                    ):
                        self.db.insert_profiles([profile_data])
                        profiles_scraped += 1
                else:  # tiktok
                    profile_data = self.scrape_tiktok_profile(url)
                    if profile_data:
                        self.db.insert_profiles([profile_data])
                        profiles_scraped += 1

                # Random delay between profile scrapes
                self.random_delay()

            # Move to next page of results
            start += 10

        return profiles_scraped

    def export_profiles_to_json(self, filename: str):
        """Export all profiles from database to a JSON file"""
        try:
            profiles = list(self.db.db.profiles.find({}, {"_id": 0}))
            with open(filename, "w") as f:
                json.dump(profiles, f, indent=2, default=str)
            logging.info(f"Exported {len(profiles)} profiles to {filename}")
        except Exception as e:
            logging.error(f"Error exporting profiles to JSON: {e}")


def main():
    """Main function to run the scraper"""
    try:
        scraper = SocialMediaScraper()

        # Scrape Instagram profiles
        logging.info("Starting Instagram scraping...")
        instagram_count = scraper.scrape_profiles(
            "instagram", INSTAGRAM_QUERY, MAX_PROFILES
        )
        logging.info(f"Scraped {instagram_count} Instagram profiles")

        # Scrape TikTok profiles
        logging.info("Starting TikTok scraping...")
        tiktok_count = scraper.scrape_profiles(
            "tiktok", TIKTOK_QUERY, MAX_PROFILES
        )
        logging.info(f"Scraped {tiktok_count} TikTok profiles")

        # Export results to JSON
        scraper.export_profiles_to_json("social_media_profiles.json")

    except Exception as e:
        logging.error(f"Failed to initialize scraper: {e}")
        logging.error("Please check your MongoDB connection and credentials")
    finally:
        logging.info("Scraping completed")


if __name__ == "__main__":
    main()
