# Source Code Context

Generated on: 2025-09-15T08:56:00Z

## Repository Overview
- Total Files: 6
- Total Size: 33218 bytes

## Directory Structure
```
context/
  images/
scraper/
  .env
  __init__.py
  __pycache__/
  main.py
  requirements.txt
  social_media_profiles.json
  social_media_scraper.log

```

## File Contents


### File: scraper/.env

```
USE_MONGODB=true
MONGO_URI=mongodb://booking:booking@127.0.0.1:27017
DATABASE_NAME=lead_generation
COLLECTION_NAME=instagram_leads
REQUEST_DELAY_MS=2000
SCRAPER_KEYWORDS=designer,photographer,artist,entrepreneur
SERPAPI=0e8bfd8bc7d419173b41f90a27501cff53365e87c1eb1c3c5605847b3e19a068

```





### File: scraper/__init__.py

```python

```





### File: scraper/main.py

```python
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

```





### File: scraper/requirements.txt

```
playwright
fastapi
uvicorn
motor
pymongo
python-dotenv
email-validator
python-multipart
jinja2
aiohttp
instaloader
bs4

```





### File: scraper/social_media_profiles.json

```json
[
  {
    "profile_url": "https://www.instagram.com/lindanguyenlee/",
    "bio": "lifestyle vlogs | fashion | beauty | travel \u2728\nall things nyc + events @lindaxnyc \n\ud83d\udccdnyc \ud83d\udd1c NYFW\n\ud83d\udc8c linda@theviralistgroup.com \ud83e\udd2d\n\ud83d\udd17 \u2935\ufe0f",
    "followers": 21016,
    "following": 1997,
    "full_name": "linda \u2022 nyc blogger \u2022 influencer",
    "posts": 1199,
    "scraped_at": "2025-09-14 11:41:09.843000",
    "type": "instagram",
    "username": "lindanguyenlee"
  },
  {
    "profile_url": "https://www.instagram.com/lindaxnyc/",
    "bio": "nyc freebies & popups \ud83c\udf4e\npersonal acct: @lindanguyenlee \ud83d\udc97\nexperiences, food, lifestyle vlogs \ntiktok: 53k \ud83d\ude2d\n\ud83d\udc8c linda@theviralistgroup.com",
    "followers": 45728,
    "following": 920,
    "full_name": "linda from nyc | nyc influencer | nyc experiences",
    "posts": 618,
    "scraped_at": "2025-09-14 11:41:15.794000",
    "type": "instagram",
    "username": "lindaxnyc"
  },
  {
    "profile_url": "https://www.instagram.com/lighttravelsfaster/",
    "bio": "\ud835\ude49\ud835\ude5a\ud835\ude6c\ud835\ude54\ud835\ude64\ud835\ude67\ud835\ude60\ud835\ude3e\ud835\ude5e\ud835\ude69\ud835\ude6e\ud835\ude4d\ud835\ude5a\ud835\ude63\ud835\ude56\ud835\ude5e\ud835\ude68\ud835\ude68\ud835\ude56\ud835\ude63\ud835\ude58\ud835\ude5a\ud835\ude52\ud835\ude64\ud835\ude62\ud835\ude56\ud835\ude63\n\u2728Luxury Travel Expert + Lifestyle, Fashion\ud83d\udccdNYC\n\u2696\ufe0fWallStLawyer\u22c6TVHost\u22c6Write @matadornetwork\n\ud83d\udd1c#NYFW \u22c6\ud835\udff4\ud835\udfed+\ud835\uddd6\ud835\uddfc\ud835\ude02\ud835\uddfb\ud835\ude01\ud835\uddff\ud835\uddf6\ud835\uddf2\ud835\ude00\ud83d\udc69\ud83c\udffc\u200d\ud83c\udf93NYULaw",
    "followers": 349980,
    "following": 5818,
    "full_name": "Eileen Rhein, Esq.\u2728NYC Influencer\u2728Travel, Fashion",
    "posts": 2119,
    "scraped_at": "2025-09-14 11:41:25.396000",
    "type": "instagram",
    "username": "lighttravelsfaster"
  },
  {
    "profile_url": "https://www.instagram.com/toosha_z/",
    "bio": "\u2708\ufe0f Unlocking unforgettable NYC adventures & global travel tips\ud83d\uddfd\ud83c\udf0d\n\ud83c\udf1fInspiring you to explore and discover hidden gems\n\ud83d\udcf8 Join me for all the best tips",
    "followers": 22400,
    "following": 2532,
    "full_name": "Toosha Z | NYC travel influencer | content creator",
    "posts": 1294,
    "scraped_at": "2025-09-14 11:41:34.169000",
    "type": "instagram",
    "username": "toosha_z"
  },
  {
    "profile_url": "https://www.instagram.com/thecreativegentleman/",
    "bio": "\ud83d\udccdNYC | DC\nMen\u2019s Style | Travel | Fatherhood | UGC Creator\n\ud83d\udce9 info@thecreativegentleman.net\n\u201cStyle. Creativity. Fatherhood. Redefined\u201d",
    "followers": 42775,
    "following": 3262,
    "full_name": "Oneil Gardner | NYC & DC Influencer",
    "posts": 1386,
    "scraped_at": "2025-09-14 11:41:45.963000",
    "type": "instagram",
    "username": "thecreativegentleman"
  },
  {
    "profile_url": "https://www.instagram.com/kaishacreates/",
    "bio": "\ud83d\udd0c Your go to guide for NYC arts & culture via theatre, tv/film, and fashion\n\ud83d\udc8c hello@kaishacreates.com\n\ud83c\udf1f Actor, Host & Digital Creator",
    "followers": 23877,
    "following": 3616,
    "full_name": "Kaisha Huguley | NYC Influencer",
    "posts": 1705,
    "scraped_at": "2025-09-14 11:41:53.624000",
    "type": "instagram",
    "username": "kaishacreates"
  },
  {
    "profile_url": "https://www.instagram.com/thedianaedelman/",
    "bio": "\ud83d\udccdNYC \ud83c\udf31 Award-winning plant-based travel \ud83e\uddf3 & food curator sharing vegan eats \ud83c\udfc6 James Beard judge \ud83d\udcfa Host of COX's The Good Fork \ud83d\udce7 diana@vegansbaby.com",
    "followers": 31049,
    "following": 1239,
    "full_name": "Diana Edelman \ud83c\udf31 AKA Vegans, Baby\ud83d\uddfd NYC influencer",
    "posts": 3210,
    "scraped_at": "2025-09-14 11:42:02.249000",
    "type": "instagram",
    "username": "thedianaedelman"
  },
  {
    "profile_url": "https://www.instagram.com/vickirutwind/",
    "bio": "\u2708\ufe0f Travel + life in NYC at age 41. \n\ud83d\udc8d Married. No kids. \ud83d\udc36 Dog mom to @louie_poochon.\n\ud83d\udc8c vicki@fashiontravelrepeat.com",
    "followers": 131972,
    "following": 1113,
    "full_name": "Vicki \u2708\ufe0f Travel + NYC Lifestyle",
    "posts": 2013,
    "scraped_at": "2025-09-14 11:42:06.415000",
    "type": "instagram",
    "username": "vickirutwind"
  },
  {
    "profile_url": "https://www.instagram.com/leahmarie.nyc/",
    "bio": "\ud83c\uddf5\ud83c\udded X \ud83c\uddee\ud83c\uddea\nNYC based | Exploring NYC & beyond\nFun getaways, hidden gems & tasty  bites\n\ud83d\udc8c: leahmarietravels@gmail.com",
    "followers": 9990,
    "following": 7494,
    "full_name": "Leah | NYC Influencer | Travel, Food + Fashion",
    "posts": 908,
    "scraped_at": "2025-09-14 11:42:21.650000",
    "type": "instagram",
    "username": "leahmarie.nyc"
  },
  {
    "profile_url": "https://www.tiktok.com/discover/nyc-lifestyle-influencer",
    "scraped_at": "2025-09-14 11:42:37.394000",
    "type": "tiktok",
    "username": "nyc-lifestyle-influencer"
  },
  {
    "profile_url": "https://www.tiktok.com/discover/nyc-micro-influencers",
    "scraped_at": "2025-09-14 11:42:44.967000",
    "type": "tiktok",
    "username": "nyc-micro-influencers"
  },
  {
    "profile_url": "https://www.tiktok.com/discover/nyc-family-influencers",
    "scraped_at": "2025-09-14 11:42:51.864000",
    "type": "tiktok",
    "username": "nyc-family-influencers"
  },
  {
    "profile_url": "https://www.tiktok.com/@daadisnacks/video/7514367598155369759",
    "scraped_at": "2025-09-14 11:42:58.005000",
    "type": "tiktok",
    "username": "7514367598155369759"
  },
  {
    "profile_url": "https://www.tiktok.com/@crysscharms",
    "scraped_at": "2025-09-14 11:43:05.608000",
    "type": "tiktok",
    "username": "@crysscharms"
  },
  {
    "profile_url": "https://www.tiktok.com/discover/nyc-influencer-day-in-my-life",
    "scraped_at": "2025-09-14 11:43:13.273000",
    "type": "tiktok",
    "username": "nyc-influencer-day-in-my-life"
  },
  {
    "profile_url": "https://www.tiktok.com/@oldloserinbrooklyn/video/7479986328659135774",
    "scraped_at": "2025-09-14 11:43:19.545000",
    "type": "tiktok",
    "username": "7479986328659135774"
  },
  {
    "profile_url": "https://www.tiktok.com/discover/nyc-influencer-male-vloggers",
    "scraped_at": "2025-09-14 11:43:25.836000",
    "type": "tiktok",
    "username": "nyc-influencer-male-vloggers"
  },
  {
    "profile_url": "https://www.tiktok.com/discover/influencer-bars-nyc",
    "scraped_at": "2025-09-14 11:43:40.589000",
    "type": "tiktok",
    "username": "influencer-bars-nyc"
  }
]
```





### File: scraper/social_media_scraper.log

```
2025-09-14 11:11:09,203 - ERROR - Failed to connect to MongoDB: localhost:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms), Timeout: 30s, Topology Description: <TopologyDescription id: 68c6a2af0b5e9154f22a9d8c, topology_type: Unknown, servers: [<ServerDescription ('localhost', 27017) server_type: Unknown, rtt: None, error=AutoReconnect('localhost:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms)')>]>
2025-09-14 11:11:09,204 - ERROR - Failed to initialize scraper: localhost:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms), Timeout: 30s, Topology Description: <TopologyDescription id: 68c6a2af0b5e9154f22a9d8c, topology_type: Unknown, servers: [<ServerDescription ('localhost', 27017) server_type: Unknown, rtt: None, error=AutoReconnect('localhost:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms)')>]>
2025-09-14 11:11:09,204 - ERROR - Please check your MongoDB connection and credentials
2025-09-14 11:11:09,204 - INFO - Scraping completed
2025-09-14 11:13:01,552 - ERROR - Failed to connect to MongoDB: localhost:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms), Timeout: 30s, Topology Description: <TopologyDescription id: 68c6a31f408976f96cb0ba44, topology_type: Unknown, servers: [<ServerDescription ('localhost', 27017) server_type: Unknown, rtt: None, error=AutoReconnect('localhost:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms)')>]>
2025-09-14 11:13:01,553 - ERROR - Failed to initialize scraper: localhost:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms), Timeout: 30s, Topology Description: <TopologyDescription id: 68c6a31f408976f96cb0ba44, topology_type: Unknown, servers: [<ServerDescription ('localhost', 27017) server_type: Unknown, rtt: None, error=AutoReconnect('localhost:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms)')>]>
2025-09-14 11:13:01,553 - ERROR - Please check your MongoDB connection and credentials
2025-09-14 11:13:01,553 - INFO - Scraping completed
2025-09-14 11:14:37,729 - ERROR - Failed to connect to MongoDB: localhost:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms), Timeout: 30s, Topology Description: <TopologyDescription id: 68c6a37fe9c8d11549a30129, topology_type: Unknown, servers: [<ServerDescription ('localhost', 27017) server_type: Unknown, rtt: None, error=AutoReconnect('localhost:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms)')>]>
2025-09-14 11:14:37,729 - ERROR - Failed to initialize scraper: localhost:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms), Timeout: 30s, Topology Description: <TopologyDescription id: 68c6a37fe9c8d11549a30129, topology_type: Unknown, servers: [<ServerDescription ('localhost', 27017) server_type: Unknown, rtt: None, error=AutoReconnect('localhost:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms)')>]>
2025-09-14 11:14:37,729 - ERROR - Please check your MongoDB connection and credentials
2025-09-14 11:14:37,730 - INFO - Scraping completed
2025-09-14 11:16:43,807 - ERROR - Failed to connect to MongoDB: 127.0.0.1:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms), Timeout: 30s, Topology Description: <TopologyDescription id: 68c6a3fd0f510c956a72b2ac, topology_type: Unknown, servers: [<ServerDescription ('127.0.0.1', 27017) server_type: Unknown, rtt: None, error=AutoReconnect('127.0.0.1:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms)')>]>
2025-09-14 11:16:43,807 - ERROR - Failed to initialize scraper: 127.0.0.1:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms), Timeout: 30s, Topology Description: <TopologyDescription id: 68c6a3fd0f510c956a72b2ac, topology_type: Unknown, servers: [<ServerDescription ('127.0.0.1', 27017) server_type: Unknown, rtt: None, error=AutoReconnect('127.0.0.1:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms)')>]>
2025-09-14 11:16:43,807 - ERROR - Please check your MongoDB connection and credentials
2025-09-14 11:16:43,807 - INFO - Scraping completed
2025-09-14 11:35:53,902 - ERROR - Failed to connect to MongoDB: 127.0.0.1:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms), Timeout: 30s, Topology Description: <TopologyDescription id: 68c6a87bcd316430a8f77b33, topology_type: Unknown, servers: [<ServerDescription ('127.0.0.1', 27017) server_type: Unknown, rtt: None, error=AutoReconnect('127.0.0.1:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms)')>]>
2025-09-14 11:35:53,902 - ERROR - Failed to initialize scraper: 127.0.0.1:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms), Timeout: 30s, Topology Description: <TopologyDescription id: 68c6a87bcd316430a8f77b33, topology_type: Unknown, servers: [<ServerDescription ('127.0.0.1', 27017) server_type: Unknown, rtt: None, error=AutoReconnect('127.0.0.1:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms)')>]>
2025-09-14 11:35:53,902 - ERROR - Please check your MongoDB connection and credentials
2025-09-14 11:35:53,902 - INFO - Scraping completed
2025-09-14 11:38:43,759 - INFO - Scraping completed
2025-09-14 11:39:35,574 - ERROR - Failed to connect to MongoDB: 127.0.0.1:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms), Timeout: 30s, Topology Description: <TopologyDescription id: 68c6a95911b8277f004396b2, topology_type: Unknown, servers: [<ServerDescription ('127.0.0.1', 27017) server_type: Unknown, rtt: None, error=AutoReconnect('127.0.0.1:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms)')>]>
2025-09-14 11:39:35,574 - ERROR - Failed to initialize scraper: 127.0.0.1:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms), Timeout: 30s, Topology Description: <TopologyDescription id: 68c6a95911b8277f004396b2, topology_type: Unknown, servers: [<ServerDescription ('127.0.0.1', 27017) server_type: Unknown, rtt: None, error=AutoReconnect('127.0.0.1:27017: [Errno 111] Connection refused (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms)')>]>
2025-09-14 11:39:35,574 - ERROR - Please check your MongoDB connection and credentials
2025-09-14 11:39:35,575 - INFO - Scraping completed
2025-09-14 11:40:53,573 - INFO - MongoDB indexes created successfully
2025-09-14 11:40:53,573 - INFO - Successfully connected to MongoDB
2025-09-14 11:40:53,574 - INFO - Starting Instagram scraping...
2025-09-14 11:40:53,575 - INFO - Fetching SerpAPI result for site:instagram.com intitle:NYC influencer (start: 0)
2025-09-14 11:41:09,843 - INFO - Scraped Instagram profile: lindanguyenlee (21016 followers)
2025-09-14 11:41:15,794 - INFO - Scraped Instagram profile: lindaxnyc (45728 followers)
2025-09-14 11:41:25,396 - INFO - Scraped Instagram profile: lighttravelsfaster (349980 followers)
2025-09-14 11:41:34,169 - INFO - Scraped Instagram profile: toosha_z (22400 followers)
2025-09-14 11:41:45,963 - INFO - Scraped Instagram profile: thecreativegentleman (42775 followers)
2025-09-14 11:41:53,624 - INFO - Scraped Instagram profile: kaishacreates (23877 followers)
2025-09-14 11:42:02,249 - INFO - Scraped Instagram profile: thedianaedelman (31049 followers)
2025-09-14 11:42:06,415 - INFO - Scraped Instagram profile: vickirutwind (131972 followers)
2025-09-14 11:42:10,431 - INFO - Fetching SerpAPI result for site:instagram.com intitle:NYC influencer (start: 10)
2025-09-14 11:42:21,650 - INFO - Scraped Instagram profile: leahmarie.nyc (9990 followers)
2025-09-14 11:42:26,446 - INFO - Scraped 9 Instagram profiles
2025-09-14 11:42:26,446 - INFO - Starting TikTok scraping...
2025-09-14 11:42:26,447 - INFO - Fetching SerpAPI result for site:tiktok.com intitle:NYC influencer (start: 0)
2025-09-14 11:42:37,394 - INFO - Added TikTok profile: nyc-lifestyle-influencer
2025-09-14 11:42:44,967 - INFO - Added TikTok profile: nyc-micro-influencers
2025-09-14 11:42:51,865 - INFO - Added TikTok profile: nyc-family-influencers
2025-09-14 11:42:58,005 - INFO - Added TikTok profile: 7514367598155369759
2025-09-14 11:43:05,608 - INFO - Added TikTok profile: @crysscharms
2025-09-14 11:43:13,274 - INFO - Added TikTok profile: nyc-influencer-day-in-my-life
2025-09-14 11:43:19,545 - INFO - Added TikTok profile: 7479986328659135774
2025-09-14 11:43:25,836 - INFO - Added TikTok profile: nyc-influencer-male-vloggers
2025-09-14 11:43:28,529 - INFO - Fetching SerpAPI result for site:tiktok.com intitle:NYC influencer (start: 10)
2025-09-14 11:43:40,589 - INFO - Added TikTok profile: influencer-bars-nyc
2025-09-14 11:43:43,635 - INFO - Scraped 9 TikTok profiles
2025-09-14 11:43:43,637 - INFO - Exported 18 profiles to social_media_profiles.json
2025-09-14 11:43:43,637 - INFO - Scraping completed

```




