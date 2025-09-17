import csv
import json
import logging
import os
import random
import re
import threading

# Add more aggressive rate limiting in your scraper
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import instaloader
import pymongo
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from flask import (
    Flask,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from pymongo import MongoClient, UpdateOne
from pymongo.errors import DuplicateKeyError, OperationFailure
from serpapi import GoogleSearch

load_dotenv()

# ---------------- CONFIG ----------------
SERPAPI_KEY = os.getenv("SERPAPI", "2s7017")
SERPERAPI_KEY = os.getenv("SERPERAPI", "2s7017")
MONGO_URI = os.getenv(
    "MONGO_URI", "mongodb://booking:booking@127.0.0.1:27017/scraper"
)
MONGO_USERNAME = os.getenv("MONGO_USERNAME", "")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "")
DATABASE_NAME = "social_scraper"
DEFAULT_INSTAGRAM_QUERY = "site:instagram.com intitle:NYC "
DEFAULT_TIKTOK_QUERY = "site:tiktok.com intitle:NYC "
MAX_PROFILES = 5000
MIN_FOLLOWERS = 5000

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

# Add to your CONFIG section
DECODO_PROXY_USERNAME = os.getenv("DECODO_PROXY_USERNAME", "spjclaqiw1")
DECODO_PROXY_PASSWORD = os.getenv("DECODO_PROXY_PASSWORD", "kvxda0KrTjNye8+U39")
DECODO_PROXY_GATEWAY = os.getenv("DECODO_PROXY_GATEWAY", "gate.decodo.com")
DECODO_PROXY_PORTS = os.getenv(
    "DECODO_PROXY_PORTS", "10001,10002,10003,10004"
).split(",")
DECODO_PROXY_ENABLED = (
    os.getenv("DECODO_PROXY_ENABLED", "true").lower() == "true"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("social_media_scraper.log"),
        logging.StreamHandler(),
    ],
)

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Global variables for scraping state
scraping_status = {
    "running": False,
    "platform": None,
    "current_query": None,
    "profiles_scraped": 0,
    "start_time": None,
    "end_time": None,
}


class ProxyManager:
    """Manage Decodo proxy rotation and configuration"""

    def __init__(
        self,
        enabled: bool,
        gateway: str,
        username: str,
        password: str,
        ports: List[str] = None,
    ):
        self.enabled = enabled
        self.gateway = gateway
        self.username = username
        self.password = password
        self.ports = ports or ["10001"]  # Default port if none provided
        self.current_proxy_index = 0
        self.session_duration = 1800  # 30 minutes session
        self.last_session_change = time.time()

    def get_proxy(self) -> Optional[Dict[str, str]]:
        """Get a proxy configuration for requests with rotation and session management"""
        if not self.enabled or not self.gateway:
            return None

        # Rotate session every 30 minutes or use port rotation
        current_time = time.time()
        if current_time - self.last_session_change > self.session_duration:
            self.current_proxy_index = (self.current_proxy_index + 1) % len(
                self.ports
            )
            self.last_session_change = current_time

        port = self.ports[self.current_proxy_index]
        proxy_url = (
            f"http://{self.username}:{self.password}@{self.gateway}:{port}"
        )

        return {
            "http": proxy_url,
            "https": proxy_url,
        }

    def get_proxy_for_instaloader(self) -> Optional[str]:
        """Get a proxy URL for Instaloader"""
        if not self.enabled or not self.gateway:
            return None

        port = self.ports[self.current_proxy_index % len(self.ports)]
        return f"http://{self.username}:{self.password}@{self.gateway}:{port}"

    def test_proxy_connection(self) -> bool:
        """Test if the proxy connection is working"""
        if not self.enabled:
            logging.info("Proxy is not enabled")
            return True

        test_url = "https://httpbin.org/ip"
        proxies = self.get_proxy()

        try:
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            }

            response = requests.get(
                test_url, proxies=proxies, headers=headers, timeout=15
            )
            response_data = response.json()
            logging.info(
                f"✅ Proxy test successful. Your IP is: {response_data.get('origin')}"
            )
            return True
        except Exception as e:
            logging.error(f"❌ Proxy test failed: {e}")
            # Test without proxy to compare
            try:
                response = requests.get(test_url, timeout=10)
                response_data = response.json()
                logging.info(
                    f"Direct connection IP: {response_data.get('origin')}"
                )
            except:
                pass
            return False

    def rotate_proxy(self):
        """Force rotate to next proxy"""
        self.current_proxy_index = (self.current_proxy_index + 1) % len(
            self.ports
        )
        logging.info(
            f"Rotated to proxy port: {self.ports[self.current_proxy_index]}"
        )


# Initialize the proxy manager
proxy_manager = ProxyManager(
    DECODO_PROXY_ENABLED,
    DECODO_PROXY_GATEWAY,
    DECODO_PROXY_USERNAME,
    DECODO_PROXY_PASSWORD,
    DECODO_PROXY_PORTS,
)


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
            # Index for search_tags collection
            self.db.search_tags.create_index(
                [("tag", pymongo.ASCENDING)], unique=True
            )
            # Index for scraping_sessions collection
            self.db.scraping_sessions.create_index(
                [("session_id", pymongo.ASCENDING)], unique=True
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

    def get_all_tags(self) -> List[Dict]:
        """Get all search tags from database"""
        try:
            tags = list(self.db.search_tags.find({}, {"_id": 0}))
            return tags
        except Exception as e:
            logging.error(f"Error retrieving tags: {e}")
            return []

    def add_tag(self, tag: str, platform: str) -> bool:
        """Add a new search tag"""
        try:
            self.db.search_tags.insert_one(
                {
                    "tag": tag,
                    "platform": platform,
                    "created_at": datetime.utcnow(),
                    "used": False,
                }
            )
            return True
        except DuplicateKeyError:
            logging.warning(f"Tag '{tag}' already exists")
            return False
        except Exception as e:
            logging.error(f"Error adding tag: {e}")
            return False

    def update_tag(self, old_tag: str, new_tag: str, platform: str) -> bool:
        """Update an existing tag"""
        try:
            result = self.db.search_tags.update_one(
                {"tag": old_tag},
                {"$set": {"tag": new_tag, "platform": platform}},
            )
            return result.modified_count > 0
        except Exception as e:
            logging.error(f"Error updating tag: {e}")
            return False

    def delete_tag(self, tag: str) -> bool:
        """Delete a tag"""
        try:
            result = self.db.search_tags.delete_one({"tag": tag})
            return result.deleted_count > 0
        except Exception as e:
            logging.error(f"Error deleting tag: {e}")
            return False

    def mark_tag_used(self, tag: str) -> bool:
        """Mark a tag as used"""
        try:
            result = self.db.search_tags.update_one(
                {"tag": tag},
                {"$set": {"used": True, "used_at": datetime.utcnow()}},
            )
            return result.modified_count > 0
        except Exception as e:
            logging.error(f"Error marking tag as used: {e}")
            return False

    def get_profiles(
        self, page: int = 1, per_page: int = 10
    ) -> Tuple[List[Dict], int]:
        """Get profiles with pagination"""
        try:
            skip = (page - 1) * per_page
            total = self.db.profiles.count_documents({})
            profiles = list(
                self.db.profiles.find({}, {"_id": 0}).skip(skip).limit(per_page)
            )
            return profiles, total
        except Exception as e:
            logging.error(f"Error retrieving profiles: {e}")
            return [], 0

    def save_scraping_session(self, session_data: Dict) -> bool:
        """Save scraping session data"""
        try:
            self.db.scraping_sessions.insert_one(session_data)
            return True
        except Exception as e:
            logging.error(f"Error saving scraping session: {e}")
            return False

    def get_last_scraping_session(self) -> Optional[Dict]:
        """Get the last scraping session"""
        try:
            session = self.db.scraping_sessions.find_one(
                {}, sort=[("start_time", pymongo.DESCENDING)]
            )
            return session
        except Exception as e:
            logging.error(f"Error retrieving scraping session: {e}")
            return None


class SocialMediaScraper:
    """Main scraper class for handling social media profile extraction"""

    def __init__(self, db_client: MongoDBClient):
        self.db = db_client
        self.instagram_loader = instaloader.Instaloader()
        self.setup_instaloader()

    def test_proxy_connection(self) -> bool:
        """Test if the proxy connection is working"""
        if not proxy_manager.enabled:
            logging.info("Proxy is not enabled")
            return True

        test_url = "https://httpbin.org/ip"
        proxies = proxy_manager.get_proxy()

        try:
            response = requests.get(test_url, proxies=proxies, timeout=10)
            response_data = response.json()
            logging.info(
                f"Proxy test successful. Your IP is: {response_data.get('origin')}"
            )
            return True
        except Exception as e:
            print(e)
            logging.error(f"Proxy test failed: {e}")
            return False

    def setup_instaloader(self):
        """Configure Instaloader with random user agent and proxy for bot evasion"""
        random_user_agent = random.choice(USER_AGENTS)
        self.instagram_loader.context.user_agent = random_user_agent
        self.instagram_loader.context.delay_range = [MIN_DELAY, MAX_DELAY]

        # Set up proxy for Instaloader if enabled
        proxy_url = proxy_manager.get_proxy_for_instaloader()
        if proxy_url:
            self.instagram_loader.context.proxy = proxy_url
            logging.info("Proxy configured for Instaloader")

    def random_delay(
        self, min_delay: float = MIN_DELAY, max_delay: float = MAX_DELAY
    ):
        """Add a random delay between requests to avoid detection"""
        delay = random.uniform(min_delay, max_delay)
        logging.debug(f"Waiting for {delay:.2f} seconds")
        time.sleep(delay)

    def safe_request(self, func, *args, **kwargs):
        """Wrapper function with exponential backoff"""
        max_retries = 5
        base_delay = 30  # Start with 30 seconds

        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if "401" in str(e) or "wait" in str(e).lower():
                    delay = base_delay * (2**attempt) + random.uniform(0, 5)
                    logging.warning(
                        f"Rate limited. Waiting {delay:.1f} seconds (attempt {attempt + 1})"
                    )
                    time.sleep(delay)
                else:
                    raise e
        raise Exception("Max retries exceeded")

    def export_profiles_to_csv(self, filename: str):
        """Export all profiles from database to a CSV file"""
        try:
            profiles = list(self.db.db.profiles.find({}, {"_id": 0}))

            if not profiles:
                logging.warning("No profiles to export")
                return

            # Define the CSV fieldnames based on profile structure
            fieldnames = [
                "username",
                "full_name",
                "followers",
                "following",
                "posts",
                "bio",
                "emails",  # Add emails field
                "profile_url",
                "type",
                "scraped_at",
            ]

            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

                for profile in profiles:
                    # Clean up the bio field by removing problematic characters
                    if "bio" in profile and profile["bio"]:
                        # Remove non-printable characters but keep emojis
                        profile["bio"] = "".join(
                            char
                            for char in profile["bio"]
                            if char.isprintable() or char in ["\n", "\t", "\r"]
                        )

                    # Convert emails list to string for CSV
                    if "emails" in profile and profile["emails"]:
                        profile["emails"] = ", ".join(profile["emails"])
                    else:
                        profile["emails"] = ""

                    writer.writerow(profile)

            logging.info(f"Exported {len(profiles)} profiles to {filename}")
        except Exception as e:
            logging.error(f"Error exporting profiles to CSV: {e}")

    def fetch_tiktok_serpapi_urls(
        self, query: str, start: int = 0
    ) -> List[str]:
        """Fetch TikTok profile URLs from SerpAPI with TikTok-specific filtering"""
        urls = self.fetch_serpapi_urls(query, start, "tiktok")

        # Filter out non-profile URLs more specifically for TikTok
        profile_urls = []
        for url in urls:
            # TikTok profile URLs typically have the format: https://www.tiktok.com/@username
            if "/@" in url and not any(
                x in url for x in ["/video/", "/tag/", "/music/"]
            ):
                profile_urls.append(url)

        return urls

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
                "location": "New York, United States",
                "google_domain": "google.com",
                "gl": "us",
                "hl": "en",
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

    def extract_emails_from_text(self, text: str) -> List[str]:
        """Extract email addresses from text using regex"""
        if not text:
            return []

        # Regex pattern for matching email addresses
        email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
        emails = re.findall(email_pattern, text)

        return emails

    def scrape_instagram_profile(self, url: str) -> Optional[Dict]:
        """Scrape Instagram profile data with enhanced proxy handling and retries"""
        username = self.extract_username_from_url(url)
        if not username:
            logging.warning(f"Could not extract username from URL: {url}")
            return None

        max_retries = 5
        for attempt in range(max_retries):
            try:
                # Rotate proxy every 2-3 requests to avoid detection
                if attempt > 0 and attempt % 2 == 0:
                    proxy_manager.rotate_proxy()
                    self.setup_instaloader()  # Re-setup instaloader with new proxy

                # Add progressive delays between retries
                if attempt > 0:
                    delay = min(
                        60, 5 * (2**attempt)
                    )  # Exponential backoff max 60s
                    logging.info(
                        f"Waiting {delay} seconds before retry {attempt + 1}"
                    )
                    time.sleep(delay + random.uniform(1, 3))

                profile = instaloader.Profile.from_username(
                    self.instagram_loader.context, username
                )

                # Extract emails from bio
                emails = self.extract_emails_from_text(profile.biography)

                profile_data = {
                    "username": profile.username,
                    "full_name": profile.full_name,
                    "followers": profile.followers,
                    "following": profile.followees,
                    "posts": profile.mediacount,
                    "bio": profile.biography,
                    "emails": emails,
                    "profile_url": f"https://www.instagram.com/{profile.username}/",
                    "type": "instagram",
                    "scraped_at": datetime.utcnow(),
                }

                logging.info(
                    f"✅ Successfully scraped Instagram: {profile.username} ({profile.followers} followers)"
                )
                return profile_data

            except instaloader.exceptions.ProfileNotExistsException:
                logging.warning(f"Instagram profile does not exist: {username}")
                return None
            except instaloader.exceptions.QueryReturnedBadRequestException as e:
                logging.warning(
                    f"Instagram blocked request for {username} (attempt {attempt + 1}): {e}"
                )
                # Rotate proxy on block
                proxy_manager.rotate_proxy()
                self.setup_instaloader()
            except instaloader.exceptions.ConnectionException as e:
                logging.warning(
                    f"Connection issue for {username} (attempt {attempt + 1}): {e}"
                )
                proxy_manager.rotate_proxy()
                self.setup_instaloader()
            except Exception as e:
                logging.warning(
                    f"Attempt {attempt + 1} failed for Instagram {username}: {e}"
                )
                if "401" in str(e) or "wait" in str(e).lower():
                    proxy_manager.rotate_proxy()
                    self.setup_instaloader()

        logging.error(f"All attempts failed for Instagram {username}")
        return None

    def scrape_tiktok_profile(self, url: str) -> Optional[Dict]:
        """Scrape TikTok profile data using web scraping with enhanced proxy support"""
        username = self.extract_username_from_url(url)
        if not username:
            logging.warning(f"Could not extract username from URL: {url}")
            return None

        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Set up headers to mimic a real browser
                headers = {
                    "User-Agent": random.choice(USER_AGENTS),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Accept-Encoding": "gzip, deflate",
                    "DNT": "1",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1",
                }

                # Get proxy configuration
                proxies = proxy_manager.get_proxy()

                # Add delay between requests
                time.sleep(random.uniform(2, 4))

                # Make request to TikTok profile with proxy support
                response = requests.get(
                    url, headers=headers, timeout=15, proxies=proxies
                )
                response.raise_for_status()

                # Parse HTML content
                soup = BeautifulSoup(response.content, "html.parser")

                # Look for the universal data script tag
                script_tag = soup.find(
                    "script", id="__UNIVERSAL_DATA_FOR_REHYDRATION__"
                )
                if not script_tag or not script_tag.text:
                    logging.warning(
                        f"No universal data script tag found for: {username}"
                    )
                    return None

                # Parse the JSON data
                try:
                    data = json.loads(script_tag.text)
                except json.JSONDecodeError as e:
                    logging.warning(
                        f"Failed to parse JSON data for {username}: {e}"
                    )
                    return None

                # Try multiple possible paths to extract user info
                user_info = None

                # Path 1: Try the new structure (as seen in your HTML)
                try:
                    user_info = (
                        data.get("__DEFAULT_SCOPE__", {})
                        .get("webapp.user-detail", {})
                        .get("userInfo", {})
                        .get("user", {})
                    )
                except (KeyError, AttributeError):
                    pass

                # Path 2: Try alternative structure for profile pages
                if not user_info:
                    try:
                        user_info = (
                            data.get("__DEFAULT_SCOPE__", {})
                            .get("webapp.user-detail", {})
                            .get("user", {})
                        )
                    except (KeyError, AttributeError):
                        pass

                # Path 3: Try another common structure
                if not user_info:
                    try:
                        # Look for user data in other possible locations
                        for key in data.keys():
                            if (
                                "user" in key.lower()
                                or "profile" in key.lower()
                            ):
                                user_info = data[key].get("user", {})
                                if user_info:
                                    break
                    except (KeyError, AttributeError):
                        pass

                # If we still can't find user info, try extracting from meta tags
                if not user_info:
                    user_info = {}
                    # Extract from meta tags as fallback
                    meta_tags = soup.find_all("meta")
                    for meta in meta_tags:
                        property_attr = meta.get("property", "")
                        content = meta.get("content", "")

                        if property_attr == "og:title":
                            # Extract username from title (e.g., "User (@username) on TikTok")
                            title_parts = content.split("(@")
                            if len(title_parts) > 1:
                                user_info["nickname"] = title_parts[0].strip()
                                user_info["uniqueId"] = (
                                    title_parts[1].split(")")[0].strip()
                                )
                        elif property_attr == "og:description":
                            user_info["signature"] = content

                # If we still don't have basic info, try to extract from the page
                if not user_info.get("uniqueId"):
                    user_info["uniqueId"] = username

                if not user_info.get("nickname"):
                    # Try to get nickname from title tag
                    title_tag = soup.find("title")
                    if title_tag:
                        title_text = title_tag.text
                        if "on TikTok" in title_text:
                            user_info["nickname"] = title_text.split(
                                "on TikTok"
                            )[0].strip()
                        else:
                            user_info["nickname"] = username

                # Try to extract stats from the page content
                stats = user_info.get("stats", {})

                # Look for follower count in the HTML
                follower_text = soup.find(
                    string=re.compile(r"followers", re.IGNORECASE)
                )
                if follower_text:
                    # Try to find numbers near the follower text
                    numbers = re.findall(r"[\d,]+", follower_text)
                    if numbers:
                        stats["followerCount"] = int(
                            numbers[0].replace(",", "")
                        )

                # Look for following count
                following_text = soup.find(
                    string=re.compile(r"following", re.IGNORECASE)
                )
                if following_text:
                    numbers = re.findall(r"[\d,]+", following_text)
                    if numbers:
                        stats["followingCount"] = int(
                            numbers[0].replace(",", "")
                        )

                # Look for post count
                posts_text = soup.find(
                    string=re.compile(r"posts|videos", re.IGNORECASE)
                )
                if posts_text:
                    numbers = re.findall(r"[\d,]+", posts_text)
                    if numbers:
                        stats["videoCount"] = int(numbers[0].replace(",", ""))

                # Extract bio/signature
                if "signature" not in user_info:
                    # Try to find bio in the page
                    bio_elements = soup.find_all(
                        ["p", "div"],
                        class_=re.compile(
                            r"bio|description|signature", re.IGNORECASE
                        ),
                    )
                    if bio_elements:
                        user_info["signature"] = bio_elements[0].get_text(
                            strip=True
                        )
                    else:
                        user_info["signature"] = ""

                # Extract emails from bio
                emails = self.extract_emails_from_text(
                    user_info.get("signature", "")
                )

                # Create profile data with fallback values
                profile_data = {
                    "username": user_info.get("uniqueId", username),
                    "full_name": user_info.get("nickname", username),
                    "followers": stats.get("followerCount", 0),
                    "following": stats.get("followingCount", 0),
                    "emails": emails,
                    "posts": stats.get("videoCount", 0),
                    "bio": user_info.get("signature", ""),
                    "profile_url": url,
                    "type": "tiktok",
                    "scraped_at": datetime.utcnow(),
                }

                logging.info(
                    f"Scraped TikTok profile: {profile_data['username']} ({profile_data['followers']} followers)"
                )
                return profile_data

            except requests.exceptions.RequestException as e:
                logging.warning(
                    f"Attempt {attempt + 1} failed for TikTok {username}: {e}"
                )
                # Rotate proxy on failure
                proxy_manager.rotate_proxy()
                time.sleep(FAILURE_DELAY * (attempt + 1))
            except (json.JSONDecodeError, KeyError) as e:
                logging.warning(
                    f"Failed to parse TikTok data for {username}: {e}"
                )
                # Try to debug by saving the HTML
                if attempt == 0:
                    debug_filename = f"tiktok_debug_{username}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
                    with open(debug_filename, "w", encoding="utf-8") as f:
                        f.write(str(soup))
                    logging.info(f"Saved debug HTML to {debug_filename}")
                return None
            except Exception as e:
                logging.warning(
                    f"Unexpected error scraping TikTok {username}: {e}"
                )
                proxy_manager.rotate_proxy()
                time.sleep(FAILURE_DELAY * (attempt + 1))

        return None

    def scrape_profiles(
        self, platform: str, query: str, max_profiles: int, update_callback=None
    ) -> int:
        """Main method to scrape profiles for a specific platform"""
        profiles_scraped = 0
        start = 0

        # Mark tag as used
        self.db.mark_tag_used(query)

        while profiles_scraped < max_profiles:
            # Fetch URLs from search results - use platform-specific method for TikTok
            if platform == "tiktok":
                urls = self.fetch_tiktok_serpapi_urls(query, start)
            else:
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
                        if update_callback:
                            update_callback(profiles_scraped, profile_data)
                else:  # tiktok
                    profile_data = self.scrape_tiktok_profile(url)
                    if profile_data:
                        self.db.insert_profiles([profile_data])
                        profiles_scraped += 1
                        if update_callback:
                            update_callback(profiles_scraped, profile_data)

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


# Initialize database and scraper
db_client = MongoDBClient(
    MONGO_URI, DATABASE_NAME, MONGO_USERNAME, MONGO_PASSWORD
)
scraper = SocialMediaScraper(db_client)

scraper.test_proxy_connection()

# Add default tags if they don't exist
if not any(
    tag["tag"] == DEFAULT_INSTAGRAM_QUERY for tag in db_client.get_all_tags()
):
    db_client.add_tag(DEFAULT_INSTAGRAM_QUERY, "instagram")

if not any(
    tag["tag"] == DEFAULT_TIKTOK_QUERY for tag in db_client.get_all_tags()
):
    db_client.add_tag(DEFAULT_TIKTOK_QUERY, "tiktok")


# Flask Routes
@app.route("/")
def index():
    tags = db_client.get_all_tags()
    profiles, total_profiles = db_client.get_profiles()
    last_session = db_client.get_last_scraping_session()

    return render_template(
        "index.html",
        tags=tags,
        profiles=profiles,
        total_profiles=total_profiles,
        scraping_status=scraping_status,
        last_session=last_session,
        MAX_PROFILES=MAX_PROFILES,
    )  # Add this line


@app.route("/add_tag", methods=["POST"])
def add_tag():
    tag = request.form.get("tag")
    platform = request.form.get("platform")

    if tag and platform:
        if db_client.add_tag(tag, platform):
            flash("Tag added successfully", "success")
        else:
            flash("Failed to add tag. It may already exist.", "error")
    else:
        flash("Both tag and platform are required", "error")

    return redirect(url_for("index"))


@app.route("/edit_tag", methods=["POST"])
def edit_tag():
    old_tag = request.form.get("old_tag")
    new_tag = request.form.get("new_tag")
    platform = request.form.get("platform")

    if old_tag and new_tag and platform:
        if db_client.update_tag(old_tag, new_tag, platform):
            flash("Tag updated successfully", "success")
        else:
            flash("Failed to update tag", "error")
    else:
        flash("All fields are required", "error")

    return redirect(url_for("index"))


@app.route("/delete_tag/<tag>")
def delete_tag(tag):
    if db_client.delete_tag(tag):
        flash("Tag deleted successfully", "success")
    else:
        flash("Failed to delete tag", "error")

    return redirect(url_for("index"))


@app.route("/start_scraping", methods=["POST"])
def start_scraping():
    if scraping_status["running"]:
        flash("Scraping is already in progress", "error")
        return redirect(url_for("index"))

    platform = request.form.get("platform")
    query = request.form.get("query")
    max_profiles = int(request.form.get("max_profiles", MAX_PROFILES))

    if not platform or not query:
        flash("Platform and query are required", "error")
        return redirect(url_for("index"))

    # Start scraping in a separate thread
    def scrape_thread():
        global scraping_status
        scraping_status = {
            "running": True,
            "platform": platform,
            "current_query": query,
            "profiles_scraped": 0,
            "start_time": datetime.now().isoformat(),
            "end_time": None,
        }

        def update_callback(count, profile):
            global scraping_status
            scraping_status["profiles_scraped"] = count

        try:
            count = scraper.scrape_profiles(
                platform, query, max_profiles, update_callback
            )

            # Save session data
            session_data = {
                "session_id": datetime.now().timestamp(),
                "platform": platform,
                "query": query,
                "max_profiles": max_profiles,
                "profiles_scraped": count,
                "start_time": scraping_status["start_time"],
                "end_time": datetime.now().isoformat(),
                "status": "completed",
            }
            db_client.save_scraping_session(session_data)

        except Exception as e:
            logging.error(f"Scraping failed: {e}")

        finally:
            scraping_status["running"] = False
            scraping_status["end_time"] = datetime.now().isoformat()

    thread = threading.Thread(target=scrape_thread)
    thread.daemon = True
    thread.start()

    flash("Scraping started successfully", "success")
    return redirect(url_for("index"))


@app.route("/stop_scraping")
def stop_scraping():
    global scraping_status
    if scraping_status["running"]:
        # This is a simple implementation - in a real scenario you'd need a way to stop the scraper
        scraping_status["running"] = False
        flash("Scraping will stop after current operation completes", "info")
    else:
        flash("No scraping is currently running", "error")

    return redirect(url_for("index"))


@app.route("/get_status")
def get_status():
    profiles, total = db_client.get_profiles()
    return jsonify(
        {"scraping_status": scraping_status, "total_profiles": total}
    )


@app.route("/profiles")
def profiles():
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 10))

    profiles_list, total = db_client.get_profiles(page, per_page)
    total_pages = (total + per_page - 1) // per_page

    return render_template(
        "profiles.html",
        profiles=profiles_list,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        total=total,
    )


@app.route("/export_profiles")
def export_profiles():
    filename = (
        f"social_media_profiles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    scraper.export_profiles_to_csv(filename)

    # Return the file for download
    return send_file(
        filename,
        as_attachment=True,
        download_name=filename,
        mimetype="text/csv",
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
