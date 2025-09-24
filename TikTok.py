import json
import os
import re
import time
from datetime import datetime

import pymongo
import requests
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
load_dotenv()


class TikTokProfileExtractor:
    def __init__(self):
        # API keys
        self.brightdata_tag_api_key = os.getenv("BRIGHTDATA_TAG_API_KEY")
        self.brightdata_profile_api_key = os.getenv(
            "BRIGHTDATA_PROFILE_API_KEY"
        )
        self.mongo_uri = os.getenv("MONGO_URI")

        # Dataset IDs
        self.hashtag_dataset_id = os.getenv(
            "HASHTAG_DATASET_ID", "gd_lu702nij2f790tmv9h"
        )
        self.profile_dataset_id = os.getenv(
            "PROFILE_DATASET_ID", "gd_l1villgoiiidt09ci"
        )

        # Initialize MongoDB
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client.tiktok_scraper_testing

        # Collections
        self.posts_collection = self.db.posts
        self.profiles_collection = self.db.profiles
        self.searches_collection = self.db.searches
        self.raw_collection = self.db.raw_snapshots

        # Create indexes
        self.profiles_collection.create_index("username", unique=True)
        self.posts_collection.create_index("post_id", unique=True)
        self.searches_collection.create_index(
            [("keyword", 1), ("timestamp", 1)]
        )

        # Target profiles count
        self.target_profiles_count = 5000

        # Keywords for searching
        self.primary_keywords = [
            "NYC",
            "New York City",
            "New York",
            "NY",
            "Manhattan",
            "Brooklyn",
            "Queens",
            "Bronx",
            "Staten Island",
            "NYC Life",
        ]
        self.discovered_keywords = set()

        # Email regex pattern
        self.email_pattern = re.compile(
            r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
        )

        # Control variables
        self.is_running = True

    def log_message(self, message):
        """Print log message with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}")

    def extract_emails_from_text(self, text):
        """Extract emails from text using regex"""
        if not text:
            return []
        return self.email_pattern.findall(text)

    def extract_keywords_from_bio(self, bio):
        """Extract potential location keywords from bio"""
        if not bio:
            return []

        keywords = set()

        # Common location indicators
        location_indicators = [
            "based in",
            "located in",
            "from",
            "living in",
            "📍",
            "🏠",
            "city:",
            "location:",
            "area:",
            "based:",
            "📍",
            "🏙️",
        ]

        # Convert to lowercase for matching
        bio_lower = bio.lower()

        # Look for location patterns
        for indicator in location_indicators:
            if indicator in bio_lower:
                # Extract the next word after indicator
                start_idx = bio_lower.find(indicator) + len(indicator)
                remaining_text = bio_lower[start_idx:].strip()
                if remaining_text:
                    next_word = remaining_text.split()[0]
                    # Clean the word and capitalize
                    clean_word = re.sub(r"[^a-zA-Z]", "", next_word).title()
                    if len(clean_word) > 2:
                        keywords.add(clean_word)

        # Also look for any capitalized words that might be locations
        words = bio.split()
        for word in words:
            if (
                len(word) > 2
                and word[0].isupper()
                and not word.startswith(("http", "www", "@"))
            ):
                clean_word = re.sub(r"[^a-zA-Z]", "", word)
                if len(clean_word) > 2:
                    keywords.add(clean_word)

        return list(keywords)

    def process_post_data(self, post_data):
        """Process individual post data to extract profile information"""
        try:
            # Extract profile information
            username = post_data.get("profile_username")
            if not username:
                return None

            # Check if profile already exists
            if self.profiles_collection.find_one({"username": username}):
                return None

            # Extract profile data
            bio = post_data.get("profile_biography", "") or ""
            emails = self.extract_emails_from_text(bio)

            # Extract followers count if available
            followers = post_data.get("profile_followers")
            if followers and isinstance(followers, str):
                try:
                    # Convert string followers to int (remove commas, etc.)
                    followers = int(re.sub(r"[^\d]", "", followers))
                except (ValueError, TypeError):
                    followers = None

            # Extract additional keywords from bio
            discovered_keywords = self.extract_keywords_from_bio(bio)
            for keyword in discovered_keywords:
                self.discovered_keywords.add(keyword)

            # Create comprehensive profile document
            profile = {
                "username": username,
                "profile_id": post_data.get("profile_id"),
                "bio": bio,
                "avatar_url": post_data.get("profile_avatar"),
                "profile_url": post_data.get("profile_url"),
                "is_verified": post_data.get("is_verified", False),
                "emails": emails,
                "followers_count": followers,
                "post_count": 1,
                "video_count": post_data.get("video_count", 0),
                "digg_count": post_data.get("digg_count", 0),
                "share_count": post_data.get("share_count", 0),
                "comment_count": post_data.get("comment_count", 0),
                "play_count": post_data.get("play_count", 0),
                "discovered_keywords": discovered_keywords,
                "has_emails": len(emails) > 0,
                "extraction_date": datetime.now(),
                "source_post_id": post_data.get("post_id"),
                "last_activity_date": post_data.get("create_time"),
                "video_duration": post_data.get("video_duration"),
                "hashtags": post_data.get("hashtags", []),
                "original_sound": post_data.get("original_sound"),
                "music_title": (
                    post_data.get("music", {}).get("title", "")
                    if post_data.get("music")
                    else ""
                ),
            }

            # Insert into database
            self.profiles_collection.insert_one(profile)
            return profile

        except pymongo.errors.DuplicateKeyError:
            return None
        except Exception as e:
            self.log_message(f"Error processing post for {username}: {str(e)}")
            return None

    def search_with_keyword(self, keyword):
        """Search for posts using a specific keyword"""
        self.log_message(f"🔍 Searching with keyword: {keyword}")

        try:
            url = "https://api.brightdata.com/datasets/v3/trigger"
            headers = {
                "Authorization": f"Bearer {self.brightdata_tag_api_key}",
                "Content-Type": "application/json",
            }
            params = {
                "dataset_id": self.hashtag_dataset_id,
                "include_errors": "true",
                "type": "discover_new",
                "discover_by": "keyword",
            }

            data = [{"search_keyword": keyword, "country": "AU"}]

            response = requests.post(
                url, headers=headers, params=params, json=data
            )

            if response.status_code != 200:
                self.log_message(
                    f"❌ API request failed: {response.status_code}"
                )
                return None

            result = response.json()

            if "snapshot_id" in result:
                snapshot_id = result["snapshot_id"]
                self.log_message(f"📊 Snapshot started: {snapshot_id}")

                # Monitor snapshot and get data
                posts_data = self.monitor_snapshot(
                    snapshot_id, self.brightdata_tag_api_key
                )
                return posts_data
            else:
                self.log_message(f"❌ No snapshot ID returned: {result}")
                return None

        except Exception as e:
            self.log_message(
                f"❌ Error searching with keyword {keyword}: {str(e)}"
            )
            return None

    def monitor_snapshot(self, snapshot_id, api_key):
        """Monitor the status of a snapshot until completion"""
        self.log_message(f"👀 Monitoring snapshot {snapshot_id}")

        max_wait_time = 3600  # 1 hour maximum
        start_time = time.time()
        check_count = 0

        while time.time() - start_time < max_wait_time:
            try:
                check_count += 1

                # Check status
                url = f"https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"
                headers = {"Authorization": f"Bearer {api_key}"}

                response = requests.get(url, headers=headers)

                if response.status_code != 200:
                    if (
                        check_count % 5 == 0
                    ):  # Log every 5th error to avoid spam
                        self.log_message(
                            f"⚠️ Status check error: {response.status_code}"
                        )
                    time.sleep(30)
                    continue

                status_data = response.json()
                status = status_data.get("status", "unknown")

                if check_count % 10 == 0:  # Log status every 10 checks
                    self.log_message(f"📊 Snapshot status: {status}")

                if status == "ready":
                    self.log_message(
                        "✅ Snapshot completed, downloading data..."
                    )
                    return self.download_snapshot_data(snapshot_id, api_key)
                elif status in ["failed", "error"]:
                    self.log_message("❌ Snapshot failed")
                    return None
                else:
                    time.sleep(30)  # Wait before checking again

            except Exception as e:
                self.log_message(f"⚠️ Error monitoring snapshot: {str(e)}")
                time.sleep(60)

        self.log_message("⏰ Snapshot monitoring timed out")
        return None

    def download_snapshot_data(self, snapshot_id, api_key):
        """Download the completed snapshot data"""
        max_retries = 5
        retry_delay = 30

        for attempt in range(max_retries):
            try:
                self.log_message(
                    f"📥 Download attempt {attempt + 1}/{max_retries}"
                )

                url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"
                headers = {"Authorization": f"Bearer {api_key}"}
                params = {"format": "json"}

                response = requests.get(url, headers=headers, params=params)

                if response.status_code == 202:
                    self.log_message("⏳ Data not ready yet, waiting...")
                    time.sleep(retry_delay)
                    continue
                elif response.status_code != 200:
                    self.log_message(
                        f"❌ Download error: {response.status_code}"
                    )
                    if attempt == max_retries - 1:
                        return None
                    time.sleep(retry_delay)
                    continue

                try:
                    data = response.json()
                    self.log_message(
                        f"✅ Downloaded {len(data) if isinstance(data, list) else 1} items"
                    )
                    return data
                except json.JSONDecodeError:
                    self.log_message("❌ Response is not valid JSON")
                    if attempt == max_retries - 1:
                        return None
                    time.sleep(retry_delay)
                    continue

            except Exception as e:
                self.log_message(f"❌ Download error: {str(e)}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(retry_delay)

        self.log_message("❌ Failed to download snapshot data")
        return None

    def extract_profiles_from_posts(self, posts_data):
        """Extract profiles from posts data"""
        if not posts_data:
            self.log_message("📭 No posts data to process")
            return 0

        profiles_extracted = 0
        posts_list = (
            posts_data if isinstance(posts_data, list) else [posts_data]
        )

        self.log_message(f"📊 Processing {len(posts_list)} posts...")

        for i, post in enumerate(posts_list, 1):
            profile = self.process_post_data(post)
            if profile:
                profiles_extracted += 1
                email_status = (
                    "📧 with email" if profile["has_emails"] else "📭 no email"
                )
                self.log_message(
                    f"✅ [{profiles_extracted}] Extracted: {profile['username']} {email_status}"
                )

                # Update progress periodically
                if profiles_extracted % 10 == 0:
                    self.show_progress()

        return profiles_extracted

    def get_next_keyword(self):
        """Get the next keyword to search with"""
        # First try primary keywords
        for keyword in self.primary_keywords:
            if not self.searches_collection.find_one({"keyword": keyword}):
                return keyword

        # Then try discovered keywords
        for keyword in list(self.discovered_keywords):
            if not self.searches_collection.find_one({"keyword": keyword}):
                return keyword

        # Add some fallback NYC-related keywords
        fallback_keywords = [
            "NYC TikTok",
            "New York TikTok",
            "NYC creator",
            "NYC influencer",
        ]
        for keyword in fallback_keywords:
            if not self.searches_collection.find_one({"keyword": keyword}):
                return keyword

        return None

    def show_progress(self):
        """Show current progress"""
        current_count = self.profiles_collection.count_documents({})
        email_count = self.profiles_collection.count_documents(
            {"has_emails": True}
        )
        progress = (current_count / self.target_profiles_count) * 100

        self.log_message("=" * 60)
        self.log_message(
            f"📈 PROGRESS: {current_count}/{self.target_profiles_count} profiles ({progress:.1f}%)"
        )
        self.log_message(f"📧 Profiles with emails: {email_count}")
        self.log_message(
            f"🔑 Unique keywords discovered: {len(self.discovered_keywords)}"
        )
        self.log_message("=" * 60)

    def show_final_stats(self):
        """Show final statistics when done"""
        current_count = self.profiles_collection.count_documents({})
        email_count = self.profiles_collection.count_documents(
            {"has_emails": True}
        )

        self.log_message("🎉 EXTRACTION COMPLETED!")
        self.log_message("=" * 60)
        self.log_message(f"📊 FINAL STATISTICS:")
        self.log_message(f"   Total profiles collected: {current_count}")
        self.log_message(f"   Profiles with emails: {email_count}")
        self.log_message(
            f"   Email extraction rate: {(email_count/current_count*100) if current_count > 0 else 0:.1f}%"
        )
        self.log_message(
            f"   Unique keywords used: {len(self.discovered_keywords) + len(self.primary_keywords)}"
        )
        self.log_message("=" * 60)

    def export_profiles(self):
        """Export profiles to JSON file"""
        try:
            profiles = list(self.profiles_collection.find({}, {"_id": 0}))
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"tiktok_profiles_nyc_{timestamp}.json"

            with open(filename, "w", encoding="utf-8") as f:
                json.dump(
                    profiles, f, indent=2, ensure_ascii=False, default=str
                )

            self.log_message(
                f"💾 Exported {len(profiles)} profiles to {filename}"
            )
            return filename

        except Exception as e:
            self.log_message(f"❌ Error exporting data: {str(e)}")
            return None

    def run_extraction(self):
        """Main extraction loop"""
        self.log_message("🚀 Starting TikTok Profile Extraction")
        self.log_message(f"🎯 Target: {self.target_profiles_count} profiles")
        self.log_message(
            f"🔑 Primary keywords: {', '.join(self.primary_keywords)}"
        )
        self.log_message("=" * 60)

        try:
            iteration = 0
            while self.is_running:
                iteration += 1
                current_count = self.profiles_collection.count_documents({})

                # Check if target reached
                if current_count >= self.target_profiles_count:
                    self.log_message("🎯 Target reached! Stopping extraction.")
                    break

                self.log_message(f"🔄 Iteration {iteration}")
                self.show_progress()

                # Get next keyword to search with
                keyword = self.get_next_keyword()
                if not keyword:
                    self.log_message("❌ No more keywords available. Stopping.")
                    break

                # Mark keyword as used
                self.searches_collection.insert_one(
                    {
                        "keyword": keyword,
                        "searched_at": datetime.now(),
                        "iteration": iteration,
                    }
                )

                # Search with keyword
                posts_data = self.search_with_keyword(keyword)
                if posts_data:
                    # Extract profiles from posts
                    profiles_count = self.extract_profiles_from_posts(
                        posts_data
                    )
                    self.log_message(
                        f"📥 Extracted {profiles_count} profiles from keyword: '{keyword}'"
                    )
                else:
                    self.log_message(
                        f"⚠️ No data returned for keyword: '{keyword}'"
                    )

                # Small delay between searches to avoid rate limiting
                self.log_message("⏳ Waiting 10 seconds before next search...")
                time.sleep(10)

                # Show progress every iteration
                self.show_progress()

        except KeyboardInterrupt:
            self.log_message("⏹️ Extraction interrupted by user")
        except Exception as e:
            self.log_message(f"❌ Error in extraction loop: {str(e)}")

        finally:
            self.is_running = False
            self.show_final_stats()

            # Export data
            export_file = self.export_profiles()
            if export_file:
                self.log_message(f"✅ Data exported to: {export_file}")

            self.log_message("🏁 Extraction process completed")

    def stop(self):
        """Stop the extraction process"""
        self.is_running = False
        self.log_message("🛑 Stopping extraction...")


def main():
    # Check if environment variables are set
    required_vars = [
        "BRIGHTDATA_TAG_API_KEY",
        "BRIGHTDATA_PROFILE_API_KEY",
        "MONGO_URI",
    ]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        print("Please set these variables in your .env file")
        exit(1)

    # Create and run the extractor
    extractor = TikTokProfileExtractor()

    try:
        extractor.run_extraction()
    except KeyboardInterrupt:
        extractor.stop()
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
