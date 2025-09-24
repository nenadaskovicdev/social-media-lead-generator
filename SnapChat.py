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


class SnapchatProfileExtractor:
    def __init__(self):
        # API keys
        self.brightdata_post_api_key = os.getenv(
            "BRIGHTDATA_SNAPCHAT_POST_API_KEY"
        )
        self.brightdata_profile_api_key = os.getenv(
            "BRIGHTDATA_SNAPCHAT_PROFILE_API_KEY"
        )
        self.mongo_uri = os.getenv("MONGO_URI")

        # Dataset IDs
        self.post_dataset_id = os.getenv(
            "SNAPCHAT_POST_DATASET_ID", "gd_ma0ydx431w6stl16ge"
        )
        self.profile_dataset_id = os.getenv(
            "SNAPCHAT_PROFILE_DATASET_ID", "gd_maxv8l0y12r9y28uus"
        )

        # Initialize MongoDB
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client.snapchat_scraper_testing_alpha  # Fixed typo here

        # Collections
        self.posts_collection = self.db.posts
        self.profiles_collection = self.db.profiles
        self.searches_collection = self.db.searches
        self.raw_collection = self.db.raw_snapshots

        # Create indexes
        self.profiles_collection.create_index("profile_handle", unique=True)
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
        self.profile_urls_to_process = set()

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
            "🏙️",
        ]

        bio_lower = bio.lower()
        for indicator in location_indicators:
            if indicator in bio_lower:
                start_idx = bio_lower.find(indicator) + len(indicator)
                remaining_text = bio_lower[start_idx:].strip()
                if remaining_text:
                    next_word = remaining_text.split()[0]
                    clean_word = re.sub(r"[^a-zA-Z]", "", next_word).title()
                    if len(clean_word) > 2:
                        keywords.add(clean_word)

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

    def extract_profile_handle_from_url(self, profile_link):
        """Extract profile handle from Snapchat profile URL"""
        if not profile_link:
            return None

        # Handle different URL formats
        if "/add/" in profile_link:
            # Format: https://www.snapchat.com/add/username
            parts = profile_link.split("/add/")
            if len(parts) > 1:
                handle = parts[1].split("/")[0].split("?")[0]
                return handle if handle else None
        elif "snapchat.com/add/" in profile_link:
            # Alternative format handling
            import re

            match = re.search(r"snapchat\.com/add/([^/?]+)", profile_link)
            if match:
                return match.group(1)

        return None

    def process_post_data(self, post_data):
        """Process individual post data to extract profile information and collect profile URLs"""
        try:
            # Debug: Log the structure of incoming data
            if not isinstance(post_data, dict):
                self.log_message(
                    f"⚠️ Post data is not a dict: {type(post_data)}"
                )
                return None

            # Extract profile information using correct field names from Snapchat API
            profile_handle = post_data.get("profile_handle")
            profile_link = post_data.get("profile_link")

            post_doc = {
                "post_id": post_data.get("post_id"),
                "profile_handle": profile_handle,
                "profile_name": post_data.get("profile_name"),
                "profile_link": profile_link,
                "num_comments": post_data.get("num_comments"),
                "num_shares": post_data.get("num_shares"),
                "num_views": post_data.get("num_views"),
                "upload_date": post_data.get("upload_date"),
                "description": post_data.get("description"),
                "title": post_data.get("title"),
                "duration": post_data.get("duration"),
                "video_url": post_data.get("video_url"),
                "thumbnail_url": post_data.get("thumbnail_url"),
                "dimensions": post_data.get("dimensions"),
                "hashtags": post_data.get("hashtags", []),
                "comments": post_data.get("comments", []),
                "extraction_date": datetime.now(),
                "sound": post_data.get("sound", {}),
                "up_next": post_data.get("up_next", []),
            }

            # Insert post into database (ignore duplicates)
            try:
                self.posts_collection.insert_one(post_doc)
                self.log_message(
                    f"📝 Stored post for profile: {profile_handle}"
                )
            except pymongo.errors.DuplicateKeyError:
                self.log_message(
                    f"⚠️ Duplicate post: {post_data.get('post_id')}"
                )
            except Exception as e:
                self.log_message(f"⚠️ Error storing post: {str(e)}")

            # Add profile URL to processing queue if not already processed
            if profile_link and not self.profiles_collection.find_one(
                {"profile_handle": profile_handle}
            ):
                self.profile_urls_to_process.add(profile_link)
                self.log_message(f"➕ Queued profile: {profile_handle}")
                return profile_handle
            else:
                self.log_message(
                    f"⏩ Profile already processed or no link: {profile_handle}"
                )
                return None

        except Exception as e:
            self.log_message(f"❌ Error processing post: {str(e)}")
            import traceback

            self.log_message(f"🔍 Stack trace: {traceback.format_exc()}")
            return None

    def process_profile_data(self, profile_data):
        """Process profile data to extract comprehensive profile information"""
        try:
            if not isinstance(profile_data, dict):
                self.log_message(
                    f"⚠️ Profile data is not a dict: {type(profile_data)}"
                )
                return None

            # Extract basic profile information - adjust field names based on actual API response
            profile_handle = profile_data.get(
                "profile_handle"
            ) or profile_data.get("username")
            if not profile_handle:
                # Try to extract from URL if direct handle not available
                profile_url = profile_data.get(
                    "profile_url"
                ) or profile_data.get("url")
                if profile_url:
                    profile_handle = self.extract_profile_handle_from_url(
                        profile_url
                    )

            if not profile_handle:
                self.log_message("⚠️ No profile handle found in profile data")
                self.log_message(
                    f"📋 Profile data keys: {list(profile_data.keys())}"
                )
                return None

            # Check if profile already exists
            if self.profiles_collection.find_one(
                {"profile_handle": profile_handle}
            ):
                self.log_message(f"⏩ Profile already exists: {profile_handle}")
                return None

            # Extract bio and other profile information
            bio = (
                profile_data.get("bio", "")
                or profile_data.get("description", "")
                or ""
            )
            display_name = profile_data.get(
                "display_name", ""
            ) or profile_data.get("profile_name", "")

            # Extract emails from bio
            emails = self.extract_emails_from_text(bio)

            # Extract additional keywords from bio
            discovered_keywords = self.extract_keywords_from_bio(bio)
            for keyword in discovered_keywords:
                self.discovered_keywords.add(keyword)

            # Create comprehensive profile document
            profile = {
                "profile_handle": profile_handle,
                "display_name": display_name,
                "bio": bio,
                "profile_url": profile_data.get("profile_url")
                or profile_data.get("url"),
                "avatar_url": profile_data.get("avatar_url")
                or profile_data.get("profile_avatar"),
                "snapcode_url": profile_data.get("snapcode_url"),
                "is_verified": profile_data.get("is_verified", False),
                "is_celebrity": profile_data.get("is_celebrity", False),
                "is_public": profile_data.get("is_public", True),
                "emails": emails,
                "has_emails": len(emails) > 0,
                "subscriber_count": profile_data.get("subscriber_count")
                or profile_data.get("followers_count"),
                "snapchat_score": profile_data.get("snapchat_score"),
                "birthday": profile_data.get("birthday"),
                "location": profile_data.get("location"),
                "website": profile_data.get("website"),
                "discovered_keywords": discovered_keywords,
                "extraction_date": datetime.now(),
                "stories_count": profile_data.get("stories_count", 0),
                "highlights": profile_data.get("highlights", []),
                "lenses": profile_data.get("lenses", []),
                "friend_emoji": profile_data.get("friend_emoji"),
                "snap_streak": profile_data.get("snap_streak"),
                "bitmoji_url": profile_data.get("bitmoji_url"),
                "join_date": profile_data.get("join_date"),
                "last_updated": profile_data.get("last_updated"),
                "raw_data": profile_data,  # Store raw data for debugging
            }

            # Insert into database
            self.profiles_collection.insert_one(profile)
            self.log_message(
                f"✅ Stored profile: {profile_handle} with {len(emails)} emails"
            )
            return profile

        except pymongo.errors.DuplicateKeyError:
            self.log_message(f"⏩ Duplicate profile: {profile_handle}")
            return None
        except Exception as e:
            self.log_message(
                f"❌ Error processing profile for {profile_handle}: {str(e)}"
            )
            import traceback

            self.log_message(f"🔍 Stack trace: {traceback.format_exc()}")
            return None

    def search_with_keyword(self, keyword):
        """Search for posts using a specific keyword"""
        self.log_message(f"🔍 Searching Snapchat with keyword: {keyword}")

        try:
            url = "https://api.brightdata.com/datasets/v3/trigger"
            headers = {
                "Authorization": f"Bearer {self.brightdata_post_api_key}",
                "Content-Type": "application/json",
            }
            params = {
                "dataset_id": self.post_dataset_id,
                "include_errors": "true",
                "type": "discover_new",
                "discover_by": "search_url",
            }

            search_keyword = keyword.replace(" ", "").lower()
            data = [
                {
                    "url": f"https://www.snapchat.com/explore/{search_keyword}",
                    "tab": "Users",
                }
            ]

            self.log_message(f"📤 Sending search request for: {search_keyword}")
            response = requests.post(
                url, headers=headers, params=params, json=data
            )

            if response.status_code != 200:
                self.log_message(
                    f"❌ API request failed: {response.status_code} - {response.text}"
                )
                return None

            result = response.json()
            self.log_message(f"📥 API response: {result}")

            # Save raw result to debug.json in current directory
            debug_file = os.path.join(os.getcwd(), "debug.json")
            with open(debug_file, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            self.log_message(f"💾 Debug JSON saved to: {debug_file}")

            if "snapshot_id" in result:
                snapshot_id = result["snapshot_id"]
                self.log_message(f"📊 Snapshot started: {snapshot_id}")
                posts_data = self.monitor_snapshot(
                    snapshot_id, self.brightdata_post_api_key
                )
                # Save raw result to debug.json in current directory
                debug_file = os.path.join(os.getcwd(), "debug1.json")
                with open(debug_file, "w", encoding="utf-8") as f:
                    json.dump(posts_data, f, indent=2, ensure_ascii=False)
                self.log_message(f"💾 Debug JSON saved to: {debug_file}")
                return posts_data
            else:
                self.log_message(f"❌ No snapshot ID returned: {result}")
                return None

        except Exception as e:
            self.log_message(
                f"❌ Error searching with keyword {keyword}: {str(e)}"
            )
            import traceback

            self.log_message(f"🔍 Stack trace: {traceback.format_exc()}")
            return None

    def get_profile_details(self, profile_urls):
        """Get detailed profile information for multiple profile URLs"""
        if not profile_urls:
            self.log_message("⚠️ No profile URLs to process")
            return None

        self.log_message(
            f"👤 Fetching details for {len(profile_urls)} profiles"
        )

        try:
            url = "https://api.brightdata.com/datasets/v3/trigger"
            headers = {
                "Authorization": f"Bearer {self.brightdata_profile_api_key}",
                "Content-Type": "application/json",
            }
            params = {
                "dataset_id": self.profile_dataset_id,
                "include_errors": "true",
            }

            # Prepare profile data - take first 10 URLs
            urls_to_process = list(profile_urls)[:10]
            data = []
            for profile_url in urls_to_process:
                data.append(
                    {"url": profile_url, "collect_all_highlights": False}
                )

            self.log_message(f"📤 Sending profile request for {len(data)} URLs")
            response = requests.post(
                url, headers=headers, params=params, json=data
            )

            if response.status_code != 200:
                self.log_message(
                    f"❌ Profile API request failed: {response.status_code} - {response.text}"
                )
                return None

            result = response.json()
            self.log_message(f"📥 Profile API response: {result}")

            if "snapshot_id" in result:
                snapshot_id = result["snapshot_id"]
                self.log_message(f"📊 Profile snapshot started: {snapshot_id}")

                # Monitor snapshot and get data
                profile_data = self.monitor_snapshot(
                    snapshot_id, self.brightdata_profile_api_key
                )
                return profile_data
            else:
                self.log_message(
                    f"❌ No profile snapshot ID returned: {result}"
                )
                return None

        except Exception as e:
            self.log_message(f"❌ Error fetching profile details: {str(e)}")
            import traceback

            self.log_message(f"🔍 Stack trace: {traceback.format_exc()}")
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
                    if check_count % 5 == 0:
                        self.log_message(
                            f"⚠️ Status check error: {response.status_code}"
                        )
                    time.sleep(30)
                    continue

                status_data = response.json()
                status = status_data.get("status", "unknown")

                if check_count % 10 == 0:
                    self.log_message(
                        f"📊 Snapshot status: {status} (check {check_count})"
                    )

                if status == "ready":
                    self.log_message(
                        "✅ Snapshot completed, downloading data..."
                    )
                    return self.download_snapshot_data(snapshot_id, api_key)
                elif status in ["failed", "error"]:
                    self.log_message("❌ Snapshot failed")
                    return None
                else:
                    time.sleep(30)

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
        """Extract profiles from posts data and queue profile URLs for detailed scraping"""
        if not posts_data:
            self.log_message("📭 No posts data to process")
            return 0

        profiles_extracted = 0
        posts_list = (
            posts_data if isinstance(posts_data, list) else [posts_data]
        )

        self.log_message(f"📊 Processing {len(posts_list)} posts...")

        for i, post in enumerate(posts_list, 1):
            profile_handle = self.process_post_data(post)
            if profile_handle:
                profiles_extracted += 1
                self.log_message(
                    f"✅ [{profiles_extracted}] Queued profile: {profile_handle}"
                )

        # Now process the collected profile URLs
        if self.profile_urls_to_process:
            self.log_message(
                f"👤 Processing {len(self.profile_urls_to_process)} queued profiles..."
            )
            profile_data = self.get_profile_details(
                self.profile_urls_to_process
            )

            if profile_data:
                profiles_processed = self.process_profile_data_batch(
                    profile_data
                )
                self.log_message(
                    f"✅ Processed {profiles_processed} profiles with details"
                )

                # Clear the processed URLs
                processed_urls = list(self.profile_urls_to_process)[
                    :10
                ]  # We processed first 10
                for url in processed_urls:
                    self.profile_urls_to_process.discard(url)

                return profiles_processed
            else:
                self.log_message("❌ Failed to get profile details")
                return profiles_extracted
        else:
            self.log_message("⚠️ No profile URLs collected from posts")
            return profiles_extracted

    def process_profile_data_batch(self, profile_data):
        """Process a batch of profile data"""
        if not profile_data:
            return 0

        profiles_processed = 0
        profile_list = (
            profile_data if isinstance(profile_data, list) else [profile_data]
        )

        self.log_message(f"🔧 Processing {len(profile_list)} profile items...")

        for i, profile in enumerate(profile_list, 1):
            processed_profile = self.process_profile_data(profile)
            if processed_profile:
                profiles_processed += 1
                email_status = (
                    "📧 with email"
                    if processed_profile["has_emails"]
                    else "📭 no email"
                )
                self.log_message(
                    f"✅ [{profiles_processed}] Profile: {processed_profile['profile_handle']} {email_status}"
                )

        return profiles_processed

    def get_next_keyword(self):
        """Get the next keyword to search with"""
        for keyword in self.primary_keywords:
            if not self.searches_collection.find_one({"keyword": keyword}):
                return keyword

        for keyword in list(self.discovered_keywords):
            if not self.searches_collection.find_one({"keyword": keyword}):
                return keyword

        fallback_keywords = [
            "NYC Snapchat",
            "New York Snapchat",
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
        posts_count = self.posts_collection.count_documents({})
        progress = (current_count / self.target_profiles_count) * 100

        self.log_message("=" * 60)
        self.log_message(
            f"📈 PROGRESS: {current_count}/{self.target_profiles_count} profiles ({progress:.1f}%)"
        )
        self.log_message(f"📧 Profiles with emails: {email_count}")
        self.log_message(f"📊 Posts collected: {posts_count}")
        self.log_message(
            f"🔑 Unique keywords discovered: {len(self.discovered_keywords)}"
        )
        self.log_message(
            f"⏳ Profiles queued for processing: {len(self.profile_urls_to_process)}"
        )
        self.log_message("=" * 60)

    def show_final_stats(self):
        """Show final statistics when done"""
        current_count = self.profiles_collection.count_documents({})
        email_count = self.profiles_collection.count_documents(
            {"has_emails": True}
        )
        posts_count = self.posts_collection.count_documents({})

        self.log_message("🎉 EXTRACTION COMPLETED!")
        self.log_message("=" * 60)
        self.log_message(f"📊 FINAL STATISTICS:")
        self.log_message(f"   Total profiles collected: {current_count}")
        self.log_message(f"   Profiles with emails: {email_count}")
        self.log_message(f"   Total posts collected: {posts_count}")
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
            profiles = list(
                self.profiles_collection.find({}, {"_id": 0, "raw_data": 0})
            )
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"snapchat_profiles_nyc_{timestamp}.json"

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
        self.log_message("🚀 Starting Snapchat Profile Extraction")
        self.log_message(f"🎯 Target: {self.target_profiles_count} profiles")
        self.log_message(
            f"🔑 Primary keywords: {', '.join(self.primary_keywords)}"
        )
        self.log_message("=" * 60)

        try:
            iteration = 0
            while self.is_running and iteration < 50:  # Safety limit
                iteration += 1
                current_count = self.profiles_collection.count_documents({})

                if current_count >= self.target_profiles_count:
                    self.log_message("🎯 Target reached! Stopping extraction.")
                    break

                self.log_message(f"🔄 Iteration {iteration}")
                self.show_progress()

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

                posts_data = self.search_with_keyword(keyword)
                if posts_data:
                    profiles_count = self.extract_profiles_from_posts(
                        posts_data
                    )
                    self.log_message(
                        f"📥 Processed {profiles_count} profiles from keyword: '{keyword}'"
                    )
                else:
                    self.log_message(
                        f"⚠️ No data returned for keyword: '{keyword}'"
                    )

                self.log_message("⏳ Waiting 15 seconds before next search...")
                time.sleep(15)

                self.show_progress()

        except KeyboardInterrupt:
            self.log_message("⏹️ Extraction interrupted by user")
        except Exception as e:
            self.log_message(f"❌ Error in extraction loop: {str(e)}")
            import traceback

            self.log_message(f"🔍 Stack trace: {traceback.format_exc()}")

        finally:
            self.is_running = False
            self.show_final_stats()
            export_file = self.export_profiles()
            if export_file:
                self.log_message(f"✅ Data exported to: {export_file}")
            self.log_message("🏁 Extraction process completed")

    def stop(self):
        """Stop the extraction process"""
        self.is_running = False
        self.log_message("🛑 Stopping extraction...")


def main():
    required_vars = [
        "BRIGHTDATA_SNAPCHAT_POST_API_KEY",
        "BRIGHTDATA_SNAPCHAT_PROFILE_API_KEY",
        "MONGO_URI",
    ]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        print("Please set these variables in your .env file")
        exit(1)

    extractor = SnapchatProfileExtractor()

    try:
        extractor.run_extraction()
    except KeyboardInterrupt:
        extractor.stop()
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback

        print(f"🔍 Stack trace: {traceback.format_exc()}")
        exit(1)


if __name__ == "__main__":
    main()
