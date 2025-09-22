import json
import os
import re
import threading
import time
import tkinter as tk
from datetime import datetime
from tkinter import messagebox, scrolledtext, ttk
from urllib.parse import urlparse

import pymongo
import requests
from dotenv import load_dotenv
from pymongo import IndexModel, MongoClient

# Load environment variables
load_dotenv()


class TikTokScraper:
    def __init__(self):
        # API keys - one for hashtag search, one for profile scraping
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
        self.db = self.client.tiktok_scraper_prod

        # Collections
        self.posts_collection = self.db.posts
        self.users_collection = self.db.users
        self.searches_collection = self.db.searches
        self.raw_collection = self.db.raw_snapshots
        self.profiles_collection = self.db.profiles

        # Create indexes
        self.profiles_collection.create_index("username", unique=True)
        self.posts_collection.create_index("post_id", unique=True)
        self.users_collection.create_index("username", unique=True)
        self.searches_collection.create_index(
            [("hashtag", 1), ("timestamp", 1)]
        )
        self.raw_collection.create_index("snapshot_id", unique=True)

        # Initialize UI
        self.root = tk.Tk()
        self.root.title("TikTok Hashtag & Profile Scraper")
        self.root.geometry("900x700")
        self.setup_ui()

    def setup_ui(self):
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Hashtag search section
        hashtag_frame = ttk.LabelFrame(
            main_frame, text="Hashtag Search", padding="5"
        )
        hashtag_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=5)

        ttk.Label(hashtag_frame, text="Hashtag to search:").grid(
            row=0, column=0, sticky=tk.W, pady=5
        )
        self.hashtag_entry = ttk.Entry(hashtag_frame, width=30)
        self.hashtag_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=5)
        self.hashtag_entry.insert(0, "NYC")

        ttk.Button(
            hashtag_frame,
            text="Search Hashtag",
            command=self.start_hashtag_search,
        ).grid(row=0, column=2, pady=5, padx=5)

        # Input section for usernames
        input_frame = ttk.LabelFrame(
            main_frame, text="Profile Scraping", padding="5"
        )
        input_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=5)

        ttk.Label(input_frame, text="Usernames to scrape (one per line):").grid(
            row=0, column=0, sticky=tk.W, pady=5
        )

        self.usernames_text = scrolledtext.ScrolledText(
            input_frame, width=70, height=8
        )
        self.usernames_text.grid(row=1, column=0, columnspan=3, pady=5)

        # Add some default usernames
        default_usernames = []
        urls = [
            "https://www.tiktok.com/discover/profile-pictures-with-the-new-york-hat",
            "https://www.tiktok.com/@kamil_szymczak/video/7498085927357566230",
            # ... (other URLs from your example)
        ]

        for url in urls:
            match = re.search(r"tiktok\.com/@([\w\d_]+)", url)
            if match:
                default_usernames.append(match.group(1))
        self.usernames_text.insert(tk.END, "\n".join(default_usernames))

        ttk.Button(
            input_frame,
            text="Start Profile Scraping",
            command=self.start_profile_scraping,  # Fixed method name
        ).grid(row=2, column=0, pady=5)
        ttk.Button(
            input_frame, text="Check Status", command=self.check_status
        ).grid(row=2, column=1, pady=5)

        # Progress section
        progress_frame = ttk.LabelFrame(
            main_frame, text="Progress", padding="5"
        )
        progress_frame.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=5)

        self.progress = ttk.Progressbar(progress_frame, mode="indeterminate")
        self.progress.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=5)

        self.status_label = ttk.Label(progress_frame, text="Ready")
        self.status_label.grid(row=1, column=0, sticky=tk.W, pady=5)

        # Log section
        log_frame = ttk.LabelFrame(main_frame, text="Log", padding="5")
        log_frame.grid(row=3, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)

        self.log_text = scrolledtext.ScrolledText(
            log_frame, width=90, height=15
        )
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Results section
        results_frame = ttk.LabelFrame(main_frame, text="Results", padding="5")
        results_frame.grid(row=4, column=0, sticky=(tk.W, tk.E), pady=5)

        ttk.Button(
            results_frame, text="Show Collected Posts", command=self.show_posts
        ).grid(row=0, column=0, pady=5)
        ttk.Button(
            results_frame,
            text="Show Collected Profiles",
            command=self.show_profiles,
        ).grid(row=0, column=1, pady=5)
        ttk.Button(
            results_frame, text="Export Data", command=self.export_data
        ).grid(row=0, column=2, pady=5)

        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(3, weight=1)
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

    def log_message(self, message):
        """Add message to log with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        self.log_text.insert(tk.END, log_entry)
        self.log_text.see(tk.END)
        self.root.update_idletasks()

    def start_profile_scraping(self):
        """Start the profile scraping process in a separate thread"""
        usernames = self.usernames_text.get(1.0, tk.END).strip().split("\n")
        usernames = [u.strip() for u in usernames if u.strip()]

        if not usernames:
            messagebox.showerror("Error", "Please enter at least one username")
            return

        # Filter out already scraped usernames
        new_usernames = []
        for username in usernames:
            if not self.profiles_collection.find_one({"username": username}):
                new_usernames.append(username)
            else:
                self.log_message(
                    f"Username {username} already scraped, skipping"
                )

        if not new_usernames:
            messagebox.showinfo(
                "Info", "All usernames have already been scraped"
            )
            return

        # Start scraping in a separate thread
        threading.Thread(
            target=self.scrape_profiles, args=(new_usernames,), daemon=True
        ).start()

    def monitor_snapshot(self, snapshot_id, api_key):
        """Monitor the status of a snapshot until completion"""
        self.log_message(f"Monitoring snapshot {snapshot_id}")

        max_wait_time = 3600  # 1 hour maximum
        start_time = time.time()

        while time.time() - start_time < max_wait_time:
            try:
                # Check status
                url = f"https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"
                headers = {
                    "Authorization": f"Bearer {api_key}",
                }

                response = requests.get(url, headers=headers)

                if response.status_code != 200:
                    self.log_message(
                        f"Error checking status: {response.status_code}"
                    )
                    time.sleep(60)  # Wait before retrying
                    continue

                status_data = response.json()
                status = status_data.get("status")

                # Update status in DB
                self.raw_collection.update_one(
                    {"snapshot_id": snapshot_id},
                    {
                        "$set": {
                            "status": status,
                            "last_checked": datetime.now(),
                        }
                    },
                )

                self.log_message(f"Snapshot status: {status}")

                if status == "ready":
                    self.log_message("Snapshot completed, downloading data...")
                    return self.download_snapshot_data(snapshot_id, api_key)
                elif status in ["failed", "error"]:
                    self.log_message("Snapshot failed")
                    return None
                else:
                    # Wait before checking again
                    time.sleep(30)

            except Exception as e:
                self.log_message(f"Error monitoring snapshot: {str(e)}")
                time.sleep(60)

        self.log_message(
            f"Snapshot monitoring timed out after {max_wait_time} seconds"
        )
        return None

    def download_snapshot_data(self, snapshot_id, api_key):
        """Download the completed snapshot data with retry logic"""
        max_retries = 5
        retry_delay = 30  # seconds

        for attempt in range(max_retries):
            try:
                self.log_message(
                    f"Attempt {attempt + 1}/{max_retries} to download snapshot data..."
                )

                # Get download URL
                url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"
                headers = {
                    "Authorization": f"Bearer {api_key}",
                }
                params = {
                    "format": "json",
                }

                response = requests.get(url, headers=headers, params=params)

                if response.status_code == 202:
                    # Data not ready yet, wait and retry
                    self.log_message(
                        f"Data not ready yet (202), retrying in {retry_delay} seconds..."
                    )
                    time.sleep(retry_delay)
                    continue
                elif response.status_code != 200:
                    self.log_message(
                        f"Error downloading data: {response.status_code}"
                    )
                    if attempt == max_retries - 1:  # Last attempt
                        return None
                    time.sleep(retry_delay)
                    continue

                # Parse the response
                try:
                    data = response.json()
                    print(data)
                except json.JSONDecodeError:
                    self.log_message("Error: Response is not valid JSON")
                    if attempt == max_retries - 1:
                        return None
                    time.sleep(retry_delay)
                    continue

                # Update the snapshot record with the downloaded data
                self.raw_collection.update_one(
                    {"snapshot_id": snapshot_id},
                    {
                        "$set": {
                            "downloaded_at": datetime.now(),
                            "snapshot_data": data,
                        }
                    },
                )

                self.log_message(f"Downloaded data for snapshot {snapshot_id}")
                return data

            except requests.exceptions.RequestException as e:
                self.log_message(f"Network error downloading data: {str(e)}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(retry_delay)
            except Exception as e:
                self.log_message(f"Error downloading snapshot data: {str(e)}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(retry_delay)

        self.log_message(
            f"Failed to download snapshot {snapshot_id} after {max_retries} attempts"
        )
        return None

    def extract_profiles(self, data, snapshot_id):
        """Extract profile information from snapshot data"""
        try:
            self.log_message("Extracting profile information...")
            extracted = self.extract_accounts(data)
            print(extracted)
            new_profiles = 0
            for profile in extracted:
                try:
                    # Check if profile already exists
                    if self.profiles_collection.find_one(
                        {"username": profile["username"]}
                    ):
                        continue

                    # Add snapshot ID and timestamp
                    profile["snapshot_id"] = snapshot_id
                    profile["extracted_at"] = datetime.now()

                    # Insert into database
                    self.profiles_collection.insert_one(profile)
                    new_profiles += 1

                except pymongo.errors.DuplicateKeyError:
                    # Skip duplicate usernames
                    continue

            self.log_message(f"Extracted {new_profiles} new profiles")

        except Exception as e:
            self.log_message(f"Error extracting profiles: {str(e)}")

    def check_status(self):
        """Check the status of all snapshots"""
        try:
            snapshots = list(
                self.raw_collection.find().sort("started_at", -1).limit(10)
            )

            status_window = tk.Toplevel(self.root)
            status_window.title("Snapshot Status")
            status_window.geometry("600x400")

            # Create treeview
            columns = ("snapshot_id", "status", "started_at", "usernames_count")
            tree = ttk.Treeview(status_window, columns=columns, show="headings")

            # Define headings
            tree.heading("snapshot_id", text="Snapshot ID")
            tree.heading("status", text="Status")
            tree.heading("started_at", text="Started At")
            tree.heading("usernames_count", text="Usernames Count")

            # Define columns
            tree.column("snapshot_id", width=150)
            tree.column("status", width=100)
            tree.column("started_at", width=150)
            tree.column("usernames_count", width=100)

            # Add data
            for snapshot in snapshots:
                tree.insert(
                    "",
                    "end",
                    values=(
                        snapshot.get("snapshot_id", ""),
                        snapshot.get("status", "unknown"),
                        (
                            snapshot.get("started_at", "").strftime(
                                "%Y-%m-%d %H:%M"
                            )
                            if snapshot.get("started_at")
                            else ""
                        ),
                        len(snapshot.get("usernames", [])),
                    ),
                )

            tree.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        except Exception as e:
            messagebox.showerror("Error", f"Failed to check status: {str(e)}")

    def show_profiles(self):
        """Show collected profiles in a new window"""
        try:
            profiles = list(
                self.profiles_collection.find()
                .sort("extracted_at", -1)
                .limit(50)
            )

            profiles_window = tk.Toplevel(self.root)
            profiles_window.title("Collected Profiles")
            profiles_window.geometry("800x600")

            # Create text widget
            text_widget = scrolledtext.ScrolledText(
                profiles_window, width=90, height=30
            )
            text_widget.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

            # Add profile data
            for profile in profiles:
                text_widget.insert(
                    tk.END, f"Username: {profile.get('username', 'N/A')}\n"
                )
                text_widget.insert(
                    tk.END, f"Full Name: {profile.get('full_name', 'N/A')}\n"
                )
                text_widget.insert(
                    tk.END, f"Followers: {profile.get('followers', 'N/A')}\n"
                )
                text_widget.insert(
                    tk.END, f"Bio: {profile.get('bio', 'N/A')}\n"
                )
                text_widget.insert(
                    tk.END, f"Emails: {', '.join(profile.get('emails', []))}\n"
                )
                text_widget.insert(
                    tk.END,
                    f"Extra Usernames: {', '.join(profile.get('extra_usernames', []))}\n",
                )
                text_widget.insert(tk.END, "-" * 50 + "\n\n")

            text_widget.config(state=tk.DISABLED)

        except Exception as e:
            messagebox.showerror("Error", f"Failed to show profiles: {str(e)}")

    def export_data(self):
        """Export data to JSON file"""
        try:
            # Get all profiles
            profiles = list(self.profiles_collection.find({}, {"_id": 0}))

            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"tiktok_profiles_{timestamp}.json"

            # Write to file
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(
                    profiles, f, indent=2, ensure_ascii=False, default=str
                )

            self.log_message(f"Exported {len(profiles)} profiles to {filename}")
            messagebox.showinfo(
                "Success", f"Exported {len(profiles)} profiles to {filename}"
            )

        except Exception as e:
            messagebox.showerror("Error", f"Failed to export data: {str(e)}")

    def start_hashtag_search(self):
        """Start hashtag search process"""
        hashtag = self.hashtag_entry.get().strip()
        if not hashtag:
            messagebox.showerror("Error", "Please enter a hashtag")
            return

        # Remove # if present
        if hashtag.startswith("#"):
            hashtag = hashtag[1:]

        # Get previously scraped post IDs for this hashtag
        exclude_ids = self.get_existing_post_ids(hashtag)

        self.log_message(
            f"Starting hashtag search for #{hashtag}, excluding {len(exclude_ids)} existing posts"
        )

        # Start search in a separate thread
        threading.Thread(
            target=self.process_hashtag_search,
            args=(hashtag, exclude_ids),
            daemon=True,
        ).start()

    def get_existing_post_ids(self, hashtag):
        """Get post IDs already in database for a hashtag"""
        # Get posts from previous searches for this hashtag
        post_ids = []
        previous_searches = self.searches_collection.find({"hashtag": hashtag})
        for search in previous_searches:
            if "post_ids" in search:
                post_ids.extend(search["post_ids"])
        return post_ids

    def process_hashtag_search(self, hashtag, exclude_ids):
        try:
            while True:
                self.progress.start()
                self.status_label.config(text=f"Searching for #{hashtag}...")

                result = self.search_hashtag(hashtag, exclude_ids)
                if "snapshot_id" not in result:
                    self.log_message(
                        f"Error: No snapshot ID returned: {result}"
                    )
                    break

                snapshot_id = result["snapshot_id"]
                self.log_message(f"Hashtag snapshot started: {snapshot_id}")

                search_data = {
                    "hashtag": hashtag,
                    "snapshot_id": snapshot_id,
                    "excluded_post_ids": exclude_ids,
                    "timestamp": datetime.now(),
                    "status": "started",
                }
                search_id = self.searches_collection.insert_one(
                    search_data
                ).inserted_id

                posts_data = self.monitor_snapshot(
                    snapshot_id, self.brightdata_tag_api_key
                )
                if not posts_data:
                    break

                new_posts = self.extract_posts(posts_data, hashtag)
                if not new_posts:
                    self.log_message(
                        f"No new posts for #{hashtag}, stopping loop"
                    )
                    break

                self.log_message(
                    f"Extracted {len(new_posts)} new posts from #{hashtag}"
                )

                # 🔑 Discover hashtags in new posts
                hashtag_counts = {}
                for post in new_posts:
                    for h in post.get("hashtags", []):
                        tag = h.get("name") if isinstance(h, dict) else str(h)
                        if not tag:
                            continue
                        hashtag_counts[tag] = hashtag_counts.get(tag, 0) + 1

                # 🔑 Add trending hashtags (>=6 posts) to searches
                for tag, count in hashtag_counts.items():
                    if count >= 6 and not self.searches_collection.find_one(
                        {"hashtag": tag}
                    ):
                        self.log_message(
                            f"Discovered trending hashtag #{tag} in {count} posts, adding to queue"
                        )
                        threading.Thread(
                            target=self.process_hashtag_search,
                            args=(tag, self.get_existing_post_ids(tag)),
                            daemon=True,
                        ).start()

                users = self.extract_users_from_posts(new_posts)
                self.log_message(f"Found {len(users)} unique users in posts")

                post_ids = [post["post_id"] for post in new_posts]
                exclude_ids.extend(post_ids)

                self.searches_collection.update_one(
                    {"_id": search_id},
                    {"$set": {"post_ids": post_ids, "status": "completed"}},
                )

                if users:
                    self.scrape_profiles(users)

                time.sleep(5)
        except Exception as e:
            self.log_message(f"Error in hashtag search loop: {str(e)}")
        finally:
            self.progress.stop()
            self.status_label.config(text="Hashtag search completed")

    def search_hashtag(self, hashtag, exclude_post_ids):
        """Search for posts by hashtag using Bright Data API"""
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

        data = [
            {
                "search_keyword": hashtag,
                "posts_to_not_include": exclude_post_ids,
                "country": "AU",
            }
        ]

        response = requests.post(url, headers=headers, params=params, json=data)
        return response.json()

    def extract_posts(self, posts_data, hashtag):
        """Extract post information from API response"""
        new_posts = []

        # Handle both list and single post responses
        posts = posts_data if isinstance(posts_data, list) else [posts_data]

        for post in posts:
            try:
                # Different API responses might have different field names for post_id
                post_id = post.get("id") or post.get("post_id")
                if not post_id:
                    continue

                # Check if post already exists
                if self.posts_collection.find_one({"post_id": post_id}):
                    continue

                # Extract author information safely
                author_info = post.get("author", {})
                if not author_info and "authorMeta" in post:
                    author_info = post["authorMeta"]

                # Extract music information safely
                music_info = post.get("music", {})
                if not music_info and "musicMeta" in post:
                    music_info = post["musicMeta"]

                # Prepare post document
                post_doc = {
                    "post_id": post_id,
                    "hashtag": hashtag,
                    "text": post.get("text")
                    or post.get("desc")
                    or post.get("description")
                    or "",
                    "create_time": post.get("create_time")
                    or post.get("createTime")
                    or post.get("createTimeISO"),
                    "author": {
                        "username": author_info.get("uniqueId")
                        or author_info.get("nickName")
                        or author_info.get("name")
                        or post.get("profile_username"),
                        "user_id": author_info.get("id")
                        or author_info.get("userId")
                        or post.get("profile_id"),
                        "verified": author_info.get("verified", False),
                        "avatar": author_info.get("avatar")
                        or author_info.get("avatarLarger")
                        or post.get("profile_avatar"),
                        "signature": author_info.get("signature", ""),
                        "following": author_info.get("following", 0),
                        "fans": author_info.get("fans", 0),
                        "heart": author_info.get("heart", 0),
                        "video": author_info.get("video", 0),
                        "digg": author_info.get("digg", 0),
                    },
                    "stats": {
                        "digg_count": post.get("digg_count")
                        or post.get("diggCount")
                        or post.get("likes", 0),
                        "share_count": post.get("share_count")
                        or post.get("shareCount", 0),
                        "collect_count": post.get("collect_count")
                        or post.get("collectCount", 0),
                        "comment_count": post.get("comment_count")
                        or post.get("commentCount", 0),
                        "play_count": post.get("play_count")
                        or post.get("playCount", 0),
                    },
                    "video": {
                        "duration": post.get("video_duration")
                        or post.get("duration", 0),
                        "url": post.get("video_url")
                        or post.get("webVideoUrl")
                        or "",
                        "cover": post.get("preview_image")
                        or post.get("coverUrl")
                        or post.get("originalCoverUrl")
                        or "",
                        "ratio": post.get("ratio", ""),
                        "width": post.get("width", 0),
                        "height": post.get("height", 0),
                    },
                    "music": {
                        "id": music_info.get("id")
                        or music_info.get("musicId", ""),
                        "title": music_info.get("title")
                        or music_info.get("musicName", ""),
                        "author": music_info.get("author")
                        or music_info.get("musicAuthor", ""),
                        "original": music_info.get("original", False),
                        "play_url": music_info.get("play_url")
                        or music_info.get("playUrl", ""),
                        "cover_url": music_info.get("cover_url")
                        or music_info.get("coverMediumUrl", ""),
                    },
                    "hashtags": post.get("hashtags") or [],
                    "original_sound": post.get("original_sound", ""),
                    "post_type": post.get("post_type", "video"),
                    "is_verified": post.get("is_verified", False),
                    "url": post.get("url")
                    or post.get("webVideoUrl")
                    or f"https://www.tiktok.com/@{author_info.get('uniqueId', '')}/video/{post_id}",
                    "collected_at": datetime.now(),
                }

                # Insert into database
                self.posts_collection.insert_one(post_doc)
                new_posts.append(post_doc)

            except Exception as e:
                self.log_message(f"Error processing post: {str(e)}")
                continue

        return new_posts

    def extract_accounts(self, data):
        """Extract account info from raw TikTok snapshot data focusing only on emails"""
        import json
        import re
        from datetime import datetime

        EMAIL_RE = re.compile(
            r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", re.I
        )

        def find_emails(obj):
            return set(EMAIL_RE.findall(json.dumps(obj)))

        extracted = []
        for idx, item in enumerate(
            data if isinstance(data, list) else [data], start=1
        ):
            try:
                username = (
                    item.get("account_id")
                    or item.get("uniqueId")
                    or item.get("nickname")
                )
                profile_url = (
                    item.get("url") or f"https://www.tiktok.com/@{username}"
                )
                bio = (
                    item.get("biography")
                    or item.get("signature")
                    or item.get("bio")
                    or ""
                )
                followers = (
                    item.get("followers") or item.get("followerCount") or 0
                )
                following = (
                    item.get("following") or item.get("followingCount") or 0
                )
                likes = (
                    item.get("likes")
                    or item.get("heartCount")
                    or item.get("heart")
                    or 0
                )
                videos_count = (
                    item.get("videos_count") or item.get("videoCount") or 0
                )
                full_name = item.get("nickname") or item.get("nickName") or ""

                emails = find_emails(bio)

                extracted.append(
                    {
                        "account_key": username,
                        "username": username,
                        "profile_url": profile_url,
                        "full_name": full_name,
                        "bio": bio,
                        "followers": followers,
                        "following": following,
                        "likes": likes,
                        "videos_count": videos_count,
                        "emails": sorted(emails),
                        "create_time": item.get("create_time"),
                        "extracted_at": datetime.now(),
                    }
                )

            except Exception as e:
                print(f"[ERROR] Error extracting item {idx}: {e}")
                continue

        return extracted

    def extract_users_from_posts(self, posts):
        """Extract unique usernames from posts"""
        users = set()
        for post in posts:
            if "author" in post and "username" in post["author"]:
                users.add(post["author"]["username"])
        return list(users)

    def scrape_profiles(self, usernames):
        """Scrape user profiles - modified from original code"""
        try:
            self.status_label.config(
                text=f"Scraping {len(usernames)} profiles..."
            )

            # Filter out already scraped usernames
            new_usernames = []
            for username in usernames:
                if not self.users_collection.find_one({"username": username}):
                    new_usernames.append(username)
                else:
                    self.log_message(
                        f"Username {username} already scraped, skipping"
                    )

            if not new_usernames:
                self.log_message("All users have already been scraped")
                return

            # Prepare data for API - batch usernames to avoid rate limiting
            batch_size = 20
            for i in range(0, len(new_usernames), batch_size):
                batch = new_usernames[i : i + batch_size]
                self.log_message(
                    f"Processing batch {i//batch_size + 1} with {len(batch)} users"
                )

                data = []
                for username in batch:
                    data.append(
                        {
                            "url": f"https://www.tiktok.com/@{username}",
                            "country": "US",
                        }
                    )

                # Use profile API key for this request
                params = {
                    "dataset_id": self.profile_dataset_id,
                    "include_errors": "true",
                }

                url = "https://api.brightdata.com/datasets/v3/trigger"
                headers = {
                    "Authorization": f"Bearer {self.brightdata_profile_api_key}",
                    "Content-Type": "application/json",
                }

                self.log_message("Sending request to BrightData Profile API...")
                response = requests.post(
                    url, headers=headers, params=params, json=data
                )

                if response.status_code != 200:
                    self.log_message(
                        f"API request failed with status {response.status_code}"
                    )
                    continue

                result = response.json()
                snapshot_id = result.get("snapshot_id")
                if not snapshot_id:
                    self.log_message(
                        "Error: No snapshot ID returned from Profile API"
                    )
                    continue

                self.log_message(
                    f"Profile snapshot started with ID: {snapshot_id}"
                )

                # Store snapshot info
                snapshot_data = {
                    "snapshot_id": snapshot_id,
                    "usernames": batch,
                    "status": "started",
                    "started_at": datetime.now(),
                }
                self.raw_collection.insert_one(snapshot_data)
                profile_data = self.monitor_snapshot(
                    snapshot_id, self.brightdata_tag_api_key
                )

                if profile_data is not None:
                    print(
                        f"[INFO] Snapshot {snapshot_id} returned {len(profile_data)} profiles"
                    )
                    for i, profile in enumerate(profile_data, start=1):
                        print(profile)
                        print(
                            f"[DEBUG] Processing profile {i}: {profile.get('username', 'N/A')}"
                        )
                        self.extract_profiles(profile, snapshot_id)
                else:
                    print(f"[WARNING] Snapshot {snapshot_id} returned None")

                # Add delay between batches to avoid rate limiting
                time.sleep(10)

        except Exception as e:
            self.log_message(f"Error in profile scraping: {str(e)}")

    def show_posts(self):
        """Show collected posts in a new window"""
        try:
            posts = list(
                self.posts_collection.find().sort("collected_at", -1).limit(50)
            )

            posts_window = tk.Toplevel(self.root)
            posts_window.title("Collected Posts")
            posts_window.geometry("800x600")

            # Create text widget
            text_widget = scrolledtext.ScrolledText(
                posts_window, width=90, height=30
            )
            text_widget.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

            # Add post data
            for post in posts:
                text_widget.insert(
                    tk.END, f"Post ID: {post.get('post_id', 'N/A')}\n"
                )
                text_widget.insert(
                    tk.END, f"Hashtag: #{post.get('hashtag', 'N/A')}\n"
                )
                text_widget.insert(tk.END, f"Text: {post.get('text', 'N/A')}\n")
                if "author" in post and "username" in post["author"]:
                    text_widget.insert(
                        tk.END, f"Author: @{post['author']['username']}\n"
                    )
                text_widget.insert(
                    tk.END, f"Collected: {post.get('collected_at', 'N/A')}\n"
                )
                text_widget.insert(tk.END, "-" * 50 + "\n\n")

            text_widget.config(state=tk.DISABLED)

        except Exception as e:
            messagebox.showerror("Error", f"Failed to show posts: {str(e)}")

    def run(self):
        """Start the application"""
        self.root.mainloop()


if __name__ == "__main__":
    # Check if environment variables are set
    required_vars = [
        "BRIGHTDATA_TAG_API_KEY",
        "BRIGHTDATA_PROFILE_API_KEY",
        "MONGO_URI",
    ]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(
            f"Please set the following environment variables: {', '.join(missing_vars)}"
        )
        exit(1)

    app = TikTokScraper()
    app.run()
