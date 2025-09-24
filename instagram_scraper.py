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


class InstagramScraper:
    def __init__(self):
        self.brightdata_api_key = os.getenv("BRIGHTDATA_API_KEY")
        self.mongo_uri = os.getenv("MONGO_URI")
        self.dataset_id = os.getenv("DATASET_ID", "gd_l1vikfch901nx3by4")

        # Initialize MongoDB
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client.instagram_scraper
        self.raw_collection = self.db.raw_snapshots
        self.profiles_collection = self.db.profiles

        # Create indexes
        self.profiles_collection.create_index("username", unique=True)
        self.raw_collection.create_index("snapshot_id", unique=True)

        # Initialize UI
        self.root = tk.Tk()
        self.root.title("Instagram Profile Scraper")
        self.root.geometry("800x600")

        self.setup_ui()

    def setup_ui(self):
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Input section
        input_frame = ttk.LabelFrame(
            main_frame, text="Scraping Controls", padding="5"
        )
        input_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=5)

        ttk.Label(input_frame, text="Initial Usernames (one per line):").grid(
            row=0, column=0, sticky=tk.W, pady=5
        )

        self.usernames_text = scrolledtext.ScrolledText(
            input_frame, width=70, height=8
        )
        self.usernames_text.grid(row=1, column=0, columnspan=2, pady=5)

        # Add some default usernames
        default_usernames = [
            "newyork",
            "humansofny",
            "nyctourism",
            "pictures.of.ny",
            "visitnewyork",
            "whatisnewyork",
            "nybucketlist",
            "landmarksofny",
        ]
        self.usernames_text.insert(tk.END, "\n".join(default_usernames))

        ttk.Button(
            input_frame, text="Start Scraping", command=self.start_scraping
        ).grid(row=2, column=0, pady=5, sticky=tk.W)
        ttk.Button(
            input_frame, text="Check Status", command=self.check_status
        ).grid(row=2, column=1, pady=5, sticky=tk.E)

        # Progress section
        progress_frame = ttk.LabelFrame(
            main_frame, text="Progress", padding="5"
        )
        progress_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=5)

        self.progress = ttk.Progressbar(progress_frame, mode="indeterminate")
        self.progress.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=5)

        self.status_label = ttk.Label(progress_frame, text="Ready")
        self.status_label.grid(row=1, column=0, sticky=tk.W, pady=5)

        # Log section
        log_frame = ttk.LabelFrame(main_frame, text="Log", padding="5")
        log_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)

        self.log_text = scrolledtext.ScrolledText(
            log_frame, width=90, height=15
        )
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Results section
        results_frame = ttk.LabelFrame(main_frame, text="Results", padding="5")
        results_frame.grid(row=3, column=0, sticky=(tk.W, tk.E), pady=5)

        ttk.Button(
            results_frame,
            text="Show Collected Profiles",
            command=self.show_profiles,
        ).grid(row=0, column=0, pady=5)
        ttk.Button(
            results_frame, text="Export Data", command=self.export_data
        ).grid(row=0, column=1, pady=5)

        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(2, weight=1)
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

    def log_message(self, message):
        """Add message to log with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        self.log_text.insert(tk.END, log_entry)
        self.log_text.see(tk.END)
        self.root.update_idletasks()

    def start_scraping(self):
        """Start the scraping process in a separate thread"""
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

    def scrape_profiles(self, usernames):
        """Scrape profiles using BrightData API"""
        try:
            self.progress.start()
            self.status_label.config(text="Starting scraping process...")
            self.log_message(f"Starting to scrape {len(usernames)} profiles")

            # Prepare data for API
            data = [{"user_name": username} for username in usernames]

            # Make API request
            url = "https://api.brightdata.com/datasets/v3/trigger"
            headers = {
                "Authorization": f"Bearer {self.brightdata_api_key}",
                "Content-Type": "application/json",
            }
            params = {
                "dataset_id": self.dataset_id,
                "include_errors": "true",
                "type": "discover_new",
                "discover_by": "user_name",
            }

            self.log_message("Sending request to BrightData API...")
            response = requests.post(
                url, headers=headers, params=params, json=data
            )

            if response.status_code != 200:
                self.log_message(
                    f"Error: API request failed with status {response.status_code}"
                )
                return

            result = response.json()
            snapshot_id = result.get("snapshot_id", {})
            if not snapshot_id:
                self.log_message("Error: No snapshot ID returned from API")
                return

            self.log_message(f"Snapshot started with ID: {snapshot_id}")

            # Store snapshot info in MongoDB
            snapshot_data = {
                "snapshot_id": snapshot_id,
                "usernames": usernames,
                "status": "started",
                "started_at": datetime.now(),
                "snapshot_data": result,
            }
            self.raw_collection.insert_one(snapshot_data)

            # Monitor the snapshot status
            self.monitor_snapshot(snapshot_id)

        except Exception as e:
            self.log_message(f"Error in scraping process: {str(e)}")
        finally:
            self.progress.stop()
            self.status_label.config(text="Scraping completed")

    def monitor_snapshot(self, snapshot_id):
        """Monitor the status of a snapshot until completion"""
        self.log_message(f"Monitoring snapshot {snapshot_id}")

        while True:
            try:
                # Check status
                url = f"https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"
                headers = {
                    "Authorization": f"Bearer {self.brightdata_api_key}",
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
                    self.download_snapshot_data(snapshot_id)
                    break
                elif status in ["failed", "error"]:
                    self.log_message("Snapshot failed")
                    break
                else:
                    # Wait before checking again
                    time.sleep(30)

            except Exception as e:
                self.log_message(f"Error monitoring snapshot: {str(e)}")
                time.sleep(60)

    def download_snapshot_data(self, snapshot_id):
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
                    "Authorization": f"Bearer {self.brightdata_api_key}",
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
                        return
                    time.sleep(retry_delay)
                    continue

                # Parse the response
                try:
                    data = response.json()
                except json.JSONDecodeError:
                    self.log_message("Error: Response is not valid JSON")
                    if attempt == max_retries - 1:
                        return
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

                # Extract and store profiles
                self.extract_profiles(data, snapshot_id)
                break  # Success, exit retry loop

            except requests.exceptions.RequestException as e:
                self.log_message(f"Network error downloading data: {str(e)}")
                if attempt == max_retries - 1:
                    return
                time.sleep(retry_delay)
            except Exception as e:
                self.log_message(f"Error downloading snapshot data: {str(e)}")
                if attempt == max_retries - 1:
                    return
                time.sleep(retry_delay)
        if attempt == max_retries - 1:
            self.log_message(
                f"Failed to download snapshot {snapshot_id} after {max_retries} attempts"
            )

    def extract_profiles(self, data, snapshot_id):
        """Extract profile information from snapshot data"""
        try:
            self.log_message("Extracting profile information...")

            if isinstance(data, dict):
                records = data.get("items") or data.get("data") or [data]
            else:
                records = data

            extracted = self.extract_accounts(records)

            # Store extracted profiles
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

            # Find new usernames to scrape from the extracted data
            new_usernames = set()
            for profile in extracted:
                for username in profile.get("extra_usernames", []):
                    # Check if we've already scraped this username
                    if not self.profiles_collection.find_one(
                        {"username": username}
                    ):
                        new_usernames.add(username)

            if new_usernames:
                self.log_message(
                    f"Found {len(new_usernames)} new usernames to scrape"
                )

                # Convert to list and process in batches
                all_new_usernames = list(new_usernames)
                batch_size = 200

                # Process all usernames in batches
                for i in range(0, len(all_new_usernames), batch_size):
                    batch = all_new_usernames[i : i + batch_size]
                    self.log_message(
                        f"Starting batch {i//batch_size + 1} with {len(batch)} usernames"
                    )

                    # Start scraping this batch
                    threading.Thread(
                        target=self.scrape_profiles,
                        args=(batch,),
                        daemon=True,
                    ).start()

                    # Add a small delay between batches to avoid overwhelming the API
                    time.sleep(2)

            else:
                self.log_message("No new usernames found to scrape")

        except Exception as e:
            self.log_message(f"Error extracting profiles: {str(e)}")

    def extract_accounts(self, data):
        """Extract account information from raw data (using the provided code)"""
        EMAIL_RE = re.compile(
            r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", re.I
        )
        MENTION_RE = re.compile(r"@([A-Za-z0-9._]+)")
        INSTAGRAM_URL_RE = re.compile(r"instagram\.com/([A-Za-z0-9._-]+)", re.I)

        def find_emails(obj):
            s = json.dumps(obj)
            return set(EMAIL_RE.findall(s))

        def find_mentions(obj):
            s = json.dumps(obj)
            return set(MENTION_RE.findall(s))

        def find_instagram_usernames(obj):
            s = json.dumps(obj)
            return set(m.group(1) for m in INSTAGRAM_URL_RE.finditer(s))

        def gather_links(obj):
            links = set()
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if isinstance(v, str) and (
                        v.startswith("http://") or v.startswith("https://")
                    ):
                        links.add(v)
                    else:
                        links |= gather_links(v)
            elif isinstance(obj, list):
                for it in obj:
                    links |= gather_links(it)
            return links

        out = []
        for item in data:
            account = (
                item.get("account")
                or item.get("profile_name")
                or item.get("full_name")
                or item.get("id")
            )
            bio = item.get("biography") or item.get("bio") or ""
            followers = (
                item.get("followers") or item.get("followers_count") or None
            )
            profile_url = (
                item.get("profile_url")
                or item.get("url")
                or item.get("profile_image_link")
            )
            # collect emails, mentions, links, extra usernames
            emails = set()
            mentions = set()
            inst_users = set()
            links = set()
            emails |= find_emails(item)
            mentions |= find_mentions(item)
            inst_users |= find_instagram_usernames(item)
            links |= gather_links(item)
            # also extract from posts captions
            posts = item.get("posts") or []
            for p in posts:
                emails |= find_emails(p)
                mentions |= find_mentions(p)
                inst_users |= find_instagram_usernames(p)
                links |= gather_links(p)
            out.append(
                {
                    "account_key": account,
                    "username": item.get("account")
                    or item.get("input", {}).get("user_name")
                    or item.get("profile_name"),
                    "profile_url": profile_url,
                    "full_name": item.get("full_name"),
                    "bio": bio,
                    "followers": followers,
                    "emails": sorted(emails),
                    "extra_links": sorted(links),
                    "extra_usernames": sorted(
                        set(list(mentions) + list(inst_users))
                    ),
                    "timestamp": item.get("timestamp"),
                }
            )
        return out

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
        """Show collected profiles"""
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
            filename = f"instagram_profiles_{timestamp}.json"

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

    def run(self):
        """Start the application"""
        self.root.mainloop()


if __name__ == "__main__":
    # Check if environment variables are set
    if not os.getenv("BRIGHTDATA_API_KEY") or not os.getenv("MONGO_URI"):
        print(
            "Please create a .env file with BRIGHTDATA_API_KEY and MONGO_URI variables"
        )
        exit(1)

    app = InstagramScraper()
    app.run()
