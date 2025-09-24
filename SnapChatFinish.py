import json
import os
import re
import time
from datetime import datetime

import pandas as pd
import requests

# Configuration
BEARER_TOKEN = (
    "89780cb1aa76643eed7123a9473c3254f83cb79661afb22272a99ac50cd0b280"
)
BASE_URL = "https://api.brightdata.com/datasets/v3/snapshot/"
FOLDER_NAME = "snapchat_profiles_data"


def read_snapshot_ids_from_file(filename):
    """Read snapshot IDs from a text file"""
    try:
        with open(filename, "r") as file:
            snapshot_ids = [line.strip() for line in file if line.strip()]
        return snapshot_ids
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return []


def check_existing_json(snapshot_id, folder):
    """Check if JSON file already exists and return data if valid"""
    filename = os.path.join(folder, f"{snapshot_id}.json")

    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                data = json.load(f)
            # Validate that the JSON has some expected structure
            if data and (isinstance(data, dict) or isinstance(data, list)):
                print(f"  ✓ Using cached JSON for {snapshot_id}")
                return data, filename
            else:
                print(f"  ⚠ Cached JSON invalid, re-downloading {snapshot_id}")
                return None, filename
        except (json.JSONDecodeError, Exception) as e:
            print(
                f"  ⚠ Error reading cached JSON, re-downloading {snapshot_id}: {e}"
            )
            return None, filename
    return None, filename


def download_snapshot(snapshot_id, retries=5, delay=30):
    """Download a single snapshot from Bright Data API"""
    url = f"{BASE_URL}{snapshot_id}"
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
    }
    params = {"format": "json"}

    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, dict) and data.get("status") == "building":
                    print(
                        f"Snapshot {snapshot_id} is still building, retrying in {delay}s..."
                    )
                    time.sleep(delay)
                    continue
                return data
            else:
                print(
                    f"Unexpected status {response.status_code}, retry {attempt+1}/{retries}"
                )
        except requests.exceptions.RequestException as e:
            print(
                f"Error downloading snapshot {snapshot_id}, attempt {attempt+1}/{retries}: {e}"
            )

        time.sleep(delay)

    print(f"Failed to download snapshot {snapshot_id} after {retries} retries.")
    return None


def extract_email_from_text(text):
    """Extract email addresses from text using regex"""
    if not text:
        return ""

    email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
    emails = re.findall(email_pattern, text)
    return ", ".join(emails) if emails else ""


def extract_profile_data_from_item(profile_item):
    if not isinstance(profile_item, dict):
        return None

    video = profile_item.get("video", {})

    description = profile_item.get("profile_description", "")
    email = extract_email_from_text(description)

    if not email and video.get("description"):
        email = extract_email_from_text(video.get("description", ""))

    profile_data = {
        "username": profile_item.get("profile_username", ""),
        "name": profile_item.get("profile_name", ""),
        "description": description,
        "email": email,
        "follower_count": profile_item.get("profile_follower_count", 0),
        "location": profile_item.get("profile_address", ""),
        "category": profile_item.get("profile_category", ""),
        "subcategory": profile_item.get("profile_subcategory", ""),
        "profile_url": profile_item.get("profile_url", ""),
        "creation_date": profile_item.get("profile_date_created", ""),
        "last_update": profile_item.get("profile_date_modified", ""),
        "has_highlights": profile_item.get(
            "profile_has_curated_highlights", False
        ),
        "has_spotlight": profile_item.get(
            "profile_has_spotlight_highlights", False
        ),
        "has_story": profile_item.get("profile_has_story", False),
        "badge_type": profile_item.get("profile_badge", 0),
        "snapshot_timestamp": profile_item.get("timestamp", ""),
        "video_upload_date": video.get("upload_date", ""),
        "video_duration": video.get("duration", ""),
        "video_view_count": video.get("view_count", 0),
        "snapshot_id": profile_item.get("snapshot_id", ""),
    }

    if profile_data["username"] or profile_data["name"]:
        return profile_data
    return None


def extract_profile_data(profile_json):
    """Extract relevant data from profile JSON - handles both list and dict responses"""
    if not profile_json:
        return []

    all_profiles = []

    # Handle list response (multiple profiles)
    if isinstance(profile_json, list):
        print(f"    Processing {len(profile_json)} profiles in list")
        for i, item in enumerate(profile_json):
            profile_data = extract_profile_data_from_item(item)
            if profile_data:
                all_profiles.append(profile_data)

    # Handle dictionary response (single profile)
    elif isinstance(profile_json, dict):
        profile_data = extract_profile_data_from_item(profile_json)
        if profile_data:
            all_profiles.append(profile_data)

    else:
        print(f"    ⚠ Unexpected data type: {type(profile_json)}")

    return all_profiles


def create_folder():
    """Create folder for storing JSON files"""
    if not os.path.exists(FOLDER_NAME):
        os.makedirs(FOLDER_NAME)
        print(f"Created folder: {FOLDER_NAME}")
    return FOLDER_NAME


def save_json_to_file(data, snapshot_id, folder):
    """Save JSON data to file"""
    filename = os.path.join(folder, f"{snapshot_id}.json")
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return filename
    except Exception as e:
        print(f"Error saving file for {snapshot_id}: {e}")
        return None


def get_existing_json_files(folder):
    """Get list of existing JSON files in the folder"""
    if not os.path.exists(folder):
        return set()

    json_files = set()
    for file in os.listdir(folder):
        if file.endswith(".json"):
            # Remove .json extension to get snapshot ID
            snapshot_id = file[:-5]
            json_files.add(snapshot_id)
    return json_files


def main():
    # Step 1: Read snapshot IDs from file
    snapshot_ids = read_snapshot_ids_from_file("snapchat_snapshot_ids.txt")
    if not snapshot_ids:
        print("No snapshot IDs found. Please check your snapshots.txt file.")
        return

    print(f"Found {len(snapshot_ids)} snapshot IDs to process")

    # Step 2: Create folder for JSON files
    folder = create_folder()

    # Step 3: Check for existing JSON files
    existing_files = get_existing_json_files(folder)
    print(f"Found {len(existing_files)} existing JSON files in folder")

    # Step 4: Process each snapshot
    all_profiles_data = []
    downloaded_count = 0
    cached_count = 0
    failed_count = 0

    for i, snapshot_id in enumerate(snapshot_ids, 1):
        print(f"Processing {i}/{len(snapshot_ids)}: {snapshot_id}")

        # Check if JSON already exists
        existing_data, filename = check_existing_json(snapshot_id, folder)

        if existing_data is not None:
            # Use existing JSON data
            snapshot_data = existing_data
            cached_count += 1
        else:
            # Download new snapshot
            snapshot_data = download_snapshot(snapshot_id)
            downloaded_count += 1

            if snapshot_data:
                # Save JSON to file
                filename = save_json_to_file(snapshot_data, snapshot_id, folder)
                if filename:
                    print(f"  ✓ Downloaded and saved to {filename}")

                # Add snapshot ID to the data for tracking
                if isinstance(snapshot_data, dict):
                    snapshot_data["snapshot_id"] = snapshot_id
                elif isinstance(snapshot_data, list):
                    for item in snapshot_data:
                        if isinstance(item, dict):
                            item["snapshot_id"] = snapshot_id
            else:
                print(f"  ✗ Failed to download {snapshot_id}")
                failed_count += 1
                continue

        # Extract profile data (returns list of profiles)
        profiles = extract_profile_data(snapshot_data)

        if profiles:
            all_profiles_data.extend(profiles)
            print(f"  ✓ Extracted {len(profiles)} profile(s)")
            for profile in profiles:
                username_display = (
                    profile["username"]
                    if profile["username"]
                    else profile["name"] if profile["name"] else "Unknown"
                )
                print(
                    f"    - {username_display} ({profile['follower_count']} followers)"
                )
        else:
            print(f"  ⚠ Could not extract profile data from {snapshot_id}")
            # Debug: print the type and a sample of the data
            print(f"    Data type: {type(snapshot_data)}")
            if isinstance(snapshot_data, dict):
                print(f"    Keys: {list(snapshot_data.keys())}")
            elif isinstance(snapshot_data, list) and len(snapshot_data) > 0:
                print(f"    First item type: {type(snapshot_data[0])}")
                if isinstance(snapshot_data[0], dict):
                    print(
                        f"    First item keys: {list(snapshot_data[0].keys())}"
                    )

        # Add small delay for new downloads only (be respectful to the API)
        if existing_data is None:
            time.sleep(0.5)

    # Step 5: Export to Excel
    if all_profiles_data:
        # Create DataFrame
        df = pd.DataFrame(all_profiles_data)

        # Sort by follower count (descending)
        df = df.sort_values("follower_count", ascending=False)

        # Create Excel filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_filename = f"snapchat_profiles_export_{timestamp}.xlsx"

        # Export to Excel with formatting
        with pd.ExcelWriter(excel_filename, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Profiles", index=False)

            # Auto-adjust column widths
            worksheet = writer.sheets["Profiles"]
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = (
                    adjusted_width
                )

        print(f"\n✅ Processing completed!")
        print(f"📊 Excel file created: {excel_filename}")
        print(f"💾 JSON files location: {folder}")

        # Display detailed summary
        print(f"\n📈 Processing Summary:")
        print(f"   Total snapshots to process: {len(snapshot_ids)}")
        print(f"   ✅ Downloaded: {downloaded_count}")
        print(f"   💾 From cache: {cached_count}")
        print(f"   ❌ Failed: {failed_count}")
        print(f"   📋 Total profiles extracted: {len(all_profiles_data)}")

        print(f"\n📊 Profile Statistics:")
        print(
            f"   Profiles with email: {len([p for p in all_profiles_data if p['email']])}"
        )
        print(f"   Total followers: {df['follower_count'].sum():,}")
        if len(all_profiles_data) > 0:
            print(f"   Average followers: {df['follower_count'].mean():,.0f}")

        if len(all_profiles_data) > 0:
            print(f"   Top 5 profiles by followers:")
            top_5 = df.head()[["username", "name", "follower_count"]]
            for _, row in top_5.iterrows():
                username_display = (
                    row["username"]
                    if row["username"]
                    else row["name"] if row["name"] else "Unknown"
                )
                print(
                    f"     - {username_display}: {row['follower_count']:,} followers"
                )

    else:
        print("❌ No profile data was successfully extracted.")


if __name__ == "__main__":
    main()
