import os
import re
import sys
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, errors

load_dotenv()
mongo_uri = os.getenv("MONGO_URI")

try:
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
except errors.ServerSelectionTimeoutError as e:
    print("MongoDB connection failed:", e)
    sys.exit(1)

db = client.instagram_scraper
profiles_collection = db.profiles

try:
    total_docs = profiles_collection.count_documents({})
    print(f"Total profiles in collection: {total_docs}")

    export = (
        input("Do you want to export the profiles? (yes/no): ").strip().lower()
    )
    if export != "yes":
        print("Export canceled.")
        sys.exit(0)

    output_file = f"profiles_extracted_emails_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    # Email regex pattern
    email_pattern = re.compile(
        r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
    )

    # Process in batches to handle large datasets
    batch_size = 1000
    profiles_list = []

    cursor = profiles_collection.find(
        {},
        {
            "_id": 0,
            "username": 1,
            "emails": 1,
            "bio": 1,
            "profile_url": 1,
            "followers": 1,
        },
    ).batch_size(batch_size)

    for profile in cursor:
        # Extract emails from bio using regex
        bio_emails = []
        if "bio" in profile:
            bio_emails = email_pattern.findall(profile["bio"])

        # Combine existing emails with those found in bio
        all_emails = list(set(profile.get("emails", []) + bio_emails))

        profiles_list.append(
            {
                "username": profile.get("username", ""),
                "email_from_db": ", ".join(profile.get("emails", [])),
                "email_from_bio": ", ".join(bio_emails),
                "all_emails": ", ".join(all_emails),
                "bio": (
                    profile.get("bio", "")[:500] + "..."
                    if len(profile.get("bio", "")) > 500
                    else profile.get("bio", "")
                ),  # Truncate long bios
                "profile_url": profile.get("profile_url", ""),
                "followers": profile.get("followers", 0),
            }
        )

        # Process in batches to avoid memory issues
        if len(profiles_list) % batch_size == 0:
            print(f"Processed {len(profiles_list)} profiles...")

    # Create DataFrame and export to Excel
    df = pd.DataFrame(profiles_list)

    # Write to Excel with auto-adjusted column widths
    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Profiles")

        # Auto-adjust column widths
        worksheet = writer.sheets["Profiles"]
        for idx, col in enumerate(df.columns):
            max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(idx, idx, max_len)

    print(f"Exported {len(profiles_list)} profiles to {output_file}")

except Exception as e:
    print("Failed:", e)
    sys.exit(1)
