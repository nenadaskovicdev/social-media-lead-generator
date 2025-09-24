import json
import os

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient

# Load env
load_dotenv()
mongo_uri = os.getenv("MONGO_URI")

# Connect MongoDB
client = MongoClient(mongo_uri)
db = client.tiktok_scraper_testing
profiles_collection = db.profiles

# Fetch profiles
profiles = list(
    profiles_collection.find(
        {},
        {
            "_id": 0,
            "username": 1,
            "emails": 1,
            "followers_count": 1,
            "bio": 1,
            "profile_url": 1,
            "avatar_url": 1,
            "is_verified": 1,
        },
    )
)

# Export Excel
df = pd.DataFrame(profiles)
df.to_excel("profiles_export.xlsx", index=False, engine="openpyxl")

print(f"✅ Exported {len(profiles)} profiles to Excel")
