import re

import pandas as pd
from pymongo import MongoClient
from sklearn.utils import shuffle  # to shuffle data randomly

MONGO_URI = (
    "mongodb://abeselom:strongpassword@localhost:27017/scraper?authSource=admin"
)

client = MongoClient(MONGO_URI)
db = client.tiktok_scraper_snapshots
datasets = db.datasets  # raw dataset contents


def extract_email(text):
    if not isinstance(text, str):
        return None
    emails = re.findall(r"[\w\.-]+@[\w\.-]+", text)
    return emails[0] if emails else None


def export_to_excel(filename="tiktok_profiles_emails.xlsx"):
    # Load CSV
    try:
        df_csv = pd.read_csv("tik_tok_profile.csv")
        df_csv["email"] = df_csv["biography"].apply(extract_email)
    except FileNotFoundError:
        df_csv = pd.DataFrame()

    # Load MongoDB
    records = list(datasets.find({}))
    data = []
    for rec in records:
        bio_text = rec.get("description") or rec.get("profile_biography")
        data.append(
            {
                "username": rec.get("account_id"),
                "email": extract_email(bio_text),
                "bio": rec.get("description"),
                "profile_bio": rec.get("profile_biography"),
                "followers": rec.get("profile_followers", 0),
                "following": rec.get("following", 0),
                "likes": rec.get("digg_count", 0),
                "videos_count": rec.get("videos_count", 0),
                "_snapshot_id": rec.get("_snapshot_id"),
            }
        )
    df_mongo = pd.DataFrame(data)

    # Combine and shuffle
    df_combined = pd.concat([df_csv, df_mongo], ignore_index=True)
    df_combined = shuffle(df_combined, random_state=42)

    df_combined.to_excel(filename, index=False)
    print(f"Exported {len(df_combined)} shuffled records to {filename}")


if __name__ == "__main__":
    export_to_excel()
