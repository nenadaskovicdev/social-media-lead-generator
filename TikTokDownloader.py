import json

import requests
from pymongo import MongoClient

MONGO_URI = (
    "mongodb://abeselom:strongpassword@localhost:27017/scraper?authSource=admin"
)
BRIGHTDATA_API_TOKEN = (
    "89780cb1aa76643eed7123a9473c3254f83cb79661afb22272a99ac50cd0b280"
)

client = MongoClient(MONGO_URI)
db = client.tiktok_scraper_snapshots
snapshots = db.snapshots
datasets = db.datasets  # raw dataset contents


def save_snapshots(data):
    for snap in data:
        snapshots.update_one({"id": snap["id"]}, {"$set": snap}, upsert=True)

        if snap.get("downloadable") and snap.get("success_rate", 0) > 0:
            dataset_id = snap["dataset_id"]
            snapshot_id = snap["id"]
            url = (
                f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"
            )

            headers = {"Authorization": f"Bearer {BRIGHTDATA_API_TOKEN}"}

            try:
                resp = requests.get(url, headers=headers, stream=True)
                resp.raise_for_status()

                # check first few lines
                print(f"Raw response preview for dataset {dataset_id}:")
                lines = []
                for i, line in enumerate(resp.iter_lines(decode_unicode=True)):
                    if line.strip():
                        print(line)
                        lines.append(line)
                    if i >= 9:  # first 10 lines
                        break

                # reset response to read all lines
                resp.close()
                resp = requests.get(url, headers=headers, stream=True)
                resp.raise_for_status()

                records = []
                for line in resp.iter_lines(decode_unicode=True):
                    if line.strip():
                        records.append(json.loads(line))

                for rec in records:
                    rec["_snapshot_id"] = snap["id"]
                    datasets.update_one(
                        {"_snapshot_id": snap["id"], "id": rec.get("id")},
                        {"$set": rec},
                        upsert=True,
                    )
                print(
                    f"Inserted dataset {dataset_id} with {len(records)} records"
                )

            except Exception as e:
                print(f"Failed downloading {dataset_id}: {e}")


if __name__ == "__main__":
    with open("snapshots.json") as f:
        snapshot_data = json.load(f)
    save_snapshots(snapshot_data)
