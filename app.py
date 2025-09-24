import threading
import time
from datetime import datetime

import requests
from flask import Flask, jsonify, render_template, request
from pymongo import MongoClient

app = Flask(__name__)

# MongoDB setup
client = MongoClient(
    "mongodb+srv://scraper:mX2kN051v4xn1oHZ@awema-test.cmbpzxs.mongodb.net/?retryWrites=true&w=majority&appName=awema-test"
)
db = client["tiktok_db"]
collection = db["profiles"]
snapshots_collection = db["snapshots"]

# BrightData configuration
BRIGHTDATA_AUTH_TOKEN = (
    "118433fef297539683d9fb090647c3bcfb559f5867af1fa65f8953a82f9fcbfd"
)
BRIGHTDATA_DATASET_ID = "gd_l1villgoiiidt09ci"
BRIGHTDATA_API_URL = "https://api.brightdata.com/datasets/v3"


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/trigger_scraping", methods=["POST"])
def trigger_scraping():
    data = request.json
    search_urls = data.get("search_urls", [])
    countries = data.get("countries", [])

    if not search_urls:
        return jsonify({"error": "No search URLs provided"}), 400

    # Start scraping in a background thread to avoid blocking
    thread = threading.Thread(
        target=run_scraping, args=(search_urls, countries)
    )
    thread.start()

    return jsonify({"message": "Scraping started in background"}), 202


def run_scraping(search_urls, countries):
    # Prepare data for BrightData
    data = []
    for i, url in enumerate(search_urls):
        country = countries[i] if i < len(countries) else "US"
        data.append({"search_url": url, "country": country})

    # BrightData request
    url = f"{BRIGHTDATA_API_URL}/trigger"
    headers = {
        "Authorization": f"Bearer {BRIGHTDATA_AUTH_TOKEN}",
        "Content-Type": "application/json",
    }
    params = {
        "dataset_id": BRIGHTDATA_DATASET_ID,
        "include_errors": "true",
        "type": "discover_new",
        "discover_by": "search_url",
    }

    try:
        response = requests.post(url, headers=headers, params=params, json=data)
        result = response.json()

        # Check if we got a snapshot ID instead of profiles
        if "snapshot_id" in result:
            snapshot_id = result.get("snapshot_id")
            # Save snapshot info to MongoDB
            snapshot_data = {
                "timestamp": datetime.now(),
                "snapshot_id": snapshot_id,
                "status": "requested",
                "type": "trigger_response",
                "search_urls": search_urls,
                "countries": countries,
            }
            snapshots_collection.insert_one(snapshot_data)

            # Wait for snapshot to be ready and then download it
            time.sleep(10)  # Wait a bit for snapshot processing
            download_snapshot_data(snapshot_id)

            # Log the scraping activity
            log_entry = {
                "timestamp": datetime.now(),
                "search_urls": search_urls,
                "countries": countries,
                "snapshot_id": snapshot_id,
                "status": "snapshot_created_and_downloaded",
            }
        else:
            # This is a direct profiles response, save the profiles
            profiles = result
            if isinstance(profiles, list):
                collection.insert_many(profiles)
            else:
                collection.insert_one(profiles)

            # Log the scraping activity
            log_entry = {
                "timestamp": datetime.now(),
                "search_urls": search_urls,
                "countries": countries,
                "profiles_count": (
                    len(profiles) if isinstance(profiles, list) else 1
                ),
                "status": "success",
            }

        db.scraping_logs.insert_one(log_entry)

    except Exception as e:
        # Log error
        error_entry = {
            "timestamp": datetime.now(),
            "search_urls": search_urls,
            "countries": countries,
            "error": str(e),
            "status": "error",
        }
        db.scraping_logs.insert_one(error_entry)


def download_snapshot_data(snapshot_id):
    """Download snapshot data from BrightData and save to MongoDB"""
    try:
        # Get snapshot data
        url = f"{BRIGHTDATA_API_URL}/snapshot/{snapshot_id}"
        headers = {"Authorization": f"Bearer {BRIGHTDATA_AUTH_TOKEN}"}
        params = {"format": "json"}

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            snapshot_data = response.json()

            # Save snapshot data to MongoDB
            if isinstance(snapshot_data, list):
                collection.insert_many(snapshot_data)
                count = len(snapshot_data)
            else:
                collection.insert_one(snapshot_data)
                count = 1

            # Update snapshot status in database
            snapshots_collection.update_one(
                {"snapshot_id": snapshot_id},
                {
                    "$set": {
                        "downloaded_at": datetime.now(),
                        "status": "downloaded",
                        "profiles_count": count,
                    }
                },
            )

            # Log the download activity
            log_entry = {
                "timestamp": datetime.now(),
                "snapshot_id": snapshot_id,
                "profiles_count": count,
                "status": "snapshot_downloaded",
            }
            db.scraping_logs.insert_one(log_entry)

            return True, count
        else:
            # Snapshot might not be ready yet, wait and retry
            time.sleep(5)
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                snapshot_data = response.json()

                if isinstance(snapshot_data, list):
                    collection.insert_many(snapshot_data)
                    count = len(snapshot_data)
                else:
                    collection.insert_one(snapshot_data)
                    count = 1

                snapshots_collection.update_one(
                    {"snapshot_id": snapshot_id},
                    {
                        "$set": {
                            "downloaded_at": datetime.now(),
                            "status": "downloaded",
                            "profiles_count": count,
                        }
                    },
                )

                log_entry = {
                    "timestamp": datetime.now(),
                    "snapshot_id": snapshot_id,
                    "profiles_count": count,
                    "status": "snapshot_downloaded_retry",
                }
                db.scraping_logs.insert_one(log_entry)

                return True, count
            else:
                raise Exception(
                    f"Failed to download snapshot: {response.status_code}"
                )

    except Exception as e:
        # Log error
        error_entry = {
            "timestamp": datetime.now(),
            "snapshot_id": snapshot_id,
            "error": str(e),
            "status": "download_error",
        }
        db.scraping_logs.insert_one(error_entry)
        return False, 0


@app.route("/create_snapshot", methods=["POST"])
def create_snapshot():
    try:
        # Trigger snapshot creation
        url = f"{BRIGHTDATA_API_URL}/snapshots"
        headers = {"Authorization": f"Bearer {BRIGHTDATA_AUTH_TOKEN}"}
        params = {"dataset_id": BRIGHTDATA_DATASET_ID, "status": "ready"}

        response = requests.post(url, headers=headers, params=params)
        result = response.json()

        # Save snapshot info to MongoDB
        snapshot_data = {
            "timestamp": datetime.now(),
            "snapshot_id": result.get("snapshot_id"),
            "status": "requested",
            "type": "manual_creation",
        }
        snapshots_collection.insert_one(snapshot_data)

        return (
            jsonify({"message": "Snapshot creation requested", "data": result}),
            202,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/get_snapshot/<snapshot_id>", methods=["POST"])
def get_snapshot(snapshot_id):
    try:
        success, count = download_snapshot_data(snapshot_id)

        if success:
            return (
                jsonify(
                    {
                        "message": f"Snapshot {snapshot_id} downloaded and saved",
                        "profiles_count": count,
                    }
                ),
                200,
            )
        else:
            return (
                jsonify(
                    {"error": f"Failed to download snapshot {snapshot_id}"}
                ),
                500,
            )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/get_snapshots", methods=["GET"])
def get_snapshots():
    try:
        # Get list of snapshots from BrightData
        url = f"{BRIGHTDATA_API_URL}/snapshots"
        headers = {"Authorization": f"Bearer {BRIGHTDATA_AUTH_TOKEN}"}
        params = {"dataset_id": BRIGHTDATA_DATASET_ID, "status": "ready"}

        response = requests.get(url, headers=headers, params=params)
        brightdata_snapshots = response.json()

        # Also get snapshots from our database
        db_snapshots = list(snapshots_collection.find().sort("timestamp", -1))

        # Convert ObjectId to string for JSON serialization
        for snapshot in db_snapshots:
            snapshot["_id"] = str(snapshot["_id"])
            if "timestamp" in snapshot:
                snapshot["timestamp"] = snapshot["timestamp"].isoformat()
            if "downloaded_at" in snapshot:
                snapshot["downloaded_at"] = snapshot[
                    "downloaded_at"
                ].isoformat()

        return (
            jsonify(
                {
                    "brightdata_snapshots": brightdata_snapshots,
                    "database_snapshots": db_snapshots,
                }
            ),
            200,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/get_profiles", methods=["GET"])
def get_profiles():
    try:
        limit = int(request.args.get("limit", 10))
        skip = int(request.args.get("skip", 0))
        snapshot_id = request.args.get("snapshot_id")

        # Build query based on whether snapshot_id is provided
        query = {}
        if snapshot_id:
            query["snapshot_id"] = snapshot_id

        profiles = list(collection.find(query).skip(skip).limit(limit))
        total = collection.count_documents(query)

        # Convert ObjectId to string for JSON serialization
        for profile in profiles:
            profile["_id"] = str(profile["_id"])

        return (
            jsonify(
                {
                    "profiles": profiles,
                    "total": total,
                    "skip": skip,
                    "limit": limit,
                    "snapshot_id": snapshot_id,
                }
            ),
            200,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/get_snapshot_profiles/<snapshot_id>", methods=["GET"])
def get_snapshot_profiles(snapshot_id):
    """Get profiles from a specific snapshot"""
    try:
        limit = int(request.args.get("limit", 10))
        skip = int(request.args.get("skip", 0))

        # Query for profiles from this snapshot
        query = {"snapshot_id": snapshot_id}
        profiles = list(collection.find(query).skip(skip).limit(limit))
        total = collection.count_documents(query)

        # Convert ObjectId to string for JSON serialization
        for profile in profiles:
            profile["_id"] = str(profile["_id"])

        return (
            jsonify(
                {
                    "profiles": profiles,
                    "total": total,
                    "skip": skip,
                    "limit": limit,
                    "snapshot_id": snapshot_id,
                }
            ),
            200,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/get_logs", methods=["GET"])
def get_logs():
    try:
        limit = int(request.args.get("limit", 10))
        logs = list(db.scraping_logs.find().sort("timestamp", -1).limit(limit))

        # Convert ObjectId to string for JSON serialization
        for log in logs:
            log["_id"] = str(log["_id"])
            # Convert timestamp to string
            log["timestamp"] = log["timestamp"].isoformat()

        return jsonify(logs), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/check_snapshot_status/<snapshot_id>", methods=["GET"])
def check_snapshot_status(snapshot_id):
    """Check the status of a specific snapshot"""
    try:
        url = f"{BRIGHTDATA_API_URL}/snapshot/{snapshot_id}/status"
        headers = {"Authorization": f"Bearer {BRIGHTDATA_AUTH_TOKEN}"}

        response = requests.get(url, headers=headers)
        status_data = response.json()

        return jsonify(status_data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
