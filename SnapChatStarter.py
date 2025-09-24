import time

import requests

url = "https://api.brightdata.com/datasets/v3/trigger"
headers = {
    "Authorization": "Bearer 89780cb1aa76643eed7123a9473c3254f83cb79661afb22272a99ac50cd0b280",
    "Content-Type": "application/json",
}
params = {
    "dataset_id": "gd_maxv8l0y12r9y28uus",
    "include_errors": "true",
}

# Load usernames and remove duplicates
with open("snapchat_usernames_nyc_serper.txt") as f:
    usernames = list(dict.fromkeys(line.strip() for line in f if line.strip()))

print(f"[INFO] Loaded {len(usernames)} unique usernames.")


# Function to build payload
def build_payload(username):
    return {
        "url": f"https://www.snapchat.com/add/{username}",
        "collect_all_highlights": False,
    }


batch_size = 50
with open("snapchat_snapshot_ids.txt", "a") as out_file:
    for i in range(0, len(usernames), batch_size):
        batch = usernames[i : i + batch_size]
        data = [build_payload(u) for u in batch]
        print(
            f"[INFO] Sending batch {i//batch_size + 1} containing {len(batch)} usernames"
        )
        try:
            response = requests.post(
                url, headers=headers, params=params, json=data, timeout=60
            )
            resp_json = response.json()
            if response.status_code == 200:
                if isinstance(resp_json, list):
                    for item in resp_json:
                        snapshot_id = item.get("snapshot_id")
                        if snapshot_id:
                            out_file.write(snapshot_id + "\n")
                            print(f"[SUCCESS] Snapshot ID saved: {snapshot_id}")
                elif "snapshot_id" in resp_json:
                    snapshot_id = resp_json["snapshot_id"]
                    out_file.write(snapshot_id + "\n")
                    print(f"[SUCCESS] Snapshot ID saved: {snapshot_id}")
                else:
                    print(f"[WARN] No snapshot IDs returned: {resp_json}")
            else:
                print(
                    f"[WARN] Unexpected response ({response.status_code}): {resp_json}"
                )
        except Exception as e:
            print(f"[ERROR] Batch failed: {e}")
        time.sleep(5)
