import requests

SNAPSHOT_ID = "s_mfpvzexdpsqzr9088"  # replace with real snapshot id
url = f"https://api.brightdata.com/datasets/v3/snapshot/{SNAPSHOT_ID}"

headers = {
    "Authorization": "Bearer 89780cb1aa76643eed7123a9473c3254f83cb79661afb22272a99ac50cd0b280",
}

params = {"format": "json"}

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    with open("snapshot.json", "wb") as f:
        f.write(response.content)
    print("Downloaded snapshot.json")
else:
    print("Error:", response.status_code, response.text)
