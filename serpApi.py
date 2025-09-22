import time

import requests

API_KEY = "a1e8f767523296597a62ad696a426bdbb74351c13b1abe2b672f0d1f22c7bcee"
query = 'site:tiktok.com "New York" profile'


def fetch_profiles(start=0):
    params = {
        "engine": "google",
        "q": query,
        "api_key": API_KEY,
        "num": 100,  # max per page
        "start": start,
    }
    response = requests.get("https://serpapi.com/search", params=params)
    data = response.json()
    results = []
    for result in data.get("organic_results", []):
        link = result.get("link")
        if link and "tiktok.com" in link:  # only TikTok
            results.append(link)
    return results


all_profiles = []
start = 0
while True:
    profiles = fetch_profiles(start)
    if not profiles:
        break
    all_profiles.extend(profiles)
    start += 100
    time.sleep(1)  # avoid rate limits

print("Total found:", len(all_profiles))
for p in all_profiles:
    print(p)
