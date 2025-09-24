import http.client
import json
import re
import time

API_KEY = "f450c47ce325dc0e78935fdebee2ff21b34a6fe9"

queries = [
    'site:snapchat.com "Harlem" profile',
    'site:snapchat.com "Manhattan" profile',
    'site:snapchat.com "musician" profile',
    'site:snapchat.com "artist" profile',
    'site:snapchat.com "model" profile',
    'site:snapchat.com "actor" profile',
    'site:snapchat.com "actress" profile',
    'site:snapchat.com "blogger" profile',
    'site:snapchat.com "photographer" profile',
]


queries += [
    'site:snapchat.com "Yankees" profile',
    'site:snapchat.com "Knicks" profile',
    'site:snapchat.com "Mets" profile',
    'site:snapchat.com "Giants" profile',
    'site:snapchat.com "Rangers" profile',
    'site:snapchat.com "NYCFC" profile',
    'site:snapchat.com "fitness" profile',
    'site:snapchat.com "fashion" profile',
    'site:snapchat.com "food" profile',
    'site:snapchat.com "chef" profile',
    'site:snapchat.com "dancer" profile',
    'site:snapchat.com "comedian" profile',
    'site:snapchat.com "lifestyle" profile',
    'site:snapchat.com "travel" profile',
]


def fetch_profiles_serper(query, page=1):
    try:
        conn = http.client.HTTPSConnection("google.serper.dev", timeout=10)
        payload = json.dumps({"q": query, "page": page})
        headers = {"X-API-KEY": API_KEY, "Content-Type": "application/json"}
        conn.request("POST", "/search", payload, headers)
        res = conn.getresponse()
        data = json.loads(res.read())
        results = []
        for r in data.get("organic", []):
            link = r.get("link")
            if link and "snapchat.com" in link:
                results.append(link)
        print(
            f"[INFO] Fetched {len(results)} links for query: {query} | Page: {page}"
        )
        return results
    except Exception as e:
        print(f"[ERROR] Request failed for query '{query}' page {page}: {e}")
        traceback.print_exc()
        time.sleep(5)
        return []


def clean_username(link):
    match = re.search(r"snapchat\.com/(?:add/|@)([A-Za-z0-9._-]+)", link)
    return match.group(1) if match else None


all_usernames = set()
with open("snapchat_usernames_nyc_serper.txt", "a") as f:
    for query in queries:
        print(f"[INFO] Starting query: {query}")
        seen_links = set()
        page = 1
        while len(all_usernames) < 5000:
            profiles = fetch_profiles_serper(query, page)
            if not profiles:
                print(
                    f"[WARN] No profiles found for query: {query} page {page}."
                )
                break
            new_links = [link for link in profiles if link not in seen_links]
            if not new_links:
                print(
                    f"[INFO] All fetched links already processed for query: {query} page {page}."
                )
                break
            for link in new_links:
                seen_links.add(link)
                username = clean_username(link)
                if username and username not in all_usernames:
                    all_usernames.add(username)
                    f.write(username + "\n")
                    f.flush()
                    print(
                        f"[NEW] Added username: {username} | Total: {len(all_usernames)}"
                    )
            page += 1
            time.sleep(2)
        if len(all_usernames) >= 5000:
            break

print("[DONE] Collected usernames:", len(all_usernames))
