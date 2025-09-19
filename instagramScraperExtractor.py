# I created this script to extract username, bio, followers, emails, extra links and extra usernames from snapshot.json
import json
import re
import sys
from urllib.parse import urlparse

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", re.I)
MENTION_RE = re.compile(r"@([A-Za-z0-9._]+)")
INSTAGRAM_URL_RE = re.compile(r"instagram\.com/([A-Za-z0-9._-]+)", re.I)


def find_emails(obj):
    s = json.dumps(obj)
    return set(EMAIL_RE.findall(s))


def find_mentions(obj):
    s = json.dumps(obj)
    return set(MENTION_RE.findall(s))


def find_instagram_usernames(obj):
    s = json.dumps(obj)
    return set(m.group(1) for m in INSTAGRAM_URL_RE.finditer(s))


def gather_links(obj):
    links = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str) and (
                v.startswith("http://") or v.startswith("https://")
            ):
                links.add(v)
            else:
                links |= gather_links(v)
    elif isinstance(obj, list):
        for it in obj:
            links |= gather_links(it)
    return links


def extract_accounts(data):
    out = []
    for item in data:
        account = (
            item.get("account")
            or item.get("profile_name")
            or item.get("full_name")
            or item.get("id")
        )
        bio = item.get("biography") or item.get("bio") or ""
        followers = item.get("followers") or item.get("followers_count") or None
        profile_url = (
            item.get("profile_url")
            or item.get("url")
            or item.get("profile_image_link")
        )
        # collect emails, mentions, links, extra usernames
        emails = set()
        mentions = set()
        inst_users = set()
        links = set()
        emails |= find_emails(item)
        mentions |= find_mentions(item)
        inst_users |= find_instagram_usernames(item)
        links |= gather_links(item)
        # also extract from posts captions
        posts = item.get("posts") or []
        for p in posts:
            emails |= find_emails(p)
            mentions |= find_mentions(p)
            inst_users |= find_instagram_usernames(p)
            links |= gather_links(p)
        out.append(
            {
                "account_key": account,
                "username": item.get("account")
                or item.get("input", {}).get("user_name")
                or item.get("profile_name"),
                "profile_url": profile_url,
                "full_name": item.get("full_name"),
                "bio": bio,
                "followers": followers,
                "emails": sorted(emails),
                "extra_links": sorted(links),
                "extra_usernames": sorted(
                    set(list(mentions) + list(inst_users))
                ),
                "timestamp": item.get("timestamp"),
            }
        )
    return out


def main(path="snapshot.json"):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        # maybe wrapper
        records = data.get("items") or data.get("data") or [data]
    else:
        records = data
    extracted = extract_accounts(records)
    print(json.dumps(extracted, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "snapshot.json")
