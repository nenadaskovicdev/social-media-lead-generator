import os

from pymongo import MongoClient

MONGO_URI = (
    "mongodb://abeselom:strongpassword@localhost:27017/scraper?authSource=admin"
)

client = MongoClient(MONGO_URI)
db = client.tiktok_scraper_prod
posts = db.posts

results = []
for post in posts.find(
    {},
    {
        "author.username": 1,
        "author.fans": 1,
        "author.email": 1,
        "author.signature": 1,
        "_id": 0,
    },
):
    author = post.get("author", {})
    results.append(
        {
            "username": author.get("username"),
            "email": author.get("email"),
            "followers": author.get("fans"),
            "bio": author.get("signature"),  # bio field
        }
    )

for r in results:
    print(r)

print("Total:", len(results))
print("SAMPLE:", posts.find_one())
