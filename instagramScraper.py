import requests

url = "https://api.brightdata.com/datasets/v3/trigger"
headers = {
    "Authorization": "Bearer 89780cb1aa76643eed7123a9473c3254f83cb79661afb22272a99ac50cd0b280",
    "Content-Type": "application/json",
}
params = {
    "dataset_id": "gd_l1vikfch901nx3by4",
    "include_errors": "true",
    "type": "discover_new",
    "discover_by": "user_name",
}

urls = [
    "https://www.instagram.com/newyork/",
    "https://www.instagram.com/humansofny/",
    "https://www.instagram.com/nyctourism/",
    "https://www.instagram.com/pictures.of.ny/",
    "https://www.instagram.com/visitnewyork/",
    "https://www.instagram.com/whatisnewyork/",
    "https://www.instagram.com/nybucketlist/",
    "https://www.instagram.com/landmarksofny/",
    "https://www.instagram.com/nycity__ig/",
    "https://www.instagram.com/visitnewyork/",
    "https://www.instagram.com/untappedny/",
    "https://www.instagram.com/loving_newyork/",
    "https://www.instagram.com/nyclivesnyc/",
    "https://www.instagram.com/iloveny/",
    "https://www.instagram.com/nyc/",
    "https://www.instagram.com/new.york.ing/",
    "https://www.instagram.com/newyorkcityfeelings/",
    "https://www.instagram.com/nyc.thenandnow/",
    "https://www.instagram.com/peopleofnewyork_/",
    "https://www.instagram.com/mynewyork_/",
    "https://www.instagram.com/hernewyorkedit/",
    "https://www.instagram.com/newyorkcityskyline/",
    "https://www.instagram.com/newyorkcity/",
    "https://www.instagram.com/peopleofnewyork_/",
    "https://www.instagram.com/iloveny/",
    "https://www.instagram.com/untappedny/",
    "https://www.instagram.com/loving_newyork/",
    "https://www.instagram.com/new.york.ing/",
    "https://www.instagram.com/nyuniversity/",
]

data = [{"user_name": u.strip("/").split("/")[-1]} for u in urls]

response = requests.post(url, headers=headers, params=params, json=data)
print(response.json())
