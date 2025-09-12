import os
from typing import List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from motor.motor_asyncio import AsyncIOMotorClient

from .models import FilterRequest, InstagramLead, LeadResponse

load_dotenv()

app = FastAPI(title="Lead Generation Tool", version="1.0.0")
templates = Jinja2Templates(directory="web_app/templates")

# MongoDB connection
client = AsyncIOMotorClient(
    os.getenv("MONGODB_URI", "mongodb://localhost:27017")
)
db = client[os.getenv("DATABASE_NAME", "lead_generation")]
collection = db[os.getenv("COLLECTION_NAME", "instagram_leads")]


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/leads", response_model=LeadResponse)
async def get_leads(
    username: Optional[str] = None,
    country: Optional[str] = None,
    page: int = 1,
    limit: int = 20,
):
    skip = (page - 1) * limit
    query = {}

    if username:
        query["username"] = {"$regex": username, "$options": "i"}
    if country:
        query["country"] = {"$regex": country, "$options": "i"}

    try:
        cursor = collection.find(query).skip(skip).limit(limit)
        leads = await cursor.to_list(length=limit)
        total = await collection.count_documents(query)

        return LeadResponse(
            leads=leads,
            total=total,
            page=page,
            pages=(total + limit - 1) // limit,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/countries")
async def get_countries():
    try:
        countries = await collection.distinct("country")
        return {"countries": [c for c in countries if c]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
