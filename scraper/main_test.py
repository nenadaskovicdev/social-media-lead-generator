import asyncio
import logging
import os
from datetime import datetime
from typing import List

import motor.motor_asyncio
from dotenv import load_dotenv

from .instagram_scraper import InstagramScraper
from .utils import get_env_list

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def scrape_and_store(country: str):
    # Setup MongoDB connection
    client = motor.motor_asyncio.AsyncIOMotorClient(
        os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    )
    db = client[os.getenv("DATABASE_NAME", "lead_generation")]
    collection = db[os.getenv("COLLECTION_NAME", "instagram_leads")]

    # Get search keywords
    keywords = get_env_list("SCRAPER_KEYWORDS", ["designer", "photographer"])

    async with InstagramScraper(headless=True) as scraper:
        all_usernames = []

        # Discover profiles based on keywords and country
        for keyword in keywords:
            logger.info(
                f"Discovering profiles for keyword: {keyword} in {country}"
            )
            usernames = await scraper.discover_profiles(
                keyword, country, limit=20
            )
            all_usernames.extend(usernames)

        # Remove duplicates
        all_usernames = list(set(all_usernames))
        logger.info(f"Found {len(all_usernames)} unique profiles to scrape")

        # Scrape each profile
        successful_scrapes = 0
        for username in all_usernames:
            logger.info(f"Scraping profile: {username}")
            profile_data = await scraper.scrape_profile(username, country)

            if profile_data:
                # Add timestamp and source country
                profile_data["timestamp"] = datetime.utcnow()
                profile_data["source_country"] = country

                # Update or insert in MongoDB
                await collection.update_one(
                    {"username": profile_data["username"]},
                    {"$set": profile_data},
                    upsert=True,
                )
                successful_scrapes += 1
                logger.info(f"Successfully stored profile: {username}")

        logger.info(
            f"Scraping completed. Successfully stored {successful_scrapes} profiles"
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Instagram Lead Generation Scraper"
    )
    parser.add_argument(
        "--country", required=True, help="Target country for lead generation"
    )

    args = parser.parse_args()

    asyncio.run(scrape_and_store(args.country.lower()))
