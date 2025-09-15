
Social Media Scraper - TikTok Integration Update
Project Description

This project is a social media profile scraper that collects data from Instagram and TikTok profiles. The system uses SerpAPI to discover relevant profiles and then scrapes detailed information from each platform. The scraped data is stored in MongoDB for persistence and can be exported to JSON format.
Current Implementation Status
Completed Features:

    Instagram Profile Scraping

        Uses Instaloader library to extract detailed profile information

        Collects username, full name, follower count, following count, posts count, bio, and profile URL

        Implements retry logic and error handling for failed requests

        Filters profiles by minimum follower count (5,000+)

    TikTok Profile Discovery

        Uses SerpAPI to find TikTok profiles based on search queries

        Extracts profile URLs from search results

        Stores basic profile information (URL and username)

    Data Storage

        MongoDB integration for storing scraped profiles

        Prevents duplicate entries using unique indexes

        Caches SerpAPI results to avoid redundant API calls

    Rate Limiting & Bot Evasion

        Implements random delays between requests

        Uses rotating user agents

        Handles platform-specific rate limits and blocks

    Export Functionality

        Exports all collected profiles to JSON format

Next Steps: TikTok API Integration
Planned TikTok Enhancements:

    TikTok API Integration

        Implement official TikTok API access for richer data collection

        Extract detailed metrics: follower count, following count, likes, videos count

        Retrieve engagement metrics and content analytics

    Enhanced TikTok Data Points

        Profile verification status

        Video statistics (views, likes, comments, shares)

        Follower demographics and growth trends

        Content categorization and hashtag analysis

    Advanced Filtering

        Filter by engagement rate

        Filter by content type/category

        Filter by follower growth patterns

    Batch Processing

        Process multiple TikTok profiles efficiently

        Implement pagination for profiles with many videos

Technical Requirements for TikTok API:

    TikTok Business Account access

    API credentials and authentication setup

    Compliance with TikTok's API usage policies

    Implementation of rate limiting specific to TikTok's API constraints

Setup Instructions

    Install dependencies:

bash

pip install -r requirements.txt

    Configure environment variables in .env:

text

MONGO_URI=your_mongodb_connection_string
SERPAPI=your_serpapi_key
TIKTOK_ACCESS_TOKEN=your_tiktok_api_access_token (to be added)

    Run the scraper:

bash

python main.py

