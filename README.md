# Lead Generation Tool

A Python-based tool for scraping Instagram profiles, storing them in MongoDB, and browsing through a web interface.

## Features

- Instagram profile scraping using Playwright
- Country-based filtering
- Email and phone number extraction from bios
- MongoDB storage with duplicate prevention
- FastAPI web interface with search and pagination
- Configurable rate limiting and proxy support

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
playwright install
