import asyncio
import logging
import os
from typing import Dict, List, Optional
from urllib.parse import urljoin

from playwright.async_api import async_playwright

from .utils import (
    extract_emails,
    extract_phone_numbers,
    rate_limited_sleep,
    should_include_profile,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InstagramScraper:
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.base_url = "https://www.instagram.com/"

    async def __aenter__(self):
        self.playwright = await async_playwright().start()

        # Setup browser with proxy if configured
        proxy = None
        if os.getenv("PROXY_ENABLED", "False").lower() == "true":
            proxy_servers = os.getenv("PROXY_LIST", "").split(",")
            if proxy_servers:
                proxy = {"server": proxy_servers[0].strip()}

        self.browser = await self.playwright.chromium.launch(
            headless=self.headless, proxy=proxy
        )

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.browser.close()
        await self.playwright.stop()

    async def scrape_profile(
        self, username: str, target_country: str = None
    ) -> Optional[Dict]:
        context = await self.browser.new_context()
        page = await context.new_page()

        try:
            profile_url = urljoin(self.base_url, username)
            await page.goto(profile_url)
            await page.wait_for_selector("header section", timeout=10000)

            # Extract profile data
            profile_data = await page.evaluate(
                """() => {
                const header = document.querySelector('header section');
                if (!header) return null;
                
                const username = window.location.pathname.split('/').filter(Boolean)[0];
                const fullName = header.querySelector('h1') ? header.querySelector('h1').textContent : '';
                const bio = header.querySelector('.-vDIg span') ? header.querySelector('.-vDIg span').textContent : '';
                
                const metaElements = header.querySelectorAll('span._ac2a');
                const followers = metaElements[0] ? metaElements[0].getAttribute('title') || metaElements[0].textContent : '0';
                const following = metaElements[1] ? metaElements[1].textContent : '0';
                const posts = metaElements[2] ? metaElements[2].textContent : '0';
                
                // Extract recent post locations (if available)
                const postElements = document.querySelectorAll('article div div div div a');
                const geotags = [];
                for (let i = 0; i < Math.min(9, postElements.length); i++) {
                    const locationSpan = postElements[i].querySelector('div div div span');
                    if (locationSpan && locationSpan.textContent) {
                        geotags.push(locationSpan.textContent);
                    }
                }
                
                return {
                    username,
                    fullName,
                    bio,
                    followers: followers.replace(/,/g, ''),
                    following: following.replace(/,/g, ''),
                    posts: posts.replace(/,/g, ''),
                    geotags: Array.from(new Set(geotags)) // Remove duplicates
                };
            }"""
            )

            if not profile_data:
                return None

            # Extract contact information
            emails = extract_emails(profile_data["bio"])
            phone_numbers = extract_phone_numbers(profile_data["bio"])

            # Apply country filter
            if target_country and not should_include_profile(
                profile_data["bio"], profile_data["geotags"], target_country
            ):
                return None

            result = {
                "username": profile_data["username"],
                "full_name": profile_data["fullName"],
                "bio": profile_data["bio"],
                "follower_count": int(profile_data["followers"]),
                "following_count": int(profile_data["following"]),
                "post_count": int(profile_data["posts"]),
                "recent_geotags": profile_data["geotags"],
                "emails": emails,
                "phone_numbers": phone_numbers,
                "profile_url": profile_url,
                "country": target_country,
            }

            return result

        except Exception as e:
            logger.error(f"Error scraping profile {username}: {str(e)}")
            return None
        finally:
            await context.close()
            await rate_limited_sleep()

    async def discover_profiles(
        self, keyword: str, country: str, limit: int = 50
    ) -> List[str]:
        context = await self.browser.new_context()
        page = await context.new_page()
        usernames = []

        try:
            search_url = f"https://www.instagram.com/web/search/topsearch/?query={keyword} {country}"

            # Intercept the API response directly instead of waiting on page
            response = await context.request.get(search_url)
            data = await response.json()
            print(data)
            if "users" in data:
                for user in data["users"][:limit]:
                    if "user" in user and "username" in user["user"]:
                        usernames.append(user["user"]["username"])

        except Exception as e:
            logger.error(f"Error discovering profiles: {str(e)}")
        finally:
            await context.close()

        return usernames
