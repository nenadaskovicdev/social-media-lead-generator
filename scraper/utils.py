import asyncio
import os
import random
import re
from typing import List, Optional

from dotenv import load_dotenv
from email_validator import EmailNotValidError, validate_email

load_dotenv()


def get_env_list(key: str, default: List[str] = []) -> List[str]:
    value = os.getenv(key)
    if value:
        return [item.strip() for item in value.split(",")]
    return default


def extract_emails(text: str) -> List[str]:
    if not text:
        return []

    email_regex = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
    potential_emails = re.findall(email_regex, text)

    valid_emails = []
    for email in potential_emails:
        try:
            valid = validate_email(email)
            valid_emails.append(valid.email)
        except EmailNotValidError:
            continue

    return valid_emails


def extract_phone_numbers(text: str) -> List[str]:
    if not text:
        return []

    # Basic international phone number pattern
    phone_regex = (
        r"(\+?\d{1,3}[-.\s]?)?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}"
    )
    potential_phones = re.findall(phone_regex, text)

    return [
        phone[0] if isinstance(phone, tuple) else phone
        for phone in potential_phones
    ]


def should_include_profile(
    bio: str, geotags: List[str], target_country: str
) -> bool:
    if not target_country:
        return True

    target_country_lower = target_country.lower()

    # Check bio for country mention
    if bio and target_country_lower in bio.lower():
        return True

    # Check geotags for country mention
    if geotags:
        for tag in geotags:
            if tag and target_country_lower in tag.lower():
                return True

    return False


async def rate_limited_sleep():
    delay_ms = int(os.getenv("REQUEST_DELAY_MS", 2000))
    jitter = random.uniform(0.8, 1.2)  # Add 20% jitter
    await asyncio.sleep((delay_ms / 1000) * jitter)
