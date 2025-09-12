from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class InstagramLead(BaseModel):
    username: str
    full_name: Optional[str] = None
    bio: Optional[str] = None
    follower_count: int = 0
    following_count: int = 0
    post_count: int = 0
    recent_geotags: List[str] = []
    emails: List[str] = []
    phone_numbers: List[str] = []
    profile_url: str
    country: Optional[str] = None
    source_country: str
    timestamp: datetime


class LeadResponse(BaseModel):
    leads: List[InstagramLead]
    total: int
    page: int
    pages: int


class FilterRequest(BaseModel):
    username: Optional[str] = None
    country: Optional[str] = None
    page: int = 1
    limit: int = 20
