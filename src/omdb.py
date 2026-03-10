"""
OMDB API client: fetch IMDb ratings by IMDb ID.
Uses tenacity for retry logic on HTTP errors.
"""

import os
import httpx
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception


def should_retry(exception: Exception) -> bool:
    """Retry on HTTP 429 (rate limit) or 5xx errors."""
    if isinstance(exception, httpx.HTTPStatusError):
        return exception.response.status_code in [429, 500, 502, 503, 504]
    return False


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=4),
    retry=retry_if_exception(should_retry)
)
async def get_rating(imdb_id: str) -> Optional[float]:
    """
    Fetch IMDb rating from OMDB API.
    
    Args:
        imdb_id: IMDb ID (e.g., "tt1234567")
    
    Returns:
        Rating as float or None if not found
    """
    api_key = os.getenv("OMDB_API_KEY")
    if not api_key:
        raise ValueError("OMDB_API_KEY not set in environment")
    
    if not imdb_id:
        return None
    
    url = "http://www.omdbapi.com/"
    params = {
        "i": imdb_id,
        "apikey": api_key
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data.get("Response") == "True":
            rating_str = data.get("imdbRating")
            if rating_str and rating_str != "N/A":
                return float(rating_str)
        
        return None
