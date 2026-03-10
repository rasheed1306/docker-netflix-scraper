"""
YouTube API client: fallback trailer lookup when TMDB videos returns nothing.
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
async def get_trailer(title: str, year: int) -> Optional[str]:
    """
    Fallback: fetch trailer URL from YouTube API when TMDB has none.
    
    Args:
        title: Movie title
        year: Release year
    
    Returns:
        YouTube trailer URL or None
    """
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        return None
    
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet",
        "type": "video",
        "maxResults": "1",
        "q": f"{title} {year} official trailer",
        "key": api_key
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        items = data.get("items", [])
        if items:
            video_id = items[0].get("id", {}).get("videoId")
            if video_id:
                return f"https://www.youtube.com/watch?v={video_id}"
        
        return None
