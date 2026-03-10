"""
TMDB API client: discover movies, fetch detail, fetch videos.
Uses tenacity for retry logic on HTTP errors.
"""

import os
import httpx
from datetime import datetime
from typing import Optional, List, Dict
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
def discover_movies(page: int, min_date: Optional[str] = None) -> List[Dict]:
    """
    Discover Netflix AU movies from TMDB.
    
    Args:
        page: Page number for pagination
        min_date: Minimum release date in YYYY-MM-DD format
    
    Returns:
        List of movie dicts with tmdb_id and basic info
    """
    api_key = os.getenv("TMDB_API_KEY")
    if not api_key:
        raise ValueError("TMDB_API_KEY not set in environment")
    
    url = "https://api.themoviedb.org/3/discover/movie"
    params = {
        "api_key": api_key,
        "with_watch_providers": "8",  # Netflix
        "watch_region": "AU",
        "sort_by": "primary_release_date.desc",
        "primary_release_date.gte": min_date or "2020-01-01",
        "page": page
    }
    
    with httpx.Client() as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=4),
    retry=retry_if_exception(should_retry)
)
async def get_detail(tmdb_id: int) -> Dict:
    """
    Fetch detailed movie info from TMDB.
    
    Returns dict with: imdb_id, runtime, genres, poster_path, overview, release_date
    """
    api_key = os.getenv("TMDB_API_KEY")
    if not api_key:
        raise ValueError("TMDB_API_KEY not set in environment")
    
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
    params = {"api_key": api_key}
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        return {
            "imdb_id": data.get("imdb_id"),
            "runtime": data.get("runtime"),
            "genres": [g["name"] for g in data.get("genres", [])],
            "poster_path": data.get("poster_path"),
            "overview": data.get("overview"),
            "release_date": data.get("release_date"),
        }


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=4),
    retry=retry_if_exception(should_retry)
)
async def get_videos(tmdb_id: int) -> Optional[str]:
    """
    Fetch trailer URL from TMDB videos.
    
    Returns YouTube trailer URL or None.
    """
    api_key = os.getenv("TMDB_API_KEY")
    if not api_key:
        raise ValueError("TMDB_API_KEY not set in environment")
    
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/videos"
    params = {"api_key": api_key}
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Find YouTube trailer
        for video in data.get("results", []):
            if video.get("site") == "YouTube" and video.get("type") == "Trailer":
                video_id = video.get("key")
                return f"https://www.youtube.com/watch?v={video_id}"
        
        return None
