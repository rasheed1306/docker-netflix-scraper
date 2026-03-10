"""
YouTube fallback: generates a YouTube search URL when TMDB videos returns nothing.
No API call needed — zero quota usage.
"""

from urllib.parse import quote_plus
from typing import Optional


def get_search_url(title: str, year: int) -> Optional[str]:
    if not title or not year:
        return None
    query = quote_plus(f"{title} {year} official trailer")
    return f"https://www.youtube.com/results?search_query={query}"
