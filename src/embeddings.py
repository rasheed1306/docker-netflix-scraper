"""
OpenAI embeddings: batch 20 descriptions into one API call per page.
"""

import os
from typing import List
import openai
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    retry=retry_if_exception_type(openai.RateLimitError)
)
def _clean(text: str) -> str:
    """Strip whitespace and remove control characters that break JSON."""
    import re
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text).strip()


def batch_embed(descriptions: List[str]) -> List[List[float]]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not set in environment")

    cleaned = [_clean(d) for d in descriptions]
    if not cleaned or all(d == "" for d in cleaned):
        return [[] for _ in descriptions]

    client = openai.OpenAI(api_key=api_key)
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=cleaned
    )
    return [item.embedding for item in response.data]
