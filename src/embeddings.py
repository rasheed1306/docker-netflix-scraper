"""
OpenAI embeddings: batch 20 descriptions into one API call per page.
"""

import os
from typing import List
import openai


def batch_embed(descriptions: List[str]) -> List[List[float]]:
    """
    Generate embeddings for a batch of descriptions (up to 20).
    
    Args:
        descriptions: List of movie descriptions
    
    Returns:
        List of embeddings (one per description)
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not set in environment")
    
    client = openai.OpenAI(api_key=api_key)
    
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=descriptions
    )
    
    # Extract embeddings in the same order as input
    embeddings = [item.embedding for item in response.data]
    return embeddings
