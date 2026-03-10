"""
Main orchestrator: detects cold start vs incremental, loops pages, calls all modules.
Enriches movies concurrently, upserts to Supabase.
"""

import asyncio
import os
import logging
from datetime import timedelta
from typing import List, Dict, Optional

from dotenv import load_dotenv
from src import db, tmdb, omdb, youtube, embeddings

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_PAGES = 500  # Stage 1: 1 page. Stage 2: 500 pages


async def enrich_movie(movie: Dict, existing_ids: set) -> Optional[Dict]:
    """
    Enrich a single movie with detail, rating, trailer, and prepare for embedding.
    
    Args:
        movie: Raw TMDB discover result
        existing_ids: Set of known TMDB IDs for deduplication
    
    Returns:
        Enriched movie dict or None if already exists
    """
    tmdb_id: int | None = movie.get("id")
    
    if not tmdb_id or tmdb_id in existing_ids:
        return None
    
    title = movie.get("title")
    release_date_str = movie.get("release_date", "")
    release_year = int(release_date_str[:4]) if release_date_str else None
    
    try:
        detail = await tmdb.get_detail(tmdb_id)
    except Exception as e:
        logger.error(f"Error enriching movie {tmdb_id} ({title}): {e}")
        return None

    imdb_id = detail.get("imdb_id")

    # Step 2: Fetch rating (depends on imdb_id)
    rating = None
    if imdb_id:
        try:
            rating = await omdb.get_rating(imdb_id)
        except Exception as e:
            logger.warning(f"OMDB failed for {tmdb_id} ({title}): {e}")

    trailer_url = youtube.get_search_url(title, release_year) if title and release_year else None

    return {
        "tmdb_id": tmdb_id,
        "title": title,
        "description": detail.get("overview") or "",
        "poster_url": f"https://image.tmdb.org/t/p/w342{detail.get('poster_path')}" if detail.get("poster_path") else None,
        "trailer_url": trailer_url,
        "genre": detail.get("genres") or [],
        "runtime": detail.get("runtime"),
        "rating": rating,
        "release_year": release_year,
        "imdb_id": imdb_id,
        "embedding": None,  # Will be set after batch embedding
    }


async def process_page(page: int, min_date: Optional[str], existing_ids: set) -> tuple[List[Dict], int]:
    """
    Process a single page: discover, filter, enrich, embed, upsert.
    
    Returns:
        (enriched_movies, movies_added)
    """
    logger.info(f"Processing page {page}...")
    
    # Discover movies
    raw_movies = tmdb.discover_movies(page, min_date)
    logger.info(f"Discovered {len(raw_movies)} movies on page {page}")
    
    # Filter against existing IDs
    new_movies = [m for m in raw_movies if m.get("id") not in existing_ids]
    logger.info(f"After dedup: {len(new_movies)} new movies")
    
    if not new_movies:
        return [], 0
    
    # Enrich all movies concurrently
    enriched = await asyncio.gather(*[enrich_movie(m, existing_ids) for m in new_movies])
    enriched_movies = [m for m in enriched if m is not None]
    logger.info(f"Enriched {len(enriched_movies)} movies")
    
    if not enriched_movies:
        return [], 0
    
    # Batch embed descriptions — skip movies with no/whitespace-only description
    embeddable = [m for m in enriched_movies if (m.get("description") or "").strip()]
    if embeddable:
        try:
            embeddings_list = embeddings.batch_embed([m["description"] for m in embeddable])
            for movie, embedding in zip(embeddable, embeddings_list):
                movie["embedding"] = embedding
            logger.info(f"Generated embeddings for {len(embeddable)} movies")
        except Exception as e:
            logger.error(f"Error generating embeddings (skipping upsert for this page): {e}")
            return [], 0
    
    # Upsert to database
    try:
        db.upsert_movies(enriched_movies)
        logger.info(f"Upserted {len(enriched_movies)} movies to database")
    except Exception as e:
        logger.error(f"Error upserting movies: {e}")
    
    # Sleep 1s between pages
    await asyncio.sleep(1)
    
    return enriched_movies, len(enriched_movies)


async def backfill_ratings() -> None:
    candidates = db.get_null_candidates("rating")
    if not candidates:
        logger.info("No null-rating candidates to backfill.")
        return

    logger.info(f"Backfilling ratings for {len(candidates)} movies...")
    sem = asyncio.Semaphore(10)
    updates = []

    async def try_fetch(c: dict) -> None:
        async with sem:
            try:
                rating = await omdb.get_rating(c["imdb_id"])
                if rating is not None:
                    updates.append((rating, c["tmdb_id"]))
            except Exception as e:
                logger.warning(f"Rating backfill failed for tmdb_id={c['tmdb_id']}: {e}")

    await asyncio.gather(*[try_fetch(c) for c in candidates])
    db.batch_update("rating", updates)
    logger.info(f"Rating backfill complete. Updated {len(updates)} movies.")


async def backfill_embeddings() -> None:
    candidates = db.get_null_candidates("embedding")
    if not candidates:
        logger.info("No null-embedding candidates to backfill.")
        return

    logger.info(f"Backfilling embeddings for {len(candidates)} movies...")
    updates = []
    # Filter out movies with no/whitespace-only description — OpenAI rejects empty inputs
    embeddable = [c for c in candidates if (c.get("description") or "").strip()]
    skipped = len(candidates) - len(embeddable)
    if skipped:
        logger.info(f"Skipping {skipped} movies with no description")
    for i in range(0, len(embeddable), 20):
        chunk = embeddable[i:i + 20]
        try:
            vectors = embeddings.batch_embed([c["description"] for c in chunk])
            updates.extend((v, c["tmdb_id"]) for c, v in zip(chunk, vectors))
        except Exception as e:
            logger.warning(f"Embedding backfill failed for chunk at {i}: {e}")
    db.batch_update("embedding", updates)
    logger.info(f"Embedding backfill complete. Updated {len(updates)} movies.")


async def backfill_trailers() -> None:
    candidates = db.get_null_candidates("trailer_url")
    if not candidates:
        logger.info("No null-trailer candidates to backfill.")
        return

    logger.info(f"Backfilling trailers for {len(candidates)} movies...")
    updates = []
    for c in candidates:
        if c["title"] and c["release_year"]:
            url = youtube.get_search_url(c["title"], c["release_year"])
            if url:
                updates.append((url, c["tmdb_id"]))
    db.batch_update("trailer_url", updates)
    logger.info(f"Trailer backfill complete. Updated {len(updates)} movies.")


async def run_scraper():
    """
    Main scraper orchestrator.
    """
    total_added = 0
    error_msg = None
    
    try:
        logger.info("Starting Netflix AU movie scraper...")
        
        # Detect cold start vs incremental
        is_cold = db.is_cold_start()
        
        if is_cold:
            logger.info("Cold start detected. Discovering from 2020-01-01")
            min_date = "2020-01-01"
        else:
            last_date = db.get_last_ingestion_date()
            if last_date is None:
                min_date = "2020-01-01"
            else:
                min_date = (last_date - timedelta(days=8)).strftime("%Y-%m-%d")
            logger.info(f"Incremental run. Discovering from {min_date}")
        
        # Load existing IDs once
        existing_ids = db.load_existing_tmdb_ids()
        logger.info(f"Loaded {len(existing_ids)} existing TMDB IDs")

        # Backfill missing data for existing movies
        await backfill_ratings()
        await backfill_embeddings()
        await backfill_trailers()

        # Process pages
        for page in range(1, MAX_PAGES + 1):
            _, movies_added = await process_page(page, min_date, existing_ids)
            total_added += movies_added
        
        logger.info(f"Scraper completed. Total movies added: {total_added}")
        db.write_ingestion_log("success", total_added)
        
    except Exception as e:
        logger.error(f"Scraper error: {e}")
        error_msg = str(e)
        db.write_ingestion_log("error", total_added, error_msg)


def main():
    """Entry point."""
    asyncio.run(run_scraper())


if __name__ == "__main__":
    main()
