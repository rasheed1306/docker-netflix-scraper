"""
Supabase Postgres operations: load existing IDs, upsert movies, write ingestion logs.
Handles transient Postgres errors with tenacity retry.
"""

import os
from datetime import datetime
from typing import Set, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import psycopg


def get_db_connection():
    """Create and return a database connection."""
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise ValueError("SUPABASE_DB_URL not set in environment")
    return psycopg.connect(db_url)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=4),
    retry=retry_if_exception_type(psycopg.OperationalError)
)
def is_cold_start() -> bool:
    """Check if database is empty (cold start detection)."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.movies")
            count = cur.fetchone()[0]
            return count == 0


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=4),
    retry=retry_if_exception_type(psycopg.OperationalError)
)
def get_last_ingestion_date() -> Optional[datetime]:
    """Get the date of the last ingestion for incremental window calculation."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(added_at) FROM public.movies")
            result = cur.fetchone()[0]
            return result


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=4),
    retry=retry_if_exception_type(psycopg.OperationalError)
)
def load_existing_tmdb_ids() -> Set[int]:
    """Load all existing TMDB IDs from database into a set."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT tmdb_id FROM public.movies")
            return {row[0] for row in cur.fetchall()}


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=4),
    retry=retry_if_exception_type(psycopg.OperationalError)
)
def upsert_movies(movies: list) -> None:
    """
    Upsert movies to the database. Handles ON CONFLICT by updating all fields
    except embedding (embedding is never overwritten on re-runs).
    
    movies: List of dicts with keys:
        tmdb_id, title, description, poster_url, trailer_url, genre, runtime, rating, release_year, embedding
    """
    if not movies:
        return

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for movie in movies:
                cur.execute(
                    """
                    INSERT INTO public.movies 
                    (tmdb_id, title, description, poster_url, trailer_url, genre, runtime, rating, release_year, embedding)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tmdb_id) 
                    DO UPDATE SET
                        title = EXCLUDED.title,
                        description = EXCLUDED.description,
                        poster_url = EXCLUDED.poster_url,
                        trailer_url = EXCLUDED.trailer_url,
                        genre = EXCLUDED.genre,
                        runtime = EXCLUDED.runtime,
                        rating = EXCLUDED.rating,
                        release_year = EXCLUDED.release_year
                    """,
                    (
                        movie.get("tmdb_id"),
                        movie.get("title"),
                        movie.get("description"),
                        movie.get("poster_url"),
                        movie.get("trailer_url"),
                        movie.get("genre"),
                        movie.get("runtime"),
                        movie.get("rating"),
                        movie.get("release_year"),
                        movie.get("embedding"),
                    )
                )
            conn.commit()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=4),
    retry=retry_if_exception_type(psycopg.OperationalError)
)
def get_null_rating_candidates() -> list[dict]:
    """Return all movies where rating is NULL and imdb_id is known."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT tmdb_id, rating FROM public.movies "
                "WHERE rating IS NULL AND tmdb_id IS NOT NULL"
            )
            return [{"tmdb_id": row[0], "rating": row[1]} for row in cur.fetchall()]


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=4),
    retry=retry_if_exception_type(psycopg.OperationalError)
)
def update_rating(tmdb_id: int, rating: float) -> None:
    """Update the rating for a single movie by tmdb_id."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE public.movies SET rating = %s WHERE tmdb_id = %s",
                (rating, tmdb_id)
            )
            conn.commit()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=4),
    retry=retry_if_exception_type(psycopg.OperationalError)
)
def write_ingestion_log(status: str, movies_added: int, error: Optional[str] = None) -> None:
    """Write an ingestion log entry to track run status and progress."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.ingestion_logs (status, movies_added, error)
                VALUES (%s, %s, %s)
                """,
                (status, movies_added, error)
            )
            conn.commit()
