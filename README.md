# Netflix AU Movie Scraper

A containerised Python scraper that automatically fetches Netflix AU movies from TMDB, enriches them with IMDb ratings and AI embeddings, and stores everything in Supabase.

## Features

- **Weekly automated runs** — Scheduled for Sunday 2 AM AEST via APScheduler
- **Smart ingestion** — Detects cold start vs incremental mode; skips duplicate movies
- **Rich metadata** — Pulls movie details, trailers, genres, ratings, and runtime
- **AI embeddings** — Generates vector embeddings for semantic search (1536-dim, `text-embedding-3-small`)
- **Fallback logic** — Uses YouTube API for trailers if TMDB has none
- **Error resilience** — Retry logic on external APIs + Postgres transient errors; detailed logging

## What Gets Created

| Component | Purpose |
|-----------|---------|
| `main.py` | Orchestrates the full scrape: detects mode, loops pages, calls all modules |
| `db.py` | Handles all Supabase operations: load IDs, upsert movies, write logs |
| `tmdb.py` | Fetches movie metadata from TMDB API with retries |
| `omdb.py` | Fetches IMDb ratings |
| `youtube.py` | Fallback trailer lookup via YouTube API |
| `embeddings.py` | Batches descriptions → OpenAI embeddings (20 movies per call) |
| `scheduler.py` | APScheduler daemon: runs on schedule or immediate if >6 days since last run |
| `Dockerfile` | Python 3.11-slim + `uv` package manager |
| `docker-compose.yml` | `restart: unless-stopped`, mounts source, loads `.env` |
| `.env` | API keys: `TMDB_API_KEY`, `OMDB_API_KEY`, `OPENAI_API_KEY`, `SUPABASE_DB_URL` |

## How It Works

### Run Mode Detection
- **Cold start**: If `public.movies` is empty, fetch from 2020-01-01
- **Incremental**: Otherwise, fetch from `MAX(added_at) - 8 days`

### Null Rating Backfill Pass
At the start of every run, before the page loop:
1. Query all rows where `rating IS NULL` and `imdb_id IS NOT NULL`
2. Call OMDB concurrently for all candidates
3. Update rows where a rating is now available
4. Skip still-NULL results — they'll be retried next week

### Per-Page Pipeline (20 movies)
1. Filter out movies already in DB (using `tmdb_id` set)
2. Fetch details: `/movie/{id}` → `/movie/{id}/videos` → OMDB rating
3. Batch all descriptions into one OpenAI embedding call
4. Upsert to Supabase (embeddings never overwritten on re-runs)
5. Close DB connection, sleep 1s, reconnect
6. Log the run (success/failed + error message)

## Database Schema

### `public.movies`
- `id` (UUID, primary key)
- `tmdb_id` (int, unique, upsert key)
- `title` (text)
- `description` (text)
- `embedding` (vector, 1536 dims) — *never overwritten*
- `poster_url`, `trailer_url` (nullable text)
- `genre`, `runtime`, `rating` (nullable)
- `release_year` (int)
- `added_at` (timestamp)

### `public.ingestion_logs`
- `id` (UUID, primary key)
- `run_at` (timestamp)
- `status` (text: `success` or `failed`)
- `movies_added` (int, nullable)
- `error` (text, nullable)

## Environment Setup

### `.env` Configuration
Copy `.env.example` to `.env` and fill in your API keys:

```bash
cp .env.example .env
```

Required variables:
- `TMDB_API_KEY` — from [TMDB API](https://www.themoviedb.org/settings/api)
- `OMDB_API_KEY` — from [OMDB API](http://www.omdbapi.com/apikey.aspx)
- `OPENAI_API_KEY` — from [OpenAI Platform](https://platform.openai.com/api-keys)
- `SUPABASE_DB_URL` — PostgreSQL connection string from Supabase
- `YOUTUBE_API_KEY` — from [Google Cloud Console](https://console.cloud.google.com/) (required — many TMDB video endpoints return false data)

## About `uv`

This project uses **`uv`**, a modern Python package manager that's significantly faster than `pip`:

- **Speed**: 10–100x faster than pip, even on first install
- **Deterministic builds**: `uv.lock` ensures reproducible environments across machines
- **Single binary**: Ships as a standalone executable, simplifies Docker setup
- **Workspace support**: Manages multiple Python projects easily

The `pyproject.toml` declares all dependencies. `uv` handles installation in the Docker container automatically—no need for `pip install -r requirements.txt`.

## Quick Start

### Prerequisites
- Docker & Docker Compose
- `.env` file (copy from `.env.example`)

### Setup & Test
```bash
# 1. Set MAX_PAGES = 1 in main.py for testing
docker compose run ingestion uv run main.py

# 2. Verify in Supabase:
#    - 20 rows in public.movies
#    - Ratings, embeddings populated
#    - Some NULL trailer URLs (expected)
#    - 1 success entry in ingestion_logs

# 3. Run full cold start (set MAX_PAGES = 500)
#    Keep your machine on — takes several hours
```

### Production
```bash
# Start the scheduler daemon
docker compose up -d

# View logs
docker compose logs -f ingestion

# Run ad-hoc scrape
docker compose exec ingestion uv run main.py
```

### Local Development (without Docker)
```bash
# Install uv (if not already installed)
# On macOS: brew install uv
# On Windows: pip install uv
# Or download from https://github.com/astral-sh/uv

# Install dependencies
uv sync

# Run the scraper
uv run main.py

# Run a specific module
uv run python -m tmdb
```

## Known Limitations

- **Minor catalogue gaps**: TMDB's Netflix AU tags are community-sourced
- **NULL ratings**: New releases may not yet be indexed in OMDB — the backfill pass retries these automatically on every subsequent run
- **NULL trailers**: Expected for some movies; UI should hide the trailer button conditionally
- **Cold start duration**: Full fetch requires laptop to stay on (~8 hours for 500 pages)
- **Incremental runs**: Fast (~10–15 seconds) once DB is populated
