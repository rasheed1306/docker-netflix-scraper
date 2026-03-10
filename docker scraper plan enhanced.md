
### What You're Building

A Dockerised Python scraper that runs weekly, pulls new Netflix AU movies from TMDB, enriches them with IMDb ratings and embeddings, and stores them in Supabase.

**Status**: Fully implemented with automatic backfill passes for missing data and concurrency limits.

***

### Files to Create

| File                 | What it does                                                                                                                                                                                                                                              | Status |
| :------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----- |
| `main.py`            | Orchestrates the full run — detects cold start vs. incremental, loops pages, calls everything; includes backfill passes for ratings, embeddings, and trailers                                                                                           | ✅ Implemented |
| `db.py`              | All Supabase queries: load existing IDs, upsert movies, batch updates, write ingestion logs; consolidated functions with retry logic                                                                                                                   | ✅ Consolidated |
| `tmdb.py`            | Calls `/discover/movie?with_watch_providers=8&watch_region=AU`, `/movie/{id}` with retry logic                                                                                                                | ✅ |
| `youtube.py`         | Generates YouTube search URLs as trailer fallback — no API key, no quota, always returns a URL                                                                                                                | ✅ Replaced API with search URL |
| `omdb.py`            | Fetches IMDb rating using `imdb_id` from TMDB                                                                                                                                                                                                             | ✅ |
| `embeddings.py`      | Batches 20 movie descriptions → one OpenAI embedding call per page. Am using `text-embedding-3-small`                                                                                                                                                     | ✅ |
| `scheduler.py`       | APScheduler — runs every Sunday 2 AM AEST; triggers immediately if last run was >6 days ago                                                                                                                                                               | ✅ |
| `Dockerfile`         | `python:3.11-slim` base, `uv` for deps, no secrets baked in                                                                                                                                                                                               | ✅ |
| `docker-compose.yml` | `restart: unless-stopped`, mounts `./src`, loads `.env`                                                                                                                                                                                                   | ✅ |
| `.env`               | Holds `TMDB_API_KEY`, `OMDB_API_KEY`, `OPENAI_API_KEY`, `SUPABASE_DB_URL`, `YOUTUBE_API_KEY`                                                                                                                                                              | ✅ |


***

### Core Logic in `main.py`

1. **Detect run mode**: if `public.movies` is empty → cold start (fetch from 2020-01-01). Otherwise → incremental (`MAX(added_at) - 8 days`).
2. **Backfill passes** (run first on every execution):
   - Ratings: Query `rating IS NULL AND imdb_id IS NOT NULL`, call OMDB concurrently (10 at a time), batch update
   - Embeddings: Query `embedding IS NULL`, batch descriptions into OpenAI calls (20 per call), batch update
   - Trailers: Query `trailer_url IS NULL`, generate YouTube search URLs directly — no API call, instant, batch update
3. **Load existing IDs**: bulk fetch all `tmdb_id`s into a Python set — done once before the page loop.
4. **Per page (20 movies)**:
    - Filter out already-stored movies using the set
    - For each new movie: call `/movie/{id}` → OMDB rating → YouTube search URL as trailer fallback
    - Batch all descriptions → one OpenAI embedding call
    - Upsert everything to Supabase
    - Close DB connection, sleep 1s, open fresh connection
5. **Log the run** to `ingestion_logs` (`success` or `failed` + error message).

***

### Database Tables to Create in Supabase

**`public.movies`** — one row per movie, with `tmdb_id` as unique upsert key. Columns: `id`, `tmdb_id`, `title`, `description`, `embedding` (vector 1536), `poster_url`, `trailer_url`, `genre`, `runtime`, `rating`, `release_year`, `added_at`. Note: `embedding` is **never overwritten** on re-runs.[^2]

**`public.ingestion_logs`** — one row per run. Columns: `id`, `run_at`, `status`, `movies_added`, `error`.

***

### How to Test

1. Set `MAX_PAGES = 1` in `main.py`
2. Run `docker compose run ingestion python main.py`
3. Check Supabase for 20 rows — verify ratings, embeddings, and trailer URLs (all rows should have a URL); backfill passes run first and fill in any gaps
4. Check `ingestion_logs` for a `success` entry
5. Set `MAX_PAGES = 500` and run the full cold start overnight[^2]

***

### Known Limitations

- TMDB's Netflix AU tags are community-sourced — minor catalogue gaps are unavoidable[^2]
- New releases may have `NULL` ratings (OMDB not yet indexed) — handled gracefully with backfill retries
- `NULL` trailer URLs no longer occur — every movie gets a YouTube search URL fallback
- Laptop must stay on for the cold start; incremental runs are ~10–15 seconds[^2]


NOTE want to add:
- use tenacity for retry logic  make sure `db.py` also handles transient Postgres errors, not just the external APIs ✅ Implemented
