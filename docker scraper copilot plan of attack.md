## Full Plan of Attack — Netflix AU Movie Scraper

---

### What We're Building

A Dockerised Python scraper that runs weekly, pulls Netflix AU movies from TMDB (from 2020 onwards), enriches them with IMDb ratings and OpenAI embeddings, and stores everything in Supabase. Runs on a schedule (Sunday 2 AM AEST) with immediate catch-up if more than 6 days have passed since last run.

**Status**: Fully implemented with backfill passes, quota handling, and concurrency limits.

Delivery was split into two stages:
- **Stage 1** — working scraper, `MAX_PAGES = 1`, verify data in Supabase ✅
- **Stage 2** — Docker, scheduler, weekly automation ✅

---

### File Structure

| File | Purpose | Status |
|------|---------|--------|
| main.py | Orchestrates the full run: detects cold start vs incremental, loops pages, calls all modules | ✅ Implemented with backfill passes |
| `db.py` | All Supabase Postgres operations: load existing IDs, upsert movies, write ingestion logs. Handles transient Postgres errors with tenacity | ✅ Consolidated into `get_null_candidates` and `batch_update` |
| `tmdb.py` | Discover movies + fetch detail + fetch videos, with tenacity retry | ✅ |
| `omdb.py` | Fetch IMDb rating by `imdb_id`, with tenacity retry | ✅ |
| `youtube.py` | Generates YouTube search URLs as trailer fallback — no API call, no quota | ✅ Replaced API with search URL generator |
| `embeddings.py` | Batch 20 descriptions → one OpenAI call per page | ✅ |
| `scheduler.py` | APScheduler daemon — Sunday 2 AM AEST, immediate trigger if >6 days since last run (Stage 2 only) | ✅ |
| Dockerfile | `python:3.11-slim`, installs `uv`, no secrets baked in | ✅ |
| docker-compose.yml | `restart: unless-stopped`, mounts `./src`, loads .env | ✅ |
| .env | Real secrets — never committed | ✅ |
| .env.example | Template, all five keys documented consistently, no `(REQUIRED)` markers | ✅ |

---

### Dependencies (pyproject.toml)

```toml
dependencies = [
    "httpx",
    "tenacity",
    "openai",
    "psycopg[binary]",
    "python-dotenv",
    "apscheduler",   # dormant until Stage 2
]
```

Managed via `uv`. No `pip install -r requirements.txt`. Docker installs via `uv sync --frozen`.

---

### Supabase SQL — Run Once Before Stage 1

```sql
create extension if not exists vector;

create table public.movies (
  id           uuid primary key default gen_random_uuid(),
  tmdb_id      integer unique not null,
  title        text not null,
  description  text,
  embedding    vector(1536),
  poster_url   text,
  trailer_url  text,
  genre        text[],
  runtime      integer,
  rating       numeric(3,1),
  release_year integer,
  added_at     timestamptz default now()
);

create table public.ingestion_logs (
  id           uuid primary key default gen_random_uuid(),
  run_at       timestamptz default now(),
  status       text not null,
  movies_added integer,
  error        text
);
```

Column decisions:
- `genre` → `text[]` — TMDB returns multiple genres per movie
- `rating` → `numeric(3,1)` — e.g. `7.4`
- `runtime` → `integer` — minutes
- `embedding` → `vector(1536)` — requires `pgvector` extension, **never overwritten** on re-runs

---

### Endpoints

**TMDB** (base URL: `https://api.themoviedb.org/3`)
```
GET /discover/movie
  ?with_watch_providers=8
  &watch_region=AU
  &sort_by=primary_release_date.desc
  &primary_release_date.gte=2020-01-01
  &page={n}

GET /movie/{id}
  → imdb_id, runtime, genres[], poster_path, overview, release_date
```

**OMDB**
```
GET http://www.omdbapi.com/?i={imdb_id}&apikey={key}
  → imdbRating
```

**YouTube** (trailer fallback — no API call)
```
youtube.get_search_url(title, year)
  → https://www.youtube.com/results?search_query={title}+{year}+official+trailer
```

**OpenAI**
```
POST https://api.openai.com/v1/embeddings
  model: text-embedding-3-small
  input: [description_1, description_2, ... description_20]  ← 1 call per page
```

**Supabase (direct Postgres via psycopg)**
```sql
SELECT COUNT(*) FROM public.movies                    -- cold start detection
SELECT MAX(added_at) FROM public.movies               -- incremental window
SELECT tmdb_id FROM public.movies                     -- load existing IDs into set (once)
SELECT tmdb_id, imdb_id FROM public.movies
  WHERE rating IS NULL AND imdb_id IS NOT NULL         -- backfill candidates
UPDATE public.movies SET rating = $1 WHERE tmdb_id = $2  -- backfill update
INSERT INTO public.movies ... ON CONFLICT (tmdb_id)
  DO UPDATE SET ... (embedding excluded if already set)
INSERT INTO public.ingestion_logs ...
```

---

### Run Mode Detection (main.py)

- **Cold start**: `COUNT(*) = 0` → discover from `primary_release_date.gte=2020-01-01`
- **Incremental**: `COUNT(*) > 0` → discover from `MAX(added_at) - 8 days` (8-day buffer to catch late-indexed titles)

---

### Null Rating Backfill Pass (main.py — runs before page loop)

At the start of every run, before the main page loop:

1. Query `SELECT tmdb_id, imdb_id FROM public.movies WHERE rating IS NULL AND imdb_id IS NOT NULL`
2. `asyncio.gather(*[omdb.get_rating(row.imdb_id) for row in candidates])` — all concurrent
3. For each result that returns a rating: `UPDATE public.movies SET rating = $1 WHERE tmdb_id = $2`
4. If OMDB still returns NULL, skip — the row remains a candidate and will be retried next week

Uses the same tenacity retry strategy as the main enrichment path. Rows with no `imdb_id` are excluded (nothing to query against).

---

### Concurrency Model (asyncio)

All 20 movies on a page are enriched concurrently:

```python
await asyncio.gather(*[enrich(movie) for movie in new_movies])
```

Per movie, `enrich()` runs in two steps:

```
Step 1: detail = await tmdb.get_detail(id)
Step 2: rating = await omdb.get_rating(detail.imdb_id)   # needs imdb_id from step 1
        trailer = youtube.get_search_url(title, year)     # sync, no API call
```

All 20 movies still run concurrently against each other.

---

### Retry Strategy (tenacity)

All external HTTP calls:
- 3 retries, exponential backoff (1s → 2s → 4s)
- Retry on: `httpx.HTTPStatusError` with 429 or 5xx status

Postgres (`db.py`):
- 3 retries, exponential backoff
- Retry on: `psycopg.OperationalError` (transient connection drops)
- Applies to: reads, upserts, and log writes

---

### Per-Page Data Flow

```
0. [before page loop] backfill passes:
     ratings:   query WHERE rating IS NULL AND imdb_id IS NOT NULL
                asyncio.gather(*[omdb.get_rating(imdb_id) for each], semaphore=10)
                batch UPDATE rating
     embeddings: query WHERE embedding IS NULL
                batch_embed(descriptions, chunk=20)
                batch UPDATE embedding
     trailers:  query WHERE trailer_url IS NULL
                generate youtube.get_search_url(title, year) for each — no API
                batch UPDATE trailer_url
1. discover page N (20 movies)
2. filter against known tmdb_id set → new_movies
3. asyncio.gather(*[enrich(m) for m in new_movies])
     per movie:
       tmdb_detail(id)
       → omdb_rating(imdb_id)
       → youtube.get_search_url(title, year)  # sync, always returns a URL
4. batch embed all descriptions → 1 OpenAI call
5. upsert all enriched movies to public.movies
6. sleep 1s
7. after all pages: write ingestion_log (status, movies_added)
```

---

### .env.example Style Rules

All five keys follow the same comment format — no `(REQUIRED)`, `(OPTIONAL)`, or any other qualifiers:

```env
# TMDB API Key (from https://www.themoviedb.org/settings/api)
TMDB_API_KEY=your_tmdb_api_key_here

# OMDB API Key (from http://www.omdbapi.com/apikey.aspx)
OMDB_API_KEY=your_omdb_api_key_here

# OpenAI API Key (from https://platform.openai.com/api-keys)
OPENAI_API_KEY=your_openai_api_key_here

# Supabase PostgreSQL Connection String
# Format: postgresql://user:password@host:5432/database
SUPABASE_DB_URL=postgresql://user:password@your-project.supabase.co:5432/postgres

# YouTube API Key (from https://console.cloud.google.com/)
YOUTUBE_API_KEY=your_youtube_api_key_here
```

---

### Docker (Dockerfile)

```dockerfile
FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock* ./
COPY src/ ./src/
RUN uv sync --frozen --no-dev
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
CMD ["uv", "run", "main.py"]
```

### Docker Compose (docker-compose.yml)

```yaml
services:
  ingestion:
    build: .
    restart: unless-stopped
    env_file: .env
    volumes:
      - ./src:/app/src
```

---

### Stage 1 — Verification Checklist (before Stage 2)

1. `public.movies` has ~20 rows
2. `embedding` column populated (not NULL)
3. `rating` populated on movies that have an `imdb_id`
4. At least some `trailer_url` values populated
5. `public.ingestion_logs` has one `success` row
6. Re-run produces 0 new inserts (dedup working)

---

### Stage 2 (after Stage 1 passes)

- Enable `scheduler.py` with APScheduler
- Sunday 2 AM AEST cron
- Immediate trigger if `MAX(run_at)` in `ingestion_logs` is >6 days ago
- Set `MAX_PAGES = 500`
- `docker compose up -d`

---

**Ready to proceed?** Run the SQL above in Supabase and confirm — then I'll build all the files.