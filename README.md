# FastAPI Backend Boilerplate

This repository contains a production-oriented FastAPI boilerplate with:

- clean app startup/shutdown lifecycle (`lifespan`)
- centralized typed configuration via Pydantic settings
- structured request logging middleware
- singleton clients for MongoDB, Redis, and outbound HTTP
- health endpoints (`/livez`, `/readyz`)
- sample feature router (`/items`)

## Project Layout

```text
Backend/
  app_logging.py     # logger config + HTTP logging middleware
  config.py          # Pydantic settings (App, Mongo, Redis, Http)
  http_client.py     # async aiohttp client singleton + helpers
  mongo_client.py    # async PyMongo singleton + index management
  redis_client.py    # async Redis singleton + cache + rate limiter
  items.py           # sample APIRouter with in-memory data
  main.py            # FastAPI app entrypoint, lifespan wiring
  format.sh          # autoflake + isort + black formatter script
.env.example         # environment variable template
```

## Core Architecture

### 1. App lifecycle

`Backend/main.py` uses `@asynccontextmanager` for `lifespan`.

On startup:

- initializes `MongoDB()` singleton
- initializes `Redis()` singleton
- starts global `http` client session
- performs readiness pings for Mongo and Redis

On shutdown:

- closes HTTP, Redis, and Mongo in parallel
- logs teardown errors without crashing shutdown

### 2. Logging

`Backend/app_logging.py` provides:

- millisecond timestamp formatter
- centralized logger setup
- uvicorn default handler override
- request/response middleware with:
  - request ID (`X-Request-ID`)
  - response time (`X-Response-Time`)
  - method/path/status/timing logs

### 3. Configuration

`Backend/config.py` uses Pydantic settings classes:

- `App` (APP_NAME, ENV)
- `Mongo` (MONGO_* variables)
- `Redis` (REDIS_* variables)
- `Http` (HTTP_* variables)

All classes load from:

1. project `.env`
2. `Backend/.env`
3. process environment variables

The global `settings` object exposes:

- `settings.app`
- `settings.mongo`
- `settings.redis`
- `settings.http`

### 4. Data and infrastructure clients

#### MongoDB client (`mongo_client.py`)

- singleton `AsyncMongoClient`
- typed `IndexSpec` for startup index creation
- `connect()`, `ping()`, `close()`
- `collection()` helper
- `ensure_indexes()` with grouped batch creation and conflict-safe logging
- runnable `__main__` example flow with multiple CRUD/index/aggregate demos

#### Redis client (`redis_client.py`)

- singleton async `redis-py` client + pooled connections
- namespaced key builder (`APP_NAME:ENV:*`)
- resilient retry/backoff setup
- helpers:
  - `cache` (JSON set/get/invalidate/prefix invalidation)
  - `rate_limiter` (Lua-based atomic fixed-window limiter)
- runnable `__main__` example flow with strings/counters/hashes/lists/sets/zsets/pipeline/cache/rate-limit demos

#### HTTP client (`http_client.py`)

- shared `aiohttp.ClientSession` singleton
- unified request method for GET/POST/PUT/PATCH/DELETE
- retry with exponential backoff
- per-request timeout overrides
- response helpers (`json()`, `text()`, `content()`, `raise_for_status()`)
- SSE streaming consumer with reconnect support
- multipart upload helper
- runnable `__main__` example flow with many outbound request patterns

## API Endpoints

Current endpoints:

- `GET /livez` → liveness check
- `GET /readyz` → checks Mongo + Redis connectivity
- `GET /items/` → list mock items
- `POST /items/` → create item in in-memory list

## Environment Setup

Use `.env.example` as the template:

```bash
cp .env.example .env
```

Then set values for:

- Mongo: `MONGO_URI`, `MONGO_DB`, TLS/timeouts/pool if needed
- Redis: `REDIS_URI`, connection/retry settings
- HTTP defaults: `HTTP_BASE_URL`, timeout/retry options
- App metadata: `APP_NAME`, `ENV`

## Quick Start

1. Install dependencies

```bash
python -m pip install fastapi uvicorn aiohttp pymongo redis pydantic pydantic-settings
```

2. Configure environment

```bash
cp .env.example .env
```

3. Run API (from `Backend/` because imports are module-local)

```bash
cd Backend
uvicorn main:app --reload
```

4. Open docs

- Swagger UI: `http://127.0.0.1:8000/docs`
- ReDoc: `http://127.0.0.1:8000/redoc`

## Notes

- `items.py` is intentionally simple and in-memory; swap with real DB-backed modules.
- Lifespan wiring is ready for additional shared resources (queues, schedulers, telemetry).
- Singleton modules also support direct script execution for local integration tests/examples.
