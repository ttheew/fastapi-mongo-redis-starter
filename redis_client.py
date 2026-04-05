"""
redis_client.py — Redis singleton client (redis-py 5+ / redis.asyncio).

The class uses __new__ to enforce a single instance per process.
Every call to Redis() returns the same object — one connection pool,
shared across the whole process.

Usage:
    from redis_client import Redis

    # lifespan startup
    await Redis().connect()
    await Redis().ping()

    # lifespan shutdown
    await Redis().close()

    # anywhere in the app — raw client
    await Redis().client.set(Redis().key("session", uid), data, ex=86400)

    # cache helper
    await Redis().cache.set("user:123", {"name": "Alice"}, ttl=300)
    data = await Redis().cache.get("user:123")

    # rate limiter
    allowed = await Redis().rate_limiter.check("login", ip, limit=5, window=60)
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as aioredis
from redis.asyncio.connection import SSLConnection
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import AuthenticationError, BusyLoadingError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

try:
    from Backend.config import settings
except ImportError:  # pragma: no cover - script execution fallback
    from config import settings

logger = logging.getLogger(__name__)


class Redis:
    """
    Async Redis client enforced as a singleton via __new__.

    One instance is created on the first Redis() call. Every subsequent
    call returns the same object — one ConnectionPool, shared across
    the whole process.

    Exposes:
        Redis().client          → raw aioredis.Redis for any command
        Redis().key(...)        → namespaced key builder
        Redis().cache           → RedisCache helper (JSON get/set/invalidate)
        Redis().rate_limiter    → RateLimiter helper (atomic fixed-window)

    Lifecycle (wire into FastAPI lifespan):
        await Redis().connect()
        await Redis().ping()
        ...
        await Redis().close()

    Note: after close(), call connect() again before reuse. The singleton
    instance is retained but its internal pool is torn down; connect() will
    rebuild it safely.
    """

    _instance: "Redis | None" = None
    _redis: aioredis.Redis | None = None
    _pool: aioredis.ConnectionPool | None = None
    _app_name: str = ""
    _env: str = ""

    _lua_incr_ttl: Any = None
    _lua_get_reset: Any = None

    def __new__(cls) -> "Redis":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            logger.debug("Redis singleton instantiated")
        return cls._instance

    async def connect(self) -> None:
        """
        Build the connection pool and Redis client.

        Safe to call multiple times — a no-op if already connected.

        Reads configuration from settings (env vars and/or .env).
        """
        if self._redis is not None:
            return

        uri: str = settings.redis.uri
        max_conn: int = settings.redis.max_connections
        self._app_name = settings.app.app_name
        self._env = settings.app.env
        use_tls: bool = uri.startswith("rediss://")

        retry = Retry(
            ExponentialBackoff(
                cap=settings.redis.retry_backoff_cap,
                base=settings.redis.retry_backoff_base,
            ),
            retries=settings.redis.retry_attempts,
            supported_errors=(
                RedisConnectionError,
                RedisTimeoutError,
                BusyLoadingError,
            ),
        )

        pool_kwargs: dict[str, Any] = {
            "max_connections": max_conn,
            "decode_responses": True,
            "socket_connect_timeout": settings.redis.socket_connect_timeout,
            "socket_timeout": settings.redis.socket_timeout,
            "socket_keepalive": True,
            "socket_keepalive_options": {},
            "health_check_interval": settings.redis.health_check_interval,
            "retry": retry,
            "retry_on_error": [RedisConnectionError, RedisTimeoutError],
        }

        if use_tls:
            pool_kwargs["connection_class"] = SSLConnection
            pool_kwargs["ssl_cert_reqs"] = "required"
            tls_ca = settings.redis.tls_ca_file
            if tls_ca:
                pool_kwargs["ssl_ca_certs"] = tls_ca

        self._pool = aioredis.ConnectionPool.from_url(uri, **pool_kwargs)
        self._redis = aioredis.Redis(connection_pool=self._pool)

        self._lua_incr_ttl = self._redis.register_script("""
            local current = redis.call('INCR', KEYS[1])
            if current == 1 then
                redis.call('EXPIRE', KEYS[1], ARGV[1])
            end
            return current
        """)

        self._lua_get_reset = self._redis.register_script("""
            local val = redis.call('GET', KEYS[1])
            redis.call('DEL', KEYS[1])
            return val
        """)

        logger.info("Redis connected  uri=%s  pool=%d", uri, max_conn)

    async def ping(self) -> bool:
        """Lightweight ping. Returns True/False, never raises."""
        try:
            result = await self.client.ping()
            logger.info("Redis ✓ ping OK")
            return bool(result)
        except (RedisConnectionError, RedisTimeoutError, AuthenticationError) as exc:
            logger.error("Redis ✗ ping FAILED: %s", exc)
            return False

    async def close(self) -> None:
        """
        Drain the connection pool and reset connection state.

        The singleton instance itself is retained — call connect() again
        before reuse. Safe to call even if connect() was never called.
        """
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None
            self._lua_incr_ttl = None
            self._lua_get_reset = None
        if self._pool is not None:
            await self._pool.aclose()
            self._pool = None
        logger.info("Redis client closed")

    @property
    def client(self) -> aioredis.Redis:
        """Raw aioredis.Redis instance. Raises if not connected."""
        if self._redis is None:
            raise RuntimeError("Call await Redis().connect() before using .client")
        return self._redis

    def key(self, *parts: str) -> str:
        """
        Build a namespaced Redis key: {APP_NAME}:{ENV}:{parts...}

        Prevents key collisions across apps and environments sharing
        the same Redis instance and enables ACL scoping.

        Examples:
            Redis().key("session", user_id)       → "app:dev:session:abc123"
            Redis().key("ratelimit", "login", ip) → "app:dev:ratelimit:login:1.2.3.4"
        """
        return f"{self._app_name}:{self._env}:" + ":".join(parts)

    @property
    def cache(self) -> "_RedisCache":
        return _RedisCache(self)

    @property
    def rate_limiter(self) -> "_RateLimiter":
        return _RateLimiter(self)

    def __repr__(self) -> str:
        return f"<Redis connected={self._redis is not None}>"


class _RedisCache:
    """
    Lightweight typed JSON cache.

    All methods silently absorb Redis errors and return safe fallback
    values — a cache failure must never crash the main request path.

    Usage:
        await Redis().cache.set("user:123", {"name": "Alice"}, ttl=300)
        data  = await Redis().cache.get("user:123")      # dict | None
        await Redis().cache.invalidate("user:123")
        n     = await Redis().cache.invalidate_prefix("user:")
    """

    def __init__(self, redis: Redis) -> None:
        self._r = redis

    def _k(self, cache_key: str) -> str:
        return self._r.key("cache", cache_key)

    async def get(self, cache_key: str) -> Any | None:
        try:
            raw = await self._r.client.get(self._k(cache_key))
            return json.loads(raw) if raw is not None else None
        except (RedisConnectionError, RedisTimeoutError, ResponseError) as exc:
            logger.warning("cache.get failed [%s]: %s", cache_key, exc)
            return None

    async def set(self, cache_key: str, value: Any, ttl: int = 300) -> bool:
        try:
            await self._r.client.setex(
                self._k(cache_key), ttl, json.dumps(value, default=str)
            )
            return True
        except (RedisConnectionError, RedisTimeoutError, ResponseError) as exc:
            logger.warning("cache.set failed [%s]: %s", cache_key, exc)
            return False

    async def invalidate(self, cache_key: str) -> bool:
        try:
            await self._r.client.delete(self._k(cache_key))
            return True
        except (RedisConnectionError, RedisTimeoutError) as exc:
            logger.warning("cache.invalidate failed [%s]: %s", cache_key, exc)
            return False

    async def invalidate_prefix(self, prefix: str) -> int:
        """
        Delete all cache keys matching a prefix using SCAN (safe for production).
        Deletions are batched in a pipeline to minimise round trips.
        Returns the count of deleted keys.
        """
        pattern = self._k(prefix) + "*"
        deleted = 0
        try:
            keys_batch: list[str] = []
            async for batch_key in self._r.client.scan_iter(pattern, count=100):
                keys_batch.append(batch_key)
                if len(keys_batch) >= 100:
                    async with self._r.client.pipeline(transaction=False) as pipe:
                        for k in keys_batch:
                            pipe.delete(k)
                        await pipe.execute()
                    deleted += len(keys_batch)
                    keys_batch = []
            if keys_batch:
                async with self._r.client.pipeline(transaction=False) as pipe:
                    for k in keys_batch:
                        pipe.delete(k)
                    await pipe.execute()
                deleted += len(keys_batch)
        except (RedisConnectionError, RedisTimeoutError) as exc:
            logger.warning("cache.invalidate_prefix failed [%s]: %s", prefix, exc)
        return deleted


class _RateLimiter:
    """
    Atomic fixed-window rate limiter backed by a Lua script.

    The counter key is set to expire after `window` seconds on the first
    increment, so the window is naturally rolling per key creation time.
    All increments within a window are a single round trip (no race condition).

    Usage:
        limiter = Redis().rate_limiter
        allowed = await limiter.check("login", ip, limit=5, window=60)
        if not allowed:
            raise TooManyRequestsError()

        # Get current count without incrementing
        count = await limiter.get_count("login", ip)
    """

    def __init__(self, redis: Redis) -> None:
        self._r = redis

    def _k(self, action: str, identifier: str) -> str:
        return self._r.key("ratelimit", action, identifier)

    async def check(
        self, action: str, identifier: str, *, limit: int, window: int
    ) -> bool:
        """
        Increment the counter and return True if the request is within the limit.

        Args:
            action:     Logical action name, e.g. "login", "send_email".
            identifier: Per-actor key, e.g. IP address or user ID.
            limit:      Maximum allowed requests per window.
            window:     Window duration in seconds.

        Returns:
            True  → request is allowed (count ≤ limit).
            False → rate limit exceeded.
        """
        if self._r._lua_incr_ttl is None:
            raise RuntimeError("Call await Redis().connect() before using rate_limiter")
        try:
            count = await self._r._lua_incr_ttl(
                keys=[self._k(action, identifier)],
                args=[str(window)],
            )
            return int(count) <= limit
        except (RedisConnectionError, RedisTimeoutError) as exc:
            logger.error(
                "rate_limiter.check failed [%s/%s]: %s — failing open",
                action,
                identifier,
                exc,
            )
            return True

    async def get_count(self, action: str, identifier: str) -> int:
        """
        Return the current request count for an actor without incrementing.
        Returns 0 if no counter exists (window has expired or never started).
        """
        try:
            raw = await self._r.client.get(self._k(action, identifier))
            return int(raw) if raw is not None else 0
        except (RedisConnectionError, RedisTimeoutError) as exc:
            logger.warning(
                "rate_limiter.get_count failed [%s/%s]: %s", action, identifier, exc
            )
            return 0


async def _run_redis_examples() -> None:
    redis_client = Redis()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    logger.info("Redis examples starting run_id=%s", run_id)

    try:
        await redis_client.connect()
        if not await redis_client.ping():
            logger.error("Redis is unreachable; skipping examples.")
            return

        key_prefix = redis_client.key("demo", run_id)

        k_profile = f"{key_prefix}:profile"
        await redis_client.client.set(k_profile, "alice", ex=120)
        profile = await redis_client.client.get(k_profile)
        ttl_profile = await redis_client.client.ttl(k_profile)
        logger.info("Example 1: profile=%s ttl=%s", profile, ttl_profile)

        k_counter = f"{key_prefix}:counter"
        await redis_client.client.set(k_counter, 0)
        await redis_client.client.incrby(k_counter, 5)
        await redis_client.client.incr(k_counter)
        counter_value = await redis_client.client.get(k_counter)
        logger.info("Example 2: counter=%s", counter_value)

        k_user_hash = f"{key_prefix}:user_hash"
        await redis_client.client.hset(
            k_user_hash,
            mapping={"name": "Alice", "email": "alice@example.com", "plan": "pro"},
        )
        user_hash = await redis_client.client.hgetall(k_user_hash)
        logger.info("Example 3: hash=%s", user_hash)

        k_jobs = f"{key_prefix}:jobs"
        await redis_client.client.rpush(k_jobs, "job-1", "job-2", "job-3")
        queued_jobs = await redis_client.client.lrange(k_jobs, 0, -1)
        next_job = await redis_client.client.lpop(k_jobs)
        logger.info("Example 4: queue=%s next=%s", queued_jobs, next_job)

        k_tags = f"{key_prefix}:tags"
        await redis_client.client.sadd(k_tags, "python", "fastapi", "redis")
        has_redis = await redis_client.client.sismember(k_tags, "redis")
        all_tags = sorted(await redis_client.client.smembers(k_tags))
        logger.info("Example 5: has_redis=%s tags=%s", has_redis, all_tags)

        k_scores = f"{key_prefix}:scores"
        await redis_client.client.zadd(
            k_scores,
            mapping={"alice": 120.5, "bob": 99.0, "carol": 154.25},
        )
        leaderboard = await redis_client.client.zrevrange(
            k_scores,
            0,
            -1,
            withscores=True,
        )
        logger.info("Example 6: leaderboard=%s", leaderboard)

        k_metrics = f"{key_prefix}:metrics"
        async with redis_client.client.pipeline(transaction=True) as pipe:
            pipe.hset(k_metrics, mapping={"views": 1, "clicks": 0})
            pipe.hincrby(k_metrics, "views", 9)
            pipe.hincrby(k_metrics, "clicks", 2)
            pipe.hgetall(k_metrics)
            pipeline_result = await pipe.execute()
        logger.info("Example 7: pipeline result=%s", pipeline_result)

        scanned = sorted(
            [
                key
                async for key in redis_client.client.scan_iter(
                    f"{key_prefix}:*", count=100
                )
            ]
        )
        logger.info("Example 8: scanned keys count=%d", len(scanned))

        cache_key = f"profile:{run_id}"
        await redis_client.cache.set(
            cache_key,
            {"user_id": 42, "name": "Alice", "features": ["a", "b", "c"]},
            ttl=180,
        )
        cached_profile = await redis_client.cache.get(cache_key)
        logger.info("Example 9: cached_profile=%s", cached_profile)

        await redis_client.cache.set(f"batch:{run_id}:1", {"ok": True}, ttl=60)
        await redis_client.cache.set(f"batch:{run_id}:2", {"ok": True}, ttl=60)
        removed_one = await redis_client.cache.invalidate(cache_key)
        removed_prefix = await redis_client.cache.invalidate_prefix(f"batch:{run_id}:")
        logger.info(
            "Example 10: cache invalidation single=%s prefix_deleted=%d",
            removed_one,
            removed_prefix,
        )

        ip = "192.0.2.1"
        for i in range(7):
            allowed = await redis_client.rate_limiter.check(
                "login", ip, limit=5, window=60
            )
            logger.info(
                "Example 11: rate_limiter attempt=%d allowed=%s", i + 1, allowed
            )
        current = await redis_client.rate_limiter.get_count("login", ip)
        logger.info("Example 11: rate_limiter current_count=%d", current)

        cleanup_count = 0
        async for key in redis_client.client.scan_iter(f"{key_prefix}:*", count=100):
            await redis_client.client.delete(key)
            cleanup_count += 1
        logger.info("Cleanup: removed %d raw demo keys", cleanup_count)
    finally:
        await redis_client.close()
        logger.info("Redis examples completed.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    asyncio.run(_run_redis_examples())
