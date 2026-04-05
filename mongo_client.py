"""
db.py — MongoDB singleton client (PyMongo 4.10+ AsyncMongoClient).

The class uses __new__ to enforce a single instance per process.
Every call to MongoDB() returns the same object — one client, one pool.

Usage:
    from db import MongoDB, IndexSpec

    # lifespan startup
    await MongoDB().connect()
    await MongoDB().ensure_indexes([
        IndexSpec("users", [("email", 1)], unique=True, name="email_unique"),
        IndexSpec("users", [("username", 1)], unique=True, name="username_unique"),
        IndexSpec("users", [("expires_at", 1)], expire_after_seconds=0, name="user_ttl"),
        IndexSpec("posts", [("author_id", 1), ("created_at", -1)], name="author_feed"),
    ])
    await MongoDB().ping()

    # lifespan shutdown
    await MongoDB().close()

    # anywhere in the app
    users = MongoDB().collection("users")
    doc   = await users.find_one({"email": "a@b.com"})
    db    = MongoDB().db          # raw AsyncDatabase
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from pymongo import AsyncMongoClient, IndexModel
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import (
    ConfigurationError,
    ConnectionFailure,
    OperationFailure,
    ServerSelectionTimeoutError,
)
from pymongo.server_api import ServerApi

try:
    from Backend.config import settings
except ImportError:  # pragma: no cover - script execution fallback
    from config import settings

logger = logging.getLogger(__name__)


@dataclass
class IndexSpec:
    """
    Describe a MongoDB index to be created on startup.

    Args:
        collection:           Collection name the index belongs to.
        keys:                 List of (field, direction) tuples.
                            Direction: 1 = ASC, -1 = DESC, "text" = TEXT,
                            "2dsphere" = GEO.
        name:                 Explicit index name (recommended for predictable drops).
        unique:               Enforce uniqueness constraint (default False).
        sparse:               Only index docs that contain the field (default False).
        expire_after_seconds: TTL index — MongoDB auto-deletes docs after N seconds.
                            Set to 0 when expires_at field holds the exact datetime.
        partial_filter:       Partial filter expression, e.g. {"active": True}.
        database:             Override the default database for this index.

    Examples:
        # Simple unique index
        IndexSpec("users", [("email", 1)], unique=True, name="email_unique")

        # Compound index — user feed sorted by time
        IndexSpec("posts", [("author_id", 1), ("created_at", -1)], name="author_feed")

        # TTL — auto-delete expired sessions
        IndexSpec("sessions", [("expires_at", 1)], expire_after_seconds=0, name="session_ttl")

        # Partial — only index active users (smaller, faster)
        IndexSpec("users", [("email", 1)],
                    partial_filter={"active": True}, name="email_active")

        # Full-text search
        IndexSpec("articles", [("title", "text"), ("body", "text")], name="article_fts")

        # Geospatial
        IndexSpec("places", [("location", "2dsphere")], name="place_geo")

        # Cross-database index
        IndexSpec("orders", [("status", 1)], database="billing", name="order_status")
    """

    collection: str
    keys: list[tuple[str, Any]]
    name: str | None = None
    unique: bool = False
    sparse: bool = False
    expire_after_seconds: int | None = None
    partial_filter: dict[str, Any] | None = None
    database: str | None = None

    def to_index_model(self) -> IndexModel:
        kwargs: dict[str, Any] = {}
        if self.name:
            kwargs["name"] = self.name
        if self.unique:
            kwargs["unique"] = True
        if self.sparse:
            kwargs["sparse"] = True
        if self.expire_after_seconds is not None:
            kwargs["expireAfterSeconds"] = self.expire_after_seconds
        if self.partial_filter:
            kwargs["partialFilterExpression"] = self.partial_filter
        return IndexModel(self.keys, **kwargs)


class MongoDB:
    """
    Async MongoDB client enforced as a singleton via __new__.

    One instance is created on the first MongoDB() call. Every subsequent
    call returns the same object — one AsyncMongoClient, one connection pool,
    shared across the whole process.

    Lifecycle (wire into FastAPI lifespan):
        await MongoDB().connect()
        await MongoDB().ensure_indexes([...])
        await MongoDB().ping()
        ...
        await MongoDB().close()
    """

    _instance: "MongoDB | None" = None
    _client: AsyncMongoClient | None = None
    _db: AsyncDatabase | None = None

    def __new__(cls) -> "MongoDB":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            logger.debug("MongoDB singleton instantiated")
        return cls._instance

    async def connect(self) -> None:
        """
        Build the AsyncMongoClient and store it on the singleton.

        Safe to call multiple times — a no-op if already connected.
        Runs the client construction in a thread to avoid blocking the event
        loop during DNS resolution or TLS handshake at startup.

        Note: AsyncMongoClient connects lazily on the first real operation;
        call ping() after connect() to verify the server is reachable.

        Reads configuration from settings (env vars and/or .env).
        """
        if self._client is not None:
            return

        uri: str = settings.mongo.uri
        db_name: str = settings.mongo.db
        use_tls: bool = settings.mongo.tls
        tls_ca: str | None = settings.mongo.tls_ca_file

        kwargs: dict[str, Any] = {
            "server_api": ServerApi("1"),  # stable API — survives server upgrades
            # Timeouts (ms)
            "connectTimeoutMS": settings.mongo.connect_timeout_ms,
            "socketTimeoutMS": settings.mongo.socket_timeout_ms,
            "serverSelectionTimeoutMS": settings.mongo.server_selection_timeout_ms,
            # Connection pool
            "maxPoolSize": settings.mongo.max_pool_size,
            "minPoolSize": settings.mongo.min_pool_size,
            "maxIdleTimeMS": settings.mongo.max_idle_time_ms,
            # Reliability
            "retryWrites": settings.mongo.retry_writes,
            "retryReads": settings.mongo.retry_reads,
            # Write safety — survives primary failover
            "w": settings.mongo.write_concern_w,
            "journal": settings.mongo.journal,
            "wtimeoutMS": settings.mongo.wtimeout_ms,
        }

        if use_tls:
            kwargs["tls"] = True
            if tls_ca:
                kwargs["tlsCAFile"] = tls_ca

        try:
            self._client = AsyncMongoClient(uri, **kwargs)
            self._db = self._client[db_name]
            logger.info("MongoDB connected  uri=%s  db=%s", uri, db_name)
        except ConfigurationError as exc:
            logger.critical("MongoDB configuration error: %s", exc)
            raise

    async def ping(self) -> bool:
        """
        Lightweight server ping. Returns True/False, never raises.

        Raises RuntimeError if connect() has not been called yet.
        """
        if self._client is None:
            raise RuntimeError("Call await MongoDB().connect() before calling ping()")
        try:
            await self.client.admin.command("ping")
            logger.info("MongoDB ✓ ping OK")
            return True
        except (
            ConnectionFailure,
            ServerSelectionTimeoutError,
            OperationFailure,
        ) as exc:
            logger.error("MongoDB ✗ ping FAILED: %s", exc)
            return False

    async def close(self) -> None:
        """
        Close the connection pool and reset singleton state.
        Safe to call even if connect() was never called.
        """
        if self._client is not None:
            await self._client.close()
            self._client = None
            self._db = None
            logger.info("MongoDB client closed")

    @property
    def db(self) -> AsyncDatabase:
        """Default AsyncDatabase. Raises RuntimeError if not connected."""
        if self._db is None:
            raise RuntimeError("Call await MongoDB().connect() before accessing .db")
        return self._db

    @property
    def client(self) -> AsyncMongoClient:
        """Raw AsyncMongoClient. Raises RuntimeError if not connected."""
        if self._client is None:
            raise RuntimeError(
                "Call await MongoDB().connect() before accessing .client"
            )
        return self._client

    def collection(self, name: str, database: str | None = None) -> AsyncCollection:
        """
        Return an async collection from the default (or named) database.

        Args:
            name:     Collection name.
            database: Override the default database.

        Examples:
            users  = MongoDB().collection("users")
            orders = MongoDB().collection("orders", database="billing")
        """
        if self._client is None:
            raise RuntimeError("Call await MongoDB().connect() first")
        target = self._client[database] if database else self.db
        return target[name]

    async def ensure_indexes(self, specs: list[IndexSpec]) -> None:
        """
        Create indexes from a list of IndexSpec objects.

        Idempotent — MongoDB silently skips indexes that already exist
        with the same name. Safe to call on every application startup.

        Specs are grouped by (database, collection) and issued as a
        single create_indexes call per collection for efficiency.

        If an index definition conflicts with an existing one (same name,
        different keys or options), an OperationFailure is raised with a
        clear log message. Drop the old index manually before redeploying.

        Args:
            specs: List of IndexSpec instances.

        Example:
            await MongoDB().ensure_indexes([
                IndexSpec("users", [("email", 1)],
                        unique=True, name="email_unique"),
                IndexSpec("users", [("username", 1)],
                        unique=True, name="username_unique"),
                IndexSpec("users", [("expires_at", 1)],
                        expire_after_seconds=0, sparse=True, name="user_ttl"),
                IndexSpec("posts", [("author_id", 1), ("created_at", -1)],
                        name="author_feed"),
                IndexSpec("posts", [("title", "text"), ("body", "text")],
                        name="post_fts"),
                IndexSpec("places", [("location", "2dsphere")], name="place_geo"),
            ])
        """
        if not specs:
            logger.debug("ensure_indexes called with empty spec list — skipping")
            return

        # Group by (database override, collection) → batch per collection
        groups: dict[tuple[str | None, str], list[IndexSpec]] = defaultdict(list)
        for spec in specs:
            groups[(spec.database, spec.collection)].append(spec)

        for (database, collection_name), group in groups.items():
            col = self.collection(collection_name, database=database)
            models = [s.to_index_model() for s in group]
            index_names = [s.name or "unnamed" for s in group]
            try:
                await col.create_indexes(models)
                logger.info(
                    "MongoDB indexes ensured  collection=%s  indexes=%s",
                    collection_name,
                    index_names,
                )
            except OperationFailure as exc:
                logger.critical(
                    "MongoDB index creation failed  collection=%s  indexes=%s  "
                    "error=%s  — drop the conflicting index and redeploy.",
                    collection_name,
                    index_names,
                    exc,
                )
                raise

    def __repr__(self) -> str:
        db_name = self._db.name if self._db else "—"
        return f"<MongoDB connected={self._client is not None} db={db_name}>"


async def _run_mongo_examples() -> None:
    mongo = MongoDB()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    logger.info("Mongo examples starting run_id=%s", run_id)

    try:
        await mongo.connect()
        if not await mongo.ping():
            logger.error("MongoDB is unreachable; skipping examples.")
            return

        await mongo.ensure_indexes(
            [
                IndexSpec(
                    "users_demo",
                    [("email", 1)],
                    unique=True,
                    name="users_demo_email_unique",
                ),
                IndexSpec(
                    "orders_demo",
                    [("user_id", 1), ("created_at", -1)],
                    name="orders_demo_user_created_at",
                ),
                IndexSpec(
                    "sessions_demo",
                    [("expires_at", 1)],
                    expire_after_seconds=0,
                    name="sessions_demo_ttl",
                ),
            ]
        )

        users = mongo.collection("users_demo")
        orders = mongo.collection("orders_demo")
        sessions = mongo.collection("sessions_demo")

        user_email = f"demo_{run_id}@example.com"
        create_user = await users.insert_one(
            {
                "name": "Example User",
                "email": user_email,
                "login_count": 0,
                "demo_run": run_id,
                "created_at": datetime.now(timezone.utc),
            }
        )
        user_id = create_user.inserted_id
        logger.info("Example 1: inserted user _id=%s", user_id)

        profile = await users.find_one(
            {"_id": user_id}, {"_id": 1, "email": 1, "name": 1}
        )
        logger.info("Example 2: find_one projection -> %s", profile)

        update_result = await users.update_one(
            {"_id": user_id},
            {"$set": {"name": "Example User Updated"}, "$inc": {"login_count": 1}},
        )
        logger.info(
            "Example 3: update_one matched=%d modified=%d",
            update_result.matched_count,
            update_result.modified_count,
        )

        upsert_result = await users.update_one(
            {"email": f"upsert_{run_id}@example.com"},
            {
                "$setOnInsert": {
                    "name": "Upserted User",
                    "created_at": datetime.now(timezone.utc),
                    "demo_run": run_id,
                }
            },
            upsert=True,
        )
        logger.info("Example 4: upserted_id=%s", upsert_result.upserted_id)

        now = datetime.now(timezone.utc)
        order_docs = [
            {
                "user_id": user_id,
                "amount": 29.99,
                "status": "paid",
                "created_at": now - timedelta(minutes=20),
                "demo_run": run_id,
            },
            {
                "user_id": user_id,
                "amount": 59.50,
                "status": "paid",
                "created_at": now - timedelta(minutes=10),
                "demo_run": run_id,
            },
            {
                "user_id": user_id,
                "amount": 12.00,
                "status": "pending",
                "created_at": now - timedelta(minutes=5),
                "demo_run": run_id,
            },
        ]
        inserted_orders = await orders.insert_many(order_docs)
        logger.info(
            "Example 5: insert_many count=%d", len(inserted_orders.inserted_ids)
        )

        recent_orders = [
            doc
            async for doc in orders.find({"user_id": user_id, "demo_run": run_id})
            .sort("created_at", -1)
            .limit(2)
        ]
        logger.info("Example 6: recent order sample -> %s", recent_orders)

        pipeline = [
            {"$match": {"user_id": user_id, "demo_run": run_id}},
            {
                "$group": {
                    "_id": "$status",
                    "total_amount": {"$sum": "$amount"},
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"total_amount": -1}},
        ]
        totals = [doc async for doc in orders.aggregate(pipeline)]
        logger.info("Example 7: aggregate totals -> %s", totals)

        statuses = await orders.distinct("status", {"demo_run": run_id})
        logger.info("Example 8: distinct statuses -> %s", statuses)

        ttl_result = await sessions.insert_one(
            {
                "session_id": f"session-{run_id}",
                "user_id": user_id,
                "expires_at": datetime.now(timezone.utc) + timedelta(seconds=90),
                "demo_run": run_id,
            }
        )
        logger.info("Example 9: inserted TTL session _id=%s", ttl_result.inserted_id)

        deleted_orders = await orders.delete_many({"demo_run": run_id})
        deleted_users = await users.delete_many({"demo_run": run_id})
        deleted_sessions = await sessions.delete_many({"demo_run": run_id})
        logger.info(
            "Example 10: cleanup users=%d orders=%d sessions=%d",
            deleted_users.deleted_count,
            deleted_orders.deleted_count,
            deleted_sessions.deleted_count,
        )
    finally:
        await mongo.close()
        logger.info("Mongo examples completed.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    asyncio.run(_run_mongo_examples())
