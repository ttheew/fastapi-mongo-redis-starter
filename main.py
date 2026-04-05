import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

import items
from app_logging import add_logging_middleware, logger
from http_client import http
from mongo_client import MongoDB
from redis_client import Redis


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀  Application starting up")
    mongo = MongoDB()
    redis = Redis()

    await mongo.connect()
    await redis.connect()
    await http.start()

    mongo_ok, redis_ok = await asyncio.gather(mongo.ping(), redis.ping())
    logger.info(
        "Singletons initialized  mongo=%s  redis=%s  http=%s",
        mongo_ok,
        redis_ok,
        "ready",
    )

    try:
        yield
    finally:
        close_results = await asyncio.gather(
            http.stop(),
            redis.close(),
            mongo.close(),
            return_exceptions=True,
        )
        for idx, result in enumerate(close_results):
            if isinstance(result, Exception):
                logger.error("Shutdown task %d failed: %s", idx, result)
        logger.info("🛑  Application shutting down")


app = FastAPI(
    title="Demo API",
    version="1.0.0",
    description="Simple FastAPI app — latest patterns (0.111+)",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

add_logging_middleware(app)


@app.get("/livez")
async def liveness():
    return {"status": "ok"}


@app.get("/readyz")
async def readiness():
    mongo_ok = await MongoDB().ping()
    redis_ok = await Redis().ping()

    if not mongo_ok or not redis_ok:
        raise HTTPException(
            status_code=503,
            detail={
                "mongo": mongo_ok,
                "redis": redis_ok,
            },
        )

    return {"mongo": True, "redis": True}


app.include_router(items.router)
