import logging
import time
import uuid

from fastapi import FastAPI, Request, Response


class _MSFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        from datetime import datetime

        return datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S.%f")[
            :-3
        ]


def _configure_logging() -> logging.Logger:
    handler = logging.StreamHandler()
    handler.setFormatter(
        _MSFormatter("%(asctime)s  %(levelname)-8s  %(name)s  %(message)s")
    )
    logging.basicConfig(level=logging.INFO, handlers=[handler])

    logging.getLogger("uvicorn").handlers = []
    logging.getLogger("uvicorn.access").handlers = []
    logging.getLogger("uvicorn.error").handlers = []

    return logging.getLogger("app")


logger = _configure_logging()


def add_logging_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def logging_middleware(request: Request, call_next) -> Response:
        request_id = str(uuid.uuid4())[:8]
        logger.info(
            "→ %s %s  [req_id=%s]",
            request.method,
            request.url.path,
            request_id,
        )
        start = time.perf_counter()
        response: Response = await call_next(request)
        elapsed = (time.perf_counter() - start) * 1000
        logger.info(
            "← %s %s  status=%d  %.1fms  [req_id=%s]",
            request.method,
            request.url.path,
            response.status_code,
            elapsed,
            request_id,
        )
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Response-Time"] = f"{elapsed:.1f}ms"
        return response
