"""
http_client.py — High-performance async HTTP client built on aiohttp.

Features:
    • Single shared ClientSession per process (connection pooling)
    • Single .request() method for all verbs: GET POST PUT PATCH DELETE
    • Custom headers, query params, JSON / form / raw body
    • Automatic retry with exponential backoff
    • Timeout control (connect + read separately)
    • SSE (Server-Sent Events) streaming consumer
    • Response JSON / text / bytes helpers
    • Context manager and standalone usage

Usage:
    from http_client import http

    # All verbs through one method
    resp = await http.request("GET",    "/users", params={"page": 2})
    resp = await http.request("POST",   "/users", json={"name": "Alice"})
    resp = await http.request("PUT",    "/users/1", json={"name": "Bob"})
    resp = await http.request("PATCH",  "/users/1", json={"active": False})
    resp = await http.request("DELETE", "/users/1")

    # SSE streaming
    async for event in http.sse("https://api.example.com/stream"):
        print(event.data)

    # One-off with context manager
    async with HttpClient(base_url="https://api.example.com") as client:
        result = await client.request("GET", "/ping")

    # Global singleton — call start() in your lifespan, stop() on shutdown
    await http.start()
    ...
    await http.stop()
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Any, Literal

import aiohttp
from aiohttp import (
    ClientConnectionError,
    ClientResponseError,
    ClientSession,
    ClientTimeout,
    ServerTimeoutError,
    TCPConnector,
)

try:
    from Backend.config import settings
except ImportError:  # pragma: no cover - script execution fallback
    from config import settings

logger = logging.getLogger(__name__)


@dataclass
class SSEEvent:
    """Parsed Server-Sent Event."""

    data: str = ""
    event: str = "message"
    id: str | None = None
    retry: int | None = None


@dataclass
class RetryConfig:
    attempts: int = 3
    backoff_base: float = 0.5  # seconds — doubles each retry
    backoff_max: float = 16.0
    # HTTP status codes that should trigger a retry
    retry_statuses: frozenset[int] = field(
        default_factory=lambda: frozenset({429, 500, 502, 503, 504})
    )


@dataclass
class HttpResponse:
    status: int
    headers: dict[str, str]
    _raw: aiohttp.ClientResponse
    _body: bytes

    @property
    def ok(self) -> bool:
        return self.status < 400

    def json(self, **kwargs: Any) -> Any:
        import json

        return json.loads(self._body, **kwargs)

    def text(self, encoding: str = "utf-8") -> str:
        return self._body.decode(encoding)

    def content(self) -> bytes:
        return self._body

    def raise_for_status(self) -> None:
        if not self.ok:
            raise ClientResponseError(
                self._raw.request_info,
                self._raw.history,
                status=self.status,
                message=f"HTTP {self.status}",
                headers=self._raw.headers,
            )

    def __repr__(self) -> str:
        return f"<HttpResponse [{self.status}]>"


class HttpClient:
    def __init__(
        self,
        *,
        base_url: str = "",
        timeout: ClientTimeout | None = None,
        retry: RetryConfig | None = None,
        default_headers: dict[str, str] | None = None,
        connector_limit: int = 100,
        ssl: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout or ClientTimeout(
            total=30,
            connect=5,
            sock_read=25,
        )
        self.retry = retry or RetryConfig()
        self.default_headers: dict[str, str] = {
            "Accept": "application/json",
            "User-Agent": f"{settings.app.app_name}/1.0",
            **(default_headers or {}),
        }
        self._connector_limit = connector_limit
        self._ssl = ssl
        self._session: ClientSession | None = None

    async def start(self) -> None:
        """Create the underlying ClientSession. Call once on startup."""
        if self._session and not self._session.closed:
            return
        connector = TCPConnector(
            limit=self._connector_limit,
            limit_per_host=20,
            ttl_dns_cache=300,  # cache DNS 5 min
            use_dns_cache=True,
            ssl=self._ssl if isinstance(self._ssl, bool) else None,
            enable_cleanup_closed=True,
        )
        self._session = ClientSession(
            connector=connector,
            timeout=self.timeout,
            headers=self.default_headers,
            raise_for_status=False,  # we handle status ourselves
            connector_owner=True,
        )
        logger.debug("HttpClient session created (limit=%d)", self._connector_limit)

    async def stop(self) -> None:
        """Close the session and release all connections."""
        if self._session and not self._session.closed:
            await self._session.close()
            logger.debug("HttpClient session closed")

    async def __aenter__(self) -> "HttpClient":
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.stop()

    def _build_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return self.base_url + "/" + path.lstrip("/") if self.base_url else path

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json: Any = None,
        data: Any = None,  # form data (dict) or raw bytes
        timeout: ClientTimeout | None = None,
        allow_redirects: bool = True,
    ) -> HttpResponse:
        if self._session is None or self._session.closed:
            await self.start()

        url = self._build_url(path)
        merged_headers = {**self.default_headers, **(headers or {})}
        last_exc: Exception | None = None

        for attempt in range(1, self.retry.attempts + 1):
            try:
                async with self._session.request(  # type: ignore[union-attr]
                    method,
                    url,
                    params=params,
                    headers=merged_headers,
                    json=json,
                    data=data,
                    timeout=timeout or self.timeout,
                    allow_redirects=allow_redirects,
                    ssl=self._ssl,
                ) as resp:
                    # Only read body if we are NOT going to retry, to avoid
                    # allocating and discarding bytes on every retry attempt.
                    if (
                        resp.status in self.retry.retry_statuses
                        and attempt < self.retry.attempts
                    ):
                        wait = min(
                            self.retry.backoff_base * (2 ** (attempt - 1)),
                            self.retry.backoff_max,
                        )
                        logger.warning(
                            "%s %s → %d (attempt %d/%d), retrying in %.1fs",
                            method,
                            url,
                            resp.status,
                            attempt,
                            self.retry.attempts,
                            wait,
                        )
                        await asyncio.sleep(wait)
                        continue

                    body = await resp.read()
                    http_resp = HttpResponse(
                        status=resp.status,
                        headers=dict(resp.headers),
                        _raw=resp,
                        _body=body,
                    )
                    logger.debug("%s %s → %d", method, url, resp.status)
                    return http_resp

            except (
                ClientConnectionError,
                ServerTimeoutError,
                asyncio.TimeoutError,
            ) as exc:
                last_exc = exc
                if attempt < self.retry.attempts:
                    wait = min(
                        self.retry.backoff_base * (2 ** (attempt - 1)),
                        self.retry.backoff_max,
                    )
                    logger.warning(
                        "%s %s network error (attempt %d/%d): %s — retrying in %.1fs",
                        method,
                        url,
                        attempt,
                        self.retry.attempts,
                        exc,
                        wait,
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.error(
                        "%s %s failed after %d attempts: %s", method, url, attempt, exc
                    )
                    raise

        # Unreachable in normal flow: the loop either returns or re-raises.
        # Guards against any future refactor that breaks that invariant.
        raise last_exc or RuntimeError(
            f"_request loop exited without result: {method} {url}"
        )

    async def request(
        self,
        method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"],
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json: Any = None,
        data: Any = None,  # form dict or raw bytes — only used for POST/PUT/PATCH
        timeout: ClientTimeout | None = None,
        allow_redirects: bool = True,
    ) -> HttpResponse:
        """
        Send an HTTP request. Single method for all verbs.

        Args:
            method:          HTTP verb — "GET" | "POST" | "PUT" | "PATCH" | "DELETE"
            path:            Absolute URL or path relative to base_url.
            params:          Query string parameters (all verbs).
            headers:         Extra headers merged over defaults (all verbs).
            json:            JSON-serialisable body — sets Content-Type automatically
                            (POST / PUT / PATCH).
            data:            Form dict or raw bytes body (POST / PUT / PATCH).
            timeout:         Per-request override. Falls back to instance default.
            allow_redirects: Follow 3xx redirects (default True).

        Returns:
            HttpResponse with .status, .headers, .json(), .text(), .content(),
            .ok, and .raise_for_status().

        Raises:
            aiohttp.ClientConnectionError / asyncio.TimeoutError after all retries.
            ValueError if both `json` and `data` are provided.
            ValueError if `json` or `data` are provided for GET or DELETE.

        Notes:
            - SSE streams: filter event.data == "[DONE]" yourself for OpenAI-style APIs.
            - Non-retryable 4xx responses (401, 403, 404 …) are returned as-is.
                Call resp.raise_for_status() or check resp.ok at the call site.

        Examples:
            # GET with query params
            resp = await http.request("GET", "/users", params={"page": 2})

            # POST JSON body
            resp = await http.request("POST", "/users", json={"name": "Alice"})

            # PUT with auth header
            resp = await http.request(
                "PUT", "/users/1",
                json={"name": "Bob"},
                headers={"Authorization": "Bearer token"},
            )

            # PATCH partial update
            resp = await http.request("PATCH", "/users/1", json={"active": False})

            # DELETE
            resp = await http.request("DELETE", "/users/1")

            # POST form data
            resp = await http.request("POST", "/login", data={"user": "a", "pass": "b"})

            # Raise on 4xx / 5xx
            resp = await http.request("GET", "/protected")
            resp.raise_for_status()
            payload = resp.json()
        """
        if json is not None and data is not None:
            raise ValueError("Provide either `json` or `data`, not both.")

        body_methods = {"POST", "PUT", "PATCH"}
        if (json is not None or data is not None) and method not in body_methods:
            raise ValueError(
                f"`json` and `data` are not supported for {method} requests. "
                f"Use params= for query-string arguments."
            )

        return await self._request(
            method,
            path,
            params=params,
            headers=headers,
            json=json,
            data=data,
            timeout=timeout,
            allow_redirects=allow_redirects,
        )

    async def sse(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json: Any = None,
        method: str = "GET",
        timeout: ClientTimeout | None = None,
        max_retries: int = 5,
        sock_read_timeout: float = 60.0,
    ) -> AsyncGenerator[SSEEvent, None]:
        """
        Consume a Server-Sent Events stream.

        Yields SSEEvent objects as they arrive.
        Automatically reconnects using Last-Event-ID on disconnect.

        Args:
            sock_read_timeout: Seconds to wait for the next chunk from the server
                before treating the connection as dead and reconnecting. Default 60s.
                Lower this (e.g. 15s) if your server sends keepalive comments
                frequently; raise it for slow producers.

        Notes:
            - OpenAI-style APIs send a final `data: [DONE]` event. Filter it yourself:
                async for event in http.sse(...):
                    if event.data == "[DONE]":
                        break
                    process(event.data)

        Usage:
            async for event in http.sse("https://api.example.com/stream"):
                if event.event == "update":
                    process(event.data)

            # POST-based SSE (e.g. OpenAI streaming)
            async for event in http.sse(
                "https://api.openai.com/v1/chat/completions",
                method="POST",
                json={"model": "gpt-4o", "stream": True, ...},
                headers={"Authorization": "Bearer sk-..."},
            ):
                if event.data == "[DONE]":
                    break
                print(event.data)
        """
        if self._session is None or self._session.closed:
            await self.start()

        url = self._build_url(path)
        sse_headers = {
            **self.default_headers,
            "Accept": "text/event-stream",
            "Cache-Control": "no-cache",
            **(headers or {}),
        }
        last_event_id: str | None = None
        sse_timeout = timeout or ClientTimeout(
            total=None,
            connect=5,
            sock_read=sock_read_timeout,
        )

        for attempt in range(max_retries):
            if last_event_id:
                sse_headers["Last-Event-ID"] = last_event_id
            try:
                async with self._session.request(  # type: ignore[union-attr]
                    method,
                    url,
                    params=params,
                    headers=sse_headers,
                    json=json,
                    timeout=sse_timeout,
                    ssl=self._ssl,
                ) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.error("SSE %s → %d: %s", url, resp.status, body[:200])
                        return

                    # Parse the SSE stream line by line
                    event = SSEEvent()
                    async for raw_line in resp.content:
                        line = raw_line.decode("utf-8").rstrip("\n\r")

                        if not line:
                            # Blank line → dispatch event if it has data
                            if event.data:
                                yield event
                            event = SSEEvent()
                            continue

                        if line.startswith(":"):
                            # Comment line — keepalive ping
                            continue

                        if ":" in line:
                            field_name, _, field_value = line.partition(":")
                            field_value = field_value.lstrip(" ")
                        else:
                            field_name, field_value = line, ""

                        match field_name:
                            case "data":
                                event.data = (
                                    event.data + "\n" + field_value
                                    if event.data
                                    else field_value
                                )
                            case "event":
                                event.event = field_value
                            case "id":
                                event.id = field_value
                                last_event_id = field_value
                            case "retry":
                                try:
                                    event.retry = int(field_value)
                                except ValueError:
                                    pass

                    return  # clean stream end

            except (ClientConnectionError, asyncio.TimeoutError) as exc:
                wait = min(
                    self.retry.backoff_base * (2**attempt), self.retry.backoff_max
                )
                logger.warning(
                    "SSE %s disconnected (attempt %d/%d): %s — reconnecting in %.1fs",
                    url,
                    attempt + 1,
                    max_retries,
                    exc,
                    wait,
                )
                await asyncio.sleep(wait)

        logger.error("SSE %s gave up after %d reconnects", url, max_retries)

    async def upload(
        self,
        path: str,
        *,
        file_bytes: bytes,
        filename: str,
        field: str = "file",
        extra_fields: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> HttpResponse:
        """
        Multipart file upload.

        Usage:
            resp = await http.upload(
                "/upload",
                file_bytes=open("doc.pdf", "rb").read(),
                filename="doc.pdf",
                extra_fields={"category": "report"},
            )
        """
        form = aiohttp.FormData()

        form.add_field(field, file_bytes, filename=filename)
        for k, v in (extra_fields or {}).items():
            form.add_field(k, v)

        return await self._request("POST", path, data=form, headers=headers)


# Global singleton.
# Wire into your framework lifespan:
#   await http.start()   # on startup
#   await http.stop()    # on shutdown
http: HttpClient = HttpClient(
    base_url=settings.http.base_url,
    timeout=ClientTimeout(
        total=settings.http.timeout_total,
        connect=settings.http.timeout_connect,
        sock_read=settings.http.timeout_sock_read,
    ),
    retry=RetryConfig(
        attempts=settings.http.retry_attempts,
        backoff_base=settings.http.retry_backoff_base,
        backoff_max=settings.http.retry_backoff_max,
    ),
    connector_limit=settings.http.connector_limit,
    ssl=settings.http.ssl,
)


async def _run_http_examples() -> None:
    base_url = settings.http.demo_base_url
    client = HttpClient(
        base_url=base_url,
        default_headers={"X-Demo-Client": "http_client.py"},
    )
    logger.info("HTTP examples starting base_url=%s", base_url)

    async def run_step(name: str, fn: Any) -> None:
        try:
            await fn()
        except Exception as exc:
            logger.error("%s failed: %s", name, exc)

    async with client:

        async def example_1_get_with_params() -> None:
            resp = await client.request(
                "GET", "/get", params={"q": "fastapi", "page": 1}
            )
            payload = resp.json()
            logger.info(
                "Example 1 GET status=%d args=%s", resp.status, payload.get("args")
            )

        async def example_2_post_json() -> None:
            resp = await client.request(
                "POST",
                "/post",
                json={"name": "Alice", "roles": ["admin", "editor"]},
            )
            payload = resp.json()
            logger.info(
                "Example 2 POST status=%d json=%s", resp.status, payload.get("json")
            )

        async def example_3_put_with_headers() -> None:
            resp = await client.request(
                "PUT",
                "/put",
                json={"active": True},
                headers={"X-Trace-ID": "demo-put-001"},
            )
            payload = resp.json()
            logger.info(
                "Example 3 PUT status=%d trace=%s",
                resp.status,
                payload["headers"].get("X-Trace-Id"),
            )

        async def example_4_patch_partial_update() -> None:
            resp = await client.request("PATCH", "/patch", json={"plan": "pro"})
            payload = resp.json()
            logger.info(
                "Example 4 PATCH status=%d body=%s", resp.status, payload.get("json")
            )

        async def example_5_delete_request() -> None:
            resp = await client.request("DELETE", "/delete")
            logger.info("Example 5 DELETE status=%d ok=%s", resp.status, resp.ok)

        async def example_6_form_submission() -> None:
            resp = await client.request(
                "POST",
                "/post",
                data={"username": "alice", "password": "secret"},
            )
            payload = resp.json()
            logger.info(
                "Example 6 FORM status=%d form=%s", resp.status, payload.get("form")
            )

        async def example_7_per_request_timeout() -> None:
            fast_timeout = ClientTimeout(total=2, connect=1, sock_read=1)
            resp = await client.request("GET", "/get", timeout=fast_timeout)
            logger.info("Example 7 timeout override status=%d", resp.status)

        async def example_8_raise_for_status() -> None:
            resp = await client.request("GET", "/status/404")
            try:
                resp.raise_for_status()
            except ClientResponseError as exc:
                logger.info("Example 8 raise_for_status captured status=%d", exc.status)

        async def example_9_parallel_requests() -> None:
            endpoints = ["/uuid", "/ip", "/user-agent"]
            responses = await asyncio.gather(
                *(client.request("GET", ep) for ep in endpoints),
                return_exceptions=True,
            )
            statuses = [
                r.status if isinstance(r, HttpResponse) else f"error:{type(r).__name__}"
                for r in responses
            ]
            logger.info("Example 9 parallel statuses=%s", statuses)

        async def example_10_multipart_upload() -> None:
            resp = await client.upload(
                "/post",
                file_bytes=b"hello-from-http-client",
                filename="demo.txt",
                extra_fields={"category": "demo", "source": "http_client.py"},
            )
            payload = resp.json()
            logger.info(
                "Example 10 upload status=%d files_keys=%s form=%s",
                resp.status,
                sorted(payload.get("files", {}).keys()),
                payload.get("form"),
            )

        async def example_11_absolute_url() -> None:
            resp = await client.request(
                "GET", "https://httpbin.org/get", params={"abs": "1"}
            )
            logger.info("Example 11 absolute URL status=%d", resp.status)

        async def example_12_optional_sse() -> None:
            sse_url = settings.http.demo_sse_url
            if not sse_url:
                logger.info("Example 12 SSE skipped (set HTTP_DEMO_SSE_URL to enable).")
                return

            count = 0
            async for event in client.sse(sse_url, max_retries=1):
                logger.info(
                    "Example 12 SSE event=%s id=%s data=%s",
                    event.event,
                    event.id,
                    event.data[:80],
                )
                count += 1
                if count >= 3:
                    break

        await run_step("Example 1 GET with params", example_1_get_with_params)
        await run_step("Example 2 POST JSON", example_2_post_json)
        await run_step("Example 3 PUT with headers", example_3_put_with_headers)
        await run_step("Example 4 PATCH partial", example_4_patch_partial_update)
        await run_step("Example 5 DELETE request", example_5_delete_request)
        await run_step("Example 6 form data", example_6_form_submission)
        await run_step("Example 7 timeout override", example_7_per_request_timeout)
        await run_step("Example 8 raise_for_status", example_8_raise_for_status)
        await run_step("Example 9 parallel requests", example_9_parallel_requests)
        await run_step("Example 10 multipart upload", example_10_multipart_upload)
        await run_step("Example 11 absolute URL", example_11_absolute_url)
        await run_step("Example 12 optional SSE", example_12_optional_sse)

    logger.info("HTTP examples completed.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    asyncio.run(_run_http_examples())
