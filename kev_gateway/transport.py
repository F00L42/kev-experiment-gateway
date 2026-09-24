"""Small streaming ASGI reverse proxy; capture failures never alter forwarding."""

from __future__ import annotations

import asyncio
import http.cookiejar
import logging

import httpx

from .capture import Capture, OutputRootError
from .config import GatewayConfig

log = logging.getLogger(__name__)
HOP_HEADERS = {
    b"connection",
    b"keep-alive",
    b"proxy-authenticate",
    b"proxy-authorization",
    b"te",
    b"trailer",
    b"transfer-encoding",
    b"upgrade",
}


def forward_headers(headers, *, request=False):
    """Remove hop-by-hop headers, including names declared by Connection."""
    excluded = HOP_HEADERS | ({b"host"} if request else set())
    for name, value in headers:
        if name.lower() == b"connection":
            excluded.update(part.strip().lower() for part in value.split(b","))
    return [(name.lower(), value) for name, value in headers if name.lower() not in excluded]


class NoCookies(http.cookiejar.CookieJar):
    """The connection pool must not turn one client's cookies into another's."""

    def extract_cookies(self, response, request):
        pass


class _ClientDisconnected(Exception):
    pass


class _RawTargetURL(httpx.URL):
    """Retain the ASGI request target, including literal dot segments.

    HTTPX normally normalizes /a/../b while constructing a URL. Its transport
    uses the public raw_path property for the actual HTTP request target.
    """

    def __init__(self, origin, target):
        super().__init__(httpx.URL(origin).copy_with(raw_path=target))
        self._target = target

    @property
    def raw_path(self):
        return self._target


class Gateway:
    def __init__(self, config: GatewayConfig, *, client=None, capture=None):
        self.config = config
        self.client = client
        self.capture = capture
        self.owns_client = client is None
        self.active_requests = set()
        self._started = False
        self._close_task = None

    async def start(self):
        if self._started:
            return
        if self._close_task is not None:
            raise RuntimeError("Gateway is closing")
        created_capture = self.capture is None
        if created_capture:
            try:
                self.capture = await asyncio.to_thread(Capture, self.config)
            except OutputRootError:
                raise
            except OSError as exc:
                raise OutputRootError(
                    f"Cannot initialize output_root '{self.config.output_root}' "
                    f"({type(exc).__name__}: {exc.strerror or exc})"
                ) from exc
        try:
            if self.client is None:
                self.client = httpx.AsyncClient(
                    timeout=httpx.Timeout(
                        connect=self.config.connect_timeout,
                        read=self.config.read_timeout,
                        write=self.config.write_timeout,
                        pool=self.config.pool_timeout,
                    ),
                    limits=httpx.Limits(
                        max_connections=self.config.max_connections,
                        max_keepalive_connections=self.config.max_keepalive_connections,
                    ),
                    trust_env=False,
                    cookies=NoCookies(),
                    follow_redirects=False,
                )
        except Exception:
            if created_capture:
                try:
                    await asyncio.to_thread(self.capture.close)
                finally:
                    self.capture = None
            raise
        self._started = True

    async def close(self):
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)

    async def _close(self):
        # Uvicorn can begin lifespan shutdown while cancelled requests still clean up.
        if self.active_requests:
            await asyncio.gather(*tuple(self.active_requests), return_exceptions=True)
        try:
            if self.owns_client and self.client is not None:
                await self.client.aclose()
        finally:
            if self.capture is not None:
                await asyncio.to_thread(self.capture.close)

    def _record(self, method, *args, **kwargs):
        try:
            return getattr(self.capture, method)(*args, **kwargs)
        except Exception as exc:
            logger = getattr(self.capture, "log", log)
            logger.error("Capture operation failed: %s (%s)", method, type(exc).__name__)
            return None

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            while True:
                event = await receive()
                if event["type"] == "lifespan.startup":
                    try:
                        await self.start()
                    except Exception as exc:
                        message = str(exc) if isinstance(exc, OutputRootError) else type(exc).__name__
                        await send({"type": "lifespan.startup.failed", "message": message})
                        return
                    await send({"type": "lifespan.startup.complete"})
                elif event["type"] == "lifespan.shutdown":
                    try:
                        await self.close()
                    except Exception as exc:
                        await send({"type": "lifespan.shutdown.failed", "message": type(exc).__name__})
                        return
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        elif scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 1008})
        elif scope["type"] == "http":
            if self._close_task is not None:
                await send(
                    {"type": "http.response.start", "status": 503, "headers": [(b"content-length", b"0")]}
                )
                await send({"type": "http.response.body", "body": b""})
                return
            if not self._started:
                raise RuntimeError("ASGI lifespan must be enabled; call start() when embedding Gateway")
            task = asyncio.current_task()
            self.active_requests.add(task)
            try:
                await self._handle(scope, receive, send)
            finally:
                self.active_requests.discard(task)

    async def _handle(self, scope, receive, send):
        headers = scope.get("headers", [])
        raw_path = scope.get("raw_path", scope["path"].encode("utf-8"))
        query = scope.get("query_string", b"")
        ctx = self._record("begin", scope["method"], raw_path, query, headers)
        outcome = "gateway_error"
        error = None
        response_started = False
        upstream = None
        upload = asyncio.Queue(maxsize=1)
        final_delivery = None
        client_disconnected = False

        def record(method, *args, **kwargs):
            if ctx is not None:
                return self._record(method, ctx, *args, **kwargs)
            return None

        async def downstream(event):
            nonlocal client_disconnected
            try:
                await send(event)
            except OSError as exc:
                client_disconnected = True
                raise _ClientDisconnected from exc

        async def request_body():
            while True:
                data, finished = await upload.get()
                if data:
                    yield data
                if finished:
                    break

        async def receive_request():
            nonlocal client_disconnected, final_delivery
            # A single consumer reads ASGI receive, even before HTTPX obtains a
            # connection. Bound read-ahead instead of accumulating full uploads.
            while True:
                event = await receive()
                if event["type"] == "http.disconnect":
                    client_disconnected = True
                    return
                if event["type"] != "http.request":
                    raise RuntimeError("Unexpected ASGI request event")
                data = event.get("body", b"")
                if data:
                    record("body", "request", data)
                finished = not event.get("more_body", False)
                if finished:
                    record("body_end", "request")
                    # Retain at most one final event outside the bounded queue.
                    # Observe disconnect immediately once the whole body arrived,
                    # even while HTTPX waits for a pooled connection or writes.
                    final_delivery = asyncio.create_task(upload.put((data, True)))
                    break
                await upload.put((data, False))
            while True:
                event = await receive()
                if event["type"] == "http.disconnect":
                    client_disconnected = True
                    return

        async def local_response(status):
            nonlocal response_started
            data = f"Gateway error ({status})\n".encode()
            local_headers = [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"content-length", str(len(data)).encode()),
            ]
            record("response_start", status, local_headers, source="gateway")
            response_started = True
            await downstream({"type": "http.response.start", "status": status, "headers": local_headers})
            body = b"" if scope["method"] == "HEAD" else data
            record("body", "response", body)
            record("body_end", "response")
            await downstream({"type": "http.response.body", "body": body, "more_body": False})

        async def exchange():
            nonlocal upstream, response_started, outcome
            if scope["method"] == "CONNECT" or any(name.lower() == b"upgrade" for name, _ in headers):
                await local_response(501)
                outcome = "gateway_error"
                return
            target = _RawTargetURL(self.config.upstream_url, raw_path + (b"?" + query if query else b""))
            body = request_body()
            try:
                # Request, unlike build_request(), merges neither client headers nor cookies.
                request = httpx.Request(
                    scope["method"],
                    target,
                    headers=forward_headers(headers, request=True),
                    content=body,
                    extensions={"timeout": self.client.timeout.as_dict()},
                )
                # Request reconstructs its URL; assign afterwards to retain raw_path.
                request.url = target
                upstream = await self.client.send(request, stream=True, follow_redirects=False)
            finally:
                await body.aclose()
            record("response_start", upstream.status_code, upstream.headers.raw, source="upstream")
            response_started = True
            await downstream(
                {
                    "type": "http.response.start",
                    "status": upstream.status_code,
                    "headers": forward_headers(upstream.headers.raw),
                }
            )
            async for chunk in upstream.aiter_raw():
                record("body", "response", chunk)
                if scope["method"] != "HEAD":
                    await downstream({"type": "http.response.body", "body": chunk, "more_body": True})
            record("body_end", "response")
            await downstream({"type": "http.response.body", "body": b"", "more_body": False})
            outcome = "completed"

        work = asyncio.create_task(exchange())
        disconnect = asyncio.create_task(receive_request())
        try:
            done, _ = await asyncio.wait({work, disconnect}, return_when=asyncio.FIRST_COMPLETED)
            if work in done:
                await work
            else:
                # A receive() exception is a transport failure, not a disconnect signal.
                await disconnect
                outcome = "client_disconnected"
        except _ClientDisconnected:
            outcome = "client_disconnected"
        except asyncio.CancelledError:
            outcome = "client_disconnected" if client_disconnected else "cancelled"
            raise
        except Exception as exc:
            work.cancel()
            await asyncio.gather(work, return_exceptions=True)
            outcome = "upstream_error" if isinstance(exc, httpx.HTTPError) else "gateway_error"
            error = {"type": type(exc).__name__, "phase": "response" if response_started else "request"}
            if response_started:
                # Let the ASGI server abort an incomplete stream; never manufacture EOF.
                # ASGI servers log this exception. Suppress URL/body-bearing messages.
                raise RuntimeError(f"Gateway response aborted ({type(exc).__name__})") from None
            try:
                await local_response(504 if isinstance(exc, httpx.TimeoutException) else 502)
            except _ClientDisconnected:
                outcome = "client_disconnected"
        finally:
            work.cancel()
            disconnect.cancel()
            await asyncio.gather(work, disconnect, return_exceptions=True)
            if final_delivery is not None:
                final_delivery.cancel()
                await asyncio.gather(final_delivery, return_exceptions=True)
            try:
                if upstream is not None:
                    await asyncio.wait_for(upstream.aclose(), timeout=5)
            except Exception as exc:
                cleanup_error = {"type": type(exc).__name__, "phase": "upstream_close"}
                error = {**(error or {}), "cleanup": cleanup_error}
                log.warning("Upstream cleanup failed: %s", type(exc).__name__)
            finally:
                record("finish", outcome, error=error)
