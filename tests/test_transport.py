from __future__ import annotations

import asyncio
import gzip
import json
import socket
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest
import uvicorn

from kev_gateway.config import GatewayConfig
from kev_gateway.transport import Gateway, forward_headers


class RecordingCapture:
    def __init__(self):
        self.records = []
        self.closed = False

    def begin(self, method, path, query, headers):
        ctx = {
            "method": method,
            "path": path,
            "query": query,
            "headers": headers,
            "request": bytearray(),
            "response": bytearray(),
            "ends": set(),
        }
        self.records.append(ctx)
        return ctx

    def body(self, ctx, direction, data):
        ctx[direction].extend(data)

    def body_end(self, ctx, direction):
        ctx["ends"].add(direction)

    def response_start(self, ctx, status, headers, source="upstream"):
        ctx.update(status=status, response_headers=headers, source=source)

    def finish(self, ctx, outcome, error=None):
        ctx.update(outcome=outcome, error=error)

    def close(self):
        self.closed = True


class BytesStream(httpx.AsyncByteStream):
    def __init__(self, *chunks, failure=None):
        self.chunks = chunks
        self.failure = failure
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk
        if self.failure:
            raise self.failure

    async def aclose(self):
        self.closed = True


def scope(**overrides):
    return {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "path": "/v1/systemone",
        "raw_path": b"/v1/systemone",
        "query_string": b"",
        "headers": [],
        **overrides,
    }


def channel(events=None):
    queue = asyncio.Queue()
    for event in events or [{"type": "http.request", "body": b"request", "more_body": False}]:
        queue.put_nowait(event)
    sent = []

    async def send(event):
        sent.append(event)

    return queue, sent, send


def configured(tmp_path, **kwargs):
    return GatewayConfig(upstream_url="http://upstream.invalid:8000", output_root=tmp_path, **kwargs)


def test_connection_nominated_headers_removed():
    headers = [
        (b"Connection", b"x-private, keep-alive"),
        (b"X-Private", b"remove"),
        (b"Host", b"wrong"),
        (b"X-Repeat", b"a"),
        (b"X-Repeat", b"b"),
    ]
    assert forward_headers(headers, request=True) == [(b"x-repeat", b"a"), (b"x-repeat", b"b")]
    assert (b"host", b"wrong") in forward_headers(headers)


def test_exact_bytes_headers_status_and_no_client_defaults(tmp_path):
    async def run():
        capture = RecordingCapture()
        compressed = gzip.compress(b'{ "answers": [] }\n')
        stream = BytesStream(compressed[:5], compressed[5:])

        async def upstream(request):
            assert request.url.raw_path == b"/a%2Fb//c/../d?x=1&x=2&empty=&q=%2f"
            assert await request.aread() == b"\x00one\xfftwo"
            assert request.headers.get_list("x-repeat") == ["one", "two"]
            assert request.headers["host"] == "upstream.invalid:8000"
            assert request.headers["authorization"] == "Bearer user-token"
            assert "x-private" not in request.headers
            assert "x-client-default" not in request.headers
            assert "cookie" not in request.headers
            return httpx.Response(
                429,
                headers=[
                    (b"content-encoding", b"gzip"),
                    (b"x-repeat", b"a"),
                    (b"x-repeat", b"b"),
                    (b"connection", b"x-internal"),
                    (b"x-internal", b"remove"),
                ],
                stream=stream,
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(upstream),
            headers={"x-client-default": "never"},
            cookies={"other-user": "secret"},
        ) as client:
            gateway = Gateway(configured(tmp_path), client=client, capture=capture)
            await gateway.start()
            queue, sent, send = channel(
                [
                    {"type": "http.request", "body": b"\x00one", "more_body": True},
                    {"type": "http.request", "body": b"\xfftwo", "more_body": False},
                ]
            )
            await gateway(
                scope(
                    raw_path=b"/a%2Fb//c/../d",
                    query_string=b"x=1&x=2&empty=&q=%2f",
                    headers=[
                        (b"host", b"client.invalid"),
                        (b"x-repeat", b"one"),
                        (b"x-repeat", b"two"),
                        (b"authorization", b"Bearer user-token"),
                        (b"connection", b"x-private"),
                        (b"x-private", b"remove"),
                    ],
                ),
                queue.get,
                send,
            )
            await gateway.close()
        assert sent[0]["status"] == 429
        assert [v for k, v in sent[0]["headers"] if k == b"x-repeat"] == [b"a", b"b"]
        assert b"x-internal" not in dict(sent[0]["headers"])
        assert b"".join(event.get("body", b"") for event in sent) == compressed
        record = capture.records[0]
        assert record["outcome"] == "completed"
        assert record["ends"] == {"request", "response"}
        assert record["response"] == compressed
        assert record["source"] == "upstream"
        assert stream.closed and capture.closed

    asyncio.run(run())


@pytest.mark.parametrize(
    "exception,status", [(httpx.ConnectError("failed"), 502), (httpx.ReadTimeout("timeout"), 504)]
)
def test_local_error_body_is_captured(tmp_path, exception, status):
    async def run():
        capture = RecordingCapture()

        async def upstream(request):
            raise exception

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            gateway = Gateway(configured(tmp_path), client=client, capture=capture)
            await gateway.start()
            queue, sent, send = channel()
            await gateway(scope(), queue.get, send)
            await gateway.close()
        record = capture.records[0]
        assert sent[0]["status"] == status
        assert record["status"] == status and record["source"] == "gateway"
        assert record["outcome"] == "upstream_error"
        assert record["response"] == sent[-1]["body"]
        assert "response" in record["ends"]

    asyncio.run(run())


def test_disconnect_during_upload(tmp_path):
    async def run():
        capture = RecordingCapture()

        async def upstream(request):
            pytest.fail("Incomplete body must not be treated as a completed request")

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            gateway = Gateway(configured(tmp_path), client=client, capture=capture)
            await gateway.start()
            queue, sent, send = channel(
                [
                    {"type": "http.request", "body": b"partial", "more_body": True},
                    {"type": "http.disconnect"},
                ]
            )
            await asyncio.wait_for(gateway(scope(), queue.get, send), 1)
            await gateway.close()
        assert sent == []
        assert capture.records[0]["outcome"] == "client_disconnected"
        assert capture.records[0]["ends"] == set()

    asyncio.run(run())


def test_disconnect_cancels_wait_for_response_headers(tmp_path):
    async def run():
        capture = RecordingCapture()
        entered, cancelled = asyncio.Event(), asyncio.Event()

        async def upstream(request):
            entered.set()
            try:
                await asyncio.Future()
            finally:
                cancelled.set()

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            gateway = Gateway(configured(tmp_path), client=client, capture=capture)
            await gateway.start()
            queue, sent, send = channel()
            task = asyncio.create_task(gateway(scope(), queue.get, send))
            await asyncio.wait_for(entered.wait(), 1)
            queue.put_nowait({"type": "http.disconnect"})
            await asyncio.wait_for(task, 1)
            await gateway.close()
        assert cancelled.is_set() and sent == []
        assert capture.records[0]["outcome"] == "client_disconnected"
        assert capture.records[0]["ends"] == {"request"}

    asyncio.run(run())


def test_disconnect_cancels_pool_wait_before_upload_is_consumed(tmp_path):
    async def run():
        entered, cancelled = asyncio.Event(), asyncio.Event()
        capture = RecordingCapture()

        class WaitingTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request):
                entered.set()
                try:
                    # Model waiting for a connection before reading request.stream.
                    await asyncio.Future()
                finally:
                    cancelled.set()

        async with httpx.AsyncClient(transport=WaitingTransport()) as client:
            gateway = Gateway(configured(tmp_path), client=client, capture=capture)
            await gateway.start()
            queue, sent, send = channel(
                [
                    {"type": "http.request", "body": b"first", "more_body": True},
                    {"type": "http.request", "body": b"last", "more_body": False},
                ]
            )
            task = asyncio.create_task(gateway(scope(), queue.get, send))
            await entered.wait()
            queue.put_nowait({"type": "http.disconnect"})
            await asyncio.wait_for(task, 1)
            await gateway.close()
        assert cancelled.is_set() and sent == []
        assert capture.records[0]["outcome"] == "client_disconnected"
        assert capture.records[0]["request"] == b"firstlast"
        assert capture.records[0]["ends"] == {"request"}

    asyncio.run(run())


def test_broken_response_aborts_without_false_eof(tmp_path):
    async def run():
        capture = RecordingCapture()
        stream = BytesStream(b"partial", failure=httpx.ReadError("broken-private-url-secret"))

        async def upstream(request):
            return httpx.Response(200, stream=stream)

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            gateway = Gateway(configured(tmp_path), client=client, capture=capture)
            await gateway.start()
            queue, sent, send = channel()
            with pytest.raises(RuntimeError, match="Gateway response aborted") as aborted:
                await gateway(scope(), queue.get, send)
            await gateway.close()
        trace = "".join(traceback.format_exception(aborted.value))
        assert "broken-private-url-secret" not in trace
        assert sent[-1] == {"type": "http.response.body", "body": b"partial", "more_body": True}
        assert capture.records[0]["outcome"] == "upstream_error"
        assert capture.records[0]["error"] == {"type": "ReadError", "phase": "response"}
        assert "response" not in capture.records[0]["ends"]
        assert stream.closed

    asyncio.run(run())


def test_capture_exception_does_not_change_wire_response(tmp_path):
    class BrokenCapture(RecordingCapture):
        def body(self, *args):
            raise OSError("Disk failed")

    async def run():
        capture = BrokenCapture()

        async def upstream(request):
            return httpx.Response(200, stream=BytesStream(b"unchanged"))

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            gateway = Gateway(configured(tmp_path), client=client, capture=capture)
            await gateway.start()
            queue, sent, send = channel()
            await gateway(scope(), queue.get, send)
            await gateway.close()
        assert b"".join(event.get("body", b"") for event in sent) == b"unchanged"
        assert capture.records[0]["outcome"] == "completed"

    asyncio.run(run())


def test_shutdown_waits_for_request_cleanup(tmp_path):
    async def run():
        capture = RecordingCapture()
        entered, release = asyncio.Event(), asyncio.Event()

        async def upstream(request):
            entered.set()
            await release.wait()
            return httpx.Response(200, stream=BytesStream(b"done"))

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            gateway = Gateway(configured(tmp_path), client=client, capture=capture)
            await gateway.start()
            queue, _, send = channel()
            task = asyncio.create_task(gateway(scope(), queue.get, send))
            await entered.wait()
            closing = asyncio.create_task(gateway.close())
            await asyncio.sleep(0)
            assert not capture.closed
            other_queue, other_sent, other_send = channel()
            await gateway(scope(), other_queue.get, other_send)
            assert other_sent[0]["status"] == 503
            release.set()
            await asyncio.wait_for(asyncio.gather(task, closing), 1)
        assert capture.closed and capture.records[0]["outcome"] == "completed"

    asyncio.run(run())


def test_redirect_not_followed_and_head_body_empty(tmp_path):
    async def run():
        capture = RecordingCapture()
        calls = []

        async def upstream(request):
            calls.append(request)
            return httpx.Response(
                307, headers={"location": "/other", "content-length": "7"}, stream=BytesStream()
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            gateway = Gateway(configured(tmp_path), client=client, capture=capture)
            await gateway.start()
            queue, sent, send = channel([{"type": "http.request", "body": b""}])
            await gateway(scope(method="HEAD"), queue.get, send)
            await gateway.close()
        assert len(calls) == 1
        assert sent[0]["status"] == 307
        assert b"".join(event.get("body", b"") for event in sent) == b""

    asyncio.run(run())


def test_shutdown_cancellation_is_not_mislabeled_disconnect(tmp_path):
    async def run():
        capture = RecordingCapture()
        entered = asyncio.Event()

        async def upstream(request):
            entered.set()
            await asyncio.Future()

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            gateway = Gateway(configured(tmp_path), client=client, capture=capture)
            await gateway.start()
            queue, sent, send = channel()
            task = asyncio.create_task(gateway(scope(), queue.get, send))
            await entered.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await gateway.close()
        assert sent == []
        assert capture.records[0]["outcome"] == "cancelled"

    asyncio.run(run())


def test_downstream_send_failure_is_disconnected(tmp_path):
    async def run():
        capture = RecordingCapture()
        stream = BytesStream(b"data")

        async def upstream(request):
            return httpx.Response(200, stream=stream)

        async def send(event):
            if event["type"] == "http.response.body":
                raise ConnectionResetError("gone")

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            gateway = Gateway(configured(tmp_path), client=client, capture=capture)
            await gateway.start()
            queue, _, _ = channel()
            await gateway(scope(), queue.get, send)
            await gateway.close()
        assert capture.records[0]["outcome"] == "client_disconnected"
        assert capture.records[0]["response"] == b"data"
        assert "response" not in capture.records[0]["ends"]
        assert stream.closed

    asyncio.run(run())


def test_upgrade_and_connect_rejected_without_upstream(tmp_path):
    async def run():
        capture = RecordingCapture()

        async def upstream(request):
            pytest.fail("CONNECT/upgrade must not reach upstream")

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            gateway = Gateway(configured(tmp_path), client=client, capture=capture)
            await gateway.start()
            for request_scope in [scope(method="CONNECT"), scope(headers=[(b"upgrade", b"websocket")])]:
                queue, sent, send = channel()
                await gateway(request_scope, queue.get, send)
                assert sent[0]["status"] == 501
            await gateway.close()
        assert all(record["source"] == "gateway" for record in capture.records)

    asyncio.run(run())


def test_real_capture_concurrent_records_and_runtime_are_separate(tmp_path):
    async def run():
        async def upstream(request):
            assert request.headers["authorization"] == "Bearer private-token"
            assert request.url.query == b"x=1&token=query-secret&x=2"
            return httpx.Response(
                200, stream=BytesStream(await request.aread()), headers={"set-cookie": "private-cookie=yes"}
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            gateway = Gateway(configured(tmp_path), client=client)
            await gateway.start()

            async def one(index):
                body = json.dumps({"state": f"state-{index}", "questions": {}}, indent=2).encode()
                queue, sent, send = channel([{"type": "http.request", "body": body}])
                await gateway(
                    scope(
                        query_string=b"x=1&token=query-secret&x=2",
                        headers=[(b"authorization", b"Bearer private-token")],
                    ),
                    queue.get,
                    send,
                )
                assert b"".join(event.get("body", b"") for event in sent) == body

            await asyncio.gather(*(one(index) for index in range(20)))
            await gateway.close()

    asyncio.run(run())
    files = list((tmp_path / "exchanges").glob("*/*.jsonl"))
    assert len(files) == 1
    content = files[0].read_text(encoding="utf-8")
    assert "private-token" not in content and "private-cookie" not in content
    assert "query-secret" not in content
    records = [json.loads(line) for line in content.splitlines()]
    assert len(records) == 20 and len({record["id"] for record in records}) == 20
    assert all(record["capture_complete"] for record in records)
    assert all(record["request"]["body"] == record["response"]["body"] for record in records)
    assert all(record["outcome"] == "completed" for record in records)
    assert not any("config" in record or "counts" in record for record in records)
    runtime = json.loads(next((tmp_path / "runtime").glob("*/run.json")).read_text(encoding="utf-8"))
    assert runtime["clean_shutdown"] and runtime["counts"]["written"] == 20
    assert {record["run_id"] for record in records} == {runtime["run_id"]}


def test_error_capture_does_not_expose_exception_url(tmp_path):
    async def run():
        async def upstream(request):
            raise httpx.ConnectError("http://upstream.invalid/?token=private-error-secret")

        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            gateway = Gateway(configured(tmp_path), client=client)
            await gateway.start()
            queue, sent, send = channel()
            await gateway(scope(), queue.get, send)
            await gateway.close()
        assert sent[0]["status"] == 502

    asyncio.run(run())
    content = next((tmp_path / "exchanges").glob("*/*.jsonl")).read_text(encoding="utf-8")
    assert "private-error-secret" not in content
    record = json.loads(content)
    assert record["error"] == {"type": "ConnectError", "phase": "request"}
    assert record["response_source"] == "gateway"
    assert record["response"]["body"] == "Gateway error (502)\n"


def test_failed_client_startup_drains_new_capture(tmp_path, monkeypatch):
    def broken_client(**kwargs):
        raise RuntimeError("Client initialization failed")

    async def run():
        gateway = Gateway(configured(tmp_path))
        monkeypatch.setattr("kev_gateway.transport.httpx.AsyncClient", broken_client)
        with pytest.raises(RuntimeError, match="Client initialization failed"):
            await gateway.start()
        assert gateway.capture is None
        await gateway.close()

    asyncio.run(run())
    runtime = json.loads(next((tmp_path / "runtime").glob("*/run.json")).read_text(encoding="utf-8"))
    assert runtime["clean_shutdown"] and runtime["ended_at"] is not None


def test_loopback_uvicorn_real_http_preserves_wire_and_cookie_isolation(tmp_path):
    """Real sockets verify HTTPX raw streaming and ASGI serialization together."""
    seen = []
    compressed = gzip.compress(b'{"answers":{"sample":2},"usage":{"total_tokens":5}}')

    class Upstream(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):
            body = self.rfile.read(int(self.headers["content-length"]))
            seen.append(
                (
                    self.requestline.split(" ")[1],
                    body,
                    self.headers.get_all("x-repeat"),
                    self.headers.get("cookie"),
                )
            )
            self.send_response(418)
            self.send_header("content-encoding", "gzip")
            self.send_header("content-length", str(len(compressed)))
            self.send_header("set-cookie", "upstream-private=one; Path=/")
            self.send_header("x-repeat", "a")
            self.send_header("x-repeat", "b")
            self.end_headers()
            self.wfile.write(compressed)

        def log_message(self, *args):
            pass

    upstream = ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
    thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    thread.start()

    async def run():
        capture = RecordingCapture()
        config = GatewayConfig(upstream_url=f"http://127.0.0.1:{upstream.server_port}", output_root=tmp_path)
        gateway = Gateway(config, capture=capture)
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(gateway, log_level="error", lifespan="on"))
        task = asyncio.create_task(server.serve(sockets=[sock]))

        async def wait_start():
            while not server.started:
                if task.done():
                    await task
                    raise RuntimeError("Server stopped before startup")
                await asyncio.sleep(0.01)

        try:
            await asyncio.wait_for(wait_start(), 5)
            for index in range(2):
                # A distinct downstream client must not inherit upstream Set-Cookie.
                async with httpx.AsyncClient(trust_env=False) as client:
                    async with client.stream(
                        "POST",
                        f"http://127.0.0.1:{port}/a%2Fb?x=1&x=2",
                        content=f"raw-{index}".encode(),
                        headers=[("x-repeat", "one"), ("x-repeat", "two")],
                    ) as response:
                        assert response.status_code == 418
                        assert response.headers.get_list("x-repeat") == ["a", "b"]
                        assert b"".join([chunk async for chunk in response.aiter_raw()]) == compressed
            # Send without a URL library normalizing the target. A leading //
            # remains a path and cannot replace the configured upstream authority.
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(
                b"POST //evil.invalid/a/../b?x=%2f HTTP/1.1\r\n"
                b"Host: localhost\r\nContent-Length: 3\r\nConnection: close\r\n"
                b"X-Repeat: one\r\nX-Repeat: two\r\n\r\nraw"
            )
            await writer.drain()
            raw_response = await asyncio.wait_for(reader.read(), 5)
            writer.close()
            await writer.wait_closed()
            response_headers, response_body = raw_response.split(b"\r\n\r\n", 1)
            assert response_headers.startswith(b"HTTP/1.1 418")
            assert response_body == compressed
        finally:
            server.should_exit = True
            await asyncio.wait_for(task, 5)
            sock.close()
        assert len(capture.records) == 3
        assert all(record["outcome"] == "completed" for record in capture.records)
        assert all(record["response"] == compressed for record in capture.records)
        assert capture.closed

    try:
        asyncio.run(run())
    finally:
        upstream.shutdown()
        upstream.server_close()
        thread.join(timeout=2)
    assert seen == [
        ("/a%2Fb?x=1&x=2", b"raw-0", ["one", "two"], None),
        ("/a%2Fb?x=1&x=2", b"raw-1", ["one", "two"], None),
        ("//evil.invalid/a/../b?x=%2f", b"raw", ["one", "two"], None),
    ]
