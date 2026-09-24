import base64
import gzip
import json
import threading
import time
import weakref
from datetime import UTC, datetime

import pytest

from kev_gateway.capture import Capture
from kev_gateway.config import GatewayConfig, load_config


def read_records(root):
    return [
        json.loads(line)
        for file in sorted((root / "exchanges").rglob("*.jsonl"))
        for line in file.read_text(encoding="utf-8").splitlines()
    ]


def exchange(cap, request=b"{}", response=b"{}", path=b"/v1/systemone", headers=()):
    ctx = cap.begin("POST", path, b"", list(headers))
    cap.body(ctx, "request", request)
    cap.body_end(ctx, "request")
    cap.response_start(ctx, 200, [])
    cap.body(ctx, "response", response)
    cap.body_end(ctx, "response")
    cap.finish(ctx, "completed")
    return ctx


def test_midnight_bucket_is_frozen_at_arrival_and_runtime_separate(tmp_path, monkeypatch):
    now = datetime(2026, 9, 24, 15, 59, 59, tzinfo=UTC)
    monkeypatch.setattr("kev_gateway.capture.utc_now", lambda: now)
    cap = Capture(GatewayConfig("http://localhost:8009", output_root=tmp_path))
    ctx = cap.begin("POST", b"/v1/systemone", b"", [])
    cap.body(ctx, "request", '{"state":"中文"}'.encode())
    cap.body_end(ctx, "request")
    now = datetime(2026, 9, 24, 16, 0, 2, tzinfo=UTC)
    cap.response_start(ctx, 200, [])
    cap.body(ctx, "response", b'{ "answers": {} }')
    cap.body_end(ctx, "response")
    cap.finish(ctx, "completed")
    exchange(cap)
    cap.close()
    assert (tmp_path / "exchanges" / "2026-09-24" / f"{cap.run_id}.jsonl").exists()
    assert (tmp_path / "exchanges" / "2026-09-25" / f"{cap.run_id}.jsonl").exists()
    records = read_records(tmp_path)
    assert records[0]["request"]["body"] == '{"state":"中文"}'
    assert records[0]["response"]["body"] == '{ "answers": {} }'
    assert all(r["capture_complete"] for r in records)
    assert all("config" not in r and "upstream_url" not in r for r in records)
    run = json.loads((cap.runtime / "run.json").read_text())
    assert run["clean_shutdown"] and run["counts"]["written"] == 2
    assert run["config"]["upstream_url"] == "http://localhost:8009"
    assert cap.memory_bytes == cap.pending_records == 0


def test_binary_compression_and_duplicate_redacted_headers(tmp_path):
    cap = Capture(GatewayConfig("http://localhost", output_root=tmp_path))
    raw_headers = [(b"Authorization", b"Bearer secret"), (b"X-Tag", b"a"), (b"X-Tag", b"b")]
    query = b"a=%2B&a=2&access_token=private&x=1+2"
    ctx = cap.begin("POST", b"/v1/systemone", query, raw_headers)
    cap.body(ctx, "request", b"\xff\xfe")
    cap.body_end(ctx, "request")
    cap.response_start(ctx, 422, [(b"content-encoding", b"gzip"), (b"set-cookie", b"secret")])
    body = gzip.compress(b'{"error":"bad input"}')
    cap.body(ctx, "response", body)
    cap.body_end(ctx, "response")
    cap.finish(ctx, "completed")
    cap.close()
    record = read_records(tmp_path)[0]
    assert record["request"]["headers"] == [["Authorization", "[REDACTED]"], ["X-Tag", "a"], ["X-Tag", "b"]]
    assert record["request"]["query"] == "a=%2B&a=2&access_token=[REDACTED]&x=1+2"
    assert base64.b64decode(record["request"]["body"]) == b"\xff\xfe"
    assert base64.b64decode(record["response"]["body"]) == body
    assert record["response"]["status"] == 422 and record["capture_complete"]
    assert raw_headers[0][1] == b"Bearer secret"


def test_body_limit_marks_gap_without_rejecting_request(tmp_path):
    cap = Capture(GatewayConfig("http://localhost", output_root=tmp_path, max_body_bytes=4))
    ctx = cap.begin("POST", b"/v1/systemone", b"", [])
    cap.body(ctx, "request", b"1234")
    cap.body(ctx, "request", b"5678")
    cap.body_end(ctx, "request")
    cap.response_start(ctx, 200, [])
    cap.body(ctx, "response", b"ok")
    cap.body_end(ctx, "response")
    cap.finish(ctx, "completed")
    cap.close()
    record = read_records(tmp_path)[0]
    assert record["request"]["body"] == "1234"
    assert record["request"]["body_bytes"] == 8
    assert not record["request"]["body_complete"]
    assert not record["capture_complete"] and record["capture_reason"] == "body_limit"
    assert cap.counts["dropped_body_bytes"] == 6


def test_budget_covers_records_currently_in_writer(tmp_path, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    cap = Capture(GatewayConfig("http://localhost", output_root=tmp_path, max_pending_records=1))
    append = cap._append

    def slow_append(ctx, record):
        entered.set()
        assert release.wait(5)
        append(ctx, record)

    monkeypatch.setattr(cap, "_append", slow_append)
    try:
        exchange(cap)
        assert entered.wait(5)
        assert cap.memory_bytes > 0 and cap.pending_records == 1
        assert exchange(cap) is None
        assert cap.counts["dropped_records"] == 1
    finally:
        release.set()
        cap.close()
    assert len(read_records(tmp_path)) == 1
    assert cap.memory_bytes == 0


def test_disk_failure_is_explicit_and_does_not_raise_in_capture_caller(tmp_path, monkeypatch):
    cap = Capture(GatewayConfig("http://localhost", output_root=tmp_path))

    def fail(ctx, record):
        raise OSError("disk full")

    monkeypatch.setattr(cap, "_append", fail)
    exchange(cap)
    cap.close()
    run = json.loads((cap.runtime / "run.json").read_text())
    assert run["counts"]["write_errors"] == 1
    assert run["counts"]["dropped_records"] == 1
    assert not run["clean_shutdown"] and run["ended_at"]
    assert cap.memory_bytes == 0


def test_metadata_paths_and_interrupted_bodies_are_not_claimed_complete(tmp_path):
    cap = Capture(GatewayConfig("http://localhost", output_root=tmp_path))
    exchange(cap, path=b"/metrics", response=b"metric 99")
    ctx = cap.begin("POST", b"/v1/systemone", b"", [])
    cap.body(ctx, "request", b'{"incomplete":')
    cap.finish(ctx, "client_disconnected")
    cap.close()
    meta, partial = read_records(tmp_path)
    assert meta["capture_mode"] == "metadata" and not meta["capture_complete"]
    assert meta["response"]["body"] is None and meta["response"]["body_bytes"] == 9
    assert partial["request"]["body"] == '{"incomplete":'
    assert partial["response"] is None and not partial["capture_complete"]


def test_multiple_process_instances_write_distinct_files(tmp_path):
    config = GatewayConfig("http://localhost", output_root=tmp_path)
    first, second = Capture(config), Capture(config)
    try:
        exchange(first)
        exchange(second)
    finally:
        first.close()
        second.close()
    assert first.run_id != second.run_id
    assert len(list((tmp_path / "exchanges").rglob("*.jsonl"))) == 2
    assert len(read_records(tmp_path)) == 2


def test_idle_writer_releases_completed_payload_before_reusing_budget(tmp_path):
    class WeakRecord(dict):
        pass

    cap = Capture(GatewayConfig("http://localhost", output_root=tmp_path))
    ctx = cap.begin("POST", b"/v1/systemone", b"", [])
    ctx.record = WeakRecord(ctx.record)
    record_ref = weakref.ref(ctx.record)
    cap.body(ctx, "request", b"x" * 1024 * 1024)
    cap.body_end(ctx, "request")
    cap.response_start(ctx, 200, [])
    cap.body(ctx, "response", b"ok")
    cap.body_end(ctx, "response")
    cap.finish(ctx, "completed")
    del ctx
    try:
        deadline = time.monotonic() + 5
        while cap.pending_records and time.monotonic() < deadline:
            time.sleep(0.01)
        assert cap.pending_records == 0 and cap.thread.is_alive()
        assert record_ref() is None
    finally:
        cap.close()


def test_config_paths_and_invalid_values(tmp_path):
    source = tmp_path / "config.toml"
    source.write_text('upstream_url="http://localhost:8009"\noutput_root="data"\n')
    config = load_config(source)
    assert config.output_root == tmp_path / "data" and config.read_timeout is None
    for url in ("http://user:secret@host", "http://host/v1", "http://host?token=secret", "ftp://host"):
        with pytest.raises(ValueError):
            GatewayConfig(url)
    for kwargs in (
        {"queue_bytes": 0},
        {"read_timeout": float("nan")},
        {"port": True},
        {"capture_paths": "*"},
    ):
        with pytest.raises(ValueError):
            GatewayConfig("http://localhost", **kwargs)
    with pytest.raises(ValueError, match="Unknown"):
        load_config(overrides={"upstream_url": "http://localhost", "typo": 1})
