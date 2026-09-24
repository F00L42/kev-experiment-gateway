"""Capture copies only: dated exchange records and separate process metadata."""

from __future__ import annotations

import base64
import json
import logging
import os
import tempfile
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote_plus
from zoneinfo import ZoneInfo

from . import __version__
from .config import GatewayConfig

SECRET_HEADERS = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "x-api-key",
        "api-key",
    }
)
SECRET_QUERY = frozenset({"api_key", "api-key", "key", "token", "access_token"})
RECORD_ALLOWANCE = 1024


class OutputRootError(OSError):
    """An actionable startup error containing only local storage information."""


def prepare_output_root(root: Path, bucket: str) -> None:
    """Create capture directories and verify actual writes before accepting HTTP."""
    for directory in (root, root / "runtime", root / "exchanges", root / "exchanges" / bucket):
        try:
            directory.mkdir(parents=True, exist_ok=True)
            # A permission-bit check alone misses ACLs, read-only mounts and full disks.
            with tempfile.TemporaryFile(dir=directory, prefix=".kev-write-") as probe:
                probe.write(b"\0")
                probe.flush()
                os.fsync(probe.fileno())
        except OSError as exc:
            raise OutputRootError(
                f"Cannot initialize output_root '{root}': directory '{directory}' "
                f"could not be created or written ({type(exc).__name__}: {exc.strerror or exc})"
            ) from exc


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def safe_headers(headers: list[tuple[bytes, bytes]]) -> list[list[str]]:
    result = []
    for raw_name, raw_value in headers:
        name = raw_name.decode("latin-1")
        secret = name.lower() in SECRET_HEADERS or "api-key" in name.lower()
        result.append([name, "[REDACTED]" if secret else raw_value.decode("latin-1")])
    return result


def safe_query(raw: bytes) -> str:
    # Preserve original ordering, repeated keys and encoding except secret values.
    parts = []
    for part in raw.decode("latin-1").split("&"):
        key, sep, value = part.partition("=")
        if unquote_plus(key).lower() in SECRET_QUERY:
            value = "[REDACTED]"
        parts.append(key + sep + value)
    return "&".join(parts)


def write_json(path: Path, value: dict) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def encode_body(raw: bytearray, headers: list[list[str]]) -> tuple[str, str]:
    compressed = any(k.lower() == "content-encoding" and v.lower() != "identity" for k, v in headers)
    if not compressed:
        try:
            return raw.decode("utf-8"), "utf8"
        except UnicodeDecodeError:
            pass
    return base64.b64encode(raw).decode("ascii"), "base64"


@dataclass
class CaptureContext:
    record: dict
    bucket: str
    start_ns: int
    reserved: int
    buffers: dict[str, bytearray] = field(
        default_factory=lambda: {"request": bytearray(), "response": bytearray()}
    )
    ended: set[str] = field(default_factory=set)
    finished: bool = False
    gap: bool = False


class Capture:
    """One process owns one file in each date bucket; disk work stays in a thread.

    The budget counts raw payload and serialized metadata estimates across active,
    queued AND currently-writing records. It is not an exact Python RSS limit;
    the single writer temporarily allocates a JSON/base64 representation too.
    """

    def __init__(self, config: GatewayConfig):
        self.config = config
        self.run_id = str(uuid.uuid4())
        self.root = config.output_root
        self.timezone = ZoneInfo(config.bucket_timezone)
        prepare_output_root(self.root, utc_now().astimezone(self.timezone).date().isoformat())
        self.runtime = self.root / "runtime" / self.run_id
        self.runtime.mkdir(parents=True, exist_ok=False)
        try:
            with (self.root / ".gitignore").open("x", encoding="utf-8") as stream:
                stream.write("*\n")
        except FileExistsError:
            pass
        self.log = logging.getLogger(f"kev_gateway.run.{self.run_id}")
        self.log.setLevel(logging.INFO)
        self.handler = logging.FileHandler(self.runtime / "service.log", encoding="utf-8")
        self.handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        self.log.addHandler(self.handler)
        self.started_at = iso_time(utc_now())
        self.condition = threading.Condition()
        self.queue: deque[CaptureContext] = deque()
        self.memory_bytes = 0
        self.pending_records = 0
        self.stopping = False
        self.failed = False
        self.touched: set[Path] = set()
        self.counts = dict(
            requests_seen=0,
            accepted=0,
            finished=0,
            written=0,
            incomplete=0,
            metadata_only=0,
            dropped_records=0,
            dropped_body_bytes=0,
            write_errors=0,
            memory_peak_bytes=0,
        )
        try:
            self._save_run()
        except BaseException:
            self.log.removeHandler(self.handler)
            self.handler.close()
            raise
        self.thread = threading.Thread(target=self._run, name="kev-capture-writer", daemon=True)
        self.thread.start()
        self.log.info("Capture started run_id=%s output=%s", self.run_id, self.root)

    def _save_run(self, ended_at: str | None = None) -> None:
        with self.condition:
            counts = dict(self.counts)
        clean = (
            ended_at is not None and counts["accepted"] == counts["finished"] and not counts["write_errors"]
        )
        write_json(
            self.runtime / "run.json",
            {
                "schema_version": 1,
                "run_id": self.run_id,
                "version": __version__,
                "started_at": self.started_at,
                "ended_at": ended_at,
                "clean_shutdown": clean,
                "bucket_timezone": self.config.bucket_timezone,
                "config": self.config.snapshot(),
                "counts": counts,
            },
        )

    def _io_failure(self, exc: Exception) -> None:
        with self.condition:
            self.counts["write_errors"] += 1
            self.failed = True
        self.log.error("Capture disk failure: %s; forwarding continues", type(exc).__name__)

    def begin(self, method: str, raw_path: bytes, query: bytes, headers: list[tuple[bytes, bytes]]):
        start_ns = time.perf_counter_ns()
        now = utc_now()
        path = raw_path.decode("ascii", errors="backslashreplace")
        full = "*" in self.config.capture_paths or path in self.config.capture_paths
        record = {
            "schema_version": 1,
            "id": str(uuid.uuid4()),
            "run_id": self.run_id,
            "started_at": iso_time(now),
            "duration_ms": None,
            "request": {
                "method": method,
                "path": path,
                "query": safe_query(query),
                "headers": safe_headers(headers),
                "body": None,
                "body_encoding": None,
                "body_bytes": 0,
                "body_complete": False,
            },
            "response": None,
            "response_source": None,
            "outcome": None,
            "capture_complete": False,
            "error": None,
            "capture_mode": "full" if full else "metadata",
            "capture_reason": None if full else "metadata_only",
        }
        reserved = len(json.dumps(record, ensure_ascii=False).encode("utf-8")) + RECORD_ALLOWANCE
        with self.condition:
            self.counts["requests_seen"] += 1
            if (
                self.stopping
                or self.failed
                or self.pending_records >= self.config.max_pending_records
                or self.memory_bytes + reserved > self.config.queue_bytes
            ):
                self.counts["dropped_records"] += 1
                # Rate-limit overload messages while keeping exact counters.
                if self.counts["dropped_records"] == 1 or self.counts["dropped_records"] % 100 == 0:
                    self.log.warning(
                        "Capture skipped requests; dropped_records=%d", self.counts["dropped_records"]
                    )
                return None
            self.memory_bytes += reserved
            self.pending_records += 1
            self.counts["accepted"] += 1
            self.counts["metadata_only"] += not full
            self.counts["memory_peak_bytes"] = max(self.counts["memory_peak_bytes"], self.memory_bytes)
        return CaptureContext(record, now.astimezone(self.timezone).date().isoformat(), start_ns, reserved)

    def _mark_gap(self, ctx: CaptureContext, reason: str) -> None:
        ctx.gap = True
        if ctx.record["capture_reason"] is None:
            ctx.record["capture_reason"] = reason

    def body(self, ctx: CaptureContext | None, direction: str, data: bytes) -> None:
        if ctx is None or ctx.finished:
            return
        side = ctx.record[direction]
        if side is None:
            raise ValueError("response_start must precede response body")
        side["body_bytes"] += len(data)
        if ctx.record["capture_mode"] != "full":
            return
        with self.condition:
            if ctx.gap or self.failed:
                self._mark_gap(ctx, "writer_failed" if self.failed else "capture_limit")
                self.counts["dropped_body_bytes"] += len(data)
                return
            if len(ctx.buffers[direction]) + len(data) > self.config.max_body_bytes:
                self._mark_gap(ctx, "body_limit")
            elif self.memory_bytes + len(data) > self.config.queue_bytes:
                self._mark_gap(ctx, "memory_limit")
            if ctx.gap:
                self.counts["dropped_body_bytes"] += len(data)
                return
            ctx.buffers[direction].extend(data)
            ctx.reserved += len(data)
            self.memory_bytes += len(data)
            self.counts["memory_peak_bytes"] = max(self.counts["memory_peak_bytes"], self.memory_bytes)

    def body_end(self, ctx: CaptureContext | None, direction: str) -> None:
        if ctx is not None and not ctx.finished:
            ctx.ended.add(direction)

    def response_start(
        self, ctx: CaptureContext | None, status: int, headers: list[tuple[bytes, bytes]], source="upstream"
    ) -> None:
        if ctx is None or ctx.finished:
            return
        safe = safe_headers(headers)
        extra = len(json.dumps(safe, ensure_ascii=False).encode("utf-8"))
        with self.condition:
            if self.memory_bytes + extra > self.config.queue_bytes:
                safe = []
                self._mark_gap(ctx, "metadata_limit")
                extra = 0
            self.memory_bytes += extra
            ctx.reserved += extra
            self.counts["memory_peak_bytes"] = max(self.counts["memory_peak_bytes"], self.memory_bytes)
        ctx.record["response"] = {
            "status": status,
            "headers": safe,
            "body": None,
            "body_encoding": None,
            "body_bytes": 0,
            "body_complete": False,
        }
        ctx.record["response_source"] = source

    def finish(self, ctx: CaptureContext | None, outcome: str, error: dict | None = None) -> None:
        if ctx is None or ctx.finished:
            return
        ctx.finished = True
        ctx.record["duration_ms"] = (time.perf_counter_ns() - ctx.start_ns) / 1_000_000
        ctx.record["outcome"] = outcome
        ctx.record["error"] = error
        with self.condition:
            self.counts["finished"] += 1
            if self.stopping:
                self.counts["dropped_records"] += 1
                self.memory_bytes -= ctx.reserved
                self.pending_records -= 1
            else:
                self.queue.append(ctx)
                self.condition.notify()

    def _serialize(self, ctx: CaptureContext) -> dict:
        for direction in ("request", "response"):
            side = ctx.record[direction]
            if side is None:
                continue
            if ctx.record["capture_mode"] == "full":
                side["body"], side["body_encoding"] = encode_body(ctx.buffers[direction], side["headers"])
                side["body_complete"] = (
                    direction in ctx.ended and len(ctx.buffers[direction]) == side["body_bytes"]
                )
        ctx.record["capture_complete"] = (
            ctx.record["capture_mode"] == "full"
            and not ctx.gap
            and ctx.record["request"]["body_complete"]
            and ctx.record["response"] is not None
            and ctx.record["response"]["body_complete"]
        )
        if not ctx.record["capture_complete"] and ctx.record["capture_reason"] is None:
            ctx.record["capture_reason"] = "body_incomplete"
        return ctx.record

    def _append(self, ctx: CaptureContext, record: dict) -> None:
        path = self.root / "exchanges" / ctx.bucket / f"{self.run_id}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        data = (json.dumps(record, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n").encode(
            "utf-8"
        )
        with path.open("ab") as stream:
            stream.write(data)
        self.touched.add(path)

    def _run(self) -> None:
        last_status = time.monotonic()
        while True:
            with self.condition:
                if not self.queue and not self.stopping:
                    self.condition.wait(timeout=1)
                if not self.queue and self.stopping:
                    break
                ctx = self.queue.popleft() if self.queue else None
            if ctx is not None:
                try:
                    if self.failed:
                        with self.condition:
                            self.counts["dropped_records"] += 1
                    else:
                        record = self._serialize(ctx)
                        self._append(ctx, record)
                        with self.condition:
                            self.counts["written"] += 1
                            self.counts["incomplete"] += not record["capture_complete"]
                except Exception as exc:
                    self._io_failure(exc)
                    with self.condition:
                        self.counts["dropped_records"] += 1
                finally:
                    reservation = ctx.reserved
                    # Do not retain the previous payload through the next idle wait.
                    ctx = None
                    record = None
                    with self.condition:
                        self.memory_bytes -= reservation
                        self.pending_records -= 1
            if time.monotonic() - last_status >= 1:
                try:
                    self._save_run()
                except OSError as exc:
                    self._io_failure(exc)
                last_status = time.monotonic()
        for path in self.touched:
            try:
                with path.open("ab") as stream:
                    stream.flush()
                    os.fsync(stream.fileno())
            except OSError as exc:
                self._io_failure(exc)
        try:
            self._save_run(ended_at=iso_time(utc_now()))
        except OSError as exc:
            self._io_failure(exc)

    def close(self) -> None:
        with self.condition:
            self.stopping = True
            self.condition.notify_all()
        self.thread.join()
        self.log.info("Capture stopped counts=%s", self.counts)
        self.log.removeHandler(self.handler)
        self.handler.close()
