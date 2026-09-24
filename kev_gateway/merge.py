"""Offline, bounded-memory exports of immutable capture-file snapshots."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import UUID


def _date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
        if parsed.isoformat() != value:
            raise ValueError
        return parsed
    except ValueError as exc:
        raise ValueError(f"Invalid ISO bucket date: {value!r}") from exc


def _uuid(value: Any, field: str) -> str:
    try:
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError
        return value
    except (ValueError, AttributeError) as exc:
        raise ValueError(f"Invalid {field}: {value!r}") from exc


def _reject_constant(value: str) -> None:
    raise ValueError(f"Non-JSON numeric constant: {value}")


def _loads(value: str | bytes) -> Any:
    return json.loads(value, parse_constant=_reject_constant)


def _validate_record(record: Any) -> str:
    if not isinstance(record, dict) or type(record.get("schema_version")) is not int:
        raise ValueError("Record must be an object with integer schema_version")
    if record["schema_version"] != 1:
        raise ValueError(f"Unsupported schema_version: {record['schema_version']!r}")
    _uuid(record.get("id"), "id")
    _uuid(record.get("run_id"), "run_id")
    try:
        stamp = datetime.fromisoformat(record["started_at"].replace("Z", "+00:00"))
        if stamp.tzinfo is None or stamp.utcoffset().total_seconds() != 0:
            raise ValueError
    except (KeyError, ValueError, TypeError, AttributeError) as exc:
        raise ValueError("started_at must be a valid UTC ISO timestamp") from exc
    return stamp.astimezone(UTC).isoformat(timespec="microseconds")


def _run_summary(root: Path, run_id: str) -> dict[str, Any]:
    metadata = root / "runtime" / run_id / "run.json"
    summary: dict[str, Any] = {
        "run_id": run_id,
        "metadata_status": "missing",
        "completeness": "not_established",
    }
    if not metadata.exists():
        return summary
    try:
        data = _loads(metadata.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or data.get("run_id") != run_id or data.get("schema_version") != 1:
            raise ValueError("run.json is not a matching run object")
        timezone = data.get("bucket_timezone")
        if timezone is not None and not isinstance(timezone, str):
            raise ValueError("run.json bucket_timezone must be a string")
        summary.update(
            started_at=data.get("started_at"),
            ended_at=data.get("ended_at"),
            clean_shutdown=data.get("clean_shutdown") is True,
            counts=data.get("counts"),
            bucket_timezone=data.get("bucket_timezone"),
        )
        if data.get("clean_shutdown") is True:
            summary["metadata_status"] = "clean_shutdown"
        elif data.get("ended_at"):
            summary["metadata_status"] = "unclean_shutdown"
        else:
            # A crashed process and a live process can have identical metadata.
            summary["metadata_status"] = "running_or_unclean"
    except (OSError, ValueError, UnicodeError) as exc:
        summary.update(metadata_status="unreadable", error=str(exc))
    return summary


def _is_within(path: Path, parent: Path) -> bool:
    return path == parent or parent in path.parents


def merge_capture(
    output_root: Path,
    destination: Path,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
    run_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Export selected date buckets (inclusive endpoints) to a NEW directory.

    All input byte boundaries are fixed before any file is read. Only complete
    newline-terminated rows are consumed; in-progress tails are reported. SQLite
    holds the sort/dedup index on disk, so memory scales with one capture row.
    A snapshot never asserts that an experiment or date bucket is complete.
    """
    root = Path(output_root).resolve()
    destination = Path(destination).resolve()
    if destination.exists():
        raise FileExistsError(f"Export destination must be new: {destination}")
    for source_area in (root / "exchanges", root / "runtime"):
        if _is_within(destination, source_area):
            raise ValueError("Export destination cannot be inside exchanges or runtime")
    lower = _date(date_from) if date_from is not None else None
    upper = _date(date_to) if date_to is not None else None
    if lower is not None and upper is not None and lower > upper:
        raise ValueError("date_from must not be after date_to")
    selected_runs = {_uuid(run, "run_id filter") for run in run_ids} if run_ids is not None else None
    source_root = root / "exchanges"
    if not source_root.is_dir():
        raise FileNotFoundError(f"Capture directory does not exist: {source_root}")

    snapshots: list[tuple[Path, os.stat_result]] = []
    for bucket in sorted(source_root.iterdir()):
        if not bucket.is_dir():
            continue
        bucket_date = _date(bucket.name)
        if (lower is not None and bucket_date < lower) or (upper is not None and bucket_date > upper):
            continue
        for source in sorted(bucket.glob("*.jsonl")):
            if not _is_within(source.resolve(), source_root.resolve()):
                raise ValueError(f"Capture source escapes exchanges: {source}")
            run_id = _uuid(source.stem, "filename run_id")
            if selected_runs is not None and run_id not in selected_runs:
                continue
            snapshots.append((source, source.stat()))

    snapshot_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    run_summaries = [_run_summary(root, run) for run in sorted({p.stem for p, _ in snapshots})]
    known_timezones = {run["bucket_timezone"] for run in run_summaries if run.get("bucket_timezone")}
    if len(known_timezones) > 1:
        raise ValueError(f"Selected runs use mixed bucket timezones: {sorted(known_timezones)}")
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "status": "snapshot",
        "completeness": "not_established",
        "known_bucket_timezone": next(iter(known_timezones), None),
        "snapshot_at": snapshot_at,
        "source_root": str(root),
        "selection": {"date_from": date_from, "date_to": date_to, "inclusive": True, "run_ids": run_ids},
        "sort": ["started_at_utc", "id"],
        "inputs": [],
        "runs": run_summaries,
        "input_records": 0,
        "output_records": 0,
        "duplicates_removed": 0,
        "trailing_bytes_skipped": 0,
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    # TemporaryDirectory cleans only the directory that this invocation creates.
    with tempfile.TemporaryDirectory(prefix=f".{destination.name}-", dir=destination.parent) as staging_name:
        staging = Path(staging_name)
        database = staging / "sort.sqlite3"
        connection = sqlite3.connect(database)
        try:
            connection.execute("PRAGMA temp_store=FILE")
            connection.execute("PRAGMA cache_size=-4096")
            connection.execute("CREATE TABLE records (id TEXT PRIMARY KEY, stamp TEXT, payload TEXT)")
            for source, initial_stat in snapshots:
                input_summary: dict[str, Any] = {
                    "path": source.relative_to(root).as_posix(),
                    "snapshot_bytes": initial_stat.st_size,
                    "records": 0,
                    "trailing_bytes_skipped": 0,
                }
                digest = hashlib.sha256()
                with source.open("rb") as stream:
                    opened_stat = os.fstat(stream.fileno())
                    if (opened_stat.st_dev, opened_stat.st_ino) != (initial_stat.st_dev, initial_stat.st_ino):
                        raise ValueError(f"Capture file was replaced during snapshot: {source}")
                    if opened_stat.st_size < initial_stat.st_size:
                        raise ValueError(f"Capture file shrank during snapshot: {source}")
                    remaining = initial_stat.st_size
                    line_number = 0
                    while remaining:
                        line = stream.readline(remaining)
                        if not line:
                            raise ValueError(f"Capture file shrank during snapshot: {source}")
                        remaining -= len(line)
                        digest.update(line)
                        line_number += 1
                        if not line.endswith(b"\n"):
                            input_summary["trailing_bytes_skipped"] = len(line)
                            manifest["trailing_bytes_skipped"] += len(line)
                            continue
                        try:
                            record = _loads(line)
                            stamp = _validate_record(record)
                            if record["run_id"] != source.stem:
                                raise ValueError("Record run_id does not match filename")
                            payload = json.dumps(record, sort_keys=True, ensure_ascii=False, allow_nan=False)
                            previous = connection.execute(
                                "SELECT payload FROM records WHERE id = ?", (record["id"],)
                            ).fetchone()
                            if previous is not None:
                                if previous[0] != payload:
                                    raise ValueError(f"Conflicting records with the same id: {record['id']}")
                                manifest["duplicates_removed"] += 1
                            else:
                                connection.execute(
                                    "INSERT INTO records VALUES (?, ?, ?)", (record["id"], stamp, payload)
                                )
                                manifest["output_records"] += 1
                        except (ValueError, UnicodeError) as exc:
                            raise ValueError(f"{source}:{line_number}: {exc}") from exc
                        manifest["input_records"] += 1
                        input_summary["records"] += 1
                    final_stat = os.fstat(stream.fileno())
                    if final_stat.st_size < initial_stat.st_size:
                        raise ValueError(f"Capture file shrank during snapshot: {source}")
                    if (
                        final_stat.st_size == initial_stat.st_size
                        and final_stat.st_mtime_ns != initial_stat.st_mtime_ns
                    ):
                        raise ValueError(f"Capture file changed during snapshot: {source}")
                    current_stat = source.stat()
                    if (current_stat.st_dev, current_stat.st_ino) != (
                        initial_stat.st_dev,
                        initial_stat.st_ino,
                    ):
                        raise ValueError(f"Capture file was replaced during snapshot: {source}")
                    input_summary["observed_bytes_after_read"] = final_stat.st_size
                input_summary["snapshot_sha256"] = digest.hexdigest()
                manifest["inputs"].append(input_summary)
                connection.commit()
            connection.execute("CREATE INDEX records_order ON records (stamp, id)")
            with (staging / "exchanges.jsonl").open("w", encoding="utf-8", newline="\n") as target:
                for (payload,) in connection.execute("SELECT payload FROM records ORDER BY stamp, id"):
                    target.write(payload + "\n")
            manifest["exported_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
            (staging / "merge.json").write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8"
            )
        finally:
            connection.close()
        database.unlink()
        # Publish into a new directory, with merge.json last as the success marker.
        # Creating the destination first guards even an empty pre-existing dir on POSIX.
        destination.mkdir(exist_ok=False)
        try:
            for artifact in ("exchanges.jsonl", "merge.json"):
                os.replace(staging / artifact, destination / artifact)
        except BaseException:
            # No recursive cleanup: only the artifacts created by this invocation.
            for artifact in ("exchanges.jsonl", "merge.json"):
                (destination / artifact).unlink(missing_ok=True)
            destination.rmdir()
            raise
    return manifest
