import base64
import gzip
import json
import zlib
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest

from kev_gateway.inspect import find_record, inspect_record
from kev_gateway.merge import merge_capture


def message(payload, *, response=False):
    raw = json.dumps(payload, ensure_ascii=False)
    fields = {"status": 200} if response else {"method": "POST", "path": "/v1/systemone", "query": ""}
    return {
        **fields,
        "headers": [["content-type", "application/json"]],
        "body": raw,
        "body_encoding": "utf8",
        "body_bytes": len(raw.encode()),
        "body_complete": True,
    }


def record(run_id=None, *, started_at="2026-09-24T12:00:00Z"):
    return {
        "schema_version": 1,
        "id": str(uuid4()),
        "run_id": run_id or str(uuid4()),
        "started_at": started_at,
        "duration_ms": 2,
        "request": message({"questions": {"yes": {"type": "noul"}}, "state": "中文"}),
        "response": message({"answers": {"yes": {"type": "noul", "noul": 0.8}}}, response=True),
        "response_source": "upstream",
        "outcome": "completed",
        "capture_complete": True,
        "error": None,
        "capture_mode": "full",
        "capture_reason": None,
    }


def write_records(root: Path, bucket: str, rows: list[dict], *, tail=b"") -> Path:
    path = root / "exchanges" / bucket / f"{rows[0]['run_id']}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"".join(json.dumps(row).encode() + b"\n" for row in rows) + tail)
    return path


def read_records(destination):
    return [
        json.loads(line)
        for line in (destination / "exchanges.jsonl").read_text(encoding="utf-8").splitlines()
    ]


def test_merge_date_buckets_are_arrival_dates_and_sort_across_runs(tmp_path):
    # 23:59:59 Shanghai is still in the 24th bucket even when it completes on the 25th.
    late = record(started_at="2026-09-24T15:59:59Z")
    late["duration_ms"] = 120_000
    early = record(started_at="2026-09-24T01:00:00Z")
    tomorrow = record(late["run_id"], started_at="2026-09-24T16:00:01Z")
    sources = [
        write_records(tmp_path, "2026-09-24", [late]),
        write_records(tmp_path, "2026-09-24", [early]),
        write_records(tmp_path, "2026-09-25", [tomorrow]),
    ]
    before = {source: source.read_bytes() for source in sources}
    export = tmp_path / "exports" / "day"
    manifest = merge_capture(tmp_path, export, date_from="2026-09-24", date_to="2026-09-24")
    assert read_records(export) == [early, late]
    assert manifest["status"] == "snapshot"
    assert manifest["output_records"] == 2
    assert all(run["metadata_status"] == "missing" for run in manifest["runs"])
    assert all(source.read_bytes() == before[source] for source in sources)
    all_export = tmp_path / "exports" / "all"
    merge_capture(tmp_path, all_export)
    assert read_records(all_export) == [early, late, tomorrow]


def test_merge_stable_tie_break_and_semantic_dedup(tmp_path):
    first = record()
    second = record(first["run_id"])
    # Identical request bodies do not cause deduplication of separate exchanges.
    path = write_records(tmp_path, "2026-09-24", [second, first])
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(first, sort_keys=True, separators=(",", ":")) + "\n")
    export = tmp_path / "export"
    manifest = merge_capture(tmp_path, export)
    assert read_records(export) == sorted([first, second], key=lambda item: item["id"])
    assert manifest["input_records"] == 3
    assert manifest["duplicates_removed"] == 1


def test_merge_conflicts_fail_without_publishing_or_removing_existing_data(tmp_path):
    first = record()
    conflicting = deepcopy(first)
    conflicting["duration_ms"] = 999
    path = write_records(tmp_path, "2026-09-24", [first, conflicting])
    before = path.read_bytes()
    destination = tmp_path / "export"
    with pytest.raises(ValueError, match="Conflicting.*same id"):
        merge_capture(tmp_path, destination)
    assert not destination.exists()
    assert path.read_bytes() == before
    assert not list(tmp_path.glob(".export-*"))


def test_merge_live_partial_tail_and_unclean_runtime_are_explicit(tmp_path):
    item = record()
    tail = b'{"schema_version":1,"id":"in progress'
    write_records(tmp_path, "2026-09-24", [item], tail=tail)
    metadata = tmp_path / "runtime" / item["run_id"] / "run.json"
    metadata.parent.mkdir(parents=True)
    metadata.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "run_id": item["run_id"],
                "started_at": item["started_at"],
                "ended_at": None,
                "clean_shutdown": False,
                "bucket_timezone": "Asia/Shanghai",
                "counts": {},
            }
        )
    )
    export = tmp_path / "export"
    manifest = merge_capture(tmp_path, export)
    assert read_records(export) == [item]
    assert manifest["trailing_bytes_skipped"] == len(tail)
    assert manifest["inputs"][0]["trailing_bytes_skipped"] == len(tail)
    assert manifest["runs"][0]["metadata_status"] == "running_or_unclean"


def test_snapshot_reads_fixed_boundary_even_when_writer_appends(tmp_path, monkeypatch):
    first = record()
    extra = record(first["run_id"])
    source = write_records(tmp_path, "2026-09-24", [first])
    initial = source.stat().st_size
    original_open = Path.open
    appended = False

    def append_on_open(path, *args, **kwargs):
        nonlocal appended
        if path == source and args and args[0] == "rb" and not appended:
            appended = True
            with original_open(source, "ab") as writer:
                writer.write(json.dumps(extra).encode() + b"\n")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", append_on_open)
    export = tmp_path / "export"
    manifest = merge_capture(tmp_path, export)
    assert read_records(export) == [first]
    assert manifest["inputs"][0]["snapshot_bytes"] == initial
    assert manifest["inputs"][0]["observed_bytes_after_read"] > initial


@pytest.mark.parametrize("mutation", ["malformed", "schema", "id", "time", "run_id"])
def test_bad_complete_rows_name_source_and_line(tmp_path, mutation):
    item = record()
    source = write_records(tmp_path, "2026-09-24", [item])
    if mutation == "malformed":
        source.write_bytes(source.read_bytes() + b"{broken}\n")
    else:
        field, value = {
            "schema": ("schema_version", 2),
            "id": ("id", "bad"),
            "time": ("started_at", "2026-09-24T12:00:00"),
            "run_id": ("run_id", str(uuid4())),
        }[mutation]
        item[field] = value
        source.write_text(json.dumps(item) + "\n", encoding="utf-8")
    with pytest.raises(ValueError) as caught:
        merge_capture(tmp_path, tmp_path / "export")
    assert str(source) in str(caught.value)
    assert ":2:" in str(caught.value) if mutation == "malformed" else ":1:" in str(caught.value)


def test_date_filters_run_filters_and_destination_guards(tmp_path):
    first = record()
    other = record()
    write_records(tmp_path, "2026-09-24", [first])
    write_records(tmp_path, "2026-09-24", [other])
    export = tmp_path / "export"
    merge_capture(tmp_path, export, run_ids=[first["run_id"]])
    assert read_records(export) == [first]
    with pytest.raises(FileExistsError):
        merge_capture(tmp_path, export)
    with pytest.raises(ValueError, match="cannot be inside"):
        merge_capture(tmp_path, tmp_path / "exchanges" / "export")
    with pytest.raises(ValueError, match="Invalid ISO"):
        merge_capture(tmp_path, tmp_path / "bad", date_from="20260924")
    with pytest.raises(ValueError, match="must not be after"):
        merge_capture(tmp_path, tmp_path / "bad", date_from="2026-09-25", date_to="2026-09-24")
    (tmp_path / "exchanges" / "not-a-date").mkdir()
    with pytest.raises(ValueError, match="Invalid ISO"):
        merge_capture(tmp_path, tmp_path / "bad")


def test_inspect_systemone_preserves_three_answer_types_and_missing(tmp_path):
    item = record()
    item["request"] = message(
        {
            "state": "中文",
            "questions": {
                "yes": {"type": "noul"},
                "which": {"type": "choice"},
                "rating": {"type": "score"},
                "missing": {"type": "score"},
            },
        }
    )
    answers = {
        "yes": {"type": "noul", "noul": 0},
        "which": {"type": "choice", "choice": "a", "confidence": 0.7, "probabilities": {"a": 0.85}},
        "rating": {"type": "score", "score": 2.5, "legend": {"2": "good"}, "probabilities": {"2": 0.5}},
        "extra": {"type": "noul", "noul": 1},
    }
    item["response"] = message(
        {"answers": answers, "usage": {"input_tokens": 15}, "latency_ms": 2}, response=True
    )
    source = write_records(tmp_path, "2026-09-24", [item])
    assert find_record(tmp_path, item["id"]) == item
    assert find_record(source, item["id"]) == item
    view = inspect_record(item)
    systemone = view["systemone"]
    assert systemone["missing_answers"] == ["missing"]
    assert systemone["extra_answers"] == ["extra"]
    entries = {entry["question_id"]: entry for entry in systemone["answers"]}
    assert entries["missing"]["answer"] is None
    assert entries["missing"]["present"] is False
    for key, value in answers.items():
        assert entries[key]["answer"] == value
    assert entries["yes"]["answer"]["noul"] == 0
    assert systemone["usage"] == {"input_tokens": 15}


@pytest.mark.parametrize("encoding", ["gzip", "deflate", "raw-deflate"])
def test_inspect_compressed_body_is_bounded_and_decoded_offline(encoding):
    item = record()
    raw = item["response"]["body"].encode()
    if encoding == "gzip":
        wire = gzip.compress(raw)
    elif encoding == "deflate":
        wire = zlib.compress(raw)
    else:
        wire = zlib.compress(raw)[2:-4]
    item["response"].update(
        body=base64.b64encode(wire).decode(),
        body_encoding="base64",
        body_bytes=len(wire),
        headers=[["Content-Encoding", "gzip" if encoding == "gzip" else "deflate"]],
    )
    assert inspect_record(item)["response"]["json"] == json.loads(raw)
    item["response"]["body"] = base64.b64encode(wire[:-2]).decode()
    assert inspect_record(item)["response"]["decode_status"] == "invalid"


def test_inspect_rejects_bombs_incomplete_and_invalid_bodies():
    item = record()
    bomb = gzip.compress(json.dumps({"answers": {}, "padding": "x" * 100_000}).encode())
    item["response"].update(
        body=base64.b64encode(bomb).decode(),
        body_encoding="base64",
        headers=[["content-encoding", "gzip"]],
    )
    assert inspect_record(item, max_decoded_bytes=1000)["response"]["decode_status"] == "too_large"
    item["response"]["body_complete"] = False
    view = inspect_record(item)
    assert view["response"]["decode_status"] == "incomplete"
    assert view["systemone"]["answers_status"] == "missing_or_invalid"
    assert view["systemone"]["answers"][0]["answer"] is None
    item["response"] = message({}, response=True)
    item["response"]["body"] = "not JSON"
    assert inspect_record(item)["response"]["decode_status"] == "invalid"
    item["response"]["body_encoding"] = "base64"
    assert inspect_record(item)["response"]["decode_status"] == "invalid"


def test_find_record_reports_missing_conflict_and_skips_partial_tail(tmp_path):
    item = record()
    source = write_records(tmp_path, "2026-09-24", [item], tail=b'{"partial":')
    assert find_record(source, item["id"]) == item
    with pytest.raises(KeyError, match="not found"):
        find_record(source, str(uuid4()))
    changed = deepcopy(item)
    changed["outcome"] = "upstream_error"
    write_records(tmp_path, "2026-09-24", [item, changed])
    with pytest.raises(ValueError, match="Conflicting"):
        find_record(source, item["id"])


def test_real_capture_round_trip_to_merge_and_inspection(tmp_path):
    from kev_gateway.capture import Capture
    from kev_gateway.config import GatewayConfig

    capture = Capture(GatewayConfig(upstream_url="http://127.0.0.1:8000", output_root=tmp_path))
    try:
        ctx = capture.begin("POST", b"/v1/systemone", b"", [(b"content-type", b"application/json")])
        assert ctx is not None
        capture.body(
            ctx,
            "request",
            json.dumps(
                {
                    "model": "kev",
                    "state": "example",
                    "questions": {"yes": {"type": "noul"}},
                }
            ).encode(),
        )
        capture.body_end(ctx, "request")
        capture.response_start(ctx, 200, [(b"content-type", b"application/json")])
        capture.body(ctx, "response", b'{"answers":{"yes":{"type":"noul","noul":0.75}}}')
        capture.body_end(ctx, "response")
        capture.finish(ctx, "completed")
    finally:
        capture.close()
    export = tmp_path / "exports" / "integration"
    manifest = merge_capture(tmp_path, export)
    assert manifest["output_records"] == 1
    assert manifest["known_bucket_timezone"] == "Asia/Shanghai"
    assert manifest["runs"][0]["metadata_status"] == "clean_shutdown"
    assert manifest["runs"][0]["completeness"] == "not_established"
    captured = find_record(export / "exchanges.jsonl", ctx.record["id"])
    view = inspect_record(captured)
    assert view["capture_complete"] is True
    assert view["systemone"]["missing_answers"] == []
    assert view["systemone"]["answers"][0]["answer"]["noul"] == 0.75


def test_mixed_known_bucket_timezones_are_rejected(tmp_path):
    for timezone in ("Asia/Shanghai", "UTC"):
        item = record()
        write_records(tmp_path, "2026-09-24", [item])
        runtime = tmp_path / "runtime" / item["run_id"]
        runtime.mkdir(parents=True)
        (runtime / "run.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "run_id": item["run_id"],
                    "bucket_timezone": timezone,
                    "clean_shutdown": True,
                    "counts": {"dropped_records": 1},
                }
            )
        )
    with pytest.raises(ValueError, match="mixed bucket timezones"):
        merge_capture(tmp_path, tmp_path / "export")
