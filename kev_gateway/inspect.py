"""Offline capture decoding and SystemOne answer inspection; no HTTP calls."""

from __future__ import annotations

import base64
import binascii
import zlib
from pathlib import Path
from typing import Any

from .merge import _loads, _validate_record


def _inflate(data: bytes, wbits: int, limit: int) -> bytes:
    decoder = zlib.decompressobj(wbits)
    output = decoder.decompress(data, limit + 1)
    if len(output) > limit or decoder.unconsumed_tail:
        raise OverflowError("Decoded body exceeds max_decoded_bytes")
    if not decoder.eof:
        raise ValueError("Compressed body ended before its end marker")
    if decoder.unused_data:
        raise ValueError("Compressed body contains trailing data or multiple members")
    return output


def _body_view(message: Any, limit: int) -> dict[str, Any]:
    if not isinstance(message, dict):
        return {"decode_status": "unavailable", "json": None}
    if message.get("body_complete") is not True:
        return {"decode_status": "incomplete", "json": None}
    body = message.get("body")
    encoding = message.get("body_encoding")
    if not isinstance(body, str):
        return {"decode_status": "unavailable", "json": None}
    try:
        if encoding == "utf8":
            raw = body.encode("utf-8")
        elif encoding == "base64":
            if len(body) > ((limit + 2) // 3) * 4:
                raise OverflowError("Encoded body exceeds max_decoded_bytes")
            raw = base64.b64decode(body, validate=True)
        else:
            return {"decode_status": "unsupported_encoding", "json": None, "error": str(encoding)}
        if len(raw) > limit:
            raise OverflowError("Body exceeds max_decoded_bytes")
        codings = []
        for name, value in message.get("headers", []):
            if name.lower() == "content-encoding":
                codings.extend(part.strip().lower() for part in value.split(","))
        for coding in reversed(codings):
            if coding in ("", "identity"):
                continue
            if coding == "gzip":
                raw = _inflate(raw, zlib.MAX_WBITS | 16, limit)
            elif coding == "deflate":
                try:
                    raw = _inflate(raw, zlib.MAX_WBITS, limit)
                except zlib.error:
                    raw = _inflate(raw, -zlib.MAX_WBITS, limit)
            else:
                return {"decode_status": "unsupported_encoding", "json": None, "error": coding}
        parsed = _loads(raw.decode("utf-8"))
        return {"decode_status": "json", "json": parsed}
    except OverflowError as exc:
        return {"decode_status": "too_large", "json": None, "error": str(exc)}
    except (ValueError, TypeError, AttributeError, UnicodeError, binascii.Error, zlib.error) as exc:
        return {"decode_status": "invalid", "json": None, "error": str(exc)}


def _systemone(request: Any, response: Any) -> dict[str, Any] | None:
    if not isinstance(request, dict) or not isinstance(request.get("questions"), dict):
        return None
    questions = request["questions"]
    # A missing/malformed answers object is not an empty successfully parsed result.
    raw_answers = response.get("answers") if isinstance(response, dict) else None
    answers = raw_answers if isinstance(raw_answers, dict) else {}
    entries = []
    for question_id in [*questions, *(key for key in answers if key not in questions)]:
        question = questions.get(question_id)
        answer = answers.get(question_id)
        entries.append(
            {
                "question_id": question_id,
                "requested_type": question.get("type") if isinstance(question, dict) else None,
                "present": question_id in answers if isinstance(raw_answers, dict) else None,
                "answer_type": answer.get("type") if isinstance(answer, dict) else None,
                # Preserve noul/choice/score, distributions, legends, and confidence
                # exactly as returned. Never infer a zero for a missing value.
                "answer": answer,
            }
        )
    return {
        "answers_status": "object" if isinstance(raw_answers, dict) else "missing_or_invalid",
        "requested_questions": list(questions),
        "missing_answers": [key for key in questions if key not in answers]
        if isinstance(raw_answers, dict)
        else None,
        "extra_answers": [key for key in answers if key not in questions]
        if isinstance(raw_answers, dict)
        else None,
        "answers": entries,
        "usage": response.get("usage") if isinstance(response, dict) else None,
        "latency_ms": response.get("latency_ms") if isinstance(response, dict) else None,
    }


def inspect_record(record: dict, max_decoded_bytes: int = 16_777_216) -> dict[str, Any]:
    """Return a JSON-friendly view, refusing semantic use of incomplete bodies.

    Decode errors remain local inspection results. Noul is the returned number
    in ``noul`` (not a boolean); choice and score retain the full server payload.
    """
    if max_decoded_bytes < 1:
        raise ValueError("max_decoded_bytes must be positive")
    _validate_record(record)
    result = {
        key: record.get(key)
        for key in (
            "id",
            "run_id",
            "started_at",
            "duration_ms",
            "outcome",
            "capture_complete",
            "capture_mode",
            "capture_reason",
            "response_source",
            "error",
        )
    }
    request = record.get("request")
    response = record.get("response")
    request_view = _body_view(request, max_decoded_bytes)
    response_view = _body_view(response, max_decoded_bytes)
    if isinstance(request, dict):
        request_view.update({key: request.get(key) for key in ("method", "path", "query", "body_complete")})
    if isinstance(response, dict):
        response_view.update({key: response.get(key) for key in ("status", "body_complete")})
    result["request"] = request_view
    result["response"] = response_view
    result["systemone"] = _systemone(request_view["json"], response_view["json"])
    return result


def find_record(path: Path, record_id: str) -> dict[str, Any]:
    """Find an ID in one JSONL file or a capture root's exchanges directory.

    A final line without a newline is an in-progress tail and is skipped. Bad
    complete rows and conflicting copies of an ID are explicit errors.
    """
    path = Path(path)
    if path.is_file():
        sources = [path]
    elif (path / "exchanges").is_dir():
        sources = sorted((path / "exchanges").glob("*/*.jsonl"))
    else:
        raise FileNotFoundError(f"Expected JSONL file or capture root: {path}")
    found: dict[str, Any] | None = None
    for source in sources:
        with source.open("rb") as stream:
            for line_number, line in enumerate(stream, 1):
                if not line.endswith(b"\n"):
                    continue
                try:
                    record = _loads(line)
                    _validate_record(record)
                except (ValueError, UnicodeError) as exc:
                    raise ValueError(f"{source}:{line_number}: {exc}") from exc
                if record["id"] == record_id:
                    if found is not None and found != record:
                        raise ValueError(f"{source}:{line_number}: Conflicting records with id {record_id}")
                    found = record
    if found is None:
        raise KeyError(f"Record ID not found: {record_id}")
    return found
