"""Small, explicit configuration for an experiment proxy."""

from __future__ import annotations

import math
import tomllib
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo


def output_path(value: str | Path) -> Path:
    if not isinstance(value, (str, Path)) or (isinstance(value, str) and not value.strip()):
        raise ValueError("output_root must be a nonempty directory path")
    return Path(value).expanduser()


@dataclass(frozen=True)
class GatewayConfig:
    upstream_url: str
    output_root: Path = Path("captures")
    host: str = "127.0.0.1"
    port: int = 8080
    bucket_timezone: str = "Asia/Shanghai"
    capture_paths: tuple[str, ...] = ("/v1/systemone",)
    connect_timeout: float = 10.0
    read_timeout: float | None = None
    write_timeout: float = 30.0
    pool_timeout: float = 30.0
    max_connections: int | None = None
    max_keepalive_connections: int = 20
    max_body_bytes: int = 16 * 1024 * 1024
    queue_bytes: int = 64 * 1024 * 1024
    max_pending_records: int = 4096

    def __post_init__(self):
        url = urlsplit(self.upstream_url)
        if (
            url.scheme not in {"http", "https"}
            or not url.hostname
            or url.username is not None
            or url.password is not None
            or url.path not in {"", "/"}
            or url.query
            or url.fragment
        ):
            raise ValueError("upstream_url must be an HTTP(S) origin without credentials, path or query")
        _ = url.port  # Validate the port even when supplied as part of the URL.
        if not isinstance(self.host, str) or not self.host:
            raise ValueError("host must be a nonempty string")
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        ZoneInfo(self.bucket_timezone)
        for name in ("connect_timeout", "read_timeout", "write_timeout", "pool_timeout"):
            value = getattr(self, name)
            if value is None and name == "read_timeout":
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{name} must be a positive finite number")
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be a positive finite number")
        for name in (
            "max_connections",
            "max_keepalive_connections",
            "max_body_bytes",
            "queue_bytes",
            "max_pending_records",
        ):
            value = getattr(self, name)
            if value is None and name == "max_connections":
                continue
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if not isinstance(self.capture_paths, (list, tuple)) or any(
            not isinstance(path, str) or (path != "*" and not path.startswith("/"))
            for path in self.capture_paths
        ):
            raise ValueError("capture_paths must contain absolute HTTP paths or '*'")
        object.__setattr__(self, "capture_paths", tuple(self.capture_paths))
        object.__setattr__(self, "output_root", output_path(self.output_root).resolve())

    def snapshot(self) -> dict:
        result = asdict(self)
        result["output_root"] = str(self.output_root)
        return result


def load_config(path: Path | None = None, overrides: dict | None = None) -> GatewayConfig:
    values = {}
    if path is not None:
        path = Path(path).resolve()
        with path.open("rb") as stream:
            values = tomllib.load(stream)
        if "output_root" in values:
            root = output_path(values["output_root"])
            values["output_root"] = root if root.is_absolute() else path.parent / root
    values.update({key: value for key, value in (overrides or {}).items() if value is not None})
    unknown = set(values) - {field.name for field in fields(GatewayConfig)}
    if unknown:
        raise ValueError(f"Unknown configuration keys: {sorted(unknown)}")
    if not values.get("upstream_url"):
        raise ValueError("Specify upstream_url in the configuration or pass --upstream")
    return GatewayConfig(**values)
