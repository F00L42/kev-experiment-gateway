"""Run the proxy, merge date buckets, or inspect one stored exchange."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from . import __version__
from .config import load_config
from .inspect import find_record, inspect_record
from .merge import merge_capture


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--version", action="version", version=__version__)
    commands = root.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="Forward HTTP and capture request/response copies")
    serve.add_argument("--config", type=Path, help="TOML file; relative output_root is relative to this file")
    serve.add_argument("--upstream", help="HTTP(S) origin, e.g. http://127.0.0.1:55733")
    serve.add_argument("--host")
    serve.add_argument("--port", type=int)
    serve.add_argument("--output", type=Path, help="Capture root; command-line paths are relative to cwd")
    serve.add_argument(
        "--read-timeout", type=float, help="Read inactivity deadline in seconds; default unlimited"
    )
    merge = commands.add_parser("merge", help="Export a snapshot from date buckets, keeping the same schema")
    merge.add_argument("root", type=Path, help="Capture root containing exchanges/")
    merge.add_argument(
        "--output", required=True, type=Path, help="New destination directory (must not exist)"
    )
    merge.add_argument("--from", dest="date_from", help="First bucket date YYYY-MM-DD, inclusive")
    merge.add_argument("--to", dest="date_to", help="Last bucket date YYYY-MM-DD, inclusive")
    merge.add_argument(
        "--run-id", action="append", dest="run_ids", help="Select a run; repeat for multiple runs"
    )
    inspect = commands.add_parser("inspect", help="Show an exchange and parse its SystemOne response offline")
    inspect.add_argument(
        "source", type=Path, help="Capture root or one JSONL file (including merged exports)"
    )
    inspect.add_argument("--id", required=True, dest="record_id")
    inspect.add_argument("--raw", action="store_true", help="Show the saved record, without decoding")
    inspect.add_argument("--max-decoded-bytes", type=int, default=16 * 1024 * 1024)
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "serve":
            import uvicorn

            from .transport import Gateway

            config = load_config(
                args.config,
                {
                    "upstream_url": args.upstream,
                    "host": args.host,
                    "port": args.port,
                    "output_root": args.output,
                    "read_timeout": args.read_timeout,
                },
            )
            logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
            # HTTP client and access logs can include query credentials; record safe copies ourselves.
            logging.getLogger("httpx").setLevel(logging.WARNING)
            logging.getLogger("httpcore").setLevel(logging.WARNING)
            gateway = Gateway(config)
            server = uvicorn.Server(
                uvicorn.Config(
                    gateway,
                    host=config.host,
                    port=config.port,
                    lifespan="on",
                    access_log=False,
                    timeout_graceful_shutdown=30,
                    server_header=False,
                )
            )
            server.run()
            return 0 if server.started else 1
        if args.command == "merge":
            manifest = merge_capture(
                args.root,
                args.output,
                date_from=args.date_from,
                date_to=args.date_to,
                run_ids=args.run_ids,
            )
            print(
                json.dumps({"output": str(args.output.resolve()), **manifest}, ensure_ascii=False, indent=2)
            )
        else:
            record = find_record(args.source, args.record_id)
            value = record if args.raw else inspect_record(record, args.max_decoded_bytes)
            print(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False))
        return 0
    except (ValueError, OSError, KeyError) as exc:
        print(f"kev-gateway: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
