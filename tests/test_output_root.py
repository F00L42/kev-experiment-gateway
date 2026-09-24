import asyncio
import json
import subprocess
import sys
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest

from kev_gateway.capture import Capture
from kev_gateway.cli import main, parser
from kev_gateway.config import GatewayConfig, load_config
from kev_gateway.transport import Gateway


def test_missing_root_created_and_existing_data_preserved_on_restart(tmp_path):
    root = tmp_path / "new" / "nested" / "captures"
    config = GatewayConfig("http://localhost", output_root=root)
    first = Capture(config)
    first.close()
    bucket = datetime.now(UTC).astimezone(ZoneInfo(config.bucket_timezone)).date().isoformat()
    assert (root / "exchanges" / bucket).is_dir()
    sentinel = root / "exchanges" / bucket / "existing.jsonl"
    sentinel.write_bytes(b'{"existing":true}\n')
    previous_run = (first.runtime / "run.json").read_bytes()
    second = Capture(config)
    second.close()
    assert first.run_id != second.run_id
    assert sentinel.read_bytes() == b'{"existing":true}\n'
    assert (first.runtime / "run.json").read_bytes() == previous_run
    assert json.loads((second.runtime / "run.json").read_text())["clean_shutdown"]
    assert not list(root.rglob(".kev-write-*"))


@pytest.mark.parametrize("obstruction", ["root", "runtime", "exchanges", "bucket", "permission"])
def test_storage_failure_stops_lifespan_before_client_or_writer(tmp_path, monkeypatch, obstruction):
    root = tmp_path / "capture"
    bucket = datetime.now(UTC).astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat()
    target = {
        "root": root,
        "runtime": root / "runtime",
        "exchanges": root / "exchanges",
        "bucket": root / "exchanges" / bucket,
        "permission": root / "exchanges",
    }[obstruction]
    if obstruction == "permission":
        from kev_gateway import capture

        temporary_file = capture.tempfile.TemporaryFile

        def deny_write(*args, **kwargs):
            if kwargs.get("dir") == target:
                raise PermissionError("test storage permission denied")
            return temporary_file(*args, **kwargs)

        monkeypatch.setattr(capture.tempfile, "TemporaryFile", deny_write)
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("existing user file", encoding="utf-8")

    async def attempt():
        gateway = Gateway(GatewayConfig("http://localhost", output_root=root))
        sent = []

        async def receive():
            return {"type": "lifespan.startup"}

        async def send(event):
            sent.append(event)

        await gateway({"type": "lifespan"}, receive, send)
        assert not gateway._started and gateway.client is None and gateway.capture is None
        assert len(sent) == 1 and sent[0]["type"] == "lifespan.startup.failed"
        assert "output_root" in sent[0]["message"] and str(target) in sent[0]["message"]

    asyncio.run(attempt())
    if obstruction != "permission":
        assert target.read_text(encoding="utf-8") == "existing user file"
    assert not list(root.glob("runtime/*/run.json"))


def test_output_root_cli_alias_overrides_toml_and_empty_paths_fail(tmp_path):
    source = tmp_path / "gateway.toml"
    source.write_text('upstream_url="http://localhost"\noutput_root="relative"\n')
    assert load_config(source).output_root == tmp_path / "relative"
    for option in ("--output-root", "--output"):
        parsed = parser().parse_args(["serve", "--config", str(source), option, str(tmp_path / "override")])
        assert load_config(parsed.config, {"output_root": parsed.output}).output_root == tmp_path / "override"
        assert main(["serve", "--upstream", "http://localhost", option, ""]) == 2
    source.write_text('upstream_url="http://localhost"\noutput_root=""\n')
    with pytest.raises(ValueError, match="nonempty"):
        load_config(source)
    with pytest.raises(ValueError, match="nonempty"):
        GatewayConfig("http://localhost", output_root="  ")


def test_cli_reports_storage_path_and_exits_nonzero(tmp_path):
    root = tmp_path / "occupied"
    root.write_text("keep me")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "kev_gateway",
            "serve",
            "--upstream",
            "http://localhost:9",
            "--output-root",
            str(root),
        ],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode != 0
    assert str(root) in result.stderr and "Cannot initialize output_root" in result.stderr
    assert root.read_text() == "keep me"
