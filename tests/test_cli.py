import json
import os
import subprocess
import sys
from pathlib import Path

from kev_gateway.capture import Capture
from kev_gateway.cli import main
from kev_gateway.config import GatewayConfig


def test_cli_merged_record_can_be_inspected_and_bad_input_returns_nonzero(tmp_path, capsys):
    cap = Capture(GatewayConfig("http://localhost", output_root=tmp_path / "capture"))
    ctx = cap.begin("POST", b"/v1/systemone", b"", [])
    cap.body(ctx, "request", b'{"questions":{"q":{"type":"noul"}},"state":"hello"}')
    cap.body_end(ctx, "request")
    cap.response_start(ctx, 200, [])
    cap.body(ctx, "response", b'{"answers":{"q":{"type":"noul","noul":0.8}},"latency_ms":1}')
    cap.body_end(ctx, "response")
    cap.finish(ctx, "completed")
    cap.close()
    record_id = ctx.record["id"]
    merged = tmp_path / "export"
    assert main(["merge", str(cap.root), "--output", str(merged)]) == 0
    assert json.loads(capsys.readouterr().out)["output_records"] == 1
    assert main(["inspect", str(merged / "exchanges.jsonl"), "--id", record_id]) == 0
    view = json.loads(capsys.readouterr().out)
    assert view["systemone"]["missing_answers"] == []
    assert view["systemone"]["answers"][0]["answer"]["noul"] == 0.8
    assert main(["merge", str(cap.root), "--output", str(merged)]) == 2
    assert "destination must be new" in capsys.readouterr().err.lower()


def test_installed_module_help_works_without_model_imports(tmp_path):
    project = Path(__file__).resolve().parents[1]
    env = {**os.environ, "PYTHONPATH": str(project)}
    result = subprocess.run(
        [sys.executable, "-m", "kev_gateway", "--help"],
        cwd=tmp_path,
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0 and "merge" in result.stdout and "inspect" in result.stdout
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import kev_gateway.cli,sys;print('torch' in sys.modules,'vllm' in sys.modules)",
        ],
        cwd=tmp_path,
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0 and result.stdout.strip() == "False False"
