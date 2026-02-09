from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "soc_status.py"


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, sort_keys=True))
            f.write("\n")


def test_soc_status_prints_latest_and_failures(tmp_path: Path):
    log_path = tmp_path / "soc_runs.jsonl"
    _write_jsonl(
        log_path,
        [
            {
                "campus": "NB",
                "term_code": "2025SU",
                "started_at": "2026-02-01T00:00:00+00:00",
                "finished_at": "2026-02-01T00:00:10+00:00",
                "result": "staged",
                "checksum": "c1",
                "snapshot_id": "s1",
                "stage_attempted": True,
                "completeness_reason": None,
                "error_code": None,
                "attempts": None,
            },
            {
                "campus": "NB",
                "term_code": "2025SU",
                "started_at": "2026-02-02T00:00:00+00:00",
                "finished_at": "2026-02-02T00:00:10+00:00",
                "result": "error",
                "checksum": None,
                "snapshot_id": None,
                "stage_attempted": False,
                "completeness_reason": "UPSTREAM_INCOMPLETE",
                "error_code": "UPSTREAM_INCOMPLETE",
                "attempts": [
                    {
                        "source": "WEBREG_PUBLIC",
                        "error_code": "UPSTREAM_INCOMPLETE",
                        "message": None,
                        "completeness_reason": "UPSTREAM_INCOMPLETE",
                        "detail": {"message": "x" * 200},
                    }
                ],
            },
            {
                "campus": "NB",
                "term_code": "2025SU",
                "started_at": "2026-02-03T00:00:00+00:00",
                "finished_at": "2026-02-03T00:00:10+00:00",
                "result": "noop",
                "checksum": "c1",
                "snapshot_id": "s1",
                "stage_attempted": True,
                "completeness_reason": None,
                "error_code": None,
                "attempts": None,
            },
        ],
    )

    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--jsonl",
            str(log_path),
            "--campus",
            "NB",
            "--term-code",
            "2025SU",
            "--last-n-failures",
            "1",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    body = json.loads(proc.stdout)
    assert body["slice"] == {"campus": "NB", "term_code": "2025SU"}
    assert body["latest"]["result"] == "noop"
    assert body["latest"]["checksum"] == "c1"
    assert len(body["last_failures"]) == 1
    assert body["last_failures"][0]["error_code"] == "UPSTREAM_INCOMPLETE"
    attempts = body["last_failures"][0]["attempts"]
    assert isinstance(attempts, list) and attempts
    assert attempts[0]["detail_message"].endswith("...")


def test_soc_status_returns_non_zero_when_slice_not_found(tmp_path: Path):
    log_path = tmp_path / "soc_runs.jsonl"
    _write_jsonl(
        log_path,
        [
            {
                "campus": "NB",
                "term_code": "2025FA",
                "started_at": "2026-02-01T00:00:00+00:00",
                "finished_at": "2026-02-01T00:00:10+00:00",
                "result": "noop",
            }
        ],
    )
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--jsonl",
            str(log_path),
            "--campus",
            "NB",
            "--term-code",
            "2025SU",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 1
    assert "No records found for requested slice" in proc.stderr
