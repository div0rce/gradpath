#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any
from uuid import uuid4

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.soc_runner import fetch_raw_payload_for_slice, stage_soc_slice


@dataclass(frozen=True)
class IngestJob:
    campus: str
    term_code: str
    source_priority: list[str]
    dry_run_first: bool = False


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _parse_sources(raw: str | None) -> list[str]:
    if not raw:
        return ["WEBREG_PUBLIC", "CSP_PUBLIC", "DEGREE_NAVIGATOR_PUBLIC"]
    return [x.strip() for x in raw.split(",") if x.strip()]


def _load_jobs_from_config(config_path: Path) -> list[IngestJob]:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    rows = payload.get("jobs", [])
    if not isinstance(rows, list):
        raise ValueError("config.jobs must be a list")
    jobs: list[IngestJob] = []
    seen_slices: set[tuple[str, str]] = set()
    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise ValueError(f"jobs[{idx}] must be an object")
        enabled_source = row.get("enabled_source")
        if not isinstance(enabled_source, str) or not enabled_source.strip():
            raise ValueError(f"jobs[{idx}].enabled_source must be a non-empty string")
        source_priority = [enabled_source.strip()]
        campus = str(row["campus"])
        term_code = str(row["term_code"])
        slice_key = (campus, term_code)
        if slice_key in seen_slices:
            raise ValueError(f"Duplicate enabled slice in config: campus={campus} term_code={term_code}")
        seen_slices.add(slice_key)
        jobs.append(
            IngestJob(
                campus=campus,
                term_code=term_code,
                source_priority=source_priority,
                dry_run_first=bool(row.get("dry_run_first", False)),
            )
        )
    return jobs


def _detail_from_exception(exc: Exception) -> dict[str, Any]:
    if exc.args and isinstance(exc.args[0], dict):
        return exc.args[0]
    return {"error_code": "SOC_RUNNER_FAILED", "message": str(exc)}


def _emit_record(record: dict[str, Any], output_path: Path | None) -> None:
    line = json.dumps(record, sort_keys=True)
    print(line)
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("a", encoding="utf-8") as f:
            f.write(line)
            f.write("\n")


def run_job(job: IngestJob, *, api_base: str) -> dict[str, Any]:
    run_id = str(uuid4())
    started_at = _utc_now()
    stage_attempted = False
    try:
        source_used, raw_payload = fetch_raw_payload_for_slice(
            campus=job.campus,
            term_code=job.term_code,
            source_priority=job.source_priority,
        )
        stage_attempted = True
        stage = stage_soc_slice(
            api_base=api_base,
            campus=job.campus,
            term_code=job.term_code,
            ingest_source=source_used,
            raw_payload=raw_payload,
            run_id=run_id,
            dry_run_first=job.dry_run_first,
            source_metadata={"runner": "run_soc_ingest.py"},
        )
        result = stage.get("result", {}) if isinstance(stage, dict) else {}
        snapshot = stage.get("snapshot", {}) if isinstance(stage, dict) else {}
        outcome = "noop" if bool(result.get("noop")) else "staged"
        return {
            "run_id": run_id,
            "source_used": source_used,
            "campus": job.campus,
            "term_code": job.term_code,
            "started_at": started_at,
            "finished_at": _utc_now(),
            "result": outcome,
            "checksum": result.get("checksum"),
            "noop": bool(result.get("noop")),
            "parse_warnings_count": int(result.get("parse_warnings_count") or 0),
            "unknown_courses_dropped_count": int(result.get("unknown_courses_dropped_count") or 0),
            "snapshot_id": snapshot.get("snapshot_id"),
            "error_code": None,
            "error_message": None,
            "completeness_reason": None,
            "attempts": None,
            "stage_attempted": stage_attempted,
        }
    except Exception as exc:
        detail = _detail_from_exception(exc)
        attempts = detail.get("attempts")
        completeness_reason = None
        if detail.get("error_code") == "UPSTREAM_INCOMPLETE":
            if isinstance(detail.get("completeness_reason"), str):
                completeness_reason = detail["completeness_reason"]
            elif isinstance(attempts, list):
                first_reason = next(
                    (
                        a.get("completeness_reason")
                        for a in attempts
                        if isinstance(a, dict) and isinstance(a.get("completeness_reason"), str)
                    ),
                    None,
                )
                completeness_reason = first_reason
        return {
            "run_id": run_id,
            "source_used": None,
            "campus": job.campus,
            "term_code": job.term_code,
            "started_at": started_at,
            "finished_at": _utc_now(),
            "result": "error",
            "checksum": None,
            "noop": False,
            "parse_warnings_count": 0,
            "unknown_courses_dropped_count": 0,
            "snapshot_id": None,
            "error_code": detail.get("error_code", "SOC_RUNNER_FAILED"),
            "error_message": detail.get("message") or json.dumps(detail, sort_keys=True),
            "completeness_reason": completeness_reason,
            "attempts": attempts if isinstance(attempts, list) else None,
            "stage_attempted": stage_attempted,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run SOC ingestion automation jobs.")
    parser.add_argument("--api-base", default="http://localhost:8000")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--campus")
    parser.add_argument("--term-code")
    parser.add_argument("--source-priority", default="WEBREG_PUBLIC,CSP_PUBLIC,DEGREE_NAVIGATOR_PUBLIC")
    parser.add_argument("--dry-run-first", action="store_true")
    parser.add_argument("--output-jsonl", type=Path)
    args = parser.parse_args()

    jobs: list[IngestJob]
    if args.config:
        jobs = _load_jobs_from_config(args.config)
    else:
        if not args.campus or not args.term_code:
            raise SystemExit("--campus and --term-code are required when --config is not provided")
        jobs = [
            IngestJob(
                campus=args.campus,
                term_code=args.term_code,
                source_priority=_parse_sources(args.source_priority),
                dry_run_first=args.dry_run_first,
            )
        ]

    any_failed = False
    for job in jobs:
        record = run_job(job, api_base=args.api_base)
        _emit_record(record, args.output_jsonl)
        if record["result"] == "error":
            any_failed = True

    return 1 if any_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
