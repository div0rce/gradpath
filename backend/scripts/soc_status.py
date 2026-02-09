#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any


def _truncate(value: Any, max_len: int = 120) -> str:
    text = str(value)
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _record_sort_key(indexed_record: tuple[int, dict[str, Any]]) -> tuple[str, str, int]:
    index, record = indexed_record
    finished_at = str(record.get("finished_at") or "")
    started_at = str(record.get("started_at") or "")
    return (finished_at, started_at, index)


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_no, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSONL at line {line_no}: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError(f"JSONL line {line_no} must be an object")
        records.append(payload)
    return records


def _summarize_attempts(attempts: Any) -> list[dict[str, Any]] | None:
    if not isinstance(attempts, list):
        return None
    summary: list[dict[str, Any]] = []
    for row in attempts:
        if not isinstance(row, dict):
            continue
        detail = row.get("detail")
        detail_message = None
        if isinstance(detail, dict):
            detail_message = detail.get("message")
            if detail_message is None and detail:
                detail_message = json.dumps(detail, sort_keys=True)
        summary.append(
            {
                "source": row.get("source"),
                "error_code": row.get("error_code"),
                "completeness_reason": row.get("completeness_reason"),
                "message": _truncate(row.get("message")) if row.get("message") is not None else None,
                "detail_message": _truncate(detail_message) if detail_message is not None else None,
            }
        )
    return summary


def build_slice_status(
    *,
    records: list[dict[str, Any]],
    campus: str,
    term_code: str,
    last_n_failures: int,
) -> dict[str, Any]:
    filtered = [
        row
        for row in records
        if str(row.get("campus") or "") == campus and str(row.get("term_code") or "") == term_code
    ]
    if not filtered:
        raise ValueError("No records found for requested slice")

    ordered = sorted(enumerate(filtered), key=_record_sort_key)
    latest = ordered[-1][1]
    failures = [row for _, row in ordered if str(row.get("result") or "") == "error"]
    last_failures = failures[-max(0, last_n_failures) :] if last_n_failures > 0 else []

    latest_view = {
        "started_at": latest.get("started_at"),
        "finished_at": latest.get("finished_at"),
        "result": latest.get("result"),
        "checksum": latest.get("checksum"),
        "snapshot_id": latest.get("snapshot_id"),
        "stage_attempted": latest.get("stage_attempted"),
        "completeness_reason": latest.get("completeness_reason"),
        "error_code": latest.get("error_code"),
    }

    failure_views = [
        {
            "started_at": row.get("started_at"),
            "finished_at": row.get("finished_at"),
            "error_code": row.get("error_code"),
            "completeness_reason": row.get("completeness_reason"),
            "stage_attempted": row.get("stage_attempted"),
            "checksum": row.get("checksum"),
            "snapshot_id": row.get("snapshot_id"),
            "attempts": _summarize_attempts(row.get("attempts")),
        }
        for row in last_failures
    ]

    return {
        "slice": {"campus": campus, "term_code": term_code},
        "latest": latest_view,
        "last_failures": failure_views,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Show latest SOC ingest status for a slice from JSONL logs.")
    parser.add_argument("--jsonl", type=Path, required=True)
    parser.add_argument("--campus", required=True)
    parser.add_argument("--term-code", required=True)
    parser.add_argument("--last-n-failures", type=int, default=5)
    args = parser.parse_args()

    try:
        records = _load_jsonl(args.jsonl)
        status = build_slice_status(
            records=records,
            campus=args.campus,
            term_code=args.term_code,
            last_n_failures=max(0, args.last_n_failures),
        )
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(json.dumps(status, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
