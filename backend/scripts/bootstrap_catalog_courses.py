#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys
from typing import Any
from urllib.parse import urlencode

import httpx
from sqlalchemy import select

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db import SessionLocal
from app.models import Course
from app.services.catalog import (
    get_active_published_snapshot,
    normalize_course_code,
    promote_snapshot,
    stage_course_overlay_snapshot,
)

DEFAULT_CAMPUSES = ["NB", "NWK", "CM"]
DEFAULT_TERM_CODES = ["2024FA", "2025SP", "2025SU", "2025FA", "2026SP"]
TERM_CODE_RE = re.compile(r"^(\d{4})(SP|SU|FA|WI)$")
NUMERIC_TERM_CODE_RE = re.compile(r"^([0179])(\d{4})$")


@dataclass(frozen=True)
class BootstrapCourseCandidate:
    code: str
    normalized_code: str
    title: str
    credits: int
    category: str | None


def map_term_code_to_soc_params(term_code: str) -> tuple[str, str] | None:
    normalized = str(term_code).strip().upper()
    match = TERM_CODE_RE.match(normalized)
    if match:
        year, suffix = match.groups()
        term_map = {"SP": "1", "SU": "7", "FA": "9"}
        if suffix in term_map:
            return year, term_map[suffix]
        return None
    numeric_match = NUMERIC_TERM_CODE_RE.match(normalized)
    if numeric_match:
        term, year = numeric_match.groups()
        return year, term
    return None


def resolve_course_identity(row: dict[str, Any]) -> tuple[str | None, str | None]:
    if "courseString" in row:
        course_string = row.get("courseString")
        if isinstance(course_string, str):
            stripped = course_string.strip()
            if stripped:
                return stripped, None
        return None, "INVALID_COURSE_STRING"

    subject = row.get("subject")
    course_number = row.get("courseNumber")
    if isinstance(subject, str) and isinstance(course_number, str):
        subject_stripped = subject.strip().upper()
        course_number_stripped = course_number.strip()
        if subject_stripped and course_number_stripped:
            return f"{subject_stripped}:{course_number_stripped}", None
    return None, "MISSING_COURSE_IDENTITY"


def parse_credits(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return 0
        try:
            if "." in stripped:
                return int(float(stripped))
            return int(stripped)
        except Exception:
            return 0
    return 0


def _extract_course_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("courses", "results", "data"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
    return []


def extract_candidates_from_payload(payload: Any) -> tuple[dict[str, BootstrapCourseCandidate], dict[str, int]]:
    rows = _extract_course_rows(payload)
    rejections: Counter[str] = Counter()
    candidates: dict[str, BootstrapCourseCandidate] = {}
    for row in rows:
        raw_code, reason = resolve_course_identity(row)
        if raw_code is None:
            rejections[reason or "UNKNOWN_STRUCTURE"] += 1
            continue
        normalized_code, _ = normalize_course_code(raw_code)
        title_raw = row.get("title")
        title = str(title_raw) if isinstance(title_raw, str) and title_raw else "(bootstrap) Unknown Title"
        credits = parse_credits(row.get("credits"))
        category = row.get("category") if isinstance(row.get("category"), str) else None
        candidate = BootstrapCourseCandidate(
            code=raw_code,
            normalized_code=normalized_code,
            title=title,
            credits=credits,
            category=category,
        )
        existing = candidates.get(normalized_code)
        if existing is None or candidate.code < existing.code:
            candidates[normalized_code] = candidate
    return candidates, dict(rejections)


def is_narrow_coverage(*, campuses: list[str], term_codes: list[str]) -> bool:
    return len(campuses) < 3 or len(term_codes) < 5


def validate_apply_gating(
    *,
    apply: bool,
    use_default_coverage: bool,
    strict_coverage: bool,
    force: bool,
    campuses: list[str],
    term_codes: list[str],
) -> None:
    narrow = is_narrow_coverage(campuses=campuses, term_codes=term_codes)
    if strict_coverage and narrow:
        raise ValueError("--strict-coverage requires at least 3 campuses and 5 term codes")
    if apply and not use_default_coverage and narrow and not force:
        raise ValueError("--apply with narrow coverage requires --force")


def compute_missing_courses(
    *,
    fetched_candidates: dict[str, BootstrapCourseCandidate],
    existing_normalized: set[str],
) -> list[BootstrapCourseCandidate]:
    missing_normalized = sorted(set(fetched_candidates.keys()) - set(existing_normalized))
    return [fetched_candidates[key] for key in missing_normalized]


def _resolve_coverage(args: argparse.Namespace) -> tuple[list[str], list[str]]:
    if args.use_default_coverage:
        return sorted(DEFAULT_CAMPUSES), sorted(DEFAULT_TERM_CODES)

    campuses = sorted({str(x).strip().upper() for x in (args.campus or []) if str(x).strip()})
    term_codes = sorted({str(x).strip().upper() for x in (args.term_code or []) if str(x).strip()})
    if not campuses or not term_codes:
        raise ValueError("When not using defaults, provide at least one --campus and one --term-code")
    return campuses, term_codes


def _fetch_courses_payload(
    *,
    soc_base: str,
    campus: str,
    term_code: str,
    timeout_s: float,
) -> tuple[Any, str]:
    mapped = map_term_code_to_soc_params(term_code)
    if mapped is None:
        raise ValueError(f"Unmappable term code: {term_code}")
    year, term = mapped
    url = f"{soc_base.rstrip('/')}/courses.json"
    params = {"year": year, "term": term, "campus": campus}
    response = httpx.get(url, params=params, timeout=timeout_s)
    response.raise_for_status()
    payload = response.json()
    source_url = f"{url}?{urlencode(sorted(params.items()))}"
    return payload, source_url


def _to_json(data: dict[str, Any]) -> str:
    return json.dumps(data, sort_keys=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Bootstrap catalog courses from Rutgers SOC APIs.")
    parser.add_argument("--soc-base", default="https://classes.rutgers.edu/soc/api")
    parser.add_argument("--campus", action="append")
    parser.add_argument("--term-code", action="append")
    parser.add_argument("--use-default-coverage", action="store_true")
    parser.add_argument("--strict-coverage", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--promote", action="store_true")
    parser.add_argument("--sample-size", type=int, default=25)
    parser.add_argument("--timeout-s", type=float, default=25.0)
    args = parser.parse_args()

    if args.promote and not args.apply:
        raise SystemExit("--promote requires --apply")

    try:
        campuses, term_codes = _resolve_coverage(args)
        validate_apply_gating(
            apply=args.apply,
            use_default_coverage=bool(args.use_default_coverage),
            strict_coverage=bool(args.strict_coverage),
            force=bool(args.force),
            campuses=campuses,
            term_codes=term_codes,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    all_candidates: dict[str, BootstrapCourseCandidate] = {}
    rejection_counts: Counter[str] = Counter()
    source_urls: list[str] = []

    for campus in sorted(campuses):
        for term_code in sorted(term_codes):
            payload, source_url = _fetch_courses_payload(
                soc_base=args.soc_base,
                campus=campus,
                term_code=term_code,
                timeout_s=float(args.timeout_s),
            )
            source_urls.append(source_url)
            candidates, rejections = extract_candidates_from_payload(payload)
            rejection_counts.update(rejections)
            for normalized_code, candidate in candidates.items():
                existing = all_candidates.get(normalized_code)
                if existing is None or candidate.code < existing.code:
                    all_candidates[normalized_code] = candidate

    with SessionLocal() as db:
        baseline = get_active_published_snapshot(db)
        existing_rows = db.execute(
            select(Course).where(Course.catalog_snapshot_id == baseline.id)
        ).scalars().all()
        existing_normalized = {normalize_course_code(row.code)[0] for row in existing_rows}

        fetched_normalized = set(all_candidates.keys())
        missing_candidates = compute_missing_courses(
            fetched_candidates=all_candidates,
            existing_normalized=existing_normalized,
        )
        missing_normalized = [row.normalized_code for row in missing_candidates]
        missing_courses = [asdict(row) for row in missing_candidates]

        summary = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "baseline_snapshot_id": baseline.id,
            "coverage": {"campuses": sorted(campuses), "term_codes": sorted(term_codes)},
            "fetched_distinct_normalized": len(fetched_normalized),
            "existing_distinct_normalized": len(existing_normalized),
            "missing_distinct_normalized": len(missing_normalized),
            "missing_samples": missing_normalized[: max(1, int(args.sample_size))],
            "rejection_counts": dict(sorted(rejection_counts.items())),
            "apply": bool(args.apply),
            "promote": bool(args.promote),
        }
        print(_to_json(summary))

        if not args.apply:
            return 0

        if not missing_courses:
            print(_to_json({"result": "noop", "reason": "no missing courses"}))
            return 0

        metadata = {
            "bootstrap_courses": {
                "input_distinct_codes": len(fetched_normalized),
                "inserted_count": len(missing_courses),
                "already_present_count": len(fetched_normalized.intersection(existing_normalized)),
                "rejected_count": int(sum(rejection_counts.values())),
                "coverage_campuses": sorted(campuses),
                "coverage_terms": sorted(term_codes),
                "source_urls": sorted(source_urls),
            }
        }
        staged = stage_course_overlay_snapshot(
            db,
            baseline_snapshot=baseline,
            missing_courses=missing_courses,
            source_metadata=metadata,
        )
        if staged is None:
            print(_to_json({"result": "noop", "reason": "no missing courses"}))
            return 0
        print(
            _to_json(
                {
                    "result": "staged",
                    "snapshot_id": staged.id,
                    "inserted_count": len(missing_courses),
                }
            )
        )

        if args.promote:
            published = promote_snapshot(db, staged.id)
            print(_to_json({"result": "promoted", "snapshot_id": published.id}))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
