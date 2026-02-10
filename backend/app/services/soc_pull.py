from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from hashlib import sha256
import random
import re
import time
from typing import Any, Callable
from urllib.parse import urlencode

import httpx

COMPLETENESS_REASONS = {
    "PAGINATION_UNCERTAIN",
    "TRUNCATED_RESULT",
    "AMBIGUOUS_TERM",
    "UPSTREAM_INCOMPLETE",
    "UNKNOWN_COMPLETENESS",
}


@dataclass(frozen=True)
class SocFetchResult:
    raw_payload: dict[str, Any]
    is_complete: bool
    completeness_reason: str | None = None


@dataclass(frozen=True)
class TermMappingResult:
    soc_year: str
    soc_term: str
    term_code: str
    campus: str


@dataclass(frozen=True)
class PagePayload:
    url: str
    payload: Any


OfferingRow = dict[str, Any]
WEBREG_TERM_CODE_RE = re.compile(r"^(\d{4})(SP|SU|FA|WI)$")
WEBREG_NUMERIC_TERM_CODE_RE = re.compile(r"^([0179])(\d{4})$")

WEBREG_RETRY_ATTEMPTS = 5
WEBREG_BACKOFF_BASE_S = 0.5
WEBREG_BACKOFF_CAP_S = 8.0
WEBREG_BACKOFF_JITTER = 0.3
WEBREG_CONNECT_TIMEOUT_S = 5.0
WEBREG_READ_TIMEOUT_S = 20.0
WEBREG_REQUEST_TIMEOUT_S = 25.0
WEBREG_SLICE_BUDGET_S = 120.0


class _SliceBudgetExceeded(RuntimeError):
    pass


def _schema_violation(message: str, **extra: Any) -> ValueError:
    detail: dict[str, Any] = {"error_code": "SOC_SCHEMA_VIOLATION", "message": message}
    if extra:
        detail.update(extra)
    return ValueError(detail)


def _is_retryable_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def _is_retryable_exception(exc: Exception) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return _is_retryable_status(exc.response.status_code)
    return isinstance(exc, (httpx.ConnectTimeout, httpx.ReadTimeout))


def _compute_backoff_delay(attempt: int, *, jitter_sample: float | None = None) -> float:
    sample = random.random() if jitter_sample is None else jitter_sample
    clamped = max(0.0, min(1.0, float(sample)))
    base_delay = min(WEBREG_BACKOFF_CAP_S, WEBREG_BACKOFF_BASE_S * (2 ** max(0, attempt - 1)))
    jitter_multiplier = 1.0 - WEBREG_BACKOFF_JITTER + (2 * WEBREG_BACKOFF_JITTER * clamped)
    return base_delay * jitter_multiplier


def validate_soc_raw_payload(payload: dict[str, Any]) -> None:
    allowed_top_keys = {"terms", "offerings", "metadata"}
    unexpected = sorted(set(payload.keys()) - allowed_top_keys)
    if unexpected:
        raise _schema_violation("Unexpected top-level keys", unexpected_keys=unexpected)

    terms = payload.get("terms")
    offerings = payload.get("offerings")
    metadata = payload.get("metadata")

    if not isinstance(terms, list):
        raise _schema_violation("terms must be a list")
    if not isinstance(offerings, list):
        raise _schema_violation("offerings must be a list")
    if not isinstance(metadata, dict):
        raise _schema_violation("metadata must be an object")

    allowed_metadata = {"source_urls", "fetched_at", "raw_hash", "parse_warnings"}
    unexpected_meta = sorted(set(metadata.keys()) - allowed_metadata)
    if unexpected_meta:
        raise _schema_violation("Unexpected metadata keys", unexpected_metadata=unexpected_meta)

    source_urls = metadata.get("source_urls", [])
    if not isinstance(source_urls, list) or any(not isinstance(x, str) for x in source_urls):
        raise _schema_violation("metadata.source_urls must be a list[str]")

    parse_warnings = metadata.get("parse_warnings", [])
    if not isinstance(parse_warnings, list) or any(not isinstance(x, str) for x in parse_warnings):
        raise _schema_violation("metadata.parse_warnings must be a list[str]")

    fetched_at = metadata.get("fetched_at")
    if not isinstance(fetched_at, str) or not fetched_at.strip():
        raise _schema_violation("metadata.fetched_at must be a non-empty string")

    raw_hash = metadata.get("raw_hash")
    if raw_hash is not None and not isinstance(raw_hash, str):
        raise _schema_violation("metadata.raw_hash must be a string when present")

    expected_term_keys = {"term_code", "campus"}
    for idx, row in enumerate(terms, start=1):
        if not isinstance(row, dict):
            raise _schema_violation("terms rows must be objects", index=idx)
        keys = set(row.keys())
        if keys != expected_term_keys:
            raise _schema_violation(
                "terms row keys mismatch",
                index=idx,
                expected=sorted(expected_term_keys),
                got=sorted(keys),
            )
        if not isinstance(row["term_code"], str) or not row["term_code"].strip():
            raise _schema_violation("terms.term_code must be non-empty string", index=idx)
        if not isinstance(row["campus"], str) or not row["campus"].strip():
            raise _schema_violation("terms.campus must be non-empty string", index=idx)

    expected_offering_keys = {"term_code", "campus", "course_code", "offered"}
    for idx, row in enumerate(offerings, start=1):
        if not isinstance(row, dict):
            raise _schema_violation("offerings rows must be objects", index=idx)
        keys = set(row.keys())
        if keys != expected_offering_keys:
            raise _schema_violation(
                "offerings row keys mismatch",
                index=idx,
                expected=sorted(expected_offering_keys),
                got=sorted(keys),
            )
        if not isinstance(row["term_code"], str) or not row["term_code"].strip():
            raise _schema_violation("offerings.term_code must be non-empty string", index=idx)
        if not isinstance(row["campus"], str) or not row["campus"].strip():
            raise _schema_violation("offerings.campus must be non-empty string", index=idx)
        if not isinstance(row["course_code"], str) or not row["course_code"].strip():
            raise _schema_violation("offerings.course_code must be non-empty string", index=idx)
        if not isinstance(row["offered"], bool):
            raise _schema_violation("offerings.offered must be bool", index=idx)


def _canonical_json_bytes(obj: dict[str, Any]) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def canonicalize_soc_raw_payload(
    payload: dict[str, Any],
    *,
    term_code: str,
    campus: str,
) -> dict[str, Any]:
    terms_in = payload.get("terms", [])
    offerings_in = payload.get("offerings", [])
    metadata_in = payload.get("metadata") or {}

    terms = [
        {"term_code": str(row["term_code"]), "campus": str(row["campus"])}
        for row in terms_in
        if isinstance(row, dict)
    ]
    matching_terms = [row for row in terms if row["term_code"] == term_code and row["campus"] == campus]
    if len(matching_terms) != 1:
        raise _schema_violation(
            "terms must contain exactly one row matching requested slice",
            term_code=term_code,
            campus=campus,
            matched=len(matching_terms),
        )
    terms = matching_terms

    offerings: list[dict[str, Any]] = []
    for row in offerings_in:
        if not isinstance(row, dict):
            continue
        row_term = str(row.get("term_code", ""))
        row_campus = str(row.get("campus", ""))
        if row_term != term_code or row_campus != campus:
            continue
        offerings.append(
            {
                "term_code": row_term,
                "campus": row_campus,
                "course_code": str(row.get("course_code", "")),
                "offered": row["offered"],
            }
        )
    offerings.sort(key=lambda row: row["course_code"])

    source_urls_raw = metadata_in.get("source_urls", [])
    if not isinstance(source_urls_raw, list):
        source_urls_raw = []
    source_urls = sorted(str(x) for x in source_urls_raw)

    parse_warnings_raw = metadata_in.get("parse_warnings", [])
    if not isinstance(parse_warnings_raw, list):
        parse_warnings_raw = []
    parse_warnings = sorted(str(x) for x in parse_warnings_raw)

    fetched_at = metadata_in.get("fetched_at")
    if not isinstance(fetched_at, str) or not fetched_at.strip():
        raise _schema_violation("metadata.fetched_at must be a non-empty string")

    canonical_payload = {
        "terms": terms,
        "offerings": offerings,
        "metadata": {
            "source_urls": source_urls,
            "fetched_at": fetched_at,
            "parse_warnings": parse_warnings,
        },
    }

    payload_without_raw_hash = {
        "terms": canonical_payload["terms"],
        "offerings": canonical_payload["offerings"],
        "metadata": {
            "source_urls": canonical_payload["metadata"]["source_urls"],
            "fetched_at": canonical_payload["metadata"]["fetched_at"],
            "parse_warnings": canonical_payload["metadata"]["parse_warnings"],
        },
    }
    canonical_payload["metadata"]["raw_hash"] = sha256(_canonical_json_bytes(payload_without_raw_hash)).hexdigest()
    validate_soc_raw_payload(canonical_payload)
    return canonical_payload


FetchJsonFn = Callable[[str, dict[str, str], dict[str, str], float], Any]


def default_json_fetcher(
    url: str,
    params: dict[str, str],
    headers: dict[str, str],
    timeout_s: float,
) -> Any:
    hard_cap = min(float(timeout_s), WEBREG_REQUEST_TIMEOUT_S)
    timeout = httpx.Timeout(
        timeout=hard_cap,
        connect=min(WEBREG_CONNECT_TIMEOUT_S, hard_cap),
        read=min(WEBREG_READ_TIMEOUT_S, hard_cap),
        write=hard_cap,
        pool=min(WEBREG_CONNECT_TIMEOUT_S, hard_cap),
    )
    response = httpx.get(url, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError({"error_code": "SOC_FETCH_FAILED", "message": "Upstream JSON payload must be an object"})
    return payload


class BasePullAdapter(ABC):
    source_id: str

    def __init__(
        self,
        *,
        base_url: str,
        fetch_json: FetchJsonFn | None = None,
        timeout_s: float = 20.0,
        user_agent: str = "GradPath-SOC-Ingest/1.0",
    ):
        self.base_url = base_url
        self.fetch_json = fetch_json or default_json_fetcher
        self.timeout_s = timeout_s
        self.user_agent = user_agent

    @abstractmethod
    def build_params(self, *, term_code: str, campus: str) -> dict[str, str]:
        raise NotImplementedError

    def fetch(self, *, term_code: str, campus: str) -> SocFetchResult:
        fetch_started_at = datetime.now(tz=timezone.utc).isoformat()
        params = self.build_params(term_code=term_code, campus=campus)
        upstream = self.fetch_json(
            self.base_url,
            params,
            {"User-Agent": self.user_agent},
            self.timeout_s,
        )
        if not isinstance(upstream, dict):
            raise ValueError({"error_code": "SOC_FETCH_FAILED", "message": "payload must be an object"})

        payload_obj = upstream.get("payload")
        if payload_obj is None:
            payload_obj = {
                "terms": upstream.get("terms", []),
                "offerings": upstream.get("offerings", []),
                "metadata": upstream.get("metadata", {}),
            }
        if not isinstance(payload_obj, dict):
            raise ValueError({"error_code": "SOC_FETCH_FAILED", "message": "payload must be an object"})

        metadata = dict(payload_obj.get("metadata") or {})
        source_urls = metadata.get("source_urls", [self.base_url])
        if not isinstance(source_urls, list):
            source_urls = [self.base_url]
        metadata["source_urls"] = [str(x) for x in source_urls]
        upstream_fetched_at = metadata.get("fetched_at")
        if isinstance(upstream_fetched_at, str) and upstream_fetched_at.strip():
            metadata["fetched_at"] = upstream_fetched_at
        else:
            metadata["fetched_at"] = fetch_started_at
        parse_warnings = metadata.get("parse_warnings", [])
        if not isinstance(parse_warnings, list):
            parse_warnings = []
        metadata["parse_warnings"] = [str(x) for x in parse_warnings]

        raw_hash = metadata.get("raw_hash")
        if raw_hash is not None:
            metadata["raw_hash"] = str(raw_hash)

        payload = {
            "terms": payload_obj.get("terms", []),
            "offerings": payload_obj.get("offerings", []),
            "metadata": metadata,
        }
        validate_soc_raw_payload(payload)

        is_complete = upstream.get("is_complete")
        if not isinstance(is_complete, bool):
            is_complete = False
        completeness_reason = upstream.get("completeness_reason")
        if completeness_reason is None and not is_complete:
            completeness_reason = "UNKNOWN_COMPLETENESS"
        if completeness_reason is not None:
            completeness_reason = str(completeness_reason)
            if completeness_reason not in COMPLETENESS_REASONS:
                completeness_reason = "UNKNOWN_COMPLETENESS"

        return SocFetchResult(
            raw_payload=payload,
            is_complete=is_complete,
            completeness_reason=completeness_reason,
        )


class WebRegPullAdapter(BasePullAdapter):
    source_id = "WEBREG_PUBLIC"

    def __init__(
        self,
        *,
        base_url: str,
        fetch_json: FetchJsonFn | None = None,
        timeout_s: float = 20.0,
        user_agent: str = "GradPath-SOC-Ingest/1.0",
    ):
        super().__init__(
            base_url=base_url,
            fetch_json=fetch_json or self._fetch_json_allow_list,
            timeout_s=timeout_s,
            user_agent=user_agent,
        )

    def build_params(self, *, term_code: str, campus: str) -> dict[str, str]:
        return {"campus": campus, "term_code": term_code}

    def _fetch_json_allow_list(
        self,
        url: str,
        params: dict[str, str],
        headers: dict[str, str],
        timeout_s: float,
    ) -> Any:
        hard_cap = min(float(timeout_s), WEBREG_REQUEST_TIMEOUT_S)
        timeout = httpx.Timeout(
            timeout=hard_cap,
            connect=min(WEBREG_CONNECT_TIMEOUT_S, hard_cap),
            read=min(WEBREG_READ_TIMEOUT_S, hard_cap),
            write=hard_cap,
            pool=min(WEBREG_CONNECT_TIMEOUT_S, hard_cap),
        )
        response = httpx.get(url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, (dict, list)):
            raise ValueError({"error_code": "SOC_FETCH_FAILED", "message": "Upstream JSON payload must be an object or list"})
        return payload

    def _build_source_url(self, url: str, params: dict[str, str]) -> str:
        query = urlencode(sorted(params.items()))
        return f"{url}?{query}" if query else url

    def _extract_upstream_fetched_at(self, payload: Any) -> str | None:
        if not isinstance(payload, dict):
            return None
        metadata = payload.get("metadata")
        if isinstance(metadata, dict):
            fetched_at = metadata.get("fetched_at")
            if isinstance(fetched_at, str) and fetched_at.strip():
                return fetched_at
        fetched_at = payload.get("fetched_at")
        if isinstance(fetched_at, str) and fetched_at.strip():
            return fetched_at
        return None

    def _extract_course_rows(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            for key in ("courses", "results", "data"):
                rows = payload.get(key)
                if isinstance(rows, list):
                    return [row for row in rows if isinstance(row, dict)]
        return []

    def _has_any_pagination_fields(self, payload: dict[str, Any]) -> bool:
        return any(
            key in payload
            for key in (
                "next_cursor",
                "next",
                "cursor_next",
                "next_offset",
                "has_more",
                "offset",
                "limit",
                "total",
            )
        )

    def _map_term_code_to_soc_params(self, term_code: str) -> tuple[str, str] | None:
        if not isinstance(term_code, str):
            return None
        normalized = term_code.strip().upper()
        match = WEBREG_TERM_CODE_RE.match(normalized)
        if match:
            year, suffix = match.groups()
            term_map = {"SP": "1", "SU": "7", "FA": "9"}
            return (year, term_map[suffix]) if suffix in term_map else None
        numeric_match = WEBREG_NUMERIC_TERM_CODE_RE.match(normalized)
        if numeric_match:
            term, year = numeric_match.groups()
            return year, term
        return None

    def _remaining_budget(self, started_monotonic: float) -> float:
        return WEBREG_SLICE_BUDGET_S - (time.monotonic() - started_monotonic)

    def _request_json_with_resilience(
        self,
        *,
        url: str,
        params: dict[str, str],
        started_monotonic: float,
        request_cache: dict[tuple[str, tuple[tuple[str, str], ...]], Any],
    ) -> Any:
        cache_key = (
            url,
            tuple(sorted((str(k), str(v)) for k, v in params.items())),
        )
        if cache_key in request_cache:
            return request_cache[cache_key]

        headers = {"User-Agent": self.user_agent}
        last_exc: Exception | None = None

        for attempt in range(1, WEBREG_RETRY_ATTEMPTS + 1):
            if self._remaining_budget(started_monotonic) <= 0:
                raise _SliceBudgetExceeded("Slice time budget exceeded")
            try:
                payload = self.fetch_json(url, params, headers, WEBREG_REQUEST_TIMEOUT_S)
                request_cache[cache_key] = payload
                return payload
            except Exception as exc:
                last_exc = exc
                if attempt >= WEBREG_RETRY_ATTEMPTS or not _is_retryable_exception(exc):
                    raise
                delay = _compute_backoff_delay(attempt)
                remaining = self._remaining_budget(started_monotonic)
                if remaining <= 0:
                    raise _SliceBudgetExceeded("Slice time budget exceeded") from exc
                time.sleep(min(delay, remaining))

        assert last_exc is not None
        raise last_exc

    def _incomplete_result(
        self,
        *,
        term_code: str,
        campus: str,
        fetched_at: str,
        source_urls: list[str],
        parse_warnings: list[str],
        reason: str,
    ) -> SocFetchResult:
        completeness_reason = reason if reason in COMPLETENESS_REASONS else "UNKNOWN_COMPLETENESS"
        raw_payload = {
            "terms": [{"term_code": term_code, "campus": campus}],
            "offerings": [],
            "metadata": {
                "source_urls": source_urls,
                "parse_warnings": parse_warnings,
                "fetched_at": fetched_at,
            },
        }
        validate_soc_raw_payload(raw_payload)
        return SocFetchResult(raw_payload=raw_payload, is_complete=False, completeness_reason=completeness_reason)

    def _webreg_fetch_courses(
        self,
        term_mapping: TermMappingResult,
        *,
        started_monotonic: float,
        request_cache: dict[tuple[str, tuple[tuple[str, str], ...]], Any],
    ) -> PagePayload:
        courses_url = f"{self.base_url.rstrip('/')}/courses.json"
        params = {
            "year": term_mapping.soc_year,
            "term": term_mapping.soc_term,
            "campus": term_mapping.campus,
        }
        payload = self._request_json_with_resilience(
            url=courses_url,
            params=params,
            started_monotonic=started_monotonic,
            request_cache=request_cache,
        )
        return PagePayload(url=self._build_source_url(courses_url, params), payload=payload)

    def _determine_payload_completeness_reason(self, payload: Any) -> str | None:
        if not isinstance(payload, dict):
            return None

        if any(
            bool(payload.get(key))
            for key in ("truncated", "is_truncated", "limit_reached", "partial", "truncated_result")
        ):
            return "TRUNCATED_RESULT"
        if payload.get("incomplete") is True or payload.get("complete") is False:
            return "UPSTREAM_INCOMPLETE"

        if not self._has_any_pagination_fields(payload):
            return None

        has_more = payload.get("has_more")
        if has_more is True:
            return "PAGINATION_UNCERTAIN"
        if isinstance(has_more, bool) and has_more is False:
            return None

        if any(payload.get(key) is not None for key in ("next_cursor", "next", "cursor_next", "next_offset")):
            return "PAGINATION_UNCERTAIN"

        total = payload.get("total")
        offset = payload.get("offset")
        limit = payload.get("limit")
        if isinstance(total, int) and isinstance(offset, int) and isinstance(limit, int):
            return None if offset + limit >= total else "PAGINATION_UNCERTAIN"

        return "PAGINATION_UNCERTAIN"

    def _resolve_course_key(self, course: dict[str, Any]) -> tuple[str | None, str | None]:
        if "courseString" in course:
            course_string = course.get("courseString")
            if isinstance(course_string, str) and course_string != "":
                return course_string, None
            return None, "Course has invalid courseString"

        subject = course.get("subject")
        course_number = course.get("courseNumber")
        if isinstance(subject, str) and subject != "" and isinstance(course_number, str) and course_number != "":
            return f"{subject}:{course_number}", None
        return None, "Course is missing identity fields (courseString or subject/courseNumber)"

    def fetch(self, *, term_code: str, campus: str) -> SocFetchResult:
        fetch_started_at = datetime.now(tz=timezone.utc).isoformat()
        started_monotonic = time.monotonic()
        request_cache: dict[tuple[str, tuple[tuple[str, str], ...]], Any] = {}
        source_urls: list[str] = []
        parse_warnings: list[str] = []

        mapped = self._map_term_code_to_soc_params(term_code)
        if mapped is None:
            return self._incomplete_result(
                term_code=term_code,
                campus=campus,
                fetched_at=fetch_started_at,
                source_urls=source_urls,
                parse_warnings=parse_warnings,
                reason="AMBIGUOUS_TERM",
            )
        term_mapping = TermMappingResult(
            soc_year=mapped[0],
            soc_term=mapped[1],
            term_code=term_code,
            campus=campus,
        )

        try:
            page = self._webreg_fetch_courses(
                term_mapping,
                started_monotonic=started_monotonic,
                request_cache=request_cache,
            )
            source_urls.append(page.url)
            upstream_fetched_at = self._extract_upstream_fetched_at(page.payload) or fetch_started_at
        except _SliceBudgetExceeded:
            return self._incomplete_result(
                term_code=term_code,
                campus=campus,
                fetched_at=fetch_started_at,
                source_urls=source_urls,
                parse_warnings=parse_warnings,
                reason="UNKNOWN_COMPLETENESS",
            )

        if not isinstance(page.payload, (dict, list)):
            parse_warnings.append("Upstream courses payload must be a list or object containing list rows")
            return self._incomplete_result(
                term_code=term_code,
                campus=campus,
                fetched_at=upstream_fetched_at,
                source_urls=source_urls,
                parse_warnings=parse_warnings,
                reason="UNKNOWN_COMPLETENESS",
            )
        if isinstance(page.payload, dict):
            payload_reason = self._determine_payload_completeness_reason(page.payload)
            if payload_reason is not None:
                return self._incomplete_result(
                    term_code=term_code,
                    campus=campus,
                    fetched_at=upstream_fetched_at,
                    source_urls=source_urls,
                    parse_warnings=parse_warnings,
                    reason=payload_reason,
                )

        courses = self._extract_course_rows(page.payload)

        offered_by_course: dict[str, bool] = {}
        for course in courses:
            course_key, course_key_error = self._resolve_course_key(course)
            if course_key is None:
                parse_warnings.append(course_key_error or "Course identity could not be resolved")
                return self._incomplete_result(
                    term_code=term_code,
                    campus=campus,
                    fetched_at=upstream_fetched_at,
                    source_urls=source_urls,
                    parse_warnings=parse_warnings,
                    reason="UNKNOWN_COMPLETENESS",
                )

            sections = course.get("sections")
            if not isinstance(sections, list):
                parse_warnings.append(f"Course {course_key} has invalid sections payload")
                return self._incomplete_result(
                    term_code=term_code,
                    campus=campus,
                    fetched_at=upstream_fetched_at,
                    source_urls=source_urls,
                    parse_warnings=parse_warnings,
                    reason="UNKNOWN_COMPLETENESS",
                )

            has_open_section = offered_by_course.get(course_key, False)
            for section in sections:
                if not isinstance(section, dict):
                    parse_warnings.append(f"Course {course_key} has non-object section rows")
                    return self._incomplete_result(
                        term_code=term_code,
                        campus=campus,
                        fetched_at=upstream_fetched_at,
                        source_urls=source_urls,
                        parse_warnings=parse_warnings,
                        reason="UNKNOWN_COMPLETENESS",
                    )
                open_status = section.get("openStatus")
                if not isinstance(open_status, bool):
                    parse_warnings.append(f"Course {course_key} has non-bool openStatus")
                    return self._incomplete_result(
                        term_code=term_code,
                        campus=campus,
                        fetched_at=upstream_fetched_at,
                        source_urls=source_urls,
                        parse_warnings=parse_warnings,
                        reason="UNKNOWN_COMPLETENESS",
                    )
                has_open_section = has_open_section or open_status
            offered_by_course[course_key] = has_open_section

        all_offerings: list[OfferingRow] = [
            {
                "term_code": term_code,
                "campus": campus,
                "course_code": course_code,
                "offered": offered,
            }
            for course_code, offered in sorted(offered_by_course.items(), key=lambda x: x[0])
        ]
        raw_payload = {
            "terms": [{"term_code": term_code, "campus": campus}],
            "offerings": all_offerings,
            "metadata": {
                "source_urls": source_urls,
                "parse_warnings": [str(x) for x in parse_warnings],
                "fetched_at": upstream_fetched_at,
            },
        }
        validate_soc_raw_payload(raw_payload)
        return SocFetchResult(raw_payload=raw_payload, is_complete=True, completeness_reason=None)


class CspPullAdapter(BasePullAdapter):
    source_id = "CSP_PUBLIC"

    def build_params(self, *, term_code: str, campus: str) -> dict[str, str]:
        return {"campus": campus, "term_code": term_code}


class DegreeNavigatorPullAdapter(BasePullAdapter):
    source_id = "DEGREE_NAVIGATOR_PUBLIC"

    def build_params(self, *, term_code: str, campus: str) -> dict[str, str]:
        return {"campus": campus, "term_code": term_code}
