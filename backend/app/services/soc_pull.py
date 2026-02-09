from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from hashlib import sha256
import random
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


Cursor = str | int


@dataclass(frozen=True)
class TermMappingResult:
    term_identifier: str
    term_code: str
    campus: str


@dataclass(frozen=True)
class PagePayload:
    url: str
    payload: dict[str, Any]


OfferingRow = dict[str, Any]

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


FetchJsonFn = Callable[[str, dict[str, str], dict[str, str], float], dict[str, Any]]


def default_json_fetcher(
    url: str,
    params: dict[str, str],
    headers: dict[str, str],
    timeout_s: float,
) -> dict[str, Any]:
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

    def build_params(self, *, term_code: str, campus: str) -> dict[str, str]:
        return {"campus": campus, "term_code": term_code}

    def _build_source_url(self, url: str, params: dict[str, str]) -> str:
        query = urlencode(sorted(params.items()))
        return f"{url}?{query}" if query else url

    def _extract_upstream_fetched_at(self, payload: dict[str, Any]) -> str | None:
        metadata = payload.get("metadata")
        if isinstance(metadata, dict):
            fetched_at = metadata.get("fetched_at")
            if isinstance(fetched_at, str) and fetched_at.strip():
                return fetched_at
        fetched_at = payload.get("fetched_at")
        if isinstance(fetched_at, str) and fetched_at.strip():
            return fetched_at
        return None

    def _extract_term_rows(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        for key in ("terms", "results", "data"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
        return []

    def _extract_offering_rows(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        for key in ("offerings", "results", "data"):
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

    def _remaining_budget(self, started_monotonic: float) -> float:
        return WEBREG_SLICE_BUDGET_S - (time.monotonic() - started_monotonic)

    def _request_json_with_resilience(
        self,
        *,
        url: str,
        params: dict[str, str],
        started_monotonic: float,
        request_cache: dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, Any]],
    ) -> dict[str, Any]:
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
                if not isinstance(payload, dict):
                    raise ValueError({"error_code": "SOC_FETCH_FAILED", "message": "Upstream payload must be an object"})
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

    def _webreg_discover_term(
        self,
        term_code: str,
        campus: str,
        *,
        started_monotonic: float,
        request_cache: dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, Any]],
    ) -> tuple[TermMappingResult | None, str, dict[str, Any]]:
        terms_url = f"{self.base_url.rstrip('/')}/terms"
        params = {"term_code": term_code, "campus": campus}
        payload = self._request_json_with_resilience(
            url=terms_url,
            params=params,
            started_monotonic=started_monotonic,
            request_cache=request_cache,
        )
        source_url = self._build_source_url(terms_url, params)
        rows = self._extract_term_rows(payload)
        matches: list[TermMappingResult] = []
        for row in rows:
            row_term = str(row.get("term_code") or row.get("code") or "")
            row_campus = str(row.get("campus") or "")
            if row_term != term_code or row_campus != campus:
                continue
            term_identifier = str(row.get("id") or row.get("term_id") or row_term)
            if not term_identifier:
                continue
            matches.append(
                TermMappingResult(
                    term_identifier=term_identifier,
                    term_code=term_code,
                    campus=campus,
                )
            )
        if len(matches) != 1:
            return None, source_url, payload
        return matches[0], source_url, payload

    def _webreg_fetch_page(
        self,
        term_mapping: TermMappingResult,
        cursor_or_offset: Cursor | None,
        *,
        started_monotonic: float,
        request_cache: dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, Any]],
    ) -> PagePayload:
        page_url = f"{self.base_url.rstrip('/')}/offerings"
        params = {
            "term_code": term_mapping.term_code,
            "campus": term_mapping.campus,
            "term_id": term_mapping.term_identifier,
        }
        if cursor_or_offset is not None:
            if isinstance(cursor_or_offset, int):
                params["offset"] = str(cursor_or_offset)
            else:
                params["cursor"] = str(cursor_or_offset)
        payload = self._request_json_with_resilience(
            url=page_url,
            params=params,
            started_monotonic=started_monotonic,
            request_cache=request_cache,
        )
        return PagePayload(url=self._build_source_url(page_url, params), payload=payload)

    def _webreg_parse_page(
        self,
        page_payload: PagePayload,
        *,
        term_code: str,
        campus: str,
    ) -> tuple[list[OfferingRow], list[str], bool, Cursor | None]:
        rows = self._extract_offering_rows(page_payload.payload)
        warnings: list[str] = []
        offerings: list[OfferingRow] = []
        for row in rows:
            course_code = row.get("course_code") or row.get("code")
            if not isinstance(course_code, str) or not course_code.strip():
                warnings.append("Skipping offering row with missing course_code")
                continue

            offered_raw = row.get("offered")
            offered_value: bool
            if isinstance(offered_raw, bool):
                offered_value = offered_raw
            elif isinstance(offered_raw, str):
                lowered = offered_raw.strip().lower()
                if lowered in {"true", "1", "yes", "y"}:
                    offered_value = True
                elif lowered in {"false", "0", "no", "n"}:
                    offered_value = False
                else:
                    warnings.append(f"Skipping offering row with unsupported offered value for {course_code}")
                    continue
            elif isinstance(offered_raw, int):
                if offered_raw in (0, 1):
                    offered_value = bool(offered_raw)
                else:
                    warnings.append(f"Skipping offering row with unsupported offered value for {course_code}")
                    continue
            else:
                warnings.append(f"Skipping offering row with missing offered value for {course_code}")
                continue

            offerings.append(
                {
                    "term_code": term_code,
                    "campus": campus,
                    "course_code": course_code.strip(),
                    "offered": offered_value,
                }
            )

        payload = page_payload.payload
        truncation_signal = any(
            bool(payload.get(key))
            for key in ("truncated", "is_truncated", "limit_reached", "partial", "truncated_result")
        )

        next_cursor: Cursor | None = None
        if isinstance(payload.get("next_cursor"), (str, int)):
            next_cursor = payload.get("next_cursor")
        elif isinstance(payload.get("next"), (str, int)):
            next_cursor = payload.get("next")
        elif isinstance(payload.get("cursor_next"), (str, int)):
            next_cursor = payload.get("cursor_next")
        elif isinstance(payload.get("next_offset"), int):
            next_cursor = int(payload["next_offset"])
        elif isinstance(payload.get("offset"), int) and isinstance(payload.get("limit"), int):
            if payload.get("has_more") is True:
                next_cursor = int(payload["offset"]) + int(payload["limit"])

        parse_warnings = payload.get("parse_warnings")
        if isinstance(parse_warnings, list):
            warnings.extend(str(x) for x in parse_warnings)

        return offerings, warnings, truncation_signal, next_cursor

    def fetch(self, *, term_code: str, campus: str) -> SocFetchResult:
        fetch_started_at = datetime.now(tz=timezone.utc).isoformat()
        started_monotonic = time.monotonic()
        request_cache: dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, Any]] = {}
        source_urls: list[str] = []
        parse_warnings: list[str] = []

        try:
            term_mapping, discovery_url, discovery_payload = self._webreg_discover_term(
                term_code,
                campus,
                started_monotonic=started_monotonic,
                request_cache=request_cache,
            )
            source_urls.append(discovery_url)
            upstream_fetched_at = self._extract_upstream_fetched_at(discovery_payload) or fetch_started_at
        except _SliceBudgetExceeded:
            return self._incomplete_result(
                term_code=term_code,
                campus=campus,
                fetched_at=fetch_started_at,
                source_urls=source_urls,
                parse_warnings=parse_warnings,
                reason="UNKNOWN_COMPLETENESS",
            )

        if term_mapping is None:
            return self._incomplete_result(
                term_code=term_code,
                campus=campus,
                fetched_at=upstream_fetched_at,
                source_urls=source_urls,
                parse_warnings=parse_warnings,
                reason="AMBIGUOUS_TERM",
            )

        cursor: Cursor | None = None
        seen_cursors: set[tuple[str, str]] = set()
        all_offerings: list[OfferingRow] = []
        saw_truncation = False
        saw_upstream_incomplete = False

        while True:
            try:
                page = self._webreg_fetch_page(
                    term_mapping,
                    cursor,
                    started_monotonic=started_monotonic,
                    request_cache=request_cache,
                )
            except _SliceBudgetExceeded:
                return self._incomplete_result(
                    term_code=term_code,
                    campus=campus,
                    fetched_at=upstream_fetched_at,
                    source_urls=source_urls,
                    parse_warnings=parse_warnings,
                    reason="UNKNOWN_COMPLETENESS",
                )
            source_urls.append(page.url)
            if upstream_fetched_at == fetch_started_at:
                maybe_upstream_ts = self._extract_upstream_fetched_at(page.payload)
                if maybe_upstream_ts:
                    upstream_fetched_at = maybe_upstream_ts

            offerings, warnings, truncation_signal, next_cursor = self._webreg_parse_page(
                page,
                term_code=term_code,
                campus=campus,
            )
            all_offerings.extend(offerings)
            parse_warnings.extend(warnings)
            saw_truncation = saw_truncation or truncation_signal

            payload = page.payload
            has_more = payload.get("has_more")
            if payload.get("incomplete") is True or payload.get("complete") is False:
                saw_upstream_incomplete = True

            if next_cursor is not None:
                cursor_key = (type(next_cursor).__name__, str(next_cursor))
                if cursor_key in seen_cursors:
                    return self._incomplete_result(
                        term_code=term_code,
                        campus=campus,
                        fetched_at=upstream_fetched_at,
                        source_urls=source_urls,
                        parse_warnings=parse_warnings,
                        reason="PAGINATION_UNCERTAIN",
                    )
                seen_cursors.add(cursor_key)
                cursor = next_cursor
                continue

            if isinstance(has_more, bool):
                if has_more:
                    return self._incomplete_result(
                        term_code=term_code,
                        campus=campus,
                        fetched_at=upstream_fetched_at,
                        source_urls=source_urls,
                        parse_warnings=parse_warnings,
                        reason="PAGINATION_UNCERTAIN",
                    )
                break

            total = payload.get("total")
            offset = payload.get("offset")
            limit = payload.get("limit")
            if isinstance(total, int) and isinstance(offset, int) and isinstance(limit, int):
                if offset + limit < total:
                    return self._incomplete_result(
                        term_code=term_code,
                        campus=campus,
                        fetched_at=upstream_fetched_at,
                        source_urls=source_urls,
                        parse_warnings=parse_warnings,
                        reason="PAGINATION_UNCERTAIN",
                    )
                break

            # No pagination fields at all implies a deterministic single-page response.
            if not self._has_any_pagination_fields(payload):
                break

            return self._incomplete_result(
                term_code=term_code,
                campus=campus,
                fetched_at=upstream_fetched_at,
                source_urls=source_urls,
                parse_warnings=parse_warnings,
                reason="UNKNOWN_COMPLETENESS",
            )

        if saw_truncation:
            return self._incomplete_result(
                term_code=term_code,
                campus=campus,
                fetched_at=upstream_fetched_at,
                source_urls=source_urls,
                parse_warnings=parse_warnings,
                reason="TRUNCATED_RESULT",
            )

        if saw_upstream_incomplete:
            return self._incomplete_result(
                term_code=term_code,
                campus=campus,
                fetched_at=upstream_fetched_at,
                source_urls=source_urls,
                parse_warnings=parse_warnings,
                reason="UPSTREAM_INCOMPLETE",
            )

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
