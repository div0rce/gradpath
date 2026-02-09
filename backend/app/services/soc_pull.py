from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from hashlib import sha256
from typing import Any, Callable

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


def _schema_violation(message: str, **extra: Any) -> ValueError:
    detail: dict[str, Any] = {"error_code": "SOC_SCHEMA_VIOLATION", "message": message}
    if extra:
        detail.update(extra)
    return ValueError(detail)


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
    if fetched_at is not None and not isinstance(fetched_at, str):
        raise _schema_violation("metadata.fetched_at must be a string when present")

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
                "offered": bool(row.get("offered", False)),
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
    if fetched_at is None:
        fetched_at = datetime.now(tz=timezone.utc).isoformat()

    canonical_payload = {
        "terms": terms,
        "offerings": offerings,
        "metadata": {
            "source_urls": source_urls,
            "fetched_at": str(fetched_at),
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
    response = httpx.get(url, params=params, headers=headers, timeout=timeout_s)
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
        metadata.setdefault("fetched_at", datetime.now(tz=timezone.utc).isoformat())
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


class CspPullAdapter(BasePullAdapter):
    source_id = "CSP_PUBLIC"

    def build_params(self, *, term_code: str, campus: str) -> dict[str, str]:
        return {"campus": campus, "term_code": term_code}


class DegreeNavigatorPullAdapter(BasePullAdapter):
    source_id = "DEGREE_NAVIGATOR_PUBLIC"

    def build_params(self, *, term_code: str, campus: str) -> dict[str, str]:
        return {"campus": campus, "term_code": term_code}
