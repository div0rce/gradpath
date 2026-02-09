from __future__ import annotations

from collections.abc import Iterable
import os
from typing import Any
from uuid import uuid4

import httpx

from app.services.soc_pull import (
    COMPLETENESS_REASONS,
    CspPullAdapter,
    DegreeNavigatorPullAdapter,
    SocFetchResult,
    WebRegPullAdapter,
    canonicalize_soc_raw_payload,
    validate_soc_raw_payload,
)

SOC_STAGE_PATH = "/v1/catalog/snapshots:stage-from-soc"

SOURCE_ALIASES = {
    "WEBREG_PUBLIC": "WEBREG_PUBLIC",
    "webreg": "WEBREG_PUBLIC",
    "CSP_PUBLIC": "CSP_PUBLIC",
    "csp": "CSP_PUBLIC",
    "DEGREE_NAVIGATOR_PUBLIC": "DEGREE_NAVIGATOR_PUBLIC",
    "degree_navigator": "DEGREE_NAVIGATOR_PUBLIC",
}


def _normalize_source(source: str) -> str:
    return SOURCE_ALIASES.get(source, source)


def _detail_from_exception(exc: Exception) -> dict[str, Any]:
    if exc.args and isinstance(exc.args[0], dict):
        return exc.args[0]
    return {"error_code": "SOC_FETCH_FAILED", "message": str(exc)}


def _normalize_completeness_reason(reason: Any) -> str:
    if isinstance(reason, str) and reason in COMPLETENESS_REASONS:
        return reason
    return "UNKNOWN_COMPLETENESS"


def build_default_adapters() -> dict[str, Any]:
    return {
        "WEBREG_PUBLIC": WebRegPullAdapter(base_url=os.getenv("WEBREG_SOC_URL", "https://webreg.rutgers.edu")),
        "CSP_PUBLIC": CspPullAdapter(base_url=os.getenv("CSP_SOC_URL", "https://sims.rutgers.edu/csp")),
        "DEGREE_NAVIGATOR_PUBLIC": DegreeNavigatorPullAdapter(
            base_url=os.getenv("DEGREE_NAV_SOC_URL", "https://dn.rutgers.edu")
        ),
    }


def fetch_raw_payload_for_slice(
    *,
    campus: str,
    term_code: str,
    source_priority: Iterable[str],
    adapters: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    adapter_map = adapters or build_default_adapters()
    attempts: list[dict[str, Any]] = []

    for source in source_priority:
        source_key = _normalize_source(source)
        adapter = adapter_map.get(source_key)
        if not adapter:
            attempts.append({"source": source_key, "error_code": "SOC_FETCH_FAILED", "message": "Unknown source"})
            continue

        try:
            result: SocFetchResult = adapter.fetch(term_code=term_code, campus=campus)
            validate_soc_raw_payload(result.raw_payload)
            payload = canonicalize_soc_raw_payload(result.raw_payload, term_code=term_code, campus=campus)

            if result.is_complete is False:
                attempts.append(
                    {
                        "source": source_key,
                        "error_code": "UPSTREAM_INCOMPLETE",
                        "completeness_reason": _normalize_completeness_reason(result.completeness_reason),
                    }
                )
                continue

            if result.completeness_reason is not None:
                attempts.append(
                    {
                        "source": source_key,
                        "error_code": "UPSTREAM_INCOMPLETE",
                        "completeness_reason": _normalize_completeness_reason(result.completeness_reason),
                    }
                )
                continue

            return source_key, payload
        except Exception as exc:
            detail = _detail_from_exception(exc)
            detail["source"] = source_key
            attempts.append(detail)

    if attempts and all(attempt.get("error_code") == "UPSTREAM_INCOMPLETE" for attempt in attempts):
        raise ValueError(
            {
                "error_code": "UPSTREAM_INCOMPLETE",
                "campus": campus,
                "term_code": term_code,
                "attempts": attempts,
            }
        )

    raise ValueError(
        {
            "error_code": "SOC_FETCH_FAILED",
            "campus": campus,
            "term_code": term_code,
            "attempts": attempts,
        }
    )


def _post_stage(
    *,
    client: Any,
    target: str,
    body: dict[str, Any],
    headers: dict[str, str],
) -> dict[str, Any]:
    response = client.post(target, json=body, headers=headers)
    if response.status_code >= 400:
        try:
            detail = response.json()
        except Exception:
            detail = {"message": response.text}
        raise ValueError({"error_code": "SOC_STAGE_FAILED", "status_code": response.status_code, "detail": detail})
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError({"error_code": "SOC_STAGE_FAILED", "message": "Invalid stage response payload"})
    return payload


def stage_soc_slice(
    *,
    api_base: str,
    campus: str,
    term_code: str,
    ingest_source: str,
    raw_payload: dict[str, Any],
    run_id: str | None = None,
    dry_run_first: bool = False,
    source_metadata: dict[str, Any] | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    stage_target = f"{api_base.rstrip('/')}{SOC_STAGE_PATH}" if api_base else SOC_STAGE_PATH
    run_id_value = run_id or str(uuid4())
    headers = {"X-SOC-RUN-ID": run_id_value}
    metadata = source_metadata or {}

    body_base = {
        "term_code": term_code,
        "campus": campus,
        "ingest_source": ingest_source,
        "raw_payload": raw_payload,
        "source_metadata": metadata,
    }

    if client is None:
        with httpx.Client(timeout=30.0) as http_client:
            return _stage_with_optional_parity(
                http_client=http_client,
                target=stage_target,
                headers=headers,
                body_base=body_base,
                dry_run_first=dry_run_first,
            )

    return _stage_with_optional_parity(
        http_client=client,
        target=stage_target,
        headers=headers,
        body_base=body_base,
        dry_run_first=dry_run_first,
    )


def _stage_with_optional_parity(
    *,
    http_client: Any,
    target: str,
    headers: dict[str, str],
    body_base: dict[str, Any],
    dry_run_first: bool,
) -> dict[str, Any]:
    if not dry_run_first:
        return _post_stage(client=http_client, target=target, body={**body_base, "dry_run": False}, headers=headers)

    dry = _post_stage(client=http_client, target=target, body={**body_base, "dry_run": True}, headers=headers)
    stage = _post_stage(client=http_client, target=target, body={**body_base, "dry_run": False}, headers=headers)

    dry_result = dry.get("result", {}) if isinstance(dry, dict) else {}
    stage_result = stage.get("result", {}) if isinstance(stage, dict) else {}
    if dry_result.get("checksum") != stage_result.get("checksum") or dry_result.get("noop") != stage_result.get("noop"):
        raise ValueError(
            {
                "error_code": "SOC_PARITY_MISMATCH",
                "dry_run": {"checksum": dry_result.get("checksum"), "noop": dry_result.get("noop")},
                "stage": {"checksum": stage_result.get("checksum"), "noop": stage_result.get("noop")},
            }
        )
    return stage
