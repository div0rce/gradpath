from __future__ import annotations

import pytest

from app.services.soc_pull import SocFetchResult
from app.services.soc_runner import fetch_raw_payload_for_slice, stage_soc_slice


class _FakeAdapter:
    def __init__(self, result: SocFetchResult | None = None, error: Exception | None = None):
        self._result = result
        self._error = error

    def fetch(self, *, term_code: str, campus: str) -> SocFetchResult:
        _ = term_code
        _ = campus
        if self._error:
            raise self._error
        assert self._result is not None
        return self._result


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self) -> dict:
        return self._payload


class _FakeClient:
    def __init__(self, responses: list[_FakeResponse]):
        self.responses = list(responses)
        self.calls: list[dict] = []

    def post(self, url: str, json: dict | None = None, headers: dict | None = None):
        self.calls.append({"url": url, "json": json, "headers": headers})
        return self.responses.pop(0)


def _complete_payload() -> dict:
    return {
        "terms": [{"term_code": "2025SU", "campus": "NB"}],
        "offerings": [{"term_code": "2025SU", "campus": "NB", "course_code": "14:540:100", "offered": True}],
        "metadata": {"source_urls": [], "parse_warnings": [], "fetched_at": "2026-02-09T00:00:00Z"},
    }


def test_fetch_raw_payload_for_slice_uses_first_complete_source():
    adapters = {
        "WEBREG_PUBLIC": _FakeAdapter(
            result=SocFetchResult(
                raw_payload=_complete_payload(),
                is_complete=False,
                completeness_reason="PAGINATION_UNCERTAIN",
            )
        ),
        "CSP_PUBLIC": _FakeAdapter(
            result=SocFetchResult(
                raw_payload=_complete_payload(),
                is_complete=True,
                completeness_reason=None,
            )
        ),
    }
    source_used, payload = fetch_raw_payload_for_slice(
        campus="NB",
        term_code="2025SU",
        source_priority=["WEBREG_PUBLIC", "CSP_PUBLIC"],
        adapters=adapters,
    )
    assert source_used == "CSP_PUBLIC"
    assert payload["offerings"][0]["course_code"] == "14:540:100"
    assert isinstance(payload["metadata"]["raw_hash"], str)


def test_fetch_raw_payload_for_slice_raises_when_all_sources_fail_or_incomplete():
    adapters = {
        "WEBREG_PUBLIC": _FakeAdapter(error=ValueError({"error_code": "SOC_FETCH_FAILED", "message": "boom"})),
        "CSP_PUBLIC": _FakeAdapter(
            result=SocFetchResult(
                raw_payload=_complete_payload(),
                is_complete=False,
                completeness_reason="UPSTREAM_INCOMPLETE",
            )
        ),
    }
    with pytest.raises(ValueError) as exc_info:
        fetch_raw_payload_for_slice(
            campus="NB",
            term_code="2025SU",
            source_priority=["WEBREG_PUBLIC", "CSP_PUBLIC"],
            adapters=adapters,
        )
    detail = exc_info.value.args[0]
    assert detail["error_code"] == "SOC_FETCH_FAILED"
    assert len(detail["attempts"]) == 2


def test_fetch_raw_payload_for_slice_blocks_when_complete_but_reason_present():
    adapters = {
        "WEBREG_PUBLIC": _FakeAdapter(
            result=SocFetchResult(
                raw_payload=_complete_payload(),
                is_complete=True,
                completeness_reason="UPSTREAM_INCOMPLETE",
            )
        ),
    }
    with pytest.raises(ValueError) as exc_info:
        fetch_raw_payload_for_slice(
            campus="NB",
            term_code="2025SU",
            source_priority=["WEBREG_PUBLIC"],
            adapters=adapters,
        )
    detail = exc_info.value.args[0]
    assert detail["error_code"] == "UPSTREAM_INCOMPLETE"
    assert detail["attempts"][0]["completeness_reason"] == "UPSTREAM_INCOMPLETE"


def test_fetch_raw_payload_for_slice_normalizes_missing_reason_to_unknown():
    adapters = {
        "WEBREG_PUBLIC": _FakeAdapter(
            result=SocFetchResult(
                raw_payload=_complete_payload(),
                is_complete=False,
                completeness_reason=None,
            )
        ),
    }
    with pytest.raises(ValueError) as exc_info:
        fetch_raw_payload_for_slice(
            campus="NB",
            term_code="2025SU",
            source_priority=["WEBREG_PUBLIC"],
            adapters=adapters,
        )
    detail = exc_info.value.args[0]
    assert detail["error_code"] == "UPSTREAM_INCOMPLETE"
    assert detail["attempts"][0]["completeness_reason"] == "UNKNOWN_COMPLETENESS"


def test_stage_soc_slice_dry_run_parity_and_header():
    client = _FakeClient(
        responses=[
            _FakeResponse(200, {"result": {"checksum": "abc", "noop": False}, "snapshot": {"snapshot_id": "1"}}),
            _FakeResponse(200, {"result": {"checksum": "abc", "noop": False}, "snapshot": {"snapshot_id": "2"}}),
        ]
    )
    body = stage_soc_slice(
        api_base="",
        campus="NB",
        term_code="2025SU",
        ingest_source="WEBREG_PUBLIC",
        raw_payload=_complete_payload(),
        run_id="run-123",
        dry_run_first=True,
        client=client,
    )
    assert body["snapshot"]["snapshot_id"] == "2"
    assert len(client.calls) == 2
    assert all(c["url"].endswith("/v1/catalog/snapshots:stage-from-soc") for c in client.calls)
    assert all(c["headers"]["X-SOC-RUN-ID"] == "run-123" for c in client.calls)
    assert all(":promote" not in c["url"] for c in client.calls)


def test_stage_soc_slice_raises_on_dry_run_parity_mismatch():
    client = _FakeClient(
        responses=[
            _FakeResponse(200, {"result": {"checksum": "a", "noop": True}, "snapshot": {"snapshot_id": "1"}}),
            _FakeResponse(200, {"result": {"checksum": "b", "noop": True}, "snapshot": {"snapshot_id": "2"}}),
        ]
    )
    with pytest.raises(ValueError) as exc_info:
        stage_soc_slice(
            api_base="",
            campus="NB",
            term_code="2025SU",
            ingest_source="WEBREG_PUBLIC",
            raw_payload=_complete_payload(),
            run_id="run-123",
            dry_run_first=True,
            client=client,
        )
    assert exc_info.value.args[0]["error_code"] == "SOC_PARITY_MISMATCH"
