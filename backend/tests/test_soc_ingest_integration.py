from __future__ import annotations

from datetime import datetime

import pytest
from sqlalchemy import select

from app.enums import CatalogSnapshotStatus
from app.db import SessionLocal
from app.models import CatalogSnapshot, Term
from app.services.soc_pull import SocFetchResult
from app.services.soc_runner import fetch_raw_payload_for_slice, stage_soc_slice
from tests.helpers import stage_payload_ready


class _FakeAdapter:
    def __init__(self, result: SocFetchResult):
        self.result = result

    def fetch(self, *, term_code: str, campus: str) -> SocFetchResult:
        _ = term_code
        _ = campus
        return self.result


class _RecordingClient:
    def __init__(self, client):
        self.client = client
        self.paths: list[str] = []

    def post(self, url: str, json: dict | None = None, headers: dict | None = None):
        self.paths.append(url)
        return self.client.post(url, json=json, headers=headers)


def _seed_baseline_snapshot(client) -> tuple[str, str]:
    stage = client.post("/v1/catalog/snapshots:stage", json=stage_payload_ready())
    assert stage.status_code == 200, stage.text
    snapshot_id = stage.json()["snapshot_id"]
    promote = client.post(f"/v1/catalog/snapshots/{snapshot_id}:promote")
    assert promote.status_code == 200, promote.text
    with SessionLocal() as db:
        term = db.execute(select(Term).where(Term.code == "2025SU")).scalars().first()
    assert term is not None
    return snapshot_id, term.id


def test_runner_stage_then_noop_for_unchanged_payload(client):
    _baseline_snapshot_id, _term_id = _seed_baseline_snapshot(client)

    payload = {
        "terms": [{"term_code": "2025SU", "campus": "NB"}],
        "offerings": [
            {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:100", "offered": True},
            {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:200", "offered": True},
        ],
        "metadata": {"source_urls": ["https://source"], "parse_warnings": [], "fetched_at": "2026-02-09T00:00:00Z"},
    }
    adapters = {
        "WEBREG_PUBLIC": _FakeAdapter(
            SocFetchResult(raw_payload=payload, is_complete=True, completeness_reason=None)
        )
    }
    source_used, raw_payload = fetch_raw_payload_for_slice(
        campus="NB",
        term_code="2025SU",
        source_priority=["WEBREG_PUBLIC"],
        adapters=adapters,
    )
    recording_client = _RecordingClient(client)

    first = stage_soc_slice(
        api_base="",
        campus="NB",
        term_code="2025SU",
        ingest_source=source_used,
        raw_payload=raw_payload,
        dry_run_first=False,
        run_id="run-1",
        client=recording_client,
    )
    assert first["result"]["noop"] is False
    assert first["snapshot"]["status"] == "STAGED"
    first_snapshot_id = first["snapshot"]["snapshot_id"]
    with SessionLocal() as db:
        first_snapshot = db.get(CatalogSnapshot, first_snapshot_id)
        assert first_snapshot is not None
        first_snapshot.status = CatalogSnapshotStatus.PUBLISHED
        first_snapshot.published_at = datetime.utcnow()
        db.add(first_snapshot)
        db.commit()

    second = stage_soc_slice(
        api_base="",
        campus="NB",
        term_code="2025SU",
        ingest_source=source_used,
        raw_payload=raw_payload,
        dry_run_first=False,
        run_id="run-2",
        client=recording_client,
    )
    assert second["result"]["noop"] is True
    assert all(":promote" not in path for path in recording_client.paths)


def test_runner_does_not_stage_when_source_is_incomplete(client):
    adapters = {
        "WEBREG_PUBLIC": _FakeAdapter(
            SocFetchResult(
                raw_payload={
                    "terms": [{"term_code": "2025SU", "campus": "NB"}],
                    "offerings": [],
                    "metadata": {"source_urls": [], "parse_warnings": [], "fetched_at": "2026-02-09T00:00:00Z"},
                },
                is_complete=False,
                completeness_reason="UPSTREAM_INCOMPLETE",
            )
        )
    }
    recording_client = _RecordingClient(client)

    with pytest.raises(ValueError) as exc_info:
        source_used, raw_payload = fetch_raw_payload_for_slice(
            campus="NB",
            term_code="2025SU",
            source_priority=["WEBREG_PUBLIC"],
            adapters=adapters,
        )
        stage_soc_slice(
            api_base="",
            campus="NB",
            term_code="2025SU",
            ingest_source=source_used,
            raw_payload=raw_payload,
            dry_run_first=False,
            run_id="run-blocked",
            client=recording_client,
        )
    detail = exc_info.value.args[0]
    assert detail["error_code"] == "UPSTREAM_INCOMPLETE"
    attempts = detail.get("attempts") or []
    assert attempts[0]["error_code"] == "UPSTREAM_INCOMPLETE"
    assert attempts[0]["completeness_reason"] == "UPSTREAM_INCOMPLETE"
    assert recording_client.paths == []
