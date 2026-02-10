from __future__ import annotations

from datetime import datetime
from hashlib import sha256

import pytest
from sqlalchemy import select

from app.db import SessionLocal
from app.enums import CatalogSnapshotStatus, CatalogSource
from app.models import CatalogSnapshot, Course, Term
from app.services.catalog import (
    get_latest_published_soc_slice_snapshot,
    promote_snapshot,
    stage_course_overlay_snapshot,
)
from app.services.soc_checksum import SocResolvedOffering, compute_soc_slice_checksum
from tests.helpers import stage_payload_ready


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


def test_latest_published_soc_slice_snapshot_order_is_deterministic(client):
    _ = client  # fixture initializes schema
    term_code = "2025SU"
    campus = "NB"
    other_term_code = "2025FA"
    other_campus = "NWK"
    with SessionLocal() as db:
        snapshots = [
            CatalogSnapshot(
                id="10000000-0000-0000-0000-000000000001",
                source=CatalogSource.SOC_SCRAPE,
                status=CatalogSnapshotStatus.PUBLISHED,
                checksum="c1",
                synced_at=datetime(2026, 1, 1, 0, 0, 0),
                created_at=datetime(2026, 1, 1, 0, 0, 0),
                published_at=None,
                source_metadata={"soc_slice": {"term_code": term_code, "campus": campus}},
            ),
            CatalogSnapshot(
                id="20000000-0000-0000-0000-000000000002",
                source=CatalogSource.SOC_SCRAPE,
                status=CatalogSnapshotStatus.PUBLISHED,
                checksum="c2",
                synced_at=datetime(2026, 1, 1, 0, 0, 1),
                created_at=datetime(2026, 1, 1, 0, 0, 1),
                published_at=None,
                source_metadata={"soc_slice": {"term_code": term_code, "campus": campus}},
            ),
            CatalogSnapshot(
                id="30000000-0000-0000-0000-000000000003",
                source=CatalogSource.SOC_SCRAPE,
                status=CatalogSnapshotStatus.PUBLISHED,
                checksum="c3",
                synced_at=datetime(2026, 1, 1, 0, 0, 2),
                created_at=datetime(2026, 1, 1, 0, 0, 2),
                published_at=datetime(2026, 1, 1, 0, 0, 5),
                source_metadata={"soc_slice": {"term_code": term_code, "campus": campus}},
            ),
            CatalogSnapshot(
                id="f0000000-0000-0000-0000-000000000004",
                source=CatalogSource.SOC_SCRAPE,
                status=CatalogSnapshotStatus.PUBLISHED,
                checksum="c4",
                synced_at=datetime(2026, 1, 1, 0, 0, 2),
                created_at=datetime(2026, 1, 1, 0, 0, 2),
                published_at=datetime(2026, 1, 1, 0, 0, 5),
                source_metadata={"soc_slice": {"term_code": term_code, "campus": campus}},
            ),
            CatalogSnapshot(
                id="90000000-0000-0000-0000-000000000009",
                source=CatalogSource.SOC_SCRAPE,
                status=CatalogSnapshotStatus.PUBLISHED,
                checksum="other",
                synced_at=datetime(2026, 1, 1, 0, 0, 9),
                created_at=datetime(2026, 1, 1, 0, 0, 9),
                published_at=datetime(2026, 1, 1, 0, 0, 9),
                source_metadata={"soc_slice": {"term_code": other_term_code, "campus": other_campus}},
            ),
        ]
        for row in snapshots:
            db.add(row)
        db.commit()
        picked = get_latest_published_soc_slice_snapshot(db, term_code=term_code, campus=campus)
        assert picked is not None
        assert picked.id == "f0000000-0000-0000-0000-000000000004"


def test_soc_checksum_is_stable_for_order_and_uuid_casing():
    term_upper = "A0B1C2D3-E4F5-6789-ABCD-EF0123456789"
    rows_one = [
        SocResolvedOffering(
            term_id=term_upper,
            course_id="BBBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB",
        ),
        SocResolvedOffering(
            term_id=term_upper,
            course_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        ),
    ]
    rows_two = [
        SocResolvedOffering(
            term_id=term_upper.lower(),
            course_id="AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA",
        ),
        SocResolvedOffering(
            term_id=term_upper.lower(),
            course_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        ),
    ]
    checksum_one = compute_soc_slice_checksum(term_upper, rows_one)
    checksum_two = compute_soc_slice_checksum(term_upper.lower(), rows_two)
    assert checksum_one == checksum_two

    term_lower = term_upper.lower()
    no_newline_payload = (
        f"{term_lower},aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa,1"
        f"{term_lower},bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb,1"
    ).encode("utf-8")
    assert checksum_one != sha256(no_newline_payload).hexdigest()


def test_soc_checksum_rejects_mixed_slice_rows():
    with pytest.raises(ValueError):
        compute_soc_slice_checksum(
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            [
                SocResolvedOffering(
                    term_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    course_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                ),
                SocResolvedOffering(
                    term_id="cccccccc-cccc-cccc-cccc-cccccccccccc",
                    course_id="dddddddd-dddd-dddd-dddd-dddddddddddd",
                ),
            ],
        )


def test_stage_from_soc_noop_true_uses_baseline_for_dry_run_and_latest_for_stage(client):
    baseline_snapshot_id, term_id = _seed_baseline_snapshot(client)
    with SessionLocal() as db:
        course_rows = db.execute(
            select(Course).where(Course.catalog_snapshot_id == baseline_snapshot_id).order_by(Course.code.asc())
        ).scalars().all()
        assert len(course_rows) >= 2
        resolved_rows = [
            SocResolvedOffering(term_id=term_id, course_id=course_rows[0].id),
            SocResolvedOffering(term_id=term_id, course_id=course_rows[1].id),
        ]
        checksum = compute_soc_slice_checksum(term_id, resolved_rows)
        soc_snapshot = CatalogSnapshot(
            id="9f000000-0000-0000-0000-000000000099",
            source=CatalogSource.SOC_SCRAPE,
            status=CatalogSnapshotStatus.PUBLISHED,
            checksum=checksum,
            synced_at=datetime(2026, 1, 2, 0, 0, 0),
            created_at=datetime(2026, 1, 2, 0, 0, 0),
            published_at=datetime(2026, 1, 2, 0, 0, 1),
            source_metadata={
                "soc_slice": {"term_id": str(term_id).lower(), "term_code": "2025SU", "campus": "NB"},
                "soc_slice_checksum": checksum,
            },
        )
        db.add(soc_snapshot)
        db.commit()

    raw_payload = {
        "terms": [{"term_code": "2025SU", "campus": "NB"}],
        "offerings": [
            {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:100", "offered": True},
            {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:200", "offered": True},
        ],
        "metadata": {"parse_warnings": [], "fetched_at": "2026-02-09T00:00:00Z"},
    }

    dry = client.post(
        "/v1/catalog/snapshots:stage-from-soc",
        json={
            "term_code": "2025SU",
            "campus": "NB",
            "dry_run": True,
            "ingest_source": "CSP_PUBLIC",
            "raw_payload": raw_payload,
        },
    )
    assert dry.status_code == 200, dry.text
    dry_body = dry.json()
    assert dry_body["result"]["noop"] is True
    assert dry_body["snapshot"]["snapshot_id"] == baseline_snapshot_id

    staged = client.post(
        "/v1/catalog/snapshots:stage-from-soc",
        json={
            "term_code": "2025SU",
            "campus": "NB",
            "dry_run": False,
            "ingest_source": "CSP_PUBLIC",
            "raw_payload": raw_payload,
        },
    )
    assert staged.status_code == 200, staged.text
    staged_body = staged.json()
    assert staged_body["result"]["noop"] is True
    assert staged_body["snapshot"]["snapshot_id"] == "9f000000-0000-0000-0000-000000000099"


def test_stage_from_soc_dry_run_and_stage_have_same_checksum_and_noop(client):
    _snapshot_id, _term_id = _seed_baseline_snapshot(client)
    raw_payload = {
        "terms": [{"term_code": "2025SU", "campus": "NB"}],
        "offerings": [
            {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:100", "offered": True},
            {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:200", "offered": True},
        ],
        "metadata": {"parse_warnings": [], "fetched_at": "2026-02-09T00:00:00Z"},
    }

    dry = client.post(
        "/v1/catalog/snapshots:stage-from-soc",
        json={
            "term_code": "2025SU",
            "campus": "NB",
            "dry_run": True,
            "ingest_source": "CSP_PUBLIC",
            "raw_payload": raw_payload,
        },
    )
    assert dry.status_code == 200, dry.text

    staged = client.post(
        "/v1/catalog/snapshots:stage-from-soc",
        json={
            "term_code": "2025SU",
            "campus": "NB",
            "dry_run": False,
            "ingest_source": "CSP_PUBLIC",
            "raw_payload": raw_payload,
        },
    )
    assert staged.status_code == 200, staged.text
    dry_body = dry.json()
    staged_body = staged.json()
    assert dry_body["result"]["checksum"] == staged_body["result"]["checksum"]
    assert dry_body["result"]["noop"] == staged_body["result"]["noop"]
    assert staged_body["snapshot"]["source"] == "SOC_SCRAPE"
    assert staged_body["snapshot"]["status"] == "STAGED"


def test_stage_from_soc_accepts_legacy_candidate_payload(client):
    _snapshot_id, _term_id = _seed_baseline_snapshot(client)
    candidate_payload = {
        "terms": [{"term_code": "2025SU", "campus": "NB"}],
        "offerings": [
            {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:100", "offered": True},
            {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:200", "offered": True},
        ],
        "metadata": {"parse_warnings": [], "fetched_at": "2026-02-09T00:00:00Z"},
    }
    res = client.post(
        "/v1/catalog/snapshots:stage-from-soc",
        json={
            "term_code": "2025SU",
            "campus": "NB",
            "dry_run": True,
            "ingest_source": "CSP_PUBLIC",
            "candidate_payload": candidate_payload,
        },
    )
    assert res.status_code == 200, res.text


def test_stage_from_soc_noop_uses_stable_slice_identity_after_promotion(client):
    _snapshot_id, _term_id = _seed_baseline_snapshot(client)
    raw_payload = {
        "terms": [{"term_code": "2025SU", "campus": "NB"}],
        "offerings": [],
        "metadata": {"parse_warnings": [], "fetched_at": "2026-02-09T00:00:00Z"},
    }

    first = client.post(
        "/v1/catalog/snapshots:stage-from-soc",
        json={
            "term_code": "2025SU",
            "campus": "NB",
            "dry_run": False,
            "ingest_source": "CSP_PUBLIC",
            "raw_payload": raw_payload,
        },
    )
    assert first.status_code == 200, first.text
    first_body = first.json()
    assert first_body["result"]["noop"] is False
    first_snapshot_id = first_body["snapshot"]["snapshot_id"]

    promote = client.post(f"/v1/catalog/snapshots/{first_snapshot_id}:promote")
    assert promote.status_code == 200, promote.text

    second = client.post(
        "/v1/catalog/snapshots:stage-from-soc",
        json={
            "term_code": "2025SU",
            "campus": "NB",
            "dry_run": False,
            "ingest_source": "CSP_PUBLIC",
            "raw_payload": raw_payload,
        },
    )
    assert second.status_code == 200, second.text
    second_body = second.json()
    assert second_body["result"]["noop"] is True
    assert second_body["snapshot"]["snapshot_id"] == first_snapshot_id


def test_soc_resolution_metadata_is_deterministic(client):
    _snapshot_id, _term_id = _seed_baseline_snapshot(client)
    raw_payload = {
        "terms": [{"term_code": "2025SU", "campus": "NB"}],
        "offerings": [
            {"term_code": "2025SU", "campus": "NB", "course_code": " 01:198:111 ", "offered": True},
            {"term_code": "2025SU", "campus": "NB", "course_code": "01:198:111", "offered": True},
            {"term_code": "2025SU", "campus": "NB", "course_code": "14 :332:221", "offered": True},
            {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:100", "offered": True},
            {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:200", "offered": False},
        ],
        "metadata": {"parse_warnings": [], "fetched_at": "2026-02-09T00:00:00Z"},
    }

    res = client.post(
        "/v1/catalog/snapshots:stage-from-soc",
        json={
            "term_code": "2025SU",
            "campus": "NB",
            "dry_run": False,
            "ingest_source": "WEBREG_PUBLIC",
            "raw_payload": raw_payload,
        },
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["result"]["unknown_courses_dropped_count"] == 2

    snapshot_id = body["snapshot"]["snapshot_id"]
    with SessionLocal() as db:
        snap = db.execute(select(CatalogSnapshot).where(CatalogSnapshot.id == snapshot_id)).scalar_one()
        md = snap.source_metadata or {}
        soc_resolution = md.get("soc_resolution")

    assert isinstance(soc_resolution, dict)
    assert soc_resolution["total_rows_seen"] == 5
    assert soc_resolution["offered_rows_seen"] == 4
    assert soc_resolution["unknown_codes_count"] == 2
    assert soc_resolution["raw_unknown_count"] == 3
    assert soc_resolution["normalized_unknown_count"] == 2
    assert soc_resolution["unknown_code_samples_raw"] == [" 01:198:111 ", "14 :332:221"]
    assert soc_resolution["unknown_code_samples_normalized"] == ["01:198:111", "14:332:221"]
    assert soc_resolution["unknown_code_sample_hash"] == sha256("01:198:111\n14:332:221".encode("utf-8")).hexdigest()


def test_soc_unknowns_drop_after_course_bootstrap_and_remain_zero_after_promotion(client):
    baseline_snapshot_id, _term_id = _seed_baseline_snapshot(client)
    raw_payload = {
        "terms": [{"term_code": "2025SU", "campus": "NB"}],
        "offerings": [
            {"term_code": "2025SU", "campus": "NB", "course_code": "01:198:111", "offered": True},
        ],
        "metadata": {"parse_warnings": [], "fetched_at": "2026-02-09T00:00:00Z"},
    }

    before = client.post(
        "/v1/catalog/snapshots:stage-from-soc",
        json={
            "term_code": "2025SU",
            "campus": "NB",
            "dry_run": False,
            "ingest_source": "WEBREG_PUBLIC",
            "raw_payload": raw_payload,
        },
    )
    assert before.status_code == 200, before.text
    before_body = before.json()
    assert before_body["result"]["unknown_courses_dropped_count"] == 1
    checksum_before = before_body["result"]["checksum"]

    with SessionLocal() as db:
        baseline_snapshot = db.execute(
            select(CatalogSnapshot).where(CatalogSnapshot.id == baseline_snapshot_id)
        ).scalar_one()
        bootstrap_staged = stage_course_overlay_snapshot(
            db,
            baseline_snapshot=baseline_snapshot,
            missing_courses=[
                {
                    "code": "01:198:111",
                    "title": "(bootstrap) Unknown Title",
                    "credits": 0,
                    "active": True,
                    "category": None,
                }
            ],
            source_metadata={"bootstrap_courses": {"inserted_count": 1}},
        )
        assert bootstrap_staged is not None
        promote_snapshot(db, bootstrap_staged.id)

    first_after = client.post(
        "/v1/catalog/snapshots:stage-from-soc",
        json={
            "term_code": "2025SU",
            "campus": "NB",
            "dry_run": False,
            "ingest_source": "WEBREG_PUBLIC",
            "raw_payload": raw_payload,
        },
    )
    assert first_after.status_code == 200, first_after.text
    first_after_body = first_after.json()
    assert first_after_body["result"]["unknown_courses_dropped_count"] == 0
    assert first_after_body["result"]["checksum"] != checksum_before
    assert first_after_body["result"]["noop"] is False
    first_after_snapshot_id = first_after_body["snapshot"]["snapshot_id"]

    promote = client.post(f"/v1/catalog/snapshots/{first_after_snapshot_id}:promote")
    assert promote.status_code == 200, promote.text

    second_after = client.post(
        "/v1/catalog/snapshots:stage-from-soc",
        json={
            "term_code": "2025SU",
            "campus": "NB",
            "dry_run": False,
            "ingest_source": "WEBREG_PUBLIC",
            "raw_payload": raw_payload,
        },
    )
    assert second_after.status_code == 200, second_after.text
    second_after_body = second_after.json()
    assert second_after_body["result"]["unknown_courses_dropped_count"] == 0
    assert second_after_body["result"]["noop"] is True


def test_stage_course_overlay_snapshot_returns_none_when_missing_set_is_empty(client):
    baseline_snapshot_id, _term_id = _seed_baseline_snapshot(client)
    with SessionLocal() as db:
        baseline_snapshot = db.execute(
            select(CatalogSnapshot).where(CatalogSnapshot.id == baseline_snapshot_id)
        ).scalar_one()
        staged = stage_course_overlay_snapshot(
            db,
            baseline_snapshot=baseline_snapshot,
            missing_courses=[],
            source_metadata={"bootstrap_courses": {"inserted_count": 0}},
        )
    assert staged is None
