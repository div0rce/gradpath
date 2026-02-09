from __future__ import annotations

from sqlalchemy import select

from app.db import SessionLocal
from app.models import ProgramVersion, Term
from tests.helpers import stage_payload


def _seed_plan(client, user_id: str) -> tuple[str, str, str]:
    stage = client.post("/v1/catalog/snapshots:stage", json=stage_payload())
    snapshot_id = stage.json()["snapshot_id"]
    client.post(f"/v1/catalog/snapshots/{snapshot_id}:promote")

    with SessionLocal() as db:
        pv = db.execute(select(ProgramVersion)).scalars().first()
        summer = db.execute(select(Term).where(Term.code == "2025SU")).scalars().first()
        fall = db.execute(select(Term).where(Term.code == "2025FA")).scalars().first()

    created = client.post(
        "/v1/plans",
        json={"user_id": user_id, "program_version_id": pv.id, "name": "Plan A"},
    )
    assert created.status_code == 200, created.text
    return created.json()["plan_id"], summer.id, fall.id


def test_summer_same_term_yes_satisfies_prereq(client, user_id):
    plan_id, summer_id, _ = _seed_plan(client, user_id)

    first = client.put(
        f"/v1/plans/{plan_id}/items/item-1",
        json={
            "term_id": summer_id,
            "position": 1,
            "raw_input": "(14:540:100) Intro",
            "completion_status": "YES",
        },
    )
    assert first.status_code == 200

    validate_second = client.post(
        f"/v1/plans/{plan_id}/items:validate",
        json={
            "term_id": summer_id,
            "position": 2,
            "raw_input": "(14:540:200) Advanced",
            "completion_status": "BLANK",
        },
    )
    assert validate_second.status_code == 200
    assert validate_second.json()["is_valid"] is True


def test_summer_same_term_no_fails_prereq(client, user_id):
    plan_id, summer_id, _ = _seed_plan(client, user_id)

    first = client.put(
        f"/v1/plans/{plan_id}/items/item-1",
        json={
            "term_id": summer_id,
            "position": 1,
            "raw_input": "(14:540:100) Intro",
            "completion_status": "NO",
        },
    )
    assert first.status_code == 200

    validate_second = client.post(
        f"/v1/plans/{plan_id}/items:validate",
        json={
            "term_id": summer_id,
            "position": 2,
            "raw_input": "(14:540:200) Advanced",
            "completion_status": "BLANK",
        },
    )
    assert validate_second.status_code == 200
    assert validate_second.json()["is_valid"] is False
    assert validate_second.json()["reason"] == "PREREQ_MISSING"
