from __future__ import annotations

from sqlalchemy import select

from app.db import SessionLocal
from app.models import ProgramVersion, Term
from tests.helpers import stage_payload


def _prepare(client, user_id: str):
    stage = client.post("/v1/catalog/snapshots:stage", json=stage_payload())
    snapshot_id = stage.json()["snapshot_id"]
    client.post(f"/v1/catalog/snapshots/{snapshot_id}:promote")
    with SessionLocal() as db:
        pv = db.execute(select(ProgramVersion)).scalars().first()
        summer = db.execute(select(Term).where(Term.code == "2025SU")).scalars().first()
    plan = client.post(
        "/v1/plans",
        json={"user_id": user_id, "program_version_id": pv.id, "name": "Plan B"},
    )
    return plan.json()["plan_id"], summer.id


def test_finalize_blocked_when_invalid(client, user_id):
    plan_id, summer_id = _prepare(client, user_id)
    client.put(
        f"/v1/plans/{plan_id}/items/item-1",
        json={
            "term_id": summer_id,
            "position": 1,
            "raw_input": "BAD INPUT",
            "completion_status": "YES",
        },
    )

    finalize = client.post(f"/v1/plans/{plan_id}/finalize")
    assert finalize.status_code == 409


def test_finalize_blocked_when_unsupported_rule(client, user_id):
    plan_id, summer_id = _prepare(client, user_id)

    # Satisfy first two requirements only; third requirement uses any[] (unsupported for v1) => UNKNOWN.
    client.put(
        f"/v1/plans/{plan_id}/items/item-1",
        json={
            "term_id": summer_id,
            "position": 1,
            "raw_input": "14:540:100",
            "completion_status": "YES",
        },
    )

    finalize = client.post(f"/v1/plans/{plan_id}/finalize")
    assert finalize.status_code == 409
    assert "unsupported" in finalize.text.lower()
