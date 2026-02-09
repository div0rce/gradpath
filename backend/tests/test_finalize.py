from __future__ import annotations

from sqlalchemy import select

from app.db import SessionLocal
from app.models import DegreePlan, PlanItem, ProgramVersion, Term
from tests.helpers import stage_payload, stage_payload_ready


def _prepare(client, user_id: str, payload: dict):
    stage = client.post("/v1/catalog/snapshots:stage", json=payload)
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


def test_finalize_requires_ready(client, user_id):
    plan_id, summer_id = _prepare(client, user_id, stage_payload())
    client.put(
        f"/v1/plans/{plan_id}/items/item-1",
        json={
            "term_id": summer_id,
            "position": 10,
            "raw_input": "14:540:100",
            "completion_status": "YES",
        },
    )

    finalize = client.post(f"/v1/plans/{plan_id}/finalize")
    assert finalize.status_code == 409
    body = finalize.json()["detail"]
    assert body["error_code"] == "PLAN_NOT_READY"
    assert {"code": "CERTIFY_REQUIRES_READY"} in body["blockers"]


def test_finalize_after_ready_succeeds(client, user_id):
    plan_id, summer_id = _prepare(client, user_id, stage_payload_ready())

    client.put(
        f"/v1/plans/{plan_id}/items/item-1",
        json={
            "term_id": summer_id,
            "position": 1,
            "raw_input": "14:540:100",
            "completion_status": "YES",
        },
    )
    client.put(
        f"/v1/plans/{plan_id}/items/item-2",
        json={
            "term_id": summer_id,
            "position": 2,
            "raw_input": "14:540:200",
            "completion_status": "YES",
        },
    )

    ready = client.post(f"/v1/plans/{plan_id}:ready")
    assert ready.status_code == 200, ready.text
    assert ready.json()["certification_state"] == "READY"

    finalize = client.post(f"/v1/plans/{plan_id}/finalize")
    assert finalize.status_code == 200, finalize.text
    assert finalize.json()["certification_state"] == "CERTIFIED"

    with SessionLocal() as db:
        plan = db.get(DegreePlan, plan_id)
        assert plan is not None
        assert plan.certification_state.value == "CERTIFIED"
        item = db.execute(
            select(PlanItem).where(PlanItem.plan_id == plan_id, PlanItem.id == "item-2")
        ).scalars().one()
        assert item.validation_meta["completionStatusAtValidation"] == "YES"
