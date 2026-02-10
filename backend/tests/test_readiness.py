from __future__ import annotations

from sqlalchemy import select

from app.db import SessionLocal
from app.models import DegreePlan, ProgramVersion, Term
from tests.helpers import stage_payload, stage_payload_ready


def _seed(client, user_id: str, payload: dict) -> tuple[str, str]:
    stage = client.post("/v1/catalog/snapshots:stage", json=payload)
    snapshot_id = stage.json()["snapshot_id"]
    client.post(f"/v1/catalog/snapshots/{snapshot_id}:promote")
    with SessionLocal() as db:
        pv = db.execute(select(ProgramVersion)).scalars().first()
        summer = db.execute(select(Term).where(Term.code == "2025SU")).scalars().first()
    created = client.post(
        "/v1/plans",
        json={"user_id": user_id, "program_version_id": pv.id, "name": "Ready Plan"},
    )
    return created.json()["plan_id"], summer.id


def test_ready_transition_success(client, user_id):
    plan_id, summer_id = _seed(client, user_id, stage_payload_ready())

    client.put(
        f"/v1/plans/{plan_id}/items/a",
        json={"term_id": summer_id, "position": 1, "raw_input": "14:540:100", "completion_status": "YES"},
    )
    client.put(
        f"/v1/plans/{plan_id}/items/b",
        json={"term_id": summer_id, "position": 2, "raw_input": "14:540:200", "completion_status": "YES"},
    )

    ready = client.post(f"/v1/plans/{plan_id}:ready")
    assert ready.status_code == 200, ready.text
    body = ready.json()
    assert body["certification_state"] == "READY"
    assert body["audit_id"] != ""


def test_ready_blocked_with_deterministic_blockers(client, user_id):
    plan_id, summer_id = _seed(client, user_id, stage_payload())

    client.put(
        f"/v1/plans/{plan_id}/items/bad",
        json={"term_id": summer_id, "position": 1, "raw_input": "BAD INPUT", "completion_status": "NO"},
    )
    ready = client.post(f"/v1/plans/{plan_id}:ready")
    assert ready.status_code == 409
    detail = ready.json()["detail"]
    assert detail["error_code"] == "PLAN_NOT_READY"
    blocker_codes = [b["code"] for b in detail["blockers"]]
    assert blocker_codes == ["INVALID_ITEMS", "MISSING_REQUIREMENTS"]


def test_ready_to_draft_on_item_mutation(client, user_id):
    plan_id, summer_id = _seed(client, user_id, stage_payload_ready())

    client.put(
        f"/v1/plans/{plan_id}/items/a",
        json={"term_id": summer_id, "position": 1, "raw_input": "14:540:100", "completion_status": "YES"},
    )
    client.put(
        f"/v1/plans/{plan_id}/items/b",
        json={"term_id": summer_id, "position": 2, "raw_input": "14:540:200", "completion_status": "YES"},
    )
    ready = client.post(f"/v1/plans/{plan_id}:ready")
    assert ready.status_code == 200

    mutate = client.put(
        f"/v1/plans/{plan_id}/items/b",
        json={"term_id": summer_id, "position": 2, "raw_input": "14:540:200", "completion_status": "NO"},
    )
    assert mutate.status_code == 200

    with SessionLocal() as db:
        plan = db.get(DegreePlan, plan_id)
        assert plan is not None
        assert plan.certification_state.value == "DRAFT"
