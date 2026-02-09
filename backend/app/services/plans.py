from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from app.enums import CertificationState, CompletionStatus, PlanItemStatus
from app.models import AuditLog, DegreePlan, PlanItem
from app.services.validation import ValidationOutcome, validate_plan_item


def upsert_plan_item(
    db: Session,
    *,
    plan_id: str,
    item_id: str,
    term_id: str,
    position: int,
    raw_input: str,
    completion_status: CompletionStatus,
) -> tuple[PlanItem, ValidationOutcome]:
    plan = db.get(DegreePlan, plan_id)
    if not plan:
        raise ValueError("Plan not found")

    if plan.certification_state == CertificationState.READY:
        plan.certification_state = CertificationState.DRAFT
        db.add(
            AuditLog(
                actor_user_id=plan.user_id,
                action="PLAN_REVERTED_TO_DRAFT",
                resource_type="DegreePlan",
                resource_id=plan.id,
                meta={"reason": "PLAN_ITEM_MUTATION"},
            )
        )

    outcome = validate_plan_item(
        db,
        plan_id=plan_id,
        term_id=term_id,
        position=position,
        raw_input=raw_input,
        completion_status=completion_status,
    )

    item = db.get(PlanItem, item_id)
    if item and item.plan_id != plan_id:
        raise ValueError("Plan item id belongs to a different plan")
    if not item:
        item = PlanItem(id=item_id, plan_id=plan_id)
        db.add(item)

    item.term_id = term_id
    item.position = position
    item.raw_input = raw_input
    item.completion_status = completion_status
    item.canonical_code = outcome.canonical_code
    item.last_validated_at = datetime.utcnow()
    item.validation_reason = outcome.reason
    item.validation_meta = {
        "missingPrereqs": outcome.missing_prereqs,
        "completionStatusAtValidation": completion_status.value,
    }
    item.plan_item_status = PlanItemStatus.VALID if outcome.is_valid else PlanItemStatus.INVALID

    db.commit()
    db.refresh(item)
    return item, outcome
