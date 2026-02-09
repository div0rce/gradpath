from __future__ import annotations

from datetime import datetime

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.enums import CertificationState, PlanItemStatus
from app.models import DegreePlan, PlanItem
from app.services.validation import ValidationOutcome, validate_plan_item


def upsert_plan_item(
    db: Session,
    *,
    plan_id: str,
    item_id: str,
    term_id: str,
    position: int,
    raw_input: str,
    completion_status,
) -> tuple[PlanItem, ValidationOutcome]:
    outcome = validate_plan_item(
        db,
        plan_id=plan_id,
        term_id=term_id,
        position=position,
        raw_input=raw_input,
    )

    item = db.get(PlanItem, item_id)
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
    item.validation_meta = {"missingPrereqs": outcome.missing_prereqs} if outcome.missing_prereqs else None
    item.plan_item_status = PlanItemStatus.VALID if outcome.is_valid else PlanItemStatus.INVALID

    db.commit()
    db.refresh(item)
    return item, outcome


def mark_plan_ready(db: Session, *, plan: DegreePlan) -> None:
    plan.certification_state = CertificationState.READY
    db.add(plan)
    db.commit()
