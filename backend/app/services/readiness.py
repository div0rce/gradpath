from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.enums import AuditRequirementStatus, PlanItemStatus
from app.models import DegreeAuditRequirement, DegreePlan, PlanItem
from app.services.audit import recompute_audit

BLOCKER_ORDER = {
    "INVALID_ITEMS": 0,
    "UNSUPPORTED_RULES": 1,
    "MISSING_REQUIREMENTS": 2,
    "UNKNOWN_REQUIREMENTS": 3,
}


@dataclass
class ReadyCheck:
    ok: bool
    blockers: list[dict]
    audit_id: str
    checked_at: datetime


def evaluate_plan_ready(db: Session, *, plan_id: str) -> ReadyCheck:
    checked_at = datetime.utcnow()
    plan = db.get(DegreePlan, plan_id)
    if not plan:
        raise ValueError("Plan not found")

    invalid_n = db.execute(
        select(PlanItem.id).where(
            and_(PlanItem.plan_id == plan_id, PlanItem.plan_item_status == PlanItemStatus.INVALID)
        )
    ).all()
    invalid_count = len(invalid_n)

    outcome = recompute_audit(db, plan_id=plan_id)
    audit = outcome.audit

    req_rows = db.execute(
        select(DegreeAuditRequirement).where(DegreeAuditRequirement.degree_audit_id == audit.id)
    ).scalars().all()

    missing_n = sum(1 for r in req_rows if r.status == AuditRequirementStatus.MISSING)
    unknown_n = sum(1 for r in req_rows if r.status == AuditRequirementStatus.UNKNOWN)

    blockers: list[dict] = []
    if invalid_count:
        blockers.append({"code": "INVALID_ITEMS", "count": invalid_count})
    if audit.has_unsupported_rules:
        blockers.append({"code": "UNSUPPORTED_RULES"})
    if missing_n:
        blockers.append({"code": "MISSING_REQUIREMENTS", "count": missing_n})
    if unknown_n:
        blockers.append({"code": "UNKNOWN_REQUIREMENTS", "count": unknown_n})

    blockers.sort(key=lambda b: BLOCKER_ORDER[b["code"]])

    return ReadyCheck(
        ok=len(blockers) == 0,
        blockers=blockers,
        audit_id=audit.id,
        checked_at=checked_at,
    )
