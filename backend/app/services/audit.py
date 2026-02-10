from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import and_, delete, select
from sqlalchemy.orm import Session

from app.enums import AuditRequirementStatus, CompletionStatus, PlanItemStatus
from app.models import (
    CatalogSnapshot,
    Course,
    DegreeAudit,
    DegreeAuditRequirement,
    DegreePlan,
    PlanItem,
    RequirementNode,
)
from app.services.degree_dsl_engine import evaluate_degree_requirement_rule


@dataclass
class AuditOutcome:
    audit: DegreeAudit
    requirement_rows: list[DegreeAuditRequirement]
    snapshot: CatalogSnapshot


def recompute_audit(db: Session, *, plan_id: str) -> AuditOutcome:
    plan = db.get(DegreePlan, plan_id)
    if not plan:
        raise ValueError("Plan not found")

    snapshot = db.get(CatalogSnapshot, plan.pinned_catalog_snapshot_id)
    if not snapshot:
        raise ValueError("Pinned snapshot not found")

    items = db.execute(select(PlanItem).where(PlanItem.plan_id == plan_id)).scalars().all()

    completed_codes: set[str] = set()
    pending_codes: set[str] = set()
    completed_credits = 0
    pending_credits = 0

    for item in items:
        if item.plan_item_status != PlanItemStatus.VALID:
            continue
        if not item.canonical_code:
            continue

        course = db.get(Course, item.course_id) if item.course_id else None
        credits = course.credits if course else 0

        if item.completion_status == CompletionStatus.YES:
            completed_codes.add(item.canonical_code)
            completed_credits += credits
        elif item.completion_status == CompletionStatus.IN_PROGRESS:
            pending_codes.add(item.canonical_code)
            pending_credits += credits

    nodes = db.execute(
        select(RequirementNode)
        .where(RequirementNode.requirement_set_id == plan.pinned_requirement_set_id)
        .order_by(RequirementNode.order_index.asc())
    ).scalars().all()

    has_unsupported_rules = False
    satisfied = 0
    pending = 0
    missing = 0
    unknown = 0

    audit = DegreeAudit(
        plan_id=plan.id,
        catalog_snapshot_id=plan.pinned_catalog_snapshot_id,
        requirement_set_id=plan.pinned_requirement_set_id,
        computed_at=datetime.utcnow(),
        has_unsupported_rules=False,
        summary={},
    )
    db.add(audit)
    db.flush()

    req_rows: list[DegreeAuditRequirement] = []
    all_known = 0

    for node in nodes:
        completed_eval = evaluate_degree_requirement_rule(node.rule, completed_codes)
        if not completed_eval.supported:
            status = AuditRequirementStatus.UNKNOWN
            detail = {"reason": "UNSUPPORTED_RULE", "explanations": completed_eval.explanation_codes}
            has_unsupported_rules = True
            unknown += 1
        elif completed_eval.satisfied:
            status = AuditRequirementStatus.SATISFIED
            detail = None
            satisfied += 1
            all_known += 1
        else:
            union_eval = evaluate_degree_requirement_rule(node.rule, completed_codes | pending_codes)
            if not union_eval.supported:
                status = AuditRequirementStatus.UNKNOWN
                detail = {"reason": "UNSUPPORTED_RULE", "explanations": union_eval.explanation_codes}
                has_unsupported_rules = True
                unknown += 1
            elif union_eval.satisfied:
                status = AuditRequirementStatus.PENDING
                detail = {
                    "missingCourses": completed_eval.missing_courses,
                    "explanations": completed_eval.explanation_codes,
                }
                pending += 1
                all_known += 1
            else:
                status = AuditRequirementStatus.MISSING
                detail = {
                    "missingCourses": completed_eval.missing_courses,
                    "explanations": completed_eval.explanation_codes,
                }
                missing += 1
                all_known += 1

        row = DegreeAuditRequirement(
            degree_audit_id=audit.id,
            requirement_node_id=node.id,
            status=status,
            detail=detail,
        )
        db.add(row)
        req_rows.append(row)

    percent = float(satisfied) / float(all_known) if all_known > 0 else 0.0

    audit.has_unsupported_rules = has_unsupported_rules
    audit.summary = {
        "completedCredits": completed_credits,
        "pendingCredits": pending_credits,
        "satisfiedRequirements": satisfied,
        "pendingRequirements": pending,
        "missingRequirements": missing,
        "unknownRequirements": unknown,
        "percentComplete": percent,
        "knownRequirementCount": all_known,
        "totalRequirementCount": len(nodes),
    }

    db.commit()
    db.refresh(audit)
    return AuditOutcome(audit=audit, requirement_rows=req_rows, snapshot=snapshot)


def latest_audit(db: Session, *, plan_id: str) -> DegreeAudit | None:
    return db.execute(
        select(DegreeAudit)
        .where(DegreeAudit.plan_id == plan_id)
        .order_by(DegreeAudit.computed_at.desc())
    ).scalars().first()
