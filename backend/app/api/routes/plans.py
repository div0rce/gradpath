from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import case, select
from sqlalchemy.orm import Session

from app.db import get_db
from app.enums import CertificationState, TermSeason
from app.models import AuditLog, CatalogSnapshot, DegreeAuditRequirement, DegreePlan, PlanItem, ProgramVersion, Term
from app.schemas import (
    AuditLatestResponse,
    AuditRequirementResult,
    CreatePlanRequest,
    CreatePlanResponse,
    FinalizeResponse,
    PlanDetailResponse,
    PlanItemDetailResponse,
    PlanTermResponse,
    ReadyResponse,
    RecomputeAuditResponse,
    UpdatePlanItemRequest,
    ValidatePlanItemRequest,
    ValidatePlanItemResponse,
)
from app.services.audit import latest_audit, recompute_audit
from app.services.plans import upsert_plan_item
from app.services.readiness import evaluate_plan_ready
from app.services.validation import validate_plan_item

router = APIRouter(prefix="/v1/plans", tags=["plans"])


@router.post("", response_model=CreatePlanResponse)
def create_plan(req: CreatePlanRequest, db: Session = Depends(get_db)) -> CreatePlanResponse:
    version = db.get(ProgramVersion, req.program_version_id)
    if not version:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Program version not found")

    plan = DegreePlan(
        user_id=req.user_id,
        program_version_id=req.program_version_id,
        pinned_catalog_snapshot_id=version.catalog_snapshot_id,
        pinned_requirement_set_id=version.requirement_set_id,
        name=req.name,
    )
    db.add(plan)
    db.commit()
    db.refresh(plan)

    return CreatePlanResponse(
        plan_id=plan.id,
        pinned_catalog_snapshot_id=plan.pinned_catalog_snapshot_id,
        pinned_requirement_set_id=plan.pinned_requirement_set_id,
    )


@router.get("/{plan_id}", response_model=PlanDetailResponse)
def get_plan(plan_id: str, db: Session = Depends(get_db)) -> PlanDetailResponse:
    plan = db.get(DegreePlan, plan_id)
    if not plan:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Plan not found")

    # Maintainer note:
    # Plan item IDs are immutable and scoped to a single plan.
    # Cross-plan reuse is rejected in the PUT mutation path.
    items = db.execute(
        select(PlanItem)
        .where(PlanItem.plan_id == plan_id)
        .order_by(PlanItem.term_id.asc(), PlanItem.position.asc())
    ).scalars().all()

    return PlanDetailResponse(
        plan_id=plan.id,
        user_id=plan.user_id,
        name=plan.name,
        program_version_id=plan.program_version_id,
        pinned_catalog_snapshot_id=plan.pinned_catalog_snapshot_id,
        pinned_requirement_set_id=plan.pinned_requirement_set_id,
        certification_state=plan.certification_state.value,
        items=[
            PlanItemDetailResponse(
                id=i.id,
                term_id=i.term_id,
                position=i.position,
                raw_input=i.raw_input,
                canonical_code=i.canonical_code,
                course_id=i.course_id,
                plan_item_status=i.plan_item_status.value,
                completion_status=i.completion_status,
                validation_reason=i.validation_reason,
                validation_meta=i.validation_meta,
                last_validated_at=i.last_validated_at,
            )
            for i in items
        ],
    )


@router.get("/{plan_id}/terms", response_model=list[PlanTermResponse])
def get_plan_terms(plan_id: str, db: Session = Depends(get_db)) -> list[PlanTermResponse]:
    plan = db.get(DegreePlan, plan_id)
    if not plan:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Plan not found")

    # Ordering contract for planner columns:
    # (year ASC, season order, code ASC) where season order is WINTER, SPRING, SUMMER, FALL.
    season_rank = case(
        (Term.season == TermSeason.WINTER, 1),
        (Term.season == TermSeason.SPRING, 2),
        (Term.season == TermSeason.SUMMER, 3),
        (Term.season == TermSeason.FALL, 4),
        else_=99,
    )
    rows = db.execute(
        select(Term)
        .where(Term.catalog_snapshot_id == plan.pinned_catalog_snapshot_id)
        .order_by(Term.year.asc(), season_rank.asc(), Term.code.asc())
    ).scalars().all()

    return [
        PlanTermResponse(
            id=t.id,
            campus=t.campus,
            code=t.code,
            year=t.year,
            season=t.season,
        )
        for t in rows
    ]


@router.post("/{plan_id}/items:validate", response_model=ValidatePlanItemResponse)
def validate_item(plan_id: str, req: ValidatePlanItemRequest, db: Session = Depends(get_db)) -> ValidatePlanItemResponse:
    try:
        outcome = validate_plan_item(
            db,
            plan_id=plan_id,
            term_id=req.term_id,
            position=req.position,
            raw_input=req.raw_input,
            completion_status=req.completion_status,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return ValidatePlanItemResponse(
        is_valid=outcome.is_valid,
        reason=outcome.reason,
        missing_prereqs=outcome.missing_prereqs,
        canonical_code=outcome.canonical_code,
        original_input=outcome.original_input,
        catalog_snapshot_id=outcome.snapshot.id,
        synced_at=outcome.snapshot.synced_at,
        source=outcome.snapshot.source,
    )


@router.put("/{plan_id}/items/{item_id}", response_model=ValidatePlanItemResponse)
def put_item(
    plan_id: str,
    item_id: str,
    req: UpdatePlanItemRequest,
    db: Session = Depends(get_db),
) -> ValidatePlanItemResponse:
    try:
        _item, outcome = upsert_plan_item(
            db,
            plan_id=plan_id,
            item_id=item_id,
            term_id=req.term_id,
            position=req.position,
            raw_input=req.raw_input,
            completion_status=req.completion_status,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return ValidatePlanItemResponse(
        is_valid=outcome.is_valid,
        reason=outcome.reason,
        missing_prereqs=outcome.missing_prereqs,
        canonical_code=outcome.canonical_code,
        original_input=outcome.original_input,
        catalog_snapshot_id=outcome.snapshot.id,
        synced_at=outcome.snapshot.synced_at,
        source=outcome.snapshot.source,
    )


@router.post("/{plan_id}:ready", response_model=ReadyResponse)
def mark_ready(plan_id: str, db: Session = Depends(get_db)) -> ReadyResponse:
    plan = db.get(DegreePlan, plan_id)
    if not plan:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Plan not found")

    try:
        check = evaluate_plan_ready(db, plan_id=plan_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    if not check.ok:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"error_code": "PLAN_NOT_READY", "blockers": check.blockers},
        )

    plan.certification_state = CertificationState.READY
    db.add(plan)
    db.add(
        AuditLog(
            actor_user_id=plan.user_id,
            action="PLAN_MARKED_READY",
            resource_type="DegreePlan",
            resource_id=plan.id,
            meta={
                "auditId": check.audit_id,
                "catalogSnapshotId": plan.pinned_catalog_snapshot_id,
                "requirementSetId": plan.pinned_requirement_set_id,
            },
        )
    )
    db.commit()

    return ReadyResponse(
        plan_id=plan.id,
        certification_state=plan.certification_state.value,
        audit_id=check.audit_id,
        checked_at=check.checked_at,
        catalog_snapshot_id=plan.pinned_catalog_snapshot_id,
        requirement_set_id=plan.pinned_requirement_set_id,
    )


@router.post("/{plan_id}/recompute-audit", response_model=RecomputeAuditResponse)
def recompute(plan_id: str, db: Session = Depends(get_db)) -> RecomputeAuditResponse:
    try:
        outcome = recompute_audit(db, plan_id=plan_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return RecomputeAuditResponse(
        audit_id=outcome.audit.id,
        has_unsupported_rules=outcome.audit.has_unsupported_rules,
        summary=outcome.audit.summary,
        catalog_snapshot_id=outcome.snapshot.id,
        synced_at=outcome.snapshot.synced_at,
        source=outcome.snapshot.source,
    )


@router.get("/{plan_id}/audit/latest", response_model=AuditLatestResponse)
def get_latest(plan_id: str, db: Session = Depends(get_db)) -> AuditLatestResponse:
    audit = latest_audit(db, plan_id=plan_id)
    if not audit:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No audit found")

    plan = db.get(DegreePlan, plan_id)
    if not plan:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Plan not found")

    snapshot = db.get(CatalogSnapshot, audit.catalog_snapshot_id)
    if not snapshot:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Snapshot not found")

    req_rows = db.execute(
        select(DegreeAuditRequirement).where(DegreeAuditRequirement.degree_audit_id == audit.id)
    ).scalars().all()

    return AuditLatestResponse(
        audit_id=audit.id,
        computed_at=audit.computed_at,
        has_unsupported_rules=audit.has_unsupported_rules,
        summary=audit.summary,
        requirements=[
            AuditRequirementResult(
                requirement_node_id=r.requirement_node_id,
                status=r.status,
                detail=r.detail,
            )
            for r in req_rows
        ],
        catalog_snapshot_id=snapshot.id,
        synced_at=snapshot.synced_at,
        source=snapshot.source,
    )


@router.post("/{plan_id}/finalize", response_model=FinalizeResponse)
def finalize(plan_id: str, db: Session = Depends(get_db)) -> FinalizeResponse:
    plan = db.get(DegreePlan, plan_id)
    if not plan:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Plan not found")

    if plan.certification_state != CertificationState.READY:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"error_code": "PLAN_NOT_READY", "blockers": [{"code": "CERTIFY_REQUIRES_READY"}]},
        )

    try:
        check = evaluate_plan_ready(db, plan_id=plan_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    if not check.ok:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"error_code": "PLAN_NOT_READY", "blockers": check.blockers},
        )

    plan.certification_state = CertificationState.CERTIFIED
    db.add(plan)
    db.add(
        AuditLog(
            actor_user_id=plan.user_id,
            action="PLAN_FINALIZED",
            resource_type="DegreePlan",
            resource_id=plan.id,
            meta={
                "auditId": check.audit_id,
                "catalogSnapshotId": plan.pinned_catalog_snapshot_id,
                "requirementSetId": plan.pinned_requirement_set_id,
            },
        )
    )
    db.commit()

    return FinalizeResponse(
        plan_id=plan_id,
        certification_state=plan.certification_state.value,
        finalized_at=datetime.utcnow(),
    )
