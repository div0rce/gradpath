from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.enums import CompletionStatus, RuleKind, TermSeason, ValidationReason
from app.models import CatalogSnapshot, Course, CourseOffering, CourseRule, DegreePlan, PlanItem, Term
from app.services.canonicalization import extract_canonical_course_code
from app.services.rule_engine import evaluate_rule

SEASON_ORDER = {
    TermSeason.WINTER: 1,
    TermSeason.SPRING: 2,
    TermSeason.SUMMER: 3,
    TermSeason.FALL: 4,
}


@dataclass
class ValidationOutcome:
    is_valid: bool
    reason: ValidationReason | None
    missing_prereqs: list[str]
    canonical_code: str | None
    original_input: str
    snapshot: CatalogSnapshot


def _term_sort_key(term: Term) -> tuple[int, int, str]:
    return (term.year, SEASON_ORDER[term.season], term.code)


def _available_history_codes(
    db: Session,
    plan_id: str,
    current_term: Term,
    current_position: int,
) -> set[str]:
    items = db.execute(select(PlanItem).where(PlanItem.plan_id == plan_id)).scalars().all()
    if not items:
        return set()

    term_ids = {item.term_id for item in items}
    terms = db.execute(select(Term).where(Term.id.in_(term_ids))).scalars().all()
    term_by_id = {term.id: term for term in terms}

    history_codes: set[str] = set()

    for item in items:
        term = term_by_id.get(item.term_id)
        if not term:
            continue

        include = False
        if _term_sort_key(term) < _term_sort_key(current_term):
            include = True
        elif (
            current_term.season == TermSeason.SUMMER
            and term.id == current_term.id
            and item.position < current_position
            and item.completion_status == CompletionStatus.YES
        ):
            include = True

        if not include:
            continue

        code = item.canonical_code or extract_canonical_course_code(item.raw_input)
        if code:
            history_codes.add(code)

    return history_codes


def validate_plan_item(
    db: Session,
    *,
    plan_id: str,
    term_id: str,
    position: int,
    raw_input: str,
) -> ValidationOutcome:
    plan = db.get(DegreePlan, plan_id)
    if not plan:
        raise ValueError("Plan not found")

    snapshot = db.get(CatalogSnapshot, plan.pinned_catalog_snapshot_id)
    if not snapshot:
        raise ValueError("Pinned catalog snapshot not found")

    original_input = raw_input
    canonical_code = extract_canonical_course_code(raw_input)

    if not raw_input.strip():
        return ValidationOutcome(
            is_valid=True,
            reason=None,
            missing_prereqs=[],
            canonical_code=None,
            original_input=original_input,
            snapshot=snapshot,
        )

    if not canonical_code:
        return ValidationOutcome(
            is_valid=False,
            reason=ValidationReason.INVALID_COURSE,
            missing_prereqs=[],
            canonical_code=None,
            original_input=original_input,
            snapshot=snapshot,
        )

    term = db.get(Term, term_id)
    if not term or term.catalog_snapshot_id != snapshot.id:
        raise ValueError("Term not found in plan snapshot")

    course = db.execute(
        select(Course).where(
            and_(Course.catalog_snapshot_id == snapshot.id, Course.code == canonical_code)
        )
    ).scalar_one_or_none()
    if not course:
        return ValidationOutcome(
            is_valid=False,
            reason=ValidationReason.INVALID_COURSE,
            missing_prereqs=[],
            canonical_code=canonical_code,
            original_input=original_input,
            snapshot=snapshot,
        )

    offering = db.execute(
        select(CourseOffering).where(
            and_(
                CourseOffering.catalog_snapshot_id == snapshot.id,
                CourseOffering.course_id == course.id,
                CourseOffering.term_id == term.id,
            )
        )
    ).scalar_one_or_none()
    if not offering or not offering.offered:
        return ValidationOutcome(
            is_valid=False,
            reason=ValidationReason.NOT_OFFERED,
            missing_prereqs=[],
            canonical_code=canonical_code,
            original_input=original_input,
            snapshot=snapshot,
        )

    prereq_rule = db.execute(
        select(CourseRule).where(
            and_(
                CourseRule.catalog_snapshot_id == snapshot.id,
                CourseRule.course_id == course.id,
                CourseRule.kind == RuleKind.PREREQ,
            )
        )
    ).scalar_one_or_none()

    if prereq_rule:
        available_codes = _available_history_codes(db, plan_id, term, position)
        eval_result = evaluate_rule(prereq_rule.rule, available_codes, allow_complex=False)
        if not eval_result.supported:
            return ValidationOutcome(
                is_valid=False,
                reason=ValidationReason.UNSUPPORTED_RULE,
                missing_prereqs=[],
                canonical_code=canonical_code,
                original_input=original_input,
                snapshot=snapshot,
            )
        if not eval_result.satisfied:
            return ValidationOutcome(
                is_valid=False,
                reason=ValidationReason.PREREQ_MISSING,
                missing_prereqs=eval_result.missing_courses,
                canonical_code=canonical_code,
                original_input=original_input,
                snapshot=snapshot,
            )

    return ValidationOutcome(
        is_valid=True,
        reason=None,
        missing_prereqs=[],
        canonical_code=canonical_code,
        original_input=original_input,
        snapshot=snapshot,
    )
