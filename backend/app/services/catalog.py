from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from app.enums import CatalogSnapshotStatus, RequirementSetStatus, RuleKind
from app.models import (
    ActiveCatalogSnapshot,
    CatalogSnapshot,
    Course,
    CourseOffering,
    CourseRule,
    PrerequisiteEdge,
    Program,
    ProgramVersion,
    RequirementNode,
    RequirementSet,
    Term,
)
from app.schemas import StageSnapshotRequest
from app.services.rule_engine import validate_rule_schema


@dataclass
class ActiveSnapshotDetails:
    snapshot: CatalogSnapshot


def _stage_validation_error(errors: list[dict[str, Any]]) -> None:
    raise ValueError({"error_code": "STAGE_VALIDATION_ERROR", "errors": errors})


def _extract_course_refs(rule: dict[str, Any]) -> set[str]:
    refs: set[str] = set()

    if "course" in rule:
        refs.add(rule["course"])
    if "all" in rule:
        for child in rule["all"]:
            refs |= _extract_course_refs(child)
    if "any" in rule:
        for child in rule["any"]:
            refs |= _extract_course_refs(child)
    if "countAtLeast" in rule:
        for child in rule["countAtLeast"]["of"]:
            refs |= _extract_course_refs(child)

    return refs


def stage_snapshot(db: Session, payload: StageSnapshotRequest) -> CatalogSnapshot:
    errors: list[dict[str, Any]] = []

    if len(payload.courses) < 1:
        errors.append({"field": "courses", "code": "MIN_ITEMS", "min": 1})
    if len(payload.terms) < 1:
        errors.append({"field": "terms", "code": "MIN_ITEMS", "min": 1})
    if len(payload.offerings) < 1:
        errors.append({"field": "offerings", "code": "MIN_ITEMS", "min": 1})
    if len(payload.programs) < 1:
        errors.append({"field": "programs", "code": "MIN_ITEMS", "min": 1})

    seen_course_codes: set[str] = set()
    for idx, c in enumerate(payload.courses, start=1):
        if c.code in seen_course_codes:
            errors.append(
                {"field": "courses", "code": "DUPLICATE_CODE", "index": idx, "value": c.code}
            )
        seen_course_codes.add(c.code)

    seen_term_keys: set[tuple[str, str]] = set()
    for idx, t in enumerate(payload.terms, start=1):
        key = (t.campus, t.code)
        if key in seen_term_keys:
            errors.append(
                {
                    "field": "terms",
                    "code": "DUPLICATE_TERM_KEY",
                    "index": idx,
                    "campus": t.campus,
                    "term_code": t.code,
                }
            )
        seen_term_keys.add(key)

    for idx, o in enumerate(payload.offerings, start=1):
        if o.course_code not in seen_course_codes:
            errors.append(
                {
                    "field": "offerings",
                    "code": "UNKNOWN_COURSE",
                    "index": idx,
                    "course_code": o.course_code,
                }
            )
        if (o.campus, o.term_code) not in seen_term_keys:
            errors.append(
                {
                    "field": "offerings",
                    "code": "UNKNOWN_TERM",
                    "index": idx,
                    "campus": o.campus,
                    "term_code": o.term_code,
                }
            )

    for idx, r in enumerate(payload.rules, start=1):
        if r.course_code not in seen_course_codes:
            errors.append(
                {
                    "field": "rules",
                    "code": "UNKNOWN_COURSE",
                    "index": idx,
                    "course_code": r.course_code,
                }
            )
        try:
            validate_rule_schema(r.rule)
        except Exception as exc:  # pragma: no cover - schema details are tested in rule_engine
            errors.append(
                {"field": "rules", "code": "INVALID_RULE_AST", "index": idx, "error": str(exc)}
            )

    for p_idx, p in enumerate(payload.programs, start=1):
        if not p.requirements:
            errors.append(
                {
                    "field": "programs",
                    "code": "MIN_REQUIREMENTS",
                    "index": p_idx,
                    "program_code": p.code,
                    "min": 1,
                }
            )
            continue
        for r_idx, req in enumerate(p.requirements, start=1):
            label = req.get("label")
            rule = req.get("rule")
            if not label:
                errors.append(
                    {
                        "field": "program_requirements",
                        "code": "MISSING_LABEL",
                        "program_code": p.code,
                        "index": r_idx,
                    }
                )
            if not isinstance(rule, dict):
                errors.append(
                    {
                        "field": "program_requirements",
                        "code": "INVALID_RULE_TYPE",
                        "program_code": p.code,
                        "index": r_idx,
                    }
                )
                continue
            try:
                validate_rule_schema(rule)
            except Exception as exc:
                errors.append(
                    {
                        "field": "program_requirements",
                        "code": "INVALID_RULE_AST",
                        "program_code": p.code,
                        "index": r_idx,
                        "error": str(exc),
                    }
                )

    if errors:
        _stage_validation_error(errors)

    snapshot = CatalogSnapshot(
        source=payload.source,
        checksum=payload.checksum,
        source_metadata=payload.source_metadata,
        status=CatalogSnapshotStatus.STAGED,
        synced_at=datetime.utcnow(),
    )
    db.add(snapshot)
    db.flush()

    course_by_code: dict[str, Course] = {}
    for c in payload.courses:
        course = Course(
            catalog_snapshot_id=snapshot.id,
            code=c.code,
            title=c.title,
            credits=c.credits,
            active=c.active,
            category=c.category,
        )
        db.add(course)
        db.flush()
        course_by_code[c.code] = course

    term_by_key: dict[tuple[str, str], Term] = {}
    for t in payload.terms:
        key = (t.campus, t.code)
        term = Term(
            catalog_snapshot_id=snapshot.id,
            campus=t.campus,
            code=t.code,
            year=t.year,
            season=t.season,
            starts_at=t.starts_at,
            ends_at=t.ends_at,
        )
        db.add(term)
        db.flush()
        term_by_key[key] = term

    for o in payload.offerings:
        course = course_by_code.get(o.course_code)
        term = term_by_key.get((o.campus, o.term_code))

        db.add(
            CourseOffering(
                catalog_snapshot_id=snapshot.id,
                course_id=course.id,  # validated above
                term_id=term.id,  # validated above
                offered=o.offered,
            )
        )

    for r in payload.rules:
        course = course_by_code.get(r.course_code)
        row = CourseRule(
            catalog_snapshot_id=snapshot.id,
            course_id=course.id,  # validated above
            kind=r.kind,
            rule=r.rule,
            notes=r.notes,
            rule_schema_version=1,
        )
        db.add(row)
        db.flush()

        if r.kind == RuleKind.PREREQ:
            for prereq_code in _extract_course_refs(r.rule):
                prereq = course_by_code.get(prereq_code)
                if prereq:
                    db.add(
                        PrerequisiteEdge(
                            catalog_snapshot_id=snapshot.id,
                            course_id=course.id,
                            prereq_course_id=prereq.id,
                            derived_from_rule_id=row.id,
                        )
                    )

    for p in payload.programs:
        program = db.execute(
            select(Program).where(and_(Program.code == p.code, Program.campus == p.campus))
        ).scalar_one_or_none()
        if not program:
            program = Program(code=p.code, name=p.name, campus=p.campus)
            db.add(program)
            db.flush()

        req_set = RequirementSet(
            program_id=program.id,
            version_label=p.requirement_set_label,
            status=RequirementSetStatus.APPROVED,
            approved_at=datetime.utcnow(),
        )
        db.add(req_set)
        db.flush()

        for idx, req in enumerate(p.requirements, start=1):
            label = req.get("label")
            rule = req.get("rule")
            db.add(
                RequirementNode(
                    requirement_set_id=req_set.id,
                    order_index=int(req.get("orderIndex", idx)),
                    label=label,
                    rule=rule,
                    rule_schema_version=1,
                )
            )

        db.add(
            ProgramVersion(
                program_id=program.id,
                requirement_set_id=req_set.id,
                catalog_snapshot_id=snapshot.id,
                catalog_year=p.catalog_year,
                campus=p.campus,
                effective_from=p.effective_from,
                effective_to=p.effective_to,
            )
        )

    db.commit()
    db.refresh(snapshot)
    return snapshot


def promote_snapshot(db: Session, snapshot_id: str) -> CatalogSnapshot:
    snapshot = db.get(CatalogSnapshot, snapshot_id)
    if not snapshot:
        raise ValueError("Snapshot not found")
    if snapshot.status != CatalogSnapshotStatus.STAGED:
        raise ValueError("Only STAGED snapshots can be promoted")

    published = db.execute(
        select(CatalogSnapshot).where(CatalogSnapshot.status == CatalogSnapshotStatus.PUBLISHED)
    ).scalars().all()
    for old in published:
        old.status = CatalogSnapshotStatus.ARCHIVED

    snapshot.status = CatalogSnapshotStatus.PUBLISHED
    snapshot.published_at = datetime.utcnow()

    active = db.get(ActiveCatalogSnapshot, 1)
    if not active:
        active = ActiveCatalogSnapshot(id=1, catalog_snapshot_id=snapshot.id)
        db.add(active)
    else:
        active.catalog_snapshot_id = snapshot.id

    db.commit()
    db.refresh(snapshot)
    return snapshot


def get_active_snapshot(db: Session) -> ActiveSnapshotDetails:
    active = db.get(ActiveCatalogSnapshot, 1)
    if not active:
        raise ValueError("No active snapshot")

    snapshot = db.get(CatalogSnapshot, active.catalog_snapshot_id)
    if not snapshot:
        raise ValueError("Active snapshot record is invalid")

    return ActiveSnapshotDetails(snapshot=snapshot)


def search_courses(db: Session, query: str) -> list[Course]:
    active = get_active_snapshot(db)
    stmt = select(Course).where(Course.catalog_snapshot_id == active.snapshot.id)
    if query:
        like_q = f"%{query}%"
        stmt = stmt.where(or_(Course.code.ilike(like_q), Course.title.ilike(like_q)))
    stmt = stmt.order_by(Course.code.asc()).limit(50)
    return db.execute(stmt).scalars().all()
