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
        if c.code in course_by_code:
            raise ValueError(f"Duplicate course code: {c.code}")
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
        if key in term_by_key:
            raise ValueError(f"Duplicate term key: {key}")
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
        if not course:
            raise ValueError(f"Offering references unknown course: {o.course_code}")
        if not term:
            raise ValueError(f"Offering references unknown term: {(o.campus, o.term_code)}")

        db.add(
            CourseOffering(
                catalog_snapshot_id=snapshot.id,
                course_id=course.id,
                term_id=term.id,
                offered=o.offered,
            )
        )

    for r in payload.rules:
        course = course_by_code.get(r.course_code)
        if not course:
            raise ValueError(f"Rule references unknown course: {r.course_code}")

        validate_rule_schema(r.rule)
        row = CourseRule(
            catalog_snapshot_id=snapshot.id,
            course_id=course.id,
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
            if not label or not isinstance(rule, dict):
                raise ValueError("Requirement entries must include label and rule")
            validate_rule_schema(rule)
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
