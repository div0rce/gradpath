from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from app.enums import CatalogSnapshotStatus, CatalogSource, RequirementSetStatus, RuleKind
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
from app.services.soc_checksum import SocResolvedOffering


@dataclass
class ActiveSnapshotDetails:
    snapshot: CatalogSnapshot


SOC_CODE_SPACE_RE = re.compile(r"\s+")


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


def get_active_published_snapshot(db: Session) -> CatalogSnapshot:
    active = get_active_snapshot(db).snapshot
    if active.status != CatalogSnapshotStatus.PUBLISHED:
        raise ValueError({"error_code": "SOC_BASELINE_REQUIRED"})
    return active


def get_latest_published_soc_slice_snapshot(db: Session, term_id: str) -> CatalogSnapshot | None:
    term_id_str = str(term_id).lower()
    # Preferred query shape for Postgres JSONB indexing:
    # source_metadata->'soc_slice'->>'term_id' = :term_id
    # Order + JSONB path are shared between dry-run and stage; do not fork.
    base_stmt = (
        select(CatalogSnapshot)
        .where(
            CatalogSnapshot.source == CatalogSource.SOC_SCRAPE,
            CatalogSnapshot.status == CatalogSnapshotStatus.PUBLISHED,
        )
        .order_by(
            func.coalesce(CatalogSnapshot.published_at, CatalogSnapshot.created_at).desc(),
            CatalogSnapshot.created_at.desc(),
            CatalogSnapshot.id.desc(),
        )
    )
    try:
        stmt = base_stmt.where(CatalogSnapshot.source_metadata["soc_slice"]["term_id"].as_string() == term_id_str)
        return db.execute(stmt).scalars().first()
    except Exception:
        rows = db.execute(base_stmt).scalars().all()
        for row in rows:
            metadata = row.source_metadata or {}
            soc_slice = metadata.get("soc_slice") or {}
            if str(soc_slice.get("term_id", "")).lower() == term_id_str:
                return row
    return None


def normalize_course_code(raw_code: str) -> tuple[str, bool]:
    """Normalize SOC course code by removing whitespace and uppercasing."""
    normalized = SOC_CODE_SPACE_RE.sub("", str(raw_code)).upper()
    return normalized, normalized != str(raw_code)


def write_soc_metadata(
    *,
    existing: dict[str, Any] | None,
    term_id: str,
    term_code: str,
    campus: str,
    checksum: str,
    ingest_source: str,
    parse_warnings_count: int,
    unknown_courses_dropped_count: int,
    zero_offerings: bool,
) -> dict[str, Any]:
    meta = dict(existing or {})
    meta["soc_slice"] = {
        "term_id": str(term_id).lower(),
        "term_code": term_code,
        "campus": campus,
    }
    meta["soc_slice_checksum"] = checksum
    meta["ingest_source"] = ingest_source
    meta["parse_warnings_count"] = parse_warnings_count
    meta["unknown_courses_dropped_count"] = unknown_courses_dropped_count
    meta["zero_offerings"] = zero_offerings
    return meta


def stage_soc_overlay_snapshot(
    db: Session,
    *,
    baseline_snapshot: CatalogSnapshot,
    baseline_term_id: str,
    resolved_offerings: list[SocResolvedOffering],
    checksum: str,
    term_code: str,
    campus: str,
    ingest_source: str,
    parse_warnings_count: int,
    unknown_courses_dropped_count: int,
    source_metadata: dict[str, Any] | None,
) -> CatalogSnapshot:
    new_snapshot = CatalogSnapshot(
        source=CatalogSource.SOC_SCRAPE,
        checksum=checksum,
        status=CatalogSnapshotStatus.STAGED,
        synced_at=datetime.utcnow(),
        source_metadata=write_soc_metadata(
            existing=source_metadata,
            term_id=baseline_term_id,
            term_code=term_code,
            campus=campus,
            checksum=checksum,
            ingest_source=ingest_source,
            parse_warnings_count=parse_warnings_count,
            unknown_courses_dropped_count=unknown_courses_dropped_count,
            zero_offerings=len(resolved_offerings) == 0,
        ),
    )
    db.add(new_snapshot)
    db.flush()

    baseline_courses = db.execute(
        select(Course).where(Course.catalog_snapshot_id == baseline_snapshot.id)
    ).scalars().all()
    baseline_terms = db.execute(select(Term).where(Term.catalog_snapshot_id == baseline_snapshot.id)).scalars().all()
    baseline_rules = db.execute(
        select(CourseRule).where(CourseRule.catalog_snapshot_id == baseline_snapshot.id)
    ).scalars().all()
    baseline_edges = db.execute(
        select(PrerequisiteEdge).where(PrerequisiteEdge.catalog_snapshot_id == baseline_snapshot.id)
    ).scalars().all()
    baseline_offerings = db.execute(
        select(CourseOffering).where(CourseOffering.catalog_snapshot_id == baseline_snapshot.id)
    ).scalars().all()

    course_id_map: dict[str, str] = {}
    for c in baseline_courses:
        copied = Course(
            catalog_snapshot_id=new_snapshot.id,
            code=c.code,
            title=c.title,
            credits=c.credits,
            active=c.active,
            category=c.category,
        )
        db.add(copied)
        db.flush()
        course_id_map[c.id] = copied.id

    term_id_map: dict[str, str] = {}
    for t in baseline_terms:
        copied = Term(
            catalog_snapshot_id=new_snapshot.id,
            campus=t.campus,
            code=t.code,
            year=t.year,
            season=t.season,
            starts_at=t.starts_at,
            ends_at=t.ends_at,
        )
        db.add(copied)
        db.flush()
        term_id_map[t.id] = copied.id

    rule_id_map: dict[str, str] = {}
    for r in baseline_rules:
        copied = CourseRule(
            catalog_snapshot_id=new_snapshot.id,
            course_id=course_id_map[r.course_id],
            kind=r.kind,
            rule=r.rule,
            rule_schema_version=r.rule_schema_version,
            notes=r.notes,
        )
        db.add(copied)
        db.flush()
        rule_id_map[r.id] = copied.id

    for e in baseline_edges:
        db.add(
            PrerequisiteEdge(
                catalog_snapshot_id=new_snapshot.id,
                course_id=course_id_map[e.course_id],
                prereq_course_id=course_id_map[e.prereq_course_id],
                derived_from_rule_id=rule_id_map[e.derived_from_rule_id] if e.derived_from_rule_id else None,
            )
        )

    target_new_term_id = term_id_map[baseline_term_id]
    for o in baseline_offerings:
        if o.term_id == baseline_term_id:
            continue
        db.add(
            CourseOffering(
                catalog_snapshot_id=new_snapshot.id,
                course_id=course_id_map[o.course_id],
                term_id=term_id_map[o.term_id],
                offered=o.offered,
            )
        )

    inserted_course_ids: set[str] = set()
    for row in resolved_offerings:
        if row.course_id in inserted_course_ids:
            continue
        inserted_course_ids.add(row.course_id)
        db.add(
            CourseOffering(
                catalog_snapshot_id=new_snapshot.id,
                course_id=course_id_map[row.course_id],
                term_id=target_new_term_id,
                offered=True,
            )
        )

    db.commit()
    db.refresh(new_snapshot)
    return new_snapshot


def search_courses(db: Session, query: str) -> list[Course]:
    active = get_active_snapshot(db)
    stmt = select(Course).where(Course.catalog_snapshot_id == active.snapshot.id)
    if query:
        like_q = f"%{query}%"
        stmt = stmt.where(or_(Course.code.ilike(like_q), Course.title.ilike(like_q)))
    stmt = stmt.order_by(Course.code.asc()).limit(50)
    return db.execute(stmt).scalars().all()
