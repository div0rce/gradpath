from __future__ import annotations

from collections import defaultdict
from hashlib import sha256
import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.db import get_db
from app.enums import CatalogSource
from app.models import Course, CourseOffering, Term
from app.schemas import (
    ActiveSnapshotResponse,
    CourseSearchResponseItem,
    StageFromCsvRequest,
    StageFromSocRequest,
    SocSliceResponse,
    SocStageResponse,
    SocStageResult,
    SnapshotResponse,
    StageSnapshotRequest,
)
from app.services.adapters import DepartmentCSVAdapter, SOCExportAdapter
from app.services.catalog import (
    get_active_published_snapshot,
    get_active_snapshot,
    get_latest_published_soc_slice_snapshot,
    normalize_course_code,
    promote_snapshot,
    search_courses,
    stage_snapshot,
    stage_soc_overlay_snapshot,
)
from app.services.soc_checksum import SocResolvedOffering, compute_soc_slice_checksum

router = APIRouter(prefix="/v1/catalog", tags=["catalog"])
SOC_RESOLUTION_SAMPLE_SIZE = 25


def _detail_from_exception(exc: Exception) -> Any:
    if exc.args:
        first = exc.args[0]
        if isinstance(first, dict):
            return first
        if isinstance(first, str):
            try:
                parsed = json.loads(first)
            except Exception:
                return first
            if isinstance(parsed, dict):
                return parsed
            return first
    return str(exc)


def _build_soc_resolution_metadata(
    *,
    total_rows_seen: int,
    offered_rows_seen: int,
    raw_unknown_codes: set[str],
    normalized_unknown_codes: set[str],
    catalog_snapshot_id: str,
    term_code: str,
    campus: str,
) -> dict[str, Any]:
    raw_by_normalized: dict[str, set[str]] = defaultdict(set)
    for raw_code in raw_unknown_codes:
        normalized_code, _ = normalize_course_code(raw_code)
        raw_by_normalized[normalized_code].add(raw_code)

    # Keep one canonical raw code per normalized identity for deterministic diagnostics.
    canonical_raw_by_normalized = {
        normalized_code: min(raw_candidates)
        for normalized_code, raw_candidates in raw_by_normalized.items()
    }
    normalized_sorted = sorted(normalized_unknown_codes)
    raw_samples = [
        canonical_raw_by_normalized[normalized_code]
        for normalized_code in normalized_sorted
        if normalized_code in canonical_raw_by_normalized
    ]
    hash_payload = "\n".join(normalized_sorted).encode("utf-8")
    normalized_unknown_count = len(normalized_unknown_codes)
    return {
        "total_rows_seen": int(total_rows_seen),
        "offered_rows_seen": int(offered_rows_seen),
        # v1 intentionally uses normalized identity as the unknown-count boundary.
        "unknown_codes_count": normalized_unknown_count,
        # Diagnostic-only metric: not suitable for gating/comparison decisions.
        "raw_unknown_count": len(raw_unknown_codes),
        "unknown_code_samples_raw": raw_samples[:SOC_RESOLUTION_SAMPLE_SIZE],
        "unknown_code_samples_normalized": normalized_sorted[:SOC_RESOLUTION_SAMPLE_SIZE],
        "normalized_unknown_count": normalized_unknown_count,
        "unknown_code_sample_hash": sha256(hash_payload).hexdigest(),
        "resolution_catalog_snapshot_id": catalog_snapshot_id,
        "resolution_term_code": term_code,
        "resolution_campus": campus,
        "resolution_version": "v1",
    }


def _get_snapshot_offered_codes(
    db: Session,
    *,
    snapshot_id: str,
    term_code: str,
    campus: str,
) -> set[str] | None:
    term_exists = db.execute(
        select(Term.id).where(
            Term.catalog_snapshot_id == snapshot_id,
            Term.code == term_code,
            Term.campus == campus,
        )
    ).first()
    if term_exists is None:
        return None

    offered_codes = db.execute(
        select(Course.code)
        .join(
            CourseOffering,
            and_(
                CourseOffering.course_id == Course.id,
                CourseOffering.catalog_snapshot_id == snapshot_id,
            ),
        )
        .join(
            Term,
            and_(
                Term.id == CourseOffering.term_id,
                Term.catalog_snapshot_id == snapshot_id,
            ),
        )
        .where(
            Course.catalog_snapshot_id == snapshot_id,
            CourseOffering.offered.is_(True),
            Term.code == term_code,
            Term.campus == campus,
        )
    ).scalars().all()
    return {normalize_course_code(code)[0] for code in offered_codes}


@router.post("/snapshots:stage", response_model=SnapshotResponse)
def stage(req: StageSnapshotRequest, db: Session = Depends(get_db)) -> SnapshotResponse:
    try:
        snapshot = stage_snapshot(db, req)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=_detail_from_exception(exc)) from exc

    return SnapshotResponse(
        snapshot_id=snapshot.id,
        status=snapshot.status,
        source=snapshot.source,
        synced_at=snapshot.synced_at,
        published_at=snapshot.published_at,
    )


@router.post("/snapshots:stage-from-csv", response_model=SnapshotResponse)
def stage_from_csv(req: StageFromCsvRequest, db: Session = Depends(get_db)) -> SnapshotResponse:
    try:
        adapter = DepartmentCSVAdapter(Path(req.bundle_dir))
        raw_payload = adapter.fetch_candidate_payload()
        canonical = adapter.to_canonical_rows(raw_payload)
        stage_req = StageSnapshotRequest(
            source=CatalogSource.DEPARTMENT_CSV,
            checksum=req.checksum,
            source_metadata=req.source_metadata or adapter.source_metadata(),
            **canonical,
        )
        snapshot = stage_snapshot(db, stage_req)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=_detail_from_exception(exc)) from exc

    return SnapshotResponse(
        snapshot_id=snapshot.id,
        status=snapshot.status,
        source=snapshot.source,
        synced_at=snapshot.synced_at,
        published_at=snapshot.published_at,
    )


@router.post("/snapshots:stage-from-soc", response_model=SocStageResponse)
def stage_from_soc(req: StageFromSocRequest, db: Session = Depends(get_db)) -> SocStageResponse:
    try:
        baseline = get_active_published_snapshot(db)
        baseline_term = db.execute(
            select(Term).where(
                and_(
                    Term.catalog_snapshot_id == baseline.id,
                    Term.campus == req.campus,
                    Term.code == req.term_code,
                )
            )
        ).scalar_one_or_none()
        if not baseline_term:
            raise ValueError(
                {"error_code": "SOC_TERM_NOT_FOUND", "term_code": req.term_code, "campus": req.campus}
            )

        # TODO: remove legacy candidate_payload fallback after clients migrate to raw_payload.
        effective_raw_payload = req.raw_payload if req.raw_payload is not None else req.candidate_payload
        if effective_raw_payload is None:
            raise ValueError({"error_code": "SOC_FETCH_FAILED", "message": "raw_payload is required"})
        adapter = SOCExportAdapter(raw_payload=effective_raw_payload, ingest_source=req.ingest_source)
        canonical = adapter.to_canonical_rows(adapter.fetch_candidate_payload())
        matching_term_rows = [
            row
            for row in canonical["terms"]
            if row.get("term_code") == req.term_code and row.get("campus") == req.campus
        ]
        if len(matching_term_rows) != 1:
            raise ValueError(
                {
                    "error_code": "SOC_SCHEMA_VIOLATION",
                    "message": "terms[] must include exactly one requested (term_code, campus) row",
                }
            )

        course_rows = db.execute(
            select(Course).where(Course.catalog_snapshot_id == baseline.id)
        ).scalars().all()
        course_by_code: dict[str, Course] = {}
        for row in course_rows:
            normalized, _ = normalize_course_code(row.code)
            course_by_code[normalized] = row

        total_rows_seen = 0
        offered_rows_seen = 0
        normalized_changes = 0
        unknown_codes: set[str] = set()
        unknown_codes_raw: set[str] = set()
        resolved_normalized_codes: set[str] = set()
        resolved_rows: list[SocResolvedOffering] = []
        for row in canonical["offerings"]:
            total_rows_seen += 1
            if row.get("term_code") != req.term_code or row.get("campus") != req.campus:
                continue
            if not row.get("offered", False):
                continue
            offered_rows_seen += 1
            raw_code = str(row["course_code"])
            normalized_code, changed = normalize_course_code(raw_code)
            if changed:
                normalized_changes += 1
            course = course_by_code.get(normalized_code)
            if not course:
                unknown_codes.add(normalized_code)
                unknown_codes_raw.add(raw_code)
                continue
            resolved_normalized_codes.add(normalized_code)
            resolved_rows.append(SocResolvedOffering(term_id=baseline_term.id, course_id=course.id))

        soc_resolution_metadata = _build_soc_resolution_metadata(
            total_rows_seen=total_rows_seen,
            offered_rows_seen=offered_rows_seen,
            raw_unknown_codes=unknown_codes_raw,
            normalized_unknown_codes=unknown_codes,
            catalog_snapshot_id=baseline.id,
            term_code=req.term_code,
            campus=req.campus,
        )

        parse_warnings = (canonical.get("metadata") or {}).get("parse_warnings") or []
        # Normalization changes are intentionally counted as parse warnings for observability.
        parse_warnings_count = (
            len(parse_warnings) if isinstance(parse_warnings, list) else 0
        ) + normalized_changes
        unique_resolved = sorted({row.course_id for row in resolved_rows})
        resolved_unique_rows = [
            SocResolvedOffering(term_id=baseline_term.id, course_id=course_id)
            for course_id in unique_resolved
        ]

        checksum = compute_soc_slice_checksum(baseline_term.id, resolved_unique_rows)
        if req.checksum and req.checksum != checksum:
            raise ValueError(
                {
                    "error_code": "SOC_CHECKSUM_MISMATCH",
                    "provided_checksum": req.checksum,
                    "computed_checksum": checksum,
                }
            )

        latest_published_soc = get_latest_published_soc_slice_snapshot(
            db,
            term_code=req.term_code,
            campus=req.campus,
            term_id_fallback=baseline_term.id,
        )
        latest_checksum = None
        if latest_published_soc:
            latest_checksum = (latest_published_soc.source_metadata or {}).get("soc_slice_checksum")
        # Checksum is a fast-path signal only.
        # Final noop decision is resolved-offering-set equality for (term_code, campus),
        # which remains stable across bootstrap/promotion boundaries.
        noop = latest_checksum == checksum if latest_checksum is not None else False
        if latest_published_soc:
            # Noop decision is resolved-slice based: identical upstream resolved offering identities
            # for (term_code, campus) are treated as noop, even if internal snapshot IDs differ.
            latest_offered_codes = _get_snapshot_offered_codes(
                db,
                snapshot_id=latest_published_soc.id,
                term_code=req.term_code,
                campus=req.campus,
            )
            if latest_offered_codes is not None:
                noop = latest_offered_codes == resolved_normalized_codes

        snapshot = baseline
        if req.dry_run:
            snapshot = baseline
        elif noop and latest_published_soc:
            snapshot = latest_published_soc
        elif not noop:
            stage_source_metadata = (req.source_metadata or {}) | adapter.source_metadata()
            stage_source_metadata["soc_resolution"] = soc_resolution_metadata
            snapshot = stage_soc_overlay_snapshot(
                db,
                baseline_snapshot=baseline,
                baseline_term_id=baseline_term.id,
                resolved_offerings=resolved_unique_rows,
                checksum=checksum,
                term_code=req.term_code,
                campus=req.campus,
                ingest_source=req.ingest_source,
                parse_warnings_count=parse_warnings_count,
                unknown_courses_dropped_count=len(unknown_codes),
                source_metadata=stage_source_metadata,
            )

        return SocStageResponse(
            snapshot=SnapshotResponse(
                snapshot_id=snapshot.id,
                status=snapshot.status,
                source=snapshot.source,
                synced_at=snapshot.synced_at,
                published_at=snapshot.published_at,
            ),
            result=SocStageResult(
                noop=noop,
                checksum=checksum,
                unknown_courses_dropped_count=len(unknown_codes),
                parse_warnings_count=parse_warnings_count,
                zero_offerings=len(resolved_unique_rows) == 0,
                slice=SocSliceResponse(
                    term_id=baseline_term.id,
                    term_code=req.term_code,
                    campus=req.campus,
                ),
            ),
        )
    except Exception as exc:
        # TODO: map SOC domain errors to narrower 409/422 statuses in a later phase.
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=_detail_from_exception(exc)) from exc


@router.post("/snapshots/{snapshot_id}:promote", response_model=SnapshotResponse)
def promote(snapshot_id: str, db: Session = Depends(get_db)) -> SnapshotResponse:
    try:
        snapshot = promote_snapshot(db, snapshot_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return SnapshotResponse(
        snapshot_id=snapshot.id,
        status=snapshot.status,
        source=snapshot.source,
        synced_at=snapshot.synced_at,
        published_at=snapshot.published_at,
    )


@router.get("/snapshots/active", response_model=ActiveSnapshotResponse)
def active(db: Session = Depends(get_db)) -> ActiveSnapshotResponse:
    try:
        details = get_active_snapshot(db)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    snap = details.snapshot
    return ActiveSnapshotResponse(
        snapshot_id=snap.id,
        status=snap.status,
        source=snap.source,
        synced_at=snap.synced_at,
        published_at=snap.published_at,
    )


@router.get("/courses/search", response_model=list[CourseSearchResponseItem])
def course_search(q: str = Query(default=""), db: Session = Depends(get_db)) -> list[CourseSearchResponseItem]:
    try:
        rows = search_courses(db, q)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return [
        CourseSearchResponseItem(code=r.code, title=r.title, credits=r.credits, active=r.active)
        for r in rows
    ]
