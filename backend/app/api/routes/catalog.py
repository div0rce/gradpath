from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.db import get_db
from app.enums import CatalogSource
from app.schemas import (
    ActiveSnapshotResponse,
    CourseSearchResponseItem,
    StageFromCsvRequest,
    SnapshotResponse,
    StageSnapshotRequest,
)
from app.services.adapters import DepartmentCSVAdapter
from app.services.catalog import get_active_snapshot, promote_snapshot, search_courses, stage_snapshot

router = APIRouter(prefix="/v1/catalog", tags=["catalog"])


@router.post("/snapshots:stage", response_model=SnapshotResponse)
def stage(req: StageSnapshotRequest, db: Session = Depends(get_db)) -> SnapshotResponse:
    try:
        snapshot = stage_snapshot(db, req)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return SnapshotResponse(
        snapshot_id=snapshot.id,
        status=snapshot.status,
        source=snapshot.source,
        synced_at=snapshot.synced_at,
        published_at=snapshot.published_at,
    )


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
