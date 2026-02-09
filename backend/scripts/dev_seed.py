from __future__ import annotations

import os
import shlex
import sys
from pathlib import Path

from sqlalchemy import and_, select

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db import SessionLocal
from app.enums import CatalogSnapshotStatus, CatalogSource, UserRole
from app.models import ActiveCatalogSnapshot, CatalogSnapshot, Program, ProgramVersion, Term, User
from app.schemas import StageSnapshotRequest
from app.services.catalog import promote_snapshot, stage_snapshot
from tests.helpers import stage_payload_ready


def _export(name: str, value: str) -> str:
    return f"export {name}={shlex.quote(value)}"


def _matching_snapshots(db, *, source: CatalogSource, checksum: str) -> list[CatalogSnapshot]:
    return db.execute(
        select(CatalogSnapshot)
        .where(and_(CatalogSnapshot.source == source, CatalogSnapshot.checksum == checksum))
        .order_by(CatalogSnapshot.synced_at.desc())
    ).scalars().all()


def _ensure_active_snapshot(db, snapshot: CatalogSnapshot) -> None:
    active = db.get(ActiveCatalogSnapshot, 1)
    if not active:
        db.add(ActiveCatalogSnapshot(id=1, catalog_snapshot_id=snapshot.id))
    else:
        active.catalog_snapshot_id = snapshot.id
        db.add(active)
    db.commit()


def _find_conflicting_program_version(
    db,
    req: StageSnapshotRequest,
) -> ProgramVersion | None:
    for p in req.programs:
        program = db.execute(
            select(Program).where(and_(Program.code == p.code, Program.campus == p.campus))
        ).scalar_one_or_none()
        if not program:
            continue
        conflict = db.execute(
            select(ProgramVersion)
            .where(
                and_(
                    ProgramVersion.program_id == program.id,
                    ProgramVersion.catalog_year == p.catalog_year,
                    ProgramVersion.campus == p.campus,
                )
            )
            .order_by(ProgramVersion.created_at.desc())
        ).scalars().first()
        if conflict:
            return conflict
    return None


def _pick_snapshot_for_seed(db, req: StageSnapshotRequest) -> CatalogSnapshot:
    matches = _matching_snapshots(db, source=req.source, checksum=req.checksum)
    published = next((s for s in matches if s.status == CatalogSnapshotStatus.PUBLISHED), None)
    if published:
        _ensure_active_snapshot(db, published)
        return published

    staged = next((s for s in matches if s.status == CatalogSnapshotStatus.STAGED), None)
    if staged:
        promoted = promote_snapshot(db, staged.id)
        return promoted

    # Prevent UNIQUE collisions by resolving existing ProgramVersion first.
    conflict = _find_conflicting_program_version(db, req)
    if conflict:
        snap = db.get(CatalogSnapshot, conflict.catalog_snapshot_id)
        if snap and snap.status == CatalogSnapshotStatus.STAGED:
            snap = promote_snapshot(db, snap.id)
        if snap and snap.status == CatalogSnapshotStatus.PUBLISHED:
            _ensure_active_snapshot(db, snap)
        if snap:
            return snap

    staged_new = stage_snapshot(db, req)
    return promote_snapshot(db, staged_new.id)


def _resolve_program_version(db, *, snapshot: CatalogSnapshot, req: StageSnapshotRequest) -> ProgramVersion:
    pv = db.execute(
        select(ProgramVersion)
        .where(ProgramVersion.catalog_snapshot_id == snapshot.id)
        .order_by(ProgramVersion.created_at.desc())
    ).scalars().first()
    if pv:
        return pv

    # Fallback for cases where staging was skipped due unique key reuse.
    for p in req.programs:
        program = db.execute(
            select(Program).where(and_(Program.code == p.code, Program.campus == p.campus))
        ).scalar_one_or_none()
        if not program:
            continue
        pv = db.execute(
            select(ProgramVersion)
            .where(
                and_(
                    ProgramVersion.program_id == program.id,
                    ProgramVersion.catalog_year == p.catalog_year,
                    ProgramVersion.campus == p.campus,
                )
            )
            .order_by(ProgramVersion.created_at.desc())
        ).scalars().first()
        if pv:
            return pv

    raise RuntimeError("Unable to resolve ProgramVersion for dev seed")


def _resolve_term(db, *, snapshot: CatalogSnapshot, code: str = "2025SU") -> Term:
    term = db.execute(
        select(Term)
        .where(and_(Term.catalog_snapshot_id == snapshot.id, Term.code == code))
        .order_by(Term.created_at.desc())
    ).scalars().first()
    if term:
        return term

    term = db.execute(select(Term).where(Term.code == code).order_by(Term.created_at.desc())).scalars().first()
    if term:
        return term

    raise RuntimeError(f"Unable to resolve Term with code {code}")


def main() -> None:
    net_id = os.getenv("DEV_NETID", "dev123")
    email = os.getenv("DEV_EMAIL", f"{net_id}@rutgers.edu")
    req = StageSnapshotRequest(**stage_payload_ready())
    req.source = CatalogSource.DEPARTMENT_CSV

    db = SessionLocal()
    try:
        user = db.execute(select(User).where(User.net_id == net_id)).scalar_one_or_none()
        if not user:
            user = User(net_id=net_id, email=email, role=UserRole.STUDENT)
            db.add(user)
            db.commit()
            db.refresh(user)

        snapshot = _pick_snapshot_for_seed(db, req)
        pv = _resolve_program_version(db, snapshot=snapshot, req=req)
        term = _resolve_term(db, snapshot=snapshot)

        print(_export("USER_ID", user.id))
        print(_export("PROGRAM_VERSION_ID", pv.id))
        print(_export("TERM_ID", term.id))
    finally:
        db.close()


if __name__ == "__main__":
    main()
