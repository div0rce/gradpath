from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from sqlalchemy import JSON, DateTime, Enum as SAEnum, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base
from app.enums import (
    AuditRequirementStatus,
    CatalogSnapshotStatus,
    CatalogSource,
    CertificationState,
    CompletionStatus,
    PlanItemStatus,
    RequirementSetStatus,
    RuleKind,
    TermSeason,
    UserRole,
    ValidationReason,
)


def _uuid() -> str:
    return str(uuid4())


class User(Base):
    __tablename__ = "user"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    net_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    email: Mapped[str] = mapped_column(String, unique=True, index=True)
    role: Mapped[UserRole] = mapped_column(SAEnum(UserRole), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AdvisorStudentAccess(Base):
    __tablename__ = "advisor_student_access"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    advisor_id: Mapped[str] = mapped_column(ForeignKey("user.id"), index=True)
    student_id: Mapped[str] = mapped_column(ForeignKey("user.id"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (UniqueConstraint("advisor_id", "student_id", name="uq_advisor_student"),)


class CatalogSnapshot(Base):
    __tablename__ = "catalog_snapshot"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    source: Mapped[CatalogSource] = mapped_column(SAEnum(CatalogSource), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    checksum: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[CatalogSnapshotStatus] = mapped_column(SAEnum(CatalogSnapshotStatus), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime)
    source_metadata: Mapped[dict | None] = mapped_column(JSON)


class ActiveCatalogSnapshot(Base):
    __tablename__ = "active_catalog_snapshot"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    catalog_snapshot_id: Mapped[str] = mapped_column(ForeignKey("catalog_snapshot.id"), unique=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Program(Base):
    __tablename__ = "program"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    code: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    campus: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (UniqueConstraint("code", "campus", name="uq_program_code_campus"),)


class RequirementSet(Base):
    __tablename__ = "requirement_set"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    program_id: Mapped[str] = mapped_column(ForeignKey("program.id"), index=True)
    version_label: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[RequirementSetStatus] = mapped_column(SAEnum(RequirementSetStatus), nullable=False)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime)
    approved_by_user_id: Mapped[str | None] = mapped_column(ForeignKey("user.id"))
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (UniqueConstraint("program_id", "version_label", name="uq_requirement_set_program_version"),)


class RequirementNode(Base):
    __tablename__ = "requirement_node"
    # Requirement nodes are program-version scoped and immutable per plan.
    # DegreePlan rows reference requirement nodes by ID only.
    # Do not copy/mutate requirement rules at plan level.

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    requirement_set_id: Mapped[str] = mapped_column(ForeignKey("requirement_set.id"), index=True)
    order_index: Mapped[int] = mapped_column(Integer, nullable=False)
    label: Mapped[str] = mapped_column(String, nullable=False)
    rule: Mapped[dict] = mapped_column(JSON, nullable=False)
    rule_schema_version: Mapped[int] = mapped_column(Integer, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (UniqueConstraint("requirement_set_id", "order_index", name="uq_req_set_order"),)


class ProgramVersion(Base):
    __tablename__ = "program_version"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    program_id: Mapped[str] = mapped_column(ForeignKey("program.id"), index=True)
    requirement_set_id: Mapped[str] = mapped_column(ForeignKey("requirement_set.id"), index=True)
    catalog_snapshot_id: Mapped[str] = mapped_column(ForeignKey("catalog_snapshot.id"), index=True)
    catalog_year: Mapped[str] = mapped_column(String, nullable=False)
    campus: Mapped[str] = mapped_column(String, nullable=False)
    effective_from: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    effective_to: Mapped[datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (UniqueConstraint("program_id", "catalog_year", "campus", name="uq_program_catalog_campus"),)


class Term(Base):
    __tablename__ = "term"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    catalog_snapshot_id: Mapped[str] = mapped_column(ForeignKey("catalog_snapshot.id"), index=True)
    campus: Mapped[str] = mapped_column(String, nullable=False)
    code: Mapped[str] = mapped_column(String, nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    season: Mapped[TermSeason] = mapped_column(SAEnum(TermSeason), nullable=False)
    starts_at: Mapped[datetime | None] = mapped_column(DateTime)
    ends_at: Mapped[datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (UniqueConstraint("catalog_snapshot_id", "campus", "code", name="uq_term_snapshot_campus_code"),)


class Course(Base):
    __tablename__ = "course"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    catalog_snapshot_id: Mapped[str] = mapped_column(ForeignKey("catalog_snapshot.id"), index=True)
    code: Mapped[str] = mapped_column(String, nullable=False)
    title: Mapped[str] = mapped_column(String, nullable=False)
    credits: Mapped[int] = mapped_column(Integer, nullable=False)
    active: Mapped[bool] = mapped_column(default=True)
    category: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (UniqueConstraint("catalog_snapshot_id", "code", name="uq_course_snapshot_code"),)


class CourseOffering(Base):
    __tablename__ = "course_offering"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    catalog_snapshot_id: Mapped[str] = mapped_column(ForeignKey("catalog_snapshot.id"), index=True)
    course_id: Mapped[str] = mapped_column(ForeignKey("course.id"), index=True)
    term_id: Mapped[str] = mapped_column(ForeignKey("term.id"), index=True)
    offered: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (UniqueConstraint("catalog_snapshot_id", "course_id", "term_id", name="uq_offering_snapshot_course_term"),)


class CourseRule(Base):
    __tablename__ = "course_rule"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    catalog_snapshot_id: Mapped[str] = mapped_column(ForeignKey("catalog_snapshot.id"), index=True)
    course_id: Mapped[str] = mapped_column(ForeignKey("course.id"), index=True)
    kind: Mapped[RuleKind] = mapped_column(SAEnum(RuleKind), nullable=False)
    rule: Mapped[dict] = mapped_column(JSON, nullable=False)
    rule_schema_version: Mapped[int] = mapped_column(Integer, default=1)
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (UniqueConstraint("catalog_snapshot_id", "course_id", "kind", name="uq_course_rule_kind"),)


class PrerequisiteEdge(Base):
    __tablename__ = "prerequisite_edge"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    catalog_snapshot_id: Mapped[str] = mapped_column(ForeignKey("catalog_snapshot.id"), index=True)
    course_id: Mapped[str] = mapped_column(ForeignKey("course.id"), index=True)
    prereq_course_id: Mapped[str] = mapped_column(ForeignKey("course.id"), index=True)
    derived_from_rule_id: Mapped[str | None] = mapped_column(ForeignKey("course_rule.id"))

    __table_args__ = (UniqueConstraint("catalog_snapshot_id", "course_id", "prereq_course_id", name="uq_edge_snapshot_course_prereq"),)


class DegreePlan(Base):
    __tablename__ = "degree_plan"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    user_id: Mapped[str] = mapped_column(ForeignKey("user.id"), index=True)
    program_version_id: Mapped[str] = mapped_column(ForeignKey("program_version.id"), index=True)
    pinned_catalog_snapshot_id: Mapped[str] = mapped_column(ForeignKey("catalog_snapshot.id"), index=True)
    pinned_requirement_set_id: Mapped[str] = mapped_column(ForeignKey("requirement_set.id"), index=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    certification_state: Mapped[CertificationState] = mapped_column(
        SAEnum(CertificationState), default=CertificationState.DRAFT
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PlanItem(Base):
    __tablename__ = "plan_item"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    plan_id: Mapped[str] = mapped_column(ForeignKey("degree_plan.id"), index=True)
    term_id: Mapped[str] = mapped_column(ForeignKey("term.id"), index=True)
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    raw_input: Mapped[str] = mapped_column(String, default="")
    canonical_code: Mapped[str | None] = mapped_column(String, index=True)
    course_id: Mapped[str | None] = mapped_column(ForeignKey("course.id"))
    plan_item_status: Mapped[PlanItemStatus] = mapped_column(SAEnum(PlanItemStatus), default=PlanItemStatus.DRAFT)
    completion_status: Mapped[CompletionStatus] = mapped_column(
        SAEnum(CompletionStatus), default=CompletionStatus.BLANK
    )
    validation_reason: Mapped[ValidationReason | None] = mapped_column(SAEnum(ValidationReason))
    validation_meta: Mapped[dict | None] = mapped_column(JSON)
    last_validated_at: Mapped[datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (UniqueConstraint("plan_id", "term_id", "position", name="uq_plan_term_position"),)


class DegreeAudit(Base):
    __tablename__ = "degree_audit"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    plan_id: Mapped[str] = mapped_column(ForeignKey("degree_plan.id"), index=True)
    catalog_snapshot_id: Mapped[str] = mapped_column(ForeignKey("catalog_snapshot.id"), index=True)
    requirement_set_id: Mapped[str] = mapped_column(ForeignKey("requirement_set.id"), index=True)
    computed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    has_unsupported_rules: Mapped[bool] = mapped_column(default=False, index=True)
    summary: Mapped[dict] = mapped_column(JSON, nullable=False)


class DegreeAuditRequirement(Base):
    __tablename__ = "degree_audit_requirement"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    degree_audit_id: Mapped[str] = mapped_column(ForeignKey("degree_audit.id"), index=True)
    requirement_node_id: Mapped[str] = mapped_column(ForeignKey("requirement_node.id"), index=True)
    status: Mapped[AuditRequirementStatus] = mapped_column(SAEnum(AuditRequirementStatus), index=True)
    detail: Mapped[dict | None] = mapped_column(JSON)

    __table_args__ = (
        UniqueConstraint("degree_audit_id", "requirement_node_id", name="uq_audit_req"),
    )


class AuditLog(Base):
    __tablename__ = "audit_log"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_uuid)
    actor_user_id: Mapped[str] = mapped_column(ForeignKey("user.id"), index=True)
    action: Mapped[str] = mapped_column(String, nullable=False)
    resource_type: Mapped[str] = mapped_column(String, nullable=False)
    resource_id: Mapped[str] = mapped_column(String, nullable=False)
    meta: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
