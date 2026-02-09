from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.enums import (
    AuditRequirementStatus,
    CatalogSnapshotStatus,
    CatalogSource,
    CompletionStatus,
    RuleKind,
    TermSeason,
    ValidationReason,
)


class ValidatePlanItemRequest(BaseModel):
    term_id: str
    position: int = Field(ge=1)
    raw_input: str
    completion_status: CompletionStatus = CompletionStatus.BLANK


class ValidatePlanItemResponse(BaseModel):
    is_valid: bool
    reason: ValidationReason | None = None
    missing_prereqs: list[str] = Field(default_factory=list)
    canonical_code: str | None = None
    original_input: str
    catalog_snapshot_id: str
    synced_at: datetime
    source: CatalogSource


class UpdatePlanItemRequest(BaseModel):
    term_id: str
    position: int = Field(ge=1)
    raw_input: str
    completion_status: CompletionStatus = CompletionStatus.BLANK


class CreatePlanRequest(BaseModel):
    user_id: str
    program_version_id: str
    name: str


class CreatePlanResponse(BaseModel):
    plan_id: str
    pinned_catalog_snapshot_id: str
    pinned_requirement_set_id: str


class CourseIngestItem(BaseModel):
    code: str
    title: str
    credits: int
    active: bool = True
    category: str | None = None


class TermIngestItem(BaseModel):
    campus: str
    code: str
    year: int
    season: TermSeason
    starts_at: datetime | None = None
    ends_at: datetime | None = None


class OfferingIngestItem(BaseModel):
    course_code: str
    term_code: str
    campus: str
    offered: bool


class RuleIngestItem(BaseModel):
    course_code: str
    kind: RuleKind
    rule: dict[str, Any]
    notes: str | None = None


class ProgramIngestItem(BaseModel):
    code: str
    name: str
    campus: str
    catalog_year: str
    effective_from: datetime
    effective_to: datetime | None = None
    requirement_set_label: str
    requirements: list[dict[str, Any]]


class StageSnapshotRequest(BaseModel):
    source: CatalogSource
    checksum: str
    source_metadata: dict[str, Any] | None = None
    courses: list[CourseIngestItem] = Field(default_factory=list)
    terms: list[TermIngestItem] = Field(default_factory=list)
    offerings: list[OfferingIngestItem] = Field(default_factory=list)
    rules: list[RuleIngestItem] = Field(default_factory=list)
    programs: list[ProgramIngestItem] = Field(default_factory=list)


class StageFromCsvRequest(BaseModel):
    bundle_dir: str
    checksum: str
    source_metadata: dict[str, Any] | None = None


class SnapshotResponse(BaseModel):
    snapshot_id: str
    status: CatalogSnapshotStatus
    source: CatalogSource
    synced_at: datetime
    published_at: datetime | None = None


class ActiveSnapshotResponse(BaseModel):
    snapshot_id: str
    status: CatalogSnapshotStatus
    source: CatalogSource
    synced_at: datetime
    published_at: datetime | None


class RecomputeAuditResponse(BaseModel):
    audit_id: str
    has_unsupported_rules: bool
    summary: dict[str, Any]
    catalog_snapshot_id: str
    synced_at: datetime
    source: CatalogSource


class AuditRequirementResult(BaseModel):
    requirement_node_id: str
    status: AuditRequirementStatus
    detail: dict[str, Any] | None = None


class AuditLatestResponse(BaseModel):
    audit_id: str
    computed_at: datetime
    has_unsupported_rules: bool
    summary: dict[str, Any]
    requirements: list[AuditRequirementResult]
    catalog_snapshot_id: str
    synced_at: datetime
    source: CatalogSource


class FinalizeResponse(BaseModel):
    plan_id: str
    certification_state: str
    finalized_at: datetime


class ReadyResponse(BaseModel):
    plan_id: str
    certification_state: str
    audit_id: str
    checked_at: datetime
    catalog_snapshot_id: str
    requirement_set_id: str


class PlanItemDetailResponse(BaseModel):
    id: str
    term_id: str
    position: int
    raw_input: str
    canonical_code: str | None = None
    course_id: str | None = None
    plan_item_status: str
    completion_status: CompletionStatus
    validation_reason: ValidationReason | None = None
    validation_meta: dict[str, Any] | None = None
    last_validated_at: datetime | None = None


class PlanDetailResponse(BaseModel):
    plan_id: str
    user_id: str
    name: str
    program_version_id: str
    pinned_catalog_snapshot_id: str
    pinned_requirement_set_id: str
    certification_state: str
    items: list[PlanItemDetailResponse]


class PlanTermResponse(BaseModel):
    id: str
    campus: str
    code: str
    year: int
    season: TermSeason


class CourseSearchResponseItem(BaseModel):
    code: str
    title: str
    credits: int
    active: bool
