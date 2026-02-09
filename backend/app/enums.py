from enum import Enum


class CatalogSnapshotStatus(str, Enum):
    STAGED = "STAGED"
    PUBLISHED = "PUBLISHED"
    FAILED = "FAILED"
    ARCHIVED = "ARCHIVED"


class CatalogSource(str, Enum):
    DEPARTMENT_CSV = "DEPARTMENT_CSV"
    SOC_SCRAPE = "SOC_SCRAPE"
    SIS = "SIS"


class UserRole(str, Enum):
    STUDENT = "STUDENT"
    ADVISOR = "ADVISOR"
    ADMIN = "ADMIN"


class RequirementSetStatus(str, Enum):
    DRAFT = "DRAFT"
    APPROVED = "APPROVED"
    RETIRED = "RETIRED"


class TermSeason(str, Enum):
    FALL = "FALL"
    WINTER = "WINTER"
    SPRING = "SPRING"
    SUMMER = "SUMMER"


class RuleKind(str, Enum):
    PREREQ = "PREREQ"
    COREQ = "COREQ"
    RESTRICTION = "RESTRICTION"


class PlanItemStatus(str, Enum):
    DRAFT = "DRAFT"
    VALID = "VALID"
    INVALID = "INVALID"


class CompletionStatus(str, Enum):
    YES = "YES"
    IN_PROGRESS = "IN_PROGRESS"
    NO = "NO"
    BLANK = "BLANK"


class ValidationReason(str, Enum):
    INVALID_COURSE = "INVALID_COURSE"
    NOT_OFFERED = "NOT_OFFERED"
    PREREQ_MISSING = "PREREQ_MISSING"
    UNSUPPORTED_RULE = "UNSUPPORTED_RULE"


class CertificationState(str, Enum):
    DRAFT = "DRAFT"
    READY = "READY"
    CERTIFIED = "CERTIFIED"


class AuditRequirementStatus(str, Enum):
    SATISFIED = "SATISFIED"
    PENDING = "PENDING"
    MISSING = "MISSING"
    UNKNOWN = "UNKNOWN"
