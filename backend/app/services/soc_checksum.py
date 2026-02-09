from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256


@dataclass(frozen=True)
class SocResolvedOffering:
    term_id: str
    course_id: str


def compute_soc_slice_checksum(term_id: str, resolved_rows: list[SocResolvedOffering]) -> str:
    term_id_str = str(term_id).lower()
    # Sort by course_id ASC and lowercase UUID strings are part of idempotency contract.
    course_ids = sorted(str(row.course_id).lower() for row in resolved_rows)
    if not all(str(row.term_id).lower() == term_id_str for row in resolved_rows):
        raise ValueError("resolved_rows must contain a single term slice")
    payload = "".join(f"{term_id_str},{course_id},1\n" for course_id in course_ids).encode("utf-8")
    return sha256(payload).hexdigest()
