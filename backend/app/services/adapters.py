from __future__ import annotations

from abc import ABC, abstractmethod
import csv
import json
from pathlib import Path
from typing import Any


class RegistrarFeedAdapter(ABC):
    @abstractmethod
    def fetch_candidate_payload(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def validate_schema(self, payload: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def to_canonical_rows(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def source_metadata(self) -> dict[str, Any]:
        raise NotImplementedError


class DepartmentCSVAdapter(RegistrarFeedAdapter):
    def __init__(self, bundle_dir: Path):
        self.bundle_dir = bundle_dir

    def fetch_candidate_payload(self) -> dict[str, Any]:
        def read_csv(filename: str) -> list[dict[str, str]]:
            path = self.bundle_dir / filename
            if not path.exists():
                raise ValueError(f"Missing required file: {path}")
            with path.open(newline="", encoding="utf-8") as f:
                return list(csv.DictReader(f))

        return {
            "courses": read_csv("courses.csv"),
            "terms": read_csv("terms.csv"),
            "offerings": read_csv("offerings.csv"),
            "rules": read_csv("rules.csv"),
            "programs": read_csv("programs.csv"),
            "program_requirements": read_csv("program_requirements.csv"),
        }

    def validate_schema(self, payload: dict[str, Any]) -> None:
        required = ["courses", "terms", "offerings", "rules", "programs", "program_requirements"]
        for key in required:
            if key not in payload:
                raise ValueError(f"Invalid DepartmentCSV payload: missing {key}")
            if not isinstance(payload[key], list):
                raise ValueError(f"Invalid DepartmentCSV payload: {key} must be a list")

    def to_canonical_rows(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.validate_schema(payload)

        def to_bool(v: str, default: bool = False) -> bool:
            if v is None:
                return default
            return str(v).strip().lower() in {"1", "true", "yes", "y", "x"}

        req_rows = payload["program_requirements"]
        req_keyed: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in req_rows:
            key = (row["program_code"], row["requirement_set_label"])
            req_keyed.setdefault(key, []).append(
                {
                    "orderIndex": int(row.get("orderIndex") or 0),
                    "label": row["label"],
                    "rule": json.loads(row["rule"]),
                }
            )

        programs: list[dict[str, Any]] = []
        for row in payload["programs"]:
            key = (row["code"], row["requirement_set_label"])
            programs.append(
                {
                    "code": row["code"],
                    "name": row["name"],
                    "campus": row["campus"],
                    "catalog_year": row["catalog_year"],
                    "effective_from": row["effective_from"],
                    "effective_to": row.get("effective_to") or None,
                    "requirement_set_label": row["requirement_set_label"],
                    "requirements": sorted(req_keyed.get(key, []), key=lambda x: x["orderIndex"]),
                }
            )

        return {
            "courses": [
                {
                    "code": row["code"],
                    "title": row["title"],
                    "credits": int(row["credits"]),
                    "active": to_bool(row.get("active", "true"), default=True),
                    "category": row.get("category") or None,
                }
                for row in payload["courses"]
            ],
            "terms": [
                {
                    "campus": row["campus"],
                    "code": row["code"],
                    "year": int(row["year"]),
                    "season": row["season"],
                    "starts_at": row.get("starts_at") or None,
                    "ends_at": row.get("ends_at") or None,
                }
                for row in payload["terms"]
            ],
            "offerings": [
                {
                    "course_code": row["course_code"],
                    "term_code": row["term_code"],
                    "campus": row["campus"],
                    "offered": to_bool(row.get("offered", "true"), default=True),
                }
                for row in payload["offerings"]
            ],
            "rules": [
                {
                    "course_code": row["course_code"],
                    "kind": row["kind"],
                    "rule": json.loads(row["rule"]),
                    "notes": row.get("notes") or None,
                }
                for row in payload["rules"]
            ],
            "programs": programs,
        }

    def source_metadata(self) -> dict[str, Any]:
        return {"adapter": "DepartmentCSVAdapter"}


class SOCExportAdapter(RegistrarFeedAdapter):
    def fetch_candidate_payload(self) -> dict[str, Any]:
        # Placeholder for scraping/export pull logic.
        return {"raw": []}

    def validate_schema(self, payload: dict[str, Any]) -> None:
        if "raw" not in payload:
            raise ValueError("Invalid SOC payload")

    def to_canonical_rows(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.validate_schema(payload)
        raise NotImplementedError("SOC normalization rules are institution-specific")

    def source_metadata(self) -> dict[str, Any]:
        return {"adapter": "SOCExportAdapter"}


class SISAdapter(RegistrarFeedAdapter):
    def fetch_candidate_payload(self) -> dict[str, Any]:
        # Explicitly blocked until contract + credentials + scopes are configured.
        raise NotImplementedError("SIS adapter requires approved integration contract")

    def validate_schema(self, payload: dict[str, Any]) -> None:
        raise NotImplementedError

    def to_canonical_rows(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def source_metadata(self) -> dict[str, Any]:
        return {"adapter": "SISAdapter"}
