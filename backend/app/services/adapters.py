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
        parse_errors: list[dict[str, Any]] = []

        def to_bool(v: str, default: bool = False) -> bool:
            if v is None:
                return default
            return str(v).strip().lower() in {"1", "true", "yes", "y", "x"}

        def parse_json_field(
            *,
            raw: str | None,
            filename: str,
            row_number: int,
            field: str,
        ) -> Any | None:
            if raw is None:
                parse_errors.append(
                    {
                        "file": filename,
                        "row": row_number,
                        "field": field,
                        "error": "Missing JSON value",
                    }
                )
                return None
            try:
                return json.loads(raw)
            except Exception as exc:
                parse_errors.append(
                    {
                        "file": filename,
                        "row": row_number,
                        "field": field,
                        "error": str(exc),
                    }
                )
                return None

        req_rows = payload["program_requirements"]
        req_keyed: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for i, row in enumerate(req_rows, start=2):
            parsed_rule = parse_json_field(
                raw=row.get("rule"),
                filename="program_requirements.csv",
                row_number=i,
                field="rule",
            )
            if parsed_rule is None:
                continue
            key = (row["program_code"], row["requirement_set_label"])
            req_keyed.setdefault(key, []).append(
                {
                    "orderIndex": int(row.get("orderIndex") or 0),
                    "label": row["label"],
                    "rule": parsed_rule,
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

        rules: list[dict[str, Any]] = []
        for i, row in enumerate(payload["rules"], start=2):
            parsed_rule = parse_json_field(
                raw=row.get("rule"),
                filename="rules.csv",
                row_number=i,
                field="rule",
            )
            if parsed_rule is None:
                continue
            rules.append(
                {
                    "course_code": row["course_code"],
                    "kind": row["kind"],
                    "rule": parsed_rule,
                    "notes": row.get("notes") or None,
                }
            )

        if parse_errors:
            raise ValueError({"error_code": "CSV_PARSE_ERROR", "errors": parse_errors})

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
            "rules": rules,
            "programs": programs,
        }

    def source_metadata(self) -> dict[str, Any]:
        return {"adapter": "DepartmentCSVAdapter"}


class SOCExportAdapter(RegistrarFeedAdapter):
    def __init__(self, *, raw_payload: dict[str, Any] | None, ingest_source: str):
        self._raw_payload = raw_payload
        self._ingest_source = ingest_source

    def fetch_candidate_payload(self) -> dict[str, Any]:
        if self._raw_payload is None:
            raise ValueError({"error_code": "SOC_FETCH_FAILED", "message": "No SOC payload provided"})
        return self._raw_payload

    def validate_schema(self, payload: dict[str, Any]) -> None:
        allowed_top_keys = {"terms", "offerings", "metadata"}
        unexpected = sorted(set(payload.keys()) - allowed_top_keys)
        if unexpected:
            raise ValueError({"error_code": "SOC_SCHEMA_VIOLATION", "unexpected_keys": unexpected})

        if "terms" not in payload or not isinstance(payload["terms"], list):
            raise ValueError({"error_code": "SOC_SCHEMA_VIOLATION", "field": "terms"})
        if "offerings" not in payload or not isinstance(payload["offerings"], list):
            raise ValueError({"error_code": "SOC_SCHEMA_VIOLATION", "field": "offerings"})

        metadata = payload.get("metadata") or {}
        if not isinstance(metadata, dict):
            raise ValueError({"error_code": "SOC_SCHEMA_VIOLATION", "field": "metadata"})
        allowed_metadata = {"source_urls", "fetched_at", "raw_hash", "parse_warnings"}
        unexpected_meta = sorted(set(metadata.keys()) - allowed_metadata)
        if unexpected_meta:
            raise ValueError({"error_code": "SOC_SCHEMA_VIOLATION", "unexpected_metadata": unexpected_meta})

        for idx, row in enumerate(payload["terms"], start=1):
            if not isinstance(row, dict):
                raise ValueError({"error_code": "SOC_SCHEMA_VIOLATION", "field": "terms", "index": idx})
            expected = {"term_code", "campus"}
            if set(row.keys()) != expected:
                raise ValueError(
                    {"error_code": "SOC_SCHEMA_VIOLATION", "field": "terms", "index": idx, "expected": sorted(expected)}
                )
            if not row["term_code"] or not row["campus"]:
                raise ValueError({"error_code": "SOC_SCHEMA_VIOLATION", "field": "terms", "index": idx})

        for idx, row in enumerate(payload["offerings"], start=1):
            if not isinstance(row, dict):
                raise ValueError({"error_code": "SOC_SCHEMA_VIOLATION", "field": "offerings", "index": idx})
            expected = {"term_code", "campus", "course_code", "offered"}
            if set(row.keys()) != expected:
                raise ValueError(
                    {
                        "error_code": "SOC_SCHEMA_VIOLATION",
                        "field": "offerings",
                        "index": idx,
                        "expected": sorted(expected),
                    }
                )
            if not row["term_code"] or not row["campus"] or not row["course_code"]:
                raise ValueError({"error_code": "SOC_SCHEMA_VIOLATION", "field": "offerings", "index": idx})
            if not isinstance(row["offered"], bool):
                raise ValueError({"error_code": "SOC_SCHEMA_VIOLATION", "field": "offerings", "index": idx})

    def to_canonical_rows(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.validate_schema(payload)
        return {
            "terms": [
                {"term_code": str(row["term_code"]), "campus": str(row["campus"])}
                for row in payload["terms"]
            ],
            "offerings": [
                {
                    "term_code": str(row["term_code"]),
                    "campus": str(row["campus"]),
                    "course_code": str(row["course_code"]),
                    "offered": bool(row["offered"]),
                }
                for row in payload["offerings"]
            ],
            "metadata": payload.get("metadata") or {},
        }

    def source_metadata(self) -> dict[str, Any]:
        return {"adapter": "SOCExportAdapter", "ingest_source": self._ingest_source}


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
