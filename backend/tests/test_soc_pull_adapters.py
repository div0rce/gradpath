from __future__ import annotations

import pytest

from app.services.soc_pull import canonicalize_soc_raw_payload


def test_canonicalize_soc_raw_payload_sorts_and_normalizes_fields():
    payload = {
        "terms": [{"term_code": "2025SU", "campus": "NB"}],
        "offerings": [
            {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:200", "offered": True},
            {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:100", "offered": True},
        ],
        "metadata": {
            "source_urls": ["https://z.example", "https://a.example"],
            "fetched_at": "2026-02-09T00:00:00Z",
            "parse_warnings": [{"foo": 1}, "already-string"],
        },
    }
    canonical = canonicalize_soc_raw_payload(payload, term_code="2025SU", campus="NB")
    assert [r["course_code"] for r in canonical["offerings"]] == ["14:540:100", "14:540:200"]
    assert canonical["metadata"]["source_urls"] == ["https://a.example", "https://z.example"]
    assert canonical["metadata"]["parse_warnings"] == ["already-string", "{'foo': 1}"]
    assert isinstance(canonical["metadata"]["raw_hash"], str)
    assert canonical["metadata"]["raw_hash"] != ""


def test_canonicalize_soc_raw_payload_raw_hash_ignores_existing_raw_hash():
    payload = {
        "terms": [{"term_code": "2025SU", "campus": "NB"}],
        "offerings": [
            {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:100", "offered": True},
        ],
        "metadata": {
            "source_urls": ["https://a.example"],
            "fetched_at": "2026-02-09T00:00:00Z",
            "parse_warnings": [],
            "raw_hash": "stale",
        },
    }
    first = canonicalize_soc_raw_payload(payload, term_code="2025SU", campus="NB")
    payload["metadata"]["raw_hash"] = "stale-but-different"
    second = canonicalize_soc_raw_payload(payload, term_code="2025SU", campus="NB")
    assert first["metadata"]["raw_hash"] == second["metadata"]["raw_hash"]


def test_canonicalize_soc_raw_payload_enforces_single_requested_term():
    with pytest.raises(ValueError) as exc_info:
        canonicalize_soc_raw_payload(
            {
                "terms": [
                    {"term_code": "2025SU", "campus": "NB"},
                    {"term_code": "2025SU", "campus": "NB"},
                ],
                "offerings": [],
                "metadata": {"source_urls": [], "parse_warnings": [], "fetched_at": "2026-02-09T00:00:00Z"},
            },
            term_code="2025SU",
            campus="NB",
        )
    assert exc_info.value.args[0]["error_code"] == "SOC_SCHEMA_VIOLATION"


def test_canonicalize_soc_raw_payload_ignores_out_of_slice_offerings():
    canonical = canonicalize_soc_raw_payload(
        {
            "terms": [{"term_code": "2025SU", "campus": "NB"}],
            "offerings": [
                {"term_code": "2025FA", "campus": "NB", "course_code": "14:540:999", "offered": True},
                {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:100", "offered": True},
            ],
            "metadata": {"source_urls": [], "parse_warnings": [], "fetched_at": "2026-02-09T00:00:00Z"},
        },
        term_code="2025SU",
        campus="NB",
    )
    assert len(canonical["offerings"]) == 1
    assert canonical["offerings"][0]["course_code"] == "14:540:100"


def test_canonicalize_soc_raw_payload_preserves_offered_bool():
    canonical = canonicalize_soc_raw_payload(
        {
            "terms": [{"term_code": "2025SU", "campus": "NB"}],
            "offerings": [
                {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:100", "offered": False},
            ],
            "metadata": {"source_urls": [], "parse_warnings": [], "fetched_at": "2026-02-09T00:00:00Z"},
        },
        term_code="2025SU",
        campus="NB",
    )
    assert canonical["offerings"][0]["offered"] is False

