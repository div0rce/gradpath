from __future__ import annotations

import pytest

from app.services.soc_pull import validate_soc_raw_payload


def test_validate_soc_raw_payload_accepts_valid_payload():
    payload = {
        "terms": [{"term_code": "2025SU", "campus": "NB"}],
        "offerings": [
            {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:100", "offered": True},
        ],
        "metadata": {
            "source_urls": ["https://example.test"],
            "fetched_at": "2026-02-09T00:00:00Z",
            "raw_hash": "abc123",
            "parse_warnings": [],
        },
    }
    validate_soc_raw_payload(payload)


def test_validate_soc_raw_payload_rejects_unexpected_top_keys():
    with pytest.raises(ValueError) as exc_info:
        validate_soc_raw_payload(
            {
                "terms": [],
                "offerings": [],
                "metadata": {"source_urls": [], "parse_warnings": []},
                "extra": 1,
            }
        )
    assert exc_info.value.args[0]["error_code"] == "SOC_SCHEMA_VIOLATION"


def test_validate_soc_raw_payload_requires_parse_warnings_list_of_strings():
    with pytest.raises(ValueError) as exc_info:
        validate_soc_raw_payload(
            {
                "terms": [{"term_code": "2025SU", "campus": "NB"}],
                "offerings": [
                    {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:100", "offered": True},
                ],
                "metadata": {"source_urls": [], "parse_warnings": [1]},
            }
        )
    assert exc_info.value.args[0]["error_code"] == "SOC_SCHEMA_VIOLATION"


def test_validate_soc_raw_payload_requires_offered_bool():
    with pytest.raises(ValueError) as exc_info:
        validate_soc_raw_payload(
            {
                "terms": [{"term_code": "2025SU", "campus": "NB"}],
                "offerings": [
                    {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:100", "offered": "yes"},
                ],
                "metadata": {"source_urls": [], "parse_warnings": []},
            }
        )
    assert exc_info.value.args[0]["error_code"] == "SOC_SCHEMA_VIOLATION"
