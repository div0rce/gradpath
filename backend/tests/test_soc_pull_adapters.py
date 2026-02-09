from __future__ import annotations

import pytest

from app.services.soc_pull import WebRegPullAdapter, canonicalize_soc_raw_payload


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


def test_webreg_pull_adapter_fetch_returns_unknown_completeness_when_not_reported():
    def fake_fetch(_url: str, _params: dict[str, str], _headers: dict[str, str], _timeout_s: float):
        return {
            "payload": {
                "terms": [{"term_code": "2025SU", "campus": "NB"}],
                "offerings": [
                    {
                        "term_code": "2025SU",
                        "campus": "NB",
                        "course_code": "14:540:100",
                        "offered": True,
                    }
                ],
                "metadata": {"source_urls": ["https://example.test"], "parse_warnings": [{"warn": 1}]},
            }
        }

    adapter = WebRegPullAdapter(base_url="https://example.test", fetch_json=fake_fetch)
    result = adapter.fetch(term_code="2025SU", campus="NB")
    assert result.is_complete is False
    assert result.completeness_reason == "UNKNOWN_COMPLETENESS"
    assert result.raw_payload["terms"] == [{"term_code": "2025SU", "campus": "NB"}]
    assert result.raw_payload["metadata"]["parse_warnings"] == ["{'warn': 1}"]
    assert "raw_hash" not in result.raw_payload["metadata"]


def test_webreg_pull_adapter_preserves_upstream_fetched_at():
    def fake_fetch(_url: str, _params: dict[str, str], _headers: dict[str, str], _timeout_s: float):
        return {
            "payload": {
                "terms": [{"term_code": "2025SU", "campus": "NB"}],
                "offerings": [
                    {
                        "term_code": "2025SU",
                        "campus": "NB",
                        "course_code": "14:540:100",
                        "offered": True,
                    }
                ],
                "metadata": {
                    "source_urls": ["https://example.test"],
                    "parse_warnings": [],
                    "fetched_at": "2026-02-09T00:00:00Z",
                },
            },
            "is_complete": True,
            "completeness_reason": None,
        }

    adapter = WebRegPullAdapter(base_url="https://example.test", fetch_json=fake_fetch)
    result = adapter.fetch(term_code="2025SU", campus="NB")
    assert result.raw_payload["metadata"]["fetched_at"] == "2026-02-09T00:00:00Z"


def test_webreg_pull_adapter_sets_fetched_at_when_missing():
    def fake_fetch(_url: str, _params: dict[str, str], _headers: dict[str, str], _timeout_s: float):
        return {
            "payload": {
                "terms": [{"term_code": "2025SU", "campus": "NB"}],
                "offerings": [
                    {
                        "term_code": "2025SU",
                        "campus": "NB",
                        "course_code": "14:540:100",
                        "offered": True,
                    }
                ],
                "metadata": {"source_urls": ["https://example.test"], "parse_warnings": []},
            },
            "is_complete": True,
            "completeness_reason": None,
        }

    adapter = WebRegPullAdapter(base_url="https://example.test", fetch_json=fake_fetch)
    result = adapter.fetch(term_code="2025SU", campus="NB")
    fetched_at = result.raw_payload["metadata"]["fetched_at"]
    assert isinstance(fetched_at, str)
    assert fetched_at.strip() != ""
