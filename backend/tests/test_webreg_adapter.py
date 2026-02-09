from __future__ import annotations

from typing import Any

from app.services.soc_pull import WebRegPullAdapter, validate_soc_raw_payload


class _FakeWebRegFetcher:
    def __init__(self, *, terms_payload: dict[str, Any], offerings_by_cursor: dict[str, dict[str, Any]]):
        self.terms_payload = terms_payload
        self.offerings_by_cursor = offerings_by_cursor
        self.calls: list[tuple[str, dict[str, str]]] = []

    def __call__(self, url: str, params: dict[str, str], _headers: dict[str, str], _timeout_s: float) -> dict[str, Any]:
        self.calls.append((url, dict(params)))
        if url.endswith("/terms"):
            return self.terms_payload
        if url.endswith("/offerings"):
            key = ""
            if "cursor" in params:
                key = f"cursor:{params['cursor']}"
                if key not in self.offerings_by_cursor:
                    key = params["cursor"]
            elif "offset" in params:
                key = f"offset:{params['offset']}"
                if key not in self.offerings_by_cursor:
                    key = params["offset"]
            return self.offerings_by_cursor[key]
        raise AssertionError(f"Unexpected URL: {url}")


def test_webreg_adapter_paginated_success_is_stageable():
    fetcher = _FakeWebRegFetcher(
        terms_payload={
            "terms": [{"id": "t1", "term_code": "2025SU", "campus": "NB"}],
            "metadata": {"fetched_at": "2026-02-09T00:00:00Z"},
        },
        offerings_by_cursor={
            "": {
                "offerings": [{"course_code": "14:540:100", "offered": True}],
                "next_cursor": "c2",
                "has_more": True,
            },
            "c2": {
                "offerings": [{"course_code": "14:540:200", "offered": False}],
                "has_more": False,
            },
        },
    )
    adapter = WebRegPullAdapter(base_url="https://example.test", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")

    assert result.is_complete is True
    assert result.completeness_reason is None
    assert result.raw_payload["terms"] == [{"term_code": "2025SU", "campus": "NB"}]
    assert result.raw_payload["metadata"]["fetched_at"] == "2026-02-09T00:00:00Z"
    assert result.raw_payload["offerings"] == [
        {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:100", "offered": True},
        {"term_code": "2025SU", "campus": "NB", "course_code": "14:540:200", "offered": False},
    ]
    validate_soc_raw_payload(result.raw_payload)


def test_webreg_adapter_ambiguous_term_mapping():
    fetcher = _FakeWebRegFetcher(
        terms_payload={
            "terms": [
                {"id": "t1", "term_code": "2025SU", "campus": "NB"},
                {"id": "t2", "term_code": "2025SU", "campus": "NB"},
            ]
        },
        offerings_by_cursor={},
    )
    adapter = WebRegPullAdapter(base_url="https://example.test", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")

    assert result.is_complete is False
    assert result.completeness_reason == "AMBIGUOUS_TERM"
    validate_soc_raw_payload(result.raw_payload)


def test_webreg_adapter_truncation_detected():
    fetcher = _FakeWebRegFetcher(
        terms_payload={"terms": [{"id": "t1", "term_code": "2025SU", "campus": "NB"}]},
        offerings_by_cursor={
            "": {
                "offerings": [{"course_code": "14:540:100", "offered": True}],
                "has_more": False,
                "truncated": True,
            }
        },
    )
    adapter = WebRegPullAdapter(base_url="https://example.test", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")

    assert result.is_complete is False
    assert result.completeness_reason == "TRUNCATED_RESULT"


def test_webreg_adapter_pagination_uncertain_on_cursor_loop():
    fetcher = _FakeWebRegFetcher(
        terms_payload={"terms": [{"id": "t1", "term_code": "2025SU", "campus": "NB"}]},
        offerings_by_cursor={
            "": {
                "offerings": [{"course_code": "14:540:100", "offered": True}],
                "next_cursor": "c1",
                "has_more": True,
            },
            "c1": {
                "offerings": [{"course_code": "14:540:200", "offered": True}],
                "next_cursor": "c1",
                "has_more": True,
            },
        },
    )
    adapter = WebRegPullAdapter(base_url="https://example.test", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")

    assert result.is_complete is False
    assert result.completeness_reason == "PAGINATION_UNCERTAIN"


def test_webreg_adapter_upstream_incomplete_signal():
    fetcher = _FakeWebRegFetcher(
        terms_payload={"terms": [{"id": "t1", "term_code": "2025SU", "campus": "NB"}]},
        offerings_by_cursor={
            "": {
                "offerings": [{"course_code": "14:540:100", "offered": True}],
                "has_more": False,
                "incomplete": True,
            }
        },
    )
    adapter = WebRegPullAdapter(base_url="https://example.test", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")

    assert result.is_complete is False
    assert result.completeness_reason == "UPSTREAM_INCOMPLETE"


def test_webreg_adapter_success_payload_passes_validator():
    fetcher = _FakeWebRegFetcher(
        terms_payload={"terms": [{"id": "t1", "term_code": "2025SU", "campus": "NB"}]},
        offerings_by_cursor={
            "": {
                "offerings": [{"course_code": "14:540:100", "offered": True}],
                "has_more": False,
            }
        },
    )
    adapter = WebRegPullAdapter(base_url="https://example.test", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")
    validate_soc_raw_payload(result.raw_payload)


def test_webreg_adapter_cursor_type_change_does_not_false_loop():
    fetcher = _FakeWebRegFetcher(
        terms_payload={"terms": [{"id": "t1", "term_code": "2025SU", "campus": "NB"}]},
        offerings_by_cursor={
            "": {
                "offerings": [{"course_code": "14:540:100", "offered": True}],
                "next_cursor": 1,
                "has_more": True,
            },
            "offset:1": {
                "offerings": [{"course_code": "14:540:200", "offered": True}],
                "next_cursor": "1",
                "has_more": True,
            },
            "cursor:1": {
                "offerings": [{"course_code": "14:540:300", "offered": False}],
                "has_more": False,
            },
        },
    )
    adapter = WebRegPullAdapter(base_url="https://example.test", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")

    assert result.is_complete is True
    assert result.completeness_reason is None
    assert [row["course_code"] for row in result.raw_payload["offerings"]] == [
        "14:540:100",
        "14:540:200",
        "14:540:300",
    ]


def test_webreg_adapter_single_page_without_pagination_fields_is_complete():
    fetcher = _FakeWebRegFetcher(
        terms_payload={"terms": [{"id": "t1", "term_code": "2025SU", "campus": "NB"}]},
        offerings_by_cursor={
            "": {
                "offerings": [{"course_code": "14:540:100", "offered": True}],
            }
        },
    )
    adapter = WebRegPullAdapter(base_url="https://example.test", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")

    assert result.is_complete is True
    assert result.completeness_reason is None


def test_webreg_adapter_incoherent_pagination_is_unknown_completeness():
    fetcher = _FakeWebRegFetcher(
        terms_payload={"terms": [{"id": "t1", "term_code": "2025SU", "campus": "NB"}]},
        offerings_by_cursor={
            "": {
                "offerings": [{"course_code": "14:540:100", "offered": True}],
                "offset": 0,
            }
        },
    )
    adapter = WebRegPullAdapter(base_url="https://example.test", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")

    assert result.is_complete is False
    assert result.completeness_reason == "UNKNOWN_COMPLETENESS"
