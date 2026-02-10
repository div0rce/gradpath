from __future__ import annotations

from typing import Any

import pytest

from app.services.soc_pull import WebRegPullAdapter, validate_soc_raw_payload


class _FakeWebRegFetcher:
    def __init__(self, *, courses_payload: Any):
        self.courses_payload = courses_payload
        self.calls: list[tuple[str, dict[str, str]]] = []

    def __call__(self, url: str, params: dict[str, str], _headers: dict[str, str], _timeout_s: float) -> Any:
        self.calls.append((url, dict(params)))
        if url.endswith("/courses.json"):
            return self.courses_payload
        raise AssertionError(f"Unexpected URL: {url}")


def test_webreg_adapter_default_fetcher_accepts_list_payload(monkeypatch: pytest.MonkeyPatch):
    class _FakeResponse:
        def raise_for_status(self) -> None:
            return

        def json(self) -> list[dict[str, Any]]:
            return [
                {
                    "courseString": "01:198:111",
                    "sections": [{"openStatus": True}],
                }
            ]

    monkeypatch.setattr("app.services.soc_pull.httpx.get", lambda *args, **kwargs: _FakeResponse())

    adapter = WebRegPullAdapter(base_url="https://classes.rutgers.edu/soc/api")
    result = adapter.fetch(term_code="2025SU", campus="NB")

    assert result.is_complete is True
    assert result.completeness_reason is None
    assert result.raw_payload["offerings"] == [
        {"term_code": "2025SU", "campus": "NB", "course_code": "01:198:111", "offered": True}
    ]
    validate_soc_raw_payload(result.raw_payload)


def test_webreg_adapter_stageable_from_courses_json():
    fetcher = _FakeWebRegFetcher(
        courses_payload={
            "courses": [
                {
                    "courseString": "01:198:111",
                    "sections": [{"openStatus": True}, {"openStatus": False}],
                },
                {
                    "courseString": "01:198:112",
                    "sections": [{"openStatus": False}],
                },
            ],
            "metadata": {"fetched_at": "2026-02-09T00:00:00Z"},
        }
    )
    adapter = WebRegPullAdapter(base_url="https://classes.rutgers.edu/soc/api", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")

    assert result.is_complete is True
    assert result.completeness_reason is None
    assert result.raw_payload["terms"] == [{"term_code": "2025SU", "campus": "NB"}]
    assert result.raw_payload["metadata"]["fetched_at"] == "2026-02-09T00:00:00Z"
    assert result.raw_payload["offerings"] == [
        {"term_code": "2025SU", "campus": "NB", "course_code": "01:198:111", "offered": True},
        {"term_code": "2025SU", "campus": "NB", "course_code": "01:198:112", "offered": False},
    ]
    assert fetcher.calls[0][1] == {"year": "2025", "term": "7", "campus": "NB"}
    validate_soc_raw_payload(result.raw_payload)


def test_webreg_adapter_ambiguous_term_mapping():
    fetcher = _FakeWebRegFetcher(courses_payload=[])
    adapter = WebRegPullAdapter(base_url="https://classes.rutgers.edu/soc/api", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025XX", campus="NB")

    assert result.is_complete is False
    assert result.completeness_reason == "AMBIGUOUS_TERM"
    assert fetcher.calls == []
    validate_soc_raw_payload(result.raw_payload)


def test_webreg_adapter_winter_term_fails_closed_until_supported():
    fetcher = _FakeWebRegFetcher(courses_payload=[])
    adapter = WebRegPullAdapter(base_url="https://classes.rutgers.edu/soc/api", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025WI", campus="NB")

    assert result.is_complete is False
    assert result.completeness_reason == "AMBIGUOUS_TERM"
    assert fetcher.calls == []
    validate_soc_raw_payload(result.raw_payload)


def test_webreg_adapter_truncation_detected():
    fetcher = _FakeWebRegFetcher(
        courses_payload={
            "courses": [{"courseString": "01:198:111", "sections": [{"openStatus": True}]}],
            "truncated": True,
        }
    )
    adapter = WebRegPullAdapter(base_url="https://classes.rutgers.edu/soc/api", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")

    assert result.is_complete is False
    assert result.completeness_reason == "TRUNCATED_RESULT"


def test_webreg_adapter_pagination_uncertain_when_has_more_true():
    fetcher = _FakeWebRegFetcher(
        courses_payload={
            "courses": [{"courseString": "01:198:111", "sections": [{"openStatus": True}]}],
            "has_more": True,
        }
    )
    adapter = WebRegPullAdapter(base_url="https://classes.rutgers.edu/soc/api", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")

    assert result.is_complete is False
    assert result.completeness_reason == "PAGINATION_UNCERTAIN"


def test_webreg_adapter_upstream_incomplete_signal():
    fetcher = _FakeWebRegFetcher(
        courses_payload={
            "courses": [{"courseString": "01:198:111", "sections": [{"openStatus": True}]}],
            "incomplete": True,
        }
    )
    adapter = WebRegPullAdapter(base_url="https://classes.rutgers.edu/soc/api", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")

    assert result.is_complete is False
    assert result.completeness_reason == "UPSTREAM_INCOMPLETE"


def test_webreg_adapter_dedupe_and_or_semantics():
    fetcher = _FakeWebRegFetcher(
        courses_payload=[
            {
                "courseString": "01:198:111",
                "sections": [{"openStatus": False}],
            },
            {
                "courseString": "01:198:111",
                "sections": [{"openStatus": True}],
            },
        ]
    )
    adapter = WebRegPullAdapter(base_url="https://classes.rutgers.edu/soc/api", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")

    assert result.is_complete is True
    assert result.raw_payload["offerings"] == [
        {"term_code": "2025SU", "campus": "NB", "course_code": "01:198:111", "offered": True}
    ]


def test_webreg_adapter_course_string_takes_precedence_over_fallback_fields():
    fetcher = _FakeWebRegFetcher(
        courses_payload=[
            {
                "courseString": "01:198:111A",
                "subject": "999",
                "courseNumber": "999",
                "sections": [{"openStatus": True}],
            }
        ]
    )
    adapter = WebRegPullAdapter(base_url="https://classes.rutgers.edu/soc/api", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")

    assert result.is_complete is True
    assert result.raw_payload["offerings"][0]["course_code"] == "01:198:111A"


def test_webreg_adapter_non_bool_open_status_fails_closed():
    fetcher = _FakeWebRegFetcher(
        courses_payload=[
            {
                "courseString": "01:198:111",
                "sections": [{"openStatus": "true"}],
            }
        ]
    )
    adapter = WebRegPullAdapter(base_url="https://classes.rutgers.edu/soc/api", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")

    assert result.is_complete is False
    assert result.completeness_reason == "UNKNOWN_COMPLETENESS"


def test_webreg_adapter_missing_identity_fails_closed():
    fetcher = _FakeWebRegFetcher(
        courses_payload=[
            {
                "sections": [{"openStatus": True}],
            }
        ]
    )
    adapter = WebRegPullAdapter(base_url="https://classes.rutgers.edu/soc/api", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")

    assert result.is_complete is False
    assert result.completeness_reason == "UNKNOWN_COMPLETENESS"


def test_webreg_adapter_empty_course_string_fails_closed():
    fetcher = _FakeWebRegFetcher(
        courses_payload=[
            {
                "courseString": "",
                "sections": [{"openStatus": True}],
            }
        ]
    )
    adapter = WebRegPullAdapter(base_url="https://classes.rutgers.edu/soc/api", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")

    assert result.is_complete is False
    assert result.completeness_reason == "UNKNOWN_COMPLETENESS"
