from __future__ import annotations

import time
from typing import Any

import httpx
import pytest

from app.services.soc_pull import (
    WEBREG_RETRY_ATTEMPTS,
    WebRegPullAdapter,
    _compute_backoff_delay,
)


def _http_status_error(status_code: int, url: str = "https://example.test/terms") -> httpx.HTTPStatusError:
    request = httpx.Request("GET", url)
    response = httpx.Response(status_code=status_code, request=request)
    return httpx.HTTPStatusError(f"{status_code} error", request=request, response=response)


class _SequenceFetcher:
    def __init__(self, sequence: list[dict[str, Any] | Exception]):
        self._sequence = list(sequence)
        self.calls: list[tuple[str, dict[str, str]]] = []

    def __call__(self, url: str, params: dict[str, str], _headers: dict[str, str], _timeout_s: float) -> dict[str, Any]:
        self.calls.append((url, dict(params)))
        if not self._sequence:
            raise AssertionError("Fetcher exhausted")
        value = self._sequence.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


def test_compute_backoff_delay_respects_cap_and_jitter_bounds():
    low = _compute_backoff_delay(8, jitter_sample=0.0)
    high = _compute_backoff_delay(8, jitter_sample=1.0)
    assert low >= 8.0 * (1.0 - 0.3)
    assert high <= 8.0 * (1.0 + 0.3)
    assert low <= high


def test_request_retries_on_transient_5xx_then_succeeds(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("app.services.soc_pull.time.sleep", lambda _seconds: None)
    fetcher = _SequenceFetcher(
        [
            _http_status_error(500),
            {"terms": [{"term_code": "2025SU", "campus": "NB"}]},
        ]
    )
    adapter = WebRegPullAdapter(base_url="https://example.test", fetch_json=fetcher)
    payload = adapter._request_json_with_resilience(
        url="https://example.test/terms",
        params={"term_code": "2025SU", "campus": "NB"},
        started_monotonic=time.monotonic(),
        request_cache={},
    )
    assert isinstance(payload, dict)
    assert len(fetcher.calls) == 2


def test_request_retries_on_429_then_fails(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("app.services.soc_pull.time.sleep", lambda _seconds: None)
    fetcher = _SequenceFetcher([_http_status_error(429)] * WEBREG_RETRY_ATTEMPTS)
    adapter = WebRegPullAdapter(base_url="https://example.test", fetch_json=fetcher)
    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        adapter._request_json_with_resilience(
            url="https://example.test/terms",
            params={"term_code": "2025SU", "campus": "NB"},
            started_monotonic=time.monotonic(),
            request_cache={},
        )
    assert exc_info.value.response.status_code == 429
    assert len(fetcher.calls) == WEBREG_RETRY_ATTEMPTS


def test_request_retries_on_connect_timeout_then_succeeds(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("app.services.soc_pull.time.sleep", lambda _seconds: None)
    fetcher = _SequenceFetcher(
        [
            httpx.ConnectTimeout("connect timeout"),
            {"terms": [{"term_code": "2025SU", "campus": "NB"}]},
        ]
    )
    adapter = WebRegPullAdapter(base_url="https://example.test", fetch_json=fetcher)
    payload = adapter._request_json_with_resilience(
        url="https://example.test/terms",
        params={"term_code": "2025SU", "campus": "NB"},
        started_monotonic=time.monotonic(),
        request_cache={},
    )
    assert isinstance(payload, dict)
    assert len(fetcher.calls) == 2


def test_request_does_not_retry_on_non_429_4xx(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("app.services.soc_pull.time.sleep", lambda _seconds: None)
    fetcher = _SequenceFetcher([_http_status_error(404)])
    adapter = WebRegPullAdapter(base_url="https://example.test", fetch_json=fetcher)
    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        adapter._request_json_with_resilience(
            url="https://example.test/terms",
            params={"term_code": "2025SU", "campus": "NB"},
            started_monotonic=time.monotonic(),
            request_cache={},
        )
    assert exc_info.value.response.status_code == 404
    assert len(fetcher.calls) == 1


def test_request_cache_prevents_duplicate_network_hits():
    fetcher = _SequenceFetcher([{"terms": [{"term_code": "2025SU", "campus": "NB"}]}])
    adapter = WebRegPullAdapter(base_url="https://example.test", fetch_json=fetcher)
    cache: dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, Any]] = {}
    started = time.monotonic()
    first = adapter._request_json_with_resilience(
        url="https://example.test/terms",
        params={"term_code": "2025SU", "campus": "NB"},
        started_monotonic=started,
        request_cache=cache,
    )
    second = adapter._request_json_with_resilience(
        url="https://example.test/terms",
        params={"term_code": "2025SU", "campus": "NB"},
        started_monotonic=started,
        request_cache=cache,
    )
    assert first == second
    assert len(fetcher.calls) == 1


def test_slice_budget_exceeded_returns_schema_valid_unknown_completeness(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr("app.services.soc_pull.WEBREG_SLICE_BUDGET_S", 0.0)
    fetcher = _SequenceFetcher([{"terms": [{"id": "t1", "term_code": "2025SU", "campus": "NB"}]}])
    adapter = WebRegPullAdapter(base_url="https://example.test", fetch_json=fetcher)
    result = adapter.fetch(term_code="2025SU", campus="NB")
    assert result.is_complete is False
    assert result.completeness_reason == "UNKNOWN_COMPLETENESS"
    assert result.raw_payload["terms"] == [{"term_code": "2025SU", "campus": "NB"}]
    assert result.raw_payload["offerings"] == []
    assert isinstance(result.raw_payload["metadata"]["fetched_at"], str)
    assert result.raw_payload["metadata"]["fetched_at"].strip()
    assert len(fetcher.calls) == 0
