"""DATA-027: the official policy source client retries a body that is cut off mid-download.

On 2026-10-21 the first production day on the release bundle lost the OFAC SDN XML (about 29 MB)
after about 8 MB: `response.read()` raised `http.client.IncompleteRead`. The client retried only
TimeoutError, ConnectionError, ssl.SSLError and URLError, so that one failed read ended the
component after a single attempt (REQUEST_FAILED). A body cut off in transit is now a retryable
transport failure like a reset or a TLS drop. HTTP statuses and every other exception are still
not retried, and the schedule (3 attempts, 1 s and 2 s) is unchanged.
"""

from __future__ import annotations

import http.client
import urllib.error
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

import ai_trading_system.official_policy_sources as official_module
from ai_trading_system.official_policy_sources import (
    OfficialPolicyTransportError,
    UrllibOfficialPolicyHttpClient,
    _blocker_code_for_download_exception,
    fetch_official_policy_sources,
)

URL = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML"


def _cut_off() -> http.client.IncompleteRead:
    return http.client.IncompleteRead(b"<sdnList>", 21257126)


def _scripted_cached_get(
    monkeypatch: pytest.MonkeyPatch,
    outcomes: list[object],
) -> list[str]:
    calls: list[str] = []

    def fake_cached_get(**kwargs: object) -> SimpleNamespace:
        calls.append(str(kwargs["url"]))
        outcome = outcomes.pop(0) if outcomes else b"{}"
        if isinstance(outcome, BaseException):
            raise outcome
        assert isinstance(outcome, bytes)
        return SimpleNamespace(status_code=200, headers={}, content=outcome)

    monkeypatch.setattr(official_module, "cached_urllib_get", fake_cached_get)
    return calls


def test_defaults_are_unchanged() -> None:
    client = UrllibOfficialPolicyHttpClient()

    assert client.max_attempts == 3
    assert tuple(client.retry_backoff_seconds) == (1.0, 2.0)


def test_cut_off_body_is_retried_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _scripted_cached_get(monkeypatch, [_cut_off(), b"<sdnList/>"])
    sleeps: list[float] = []
    client = UrllibOfficialPolicyHttpClient(sleeper=sleeps.append)

    response = client.get(URL)

    assert response.status_code == 200
    assert response.body == b"<sdnList/>"
    assert response.transport_attempt_count == 2
    assert len(calls) == 2
    assert sleeps == [1.0]


def test_two_cut_offs_then_success_uses_all_three_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _scripted_cached_get(monkeypatch, [_cut_off(), _cut_off(), b"<sdnList/>"])
    sleeps: list[float] = []
    client = UrllibOfficialPolicyHttpClient(sleeper=sleeps.append)

    response = client.get(URL)

    assert response.transport_attempt_count == 3
    assert len(calls) == 3
    assert sleeps == [1.0, 2.0]


def test_exhausted_cut_offs_raise_a_typed_transport_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _scripted_cached_get(monkeypatch, [_cut_off(), _cut_off(), _cut_off(), _cut_off()])
    sleeps: list[float] = []
    client = UrllibOfficialPolicyHttpClient(sleeper=sleeps.append)

    with pytest.raises(OfficialPolicyTransportError) as captured:
        client.get(URL)

    error = captured.value
    assert error.attempt_count == 3
    assert error.max_attempts == 3
    assert error.exception_type == "IncompleteRead"
    assert "21257126 more expected" in str(error)
    assert isinstance(error.__cause__, http.client.IncompleteRead)
    assert len(calls) == 3
    assert sleeps == [1.0, 2.0]


@pytest.mark.parametrize(
    "failure",
    [
        http.client.InvalidURL("nonnumeric port"),
        http.client.NotConnected(),
        ValueError("not a transport failure"),
    ],
)
def test_other_exceptions_are_not_retried(
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
) -> None:
    calls = _scripted_cached_get(monkeypatch, [failure, b"{}"])
    sleeps: list[float] = []
    client = UrllibOfficialPolicyHttpClient(sleeper=sleeps.append)

    with pytest.raises(type(failure)):
        client.get(URL)

    assert len(calls) == 1
    assert sleeps == []


def test_http_error_status_is_still_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    unauthorized = urllib.error.HTTPError(URL, 401, "Unauthorized", hdrs=None, fp=None)  # type: ignore[arg-type]
    calls = _scripted_cached_get(monkeypatch, [unauthorized, b"{}"])
    sleeps: list[float] = []
    client = UrllibOfficialPolicyHttpClient(sleeper=sleeps.append)

    with pytest.raises(urllib.error.HTTPError):
        client.get(URL)

    assert len(calls) == 1
    assert sleeps == []


class _CutOffResponse:
    """What `urllib.request.urlopen` returns when the body ends before Content-Length."""

    status = 200
    headers: Mapping[str, str] = {"Content-Type": "application/xml"}

    def __init__(self, body: bytes | None) -> None:
        self._body = body

    def __enter__(self) -> _CutOffResponse:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def read(self) -> bytes:
        if self._body is None:
            raise _cut_off()
        return self._body


def test_real_download_path_survives_a_cut_off_read(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [_CutOffResponse(None), _CutOffResponse(b"<sdnList/>")]
    opened: list[str] = []

    def fake_urlopen(request: object, timeout: float = 30) -> _CutOffResponse:
        opened.append(str(getattr(request, "full_url", request)))
        return responses.pop(0)

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    sleeps: list[float] = []
    client = UrllibOfficialPolicyHttpClient(sleeper=sleeps.append)

    response = client.get(URL)

    assert response.body == b"<sdnList/>"
    assert response.transport_attempt_count == 2
    assert opened == [URL, URL]
    assert sleeps == [1.0]


def test_cut_off_body_is_a_provider_unavailable_blocker() -> None:
    assert _blocker_code_for_download_exception(_cut_off()) == "PROVIDER_UNAVAILABLE"
    assert _blocker_code_for_download_exception(ValueError("x")) == "REQUEST_FAILED"


def test_exhausted_cut_offs_are_reported_as_transport_exhausted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _scripted_cached_get(monkeypatch, [_cut_off(), _cut_off(), _cut_off()])
    client = UrllibOfficialPolicyHttpClient(sleeper=lambda _seconds: None)

    report = fetch_official_policy_sources(
        as_of=date(2026, 10, 20),
        since=date(2026, 10, 17),
        raw_dir=tmp_path / "raw",
        processed_dir=tmp_path / "processed",
        api_keys={},
        selected_source_ids=["official_ofac_sdn_xml"],
        http_client=client,
    )

    assert report.status == "FAIL"
    assert report.blocker_codes == ("PROVIDER_UNAVAILABLE",)
    assert {issue.code for issue in report.issues} == {"official_policy_source_transport_exhausted"}
    assert "exception_type=IncompleteRead" in report.issues[0].message
    assert "attempt_count=3" in report.issues[0].message
