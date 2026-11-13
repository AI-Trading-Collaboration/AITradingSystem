"""DATA-035: an OpenAI precheck retry waits before it is sent.

On 2026-11-11 the daily's risk-event precheck lost all three attempts within about 15 s (the
connection was reset on the PC's proxy), and score_daily stopped. A retry now waits 60 s, then
180 s, so the attempts of one request span about four minutes instead of a few seconds.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

import pytest
import requests

from ai_trading_system import llm_precheck
from ai_trading_system.llm_precheck import (
    DEFAULT_OPENAI_MAX_RETRIES,
    OPENAI_RETRY_WAIT_SECONDS,
    OpenAIJsonResponse,
    _post_openai_json_with_retries,
)


class _Script:
    """Answers each POST from a list: an exception instance is raised, an int is an HTTP status."""

    def __init__(self, steps: list[object]) -> None:
        self.steps = list(steps)
        self.calls = 0

    def __call__(
        self,
        _url: str,
        _headers: Mapping[str, str],
        _payload: Mapping[str, Any],
        _timeout: float,
    ) -> OpenAIJsonResponse:
        step = self.steps[self.calls]
        self.calls += 1
        if isinstance(step, BaseException):
            raise step
        assert isinstance(step, int)
        return OpenAIJsonResponse(status_code=step, headers={}, body={"id": f"resp_{self.calls}"})


def _send(script: _Script, sleeps: list[float], *, max_retries: int = DEFAULT_OPENAI_MAX_RETRIES):
    return _post_openai_json_with_retries(
        post_json=script,
        http_client_name="custom",
        endpoint="https://api.openai.com/v1/responses",
        headers={"X-Client-Request-Id": "aits-llm-precheck-test"},
        payload={"input": "x"},
        timeout_seconds=120.0,
        precheck_id="precheck:test",
        input_checksum_sha256="0" * 64,
        max_retries=max_retries,
        sleep=sleeps.append,
    )


def _reset() -> requests.ConnectionError:
    return requests.ConnectionError("('Connection aborted.', ConnectionResetError(10054))")


def test_wait_schedule_is_60_then_180_seconds() -> None:
    assert OPENAI_RETRY_WAIT_SECONDS == (60.0, 180.0)
    assert DEFAULT_OPENAI_MAX_RETRIES == 2


def test_transport_failures_wait_before_each_retry_but_not_after_the_last() -> None:
    sleeps: list[float] = []
    script = _Script([_reset(), requests.exceptions.SSLError("EOF"), _reset()])

    result = _send(script, sleeps)

    assert script.calls == 3
    assert sleeps == [60.0, 180.0]
    assert result.response is None
    assert [issue.code for issue in result.issues] == ["openai_responses_api_request_failed"]
    attempts = result.diagnostics["attempts"]
    assert [attempt.get("retry_wait_seconds") for attempt in attempts] == [60.0, 180.0, None]
    assert "retry_wait_seconds" not in result.diagnostics["final_attempt"]
    assert [attempt.get("retry_wait_seconds") for attempt in result.audit_attempts] == [
        60.0,
        180.0,
        None,
    ]


def test_success_after_one_reset_waits_once_and_warns() -> None:
    sleeps: list[float] = []
    script = _Script([_reset(), 200])

    result = _send(script, sleeps)

    assert script.calls == 2
    assert sleeps == [60.0]
    assert result.response is not None
    assert result.response.status_code == 200
    assert [issue.code for issue in result.issues] == ["openai_responses_api_retry_succeeded"]


def test_retryable_http_statuses_also_wait() -> None:
    sleeps: list[float] = []
    script = _Script([503, 429, 200])

    result = _send(script, sleeps)

    assert script.calls == 3
    assert sleeps == [60.0, 180.0]
    assert result.response is not None
    attempts = result.diagnostics["attempts"]
    assert [attempt.get("retry_wait_seconds") for attempt in attempts] == [60.0, 180.0, None]


def test_last_retryable_http_status_fails_without_a_trailing_wait() -> None:
    sleeps: list[float] = []
    script = _Script([503, 503, 503])

    result = _send(script, sleeps)

    assert script.calls == 3
    assert sleeps == [60.0, 180.0]
    assert [issue.code for issue in result.issues] == ["openai_responses_api_error"]


def test_non_retryable_status_is_not_retried_and_does_not_wait() -> None:
    sleeps: list[float] = []
    script = _Script([400])

    result = _send(script, sleeps)

    assert script.calls == 1
    assert sleeps == []
    assert [issue.code for issue in result.issues] == ["openai_responses_api_error"]


def test_first_attempt_success_does_not_wait() -> None:
    sleeps: list[float] = []
    script = _Script([200])

    result = _send(script, sleeps)

    assert script.calls == 1
    assert sleeps == []
    assert result.issues == ()


def test_more_retries_than_waits_repeat_the_last_wait() -> None:
    sleeps: list[float] = []
    script = _Script([_reset(), _reset(), _reset(), _reset(), 200])

    result = _send(script, sleeps, max_retries=4)

    assert script.calls == 5
    assert sleeps == [60.0, 180.0, 180.0, 180.0]
    assert result.response is not None


def test_zero_retries_never_wait() -> None:
    sleeps: list[float] = []
    script = _Script([_reset()])

    result = _send(script, sleeps, max_retries=0)

    assert script.calls == 1
    assert sleeps == []
    assert [issue.code for issue in result.issues] == ["openai_responses_api_request_failed"]


def test_default_wait_goes_through_the_module_hook(monkeypatch: pytest.MonkeyPatch) -> None:
    assert llm_precheck._retry_sleep is time.sleep
    recorded: list[float] = []
    monkeypatch.setattr(llm_precheck, "_retry_sleep", recorded.append)
    script = _Script([_reset(), 200])

    result = _post_openai_json_with_retries(
        post_json=script,
        http_client_name="custom",
        endpoint="https://api.openai.com/v1/responses",
        headers={"X-Client-Request-Id": "aits-llm-precheck-test"},
        payload={"input": "x"},
        timeout_seconds=120.0,
        precheck_id="precheck:test",
        input_checksum_sha256="0" * 64,
    )

    assert recorded == [60.0]
    assert result.response is not None
