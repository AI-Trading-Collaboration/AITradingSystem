"""DEVX-016 S3: the pre-publication checklist and the owner authorization record."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.platform.architecture.publication_checks import (
    AUTHORIZATION_SCHEMA_VERSION,
    AUTHORIZATION_SCOPE,
    PrePublishFacts,
    PublicationAuthorizationError,
    checks_summary,
    evaluate_pre_publish_checks,
    load_authorization,
)

NOW = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
CANDIDATE = "c" * 40
MAIN = "a" * 40


def _facts(**overrides: Any) -> PrePublishFacts:
    facts = PrePublishFacts(
        main=MAIN,
        expected_main=MAIN,
        orig_head=None,
        git_locks=(),
        live_processes=(),
        lease_replay_status="PASS",
        active_leases=("lease-1",),
        expected_lease="lease-1",
        full_ended_at=NOW - timedelta(minutes=30),
        observed_at=NOW,
    )
    return replace(facts, **overrides)


def test_a_clean_state_passes_all_six_checks() -> None:
    results = evaluate_pre_publish_checks(_facts())
    assert [r.check_id for r in results] == [
        "1_main_equals_expected_main",
        "2_orig_head_absent_or_not_main",
        "3_no_stale_git_locks",
        "4_no_live_git_or_python_processes",
        "5_real_lease_store_replay",
        "6_within_two_hours_of_full_end",
    ]
    assert all(r.ok for r in results) and checks_summary(results)["all_ok"] is True


@pytest.mark.parametrize(
    "override,failing",
    [
        ({"main": "b" * 40}, "1_main_equals_expected_main"),
        ({"orig_head": MAIN}, "2_orig_head_absent_or_not_main"),
        ({"git_locks": ("AUTO_MERGE.lock",)}, "3_no_stale_git_locks"),
        (
            {"live_processes": ("123|1|python.exe|python -m pytest",)},
            "4_no_live_git_or_python_processes",
        ),
        ({"lease_replay_status": "FAIL"}, "5_real_lease_store_replay"),
        ({"active_leases": ("lease-1", "lease-2")}, "5_real_lease_store_replay"),
        ({"active_leases": ()}, "5_real_lease_store_replay"),
        ({"active_leases": ("other",)}, "5_real_lease_store_replay"),
        ({"full_ended_at": NOW - timedelta(hours=2, minutes=1)}, "6_within_two_hours_of_full_end"),
        ({"full_ended_at": NOW + timedelta(minutes=1)}, "6_within_two_hours_of_full_end"),
    ],
)
def test_each_check_fails_on_its_own_condition(override: dict[str, Any], failing: str) -> None:
    results = evaluate_pre_publish_checks(_facts(**override))
    assert [r.check_id for r in results if not r.ok] == [failing]
    summary = checks_summary(results)
    assert summary["all_ok"] is False and summary["checks"][failing]["ok"] is False


def test_the_window_comes_from_the_policy_not_from_a_constant() -> None:
    facts = _facts(full_ended_at=NOW - timedelta(hours=3))
    assert not all(r.ok for r in evaluate_pre_publish_checks(facts))
    wider = evaluate_pre_publish_checks(facts, max_hours_since_full_end=4.0)
    assert all(r.ok for r in wider)
    assert wider[-1].detail["limit_hours"] == 4.0


def test_the_two_hour_boundary_is_inclusive() -> None:
    edge = evaluate_pre_publish_checks(_facts(full_ended_at=NOW - timedelta(hours=2)))
    assert all(r.ok for r in edge)


def _record(tmp_path: Path, **overrides: Any) -> Path:
    body: dict[str, Any] = {
        "schema_version": AUTHORIZATION_SCHEMA_VERSION,
        "scope": AUTHORIZATION_SCOPE,
        "candidate_sha": CANDIDATE,
        "expected_main": MAIN,
        "force_push": False,
        "pull_request": False,
        "authorized_by": "project_owner",
        "authorized_at": (NOW - timedelta(minutes=10)).isoformat(),
        "source": "AskUserQuestion 2026-10-07: passes -> publish directly",
    }
    body.update(overrides)
    path = tmp_path / "authorization.json"
    path.write_text(json.dumps(body), encoding="utf-8")
    return path


def _load(path: Path, **kwargs: Any) -> Any:
    arguments: dict[str, Any] = {
        "candidate_sha": CANDIDATE,
        "expected_main": MAIN,
        "not_before": NOW - timedelta(hours=1),
        "now": NOW,
    }
    arguments.update(kwargs)
    return load_authorization(path, **arguments)


def test_a_matching_record_is_accepted(tmp_path: Path) -> None:
    authorization = _load(_record(tmp_path))
    assert (
        authorization.candidate_sha == CANDIDATE and authorization.authorized_by == "project_owner"
    )


@pytest.mark.parametrize(
    "overrides,code",
    [
        ({"schema_version": "other"}, "PUBLICATION_AUTHORIZATION_SCHEMA"),
        ({"scope": "PUBLISH_ANYTHING"}, "PUBLICATION_AUTHORIZATION_SCOPE"),
        ({"force_push": True}, "PUBLICATION_AUTHORIZATION_BROAD"),
        ({"pull_request": True}, "PUBLICATION_AUTHORIZATION_BROAD"),
        ({"force_push": None}, "PUBLICATION_AUTHORIZATION_BROAD"),
        ({"candidate_sha": "d" * 40}, "PUBLICATION_AUTHORIZATION_CANDIDATE"),
        ({"expected_main": "e" * 40}, "PUBLICATION_AUTHORIZATION_BASE"),
        ({"authorized_by": ""}, "PUBLICATION_AUTHORIZATION_PROVENANCE"),
        ({"source": 7}, "PUBLICATION_AUTHORIZATION_PROVENANCE"),
        ({"authorized_at": "not a time"}, "PUBLICATION_AUTHORIZATION_TIME"),
        ({"authorized_at": "2026-10-07T11:50:00"}, "PUBLICATION_AUTHORIZATION_TIME"),  # naive
        (
            {"authorized_at": (NOW + timedelta(hours=1)).isoformat()},
            "PUBLICATION_AUTHORIZATION_TIME",
        ),
        (
            {"authorized_at": (NOW - timedelta(hours=3)).isoformat()},
            "PUBLICATION_AUTHORIZATION_STALE",
        ),
    ],
)
def test_a_record_that_grants_more_or_something_else_is_refused(
    tmp_path: Path, overrides: dict[str, Any], code: str
) -> None:
    with pytest.raises(PublicationAuthorizationError) as raised:
        _load(_record(tmp_path, **overrides))
    assert raised.value.code == code


def test_a_missing_or_unreadable_record_is_refused(tmp_path: Path) -> None:
    with pytest.raises(PublicationAuthorizationError) as raised:
        _load(tmp_path / "absent.json")
    assert raised.value.code == "PUBLICATION_AUTHORIZATION_UNREADABLE"
    broken = tmp_path / "broken.json"
    broken.write_text("{not json", encoding="utf-8")
    with pytest.raises(PublicationAuthorizationError) as raised:
        _load(broken)
    assert raised.value.code == "PUBLICATION_AUTHORIZATION_UNREADABLE"
