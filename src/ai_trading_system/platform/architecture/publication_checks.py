"""DEVX-016 S3: the pre-publication checklist and the owner authorization record.

The six checks are the DEVX-021 section 8.3 checklist the manual flow ran before every local
publication; here they are pure functions over observed facts, so the decision is testable and the
facts are recorded in the run journal. The authorization record is the explicit owner permission to
push THIS candidate (ordinary push only); without a matching record nothing after the gate can run.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

AUTHORIZATION_SCHEMA_VERSION = "devx_016_publication_authorization.v1"
AUTHORIZATION_SCOPE = "ORDINARY_PUSH_THIS_CANDIDATE_ONLY"
# The Full result is only valid for local publication while the execution lease is alive (TTL 6 h,
# extended by the validation driver's heartbeat); the manual checklist required the publication to
# start within two hours of the Full's end. DEVX-021 section 8.3.
MAX_HOURS_SINCE_FULL_END = 2.0


class PublicationAuthorizationError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class PrePublishFacts:
    main: str
    expected_main: str
    orig_head: str | None
    git_locks: tuple[str, ...]
    live_processes: tuple[str, ...]
    lease_replay_status: str
    active_leases: tuple[str, ...]
    expected_lease: str
    full_ended_at: datetime
    observed_at: datetime


@dataclass(frozen=True)
class CheckResult:
    check_id: str
    ok: bool
    detail: Mapping[str, Any]


def evaluate_pre_publish_checks(
    facts: PrePublishFacts, *, max_hours_since_full_end: float = MAX_HOURS_SINCE_FULL_END
) -> tuple[CheckResult, ...]:
    hours = (facts.observed_at - facts.full_ended_at).total_seconds() / 3600
    return (
        CheckResult(
            "1_main_equals_expected_main",
            facts.main == facts.expected_main,
            {"main": facts.main, "expected": facts.expected_main},
        ),
        CheckResult(
            "2_orig_head_absent_or_not_main",
            facts.orig_head is None or facts.orig_head != facts.main,
            {"orig_head": facts.orig_head},
        ),
        CheckResult("3_no_stale_git_locks", not facts.git_locks, {"locks": list(facts.git_locks)}),
        CheckResult(
            "4_no_live_git_or_python_processes",
            not facts.live_processes,
            {"processes": list(facts.live_processes)},
        ),
        CheckResult(
            "5_real_lease_store_replay",
            facts.lease_replay_status == "PASS"
            and list(facts.active_leases) == [facts.expected_lease],
            {
                "status": facts.lease_replay_status,
                "active_leases": list(facts.active_leases),
                "expected_active": [facts.expected_lease],
            },
        ),
        CheckResult(
            "6_within_two_hours_of_full_end",
            0 <= hours <= max_hours_since_full_end,
            {"hours_since_full_end": round(hours, 2), "limit_hours": max_hours_since_full_end},
        ),
    )


def checks_summary(results: Sequence[CheckResult]) -> dict[str, Any]:
    return {
        "all_ok": all(result.ok for result in results),
        "checks": {result.check_id: {"ok": result.ok, **dict(result.detail)} for result in results},
    }


@dataclass(frozen=True)
class PublicationAuthorization:
    candidate_sha: str
    expected_main: str
    authorized_at: datetime
    authorized_by: str
    source: str


def load_authorization(
    path: Path,
    *,
    candidate_sha: str,
    expected_main: str,
    not_before: datetime,
    now: datetime | None = None,
) -> PublicationAuthorization:
    """Read and validate the owner authorization record for exactly this candidate.

    The record never grants more than an ordinary push of this one candidate: any scope other than
    ORDINARY_PUSH_THIS_CANDIDATE_ONLY, a force push or a pull request is refused, and a record older
    than the formal transaction (so given for some earlier candidate's run) is refused.
    """
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PublicationAuthorizationError(
            "PUBLICATION_AUTHORIZATION_UNREADABLE", str(path)
        ) from exc
    if not isinstance(raw, dict) or raw.get("schema_version") != AUTHORIZATION_SCHEMA_VERSION:
        raise PublicationAuthorizationError("PUBLICATION_AUTHORIZATION_SCHEMA", str(path))
    if raw.get("scope") != AUTHORIZATION_SCOPE:
        raise PublicationAuthorizationError(
            "PUBLICATION_AUTHORIZATION_SCOPE", str(raw.get("scope"))
        )
    if raw.get("force_push") is not False or raw.get("pull_request") is not False:
        raise PublicationAuthorizationError("PUBLICATION_AUTHORIZATION_BROAD", "force_push/pr")
    if raw.get("candidate_sha") != candidate_sha:
        raise PublicationAuthorizationError(
            "PUBLICATION_AUTHORIZATION_CANDIDATE", f"{raw.get('candidate_sha')} != {candidate_sha}"
        )
    if raw.get("expected_main") != expected_main:
        raise PublicationAuthorizationError(
            "PUBLICATION_AUTHORIZATION_BASE", f"{raw.get('expected_main')} != {expected_main}"
        )
    authorized_by = raw.get("authorized_by")
    source = raw.get("source")
    if not isinstance(authorized_by, str) or not authorized_by or not isinstance(source, str):
        raise PublicationAuthorizationError("PUBLICATION_AUTHORIZATION_PROVENANCE", str(path))
    try:
        authorized_at = datetime.fromisoformat(str(raw.get("authorized_at")))
    except ValueError as exc:
        raise PublicationAuthorizationError("PUBLICATION_AUTHORIZATION_TIME", str(path)) from exc
    if authorized_at.tzinfo is None:
        raise PublicationAuthorizationError("PUBLICATION_AUTHORIZATION_TIME", "naive timestamp")
    instant = (now or datetime.now(tz=UTC)).astimezone(UTC)
    if authorized_at > instant + timedelta(minutes=5):
        raise PublicationAuthorizationError("PUBLICATION_AUTHORIZATION_TIME", "from the future")
    if authorized_at < not_before:
        raise PublicationAuthorizationError(
            "PUBLICATION_AUTHORIZATION_STALE", "older than the formal transaction"
        )
    return PublicationAuthorization(
        candidate_sha=candidate_sha,
        expected_main=expected_main,
        authorized_at=authorized_at,
        authorized_by=authorized_by,
        source=source,
    )
