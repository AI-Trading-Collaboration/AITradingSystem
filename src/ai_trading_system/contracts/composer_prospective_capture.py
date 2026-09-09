"""Bounded Composer declarations; operator review is separate from execution proof."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any, ClassVar

from ai_trading_system.contracts.host_clock_evidence import HostClockEvidence, deadline_allows
from ai_trading_system.contracts.named_data_quality_execution import (
    COMPOSER_INPUT_POLICY_PATH,
    COMPOSER_INPUT_POLICY_SHA256,
    COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_PATH,
    COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_SHA256,
    COMPOSER_SCOPE_ORDER,
    NamedArtifactBinding,
    NamedDQExecutionRequest,
    NamedDQRoots,
    composer_dq_scope,
)
from ai_trading_system.contracts.prospective_capture_execution import (
    RecorderReturnObservation,
    _CaptureDTO,
)

CAPTURE_POLICY_PATH = "config/research/composer_prospective_capture_v1.yaml"
CAPTURE_POLICY_SHA256 = "5fecfd79417c200b7e9b4b8fd0a90baf56d52c3f37c1dc32214ea8660ad9bfa4"
TASK_ID = "TRADING-2560_FIRST_LAYER_COMPOSER_V2_PROSPECTIVE_OOS_OBSERVATION_V1"
TIMING_VERSION = "NEXT_XNYS_CLOSE_FORWARD_V1"
OPERATIONS = ("activate", "readiness", "capture")
CLOCK_STAGES = {
    "activate": ("pre_recorder", "witness_return", "ack_covered_through"),
    "capture": (
        "pre_dispatch",
        "dq_verified",
        "pre_input_recorder",
        "inputs_return",
        "fit_complete",
        "witness_return",
        "ack_covered_through",
    ),
    "readiness": ("pre_dispatch", "dq_verified"),
}


class ComposerCaptureError(ValueError):
    prospective_child_canonical_dq_call_count: int | None
    prospective_recorder_return_observations: list[dict[str, Any]]
    prospective_clock_failure_diagnostic: dict[str, Any] | None

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


def require(
    condition: bool, detail: str, code: str = "COMPOSER_CAPTURE_DECLARATION_INVALID"
) -> None:
    if not condition:
        raise ComposerCaptureError(code, detail)


def _fixed(value: object, expected: object, name: str) -> None:
    require(type(value) is type(expected) and value == expected, "fixed field: " + name)


@dataclass(frozen=True)
class ComposerCaptureManifest(_CaptureDTO):
    schema_version: ClassVar[str] = "composer_prospective_capture_manifest.v1"
    manifest_id: str
    roots: NamedDQRoots
    candidate_commit: str
    output_relative_path: str
    source_output_relative_path: str
    readiness_as_of: date
    allowed_feature_sessions: tuple[date, ...]
    expires_at: datetime
    evidence_purpose: str
    source_manifest_path: str = COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_PATH
    source_manifest_sha256: str = COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_SHA256
    policy_path: str = CAPTURE_POLICY_PATH
    input_policy_path: str = COMPOSER_INPUT_POLICY_PATH
    input_policy_sha256: str = COMPOSER_INPUT_POLICY_SHA256
    task_id: str = TASK_ID
    consumer_family: str = "COMPOSER"
    timing_version: str = TIMING_VERSION
    scope_order: tuple[str, ...] = COMPOSER_SCOPE_ORDER
    activation_attempt_maximum: int = 1
    readiness_attempt_maximum: int = 1
    attempts_per_feature_session: int = 1
    canonical_dq_calls_per_stage_maximum: int = 3
    canonical_dq_calls_per_manifest_maximum: int = 6
    parent_canonical_dq_call_maximum: int = 0
    future_observation_outcome_access_allowed: bool = False
    provider_calls_allowed: bool = False
    cache_mutation_allowed: bool = False
    orders: int = 0
    fills: int = 0
    positions: int = 0
    production_effect: str = "none"
    broker_action: str = "none"

    def __post_init__(self) -> None:
        self._check_types()
        require(
            re.fullmatch(r"[a-z][a-z0-9_]*_v[1-9][0-9]*", self.manifest_id) is not None,
            "versioned manifest id",
        )
        require(re.fullmatch(r"[0-9a-f]{40}", self.candidate_commit) is not None, "exact candidate")
        require(
            self.evidence_purpose in {"SYNTHETIC_ENGINEERING", "PROSPECTIVE_RESEARCH"},
            "explicit evidence role",
        )
        prefix = (
            "outputs/architecture/trading_2560_composer_known_snapshot/synthetic/"
            if self.evidence_purpose == "SYNTHETIC_ENGINEERING"
            else "outputs/research/composer_prospective/"
        )
        require(
            self.output_relative_path == prefix + self.manifest_id, "fixed purpose/id output root"
        )
        for path in (self.output_relative_path, self.source_output_relative_path):
            NamedArtifactBinding("EXECUTION", path, "0" * 64, 0)
        # The source locator describes the original publisher's identity (the
        # operational cache is data/raw); it grants no write to that location.
        require(self.output_relative_path.startswith("outputs/"), "contained outputs only")
        require(
            self.roots.evidence_root
            == self.roots.execution_root.rstrip("/") + "/" + self.output_relative_path + "/dq",
            "dedicated base DQ root",
        )
        path_type = (
            PureWindowsPath if PureWindowsPath(self.roots.execution_root).drive else PurePosixPath
        )
        output = path_type(self.roots.execution_root) / self.output_relative_path
        for protected in (
            path_type(self.roots.publication_root),
            path_type(self.roots.source_root) / self.source_output_relative_path,
        ):
            require(
                not output.is_relative_to(protected) and not protected.is_relative_to(output),
                "disjoint input/source-output and research-output roots",
            )
        require(
            len(self.allowed_feature_sessions) == 1,
            "one explicitly selected future feature session",
        )
        require(
            self.readiness_as_of > date(2025, 12, 2)
            and self.allowed_feature_sessions[0] > self.readiness_as_of,
            "readiness is not a backfilled observation",
        )
        require(
            self.expires_at.tzinfo is not None and self.expires_at.utcoffset() is not None,
            "aware expiry",
        )
        object.__setattr__(self, "expires_at", self.expires_at.astimezone(UTC))
        for name, expected in {
            "source_manifest_path": COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_PATH,
            "source_manifest_sha256": COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_SHA256,
            "policy_path": CAPTURE_POLICY_PATH,
            "input_policy_path": COMPOSER_INPUT_POLICY_PATH,
            "input_policy_sha256": COMPOSER_INPUT_POLICY_SHA256,
            "task_id": TASK_ID,
            "consumer_family": "COMPOSER",
            "timing_version": TIMING_VERSION,
            "scope_order": COMPOSER_SCOPE_ORDER,
            "activation_attempt_maximum": 1,
            "readiness_attempt_maximum": 1,
            "attempts_per_feature_session": 1,
            "canonical_dq_calls_per_stage_maximum": 3,
            "canonical_dq_calls_per_manifest_maximum": 6,
            "parent_canonical_dq_call_maximum": 0,
            "future_observation_outcome_access_allowed": False,
            "provider_calls_allowed": False,
            "cache_mutation_allowed": False,
            "orders": 0,
            "fills": 0,
            "positions": 0,
            "production_effect": "none",
            "broker_action": "none",
        }.items():
            _fixed(getattr(self, name), expected, name)


@dataclass(frozen=True)
class ComposerOwnerReview(_CaptureDTO):
    schema_version: ClassVar[str] = "composer_prospective_owner_review.v1"
    manifest_id: str
    manifest_sha256: str
    decision_ref: str
    reviewed_at: datetime
    authorization_state: str
    evidence_purpose: str
    actor: str = "Project Owner"
    status: str = "APPROVED_FOR_BOUNDED_COMPOSER_RESEARCH"
    risk_tier: str = "R1_BOUNDED_RESEARCH_SANDBOX"
    cryptographic_owner_signature_claimed: bool = False
    future_observation_outcome_access_allowed: bool = False

    def __post_init__(self) -> None:
        self._check_types()
        require(
            re.fullmatch(r"[0-9a-f]{64}", self.manifest_sha256) is not None, "exact manifest hash"
        )
        require(
            self.evidence_purpose in {"SYNTHETIC_ENGINEERING", "PROSPECTIVE_RESEARCH"},
            "explicit review role",
        )
        prefix = (
            "synthetic_review:TRADING-2560:"
            if self.evidence_purpose == "SYNTHETIC_ENGINEERING"
            else (
                "owner_instruction:TRADING-2560:2026-09-09:"
                "continue_known_snapshot_prospective_research"
            )
        )
        require(
            self.decision_ref.startswith(prefix),
            "current bounded owner scope, not historical tokens",
        )
        require(
            self.authorization_state in {"STANDING_OWNER_SCOPE", "EXACT_PREAUTHORIZED"},
            "pre-dispatch authority",
        )
        require(
            self.reviewed_at.tzinfo is not None and self.reviewed_at.utcoffset() is not None,
            "aware review time",
        )
        object.__setattr__(self, "reviewed_at", self.reviewed_at.astimezone(UTC))
        for name, expected in {
            "actor": "Project Owner",
            "status": "APPROVED_FOR_BOUNDED_COMPOSER_RESEARCH",
            "risk_tier": "R1_BOUNDED_RESEARCH_SANDBOX",
            "cryptographic_owner_signature_claimed": False,
            "future_observation_outcome_access_allowed": False,
        }.items():
            _fixed(getattr(self, name), expected, name)

    def assert_manifest(self, manifest: ComposerCaptureManifest) -> None:
        require(
            type(manifest) is ComposerCaptureManifest
            and self.manifest_id == manifest.manifest_id
            and self.manifest_sha256 == manifest.canonical_sha256
            and self.evidence_purpose == manifest.evidence_purpose
            and self.reviewed_at < manifest.expires_at,
            "review must bind exact manifest",
        )


@dataclass(frozen=True)
class ComposerCaptureRequest(_CaptureDTO):
    schema_version: ClassVar[str] = "composer_prospective_capture_request.v1"
    operation: str
    manifest: ComposerCaptureManifest
    owner_review: NamedArtifactBinding
    roots: NamedDQRoots
    candidate_commit: str
    source_manifest_path: str
    source_manifest_sha256: str
    policy_path: str
    feature_session: date | None
    named_dq_requests: tuple[NamedDQExecutionRequest, ...]

    def __post_init__(self) -> None:
        self._check_types()
        require(self.operation in OPERATIONS, "explicit Composer operation")
        for name in (
            "roots",
            "candidate_commit",
            "source_manifest_path",
            "source_manifest_sha256",
            "policy_path",
        ):
            require(getattr(self, name) == getattr(self.manifest, name), "manifest field: " + name)
        require(
            self.owner_review.root_role == "EXECUTION"
            and self.owner_review.relative_path
            == self.manifest.output_relative_path + "/control/owner_review.json"
            and self.owner_review.size_bytes > 0,
            "exact contained operator review",
        )
        if self.operation == "activate":
            require(
                self.feature_session is None and not self.named_dq_requests,
                "activation has no dummy snapshot",
            )
            return
        expected = (
            self.manifest.readiness_as_of
            if self.operation == "readiness"
            else self.manifest.allowed_feature_sessions[0]
        )
        require(
            self.feature_session == expected
            and len(self.named_dq_requests) == len(COMPOSER_SCOPE_ORDER),
            "fixed stage/session and three requests",
        )
        selectors = {item.selector for item in self.named_dq_requests}
        require(len(selectors) == 1, "three segments must bind one exact immutable snapshot")
        for segment, request in zip(COMPOSER_SCOPE_ORDER, self.named_dq_requests, strict=True):
            require(
                request.scope == composer_dq_scope(as_of=expected, segment=segment),
                "exact fixed DQ segment: " + segment,
            )
            for name in ("source_root", "publication_root", "execution_root"):
                require(
                    getattr(request.roots, name) == getattr(self.roots, name),
                    "same source and execution roots",
                )
            require(
                request.roots.evidence_root
                == self.roots.evidence_root + "/" + self.operation + "/" + segment,
                "isolated segment receipt root",
            )
            for name in (
                "candidate_commit",
                "source_manifest_path",
                "source_manifest_sha256",
                "source_output_relative_path",
            ):
                require(
                    getattr(request, name) == getattr(self.manifest, name),
                    "same DQ code/source identity: " + name,
                )
            require(
                request.policy_path == "config/data_quality.yaml",
                "unchanged reviewed canonical DQ policy",
            )

    @property
    def request_id(self) -> str:
        return "composer_capture_request_" + self.canonical_sha256

    @property
    def operation_relative_path(self) -> str:
        suffix = (
            "sessions/" + str(self.feature_session)
            if self.operation == "capture"
            else self.operation
        )
        return self.manifest.output_relative_path + "/" + suffix

    @property
    def timing_relative_path(self) -> str:
        return self.manifest.output_relative_path + "/timing"

    @property
    def required_write_paths(self) -> tuple[str, ...]:
        return (self.manifest.output_relative_path,)


def validate_composer_clock_prefix(
    clock: HostClockEvidence, returns: tuple[RecorderReturnObservation, ...], operation: str
) -> None:
    """Use the reviewed host-clock evidence; only Composer's stage order is new."""
    require(operation in CLOCK_STAGES, "clock operation")
    labels = tuple(row.label for row in clock.checkpoints)
    require(labels == CLOCK_STAGES[operation][: len(labels)], "exact original stage prefix")
    child_labels = tuple(label for label in labels if label in {"inputs_return", "witness_return"})
    require(
        len(returns) == len(child_labels) and len({row.event for row in returns}) == len(returns),
        "complete unique original child returns",
    )
    checkpoints = {row.label: row for row in clock.checkpoints}
    for label, returned in zip(child_labels, returns, strict=True):
        child, checkpoint = returned.clock_evidence, checkpoints[label]
        prior = clock.checkpoints[labels.index(label) - 1].sample
        require(
            child.provider == clock.provider
            and prior.utc_ns <= child.anchor.utc_ns
            and child.latest_sample.utc_ns <= checkpoint.sample.utc_ns
            and prior.counter_after_ns <= child.anchor.counter_before_ns
            and child.latest_sample.counter_after_ns <= checkpoint.sample.counter_before_ns
            and checkpoint.inherited_child_bound_ns == child.admission_bound_ns,
            "original child chronology and inherited conservative bound",
        )
    require(
        all(
            row.inherited_child_bound_ns is None
            for row in clock.checkpoints
            if row.label not in child_labels
        ),
        "no unverified foreign clock bounds",
    )


@dataclass(frozen=True)
class ComposerCompletionAcknowledgement(_CaptureDTO):
    schema_version: ClassVar[str] = "composer_prospective_completion_acknowledgement.v1"
    request_id: str
    request_sha256: str
    manifest_sha256: str
    operation: str
    feature_session: date | None
    candidate_commit: str
    execution_identity_sha256: str
    source_lease_id: str
    recorder_event: NamedArtifactBinding
    clock_evidence: HostClockEvidence
    recorder_returns: tuple[RecorderReturnObservation, ...]
    first_feature_session: date
    decision_effective_session: date | None
    decision_deadline: datetime | None
    authorization_state: str
    evidence_purpose: str
    technical_validation_state: str
    future_observation_outcome_access_allowed: bool = False
    historical_provider_available_at: str = "NOT_ESTABLISHED"
    acknowledgement_own_durability_time_claimed: bool = False
    production_effect: str = "none"
    broker_action: str = "none"

    def __post_init__(self) -> None:
        self._check_types()
        require(self.operation in {"activate", "capture"}, "recorded operation")
        for value in (self.request_sha256, self.manifest_sha256, self.execution_identity_sha256):
            require(re.fullmatch(r"[0-9a-f]{64}", value) is not None, "SHA256 identity")
        require(
            self.request_id == "composer_capture_request_" + self.request_sha256, "request identity"
        )
        require(
            re.fullmatch(r"[0-9a-f]{40}", self.candidate_commit) is not None, "exact code identity"
        )
        require(
            re.fullmatch(r"lease-[0-9a-f]{20}", self.source_lease_id) is not None,
            "original S4D lease",
        )
        validate_composer_clock_prefix(self.clock_evidence, self.recorder_returns, self.operation)
        require(
            tuple(row.label for row in self.clock_evidence.checkpoints)
            == CLOCK_STAGES[self.operation],
            "complete original parent clock",
        )
        require(self.recorder_returns[-1].event == self.recorder_event, "final recorder event")
        if self.operation == "activate":
            require(
                self.feature_session is None
                and self.decision_effective_session is None
                and self.decision_deadline is None
                and self.technical_validation_state == "ACTIVATION_ACKNOWLEDGED",
                "activation cannot claim a feature/return",
            )
        else:
            require(
                type(self.feature_session) is date
                and type(self.decision_effective_session) is date
                and type(self.decision_deadline) is datetime,
                "capture timing required",
            )
            assert (
                self.feature_session is not None
                and self.decision_effective_session is not None
                and self.decision_deadline is not None
            )
            require(
                self.feature_session >= self.first_feature_session
                and self.decision_effective_session > self.feature_session,
                "activation before feature/decision",
            )
            expected = (
                "CAPTURE_ACKNOWLEDGED"
                if deadline_allows(self.clock_evidence.admission_bound_ns, self.decision_deadline)
                else "LATE"
            )
            require(self.technical_validation_state == expected, "strict decision bound")
        require(
            self.authorization_state in {"STANDING_OWNER_SCOPE", "EXACT_PREAUTHORIZED"}
            and self.evidence_purpose in {"SYNTHETIC_ENGINEERING", "PROSPECTIVE_RESEARCH"},
            "separate authority/evidence role",
        )
        for name, expected_value in {
            "future_observation_outcome_access_allowed": False,
            "historical_provider_available_at": "NOT_ESTABLISHED",
            "acknowledgement_own_durability_time_claimed": False,
            "production_effect": "none",
            "broker_action": "none",
        }.items():
            _fixed(getattr(self, name), expected_value, name)
