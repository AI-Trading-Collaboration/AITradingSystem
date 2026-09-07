"""S3b bounded declarations; parsing a review is not an owner signature or a seal.

The trusted operator supplies the separately reviewed immutable control artifact.
The runtime must replay every binding, scope, clock and live execution condition.
No declaration authorizes outcome access, provider calls or trading.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import PurePosixPath, PureWindowsPath
from typing import ClassVar, Self, cast

from ai_trading_system.contracts.data_quality_execution import DataQualityDateWindow
from ai_trading_system.contracts.named_data_quality_execution import (
    EQUAL_RISK_GUARD_RATE_SERIES,
    EQUAL_RISK_PRICE_TICKERS,
    EQUAL_RISK_PRIMARY_START,
    PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
    PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
    NamedArtifactBinding,
    NamedDQExecutionRequest,
    NamedDQRoots,
    NamedDQScope,
    _NamedDTO,
)
from ai_trading_system.contracts.prospective_event_time_evidence import (
    canonical_json_bytes,
    strict_json_loads,
)

CAPTURE_POLICY_PATH = "config/research/prospective_capture_execution_v1.yaml"
CAPTURE_POLICY_SHA256 = "de5ab701d22401f258da47b3527cb659b8d3c175bf0adc8166afe06dfa82f1cf"
# Protocol names and one-attempt-per-key are execution safety invariants, not
# investment thresholds. See TRADING-2564_S3b_Prospective_Capture_Execution_V1.md.
CAPTURE_TIMING_VERSION = "NEXT_XNYS_CLOSE_FORWARD_V1"
CAPTURE_RETURN_CLOCK = "EFFECTIVE_SESSION_CLOSE_TO_NEXT_XNYS_SESSION_CLOSE"
CAPTURE_TASK_ID = "TRADING-2564_LONG_TERM_RESEARCH_CAPABILITY_IMPROVEMENT_V1"
_MANIFEST_ID = re.compile(r"[a-z][a-z0-9_-]*_v[1-9][0-9]*")
_SHA = re.compile(r"[0-9a-f]{64}")


class ProspectiveCaptureContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ProspectiveCaptureContractError("PROSPECTIVE_CAPTURE_DECLARATION_INVALID", message)


class _CaptureDTO(_NamedDTO):
    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        raw = strict_json_loads(content)
        _require(type(raw) is dict, "strict JSON object required")
        result = cls.from_dict(cast(dict[str, object], raw))
        _require(result.canonical_bytes == content, "canonical bytes required")
        return result


@dataclass(frozen=True)
class ProspectiveCaptureManifest(_CaptureDTO):
    """A finite, explicit capture scope; future snapshots are selected by exact key.

    Missing dates consume their place in the allowed-session window; they never
    create extra attempts or permission to shift the experiment's dates.
    """

    schema_version: ClassVar[str] = "prospective_capture_manifest.v1"
    manifest_id: str
    roots: NamedDQRoots
    candidate_commit: str
    source_output_relative_path: str
    output_relative_path: str
    allowed_feature_sessions: tuple[date, ...]
    expires_at: datetime
    evidence_purpose: str
    require_secondary_prices: bool
    source_manifest_path: str = PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH
    source_manifest_sha256: str = PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256
    policy_path: str = CAPTURE_POLICY_PATH
    consumer_family: str = "FIVE_CANDIDATE"
    timing_version: str = CAPTURE_TIMING_VERSION
    return_clock: str = CAPTURE_RETURN_CLOCK
    task_id: str = CAPTURE_TASK_ID
    snapshot_selection_rule: str = "EXPLICIT_IMMUTABLE_PER_SESSION_PINNED_REQUEST"
    activation_attempt_maximum: int = 1
    attempts_per_feature_session: int = 1
    canonical_dq_calls_per_capture_maximum: int = 1
    parent_canonical_dq_call_maximum: int = 0
    outcome_access_allowed: bool = False
    provider_calls_allowed: bool = False
    orders_allowed: bool = False
    fills_allowed: bool = False
    production_effect: str = "none"
    broker_action: str = "none"

    def __post_init__(self) -> None:
        self._check_types()
        _require(_MANIFEST_ID.fullmatch(self.manifest_id) is not None, "versioned manifest id")
        _require(re.fullmatch(r"[0-9a-f]{40}", self.candidate_commit) is not None, "exact commit")
        for path in (self.source_output_relative_path, self.output_relative_path):
            # Reuse the strict portable artifact locator validator.
            NamedArtifactBinding("EXECUTION", path, "0" * 64, 0)
        _require(self.source_output_relative_path.startswith("outputs/"), "explicit source outputs")
        prefix = (
            "outputs/architecture/trading_2564_s3b_prospective_capture/synthetic/"
            if self.evidence_purpose == "SYNTHETIC_ENGINEERING"
            else "outputs/research/prospective_capture/"
        )
        _require(
            self.evidence_purpose in {"SYNTHETIC_ENGINEERING", "PROSPECTIVE_RESEARCH"}
            and self.output_relative_path == prefix + self.manifest_id,
            "dedicated output root must match the evidence purpose and manifest id",
        )
        _require(
            self.roots.evidence_root
            == self.roots.execution_root.rstrip("/") + "/" + self.output_relative_path + "/dq",
            "DQ evidence must be the run's contained dedicated execution subdirectory",
        )
        path_type = (
            PureWindowsPath if PureWindowsPath(self.roots.execution_root).drive else PurePosixPath
        )
        output = path_type(self.roots.execution_root) / self.output_relative_path
        publication = path_type(self.roots.publication_root)
        _require(
            not output.is_relative_to(publication) and not publication.is_relative_to(output),
            "publication input and the complete mutable output root must be disjoint",
        )
        original_output = path_type(self.roots.source_root) / self.source_output_relative_path
        _require(
            not output.is_relative_to(original_output)
            and not original_output.is_relative_to(output),
            "original source output and capture output must be disjoint",
        )
        _require(
            bool(self.allowed_feature_sessions)
            and tuple(sorted(set(self.allowed_feature_sessions))) == self.allowed_feature_sessions
            and self.allowed_feature_sessions[0] >= EQUAL_RISK_PRIMARY_START,
            "nonempty explicit sorted unique feature sessions in the primary research window",
        )
        _require(
            self.expires_at.tzinfo is not None and self.expires_at.utcoffset() is not None,
            "aware expiration required",
        )
        object.__setattr__(self, "expires_at", self.expires_at.astimezone(UTC))
        expected = {
            "source_manifest_path": PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
            "source_manifest_sha256": PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
            "policy_path": CAPTURE_POLICY_PATH,
            "consumer_family": "FIVE_CANDIDATE",
            "timing_version": CAPTURE_TIMING_VERSION,
            "return_clock": CAPTURE_RETURN_CLOCK,
            "task_id": CAPTURE_TASK_ID,
            "snapshot_selection_rule": "EXPLICIT_IMMUTABLE_PER_SESSION_PINNED_REQUEST",
            "activation_attempt_maximum": 1,
            "attempts_per_feature_session": 1,
            "canonical_dq_calls_per_capture_maximum": 1,
            "parent_canonical_dq_call_maximum": 0,
            "outcome_access_allowed": False,
            "provider_calls_allowed": False,
            "orders_allowed": False,
            "fills_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        }
        for key, value in expected.items():
            _require(
                type(getattr(self, key)) is type(value) and getattr(self, key) == value,
                "fixed protocol/safety field: " + key,
            )

    def dq_scope(self, feature_session: date) -> NamedDQScope:
        _require(
            type(feature_session) is date and feature_session in self.allowed_feature_sessions,
            "feature session outside bounded allowlist",
        )
        return NamedDQScope(
            as_of=feature_session,
            requested_window=DataQualityDateWindow(EQUAL_RISK_PRIMARY_START, feature_session),
            expected_price_tickers=EQUAL_RISK_PRICE_TICKERS,
            expected_rate_series=EQUAL_RISK_GUARD_RATE_SERIES,
            input_roles=(
                ("prices", "rates", "secondary_prices")
                if self.require_secondary_prices
                else ("prices", "rates")
            ),
            require_secondary_prices=self.require_secondary_prices,
        )


@dataclass(frozen=True)
class ProspectiveCaptureOwnerReview(_CaptureDTO):
    """Trusted operator control file; no cryptographic owner-signature claim."""

    schema_version: ClassVar[str] = "prospective_capture_owner_review.v1"
    manifest_id: str
    manifest_sha256: str
    decision_ref: str
    reviewed_at: datetime
    authorization_state: str
    evidence_purpose: str
    actor: str = "Project Owner"
    status: str = "APPROVED_FOR_BOUNDED_CAPTURE"
    reviewed_timing_version: str = CAPTURE_TIMING_VERSION
    complete_witness_before_deadline_required: bool = True
    host_clock_trust_acknowledged: bool = True
    outcome_access_authorized: bool = False
    risk_tier: str = "R1_BOUNDED_RESEARCH_SANDBOX"
    cryptographic_owner_signature_claimed: bool = False

    def __post_init__(self) -> None:
        self._check_types()
        _require(_MANIFEST_ID.fullmatch(self.manifest_id) is not None, "versioned manifest id")
        _require(_SHA.fullmatch(self.manifest_sha256) is not None, "manifest hash")
        _require(
            self.evidence_purpose in {"SYNTHETIC_ENGINEERING", "PROSPECTIVE_RESEARCH"},
            "explicit evidence purpose",
        )
        prefix = (
            "synthetic_review:TRADING-2564:"
            if self.evidence_purpose == "SYNTHETIC_ENGINEERING"
            else "owner_decision:TRADING-2564:"
        )
        _require(
            self.decision_ref.startswith(prefix) and len(self.decision_ref) > len(prefix),
            "new task-bound review required; historical one-shot permission is not reusable",
        )
        _require(
            self.authorization_state in {"EXACT_PREAUTHORIZED", "STANDING_OWNER_SCOPE"},
            "prospective dispatch requires authorization before execution",
        )
        _require(
            self.reviewed_at.tzinfo is not None and self.reviewed_at.utcoffset() is not None,
            "aware review time required",
        )
        object.__setattr__(self, "reviewed_at", self.reviewed_at.astimezone(UTC))
        for key, expected in {
            "actor": "Project Owner",
            "status": "APPROVED_FOR_BOUNDED_CAPTURE",
            "reviewed_timing_version": CAPTURE_TIMING_VERSION,
            "complete_witness_before_deadline_required": True,
            "host_clock_trust_acknowledged": True,
            "outcome_access_authorized": False,
            "risk_tier": "R1_BOUNDED_RESEARCH_SANDBOX",
            "cryptographic_owner_signature_claimed": False,
        }.items():
            _require(
                type(getattr(self, key)) is type(expected) and getattr(self, key) == expected,
                "fixed review field: " + key,
            )

    def assert_manifest(self, manifest: ProspectiveCaptureManifest) -> None:
        _require(
            type(manifest) is ProspectiveCaptureManifest
            and self.manifest_id == manifest.manifest_id
            and self.manifest_sha256 == manifest.canonical_sha256
            and self.evidence_purpose == manifest.evidence_purpose
            and self.reviewed_at < manifest.expires_at,
            "owner review does not bind the exact manifest",
        )


@dataclass(frozen=True)
class ProspectiveCaptureRequest(_CaptureDTO):
    """Bootstrap envelope; activation has no invented snapshot, DQ request or F."""

    schema_version: ClassVar[str] = "prospective_capture_request.v1"
    operation: str
    manifest: ProspectiveCaptureManifest
    owner_review: NamedArtifactBinding
    roots: NamedDQRoots
    candidate_commit: str
    source_manifest_path: str
    source_manifest_sha256: str
    policy_path: str
    feature_session: date | None
    named_dq_request: NamedDQExecutionRequest | None

    def __post_init__(self) -> None:
        self._check_types()
        _require(self.operation in {"activate", "capture"}, "explicit parent operation required")
        for field in (
            "roots",
            "candidate_commit",
            "source_manifest_path",
            "source_manifest_sha256",
            "policy_path",
        ):
            _require(
                getattr(self, field) == getattr(self.manifest, field), "manifest field: " + field
            )
        _require(self.owner_review.root_role == "EXECUTION", "contained owner control artifact")
        _require(
            self.owner_review.relative_path
            == self.manifest.output_relative_path + "/control/owner_review.json",
            "review is the exact external operator control file; the parent never creates it",
        )
        if self.operation == "activate":
            _require(
                self.feature_session is None and self.named_dq_request is None,
                "activation cannot carry a historical feature or dummy DQ scope",
            )
        else:
            _require(
                type(self.feature_session) is date
                and type(self.named_dq_request) is NamedDQExecutionRequest,
                "capture requires exact feature and named request",
            )
            assert self.feature_session is not None and self.named_dq_request is not None
            request = self.named_dq_request
            _require(
                request.scope == self.manifest.dq_scope(self.feature_session),
                "capture DQ scope differs from the fixed full-window consumer",
            )
            for field in (
                "roots",
                "candidate_commit",
                "source_manifest_path",
                "source_manifest_sha256",
                "source_output_relative_path",
            ):
                _require(
                    getattr(request, field) == getattr(self.manifest, field),
                    "named request changes manifest field: " + field,
                )
            _require(request.policy_path == "config/data_quality.yaml", "canonical DQ policy")

    @property
    def request_id(self) -> str:
        return "prospective_capture_request_" + self.canonical_sha256

    @property
    def operation_relative_path(self) -> str:
        suffix = (
            "activation"
            if self.feature_session is None
            else "sessions/" + self.feature_session.isoformat()
        )
        return self.manifest.output_relative_path + "/" + suffix

    @property
    def timing_relative_path(self) -> str:
        return self.manifest.output_relative_path + "/timing"

    @property
    def required_write_paths(self) -> tuple[str, ...]:
        # All actual writes live below this single task-identifiable prefix.
        return (self.manifest.output_relative_path,)


@dataclass(frozen=True)
class ParentCompletionAcknowledgement(_CaptureDTO):
    """Original local completion observation; its own earlier durability is not claimed."""

    schema_version: ClassVar[str] = "prospective_parent_completion_acknowledgement.v1"
    request_id: str
    request_sha256: str
    manifest_id: str
    manifest_sha256: str
    operation: str
    feature_session: date | None
    candidate_commit: str
    execution_identity_sha256: str
    parent_pid: int
    source_lease_id: str
    recorder_event: NamedArtifactBinding
    recording_call_started_at: datetime
    witness_bundle_observed_at: datetime
    monotonic_elapsed_ns: int
    first_feature_session: date
    decision_effective_session: date | None
    decision_deadline: datetime | None
    authorization_state: str
    evidence_purpose: str
    technical_validation_state: str
    source_execution_attested: bool = True
    parent_canonical_dq_call_count: int = 0
    acknowledgement_own_durability_time_claimed: bool = False
    outcome_access_authorized: bool = False
    provider_available_at_status: str = "NOT_ESTABLISHED"
    production_effect: str = "none"
    broker_action: str = "none"

    def __post_init__(self) -> None:
        self._check_types()
        for digest in (self.request_sha256, self.manifest_sha256, self.execution_identity_sha256):
            _require(_SHA.fullmatch(digest) is not None, "exact SHA-256 identity")
        _require(
            self.request_id == "prospective_capture_request_" + self.request_sha256,
            "request id/hash correlation",
        )
        _require(_MANIFEST_ID.fullmatch(self.manifest_id) is not None, "versioned manifest id")
        _require(
            re.fullmatch(r"[0-9a-f]{40}", self.candidate_commit) is not None, "exact source commit"
        )
        _require(
            self.parent_pid > 0 and self.monotonic_elapsed_ns >= 0,
            "positive PID/nonnegative duration",
        )
        _require(
            re.fullmatch(r"lease-[0-9a-f]{20}", self.source_lease_id) is not None, "S4D lease id"
        )
        _require(self.recorder_event.root_role == "EXECUTION", "event in source execution root")
        for instant in (self.recording_call_started_at, self.witness_bundle_observed_at):
            _require(instant.tzinfo is not None and instant.utcoffset() is not None, "aware clock")
        elapsed = self.witness_bundle_observed_at - self.recording_call_started_at
        elapsed_ns = (
            elapsed.days * 86400 + elapsed.seconds
        ) * 1_000_000_000 + elapsed.microseconds * 1000
        # One microsecond is datetime representation precision, not a tolerance
        # for a tunable host clock or an investment decision threshold.
        _require(
            elapsed_ns >= 0 and elapsed_ns + 1000 >= self.monotonic_elapsed_ns,
            "UTC must enclose the inner monotonic interval",
        )
        if self.operation == "activate":
            _require(
                self.feature_session is None
                and self.decision_effective_session is None
                and self.decision_deadline is None
                and self.technical_validation_state == "ACTIVATION_ACKNOWLEDGED",
                "activation has no feature/DQ/return interval",
            )
        else:
            _require(
                self.operation == "capture"
                and type(self.feature_session) is date
                and type(self.decision_effective_session) is date
                and type(self.decision_deadline) is datetime,
                "capture time fields required",
            )
            assert self.feature_session is not None and self.decision_effective_session is not None
            assert self.decision_deadline is not None
            _require(
                self.feature_session >= self.first_feature_session
                and self.decision_effective_session > self.feature_session,
                "capture cannot precede complete activation",
            )
            expected = (
                "CAPTURE_ACKNOWLEDGED"
                if self.witness_bundle_observed_at < self.decision_deadline
                else "LATE"
            )
            _require(self.technical_validation_state == expected, "strict deadline classification")
        _require(
            self.authorization_state in {"EXACT_PREAUTHORIZED", "STANDING_OWNER_SCOPE"}
            and self.evidence_purpose in {"SYNTHETIC_ENGINEERING", "PROSPECTIVE_RESEARCH"},
            "separate authorization and evidence purpose",
        )
        for field, value in {
            "source_execution_attested": True,
            "parent_canonical_dq_call_count": 0,
            "acknowledgement_own_durability_time_claimed": False,
            "outcome_access_authorized": False,
            "provider_available_at_status": "NOT_ESTABLISHED",
            "production_effect": "none",
            "broker_action": "none",
        }.items():
            _require(
                type(getattr(self, field)) is type(value) and getattr(self, field) == value,
                "fixed acknowledgement boundary: " + field,
            )
