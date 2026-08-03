from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, ClassVar, Literal, Self

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_POLICY_PATH = Path(
    "config/research/qqq_options_cross_layer_validation_harness_v1.yaml"
)
DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_GOLDEN_PATH = Path(
    "config/research/qqq_options_cross_layer_validation_golden_v1.yaml"
)

_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_NOT_GRANTED_TOKEN = "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
_REQUIRED_PILOT_TASK = "TRADING-2492_QC_QQQ_OPTIONS_BOUNDED_FREE_CLOUD_PILOT_V1"

QQQOptionsScenarioId = Literal[
    "CORPORATE_ACTION_SCOPE_INVALID",
    "CROSSED_QUOTE_INVALID",
    "INSUFFICIENT_SETTLED_CASH",
    "ITM_EXPIRY_SCOPE_INVALID",
    "MISSING_QUOTE_INVALID",
    "NO_ELIGIBLE_CONTRACT_CASH",
    "PARTIAL_FILL_CANCELED",
    "STALE_QUOTE_REJECTED",
    "VALID_CROSS_LAYER_SYNTHETIC",
    "VENUE_REJECTED_CASH",
]
QQQOptionsStimulusClass = Literal[
    "CORPORATE_ACTION",
    "CROSSED_QUOTE",
    "INSUFFICIENT_CASH",
    "ITM_EXPIRY",
    "MISSING_QUOTE",
    "NO_CONTRACT",
    "PARTIAL_FILL",
    "STALE_QUOTE",
    "VALID",
    "VENUE_REJECT",
]
QQQOptionsTerminalLayer = Literal[
    "INPUT", "SELECTION", "EXECUTION", "ACCOUNTING", "LIFECYCLE", "RECONCILIATION"
]
QQQOptionsExpectedStatus = Literal[
    "BLOCKED", "INVALID", "PARTIAL", "READY_FOR_OWNER_REVIEW", "REJECTED"
]
QQQOptionsDQStatus = Literal["PASS", "FAIL", "NOT_EVALUATED"]
QQQOptionsArtifactRole = Literal[
    "ACCOUNTING_RESULT",
    "EXECUTION_RESULT",
    "LIFECYCLE_RESULT",
    "RECONCILIATION_RESULT",
    "SELECTION_INPUT_VALIDATION",
    "SELECTION_RESULT",
]

_SCENARIO_IDS: tuple[str, ...] = (
    "CORPORATE_ACTION_SCOPE_INVALID",
    "CROSSED_QUOTE_INVALID",
    "INSUFFICIENT_SETTLED_CASH",
    "ITM_EXPIRY_SCOPE_INVALID",
    "MISSING_QUOTE_INVALID",
    "NO_ELIGIBLE_CONTRACT_CASH",
    "PARTIAL_FILL_CANCELED",
    "STALE_QUOTE_REJECTED",
    "VALID_CROSS_LAYER_SYNTHETIC",
    "VENUE_REJECTED_CASH",
)
_AUTHORITY_IDS: tuple[str, ...] = tuple(
    f"TRADING-{task}-{kind}"
    for task in range(2481, 2491)
    for kind in ("MODULE", "POLICY")
)
_CHECKLIST_ITEM_IDS: tuple[str, ...] = (
    "CAPABILITY_RECEIPT_VERIFIED",
    "DQ_PIT_REVIEWED",
    "ENGINE_SUBSCRIPTION_BOUND",
    "EXACT_CODE_PROJECT_IDENTITY",
    "LOCAL_RECONCILIATION_EXPLAINED",
    "NO_RAW_EXPORT_CONFIRMED",
    "OWNER_TOKEN_GRANTED",
    "PRIMARY_WINDOW_BOUND",
    "RESOURCE_BOUNDARY_REVIEWED",
    "RESULT_MAPPING_COMPLETE",
    "STAGE_GATE_HANDOFF_READY",
    "TWO_PERSON_ATTESTATION_COMPLETE",
)


class QQQOptionsCrossLayerValidationError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _required_text(value: str, field_name: str) -> str:
    if not value or value.strip() != value or any(ord(char) < 32 for char in value):
        raise ValueError(f"{field_name} must be normalized non-empty text")
    return value


def _identifier(value: str, field_name: str) -> str:
    value = _required_text(value, field_name)
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be a portable identifier")
    return value


def _sha256(value: str, field_name: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be lowercase SHA-256")
    return value


def _utc(value: datetime, field_name: str) -> datetime:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    normalized = value.astimezone(UTC)
    if normalized > datetime.now(UTC):
        raise ValueError(f"{field_name} cannot be in the future")
    return normalized


def _canonical_json_bytes(payload: object) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _canonical_sha256(payload: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def _lf_sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


class _SealedModel(_StrictModel):
    content_sha256: str
    _HASH_FIELD: ClassVar[str] = "content_sha256"

    @field_validator("content_sha256")
    @classmethod
    def _validate_content_hash(cls, value: str) -> str:
        return _sha256(value, "content_sha256")

    def _semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={self._HASH_FIELD})

    def _expected_content_sha256(self) -> str:
        return _canonical_sha256(self._semantic_payload())

    @model_validator(mode="after")
    def _validate_seal(self) -> Self:
        if self.content_sha256 not in {_UNSEALED_SHA256, self._expected_content_sha256()}:
            raise ValueError("content_sha256 does not match canonical semantic payload")
        return self

    @classmethod
    def seal(cls, **payload: object) -> Self:
        candidate = cls.model_validate(
            {**payload, cls._HASH_FIELD: _UNSEALED_SHA256},
            strict=True,
        )
        return candidate.model_copy(
            update={cls._HASH_FIELD: candidate._expected_content_sha256()}
        )

    def canonical_bytes(self) -> bytes:
        if self.content_sha256 == _UNSEALED_SHA256:
            raise QQQOptionsCrossLayerValidationError(
                "QQQ_OPTIONS_VALIDATION_RECORD_UNSEALED",
                self.__class__.__name__,
            )
        return _canonical_json_bytes(self.model_dump(mode="json"))

    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            value = cls.model_validate_json(content, strict=True)
        except ValueError as exc:
            raise QQQOptionsCrossLayerValidationError(
                "QQQ_OPTIONS_VALIDATION_RECORD_INVALID",
                f"{cls.__name__}: {exc}",
            ) from exc
        if value.content_sha256 == _UNSEALED_SHA256:
            raise QQQOptionsCrossLayerValidationError(
                "QQQ_OPTIONS_VALIDATION_RECORD_UNSEALED",
                cls.__name__,
            )
        if value.canonical_bytes() != content:
            raise QQQOptionsCrossLayerValidationError(
                "QQQ_OPTIONS_VALIDATION_RECORD_NONCANONICAL",
                cls.__name__,
            )
        return value


class QQQOptionsAuthorityBinding(_PolicyModel):
    authority_id: str
    path: str
    sha256: str

    @field_validator("authority_id")
    @classmethod
    def _validate_authority_id(cls, value: str) -> str:
        return _identifier(value, "authority_id")

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: str) -> str:
        value = _required_text(value, "path")
        path = Path(value)
        if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
            raise ValueError("authority path must be a portable repository-relative path")
        return value

    @field_validator("sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _sha256(value, "sha256")


class QQQOptionsRequiredArtifactContract(_PolicyModel):
    artifact_role: QQQOptionsArtifactRole
    contract_id: str

    @field_validator("contract_id")
    @classmethod
    def _validate_contract_id(cls, value: str) -> str:
        return _identifier(value, "contract_id")


class QQQOptionsCrossLayerScenarioSpec(_PolicyModel):
    scenario_id: QQQOptionsScenarioId
    stimulus_class: QQQOptionsStimulusClass
    terminal_layer: QQQOptionsTerminalLayer
    expected_status: QQQOptionsExpectedStatus
    expected_reason_codes: tuple[str, ...]
    expected_order_count: int
    expected_fill_count: int
    cash_preservation_required: bool
    run_valid: bool
    expected_dq_status: QQQOptionsDQStatus
    expected_pit_status: QQQOptionsDQStatus
    required_artifacts: tuple[QQQOptionsRequiredArtifactContract, ...]
    evidence_classification: Literal["SYNTHETIC_TEST_ONLY_NOT_PLATFORM_EVIDENCE"]
    caveat: str

    @field_validator("expected_reason_codes")
    @classmethod
    def _validate_reasons(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        checked = tuple(_identifier(item, "expected_reason_codes") for item in value)
        if not checked or checked != tuple(sorted(checked)) or len(checked) != len(set(checked)):
            raise ValueError("expected reason codes must be non-empty, sorted, and unique")
        return checked

    @field_validator("expected_order_count", "expected_fill_count")
    @classmethod
    def _validate_counts(cls, value: int) -> int:
        if isinstance(value, bool) or value < 0:
            raise ValueError("fixture counts must be non-negative integers")
        return value

    @field_validator("caveat")
    @classmethod
    def _validate_caveat(cls, value: str) -> str:
        return _required_text(value, "caveat")

    @model_validator(mode="after")
    def _validate_artifacts_and_safety(self) -> Self:
        roles = tuple(item.artifact_role for item in self.required_artifacts)
        if not roles or roles != tuple(sorted(roles)) or len(roles) != len(set(roles)):
            raise ValueError("required artifact roles must be non-empty, sorted, and unique")
        if self.expected_fill_count > self.expected_order_count:
            raise ValueError("fill count cannot exceed order count")
        if self.expected_status == "READY_FOR_OWNER_REVIEW" and self.scenario_id != (
            "VALID_CROSS_LAYER_SYNTHETIC"
        ):
            raise ValueError("only the valid synthetic scenario may be ready for owner review")
        return self

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @property
    def fixture_sha256(self) -> str:
        return _canonical_sha256(self.semantic_payload())


class QQQOptionsCrossLayerHarnessSafety(_PolicyModel):
    research_only: Literal[True]
    synthetic_fixture_is_platform_evidence: Literal[False]
    synthetic_pass_may_authorize_pilot: Literal[False]
    investment_interpretation_allowed: Literal[False]
    pilot_authorized: Literal[False]
    range_expansion_allowed: Literal[False]
    external_platform_action_allowed: Literal[False]
    cloud_run_authorized: Literal[False]
    api_allowed: Literal[False]
    cli_allowed: Literal[False]
    remote_http_allowed: Literal[False]
    object_store_allowed: Literal[False]
    raw_options_data_export_allowed: Literal[False]
    paper_shadow_allowed: Literal[False]
    production_allowed: Literal[False]
    promotion_allowed: Literal[False]
    broker_action_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QQQOptionsCrossLayerHarnessPolicy(_PolicyModel):
    schema_version: Literal["qqq_options_cross_layer_validation_harness_policy.v1"]
    policy_id: Literal["qqq_options_cross_layer_validation_harness_v1"]
    policy_version: Literal["1.0.0"]
    status: Literal["VALIDATION_BASELINE"]
    owner: str
    owner_decision: str
    rationale: str
    intended_effect: str
    validation_plan: str
    review_condition: str
    expiry_condition: str
    primary_research_start: date
    legacy_non_default_start: date
    legacy_non_default_start_is_default: Literal[False]
    authority_bindings: tuple[QQQOptionsAuthorityBinding, ...]
    scenarios: tuple[QQQOptionsCrossLayerScenarioSpec, ...]
    cloud_checklist_item_ids: tuple[str, ...]
    safety: QQQOptionsCrossLayerHarnessSafety
    decision: Literal["QQQ_OPTIONS_VALIDATION_HARNESS_READY"]

    @field_validator(
        "owner",
        "owner_decision",
        "rationale",
        "intended_effect",
        "validation_plan",
        "review_condition",
        "expiry_condition",
    )
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("cloud_checklist_item_ids")
    @classmethod
    def _validate_checklist_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        checked = tuple(_identifier(item, "cloud_checklist_item_ids") for item in value)
        if checked != _CHECKLIST_ITEM_IDS:
            raise ValueError("cloud checklist inventory must remain complete, sorted, and exact")
        return checked

    @model_validator(mode="after")
    def _validate_policy(self) -> Self:
        if self.primary_research_start != date(2021, 2, 22):
            raise ValueError("primary research start must remain 2021-02-22")
        if self.legacy_non_default_start != date(2022, 12, 1):
            raise ValueError("legacy non-default marker drifted")
        authority_ids = tuple(item.authority_id for item in self.authority_bindings)
        if authority_ids != _AUTHORITY_IDS:
            raise ValueError("authority inventory must cover TRADING-2481 through TRADING-2490")
        scenario_ids = tuple(item.scenario_id for item in self.scenarios)
        if scenario_ids != _SCENARIO_IDS:
            raise ValueError("scenario inventory must remain complete, sorted, and exact")
        return self


@dataclass(frozen=True)
class QQQOptionsCrossLayerHarnessPolicyLoadResult:
    policy: QQQOptionsCrossLayerHarnessPolicy
    policy_path: Path
    policy_sha256: str


class QQQOptionsScenarioGolden(_PolicyModel):
    scenario_id: QQQOptionsScenarioId
    fixture_sha256: str

    @field_validator("fixture_sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _sha256(value, "fixture_sha256")


class QQQOptionsCrossLayerGoldenManifest(_PolicyModel):
    schema_version: Literal["qqq_options_cross_layer_validation_golden.v1"]
    policy_sha256: str
    scenario_goldens: tuple[QQQOptionsScenarioGolden, ...]
    corpus_sha256: str
    historical_golden_rewrite_allowed: Literal[False]
    synthetic_fixture_is_platform_evidence: Literal[False]

    @field_validator("policy_sha256", "corpus_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_inventory(self) -> Self:
        ids = tuple(item.scenario_id for item in self.scenario_goldens)
        if ids != _SCENARIO_IDS:
            raise ValueError("golden scenario inventory must remain complete, sorted, and exact")
        return self


@dataclass(frozen=True)
class LoadedQQQOptionsCrossLayerHarness:
    policy: QQQOptionsCrossLayerHarnessPolicy
    policy_path: Path
    policy_sha256: str
    golden: QQQOptionsCrossLayerGoldenManifest
    golden_path: Path
    golden_sha256: str


class QQQOptionsCrossLayerArtifactBinding(_StrictModel):
    artifact_role: QQQOptionsArtifactRole
    contract_id: str
    artifact_sha256: str

    @field_validator("contract_id")
    @classmethod
    def _validate_contract_id(cls, value: str) -> str:
        return _identifier(value, "contract_id")

    @field_validator("artifact_sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _sha256(value, "artifact_sha256")


class QQQOptionsCrossLayerObservation(_SealedModel):
    schema_version: Literal["qqq_options_cross_layer_observation.v1"]
    observation_id: str
    scenario_id: QQQOptionsScenarioId
    observed_at_utc: datetime
    terminal_layer: QQQOptionsTerminalLayer
    observed_status: QQQOptionsExpectedStatus
    reason_codes: tuple[str, ...]
    order_count: int
    fill_count: int
    cash_preservation_required: bool
    run_valid: bool
    dq_status: QQQOptionsDQStatus
    pit_status: QQQOptionsDQStatus
    artifact_bindings: tuple[QQQOptionsCrossLayerArtifactBinding, ...]
    evidence_classification: Literal["SYNTHETIC_TEST_ONLY_NOT_PLATFORM_EVIDENCE"]
    synthetic_fixture_is_platform_evidence: Literal[False]
    investment_interpretation_allowed: Literal[False]
    pilot_authorized: Literal[False]
    range_expansion_allowed: Literal[False]

    @field_validator("observation_id")
    @classmethod
    def _validate_observation_id(cls, value: str) -> str:
        return _identifier(value, "observation_id")

    @field_validator("observed_at_utc")
    @classmethod
    def _validate_observed_at(cls, value: datetime) -> datetime:
        return _utc(value, "observed_at_utc")

    @field_validator("reason_codes")
    @classmethod
    def _validate_reasons(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        checked = tuple(_identifier(item, "reason_codes") for item in value)
        if not checked or checked != tuple(sorted(checked)) or len(checked) != len(set(checked)):
            raise ValueError("reason codes must be non-empty, sorted, and unique")
        return checked

    @field_validator("order_count", "fill_count")
    @classmethod
    def _validate_counts(cls, value: int) -> int:
        if isinstance(value, bool) or value < 0:
            raise ValueError("observation counts must be non-negative integers")
        return value

    @model_validator(mode="after")
    def _validate_observation(self) -> Self:
        roles = tuple(item.artifact_role for item in self.artifact_bindings)
        if not roles or roles != tuple(sorted(roles)) or len(roles) != len(set(roles)):
            raise ValueError("artifact bindings must be non-empty, sorted, and unique by role")
        if self.fill_count > self.order_count:
            raise ValueError("fill count cannot exceed order count")
        return self


class QQQOptionsScenarioValidation(_SealedModel):
    schema_version: Literal["qqq_options_cross_layer_scenario_validation.v1"]
    scenario_id: QQQOptionsScenarioId
    fixture_sha256: str
    observation_sha256: str
    status: Literal["PASS", "FAIL"]
    mismatch_codes: tuple[str, ...]
    platform_evidence_status: Literal["NOT_EVALUATED_NO_AUTHORIZED_PILOT"]
    pilot_authorized: Literal[False]

    @field_validator("fixture_sha256", "observation_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("mismatch_codes")
    @classmethod
    def _validate_mismatches(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        checked = tuple(_identifier(item, "mismatch_codes") for item in value)
        if checked != tuple(sorted(checked)) or len(checked) != len(set(checked)):
            raise ValueError("mismatch codes must be sorted and unique")
        return checked

    @model_validator(mode="after")
    def _validate_status(self) -> Self:
        if self.status == "PASS" and self.mismatch_codes:
            raise ValueError("PASS validation cannot contain mismatches")
        if self.status == "FAIL" and not self.mismatch_codes:
            raise ValueError("FAIL validation requires at least one mismatch")
        return self


class QQQOptionsCrossLayerValidationReport(_SealedModel):
    schema_version: Literal["qqq_options_cross_layer_validation_report.v1"]
    report_id: str
    built_at_utc: datetime
    policy_sha256: str
    golden_sha256: str
    corpus_sha256: str
    scenario_validations: tuple[QQQOptionsScenarioValidation, ...]
    missing_scenario_ids: tuple[QQQOptionsScenarioId, ...]
    status: Literal["PASS", "FAIL"]
    synthetic_fixture_coverage_status: Literal["PASS", "FAIL"]
    platform_evidence_status: Literal["NOT_EVALUATED_NO_AUTHORIZED_PILOT"]
    synthetic_fixture_is_platform_evidence: Literal[False]
    investment_interpretation_allowed: Literal[False]
    pilot_authorized: Literal[False]
    range_expansion_allowed: Literal[False]
    safety: QQQOptionsCrossLayerHarnessSafety

    @field_validator("report_id")
    @classmethod
    def _validate_report_id(cls, value: str) -> str:
        return _identifier(value, "report_id")

    @field_validator("built_at_utc")
    @classmethod
    def _validate_built_at(cls, value: datetime) -> datetime:
        return _utc(value, "built_at_utc")

    @field_validator("policy_sha256", "golden_sha256", "corpus_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("missing_scenario_ids")
    @classmethod
    def _validate_missing(
        cls, value: tuple[QQQOptionsScenarioId, ...]
    ) -> tuple[QQQOptionsScenarioId, ...]:
        if value != tuple(sorted(value)) or len(value) != len(set(value)):
            raise ValueError("missing scenario ids must be sorted and unique")
        return value

    @model_validator(mode="after")
    def _validate_report(self) -> Self:
        ids = tuple(item.scenario_id for item in self.scenario_validations)
        if ids != tuple(sorted(ids)) or len(ids) != len(set(ids)):
            raise ValueError("scenario validations must be sorted and unique")
        expected_pass = not self.missing_scenario_ids and all(
            item.status == "PASS" for item in self.scenario_validations
        )
        if (self.status == "PASS") != expected_pass:
            raise ValueError("report status does not match scenario validation facts")
        if self.synthetic_fixture_coverage_status != self.status:
            raise ValueError("fixture coverage status must match report status")
        return self


class QQQOptionsCloudSmokeChecklistItem(_SealedModel):
    item_id: str
    required_authority_task: Literal[
        "TRADING-2492_QC_QQQ_OPTIONS_BOUNDED_FREE_CLOUD_PILOT_V1"
    ]
    status: Literal["PENDING_OWNER_AUTHORIZATION"]
    evidence_status: Literal["NOT_EVALUATED"]

    @field_validator("item_id")
    @classmethod
    def _validate_item_id(cls, value: str) -> str:
        return _identifier(value, "item_id")


class QQQOptionsCloudSmokeChecklist(_SealedModel):
    schema_version: Literal["qqq_options_cloud_smoke_checklist.v1"]
    checklist_id: str
    created_at_utc: datetime
    policy_sha256: str
    golden_sha256: str
    owner_authorization_token: Literal["NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"]
    status: Literal["BLOCKED_OWNER_AUTHORIZATION"]
    items: tuple[QQQOptionsCloudSmokeChecklistItem, ...]
    external_action_executed: Literal[False]
    synthetic_pass_may_authorize_pilot: Literal[False]
    pilot_authorized: Literal[False]
    range_expansion_allowed: Literal[False]
    safety: QQQOptionsCrossLayerHarnessSafety

    @field_validator("checklist_id")
    @classmethod
    def _validate_checklist_id(cls, value: str) -> str:
        return _identifier(value, "checklist_id")

    @field_validator("created_at_utc")
    @classmethod
    def _validate_created_at(cls, value: datetime) -> datetime:
        return _utc(value, "created_at_utc")

    @field_validator("policy_sha256", "golden_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_items(self) -> Self:
        ids = tuple(item.item_id for item in self.items)
        if ids != _CHECKLIST_ITEM_IDS:
            raise ValueError("cloud checklist item inventory must remain complete and exact")
        return self


def load_qqq_options_cross_layer_validation_policy(
    path: Path = DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsCrossLayerHarnessPolicyLoadResult:
    resolved = _resolve_input_path(path, project_root=project_root)
    try:
        _assert_regular_non_symlink_file(resolved, root=None)
        payload = safe_load_yaml_path(resolved)
        if not isinstance(payload, dict):
            raise TypeError("policy root must be a mapping")
        policy = QQQOptionsCrossLayerHarnessPolicy.model_validate(payload, strict=False)
        _verify_authority_bindings(policy, project_root=project_root)
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionsCrossLayerValidationError(
            "QQQ_OPTIONS_VALIDATION_POLICY_INVALID",
            f"{resolved}: {exc}",
        ) from exc
    return QQQOptionsCrossLayerHarnessPolicyLoadResult(
        policy=policy,
        policy_path=resolved,
        policy_sha256=_lf_sha256_path(resolved),
    )


def load_qqq_options_cross_layer_validation_harness(
    *,
    policy_path: Path = DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_POLICY_PATH,
    golden_path: Path = DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_GOLDEN_PATH,
    project_root: Path = PROJECT_ROOT,
) -> LoadedQQQOptionsCrossLayerHarness:
    loaded_policy = load_qqq_options_cross_layer_validation_policy(
        policy_path,
        project_root=project_root,
    )
    resolved_golden = _resolve_input_path(golden_path, project_root=project_root)
    try:
        _assert_regular_non_symlink_file(resolved_golden, root=None)
        payload = safe_load_yaml_path(resolved_golden)
        if not isinstance(payload, dict):
            raise TypeError("golden root must be a mapping")
        golden = QQQOptionsCrossLayerGoldenManifest.model_validate(payload, strict=False)
        if golden.policy_sha256 != loaded_policy.policy_sha256:
            raise ValueError("golden policy hash does not match loaded policy")
        expected_goldens = tuple(
            QQQOptionsScenarioGolden(
                scenario_id=spec.scenario_id,
                fixture_sha256=spec.fixture_sha256,
            )
            for spec in loaded_policy.policy.scenarios
        )
        if golden.scenario_goldens != expected_goldens:
            raise ValueError("scenario golden hashes drifted")
        expected_corpus = build_qqq_options_validation_corpus_sha256(
            policy_sha256=loaded_policy.policy_sha256,
            scenario_goldens=expected_goldens,
        )
        if golden.corpus_sha256 != expected_corpus:
            raise ValueError("corpus golden hash drifted")
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionsCrossLayerValidationError(
            "QQQ_OPTIONS_VALIDATION_GOLDEN_INVALID",
            f"{resolved_golden}: {exc}",
        ) from exc
    return LoadedQQQOptionsCrossLayerHarness(
        policy=loaded_policy.policy,
        policy_path=loaded_policy.policy_path,
        policy_sha256=loaded_policy.policy_sha256,
        golden=golden,
        golden_path=resolved_golden,
        golden_sha256=_lf_sha256_path(resolved_golden),
    )


def build_qqq_options_validation_corpus_sha256(
    *,
    policy_sha256: str,
    scenario_goldens: tuple[QQQOptionsScenarioGolden, ...],
) -> str:
    _sha256(policy_sha256, "policy_sha256")
    ids = tuple(item.scenario_id for item in scenario_goldens)
    if ids != _SCENARIO_IDS:
        raise QQQOptionsCrossLayerValidationError(
            "QQQ_OPTIONS_VALIDATION_GOLDEN_INVENTORY_INVALID",
            "scenario goldens must remain complete, sorted, and exact",
        )
    return _canonical_sha256(
        {
            "schema_version": "qqq_options_cross_layer_validation_corpus_identity.v1",
            "policy_sha256": policy_sha256,
            "scenario_goldens": [item.model_dump(mode="json") for item in scenario_goldens],
        }
    )


def validate_qqq_options_cross_layer_observation(
    observation: QQQOptionsCrossLayerObservation,
    *,
    policy_path: Path = DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_POLICY_PATH,
    golden_path: Path = DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_GOLDEN_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsScenarioValidation:
    loaded = load_qqq_options_cross_layer_validation_harness(
        policy_path=policy_path,
        golden_path=golden_path,
        project_root=project_root,
    )
    spec = next(
        item
        for item in loaded.policy.scenarios
        if item.scenario_id == observation.scenario_id
    )
    mismatches: set[str] = set()
    comparisons = (
        (observation.terminal_layer, spec.terminal_layer, "TERMINAL_LAYER_MISMATCH"),
        (observation.observed_status, spec.expected_status, "STATUS_MISMATCH"),
        (observation.reason_codes, spec.expected_reason_codes, "REASON_CODES_MISMATCH"),
        (observation.order_count, spec.expected_order_count, "ORDER_COUNT_MISMATCH"),
        (observation.fill_count, spec.expected_fill_count, "FILL_COUNT_MISMATCH"),
        (
            observation.cash_preservation_required,
            spec.cash_preservation_required,
            "CASH_PRESERVATION_MISMATCH",
        ),
        (observation.run_valid, spec.run_valid, "RUN_VALIDITY_MISMATCH"),
        (observation.dq_status, spec.expected_dq_status, "DQ_STATUS_MISMATCH"),
        (observation.pit_status, spec.expected_pit_status, "PIT_STATUS_MISMATCH"),
    )
    mismatches.update(code for actual, expected, code in comparisons if actual != expected)
    expected_artifacts = tuple(
        (item.artifact_role, item.contract_id) for item in spec.required_artifacts
    )
    observed_artifacts = tuple(
        (item.artifact_role, item.contract_id) for item in observation.artifact_bindings
    )
    if observed_artifacts != expected_artifacts:
        mismatches.add("ARTIFACT_CONTRACT_MISMATCH")
    return QQQOptionsScenarioValidation.seal(
        schema_version="qqq_options_cross_layer_scenario_validation.v1",
        scenario_id=spec.scenario_id,
        fixture_sha256=spec.fixture_sha256,
        observation_sha256=observation.canonical_sha256(),
        status="PASS" if not mismatches else "FAIL",
        mismatch_codes=tuple(sorted(mismatches)),
        platform_evidence_status="NOT_EVALUATED_NO_AUTHORIZED_PILOT",
        pilot_authorized=False,
    )


def build_qqq_options_cross_layer_validation_report(
    observations: tuple[QQQOptionsCrossLayerObservation, ...],
    *,
    report_id: str,
    built_at_utc: datetime,
    policy_path: Path = DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_POLICY_PATH,
    golden_path: Path = DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_GOLDEN_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsCrossLayerValidationReport:
    loaded = load_qqq_options_cross_layer_validation_harness(
        policy_path=policy_path,
        golden_path=golden_path,
        project_root=project_root,
    )
    ids = tuple(item.scenario_id for item in observations)
    if len(ids) != len(set(ids)):
        raise QQQOptionsCrossLayerValidationError(
            "QQQ_OPTIONS_VALIDATION_OBSERVATION_INVENTORY_INVALID",
            "observations must be unique by scenario_id",
        )
    normalized_observations = tuple(sorted(observations, key=lambda item: item.scenario_id))
    validations = tuple(
        validate_qqq_options_cross_layer_observation(
            item,
            policy_path=policy_path,
            golden_path=golden_path,
            project_root=project_root,
        )
        for item in normalized_observations
    )
    missing = tuple(scenario_id for scenario_id in _SCENARIO_IDS if scenario_id not in set(ids))
    passed = not missing and all(item.status == "PASS" for item in validations)
    return QQQOptionsCrossLayerValidationReport.seal(
        schema_version="qqq_options_cross_layer_validation_report.v1",
        report_id=report_id,
        built_at_utc=built_at_utc,
        policy_sha256=loaded.policy_sha256,
        golden_sha256=loaded.golden_sha256,
        corpus_sha256=loaded.golden.corpus_sha256,
        scenario_validations=validations,
        missing_scenario_ids=missing,
        status="PASS" if passed else "FAIL",
        synthetic_fixture_coverage_status="PASS" if passed else "FAIL",
        platform_evidence_status="NOT_EVALUATED_NO_AUTHORIZED_PILOT",
        synthetic_fixture_is_platform_evidence=False,
        investment_interpretation_allowed=False,
        pilot_authorized=False,
        range_expansion_allowed=False,
        safety=loaded.policy.safety,
    )


def build_qqq_options_cloud_smoke_checklist(
    *,
    checklist_id: str,
    created_at_utc: datetime,
    policy_path: Path = DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_POLICY_PATH,
    golden_path: Path = DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_GOLDEN_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsCloudSmokeChecklist:
    loaded = load_qqq_options_cross_layer_validation_harness(
        policy_path=policy_path,
        golden_path=golden_path,
        project_root=project_root,
    )
    return QQQOptionsCloudSmokeChecklist.seal(
        schema_version="qqq_options_cloud_smoke_checklist.v1",
        checklist_id=checklist_id,
        created_at_utc=created_at_utc,
        policy_sha256=loaded.policy_sha256,
        golden_sha256=loaded.golden_sha256,
        owner_authorization_token=_NOT_GRANTED_TOKEN,
        status="BLOCKED_OWNER_AUTHORIZATION",
        items=tuple(
            QQQOptionsCloudSmokeChecklistItem.seal(
                item_id=item_id,
                required_authority_task=_REQUIRED_PILOT_TASK,
                status="PENDING_OWNER_AUTHORIZATION",
                evidence_status="NOT_EVALUATED",
            )
            for item_id in loaded.policy.cloud_checklist_item_ids
        ),
        external_action_executed=False,
        synthetic_pass_may_authorize_pilot=False,
        pilot_authorized=False,
        range_expansion_allowed=False,
        safety=loaded.policy.safety,
    )


def _resolve_input_path(path: Path, *, project_root: Path) -> Path:
    return path if path.is_absolute() else project_root / path


def _assert_regular_non_symlink_file(path: Path, *, root: Path | None) -> None:
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"required regular non-symlink file missing: {path}")
    if root is None:
        return
    resolved_root = root.resolve()
    resolved_path = path.resolve()
    if not resolved_path.is_relative_to(resolved_root):
        raise ValueError(f"authority path escapes project root: {path}")
    current = resolved_root
    relative = path.relative_to(root)
    for part in relative.parts:
        current /= part
        if current.is_symlink():
            raise ValueError(f"authority path contains symlink: {path}")


def _verify_authority_bindings(
    policy: QQQOptionsCrossLayerHarnessPolicy,
    *,
    project_root: Path,
) -> None:
    for binding in policy.authority_bindings:
        path = project_root / binding.path
        _assert_regular_non_symlink_file(path, root=project_root)
        if _lf_sha256_path(path) != binding.sha256:
            raise ValueError(f"authority hash drifted: {binding.authority_id}")


__all__ = [
    "DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_GOLDEN_PATH",
    "DEFAULT_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_POLICY_PATH",
    "LoadedQQQOptionsCrossLayerHarness",
    "QQQOptionsAuthorityBinding",
    "QQQOptionsCloudSmokeChecklist",
    "QQQOptionsCloudSmokeChecklistItem",
    "QQQOptionsCrossLayerArtifactBinding",
    "QQQOptionsCrossLayerGoldenManifest",
    "QQQOptionsCrossLayerHarnessPolicy",
    "QQQOptionsCrossLayerHarnessPolicyLoadResult",
    "QQQOptionsCrossLayerObservation",
    "QQQOptionsCrossLayerScenarioSpec",
    "QQQOptionsCrossLayerValidationError",
    "QQQOptionsCrossLayerValidationReport",
    "QQQOptionsRequiredArtifactContract",
    "QQQOptionsScenarioGolden",
    "QQQOptionsScenarioValidation",
    "build_qqq_options_cloud_smoke_checklist",
    "build_qqq_options_cross_layer_validation_report",
    "build_qqq_options_validation_corpus_sha256",
    "load_qqq_options_cross_layer_validation_harness",
    "load_qqq_options_cross_layer_validation_policy",
    "validate_qqq_options_cross_layer_observation",
]
