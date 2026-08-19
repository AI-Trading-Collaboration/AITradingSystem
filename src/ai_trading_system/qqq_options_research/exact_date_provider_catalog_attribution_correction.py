"""Sealed offline proposal for TRADING-2537.

The module only constructs and verifies an export-safe candidate package.  It has
no QuantConnect, network, browser, Cloud, project-mutation, order, or broker path.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.qqq_options_research.daily_transport_per_axis_collection_proposal import (
    build_per_axis_collection_proposal_package,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "qc_qqq_options_exact_date_provider_catalog_attribution_correction_v1.yaml"
)
DEFAULT_PACKAGE_ROOT = (
    PROJECT_ROOT
    / "inputs"
    / "research"
    / "qqq_options"
    / "trading_2537_exact_date_provider_catalog_attribution_correction_v1"
)
TASK_ID = (
    "TRADING-2537_QC_QQQ_OPTIONS_EXACT_DATE_PROVIDER_CATALOG_"
    "ATTRIBUTION_CORRECTION_V1"
)
_PREDECESSOR_TASK_ID = (
    "TRADING-2535_QC_QQQ_OPTIONS_FINAL_NEVER_CHAIN_SESSION_EXPORT_SAFE_PROVIDER_"
    "TRANSPORT_ATTRIBUTION_PROPOSAL_V1"
)
_UNSEALED_SHA256 = "0" * 64
_PACKAGE_FILES = (
    "main.py",
    "owner_decision_request.md",
    "package_manifest.json",
    "proposal.json",
    "run_scope.json",
)
_OUTPUT_KEYS = (
    "TRADING2537_TARGET_SESSION_COUNT",
    "TRADING2537_TARGET_SESSION_DATE",
    "TRADING2537_TARGET_SESSION_POSITION",
    "TRADING2537_TARGET_EQUITY_SLICE_PRESENT",
    "TRADING2537_TARGET_SUBSCRIBED_CHAIN_EVENT_COUNT",
    "TRADING2537_PROVIDER_PROBE_STATUS",
    "TRADING2537_PROVIDER_QUERY_ATTEMPT_COUNT",
    "TRADING2537_EXACT_DATE_RECORD_COUNT",
    "TRADING2537_EXACT_DATE_CONTRACT_COUNT",
    "TRADING2537_NON_TARGET_RECORD_COUNT",
    "TRADING2537_CROSS_DATE_FALLBACK_DETECTED",
    "TRADING2537_ATTRIBUTION",
    "TRADING2537_ATTRIBUTION_TERMINAL",
    "TRADING2537_IDENTITY",
    "TRADING2537_EXECUTION_TERMINAL",
)
_CLASSIFICATIONS = (
    "EXACT_DATE_CATALOG_AVAILABLE_SUBSCRIPTION_MISSING",
    "EXACT_DATE_CATALOG_EMPTY",
    "NO_EXACT_DATE_PROVIDER_EVIDENCE",
    "PROVIDER_PROBE_ERROR",
    "ATTRIBUTION_INDETERMINATE",
)


class AttributionProposalError(ValueError):
    """Typed fail-closed proposal error."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


def _canonical_json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _require_sha256(value: str, field: str) -> str:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{field} must be a lowercase SHA-256")
    return value


def _require_git_sha(value: str, field: str) -> str:
    if len(value) != 40 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{field} must be a lowercase Git SHA")
    return value


def _require_relative_path(value: str, field: str) -> str:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise ValueError(f"{field} must be a normalized repository-relative path")
    return value


def _bound_file(root: Path, relative_path: str) -> Path:
    candidate = (root / relative_path).resolve(strict=False)
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise AttributionProposalError("ATTRIBUTION_PROPOSAL_PATH_ESCAPE", relative_path) from exc
    if not candidate.is_file() or candidate.is_symlink():
        raise AttributionProposalError("ATTRIBUTION_PROPOSAL_INPUT_MISSING", relative_path)
    return candidate


def _duplicate_key_rejecting_json(raw: bytes) -> dict[str, object]:
    def hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        payload = json.loads(raw.decode("utf-8"), object_pairs_hook=hook)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("record must be UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("record must be a JSON object")
    return payload


class _FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _SealedModel(_FrozenModel):
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _require_sha256(value, "content_sha256")

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return _sha256_bytes(_canonical_json_bytes(self.semantic_payload()))

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return _sha256_bytes(self.canonical_bytes)

    @model_validator(mode="after")
    def _seal_matches(self) -> Self:
        if (
            self.content_sha256 != _UNSEALED_SHA256
            and self.content_sha256 != self.compute_content_sha256()
        ):
            raise ValueError("content_sha256 does not match semantic payload")
        return self

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        draft = cls(content_sha256=_UNSEALED_SHA256, **payload)
        return cls(content_sha256=draft.compute_content_sha256(), **payload)

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        _duplicate_key_rejecting_json(raw)
        value = cls.model_validate_json(raw)
        if raw != value.canonical_bytes:
            raise ValueError("record is not canonical JSON bytes")
        return value


class AttributionProposalPolicy(_FrozenModel):
    schema_version: Literal[
        "qc_qqq_options_exact_date_provider_catalog_attribution_correction_proposal_policy.v1"
    ]
    policy_id: Literal[
        "qc_qqq_options_exact_date_provider_catalog_attribution_correction_proposal_v1"
    ]
    policy_version: Literal["1.0.0"]
    policy_status: Literal["OWNER_REVIEW_REQUIRED_EXACT_PROPOSAL"]
    task_id: Literal[
        "TRADING-2537_QC_QQQ_OPTIONS_EXACT_DATE_PROVIDER_CATALOG_ATTRIBUTION_CORRECTION_V1"
    ]
    registration_base_repository_code_sha: str
    created_at_utc: datetime
    package_root: str
    target_project_id: Literal[34808569]
    requested_start: date
    requested_end: date
    exchange_calendar: Literal["XNYS"]
    expected_session_count: Literal[1202]
    expected_never_chain_session_count: Literal[1]
    source_backtest_id: Literal["acf111f24d09a41870f9a23e93fcbe3b"]
    source_evidence_path: str
    source_evidence_file_sha256: str
    source_evidence_content_sha256: str
    source_admission_path: str
    source_admission_file_sha256: str
    source_admission_content_sha256: str
    staged_readiness_policy_path: str
    staged_readiness_policy_file_sha256: str
    staged_readiness_evaluator_path: str
    staged_readiness_evaluator_file_sha256: str
    predecessor_package_manifest_path: str
    predecessor_package_manifest_file_sha256: str
    predecessor_package_manifest_content_sha256: str
    predecessor_project_code_sha256: str
    collector_id: Literal["codex_capability_coordinator"]
    independent_reviewer_id: Literal["project_owner"]
    authorization_expires_after_hours: Literal[168]
    authorization_single_use: Literal[True]
    authorization_invalidates_on_first_run_attempt: Literal[True]
    proposed_maximum_project_mutations: Literal[1]
    proposed_maximum_cloud_backtests: Literal[1]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    result_carrier: Literal["MANUAL_DOWNLOAD_RESULTS_JSON"]
    provider_probe: Literal["QCALGORITHM_OPTION_UNIVERSE_HISTORY_EXACT_DATE_COUNT_ONLY"]
    provider_query_timing: Literal["ON_END_AFTER_UNIQUE_TARGET_FINALIZATION"]
    provider_query_interval: Literal["TARGET_DATE_TO_NEXT_CALENDAR_DATE_END_EXCLUSIVE"]
    maximum_provider_query_attempts: Literal[1]
    source_date_field: Literal["OPTION_UNIVERSE_END_TIME_DATE"]
    exact_source_date_match_required: Literal[True]
    cross_date_fallback_allowed: Literal[False]
    execution_attribution_terminal_separation_required: Literal[True]
    subscription_observation: Literal["SLICE_OPTION_CHAINS_CANONICAL_KEY_EVENT_COUNT_ONLY"]
    allowed_classifications: tuple[str, ...]
    raw_option_rows_allowed: Literal[False]
    contract_identifiers_allowed: Literal[False]
    individual_contract_fields_allowed: Literal[False]
    logs_as_data_allowed: Literal[False]
    object_store_allowed: Literal[False]
    orders_allowed: Literal[False]
    fills_allowed: Literal[False]
    external_action_authorized: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("registration_base_repository_code_sha")
    @classmethod
    def _git_sha(cls, value: str) -> str:
        return _require_git_sha(value, "registration_base_repository_code_sha")

    @field_validator(
        "source_evidence_file_sha256",
        "source_evidence_content_sha256",
        "source_admission_file_sha256",
        "source_admission_content_sha256",
        "staged_readiness_policy_file_sha256",
        "staged_readiness_evaluator_file_sha256",
        "predecessor_package_manifest_file_sha256",
        "predecessor_package_manifest_content_sha256",
        "predecessor_project_code_sha256",
    )
    @classmethod
    def _sha256s(cls, value: str, info: ValidationInfo) -> str:
        return _require_sha256(value, str(info.field_name))

    @field_validator(
        "package_root",
        "source_evidence_path",
        "source_admission_path",
        "staged_readiness_policy_path",
        "staged_readiness_evaluator_path",
        "predecessor_package_manifest_path",
    )
    @classmethod
    def _paths(cls, value: str, info: ValidationInfo) -> str:
        return _require_relative_path(value, str(info.field_name))

    @model_validator(mode="after")
    def _semantic_contract(self) -> Self:
        if self.requested_start.isoformat() != "2021-02-22":
            raise ValueError("requested_start drift")
        if self.requested_end.isoformat() != "2025-12-02":
            raise ValueError("requested_end drift")
        if self.allowed_classifications != _CLASSIFICATIONS:
            raise ValueError("allowed_classifications drift")
        if self.created_at_utc.tzinfo is None:
            raise ValueError("created_at_utc must be timezone-aware")
        return self


class AttributionRunScope(_SealedModel):
    schema_version: Literal[
        "qc_qqq_options_exact_date_provider_catalog_attribution_correction_scope.v1"
    ]
    task_id: str
    requested_start: date
    requested_end: date
    exchange_calendar: Literal["XNYS"]
    expected_session_count: Literal[1202]
    session_ids: tuple[date, ...]
    expected_never_chain_session_count: Literal[1]
    provider_probe: Literal["QCALGORITHM_OPTION_UNIVERSE_HISTORY_EXACT_DATE_COUNT_ONLY"]
    provider_query_timing: Literal["ON_END_AFTER_UNIQUE_TARGET_FINALIZATION"]
    provider_query_interval: Literal["TARGET_DATE_TO_NEXT_CALENDAR_DATE_END_EXCLUSIVE"]
    maximum_provider_query_attempts: Literal[1]
    source_date_field: Literal["OPTION_UNIVERSE_END_TIME_DATE"]
    exact_source_date_match_required: Literal[True]
    cross_date_fallback_allowed: Literal[False]
    execution_attribution_terminal_separation_required: Literal[True]
    subscription_observation: Literal["SLICE_OPTION_CHAINS_CANONICAL_KEY_EVENT_COUNT_ONLY"]
    output_keys: tuple[str, ...]
    allowed_classifications: tuple[str, ...]
    target_project_id: Literal[34808569]
    proposed_maximum_project_mutations: Literal[1]
    proposed_maximum_cloud_backtests: Literal[1]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    current_project_mutations: Literal[0]
    current_cloud_backtests: Literal[0]
    raw_option_rows_allowed: Literal[False]
    contract_identifiers_allowed: Literal[False]
    individual_contract_fields_allowed: Literal[False]
    logs_as_data_allowed: Literal[False]
    object_store_allowed: Literal[False]
    external_action_performed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @model_validator(mode="after")
    def _scope_is_exact(self) -> Self:
        if self.task_id != TASK_ID:
            raise ValueError("task_id drift")
        if (
            len(self.session_ids) != 1202
            or self.session_ids[0] != self.requested_start
            or self.session_ids[-1] != self.requested_end
            or tuple(sorted(set(self.session_ids))) != self.session_ids
        ):
            raise ValueError("session_ids must be exact, sorted, and unique")
        if self.output_keys != _OUTPUT_KEYS:
            raise ValueError("output_keys drift")
        if self.allowed_classifications != _CLASSIFICATIONS:
            raise ValueError("allowed_classifications drift")
        return self


class AttributionProposal(_SealedModel):
    schema_version: Literal[
        "qc_qqq_options_exact_date_provider_catalog_attribution_correction_proposal.v1"
    ]
    task_id: str
    proposal_status: Literal["OWNER_FINAL_TOKEN_REQUIRED"]
    registration_base_repository_code_sha: str
    policy_file_sha256: str
    policy_canonical_sha256: str
    source_backtest_id: str
    source_evidence_file_sha256: str
    source_evidence_content_sha256: str
    source_admission_file_sha256: str
    source_admission_content_sha256: str
    staged_readiness_policy_file_sha256: str
    staged_readiness_evaluator_file_sha256: str
    predecessor_package_manifest_file_sha256: str
    predecessor_package_manifest_content_sha256: str
    predecessor_project_code_sha256: str
    run_scope_content_sha256: str
    run_scope_canonical_sha256: str
    project_code_lf_byte_count: int
    project_code_lf_sha256: str
    target_project_id: Literal[34808569]
    requested_range: Literal["2021-02-22..2025-12-02"]
    expected_session_count: Literal[1202]
    expected_never_chain_session_count: Literal[1]
    maximum_provider_query_attempts: Literal[1]
    exact_source_date_match_required: Literal[True]
    cross_date_fallback_allowed: Literal[False]
    execution_attribution_terminal_separation_required: Literal[True]
    proposed_maximum_project_mutations: Literal[1]
    proposed_maximum_cloud_backtests: Literal[1]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    attribution_can_change_dq_pit_status: Literal[False]
    selection_authorized: Literal[False]
    engine_authorized: Literal[False]
    external_action_performed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("registration_base_repository_code_sha")
    @classmethod
    def _proposal_git_sha(cls, value: str) -> str:
        return _require_git_sha(value, "registration_base_repository_code_sha")

    @field_validator(
        "policy_file_sha256",
        "policy_canonical_sha256",
        "source_evidence_file_sha256",
        "source_evidence_content_sha256",
        "source_admission_file_sha256",
        "source_admission_content_sha256",
        "staged_readiness_policy_file_sha256",
        "staged_readiness_evaluator_file_sha256",
        "predecessor_package_manifest_file_sha256",
        "predecessor_package_manifest_content_sha256",
        "predecessor_project_code_sha256",
        "run_scope_content_sha256",
        "run_scope_canonical_sha256",
        "project_code_lf_sha256",
    )
    @classmethod
    def _proposal_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _require_sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _proposal_contract(self) -> Self:
        if self.task_id != TASK_ID or self.project_code_lf_byte_count <= 0:
            raise ValueError("proposal identity invalid")
        return self


class ManifestArtifact(_FrozenModel):
    kind: Literal["PROJECT_CODE", "OWNER_DECISION_REQUEST", "PROPOSAL", "RUN_SCOPE"]
    relative_path: str
    byte_count: int
    sha256: str

    @field_validator("relative_path")
    @classmethod
    def _artifact_path(cls, value: str) -> str:
        return _require_relative_path(value, "relative_path")

    @field_validator("sha256")
    @classmethod
    def _artifact_hash(cls, value: str) -> str:
        return _require_sha256(value, "sha256")

    @model_validator(mode="after")
    def _artifact_size(self) -> Self:
        if self.byte_count <= 0:
            raise ValueError("artifact byte_count must be positive")
        return self


class AttributionPackageManifest(_SealedModel):
    schema_version: Literal[
        "qc_qqq_options_exact_date_provider_catalog_attribution_correction_package_manifest.v1"
    ]
    task_id: str
    artifact_count: Literal[4]
    artifacts: tuple[ManifestArtifact, ...]
    status: Literal["SEALED_OFFLINE_PROPOSAL_OWNER_REVIEW_REQUIRED"]
    external_action_authorized: Literal[False]
    external_action_performed: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @model_validator(mode="after")
    def _manifest_exact(self) -> Self:
        if self.task_id != TASK_ID or len(self.artifacts) != 4:
            raise ValueError("manifest identity invalid")
        expected = (
            ("PROJECT_CODE", "main.py"),
            ("OWNER_DECISION_REQUEST", "owner_decision_request.md"),
            ("PROPOSAL", "proposal.json"),
            ("RUN_SCOPE", "run_scope.json"),
        )
        actual = tuple((item.kind, item.relative_path) for item in self.artifacts)
        if actual != expected:
            raise ValueError("manifest artifact inventory drift")
        return self


@dataclass(frozen=True)
class LoadedAttributionProposalPolicy:
    policy: AttributionProposalPolicy
    file_sha256: str
    canonical_sha256: str


@dataclass(frozen=True)
class BuiltAttributionProposalPackage:
    policy: LoadedAttributionProposalPolicy
    run_scope: AttributionRunScope
    proposal: AttributionProposal
    manifest: AttributionPackageManifest
    project_code_bytes: bytes
    owner_decision_request_bytes: bytes


def load_attribution_proposal_policy(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> LoadedAttributionProposalPolicy:
    root = project_root.resolve()
    resolved = policy_path.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise AttributionProposalError(
            "ATTRIBUTION_PROPOSAL_POLICY_OUTSIDE_REPOSITORY", str(resolved)
        ) from exc
    if not resolved.is_file() or resolved.is_symlink():
        raise AttributionProposalError("ATTRIBUTION_PROPOSAL_POLICY_MISSING", str(resolved))
    raw = resolved.read_bytes()
    try:
        payload = safe_load_yaml_path(resolved)
        policy = AttributionProposalPolicy.model_validate(payload)
    except (OSError, ValueError) as exc:
        raise AttributionProposalError("ATTRIBUTION_PROPOSAL_POLICY_INVALID", str(exc)) from exc
    return LoadedAttributionProposalPolicy(
        policy=policy,
        file_sha256=_sha256_bytes(raw),
        canonical_sha256=_sha256_bytes(_canonical_json_bytes(policy.model_dump(mode="json"))),
    )


def _load_bound_json(
    *, root: Path, relative_path: str, expected_file_sha256: str, expected_content: str
) -> dict[str, object]:
    path = _bound_file(root, relative_path)
    raw = path.read_bytes()
    if _sha256_bytes(raw) != expected_file_sha256:
        raise AttributionProposalError("ATTRIBUTION_PROPOSAL_SOURCE_FILE_DRIFT", relative_path)
    try:
        payload = _duplicate_key_rejecting_json(raw)
    except ValueError as exc:
        raise AttributionProposalError(
            "ATTRIBUTION_PROPOSAL_SOURCE_JSON_INVALID", relative_path
        ) from exc
    if payload.get("content_sha256") != expected_content:
        raise AttributionProposalError("ATTRIBUTION_PROPOSAL_SOURCE_CONTENT_DRIFT", relative_path)
    semantic = dict(payload)
    semantic.pop("content_sha256", None)
    compact_seal = _sha256_bytes(
        (
            json.dumps(semantic, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")
    )
    pretty_seal = _sha256_bytes(_canonical_json_bytes(semantic))
    if expected_content not in (compact_seal, pretty_seal):
        raise AttributionProposalError("ATTRIBUTION_PROPOSAL_SOURCE_SEAL_INVALID", relative_path)
    return payload


def _verify_regular_file_hash(root: Path, relative_path: str, expected: str) -> None:
    path = _bound_file(root, relative_path)
    if _sha256_bytes(path.read_bytes()) != expected:
        raise AttributionProposalError("ATTRIBUTION_PROPOSAL_BOUND_FILE_DRIFT", relative_path)


def _render_project_code(*, session_ids: tuple[date, ...], identity: str) -> bytes:
    sessions = repr(tuple(item.isoformat() for item in session_ids))
    template = """from AlgorithmImports import *
from datetime import datetime, timedelta

# TRADING-2537 corrected candidate only. External execution requires a new exact owner token.
# Zero orders; bounded aggregate runtime statistics; no logs or Object Store.
EXPECTED_SESSIONS = __SESSIONS__
IDENTITY = __IDENTITY__
REQUESTED_RANGE = "2021-02-22..2025-12-02"
EVALUATED_RANGE = "2021-02-22..2025-12-02"


class QQQOptionsExactDateProviderCatalogAttributionCorrection(QCAlgorithm):
    def initialize(self):
        self.set_start_date(2021, 2, 22)
        self.set_end_date(2025, 12, 2)
        self.set_cash(100000)
        self.settings.daily_precise_end_time = True
        self.universe_settings.asynchronous = False
        self.set_time_zone(TimeZones.NEW_YORK)
        self._equity = self.add_equity(
            "QQQ", Resolution.DAILY, data_normalization_mode=DataNormalizationMode.RAW
        ).symbol
        option = self.add_option("QQQ", Resolution.DAILY)
        option.set_filter(
            lambda universe: universe.contracts(
                lambda contracts: [contract.symbol for contract in contracts]
            )
        )
        self._option = option.symbol
        self._states = {session: self._new_state() for session in EXPECTED_SESSIONS}
        self._order_event_count = 0

    @staticmethod
    def _new_state():
        return {
            "slice_events": 0,
            "equity_slice_present": False,
            "subscribed_chain_events": 0,
        }

    @staticmethod
    def _summarize_provider_history(target_date, history):
        exact_date_record_count = 0
        exact_date_contract_count = 0
        non_target_record_count = 0
        for option_universe in history:
            source_date = option_universe.end_time.date().isoformat()
            if source_date == target_date:
                exact_date_record_count += 1
                exact_date_contract_count += sum(1 for _ in option_universe)
            else:
                non_target_record_count += 1
        return {
            "exact_date_record_count": exact_date_record_count,
            "exact_date_contract_count": exact_date_contract_count,
            "non_target_record_count": non_target_record_count,
            "cross_date_fallback_detected": non_target_record_count > 0,
        }

    @staticmethod
    def _target_position(target_date):
        if target_date == EXPECTED_SESSIONS[0]:
            return "START_BOUNDARY"
        if target_date == EXPECTED_SESSIONS[-1]:
            return "END_BOUNDARY"
        return "INTERIOR"

    def _probe_provider_catalog(self, target_date):
        result = {
            "provider_probe_status": "ERROR",
            "provider_query_attempt_count": 1,
            "exact_date_record_count": 0,
            "exact_date_contract_count": 0,
            "non_target_record_count": 0,
            "cross_date_fallback_detected": False,
            "attribution": "PROVIDER_PROBE_ERROR",
            "attribution_terminal": "ERROR",
        }
        start_time = datetime.strptime(target_date, "%Y-%m-%d")
        end_time = start_time + timedelta(days=1)
        try:
            history = self.history[OptionUniverse](self._option, start_time, end_time)
            summary = self._summarize_provider_history(target_date, history)
        except Exception:
            return result
        result.update(summary)
        if summary["cross_date_fallback_detected"]:
            result["provider_probe_status"] = "CROSS_DATE_FALLBACK"
            result["attribution"] = "NO_EXACT_DATE_PROVIDER_EVIDENCE"
            result["attribution_terminal"] = "INDETERMINATE"
        elif summary["exact_date_record_count"] == 0:
            result["provider_probe_status"] = "NO_EXACT_DATE_RECORD"
            result["attribution"] = "NO_EXACT_DATE_PROVIDER_EVIDENCE"
            result["attribution_terminal"] = "INDETERMINATE"
        elif summary["exact_date_record_count"] != 1:
            result["provider_probe_status"] = "INDETERMINATE"
            result["attribution"] = "ATTRIBUTION_INDETERMINATE"
            result["attribution_terminal"] = "INDETERMINATE"
        elif summary["exact_date_contract_count"] > 0:
            result["provider_probe_status"] = "EXACT_DATE_AVAILABLE"
            result["attribution"] = "EXACT_DATE_CATALOG_AVAILABLE_SUBSCRIPTION_MISSING"
            result["attribution_terminal"] = "RESOLVED"
        else:
            result["provider_probe_status"] = "EXACT_DATE_EMPTY"
            result["attribution"] = "EXACT_DATE_CATALOG_EMPTY"
            result["attribution_terminal"] = "RESOLVED"
        return result

    def on_data(self, data: Slice):
        session_id = data.time.date().isoformat()
        state = self._states.get(session_id)
        if state is None:
            return
        state["slice_events"] += 1
        state["equity_slice_present"] |= data.bars.get(self._equity) is not None
        chain = data.option_chains.get(self._option)
        if chain is not None and len(chain) > 0:
            state["subscribed_chain_events"] += 1

    def on_order_event(self, order_event):
        self._order_event_count += 1

    def on_end_of_algorithm(self):
        observed_sessions = sum(
            int(state["slice_events"] > 0) for state in self._states.values()
        )
        targets = [
            (session, state)
            for session, state in self._states.items()
            if state["subscribed_chain_events"] == 0
        ]
        target_count = len(targets)
        target_date = "NOT_AVAILABLE"
        target_position = "NOT_EVALUATED"
        equity_present = "NOT_EVALUATED"
        chain_events = "NOT_EVALUATED"
        provider = {
            "provider_probe_status": "NOT_EVALUATED",
            "provider_query_attempt_count": 0,
            "exact_date_record_count": 0,
            "exact_date_contract_count": 0,
            "non_target_record_count": 0,
            "cross_date_fallback_detected": False,
            "attribution": "ATTRIBUTION_INDETERMINATE",
            "attribution_terminal": "INDETERMINATE",
        }
        if observed_sessions == len(EXPECTED_SESSIONS) and target_count == 1:
            target_date, state = targets[0]
            target_position = self._target_position(target_date)
            equity_present = str(state["equity_slice_present"]).lower()
            chain_events = str(state["subscribed_chain_events"])
            provider = self._probe_provider_catalog(target_date)
        self.set_runtime_statistic("TRADING2537_TARGET_SESSION_COUNT", str(target_count))
        self.set_runtime_statistic("TRADING2537_TARGET_SESSION_DATE", target_date)
        self.set_runtime_statistic("TRADING2537_TARGET_SESSION_POSITION", target_position)
        self.set_runtime_statistic(
            "TRADING2537_TARGET_EQUITY_SLICE_PRESENT", equity_present
        )
        self.set_runtime_statistic(
            "TRADING2537_TARGET_SUBSCRIBED_CHAIN_EVENT_COUNT", chain_events
        )
        self.set_runtime_statistic(
            "TRADING2537_PROVIDER_PROBE_STATUS", provider["provider_probe_status"]
        )
        self.set_runtime_statistic(
            "TRADING2537_PROVIDER_QUERY_ATTEMPT_COUNT",
            str(provider["provider_query_attempt_count"]),
        )
        self.set_runtime_statistic(
            "TRADING2537_EXACT_DATE_RECORD_COUNT",
            str(provider["exact_date_record_count"]),
        )
        self.set_runtime_statistic(
            "TRADING2537_EXACT_DATE_CONTRACT_COUNT",
            str(provider["exact_date_contract_count"]),
        )
        self.set_runtime_statistic(
            "TRADING2537_NON_TARGET_RECORD_COUNT",
            str(provider["non_target_record_count"]),
        )
        self.set_runtime_statistic(
            "TRADING2537_CROSS_DATE_FALLBACK_DETECTED",
            str(provider["cross_date_fallback_detected"]).lower(),
        )
        self.set_runtime_statistic("TRADING2537_ATTRIBUTION", provider["attribution"])
        self.set_runtime_statistic(
            "TRADING2537_ATTRIBUTION_TERMINAL", provider["attribution_terminal"]
        )
        self.set_runtime_statistic("TRADING2537_IDENTITY", IDENTITY)
        valid = (
            observed_sessions == len(EXPECTED_SESSIONS)
            and target_count == 1
            and provider["provider_query_attempt_count"] == 1
            and self._order_event_count == 0
            and not self.portfolio.invested
        )
        self.set_runtime_statistic(
            "TRADING2537_EXECUTION_TERMINAL",
            "status=" + ("COMPLETE" if valid else "INVALID")
            + "|expected_sessions=" + str(len(EXPECTED_SESSIONS))
            + "|observed_sessions=" + str(observed_sessions)
            + "|requested_range=" + REQUESTED_RANGE
            + "|evaluated_range=" + EVALUATED_RANGE
            + "|orders=0|fills=0|portfolio_invested=false|raw_rows=false"
            + "|contract_identifiers_exported=false|individual_fields_exported=false"
            + "|logs_as_data=false|object_store=false",
        )
"""
    rendered = template.replace("__SESSIONS__", sessions).replace("__IDENTITY__", repr(identity))
    return rendered.replace("\r\n", "\n").encode("utf-8")


def _owner_request(
    *,
    policy: LoadedAttributionProposalPolicy,
    proposal: AttributionProposal,
) -> bytes:
    values = policy.policy
    lines = [
        "# TRADING-2537 exact owner decision request — proposal only",
        "",
        "本文件不是 QuantConnect 执行授权。当前 external counters 为",
        "`project_mutations/cloud_backtests/orders/fills = 0/0/0/0`。",
        "",
        "proposal ordinary push 后，由 Project Owner 在当前 Codex 对话中发送完整 token：",
        "",
        "```text",
        "owner_decision:TRADING-2537:<YYYY-MM-DD>:authorize_single_zero_order_exact_date_provider_catalog_attribution_correction_v1",
        "ordinary_pushed_main_sha:<ORDINARY_PUSHED_MAIN_SHA>",
        f"registration_base_repository_code_sha:{values.registration_base_repository_code_sha}",
        f"proposal_policy_file_sha256:{policy.file_sha256}",
        f"proposal_policy_canonical_sha256:{policy.canonical_sha256}",
        f"source_evidence_file_sha256:{values.source_evidence_file_sha256}",
        f"source_evidence_content_sha256:{values.source_evidence_content_sha256}",
        f"source_admission_file_sha256:{values.source_admission_file_sha256}",
        f"source_admission_content_sha256:{values.source_admission_content_sha256}",
        f"staged_readiness_policy_file_sha256:{values.staged_readiness_policy_file_sha256}",
        f"staged_readiness_evaluator_file_sha256:{values.staged_readiness_evaluator_file_sha256}",
        f"predecessor_package_manifest_file_sha256:{values.predecessor_package_manifest_file_sha256}",
        f"predecessor_package_manifest_content_sha256:{values.predecessor_package_manifest_content_sha256}",
        f"predecessor_project_code_sha256:{values.predecessor_project_code_sha256}",
        f"run_scope_content_sha256:{proposal.run_scope_content_sha256}",
        f"run_scope_canonical_sha256:{proposal.run_scope_canonical_sha256}",
        f"proposal_content_sha256:{proposal.content_sha256}",
        f"proposal_canonical_sha256:{proposal.canonical_sha256}",
        f"project_code_lf_byte_count:{proposal.project_code_lf_byte_count}",
        f"project_code_lf_sha256:{proposal.project_code_lf_sha256}",
        "package_manifest_content_sha256:<FINAL_TRADING_2537_PROPOSAL_PACKAGE_CONTENT_SHA256>",
        f"target_project_id:{values.target_project_id}",
        "requested_range:2021-02-22..2025-12-02",
        "expected_session_count:1202",
        "expected_never_chain_session_count:1",
        "maximum_provider_query_attempts:1",
        "exact_source_date_match_required:true",
        "cross_date_fallback_allowed:false",
        "execution_attribution_terminal_separation_required:true",
        "maximum_project_mutations:1",
        "maximum_cloud_backtests:1",
        "maximum_orders:0",
        "maximum_fills:0",
        "collector:codex_capability_coordinator",
        "independent_reviewer:project_owner",
        "authorization_expires_at_utc:<OWNER_SELECTED_EXPIRY_NOT_MORE_THAN_168_HOURS>",
        "authorization_single_use:true",
        "authorization_invalidates_on_first_run_attempt:true",
        "```",
        "",
        "任何首次 project mutation 或 run attempt 都消费授权，无论成功或失败均不得自动重试。",
        "仍禁止 raw rows、contract identifiers、individual contract fields、logs-as-data、",
        "Object Store、订单、成交、DQ/PIT admission、selection、engine 或投资结论。",
        "",
    ]
    return "\n".join(lines).encode("utf-8")


def build_attribution_proposal_package(
    *, project_root: Path = PROJECT_ROOT
) -> BuiltAttributionProposalPackage:
    root = project_root.resolve()
    loaded_policy = load_attribution_proposal_policy(
        policy_path=root / DEFAULT_POLICY_PATH.relative_to(PROJECT_ROOT),
        project_root=root,
    )
    policy = loaded_policy.policy
    evidence = _load_bound_json(
        root=root,
        relative_path=policy.source_evidence_path,
        expected_file_sha256=policy.source_evidence_file_sha256,
        expected_content=policy.source_evidence_content_sha256,
    )
    admission = _load_bound_json(
        root=root,
        relative_path=policy.source_admission_path,
        expected_file_sha256=policy.source_admission_file_sha256,
        expected_content=policy.source_admission_content_sha256,
    )
    if (
        evidence.get("backtest_id") != policy.source_backtest_id
        or evidence.get("expected_session_count") != 1202
        or evidence.get("observed_session_count") != 1202
        or evidence.get("orders") != 0
        or evidence.get("fills") != 0
        or evidence.get("raw_rows_collected") is not False
    ):
        raise AttributionProposalError(
            "ATTRIBUTION_PROPOSAL_SOURCE_FACT_DRIFT", "TRADING-2532 evidence"
        )
    diagnostics = evidence.get("diagnostic_counts")
    if (
        not isinstance(diagnostics, dict)
        or diagnostics.get("TRADING2531_SESSIONS_NEVER_CHAIN") != 1
    ):
        raise AttributionProposalError(
            "ATTRIBUTION_PROPOSAL_NEVER_CHAIN_FACT_DRIFT", "expected exact count 1"
        )
    decision = admission.get("decision")
    if not isinstance(decision, dict) or (
        decision.get("dq_status") != "FAIL" or decision.get("pit_status") != "NOT_EVALUATED"
    ):
        raise AttributionProposalError(
            "ATTRIBUTION_PROPOSAL_ADMISSION_FACT_DRIFT", "TRADING-2533 decision"
        )
    _verify_regular_file_hash(
        root, policy.staged_readiness_policy_path, policy.staged_readiness_policy_file_sha256
    )
    _verify_regular_file_hash(
        root,
        policy.staged_readiness_evaluator_path,
        policy.staged_readiness_evaluator_file_sha256,
    )
    predecessor_manifest = _load_bound_json(
        root=root,
        relative_path=policy.predecessor_package_manifest_path,
        expected_file_sha256=policy.predecessor_package_manifest_file_sha256,
        expected_content=policy.predecessor_package_manifest_content_sha256,
    )
    predecessor_artifacts = predecessor_manifest.get("artifacts")
    if (
        predecessor_manifest.get("task_id") != _PREDECESSOR_TASK_ID
        or predecessor_manifest.get("status")
        != "SEALED_OFFLINE_PROPOSAL_OWNER_REVIEW_REQUIRED"
        or not isinstance(predecessor_artifacts, list)
    ):
        raise AttributionProposalError(
            "ATTRIBUTION_PROPOSAL_PREDECESSOR_MANIFEST_DRIFT", "TRADING-2535 manifest"
        )
    predecessor_project = next(
        (
            item
            for item in predecessor_artifacts
            if isinstance(item, dict)
            and item.get("kind") == "PROJECT_CODE"
            and item.get("relative_path") == "main.py"
        ),
        None,
    )
    if (
        not isinstance(predecessor_project, dict)
        or predecessor_project.get("sha256") != policy.predecessor_project_code_sha256
    ):
        raise AttributionProposalError(
            "ATTRIBUTION_PROPOSAL_PREDECESSOR_PROJECT_DRIFT", "TRADING-2535 main.py"
        )
    predecessor = build_per_axis_collection_proposal_package(project_root=root)
    session_ids = predecessor.run_scope.session_ids
    if len(session_ids) != policy.expected_session_count:
        raise AttributionProposalError(
            "ATTRIBUTION_PROPOSAL_SESSION_SCOPE_DRIFT", str(len(session_ids))
        )
    run_scope = AttributionRunScope.seal(
        schema_version="qc_qqq_options_exact_date_provider_catalog_attribution_correction_scope.v1",
        task_id=TASK_ID,
        requested_start=policy.requested_start,
        requested_end=policy.requested_end,
        exchange_calendar=policy.exchange_calendar,
        expected_session_count=policy.expected_session_count,
        session_ids=session_ids,
        expected_never_chain_session_count=policy.expected_never_chain_session_count,
        provider_probe=policy.provider_probe,
        provider_query_timing=policy.provider_query_timing,
        provider_query_interval=policy.provider_query_interval,
        maximum_provider_query_attempts=policy.maximum_provider_query_attempts,
        source_date_field=policy.source_date_field,
        exact_source_date_match_required=policy.exact_source_date_match_required,
        cross_date_fallback_allowed=policy.cross_date_fallback_allowed,
        execution_attribution_terminal_separation_required=(
            policy.execution_attribution_terminal_separation_required
        ),
        subscription_observation=policy.subscription_observation,
        output_keys=_OUTPUT_KEYS,
        allowed_classifications=_CLASSIFICATIONS,
        target_project_id=policy.target_project_id,
        proposed_maximum_project_mutations=policy.proposed_maximum_project_mutations,
        proposed_maximum_cloud_backtests=policy.proposed_maximum_cloud_backtests,
        maximum_orders=policy.maximum_orders,
        maximum_fills=policy.maximum_fills,
        current_project_mutations=0,
        current_cloud_backtests=0,
        raw_option_rows_allowed=False,
        contract_identifiers_allowed=False,
        individual_contract_fields_allowed=False,
        logs_as_data_allowed=False,
        object_store_allowed=False,
        external_action_performed=False,
        production_effect="none",
        broker_action="none",
    )
    identity = (
        "schema=qc_qqq_options_exact_date_provider_catalog_attribution_correction_runtime.v1"
        f"|source={policy.source_evidence_content_sha256}"
        f"|admission={policy.source_admission_content_sha256}"
        f"|staged_policy={policy.staged_readiness_policy_file_sha256}"
        f"|predecessor={policy.predecessor_package_manifest_content_sha256}"
    )
    project_code = _render_project_code(session_ids=session_ids, identity=identity)
    proposal = AttributionProposal.seal(
        schema_version="qc_qqq_options_exact_date_provider_catalog_attribution_correction_proposal.v1",
        task_id=TASK_ID,
        proposal_status="OWNER_FINAL_TOKEN_REQUIRED",
        registration_base_repository_code_sha=policy.registration_base_repository_code_sha,
        policy_file_sha256=loaded_policy.file_sha256,
        policy_canonical_sha256=loaded_policy.canonical_sha256,
        source_backtest_id=policy.source_backtest_id,
        source_evidence_file_sha256=policy.source_evidence_file_sha256,
        source_evidence_content_sha256=policy.source_evidence_content_sha256,
        source_admission_file_sha256=policy.source_admission_file_sha256,
        source_admission_content_sha256=policy.source_admission_content_sha256,
        staged_readiness_policy_file_sha256=policy.staged_readiness_policy_file_sha256,
        staged_readiness_evaluator_file_sha256=policy.staged_readiness_evaluator_file_sha256,
        predecessor_package_manifest_file_sha256=(
            policy.predecessor_package_manifest_file_sha256
        ),
        predecessor_package_manifest_content_sha256=(
            policy.predecessor_package_manifest_content_sha256
        ),
        predecessor_project_code_sha256=policy.predecessor_project_code_sha256,
        run_scope_content_sha256=run_scope.content_sha256,
        run_scope_canonical_sha256=run_scope.canonical_sha256,
        project_code_lf_byte_count=len(project_code),
        project_code_lf_sha256=_sha256_bytes(project_code),
        target_project_id=policy.target_project_id,
        requested_range="2021-02-22..2025-12-02",
        expected_session_count=policy.expected_session_count,
        expected_never_chain_session_count=policy.expected_never_chain_session_count,
        maximum_provider_query_attempts=policy.maximum_provider_query_attempts,
        exact_source_date_match_required=policy.exact_source_date_match_required,
        cross_date_fallback_allowed=policy.cross_date_fallback_allowed,
        execution_attribution_terminal_separation_required=(
            policy.execution_attribution_terminal_separation_required
        ),
        proposed_maximum_project_mutations=policy.proposed_maximum_project_mutations,
        proposed_maximum_cloud_backtests=policy.proposed_maximum_cloud_backtests,
        maximum_orders=policy.maximum_orders,
        maximum_fills=policy.maximum_fills,
        attribution_can_change_dq_pit_status=False,
        selection_authorized=False,
        engine_authorized=False,
        external_action_performed=False,
        production_effect="none",
        broker_action="none",
    )
    owner_request = _owner_request(policy=loaded_policy, proposal=proposal)
    artifacts = (
        ManifestArtifact(
            kind="PROJECT_CODE",
            relative_path="main.py",
            byte_count=len(project_code),
            sha256=_sha256_bytes(project_code),
        ),
        ManifestArtifact(
            kind="OWNER_DECISION_REQUEST",
            relative_path="owner_decision_request.md",
            byte_count=len(owner_request),
            sha256=_sha256_bytes(owner_request),
        ),
        ManifestArtifact(
            kind="PROPOSAL",
            relative_path="proposal.json",
            byte_count=len(proposal.canonical_bytes),
            sha256=proposal.canonical_sha256,
        ),
        ManifestArtifact(
            kind="RUN_SCOPE",
            relative_path="run_scope.json",
            byte_count=len(run_scope.canonical_bytes),
            sha256=run_scope.canonical_sha256,
        ),
    )
    manifest = AttributionPackageManifest.seal(
        schema_version="qc_qqq_options_exact_date_provider_catalog_attribution_correction_package_manifest.v1",
        task_id=TASK_ID,
        artifact_count=4,
        artifacts=artifacts,
        status="SEALED_OFFLINE_PROPOSAL_OWNER_REVIEW_REQUIRED",
        external_action_authorized=False,
        external_action_performed=False,
        orders=0,
        fills=0,
        production_effect="none",
        broker_action="none",
    )
    return BuiltAttributionProposalPackage(
        policy=loaded_policy,
        run_scope=run_scope,
        proposal=proposal,
        manifest=manifest,
        project_code_bytes=project_code,
        owner_decision_request_bytes=owner_request,
    )


def write_attribution_proposal_package(
    *, project_root: Path = PROJECT_ROOT, package_root: Path | None = None
) -> BuiltAttributionProposalPackage:
    root = project_root.resolve()
    target = (
        package_root.resolve()
        if package_root is not None
        else root / DEFAULT_PACKAGE_ROOT.relative_to(PROJECT_ROOT)
    )
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise AttributionProposalError(
            "ATTRIBUTION_PROPOSAL_PACKAGE_OUTSIDE_REPOSITORY", str(target)
        ) from exc
    built = build_attribution_proposal_package(project_root=root)
    target.mkdir(parents=True, exist_ok=True)
    payloads = {
        "main.py": built.project_code_bytes,
        "owner_decision_request.md": built.owner_decision_request_bytes,
        "package_manifest.json": built.manifest.canonical_bytes,
        "proposal.json": built.proposal.canonical_bytes,
        "run_scope.json": built.run_scope.canonical_bytes,
    }
    for name, raw in payloads.items():
        write_bytes_atomic(target / name, raw)
    return built


def load_attribution_proposal_package(
    *, project_root: Path = PROJECT_ROOT, package_root: Path | None = None
) -> BuiltAttributionProposalPackage:
    root = project_root.resolve()
    target = (
        package_root.resolve()
        if package_root is not None
        else root / DEFAULT_PACKAGE_ROOT.relative_to(PROJECT_ROOT)
    )
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise AttributionProposalError(
            "ATTRIBUTION_PROPOSAL_PACKAGE_OUTSIDE_REPOSITORY", str(target)
        ) from exc
    if not target.is_dir() or target.is_symlink():
        raise AttributionProposalError("ATTRIBUTION_PROPOSAL_PACKAGE_MISSING", str(target))
    inventory = tuple(sorted(item.name for item in target.iterdir()))
    if inventory != _PACKAGE_FILES:
        raise AttributionProposalError(
            "ATTRIBUTION_PROPOSAL_PACKAGE_INVENTORY_INVALID", repr(inventory)
        )
    try:
        manifest = AttributionPackageManifest.from_json_bytes(
            (target / "package_manifest.json").read_bytes()
        )
        proposal = AttributionProposal.from_json_bytes((target / "proposal.json").read_bytes())
        run_scope = AttributionRunScope.from_json_bytes((target / "run_scope.json").read_bytes())
    except (OSError, ValueError) as exc:
        raise AttributionProposalError(
            "ATTRIBUTION_PROPOSAL_PACKAGE_RECORD_INVALID", str(exc)
        ) from exc
    for artifact in manifest.artifacts:
        raw = (target / artifact.relative_path).read_bytes()
        if len(raw) != artifact.byte_count or _sha256_bytes(raw) != artifact.sha256:
            raise AttributionProposalError(
                "ATTRIBUTION_PROPOSAL_PACKAGE_ARTIFACT_DRIFT", artifact.relative_path
            )
    expected = build_attribution_proposal_package(project_root=root)
    if (
        manifest != expected.manifest
        or proposal != expected.proposal
        or run_scope != expected.run_scope
        or (target / "main.py").read_bytes() != expected.project_code_bytes
        or (target / "owner_decision_request.md").read_bytes()
        != expected.owner_decision_request_bytes
    ):
        raise AttributionProposalError("ATTRIBUTION_PROPOSAL_PACKAGE_REPLAY_MISMATCH", str(target))
    return expected
