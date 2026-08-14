"""Offline authority for the TRADING-2520 daily-Slice zero-order revalidation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.qqq_options_research import (
    primary_window_daily_slice_failure_fix as predecessor_v1,
)
from ai_trading_system.qqq_options_research import (
    primary_window_export_safe_derived_aggregate_collector as collector_v1,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_ZERO_ORDER_REVALIDATION_POLICY_PATH = Path(
    "config/research/qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_v1.yaml"
)
DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_ZERO_ORDER_REVALIDATION_PACKAGE_ROOT = Path(
    "inputs/research/qqq_options/trading_2520_primary_window_daily_slice_zero_order_revalidation_v1"
)

_TASK_ID = "TRADING-2520_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_ZERO_ORDER_REVALIDATION_V1"
_PACKAGE_FILES = (
    "investigation.json",
    "main.py",
    "owner_decision_request.md",
    "package_manifest.json",
    "proposal.json",
    "run_scope.json",
)
_PACKAGE_INVENTORY = tuple(item for item in _PACKAGE_FILES if item != "package_manifest.json")
_PREDECESSOR_CODE_PATH = Path(
    "inputs/research/qqq_options/trading_2519_primary_window_daily_slice_failure_fix_v1/main.py"
)
_EXPECTED_HYPOTHESIS_IDS = (
    "H1_DAILY_SLICE_DELIVERY",
    "H2_OPTION_CONTRACT_UNDERLYING_ACCESSOR",
    "H3_DAILY_TIME_FRONTIER_IDENTITY",
    "H4_TRANSPORT_AND_COVERAGE",
)


class QCQQQOptionsDailySliceRevalidationError(ValueError):
    """Raised when the offline revalidation authority fails closed."""


def _canonical_json_bytes(value: object) -> bytes:
    def encode(item: object) -> object:
        if isinstance(item, BaseModel):
            return item.model_dump(mode="json")
        if isinstance(item, datetime):
            return item.isoformat().replace("+00:00", "Z")
        if isinstance(item, date):
            return item.isoformat()
        raise TypeError(f"unsupported canonical JSON value: {type(item).__name__}")

    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
        default=encode,
    ).encode("utf-8")


def _duplicate_key_rejecting_json(raw: bytes) -> object:
    def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid UTF-8 JSON") from exc


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _sha256(value: str, field: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{field} must be a lowercase SHA-256")
    return value


def _git_sha(value: str, field: str) -> str:
    if len(value) != 40 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{field} must be a lowercase Git SHA")
    return value


def _identifier(value: str, field: str) -> str:
    if not value or value != value.strip() or any(character.isspace() for character in value):
        raise ValueError(f"{field} must be a non-empty identifier")
    return value


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{field} must be timezone-aware UTC")
    return value


def _relative_path(value: str, field: str) -> str:
    path = Path(value)
    if not value or path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise ValueError(f"{field} must be a normalized repository-relative path")
    return value


def _bound_file(path: Path, *, root: Path, field: str) -> Path:
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{field} must be repository-relative")
    resolved = (root / path).resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError(f"{field} escapes project root") from exc
    if not resolved.is_file() or resolved.is_symlink():
        raise ValueError(f"{field} must be an existing non-symlink file")
    return resolved


def _lf_bytes(path: Path) -> bytes:
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"{path.name} must be UTF-8") from exc
    return text.replace("\r\n", "\n").replace("\r", "\n").encode("utf-8")


class _FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _SealedModel(_FrozenModel):
    content_sha256: str = Field(exclude=True)

    @field_validator("content_sha256")
    @classmethod
    def _content_hash(cls, value: str) -> str:
        return _sha256(value, "content_sha256")

    @classmethod
    def seal(cls, **payload: object) -> Self:
        expected = _sha256_bytes(_canonical_json_bytes(payload))
        return cls.model_validate({**payload, "content_sha256": expected})

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        parsed = _duplicate_key_rejecting_json(raw)
        if not isinstance(parsed, dict):
            raise ValueError("sealed record must be a JSON object")
        content_hash = parsed.pop("content_sha256", None)
        if content_hash is not None:
            raise ValueError("content_sha256 must not be serialized")
        return cls.seal(**parsed)

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))


class PredecessorAuthority(_FrozenModel):
    task_id: Literal[
        "TRADING-2519_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SCHEDULE_RESULT_FAILURE_ADMISSION_V1"
    ]
    final_main_sha: str
    package_relative_path: str
    project_code_lf_sha256: str
    failed_backtest_id: Literal["b6d711f67a47199667c8a62f86208b28"]
    failed_result_file_sha256: str
    observed_session_count: Literal[0]
    invalid_session_count: Literal[1202]

    @field_validator("final_main_sha")
    @classmethod
    def _main_sha(cls, value: str) -> str:
        return _git_sha(value, "final_main_sha")

    @field_validator("package_relative_path")
    @classmethod
    def _package_path(cls, value: str) -> str:
        return _relative_path(value, "package_relative_path")

    @field_validator("project_code_lf_sha256", "failed_result_file_sha256")
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, info.field_name or "sha256")


class CollectorAuthority(_FrozenModel):
    policy_path: str
    policy_file_sha256: str
    policy_canonical_sha256: str
    transport_map_sha256: str

    @field_validator("policy_path")
    @classmethod
    def _path(cls, value: str) -> str:
        return _relative_path(value, "collector policy_path")

    @field_validator("policy_file_sha256", "policy_canonical_sha256", "transport_map_sha256")
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, info.field_name or "sha256")


class RootCauseHypothesis(_FrozenModel):
    hypothesis_id: str
    status: Literal[
        "UNVERIFIED_PRIMARY_HYPOTHESIS",
        "CONFIRMED_OFFLINE_CODE_DEFECT",
        "UNVERIFIED_GUARDED_HYPOTHESIS",
        "UNVERIFIED_RESULT_HYPOTHESIS",
    ]
    finding: str
    successor_test: str

    @field_validator("hypothesis_id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _identifier(value, "hypothesis_id")


class RevalidationRunScopePolicy(_FrozenModel):
    target_project_id: Literal[34808569]
    requested_start: date
    requested_end: date
    expected_session_count: Literal[1202]
    primary_research_role: Literal["PRIMARY"]
    exchange_calendar: Literal["XNYS"]
    maximum_project_mutations: Literal[1]
    maximum_cloud_backtests: Literal[1]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]

    @model_validator(mode="after")
    def _range(self) -> Self:
        if self.requested_start != date(2021, 2, 22) or self.requested_end != date(2025, 12, 2):
            raise ValueError("revalidation must remain on the exact PRIMARY window")
        return self


class RevalidationSafety(_FrozenModel):
    current_owner_authorization_status: Literal["NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"]
    predecessor_v3_authorization_consumed: Literal[True]
    further_cloud_run_authorized: Literal[False]
    external_action_performed_by_task: Literal[False]
    selection_authorized: Literal[False]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    evidence_status: Literal["FAIL"]
    dq_pit_status: Literal["NOT_EVALUATED"]
    option_event_dq_pit_status: Literal["NOT_EVALUATED"]
    raw_option_rows_permitted: Literal[False]
    owner_policy_value_count: Literal[0]
    thresholds_introduced: Literal[False]
    investment_interpretation_authorized: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class DailySliceZeroOrderRevalidationPolicy(_FrozenModel):
    schema_version: Literal[
        "qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_policy.v1"
    ]
    policy_id: Literal[
        "TRADING_2520_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_ZERO_ORDER_REVALIDATION_V1"
    ]
    policy_version: Literal["1.0.0"]
    status: Literal["OFFLINE_REVALIDATION_PACKAGE_BASELINE"]
    task_id: Literal[
        "TRADING-2520_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_ZERO_ORDER_REVALIDATION_V1"
    ]
    registration_base_repository_code_sha: str
    predecessor: PredecessorAuthority
    collector_authority: CollectorAuthority
    root_cause_hypotheses: tuple[RootCauseHypothesis, ...]
    official_sources: tuple[str, ...]
    run_scope: RevalidationRunScopePolicy
    allowed_actions_if_separately_authorized: tuple[str, ...]
    prohibited_actions: tuple[str, ...]
    safety: RevalidationSafety

    @field_validator("registration_base_repository_code_sha")
    @classmethod
    def _base(cls, value: str) -> str:
        return _git_sha(value, "registration_base_repository_code_sha")

    @model_validator(mode="after")
    def _authority(self) -> Self:
        ids = tuple(item.hypothesis_id for item in self.root_cause_hypotheses)
        if ids != _EXPECTED_HYPOTHESIS_IDS or len(set(ids)) != len(ids):
            raise ValueError("root-cause hypothesis inventory drifted")
        if self.root_cause_hypotheses[1].status != "CONFIRMED_OFFLINE_CODE_DEFECT":
            raise ValueError("underlying accessor defect must remain explicitly confirmed")
        if self.allowed_actions_if_separately_authorized != (
            "QUANTCONNECT_LOGIN",
            "MODIFY_EXISTING_DEDICATED_PROJECT_ONCE",
            "RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST",
            "EXPORT_SAFE_MANUAL_DOWNLOAD_RESULTS_COLLECTION",
        ):
            raise ValueError("allowed-action inventory drifted")
        if self.prohibited_actions != (
            "API",
            "BROKER",
            "CLI",
            "HTTP",
            "INVESTMENT_INTERPRETATION",
            "LIVE",
            "OBJECT_STORE",
            "PAPER",
            "PRODUCTION",
            "PURCHASE_OR_SUBSCRIPTION",
            "RAW_OPTIONS_DATA_DOWNLOAD",
            "RAW_OPTION_ROW_LOGGING_OR_EXPORT",
            "SECOND_CLOUD_BACKTEST",
        ):
            raise ValueError("prohibited-action inventory drifted")
        return self

    @property
    def canonical_sha256(self) -> str:
        return _sha256_bytes(_canonical_json_bytes(self.model_dump(mode="json")))


@dataclass(frozen=True)
class LoadedDailySliceZeroOrderRevalidationPolicy:
    policy: DailySliceZeroOrderRevalidationPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str


class DailySliceRevalidationInvestigation(_SealedModel):
    schema_version: Literal["qc_qqq_options_daily_slice_revalidation_investigation.v1"]
    investigation_id: Literal["trading-2520-daily-slice-root-cause-v1"]
    task_id: Literal[
        "TRADING-2520_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_ZERO_ORDER_REVALIDATION_V1"
    ]
    created_at_utc: datetime
    predecessor_project_code_lf_sha256: str
    predecessor_result_file_sha256: str
    hypothesis_ids: tuple[str, ...]
    confirmed_code_defects: tuple[str, ...]
    unverified_cloud_hypotheses: tuple[str, ...]
    official_sources: tuple[str, ...]
    conclusion: Literal["OFFLINE_FIX_READY_NEW_OWNER_TOKEN_REQUIRED_BEFORE_ANY_EXTERNAL_ACTION"]
    external_action_performed: Literal[False]
    evidence_status: Literal["FAIL"]
    dq_pit_status: Literal["NOT_EVALUATED"]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]

    @field_validator("created_at_utc")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return _utc(value, "created_at_utc")

    @field_validator("predecessor_project_code_lf_sha256", "predecessor_result_file_sha256")
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, info.field_name or "sha256")


class RevalidationPackageArtifact(_FrozenModel):
    relative_path: str
    role: Literal[
        "INVESTIGATION",
        "PROJECT_CODE",
        "OWNER_DECISION_TEMPLATE",
        "PROPOSAL",
        "RUN_SCOPE",
    ]
    sha256: str
    byte_count: int = Field(ge=1)

    @field_validator("relative_path")
    @classmethod
    def _path(cls, value: str) -> str:
        return _relative_path(value, "artifact relative_path")

    @field_validator("sha256")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _sha256(value, "artifact sha256")


class DailySliceRevalidationPackageManifest(_SealedModel):
    schema_version: Literal[
        "qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_package.v1"
    ]
    package_id: Literal["TRADING_2520_PRIMARY_WINDOW_DAILY_SLICE_ZERO_ORDER_REVALIDATION_V1"]
    task_id: Literal[
        "TRADING-2520_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_ZERO_ORDER_REVALIDATION_V1"
    ]
    created_at_utc: datetime
    registration_base_repository_code_sha: str
    policy_file_sha256: str
    policy_canonical_sha256: str
    run_scope_content_sha256: str
    proposal_content_sha256: str
    project_code_lf_sha256: str
    project_code_lf_byte_count: int = Field(ge=1)
    investigation_content_sha256: str
    artifacts: tuple[RevalidationPackageArtifact, ...]
    external_action_performed: Literal[False]
    owner_authorization_status: Literal["NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"]
    evidence_status: Literal["FAIL"]
    dq_pit_status: Literal["NOT_EVALUATED"]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]

    @field_validator("created_at_utc")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return _utc(value, "created_at_utc")

    @field_validator("registration_base_repository_code_sha")
    @classmethod
    def _base(cls, value: str) -> str:
        return _git_sha(value, "registration_base_repository_code_sha")

    @field_validator(
        "policy_file_sha256",
        "policy_canonical_sha256",
        "run_scope_content_sha256",
        "proposal_content_sha256",
        "project_code_lf_sha256",
        "investigation_content_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, info.field_name or "sha256")

    @model_validator(mode="after")
    def _inventory(self) -> Self:
        paths = tuple(item.relative_path for item in self.artifacts)
        if paths != _PACKAGE_INVENTORY or len(set(paths)) != len(paths):
            raise ValueError("package artifact inventory drifted")
        return self


@dataclass(frozen=True)
class BuiltDailySliceZeroOrderRevalidationPackage:
    policy: LoadedDailySliceZeroOrderRevalidationPolicy
    run_scope: collector_v1.QCQQQOptionsDerivedAggregateCollectorRunScope
    proposal: collector_v1.QCQQQOptionsDerivedAggregateCollectorProposal
    investigation: DailySliceRevalidationInvestigation
    manifest: DailySliceRevalidationPackageManifest
    payloads: dict[str, bytes]


@dataclass(frozen=True)
class LoadedDailySliceZeroOrderRevalidationPackage:
    policy: LoadedDailySliceZeroOrderRevalidationPolicy
    run_scope: collector_v1.QCQQQOptionsDerivedAggregateCollectorRunScope
    proposal: collector_v1.QCQQQOptionsDerivedAggregateCollectorProposal
    investigation: DailySliceRevalidationInvestigation
    manifest: DailySliceRevalidationPackageManifest
    package_root: Path


def load_qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_policy(
    *,
    project_root: Path = PROJECT_ROOT,
    policy_path: Path = (
        DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_ZERO_ORDER_REVALIDATION_POLICY_PATH
    ),
) -> LoadedDailySliceZeroOrderRevalidationPolicy:
    root = project_root.resolve()
    path = _bound_file(policy_path, root=root, field="revalidation policy")
    raw = path.read_bytes()
    try:
        payload = safe_load_yaml_path(path)
        policy = DailySliceZeroOrderRevalidationPolicy.model_validate(payload)
        predecessor = (
            predecessor_v1.build_qc_qqq_options_primary_window_daily_slice_failure_fix_package(
                project_root=root
            )
        )
        if (
            predecessor.manifest.successor_project_code_lf_sha256
            != policy.predecessor.project_code_lf_sha256
            or predecessor.manifest.result_file_sha256
            != policy.predecessor.failed_result_file_sha256
            or predecessor.package_root.relative_to(root).as_posix()
            != policy.predecessor.package_relative_path
        ):
            raise ValueError("2519 predecessor identity drifted")
        collector = (
            collector_v1.load_qc_qqq_options_primary_window_derived_aggregate_collector_policy(
                project_root=root
            )
        )
        authority = policy.collector_authority
        if (
            collector.policy_path.relative_to(root).as_posix() != authority.policy_path
            or collector.policy_file_sha256 != authority.policy_file_sha256
            or collector.policy_canonical_sha256 != authority.policy_canonical_sha256
            or collector.policy.transport.canonical_sha256 != authority.transport_map_sha256
        ):
            raise ValueError("2512 collector authority drifted")
    except QCQQQOptionsDailySliceRevalidationError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsDailySliceRevalidationError(
            f"revalidation policy authority rejected: {exc}"
        ) from exc
    return LoadedDailySliceZeroOrderRevalidationPolicy(
        policy=policy,
        policy_path=path,
        policy_file_sha256=_sha256_bytes(raw),
        policy_canonical_sha256=policy.canonical_sha256,
    )


def _runtime_identity(
    *,
    scope: collector_v1.QCQQQOptionsDerivedAggregateCollectorRunScope,
    collector: collector_v1.QCQQQOptionsDerivedAggregateCollectorPolicyLoadResult,
) -> str:
    return (
        "schema=qc_qqq_options_derived_aggregate_collector_runtime.v1"
        f"|scope={scope.content_sha256}"
        f"|repository={scope.repository_code_sha}"
        f"|policy_file={collector.policy_file_sha256}"
        f"|policy_canonical={collector.policy_canonical_sha256}"
        f"|transport={collector.policy.transport.canonical_sha256}"
    )


def _replace_once(text: str, old: str, new: str, field: str) -> str:
    if text.count(old) != 1:
        raise ValueError(f"{field} replacement authority drifted")
    return text.replace(old, new, 1)


def _render_corrected_project_code(
    *,
    loaded: LoadedDailySliceZeroOrderRevalidationPolicy,
    scope: collector_v1.QCQQQOptionsDerivedAggregateCollectorRunScope,
    project_root: Path,
) -> bytes:
    source_path = _bound_file(_PREDECESSOR_CODE_PATH, root=project_root, field="2519 code")
    source_bytes = _lf_bytes(source_path)
    if _sha256_bytes(source_bytes) != loaded.policy.predecessor.project_code_lf_sha256:
        raise ValueError("2519 project code LF identity drifted")
    collector = collector_v1.load_qc_qqq_options_primary_window_derived_aggregate_collector_policy(
        project_root=project_root
    )
    identity = _runtime_identity(scope=scope, collector=collector)
    lines = source_bytes.decode("utf-8").splitlines()
    identity_rows = [index for index, line in enumerate(lines) if line.startswith("IDENTITY = ")]
    if identity_rows != [10]:
        raise ValueError("2519 runtime identity row drifted")
    lines[identity_rows[0]] = f'IDENTITY = "{identity}"'
    text = "\n".join(lines) + "\n"
    text = _replace_once(
        text,
        "        self.set_cash(100000)\n        self.set_time_zone(TimeZones.NEW_YORK)",
        "        self.set_cash(100000)\n"
        "        self.settings.daily_precise_end_time = True\n"
        "        self.set_time_zone(TimeZones.NEW_YORK)",
        "daily precise end time",
    )
    text = _replace_once(
        text,
        "        self._invalid_sessions = set()\n        self._series = {}",
        "        self._invalid_sessions = set()\n"
        "        self._chain_sessions = set()\n"
        "        self._transport_rejected_sessions = set()\n"
        "        self._series = {}",
        "diagnostic session sets",
    )
    text = _replace_once(
        text,
        "    def on_data(self, data: Slice):\n"
        "        session = self.time.date()\n"
        "        session_id = session.isoformat()\n"
        "        if session_id not in self._expected_sessions:\n"
        "            self._invalid_sessions.add(session_id)\n"
        "            return\n"
        "        chain = data.option_chains.get(self._option)\n"
        "        if chain is None:\n"
        "            return\n"
        "        self._collect_session_chain(session, list(chain))",
        "    def on_data(self, data: Slice):\n"
        "        chain = data.option_chains.get(self._option)\n"
        "        if chain is None:\n"
        "            return\n"
        "        session = data.time.date()\n"
        "        session_id = session.isoformat()\n"
        "        if session_id not in self._expected_sessions:\n"
        "            self._invalid_sessions.add(session_id)\n"
        "            return\n"
        "        self._chain_sessions.add(session_id)\n"
        "        self._collect_session_chain(session, list(chain))",
        "canonical Slice delivery",
    )
    text = _replace_once(
        text,
        '            underlying = self._finite(self._attribute(contract, "underlying"))',
        "            underlying = self._finite(\n"
        '                self._attribute(contract, "underlying_last_price")\n'
        "            )",
        "OptionContract underlying accessor",
    )
    text = _replace_once(
        text,
        "        if not candidates:\n"
        "            self._invalid_sessions.add(session_id)\n"
        "            return",
        "        if not candidates:\n"
        "            self._transport_rejected_sessions.add(session_id)\n"
        "            self._invalid_sessions.add(session_id)\n"
        "            return",
        "candidate transport diagnostic",
    )
    text = _replace_once(
        text,
        "        if not positive_oi_values or not positive_volume_values:\n"
        "            self._invalid_sessions.add(session_id)\n"
        "            return",
        "        if not positive_oi_values or not positive_volume_values:\n"
        "            self._transport_rejected_sessions.add(session_id)\n"
        "            self._invalid_sessions.add(session_id)\n"
        "            return",
        "liquidity transport diagnostic",
    )
    text = _replace_once(
        text,
        '        self.set_runtime_statistic("TRADING2512_IDENTITY", IDENTITY)\n'
        "        self.set_runtime_statistic(\n"
        '            "TRADING2512_TERMINAL",',
        '        self.set_runtime_statistic("TRADING2512_IDENTITY", IDENTITY)\n'
        "        self.set_runtime_statistic(\n"
        '            "TRADING2520_DIAGNOSTIC",\n'
        '            "chain_sessions=" + str(len(self._chain_sessions))\n'
        '            + "|valid_candidate_sessions=" + str(len(self._seen_sessions))\n'
        '            + "|transport_rejected_sessions="\n'
        "            + str(len(self._transport_rejected_sessions))\n"
        '            + "|daily_precise_end_time=true"\n'
        '            + "|underlying_accessor=underlying_last_price",\n'
        "        )\n"
        "        self.set_runtime_statistic(\n"
        '            "TRADING2512_TERMINAL",',
        "export-safe diagnostics",
    )
    required = (
        "self.settings.daily_precise_end_time = True",
        "def on_data(self, data: Slice):",
        "data.option_chains.get(self._option)",
        "session = data.time.date()",
        'self._attribute(contract, "underlying_last_price")',
        '"TRADING2520_DIAGNOSTIC"',
    )
    prohibited = (
        "self.schedule.on(",
        "after_market_open",
        "self.option_chain(self._option)",
        'self._attribute(contract, "underlying")',
    )
    if not all(fragment in text for fragment in required) or any(
        fragment in text for fragment in prohibited
    ):
        raise ValueError("corrected project code invariants failed")
    return text.encode("utf-8")


def _build_proposal(
    *,
    loaded: LoadedDailySliceZeroOrderRevalidationPolicy,
    scope: collector_v1.QCQQQOptionsDerivedAggregateCollectorRunScope,
    code_bytes: bytes,
    created_at_utc: datetime,
) -> collector_v1.QCQQQOptionsDerivedAggregateCollectorProposal:
    authority = loaded.policy.collector_authority
    return collector_v1.QCQQQOptionsDerivedAggregateCollectorProposal.seal(
        schema_version="qc_qqq_options_derived_aggregate_collector_proposal.v1",
        proposal_id="trading-2520-daily-slice-zero-order-revalidation-v1",
        issued_at_utc=created_at_utc,
        run_scope=scope,
        collector_policy_file_sha256=authority.policy_file_sha256,
        collector_policy_canonical_sha256=authority.policy_canonical_sha256,
        transport_map_sha256=authority.transport_map_sha256,
        project_code_lf_sha256=_sha256_bytes(code_bytes),
        project_code_lf_byte_count=len(code_bytes),
        maximum_project_mutations=1,
        maximum_cloud_backtests=1,
        maximum_orders=0,
        maximum_fills=0,
        authorization_status="NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS",
        decision="OWNER_AUTHORIZATION_REQUIRED",
        allowed_actions=loaded.policy.allowed_actions_if_separately_authorized,
        prohibited_actions=loaded.policy.prohibited_actions,
        owner_policy_value_count=0,
        executable_policy_authorized=False,
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        external_action_performed=False,
        investment_interpretation_generated=False,
        production_effect="none",
        broker_action="none",
    )


def _owner_decision_request_bytes(
    *,
    loaded: LoadedDailySliceZeroOrderRevalidationPolicy,
    scope: collector_v1.QCQQQOptionsDerivedAggregateCollectorRunScope,
    proposal: collector_v1.QCQQQOptionsDerivedAggregateCollectorProposal,
    code_sha256: str,
) -> bytes:
    policy = loaded.policy
    text = f"""# TRADING-2520 daily Slice zero-order revalidation request

该文件只是未签署模板，不构成授权。2518 v3 token 已在首次 run attempt 后失效，不得复用。

离线排查确认 2519 除 scheduled callback 外还误读了 `contract.underlying`；本包改用 LEAN
`OptionContract.underlying_last_price`、显式 precise daily close Slice，并新增
export-safe 诊断计数。
这些修改尚未由 Cloud 运行验证。

```text
owner_decision:TRADING-2520:<YYYY-MM-DD>:authorize_single_zero_order_primary_window_daily_slice_revalidation_v4
ordinary_pushed_main_sha:<ORDINARY_PUSHED_MAIN_SHA>
registration_base_repository_code_sha:{policy.registration_base_repository_code_sha}
revalidation_policy_file_sha256:{loaded.policy_file_sha256}
revalidation_policy_canonical_sha256:{loaded.policy_canonical_sha256}
revalidation_package_manifest_file_sha256:<PACKAGE_MANIFEST_FILE_SHA256>
revalidation_package_manifest_content_sha256:<PACKAGE_MANIFEST_CONTENT_SHA256>
proposal_content_sha256:{proposal.content_sha256}
run_scope_content_sha256:{scope.content_sha256}
corrected_project_code_lf_sha256:{code_sha256}
predecessor_failed_backtest_id:{policy.predecessor.failed_backtest_id}
predecessor_failed_result_file_sha256:{policy.predecessor.failed_result_file_sha256}
target_project_id:{policy.run_scope.target_project_id}
requested_range:{policy.run_scope.requested_start.isoformat()}..{policy.run_scope.requested_end.isoformat()}
expected_session_count:{policy.run_scope.expected_session_count}
maximum_project_mutations:{policy.run_scope.maximum_project_mutations}
maximum_cloud_backtests:{policy.run_scope.maximum_cloud_backtests}
maximum_orders:{policy.run_scope.maximum_orders}
maximum_fills:{policy.run_scope.maximum_fills}
collector:codex_capability_coordinator
independent_reviewer:project_owner
authorization_expires_at_utc:<OWNER_SELECTED_EXPIRY_NOT_MORE_THAN_168_HOURS>
authorization_single_use:true
authorization_invalidates_after_first_run_attempt:true
```

新 token 到位前 external_action 必须保持 `none`；不得登录、修改项目、运行 Cloud backtest、
下载 raw options rows 或宣称 evidence/DQ/PIT PASS。
"""
    return text.replace("\r\n", "\n").replace("\r", "\n").encode("utf-8")


def build_qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_package(
    *,
    created_at_utc: datetime,
    project_root: Path = PROJECT_ROOT,
) -> BuiltDailySliceZeroOrderRevalidationPackage:
    root = project_root.resolve()
    created_at = _utc(created_at_utc, "created_at_utc")
    loaded = load_qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_policy(
        project_root=root
    )
    policy = loaded.policy
    scope = collector_v1.build_qc_qqq_options_derived_aggregate_collector_run_scope(
        run_scope_id="trading-2520-primary-window-daily-slice-revalidation-v1",
        created_at_utc=created_at,
        repository_code_sha=policy.registration_base_repository_code_sha,
        target_project_id=policy.run_scope.target_project_id,
        requested_end=policy.run_scope.requested_end,
        project_root=root,
    )
    if len(scope.session_ids) != policy.run_scope.expected_session_count:
        raise QCQQQOptionsDailySliceRevalidationError(
            "reviewed XNYS session count differs from 1202"
        )
    code_bytes = _render_corrected_project_code(loaded=loaded, scope=scope, project_root=root)
    proposal = _build_proposal(
        loaded=loaded,
        scope=scope,
        code_bytes=code_bytes,
        created_at_utc=created_at,
    )
    investigation = DailySliceRevalidationInvestigation.seal(
        schema_version="qc_qqq_options_daily_slice_revalidation_investigation.v1",
        investigation_id="trading-2520-daily-slice-root-cause-v1",
        task_id=_TASK_ID,
        created_at_utc=created_at,
        predecessor_project_code_lf_sha256=policy.predecessor.project_code_lf_sha256,
        predecessor_result_file_sha256=policy.predecessor.failed_result_file_sha256,
        hypothesis_ids=tuple(item.hypothesis_id for item in policy.root_cause_hypotheses),
        confirmed_code_defects=("OPTION_CONTRACT_UNDERLYING_ACCESSOR_MISMATCH",),
        unverified_cloud_hypotheses=(
            "DAILY_SLICE_DELIVERY",
            "DAILY_TIME_FRONTIER_IDENTITY",
            "TRANSPORT_AND_1202_SESSION_COVERAGE",
        ),
        official_sources=policy.official_sources,
        conclusion="OFFLINE_FIX_READY_NEW_OWNER_TOKEN_REQUIRED_BEFORE_ANY_EXTERNAL_ACTION",
        external_action_performed=False,
        evidence_status="FAIL",
        dq_pit_status="NOT_EVALUATED",
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
    )
    owner_request = _owner_decision_request_bytes(
        loaded=loaded,
        scope=scope,
        proposal=proposal,
        code_sha256=_sha256_bytes(code_bytes),
    )
    payloads = {
        "investigation.json": investigation.canonical_bytes(),
        "main.py": code_bytes,
        "owner_decision_request.md": owner_request,
        "proposal.json": proposal.canonical_bytes,
        "run_scope.json": scope.canonical_bytes,
    }
    roles = {
        "investigation.json": "INVESTIGATION",
        "main.py": "PROJECT_CODE",
        "owner_decision_request.md": "OWNER_DECISION_TEMPLATE",
        "proposal.json": "PROPOSAL",
        "run_scope.json": "RUN_SCOPE",
    }
    artifacts = tuple(
        RevalidationPackageArtifact(
            relative_path=relative,
            role=roles[relative],
            sha256=_sha256_bytes(payloads[relative]),
            byte_count=len(payloads[relative]),
        )
        for relative in _PACKAGE_INVENTORY
    )
    manifest = DailySliceRevalidationPackageManifest.seal(
        schema_version=(
            "qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_package.v1"
        ),
        package_id="TRADING_2520_PRIMARY_WINDOW_DAILY_SLICE_ZERO_ORDER_REVALIDATION_V1",
        task_id=_TASK_ID,
        created_at_utc=created_at,
        registration_base_repository_code_sha=policy.registration_base_repository_code_sha,
        policy_file_sha256=loaded.policy_file_sha256,
        policy_canonical_sha256=loaded.policy_canonical_sha256,
        run_scope_content_sha256=scope.content_sha256,
        proposal_content_sha256=proposal.content_sha256,
        project_code_lf_sha256=_sha256_bytes(code_bytes),
        project_code_lf_byte_count=len(code_bytes),
        investigation_content_sha256=investigation.content_sha256,
        artifacts=artifacts,
        external_action_performed=False,
        owner_authorization_status="NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS",
        evidence_status="FAIL",
        dq_pit_status="NOT_EVALUATED",
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
    )
    payloads["package_manifest.json"] = manifest.canonical_bytes()
    return BuiltDailySliceZeroOrderRevalidationPackage(
        policy=loaded,
        run_scope=scope,
        proposal=proposal,
        investigation=investigation,
        manifest=manifest,
        payloads=payloads,
    )


def write_qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_package(
    *,
    created_at_utc: datetime,
    project_root: Path = PROJECT_ROOT,
) -> DailySliceRevalidationPackageManifest:
    root = project_root.resolve()
    built = build_qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_package(
        created_at_utc=created_at_utc, project_root=root
    )
    target = (
        root
        / DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_ZERO_ORDER_REVALIDATION_PACKAGE_ROOT
    ).resolve()
    target.mkdir(parents=True, exist_ok=True)
    for name in _PACKAGE_FILES:
        write_bytes_atomic(target / name, built.payloads[name])
    return built.manifest


def load_qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_package(
    *,
    project_root: Path = PROJECT_ROOT,
) -> LoadedDailySliceZeroOrderRevalidationPackage:
    root = project_root.resolve()
    package_root = (
        root
        / DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_ZERO_ORDER_REVALIDATION_PACKAGE_ROOT
    ).resolve()
    if not package_root.is_dir() or package_root.is_symlink():
        raise QCQQQOptionsDailySliceRevalidationError("package root is unavailable or symlinked")
    entries = tuple(sorted(package_root.iterdir(), key=lambda item: item.name))
    if tuple(item.name for item in entries) != _PACKAGE_FILES:
        raise QCQQQOptionsDailySliceRevalidationError("package inventory is not exact")
    if any(not item.is_file() or item.is_symlink() for item in entries):
        raise QCQQQOptionsDailySliceRevalidationError(
            "package entries must be non-symlink regular files"
        )
    manifest_raw = (package_root / "package_manifest.json").read_bytes()
    manifest = DailySliceRevalidationPackageManifest.from_json_bytes(manifest_raw)
    if manifest_raw != manifest.canonical_bytes():
        raise QCQQQOptionsDailySliceRevalidationError("package manifest is not canonical")
    expected = build_qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_package(
        created_at_utc=manifest.created_at_utc, project_root=root
    )
    for name in _PACKAGE_FILES:
        if (package_root / name).read_bytes() != expected.payloads[name]:
            raise QCQQQOptionsDailySliceRevalidationError(
                f"package artifact differs from canonical build: {name}"
            )
    return LoadedDailySliceZeroOrderRevalidationPackage(
        policy=expected.policy,
        run_scope=expected.run_scope,
        proposal=expected.proposal,
        investigation=expected.investigation,
        manifest=expected.manifest,
        package_root=package_root,
    )


__all__ = [
    "DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_ZERO_ORDER_REVALIDATION_PACKAGE_ROOT",
    "DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_ZERO_ORDER_REVALIDATION_POLICY_PATH",
    "BuiltDailySliceZeroOrderRevalidationPackage",
    "DailySliceRevalidationInvestigation",
    "DailySliceRevalidationPackageManifest",
    "DailySliceZeroOrderRevalidationPolicy",
    "LoadedDailySliceZeroOrderRevalidationPackage",
    "LoadedDailySliceZeroOrderRevalidationPolicy",
    "QCQQQOptionsDailySliceRevalidationError",
    "RevalidationPackageArtifact",
    "build_qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_package",
    "load_qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_package",
    "load_qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_policy",
    "write_qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_package",
]
