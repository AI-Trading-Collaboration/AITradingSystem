"""TRADING-2519 daily-Slice failure evidence and successor package authority."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.qqq_options_research import (
    primary_window_export_safe_derived_aggregate_collector as collector_v1,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_POLICY_PATH = Path(
    "config/research/qc_qqq_options_primary_window_daily_slice_failure_fix_v1.yaml"
)
_TASK_ID = (
    "TRADING-2519_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SCHEDULE_"
    "RESULT_FAILURE_ADMISSION_V1"
)
_PACKAGE_FILES = ("failure_receipt.json", "main.py", "package_manifest.json", "result.json")
_PACKAGE_INVENTORY = ("failure_receipt.json", "main.py", "result.json")
_RUNTIME_IDENTITY = (
    "schema=qc_qqq_options_derived_aggregate_collector_runtime.v1"
    "|scope=80c11d7073dcc86f1297a34b3497fe705069619d6f1f51927ab9b673172db15e"
    "|repository=83d4f9680c4f78c7c1414659d51738ba7f615a7a"
    "|policy_file=48511cc64cab07b091787e2b0cb23354424248da66e7dba8866cd9ce9a766a8f"
    "|policy_canonical=3ebdd8a4dd89aad4584fbe8bffeeabb30d9b7bd2c28cd394c0fbc346939e999f"
    "|transport=60c970b71d3c47337fb76452d1384f2463079ef5026239e875e78b8c37d3eab5"
)
_RUNTIME_TERMINAL = (
    "status=INVALID_INCOMPLETE|observed_sessions=0|invalid_sessions=1202"
    "|orders=0|fills=0|portfolio_invested=false|raw_rows=false"
    "|log_data=false|object_store=false"
)


def _canonical_json_bytes(payload: object) -> bytes:
    def encode(value: object) -> object:
        if isinstance(value, BaseModel):
            return value.model_dump(mode="json")
        if isinstance(value, datetime):
            return value.isoformat().replace("+00:00", "Z")
        if isinstance(value, date):
            return value.isoformat()
        raise TypeError(f"unsupported canonical JSON value: {type(value).__name__}")

    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
        default=encode,
    ).encode("utf-8")


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


def _required(value: str, field: str) -> str:
    if not value or value != value.strip():
        raise ValueError(f"{field} must be non-empty and trimmed")
    return value


def _mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{field} must be an object")
    return value


def _bound_file(relative: str, *, root: Path, field: str) -> Path:
    path = Path(relative)
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


class RunFailureFacts(_FrozenModel):
    owner_decision_token: Literal[
        "owner_decision:TRADING-2518:2026-08-14:"
        "authorize_single_zero_order_primary_window_derived_aggregate_collection_v3"
    ]
    ordinary_pushed_main_sha: str
    registration_base_repository_code_sha: str
    failure_fix_policy_file_sha256: str
    failure_fix_policy_canonical_sha256: str
    failed_backtest_id: Literal["9518360aeb329219cd83e78442a1d229"]
    failed_run_receipt_content_sha256: str
    previous_run_attempt_consumption_content_sha256: str
    authorization_expires_at_utc: datetime
    target_project_id: Literal[34808569]
    collector: Literal["codex_capability_coordinator"]
    independent_reviewer: Literal["project_owner"]
    project_mutation_count: Literal[1]
    cloud_backtest_count: Literal[1]
    build_id: Literal["c87c22-be5a81"]
    backtest_id: Literal["b6d711f67a47199667c8a62f86208b28"]
    backtest_name: Literal["Muscular Fluorescent Orange Bat"]
    host_class: Literal["Community B-MICRO"]
    requested_start: date
    requested_end: date
    expected_session_count: Literal[1202]
    elapsed_seconds: Literal["900.27"]
    processed_data_points: Literal[38397482]
    processed_data_points_per_second: Literal[43000]
    result_file_sha256: str
    result_byte_count: Literal[813023]
    state_status: Literal["Completed"]
    observed_session_count: Literal[0]
    invalid_session_count: Literal[1202]
    orders: Literal[0]
    fills: Literal[0]
    total_fees: Literal["$0.00"]
    start_equity: Literal["100000"]
    end_equity: Literal["100000"]
    portfolio_invested: Literal[False]
    raw_option_rows: Literal[False]
    log_data: Literal[False]
    object_store: Literal[False]
    authorization_consumed: Literal[True]
    authorization_invalidated_after_first_run_attempt: Literal[True]
    evidence_admission_status: Literal["FAIL"]
    dq_pit_status: Literal["NOT_EVALUATED"]
    typed_reason: Literal["QC_DAILY_CHAIN_SCHEDULE_PRE_DATA_EMPTY"]

    @field_validator("ordinary_pushed_main_sha", "registration_base_repository_code_sha")
    @classmethod
    def _git_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _git_sha(value, info.field_name or "git_sha")

    @field_validator(
        "failure_fix_policy_file_sha256",
        "failure_fix_policy_canonical_sha256",
        "failed_run_receipt_content_sha256",
        "previous_run_attempt_consumption_content_sha256",
        "result_file_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, info.field_name or "sha256")

    @model_validator(mode="after")
    def _scope(self) -> Self:
        if self.requested_start != date(2021, 2, 22) or self.requested_end != date(
            2025, 12, 2
        ):
            raise ValueError("run failure must remain bound to the PRIMARY window")
        if self.authorization_expires_at_utc != datetime(2026, 8, 21, tzinfo=UTC):
            raise ValueError("v3 authorization expiry drifted")
        return self


class DailySliceFailureFixPolicy(_FrozenModel):
    schema_version: Literal["qc_qqq_options_primary_window_daily_slice_failure_fix_policy.v1"]
    policy_id: Literal[
        "TRADING_2519_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_FAILURE_FIX_V1"
    ]
    policy_version: Literal["1.0.0"]
    status: Literal["REVIEWED_FAILURE_FIX_BASELINE"]
    registration_base_repository_code_sha: str
    predecessor: dict[str, object]
    run_failure: RunFailureFacts
    successor: dict[str, object]
    safety: dict[str, object]

    @field_validator("registration_base_repository_code_sha")
    @classmethod
    def _base(cls, value: str) -> str:
        return _git_sha(value, "registration_base_repository_code_sha")

    @model_validator(mode="after")
    def _authority(self) -> Self:
        predecessor = self.predecessor
        if set(predecessor) != {
            "task_id",
            "package_manifest_file_sha256",
            "package_manifest_content_sha256",
            "project_code_lf_sha256",
        }:
            raise ValueError("predecessor authority inventory drifted")
        for field in (
            "package_manifest_file_sha256",
            "package_manifest_content_sha256",
            "project_code_lf_sha256",
        ):
            _sha256(str(predecessor[field]), f"predecessor.{field}")
        successor = self.successor
        if set(successor) != {
            "package_relative_path",
            "project_code_relative_path",
            "project_code_lf_sha256",
            "result_relative_path",
            "failure_receipt_relative_path",
            "package_manifest_relative_path",
            "trigger_authority",
            "prohibited_trigger_authority",
        }:
            raise ValueError("successor authority inventory drifted")
        _sha256(str(successor["project_code_lf_sha256"]), "successor code")
        if successor["trigger_authority"] != "CANONICAL_DAILY_SLICE_OPTION_CHAINS":
            raise ValueError("successor trigger authority drifted")
        if successor["prohibited_trigger_authority"] != (
            "SCHEDULED_AFTER_MARKET_OPEN_FOR_DAILY_CHAIN"
        ):
            raise ValueError("prohibited trigger authority drifted")
        if self.safety != {
            "external_action_performed_after_consumed_run": False,
            "further_cloud_run_authorized": False,
            "selection_authorized": False,
            "engine_status": "POLICY_BLOCKED_CASH_PRESERVATION",
            "investment_interpretation_authorized": False,
            "production_effect": "none",
            "broker_action": "none",
            "raw_option_rows_permitted": False,
            "thresholds_introduced": False,
        }:
            raise ValueError("failure-fix safety boundary drifted")
        return self

    @property
    def canonical_sha256(self) -> str:
        return _sha256_bytes(_canonical_json_bytes(self.model_dump(mode="json")))


class V3RunFailureReceipt(_FrozenModel):
    schema_version: Literal["qc_qqq_options_primary_window_daily_slice_failure_receipt.v1"]
    receipt_id: Literal["trading-2519-v3-daily-schedule-failure"]
    task_id: Literal[
        "TRADING-2519_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SCHEDULE_"
        "RESULT_FAILURE_ADMISSION_V1"
    ]
    observed_at_utc: datetime
    policy_file_sha256: str
    policy_canonical_sha256: str
    run_failure: RunFailureFacts
    runtime_identity: str
    runtime_terminal: str
    chart_present: Literal[True]
    chart_series_count: Literal[0]
    strict_admission_status: Literal["FAIL"]
    strict_admission_reason: Literal["COLLECTOR_RUNTIME_TERMINAL_INCOMPLETE"]
    local_derived_aggregate_dq_status: Literal["NOT_EVALUATED"]
    local_derived_aggregate_pit_status: Literal["NOT_EVALUATED"]
    option_event_dq_status: Literal["NOT_EVALUATED"]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    external_action_performed_after_consumed_run: Literal[False]
    further_cloud_run_authorized: Literal[False]
    investment_interpretation_generated: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]
    content_sha256: str

    @field_validator("policy_file_sha256", "policy_canonical_sha256", "content_sha256")
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, info.field_name or "sha256")

    @field_validator("observed_at_utc")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("observed_at_utc must be UTC")
        return value

    def semantic_payload(self) -> dict[str, object]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return _sha256_bytes(_canonical_json_bytes(self.semantic_payload()))

    @model_validator(mode="after")
    def _seal(self) -> Self:
        if self.runtime_identity != _RUNTIME_IDENTITY:
            raise ValueError("failure receipt runtime identity mismatch")
        if self.runtime_terminal != _RUNTIME_TERMINAL:
            raise ValueError("failure receipt runtime terminal mismatch")
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("failure receipt content_sha256 mismatch")
        return self

    @classmethod
    def seal(cls, **payload: object) -> Self:
        content_hash = _sha256_bytes(_canonical_json_bytes(payload))
        return cls.model_validate({**payload, "content_sha256": content_hash})

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        payload = collector_v1._duplicate_key_rejecting_json(raw)  # noqa: SLF001
        return cls.model_validate(payload)

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return _sha256_bytes(self.canonical_bytes())


class PackageArtifact(_FrozenModel):
    relative_path: Literal["failure_receipt.json", "main.py", "result.json"]
    role: Literal["FAILURE_RECEIPT", "SUCCESSOR_PROJECT_CODE", "QC_RESULTS_EXPORT_SAFE"]
    sha256: str
    byte_count: int = Field(ge=1)

    @field_validator("sha256")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _sha256(value, "artifact.sha256")


class DailySliceFailureFixPackageManifest(_FrozenModel):
    schema_version: Literal[
        "qc_qqq_options_primary_window_daily_slice_failure_fix_package.v1"
    ]
    package_id: Literal["TRADING_2519_PRIMARY_WINDOW_DAILY_SLICE_FAILURE_FIX_V1"]
    task_id: Literal[
        "TRADING-2519_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SCHEDULE_"
        "RESULT_FAILURE_ADMISSION_V1"
    ]
    created_at_utc: datetime
    policy_file_sha256: str
    policy_canonical_sha256: str
    result_file_sha256: str
    failure_receipt_content_sha256: str
    successor_project_code_lf_sha256: str
    artifacts: tuple[PackageArtifact, ...]
    external_action_performed_after_consumed_run: Literal[False]
    further_cloud_run_authorized: Literal[False]
    evidence_admission_status: Literal["FAIL"]
    dq_pit_status: Literal["NOT_EVALUATED"]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    content_sha256: str

    @field_validator(
        "policy_file_sha256",
        "policy_canonical_sha256",
        "result_file_sha256",
        "failure_receipt_content_sha256",
        "successor_project_code_lf_sha256",
        "content_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, info.field_name or "sha256")

    def semantic_payload(self) -> dict[str, object]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return _sha256_bytes(_canonical_json_bytes(self.semantic_payload()))

    @model_validator(mode="after")
    def _seal(self) -> Self:
        if tuple(item.relative_path for item in self.artifacts) != _PACKAGE_INVENTORY:
            raise ValueError("package artifact inventory/order drifted")
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("package manifest content_sha256 mismatch")
        return self

    @classmethod
    def seal(cls, **payload: object) -> Self:
        content_hash = _sha256_bytes(_canonical_json_bytes(payload))
        return cls.model_validate({**payload, "content_sha256": content_hash})

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        payload = collector_v1._duplicate_key_rejecting_json(raw)  # noqa: SLF001
        return cls.model_validate(payload)

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return _sha256_bytes(self.canonical_bytes())


@dataclass(frozen=True)
class LoadedDailySliceFailureFixPolicy:
    policy: DailySliceFailureFixPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str


@dataclass(frozen=True)
class LoadedDailySliceFailureFixPackage:
    policy: LoadedDailySliceFailureFixPolicy
    receipt: V3RunFailureReceipt
    manifest: DailySliceFailureFixPackageManifest
    package_root: Path


def load_qc_qqq_options_primary_window_daily_slice_failure_fix_policy(
    *,
    project_root: Path = PROJECT_ROOT,
    policy_path: Path = DEFAULT_POLICY_PATH,
) -> LoadedDailySliceFailureFixPolicy:
    root = project_root.resolve()
    path = _bound_file(policy_path.as_posix(), root=root, field="policy")
    raw = path.read_bytes()
    try:
        parsed = yaml.safe_load(raw.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ValueError("daily Slice failure-fix policy is invalid YAML") from exc
    policy = DailySliceFailureFixPolicy.model_validate(parsed)
    return LoadedDailySliceFailureFixPolicy(
        policy=policy,
        policy_path=path,
        policy_file_sha256=_sha256_bytes(raw),
        policy_canonical_sha256=policy.canonical_sha256,
    )


def _result_facts(*, result_bytes: bytes, policy: DailySliceFailureFixPolicy) -> None:
    facts = policy.run_failure
    if len(result_bytes) != facts.result_byte_count:
        raise ValueError("result byte count mismatch")
    if _sha256_bytes(result_bytes) != facts.result_file_sha256:
        raise ValueError("result file SHA-256 mismatch")
    payload = collector_v1._duplicate_key_rejecting_json(result_bytes)  # noqa: SLF001
    collector_v1._reject_prohibited_result_markers(payload)  # noqa: SLF001
    result = _mapping(payload, "result")
    state = _mapping(result.get("state"), "state")
    if state.get("Status") != "Completed" or state.get("RuntimeError") not in (None, ""):
        raise ValueError("result did not complete cleanly")
    if str(state.get("OrderCount")) != "0":
        raise ValueError("result state reports orders")
    hostname = state.get("Hostname")
    if not isinstance(hostname, str) or not hostname.endswith(facts.backtest_id):
        raise ValueError("result hostname/backtest identity mismatch")
    orders = result.get("orders")
    if not isinstance(orders, (dict, list)) or len(orders) != 0:
        raise ValueError("result contains orders")
    statistics = _mapping(result.get("statistics"), "statistics")
    if (
        str(statistics.get("Total Orders")) != "0"
        or statistics.get("Total Fees") != "$0.00"
        or str(statistics.get("Start Equity")) != "100000"
        or str(statistics.get("End Equity")) != "100000"
    ):
        raise ValueError("result cash-preservation statistics drifted")
    runtime = _mapping(result.get("runtimeStatistics"), "runtimeStatistics")
    if runtime.get("TRADING2512_IDENTITY") != _RUNTIME_IDENTITY:
        raise ValueError("result runtime identity mismatch")
    if runtime.get("TRADING2512_TERMINAL") != _RUNTIME_TERMINAL:
        raise ValueError("result runtime terminal failure mismatch")
    algorithm = _mapping(result.get("algorithmConfiguration"), "algorithmConfiguration")
    if not str(algorithm.get("startDate", "")).startswith("2021-02-22"):
        raise ValueError("result start range drifted")
    if not str(algorithm.get("endDate", "")).startswith("2025-12-02"):
        raise ValueError("result end range drifted")
    charts = _mapping(result.get("charts"), "charts")
    chart = _mapping(
        charts.get("TRADING2512_EXPORT_SAFE_DERIVED_AGGREGATES_V1"), "collector chart"
    )
    if chart.get("name") != "TRADING2512_EXPORT_SAFE_DERIVED_AGGREGATES_V1":
        raise ValueError("collector chart identity mismatch")
    series = _mapping(chart.get("series"), "collector chart series")
    if series:
        raise ValueError("invalid result unexpectedly contains aggregate series")


def build_v3_run_failure_receipt(
    *,
    result_bytes: bytes,
    observed_at_utc: datetime,
    project_root: Path = PROJECT_ROOT,
) -> V3RunFailureReceipt:
    loaded = load_qc_qqq_options_primary_window_daily_slice_failure_fix_policy(
        project_root=project_root
    )
    _result_facts(result_bytes=result_bytes, policy=loaded.policy)
    return V3RunFailureReceipt.seal(
        schema_version="qc_qqq_options_primary_window_daily_slice_failure_receipt.v1",
        receipt_id="trading-2519-v3-daily-schedule-failure",
        task_id=_TASK_ID,
        observed_at_utc=observed_at_utc,
        policy_file_sha256=loaded.policy_file_sha256,
        policy_canonical_sha256=loaded.policy_canonical_sha256,
        run_failure=loaded.policy.run_failure,
        runtime_identity=_RUNTIME_IDENTITY,
        runtime_terminal=_RUNTIME_TERMINAL,
        chart_present=True,
        chart_series_count=0,
        strict_admission_status="FAIL",
        strict_admission_reason="COLLECTOR_RUNTIME_TERMINAL_INCOMPLETE",
        local_derived_aggregate_dq_status="NOT_EVALUATED",
        local_derived_aggregate_pit_status="NOT_EVALUATED",
        option_event_dq_status="NOT_EVALUATED",
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        external_action_performed_after_consumed_run=False,
        further_cloud_run_authorized=False,
        investment_interpretation_generated=False,
        production_effect="none",
        broker_action="none",
    )


def _verify_successor_code(
    *,
    code_path: Path,
    successor: dict[str, object],
) -> bytes:
    code_lf = _lf_bytes(code_path)
    if _sha256_bytes(code_lf) != successor["project_code_lf_sha256"]:
        raise ValueError("successor project code LF SHA-256 mismatch")
    code_text = code_lf.decode("utf-8")
    required_fragments = (
        "def on_data(self, data: Slice):",
        "data.option_chains.get(self._option)",
        "self._collect_session_chain(session, list(chain))",
    )
    if not all(fragment in code_text for fragment in required_fragments):
        raise ValueError("successor does not use canonical daily Slice delivery")
    prohibited_fragments = (
        "self.schedule.on(",
        "after_market_open",
        "self.option_chain(self._option)",
    )
    if any(fragment in code_text for fragment in prohibited_fragments):
        raise ValueError("scheduled daily-chain authority remains in successor")
    return code_lf


def _build_package_manifest(
    *,
    loaded: LoadedDailySliceFailureFixPolicy,
    receipt: V3RunFailureReceipt,
    code_bytes: bytes,
    result_bytes: bytes,
    created_at_utc: datetime,
) -> DailySliceFailureFixPackageManifest:
    raw = {
        "failure_receipt.json": receipt.canonical_bytes(),
        "main.py": code_bytes,
        "result.json": result_bytes,
    }
    roles = {
        "failure_receipt.json": "FAILURE_RECEIPT",
        "main.py": "SUCCESSOR_PROJECT_CODE",
        "result.json": "QC_RESULTS_EXPORT_SAFE",
    }
    artifacts = tuple(
        PackageArtifact.model_validate(
            {
                "relative_path": relative,
                "role": roles[relative],
                "sha256": _sha256_bytes(raw[relative]),
                "byte_count": len(raw[relative]),
            }
        )
        for relative in _PACKAGE_INVENTORY
    )
    return DailySliceFailureFixPackageManifest.seal(
        schema_version="qc_qqq_options_primary_window_daily_slice_failure_fix_package.v1",
        package_id="TRADING_2519_PRIMARY_WINDOW_DAILY_SLICE_FAILURE_FIX_V1",
        task_id=_TASK_ID,
        created_at_utc=created_at_utc,
        policy_file_sha256=loaded.policy_file_sha256,
        policy_canonical_sha256=loaded.policy_canonical_sha256,
        result_file_sha256=loaded.policy.run_failure.result_file_sha256,
        failure_receipt_content_sha256=receipt.content_sha256,
        successor_project_code_lf_sha256=str(
            loaded.policy.successor["project_code_lf_sha256"]
        ),
        artifacts=artifacts,
        external_action_performed_after_consumed_run=False,
        further_cloud_run_authorized=False,
        evidence_admission_status="FAIL",
        dq_pit_status="NOT_EVALUATED",
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
    )


def write_qc_qqq_options_primary_window_daily_slice_failure_fix_package(
    *,
    observed_at_utc: datetime,
    project_root: Path = PROJECT_ROOT,
) -> DailySliceFailureFixPackageManifest:
    root = project_root.resolve()
    loaded = load_qc_qqq_options_primary_window_daily_slice_failure_fix_policy(
        project_root=root
    )
    successor = loaded.policy.successor
    package_root = (root / str(successor["package_relative_path"])).resolve()
    package_root.mkdir(parents=True, exist_ok=True)
    result_path = _bound_file(
        str(successor["result_relative_path"]), root=root, field="result artifact"
    )
    code_path = _bound_file(
        str(successor["project_code_relative_path"]), root=root, field="successor code"
    )
    result_bytes = result_path.read_bytes()
    _result_facts(result_bytes=result_bytes, policy=loaded.policy)
    code_bytes = _verify_successor_code(code_path=code_path, successor=successor)
    receipt = build_v3_run_failure_receipt(
        result_bytes=result_bytes,
        observed_at_utc=observed_at_utc,
        project_root=root,
    )
    manifest = _build_package_manifest(
        loaded=loaded,
        receipt=receipt,
        code_bytes=code_bytes,
        result_bytes=result_bytes,
        created_at_utc=observed_at_utc,
    )
    write_bytes_atomic(package_root / "failure_receipt.json", receipt.canonical_bytes())
    write_bytes_atomic(package_root / "package_manifest.json", manifest.canonical_bytes())
    return manifest


def build_qc_qqq_options_primary_window_daily_slice_failure_fix_package(
    *, project_root: Path = PROJECT_ROOT
) -> LoadedDailySliceFailureFixPackage:
    root = project_root.resolve()
    loaded = load_qc_qqq_options_primary_window_daily_slice_failure_fix_policy(
        project_root=root
    )
    successor = loaded.policy.successor
    package_root = (root / str(successor["package_relative_path"])).resolve()
    result_path = _bound_file(
        str(successor["result_relative_path"]), root=root, field="result artifact"
    )
    code_path = _bound_file(
        str(successor["project_code_relative_path"]), root=root, field="successor code"
    )
    receipt_path = _bound_file(
        str(successor["failure_receipt_relative_path"]), root=root, field="failure receipt"
    )
    manifest_path = _bound_file(
        str(successor["package_manifest_relative_path"]), root=root, field="package manifest"
    )
    inventory = tuple(sorted(path.name for path in package_root.iterdir()))
    if inventory != _PACKAGE_FILES:
        raise ValueError("package file inventory is not exact")
    if any(not path.is_file() or path.is_symlink() for path in package_root.iterdir()):
        raise ValueError("package entries must be non-symlink regular files")
    code_lf = _verify_successor_code(code_path=code_path, successor=successor)
    result_bytes = result_path.read_bytes()
    receipt_raw = receipt_path.read_bytes()
    receipt = V3RunFailureReceipt.from_json_bytes(receipt_raw)
    if receipt_raw != receipt.canonical_bytes():
        raise ValueError("failure receipt is not exact canonical JSON")
    expected_receipt = build_v3_run_failure_receipt(
        result_bytes=result_bytes,
        observed_at_utc=receipt.observed_at_utc,
        project_root=root,
    )
    if receipt.canonical_bytes() != expected_receipt.canonical_bytes():
        raise ValueError("failure receipt differs from canonical result facts")
    manifest_raw = manifest_path.read_bytes()
    manifest = DailySliceFailureFixPackageManifest.from_json_bytes(manifest_raw)
    if manifest_raw != manifest.canonical_bytes():
        raise ValueError("package manifest is not exact canonical JSON")
    expected_manifest = _build_package_manifest(
        loaded=loaded,
        receipt=receipt,
        code_bytes=code_lf,
        result_bytes=result_bytes,
        created_at_utc=manifest.created_at_utc,
    )
    if manifest.canonical_bytes() != expected_manifest.canonical_bytes():
        raise ValueError("package manifest differs from canonical package")
    return LoadedDailySliceFailureFixPackage(
        policy=loaded,
        receipt=receipt,
        manifest=manifest,
        package_root=package_root,
    )


__all__ = [
    "DEFAULT_POLICY_PATH",
    "DailySliceFailureFixPackageManifest",
    "DailySliceFailureFixPolicy",
    "LoadedDailySliceFailureFixPackage",
    "LoadedDailySliceFailureFixPolicy",
    "PackageArtifact",
    "RunFailureFacts",
    "V3RunFailureReceipt",
    "build_qc_qqq_options_primary_window_daily_slice_failure_fix_package",
    "build_v3_run_failure_receipt",
    "load_qc_qqq_options_primary_window_daily_slice_failure_fix_policy",
    "write_qc_qqq_options_primary_window_daily_slice_failure_fix_package",
]
