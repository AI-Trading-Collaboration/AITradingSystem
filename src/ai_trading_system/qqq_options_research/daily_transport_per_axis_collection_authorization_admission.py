from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.qqq_options_research.daily_transport_per_axis_collection_proposal import (
    load_per_axis_collection_proposal_package,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_POLICY_PATH = Path(
    "config/research/"
    "qc_qqq_options_daily_transport_per_axis_collection_authorization_admission_v1.yaml"
)

_OWNER_DECISION = (
    "owner_decision:TRADING-2529:2026-08-16:"
    "authorize_single_zero_order_daily_transport_per_axis_export_safe_aggregate_collection_v1"
)
_FIELD_ORDER = (
    "ordinary_pushed_main_sha",
    "registration_base_repository_code_sha",
    "proposal_policy_file_sha256",
    "proposal_policy_canonical_sha256",
    "source_diagnostic_content_sha256",
    "source_diagnostic_canonical_sha256",
    "run_scope_content_sha256",
    "run_scope_canonical_sha256",
    "proposal_content_sha256",
    "proposal_canonical_sha256",
    "project_code_lf_sha256",
    "target_project_id",
    "requested_range",
    "expected_session_count",
    "maximum_project_mutations",
    "maximum_cloud_backtests",
    "maximum_orders",
    "maximum_fills",
    "collector",
    "independent_reviewer",
    "authorization_expires_at_utc",
    "authorization_single_use",
    "authorization_invalidates_on_first_run_attempt",
)
_AXES = (
    "OPTION_CHAIN_PRESENCE",
    "UNDERLYING_PRICE",
    "BID_ASK_QUOTE",
    "GREEKS",
    "IMPLIED_VOLATILITY",
    "OPEN_INTEREST",
    "VOLUME",
    "CROSS_FIELD_CONSISTENCY",
)
_STATUSES = ("PRESENT", "MISSING", "INVALID", "NOT_EVALUATED")
_AGGREGATE_KEYS = tuple(
    f"TRADING2529_{axis}_{status}_SESSIONS" for axis in _AXES for status in _STATUSES
)
_PROHIBITED_RESULT_MARKER_KEYS = frozenset(
    {
        "datasetrows",
        "logs",
        "logdata",
        "objectstore",
        "optionrows",
        "rawoptionrows",
        "rawoptionsdata",
        "rawrows",
    }
)


class PerAxisCollectionAuthorizationAdmissionError(ValueError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(code if not detail else f"{code}: {detail}")
        self.code = code
        self.detail = detail


def _fail(code: str, detail: str = "") -> None:
    raise PerAxisCollectionAuthorizationAdmissionError(code, detail)


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _content_sha256(payload: Mapping[str, Any]) -> str:
    body = dict(payload)
    body.pop("content_sha256", None)
    return sha256(_canonical_bytes(body)).hexdigest()


def _parse_utc(value: str, *, field: str) -> datetime:
    if not value.endswith("Z"):
        _fail("PER_AXIS_AUTHORIZATION_UTC_REQUIRED", field)
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        _fail("PER_AXIS_AUTHORIZATION_TIME_INVALID", field)
        raise AssertionError from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        _fail("PER_AXIS_AUTHORIZATION_UTC_REQUIRED", field)
    return parsed.astimezone(UTC)


def _load_policy(project_root: Path) -> dict[str, Any]:
    path = (project_root / DEFAULT_POLICY_PATH).resolve()
    try:
        raw = path.read_bytes()
        payload = yaml.safe_load(raw)
    except (OSError, yaml.YAMLError) as exc:
        _fail("PER_AXIS_AUTHORIZATION_POLICY_LOAD_FAILED", str(exc))
    if not isinstance(payload, dict):
        _fail("PER_AXIS_AUTHORIZATION_POLICY_MAPPING_REQUIRED")
    return payload


@dataclass(frozen=True)
class PerAxisOwnerAuthorizationAdmissionReceipt:
    payload: Mapping[str, Any]

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.payload)

    @property
    def content_sha256(self) -> str:
        return _content_sha256(self.payload)


@dataclass(frozen=True)
class PerAxisRunAttemptConsumptionReceipt:
    payload: Mapping[str, Any]

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.payload)

    @property
    def content_sha256(self) -> str:
        return _content_sha256(self.payload)


@dataclass(frozen=True)
class PerAxisExportSafeAggregateEvidence:
    payload: Mapping[str, Any]

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.payload)

    @property
    def content_sha256(self) -> str:
        return _content_sha256(self.payload)


@dataclass(frozen=True)
class PerAxisExternalActionLedger:
    payload: Mapping[str, Any]

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.payload)

    @property
    def content_sha256(self) -> str:
        return _content_sha256(self.payload)


@dataclass(frozen=True)
class PerAxisResultDownloadDeliveryIncident:
    payload: Mapping[str, Any]

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.payload)

    @property
    def content_sha256(self) -> str:
        return _content_sha256(self.payload)


@dataclass(frozen=True)
class PerAxisCollectionExecutionEvidenceManifest:
    payload: Mapping[str, Any]

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.payload)

    @property
    def content_sha256(self) -> str:
        return _content_sha256(self.payload)


def _expected_fields(*, policy: Mapping[str, Any], package: Any) -> dict[str, str]:
    proposal = package.proposal
    scope = package.run_scope
    return {
        "ordinary_pushed_main_sha": str(policy["ordinary_pushed_main_sha"]),
        "registration_base_repository_code_sha": str(
            policy["registration_base_repository_code_sha"]
        ),
        "proposal_policy_file_sha256": proposal.proposal_policy_file_sha256,
        "proposal_policy_canonical_sha256": proposal.proposal_policy_canonical_sha256,
        "source_diagnostic_content_sha256": proposal.source_diagnostic_content_sha256,
        "source_diagnostic_canonical_sha256": proposal.source_diagnostic_canonical_sha256,
        "run_scope_content_sha256": scope.content_sha256,
        "run_scope_canonical_sha256": scope.canonical_sha256,
        "proposal_content_sha256": proposal.content_sha256,
        "proposal_canonical_sha256": proposal.canonical_sha256,
        "project_code_lf_sha256": proposal.project_code_lf_sha256,
        "target_project_id": str(policy["target_project_id"]),
        "requested_range": f"{policy['requested_start']}..{policy['requested_end']}",
        "expected_session_count": str(policy["expected_session_count"]),
        "maximum_project_mutations": str(policy["maximum_project_mutations"]),
        "maximum_cloud_backtests": str(policy["maximum_cloud_backtests"]),
        "maximum_orders": str(policy["maximum_orders"]),
        "maximum_fills": str(policy["maximum_fills"]),
        "collector": str(policy["collector_id"]),
        "independent_reviewer": str(policy["independent_reviewer_id"]),
        "authorization_expires_at_utc": str(policy["authorization_expires_at_utc"]),
        "authorization_single_use": "true",
        "authorization_invalidates_on_first_run_attempt": "true",
    }


def _parse_exact_token(token_bytes: bytes) -> tuple[str, dict[str, str]]:
    if token_bytes.endswith(b"\n") or b"\r" in token_bytes:
        _fail("PER_AXIS_AUTHORIZATION_TOKEN_CANONICAL_LF_REQUIRED")
    try:
        text = token_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        _fail("PER_AXIS_AUTHORIZATION_TOKEN_UTF8_REQUIRED", str(exc))
    lines = text.split("\n")
    if len(lines) != 1 + len(_FIELD_ORDER) or lines[0] != _OWNER_DECISION:
        _fail("PER_AXIS_AUTHORIZATION_DECISION_OR_LINE_COUNT_INVALID")
    fields: dict[str, str] = {}
    observed_order: list[str] = []
    for line in lines[1:]:
        if ":" not in line:
            _fail("PER_AXIS_AUTHORIZATION_FIELD_INVALID", line)
        key, value = line.split(":", 1)
        if not key or not value or key in fields:
            _fail("PER_AXIS_AUTHORIZATION_FIELD_INVALID", key)
        observed_order.append(key)
        fields[key] = value
    if tuple(observed_order) != _FIELD_ORDER:
        _fail("PER_AXIS_AUTHORIZATION_FIELD_ORDER_INVALID")
    return text, fields


def admit_per_axis_collection_owner_authorization(
    *,
    owner_token_bytes: bytes,
    owner_token_source: str,
    reviewed_at_utc: str,
    local_main_sha: str,
    origin_main_sha: str,
    project_root: Path = PROJECT_ROOT,
) -> PerAxisOwnerAuthorizationAdmissionReceipt:
    root = project_root.resolve()
    policy = _load_policy(root)
    package = load_per_axis_collection_proposal_package(project_root=root)
    _text, fields = _parse_exact_token(owner_token_bytes)
    token_sha = sha256(owner_token_bytes).hexdigest()
    if token_sha != policy["owner_token_sha256"] or len(owner_token_bytes) != int(
        policy["owner_token_byte_count"]
    ):
        _fail("PER_AXIS_AUTHORIZATION_TOKEN_IDENTITY_MISMATCH")
    if owner_token_source != policy["owner_token_source"]:
        _fail("PER_AXIS_AUTHORIZATION_SOURCE_INVALID")
    if (
        local_main_sha != policy["ordinary_pushed_main_sha"]
        or origin_main_sha != policy["ordinary_pushed_main_sha"]
    ):
        _fail("PER_AXIS_AUTHORIZATION_PUBLISHED_MAIN_MISMATCH")
    expected = _expected_fields(policy=policy, package=package)
    if fields != expected:
        wrong = sorted(key for key in expected if fields.get(key) != expected[key])
        _fail("PER_AXIS_AUTHORIZATION_SCOPE_OR_HASH_MISMATCH", ",".join(wrong))

    reviewed_at = _parse_utc(reviewed_at_utc, field="reviewed_at_utc")
    expires_at = _parse_utc(fields["authorization_expires_at_utc"], field="expiry")
    decision_start = datetime.fromisoformat(str(policy["owner_decision_date"])).replace(tzinfo=UTC)
    if expires_at <= decision_start or expires_at > decision_start + timedelta(
        hours=int(policy["authorization_expires_after_hours_maximum"])
    ):
        _fail("PER_AXIS_AUTHORIZATION_EXPIRY_OUTSIDE_REVIEWED_WINDOW")
    if reviewed_at < decision_start or reviewed_at >= expires_at:
        _fail("PER_AXIS_AUTHORIZATION_REVIEW_TIME_OUTSIDE_WINDOW")

    receipt_body: dict[str, Any] = {
        "schema_version": (
            "qc_qqq_options_daily_transport_per_axis_owner_authorization_admission_receipt.v1"
        ),
        "task_id": policy["task_id"],
        "predecessor_task_id": policy["predecessor_task_id"],
        "status": "OWNER_AUTHORIZATION_ADMITTED_UNUSED",
        "owner_token_source": owner_token_source,
        "owner_token_sha256": token_sha,
        "owner_token_byte_count": len(owner_token_bytes),
        "reviewed_at_utc": reviewed_at_utc,
        "expires_at_utc": fields["authorization_expires_at_utc"],
        "ordinary_pushed_main_sha": local_main_sha,
        "proposal_package_manifest_sha256": package.manifest.canonical_sha256,
        "proposal_content_sha256": package.proposal.content_sha256,
        "run_scope_content_sha256": package.run_scope.content_sha256,
        "project_code_lf_sha256": package.proposal.project_code_lf_sha256,
        "target_project_id": int(fields["target_project_id"]),
        "requested_range": fields["requested_range"],
        "expected_session_count": int(fields["expected_session_count"]),
        "maximum_project_mutations": 1,
        "maximum_cloud_backtests": 1,
        "maximum_orders": 0,
        "maximum_fills": 0,
        "authorization_single_use": True,
        "authorization_invalidates_on_first_run_attempt": True,
        "authorization_consumed": False,
        "external_action_performed": False,
        "selection_authorized": False,
        "engine_status": "POLICY_BLOCKED_CASH_PRESERVATION",
        "production_effect": "none",
        "broker_action": "none",
    }
    receipt_body["content_sha256"] = _content_sha256(receipt_body)
    return PerAxisOwnerAuthorizationAdmissionReceipt(receipt_body)


def consume_on_first_cloud_run_attempt(
    *,
    admission: PerAxisOwnerAuthorizationAdmissionReceipt,
    attempted_at_utc: str,
    project_id: int,
    project_code_lf_sha256: str,
    backtest_id: str,
    attempt_status: str,
    prior_consumption: PerAxisRunAttemptConsumptionReceipt | None = None,
) -> PerAxisRunAttemptConsumptionReceipt:
    if prior_consumption is not None:
        _fail("PER_AXIS_AUTHORIZATION_ALREADY_CONSUMED")
    admitted = admission.payload
    if admitted.get("status") != "OWNER_AUTHORIZATION_ADMITTED_UNUSED" or admitted.get(
        "authorization_consumed"
    ):
        _fail("PER_AXIS_AUTHORIZATION_NOT_UNUSED")
    attempted_at = _parse_utc(attempted_at_utc, field="attempted_at_utc")
    if attempted_at >= _parse_utc(str(admitted["expires_at_utc"]), field="expiry"):
        _fail("PER_AXIS_AUTHORIZATION_EXPIRED_BEFORE_RUN_ATTEMPT")
    if project_id != admitted["target_project_id"]:
        _fail("PER_AXIS_AUTHORIZATION_PROJECT_MISMATCH")
    if project_code_lf_sha256 != admitted["project_code_lf_sha256"]:
        _fail("PER_AXIS_AUTHORIZATION_PROJECT_CODE_MISMATCH")
    if not backtest_id or attempt_status not in {"SUBMITTED", "COMPLETED", "FAILED"}:
        _fail("PER_AXIS_AUTHORIZATION_RUN_ATTEMPT_INVALID")
    body: dict[str, Any] = {
        "schema_version": (
            "qc_qqq_options_daily_transport_per_axis_run_attempt_consumption_receipt.v1"
        ),
        "task_id": admitted["task_id"],
        "authorization_admission_content_sha256": admission.content_sha256,
        "attempted_at_utc": attempted_at_utc,
        "project_id": project_id,
        "project_code_lf_sha256": project_code_lf_sha256,
        "backtest_id": backtest_id,
        "attempt_status": attempt_status,
        "cloud_backtest_attempt_count": 1,
        "maximum_cloud_backtests": 1,
        "orders": 0,
        "fills": 0,
        "authorization_consumed": True,
        "authorization_invalidated_for_further_cloud_runs": True,
        "second_cloud_run_authorized": False,
        "selection_authorized": False,
        "engine_status": "POLICY_BLOCKED_CASH_PRESERVATION",
        "production_effect": "none",
        "broker_action": "none",
    }
    body["content_sha256"] = _content_sha256(body)
    return PerAxisRunAttemptConsumptionReceipt(body)


def _duplicate_key_rejecting_json(raw: bytes) -> dict[str, Any]:
    def hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                _fail("PER_AXIS_RESULTS_DUPLICATE_JSON_KEY", key)
            result[key] = value
        return result

    try:
        payload = json.loads(raw.decode("utf-8"), object_pairs_hook=hook)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        _fail("PER_AXIS_RESULTS_JSON_INVALID", str(exc))
    if not isinstance(payload, dict):
        _fail("PER_AXIS_RESULTS_OBJECT_REQUIRED")
    return payload


def _reject_prohibited_result_markers(value: object, *, path: str = "result") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = re.sub(r"[^a-z0-9]", "", str(key).casefold())
            if normalized in _PROHIBITED_RESULT_MARKER_KEYS:
                _fail("PER_AXIS_RESULTS_PROHIBITED_CARRIER", f"{path}.{key}")
            _reject_prohibited_result_markers(child, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_prohibited_result_markers(child, path=f"{path}[{index}]")


def _mapping(value: object, field: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        _fail("PER_AXIS_RESULTS_MAPPING_REQUIRED", field)
    return value


def _zero_currency(value: object, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        _fail("PER_AXIS_RESULTS_ZERO_VALUE_REQUIRED", field)
    rendered = str(value).strip().replace("$", "").replace(",", "")
    try:
        parsed = float(rendered)
    except ValueError:
        _fail("PER_AXIS_RESULTS_ZERO_VALUE_REQUIRED", field)
    if parsed != 0:
        _fail("PER_AXIS_RESULTS_ZERO_VALUE_REQUIRED", field)


def _parse_terminal(value: object, *, expected_sessions: int) -> dict[str, str]:
    if not isinstance(value, str):
        _fail("PER_AXIS_RESULTS_TERMINAL_INVALID")
    fields: dict[str, str] = {}
    for part in value.split("|"):
        if "=" not in part:
            _fail("PER_AXIS_RESULTS_TERMINAL_INVALID")
        key, item = part.split("=", 1)
        if not key or key in fields:
            _fail("PER_AXIS_RESULTS_TERMINAL_INVALID")
        fields[key] = item
    expected_keys = {
        "status",
        "expected_sessions",
        "observed_sessions",
        "orders",
        "fills",
        "portfolio_invested",
        "raw_rows",
        "logs_as_data",
        "object_store",
    }
    if set(fields) != expected_keys:
        _fail("PER_AXIS_RESULTS_TERMINAL_INVALID")
    try:
        observed_sessions = int(fields["observed_sessions"])
    except ValueError:
        _fail("PER_AXIS_RESULTS_TERMINAL_INVALID")
    if (
        fields["status"] != "COMPLETE"
        or fields["expected_sessions"] != str(expected_sessions)
        or not 0 <= observed_sessions <= expected_sessions
        or fields["orders"] != "0"
        or fields["fills"] != "0"
        or fields["portfolio_invested"] != "false"
        or fields["raw_rows"] != "false"
        or fields["logs_as_data"] != "false"
        or fields["object_store"] != "false"
    ):
        _fail("PER_AXIS_RESULTS_TERMINAL_INVALID")
    return fields


def validate_per_axis_results_json(
    *,
    result_bytes: bytes,
    admission: PerAxisOwnerAuthorizationAdmissionReceipt,
    consumption: PerAxisRunAttemptConsumptionReceipt,
    collected_at_utc: str,
    backtest_id: str,
    project_root: Path = PROJECT_ROOT,
) -> PerAxisExportSafeAggregateEvidence:
    admitted = admission.payload
    consumed = consumption.payload
    if (
        admitted.get("status") != "OWNER_AUTHORIZATION_ADMITTED_UNUSED"
        or consumed.get("authorization_consumed") is not True
        or consumed.get("backtest_id") != backtest_id
        or consumed.get("project_id") != admitted.get("target_project_id")
        or consumed.get("project_code_lf_sha256") != admitted.get("project_code_lf_sha256")
    ):
        _fail("PER_AXIS_RESULTS_CONSUMPTION_BINDING_INVALID")
    collected_at = _parse_utc(collected_at_utc, field="collected_at_utc")
    attempted_at = _parse_utc(str(consumed["attempted_at_utc"]), field="attempted_at_utc")
    if collected_at < attempted_at:
        _fail("PER_AXIS_RESULTS_COLLECTION_PRECEDES_RUN_ATTEMPT")

    payload = _duplicate_key_rejecting_json(result_bytes)
    _reject_prohibited_result_markers(payload)
    state = _mapping(payload.get("state"), "state")
    if (
        state.get("Status") != "Completed"
        or state.get("RuntimeError") not in (None, "")
        or str(state.get("OrderCount")) != "0"
    ):
        _fail("PER_AXIS_RESULTS_STATE_INVALID")
    hostname = state.get("Hostname")
    if hostname is not None and (
        not isinstance(hostname, str) or not hostname.endswith(backtest_id)
    ):
        _fail("PER_AXIS_RESULTS_BACKTEST_IDENTITY_INVALID")
    orders = payload.get("orders")
    if not isinstance(orders, (dict, list)) or len(orders) != 0:
        _fail("PER_AXIS_RESULTS_ORDERS_NOT_EMPTY")
    statistics = _mapping(payload.get("statistics"), "statistics")
    if str(statistics.get("Total Orders")) != "0":
        _fail("PER_AXIS_RESULTS_ORDERS_NOT_ZERO")
    _zero_currency(statistics.get("Total Fees"), "statistics.Total Fees")
    algorithm = _mapping(payload.get("algorithmConfiguration"), "algorithmConfiguration")
    if not str(algorithm.get("startDate", "")).startswith("2021-02-22") or not str(
        algorithm.get("endDate", "")
    ).startswith("2025-12-02"):
        _fail("PER_AXIS_RESULTS_RANGE_INVALID")

    policy = _load_policy(project_root.resolve())
    package = load_per_axis_collection_proposal_package(project_root=project_root.resolve())
    runtime_identity = (
        "schema=qc_qqq_options_daily_transport_per_axis_runtime.v1"
        f"|scope={package.run_scope.content_sha256}"
        f"|repository={policy['registration_base_repository_code_sha']}"
        f"|source_diagnostic={package.proposal.source_diagnostic_content_sha256}"
    )
    runtime = _mapping(payload.get("runtimeStatistics"), "runtimeStatistics")
    trading_keys = {key for key in runtime if key.startswith("TRADING2529_")}
    expected_trading_keys = set(_AGGREGATE_KEYS) | {
        "TRADING2529_IDENTITY",
        "TRADING2529_TERMINAL",
    }
    if trading_keys != expected_trading_keys:
        _fail("PER_AXIS_RESULTS_AGGREGATE_KEYSET_INVALID")
    if runtime.get("TRADING2529_IDENTITY") != runtime_identity:
        _fail("PER_AXIS_RESULTS_RUNTIME_IDENTITY_INVALID")
    expected_sessions = int(admitted["expected_session_count"])
    terminal = _parse_terminal(
        runtime.get("TRADING2529_TERMINAL"), expected_sessions=expected_sessions
    )
    counts: dict[str, int] = {}
    for key in _AGGREGATE_KEYS:
        value = runtime.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            _fail("PER_AXIS_RESULTS_AGGREGATE_COUNT_INVALID", key)
        rendered = str(value)
        if not rendered.isascii() or not rendered.isdigit():
            _fail("PER_AXIS_RESULTS_AGGREGATE_COUNT_INVALID", key)
        counts[key] = int(rendered)
    axis_totals = {
        axis: sum(counts[f"TRADING2529_{axis}_{status}_SESSIONS"] for status in _STATUSES)
        for axis in _AXES
    }
    if any(total != expected_sessions for total in axis_totals.values()):
        _fail("PER_AXIS_RESULTS_AXIS_TOTAL_INVALID")

    body: dict[str, Any] = {
        "schema_version": (
            "qc_qqq_options_daily_transport_per_axis_export_safe_aggregate_evidence.v1"
        ),
        "task_id": admitted["task_id"],
        "status": "EXPORT_SAFE_PER_AXIS_AGGREGATES_COLLECTED",
        "authorization_admission_content_sha256": admission.content_sha256,
        "run_attempt_consumption_content_sha256": consumption.content_sha256,
        "collected_at_utc": collected_at_utc,
        "source_carrier": "MANUAL_DOWNLOAD_RESULTS_JSON",
        "source_result_file_sha256": sha256(result_bytes).hexdigest(),
        "source_result_byte_count": len(result_bytes),
        "backtest_id": backtest_id,
        "project_id": admitted["target_project_id"],
        "project_code_lf_sha256": admitted["project_code_lf_sha256"],
        "requested_range": admitted["requested_range"],
        "expected_session_count": expected_sessions,
        "runtime_identity": runtime_identity,
        "terminal_status": terminal["status"],
        "observed_session_count": int(terminal["observed_sessions"]),
        "per_axis_status_session_counts": counts,
        "per_axis_totals": axis_totals,
        "orders": 0,
        "fills": 0,
        "contains_raw_option_rows": False,
        "uses_logs_as_data": False,
        "uses_object_store": False,
        "selection_authorized": False,
        "engine_status": "POLICY_BLOCKED_CASH_PRESERVATION",
        "production_effect": "none",
        "broker_action": "none",
        "investment_conclusion_authorized": False,
    }
    body["content_sha256"] = _content_sha256(body)
    return PerAxisExportSafeAggregateEvidence(body)


def build_per_axis_result_download_delivery_incident(
    *,
    admission: PerAxisOwnerAuthorizationAdmissionReceipt,
    consumption: PerAxisRunAttemptConsumptionReceipt,
    selected_file_name: str,
    download_carriers: tuple[Mapping[str, Any], ...],
    browser_download_event_acknowledged: bool,
) -> PerAxisResultDownloadDeliveryIncident:
    admitted = admission.payload
    consumed = consumption.payload
    if (
        consumed.get("authorization_consumed") is not True
        or consumed.get("cloud_backtest_attempt_count") != 1
        or consumed.get("project_id") != admitted.get("target_project_id")
        or consumed.get("backtest_id") in (None, "")
    ):
        _fail("PER_AXIS_DOWNLOAD_INCIDENT_CONSUMPTION_BINDING_INVALID")
    if browser_download_event_acknowledged:
        _fail("PER_AXIS_DOWNLOAD_INCIDENT_ACKNOWLEDGED_EVENT_INVALID")
    if len(download_carriers) < 2:
        _fail("PER_AXIS_DOWNLOAD_INCIDENT_DUPLICATE_REQUIRED")

    required_keys = {"file_name", "downloaded_at_utc", "byte_count", "sha256"}
    attempted_at = _parse_utc(str(consumed["attempted_at_utc"]), field="attempted_at_utc")
    normalized: list[dict[str, Any]] = []
    previous_time: datetime | None = None
    for index, carrier in enumerate(download_carriers):
        if set(carrier) != required_keys:
            _fail("PER_AXIS_DOWNLOAD_INCIDENT_CARRIER_FIELDS_INVALID", str(index))
        file_name = carrier["file_name"]
        downloaded_at_utc = carrier["downloaded_at_utc"]
        byte_count = carrier["byte_count"]
        file_sha256 = carrier["sha256"]
        if not isinstance(file_name, str) or not file_name or Path(file_name).name != file_name:
            _fail("PER_AXIS_DOWNLOAD_INCIDENT_FILE_NAME_INVALID", str(index))
        if not isinstance(downloaded_at_utc, str):
            _fail("PER_AXIS_DOWNLOAD_INCIDENT_TIME_INVALID", str(index))
        downloaded_at = _parse_utc(
            downloaded_at_utc, field=f"download_carriers[{index}].downloaded_at_utc"
        )
        if downloaded_at < attempted_at or (
            previous_time is not None and downloaded_at <= previous_time
        ):
            _fail("PER_AXIS_DOWNLOAD_INCIDENT_TIME_ORDER_INVALID", str(index))
        if isinstance(byte_count, bool) or not isinstance(byte_count, int) or byte_count <= 0:
            _fail("PER_AXIS_DOWNLOAD_INCIDENT_BYTE_COUNT_INVALID", str(index))
        if not isinstance(file_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", file_sha256) is None:
            _fail("PER_AXIS_DOWNLOAD_INCIDENT_SHA256_INVALID", str(index))
        normalized.append(
            {
                "ordinal": index + 1,
                "file_name": file_name,
                "downloaded_at_utc": downloaded_at_utc,
                "byte_count": byte_count,
                "sha256": file_sha256,
            }
        )
        previous_time = downloaded_at

    file_names = [carrier["file_name"] for carrier in normalized]
    if len(set(file_names)) != len(file_names) or selected_file_name != file_names[0]:
        _fail("PER_AXIS_DOWNLOAD_INCIDENT_SELECTION_INVALID")
    hashes = {carrier["sha256"] for carrier in normalized}
    byte_counts = {carrier["byte_count"] for carrier in normalized}
    if len(hashes) != 1 or len(byte_counts) != 1:
        _fail("PER_AXIS_DOWNLOAD_INCIDENT_NON_IDENTICAL_CARRIER")

    body: dict[str, Any] = {
        "schema_version": (
            "qc_qqq_options_daily_transport_per_axis_result_download_delivery_incident.v1"
        ),
        "task_id": admitted["task_id"],
        "status": "IDENTICAL_DUPLICATE_DOWNLOADS_DISCLOSED",
        "authorization_admission_content_sha256": admission.content_sha256,
        "run_attempt_consumption_content_sha256": consumption.content_sha256,
        "backtest_id": consumed["backtest_id"],
        "selected_file_name": selected_file_name,
        "selected_file_sha256": normalized[0]["sha256"],
        "selected_file_byte_count": normalized[0]["byte_count"],
        "download_trigger_count": len(normalized),
        "logical_result_content_count": 1,
        "identical_duplicate_file_count": len(normalized) - 1,
        "all_downloaded_files_identical": True,
        "browser_download_event_acknowledged": False,
        "incident_cause": "DOWNLOAD_EVENT_TIMEOUT_WHILE_CHROME_PERSISTED_FILE",
        "download_carriers": normalized,
        "external_source_storage": "OWNER_PROVIDED_G_DRIVE_DOWNLOAD_DIRECTORY",
        "external_source_files_retained": True,
        "external_source_deletion_authorized": False,
        "cloud_backtest_attempt_count": 1,
        "second_cloud_run_authorized": False,
        "orders": 0,
        "fills": 0,
        "selection_authorized": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    body["content_sha256"] = _content_sha256(body)
    return PerAxisResultDownloadDeliveryIncident(body)


def build_per_axis_external_action_ledger(
    *,
    admission: PerAxisOwnerAuthorizationAdmissionReceipt,
    login_observed_at_utc: str,
    mutation_started_at_utc: str,
    mutation_verified_at_utc: str,
    mutation_lf_byte_count: int,
    consumption: PerAxisRunAttemptConsumptionReceipt,
    evidence: PerAxisExportSafeAggregateEvidence | None = None,
    download_incident: PerAxisResultDownloadDeliveryIncident | None = None,
) -> PerAxisExternalActionLedger:
    admitted = admission.payload
    consumed = consumption.payload
    login_at = _parse_utc(login_observed_at_utc, field="login_observed_at_utc")
    mutation_at = _parse_utc(mutation_started_at_utc, field="mutation_started_at_utc")
    mutation_verified_at = _parse_utc(mutation_verified_at_utc, field="mutation_verified_at_utc")
    attempted_at = _parse_utc(str(consumed.get("attempted_at_utc", "")), field="attempted_at_utc")
    if not login_at <= mutation_at <= mutation_verified_at <= attempted_at:
        _fail("PER_AXIS_EXTERNAL_ACTION_ORDER_INVALID")
    if (
        consumed.get("authorization_consumed") is not True
        or consumed.get("cloud_backtest_attempt_count") != 1
        or consumed.get("maximum_cloud_backtests") != 1
        or consumed.get("project_id") != admitted.get("target_project_id")
        or consumed.get("project_code_lf_sha256") != admitted.get("project_code_lf_sha256")
        or mutation_lf_byte_count <= 0
    ):
        _fail("PER_AXIS_EXTERNAL_ACTION_BINDING_INVALID")
    actions: list[dict[str, Any]] = [
        {
            "ordinal": 1,
            "action": "INTERACTIVE_EXISTING_ACCOUNT_LOGIN_CONFIRMED",
            "occurred_at_utc": login_observed_at_utc,
            "status": "COMPLETED",
            "browser_surface": "CHROME_INTERACTIVE_SESSION",
            "sensitive_credentials_recorded": False,
        },
        {
            "ordinal": 2,
            "action": "EXISTING_PROJECT_EXACT_CODE_MUTATION",
            "occurred_at_utc": mutation_started_at_utc,
            "verified_at_utc": mutation_verified_at_utc,
            "status": "COMPLETED",
            "project_id": admitted["target_project_id"],
            "project_mutation_count": 1,
            "project_code_lf_byte_count": mutation_lf_byte_count,
            "project_code_lf_sha256": admitted["project_code_lf_sha256"],
        },
        {
            "ordinal": 3,
            "action": "ZERO_ORDER_CLOUD_BACKTEST_ATTEMPT",
            "occurred_at_utc": consumed["attempted_at_utc"],
            "status": consumed["attempt_status"],
            "project_id": consumed["project_id"],
            "backtest_id": consumed["backtest_id"],
            "cloud_backtest_attempt_count": 1,
            "authorization_consumed": True,
            "second_cloud_run_authorized": False,
        },
    ]
    ledger_status = "RUN_SUBMITTED_RESULTS_PENDING"
    download_trigger_count = 0
    identical_duplicate_file_count = 0
    if evidence is not None:
        collected = evidence.payload
        collected_at = _parse_utc(
            str(collected.get("collected_at_utc", "")), field="collected_at_utc"
        )
        if (
            collected_at < attempted_at
            or collected.get("authorization_admission_content_sha256") != admission.content_sha256
            or collected.get("run_attempt_consumption_content_sha256") != consumption.content_sha256
            or collected.get("backtest_id") != consumed["backtest_id"]
        ):
            _fail("PER_AXIS_EXTERNAL_ACTION_RESULT_BINDING_INVALID")
        actions.append(
            {
                "ordinal": 4,
                "action": "MANUAL_DOWNLOAD_RESULTS_JSON_COLLECTION",
                "occurred_at_utc": collected["collected_at_utc"],
                "status": "COMPLETED",
                "source_result_file_sha256": collected["source_result_file_sha256"],
                "export_safe_evidence_content_sha256": evidence.content_sha256,
            }
        )
        download_trigger_count = 1
        ledger_status = "EXTERNAL_ACTION_LIFECYCLE_COMPLETE"
    if download_incident is not None:
        if evidence is None:
            _fail("PER_AXIS_EXTERNAL_ACTION_DOWNLOAD_INCIDENT_WITHOUT_RESULT")
        incident = download_incident.payload
        if (
            incident.get("authorization_admission_content_sha256") != admission.content_sha256
            or incident.get("run_attempt_consumption_content_sha256") != consumption.content_sha256
            or incident.get("backtest_id") != consumed["backtest_id"]
            or incident.get("selected_file_sha256") != evidence.payload["source_result_file_sha256"]
            or incident.get("selected_file_byte_count")
            != evidence.payload["source_result_byte_count"]
            or incident.get("all_downloaded_files_identical") is not True
        ):
            _fail("PER_AXIS_EXTERNAL_ACTION_DOWNLOAD_INCIDENT_BINDING_INVALID")
        download_trigger_count = int(incident["download_trigger_count"])
        identical_duplicate_file_count = int(incident["identical_duplicate_file_count"])
        actions[-1].update(
            {
                "download_delivery_incident_content_sha256": download_incident.content_sha256,
                "download_trigger_count": download_trigger_count,
                "identical_duplicate_file_count": identical_duplicate_file_count,
                "browser_download_event_acknowledged": False,
            }
        )
        ledger_status = "EXTERNAL_ACTION_LIFECYCLE_COMPLETE_WITH_DISCLOSED_DOWNLOAD_DUPLICATES"
    body: dict[str, Any] = {
        "schema_version": "qc_qqq_options_daily_transport_per_axis_external_action_ledger.v1",
        "task_id": admitted["task_id"],
        "status": ledger_status,
        "authorization_admission_content_sha256": admission.content_sha256,
        "run_attempt_consumption_content_sha256": consumption.content_sha256,
        "action_count": len(actions),
        "actions": actions,
        "project_mutation_count": 1,
        "maximum_project_mutations": 1,
        "cloud_backtest_attempt_count": 1,
        "maximum_cloud_backtests": 1,
        "result_download_trigger_count": download_trigger_count,
        "result_identical_duplicate_file_count": identical_duplicate_file_count,
        "orders": 0,
        "fills": 0,
        "selection_authorized": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    body["content_sha256"] = _content_sha256(body)
    return PerAxisExternalActionLedger(body)


def build_per_axis_collection_execution_evidence_manifest(
    *,
    admission: PerAxisOwnerAuthorizationAdmissionReceipt,
    consumption: PerAxisRunAttemptConsumptionReceipt,
    evidence: PerAxisExportSafeAggregateEvidence,
    download_incident: PerAxisResultDownloadDeliveryIncident,
    action_ledger: PerAxisExternalActionLedger,
) -> PerAxisCollectionExecutionEvidenceManifest:
    admitted = admission.payload
    consumed = consumption.payload
    collected = evidence.payload
    incident = download_incident.payload
    ledger = action_ledger.payload
    if (
        consumed.get("authorization_admission_content_sha256") != admission.content_sha256
        or collected.get("authorization_admission_content_sha256") != admission.content_sha256
        or collected.get("run_attempt_consumption_content_sha256") != consumption.content_sha256
        or incident.get("authorization_admission_content_sha256") != admission.content_sha256
        or incident.get("run_attempt_consumption_content_sha256") != consumption.content_sha256
        or ledger.get("authorization_admission_content_sha256") != admission.content_sha256
        or ledger.get("run_attempt_consumption_content_sha256") != consumption.content_sha256
        or consumed.get("backtest_id") != collected.get("backtest_id")
        or consumed.get("backtest_id") != incident.get("backtest_id")
        or incident.get("selected_file_sha256") != collected.get("source_result_file_sha256")
        or ledger.get("result_download_trigger_count") != incident.get("download_trigger_count")
        or ledger.get("orders") != 0
        or ledger.get("fills") != 0
    ):
        _fail("PER_AXIS_EXECUTION_MANIFEST_BINDING_INVALID")
    artifacts = [
        {
            "file_name": "authorization_admission.json",
            "schema_version": admitted["schema_version"],
            "content_sha256": admission.content_sha256,
        },
        {
            "file_name": "run_attempt_consumption_receipt.json",
            "schema_version": consumed["schema_version"],
            "content_sha256": consumption.content_sha256,
        },
        {
            "file_name": "export_safe_aggregate_evidence.json",
            "schema_version": collected["schema_version"],
            "content_sha256": evidence.content_sha256,
        },
        {
            "file_name": "result_download_delivery_incident.json",
            "schema_version": incident["schema_version"],
            "content_sha256": download_incident.content_sha256,
        },
        {
            "file_name": "external_action_ledger.json",
            "schema_version": ledger["schema_version"],
            "content_sha256": action_ledger.content_sha256,
        },
    ]
    body: dict[str, Any] = {
        "schema_version": (
            "qc_qqq_options_daily_transport_per_axis_collection_execution_evidence_manifest.v1"
        ),
        "task_id": admitted["task_id"],
        "status": "EXECUTION_EVIDENCE_SEALED_WITH_DISCLOSED_DOWNLOAD_DUPLICATES",
        "sealed_at_utc": collected["collected_at_utc"],
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
        "backtest_id": consumed["backtest_id"],
        "project_id": admitted["target_project_id"],
        "project_code_lf_sha256": admitted["project_code_lf_sha256"],
        "source_result_file_sha256": collected["source_result_file_sha256"],
        "source_result_byte_count": collected["source_result_byte_count"],
        "raw_result_committed": False,
        "download_trigger_count": incident["download_trigger_count"],
        "identical_duplicate_file_count": incident["identical_duplicate_file_count"],
        "cloud_backtest_attempt_count": 1,
        "second_cloud_run_authorized": False,
        "orders": 0,
        "fills": 0,
        "selection_authorized": False,
        "investment_conclusion_authorized": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    body["content_sha256"] = _content_sha256(body)
    return PerAxisCollectionExecutionEvidenceManifest(body)


def write_receipt(path: Path, receipt: Any) -> None:
    write_bytes_atomic(path, receipt.canonical_bytes)


__all__ = [
    "DEFAULT_POLICY_PATH",
    "PROJECT_ROOT",
    "PerAxisCollectionAuthorizationAdmissionError",
    "PerAxisOwnerAuthorizationAdmissionReceipt",
    "PerAxisRunAttemptConsumptionReceipt",
    "PerAxisExportSafeAggregateEvidence",
    "PerAxisExternalActionLedger",
    "PerAxisResultDownloadDeliveryIncident",
    "PerAxisCollectionExecutionEvidenceManifest",
    "admit_per_axis_collection_owner_authorization",
    "consume_on_first_cloud_run_attempt",
    "validate_per_axis_results_json",
    "build_per_axis_result_download_delivery_incident",
    "build_per_axis_external_action_ledger",
    "build_per_axis_collection_execution_evidence_manifest",
    "write_receipt",
]
