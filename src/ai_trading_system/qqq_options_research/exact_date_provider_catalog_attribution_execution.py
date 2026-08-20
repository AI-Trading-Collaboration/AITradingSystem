"""Fail-closed admission and export-safe evidence for TRADING-2538.

This module has no browser, network, QuantConnect, order, or broker capability.  It
only verifies the exact Owner token and sealed TRADING-2537 package, and validates
the bounded Results JSON produced by the separately authorized interactive run.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.qqq_options_research import (
    exact_date_provider_catalog_attribution_correction as proposal_v1,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_POLICY_PATH = Path(
    "config/research/qc_qqq_options_exact_date_provider_catalog_attribution_execution_v1.yaml"
)
TASK_ID = (
    "TRADING-2538_QC_QQQ_OPTIONS_EXACT_DATE_PROVIDER_CATALOG_"
    "ATTRIBUTION_ADMISSION_AND_EXECUTION_V1"
)

_TOKEN_KEYS = (
    "owner_decision",
    "ordinary_pushed_main_sha",
    "registration_base_repository_code_sha",
    "proposal_policy_file_sha256",
    "proposal_policy_canonical_sha256",
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
    "proposal_content_sha256",
    "proposal_canonical_sha256",
    "project_code_lf_byte_count",
    "project_code_lf_sha256",
    "package_manifest_content_sha256",
    "target_project_id",
    "requested_range",
    "expected_session_count",
    "expected_never_chain_session_count",
    "maximum_provider_query_attempts",
    "exact_source_date_match_required",
    "cross_date_fallback_allowed",
    "execution_attribution_terminal_separation_required",
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

_PROHIBITED_RESULT_KEYS = frozenset(
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


class ExactDateAttributionExecutionError(ValueError):
    """Typed fail-closed execution-evidence error."""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(code if not detail else f"{code}: {detail}")
        self.code = code
        self.detail = detail


def _fail(code: str, detail: str = "") -> None:
    raise ExactDateAttributionExecutionError(code, detail)


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _content_sha256(payload: Mapping[str, Any]) -> str:
    semantic = dict(payload)
    semantic.pop("content_sha256", None)
    return sha256(_canonical_bytes(semantic)).hexdigest()


@dataclass(frozen=True)
class SealedRecord:
    payload: dict[str, Any]

    @property
    def content_sha256(self) -> str:
        value = self.payload.get("content_sha256")
        if not isinstance(value, str):
            _fail("TRADING2538_RECORD_SEAL_MISSING")
        return value

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.payload)


def _seal(payload: Mapping[str, Any]) -> SealedRecord:
    body = dict(payload)
    body["content_sha256"] = _content_sha256(body)
    return SealedRecord(body)


def verify_sealed_record(record: SealedRecord, *, label: str) -> None:
    if record.payload.get("content_sha256") != _content_sha256(record.payload):
        _fail("TRADING2538_RECORD_SEAL_INVALID", label)


def load_sealed_record(path: Path) -> SealedRecord:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                _fail("TRADING2538_RECORD_DUPLICATE_KEY", key)
            result[key] = value
        return result

    try:
        payload = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicates)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        _fail("TRADING2538_RECORD_INVALID", f"{path}:{exc}")
    if not isinstance(payload, dict):
        _fail("TRADING2538_RECORD_INVALID", str(path))
    record = SealedRecord(payload)
    verify_sealed_record(record, label=path.name)
    return record


def write_sealed_record(path: Path, record: SealedRecord) -> None:
    verify_sealed_record(record, label=path.name)
    write_bytes_atomic(path, record.canonical_bytes)


def _policy(project_root: Path) -> dict[str, Any]:
    path = (project_root / DEFAULT_POLICY_PATH).resolve()
    try:
        path.relative_to(project_root.resolve())
    except ValueError:
        _fail("TRADING2538_POLICY_OUTSIDE_REPOSITORY")
    try:
        value = safe_load_yaml_path(path)
    except (OSError, ValueError) as exc:
        _fail("TRADING2538_POLICY_INVALID", str(exc))
    if not isinstance(value, dict) or value.get("task_id") != TASK_ID:
        _fail("TRADING2538_POLICY_INVALID", "task_id")
    if (
        value.get("production_effect") != "none"
        or value.get("broker_action") != "none"
        or value.get("maximum_project_mutations") != 1
        or value.get("maximum_cloud_backtests") != 1
        or value.get("maximum_provider_query_attempts") != 1
        or value.get("maximum_orders") != 0
        or value.get("maximum_fills") != 0
        or value.get("authorization_single_use") is not True
        or value.get("authorization_invalidates_on_first_run_attempt") is not True
        or value.get("exact_source_date_match_required") is not True
        or value.get("cross_date_fallback_allowed") is not False
        or value.get("execution_attribution_terminal_separation_required") is not True
    ):
        _fail("TRADING2538_POLICY_SAFETY_INVALID")
    return value


def _render_token_value(value: object) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def _expected_token_values(policy: Mapping[str, Any]) -> dict[str, str]:
    expected = {key: _render_token_value(policy[key]) for key in _TOKEN_KEYS}
    owner_line = expected["owner_decision"]
    if not owner_line.startswith("owner_decision:"):
        _fail("TRADING2538_POLICY_OWNER_DECISION_INVALID")
    expected["owner_decision"] = owner_line.split(":", 1)[1]
    return expected


def _parse_utc(value: str, *, field: str) -> datetime:
    if not value.endswith("Z"):
        _fail("TRADING2538_UTC_INVALID", field)
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        _fail("TRADING2538_UTC_INVALID", field)
    return parsed.astimezone(UTC)


def _bound_file_hash(project_root: Path, relative_path: object, expected: object) -> None:
    if not isinstance(relative_path, str) or not isinstance(expected, str):
        _fail("TRADING2538_BOUND_FILE_POLICY_INVALID")
    path = (project_root / relative_path).resolve()
    try:
        path.relative_to(project_root.resolve())
    except ValueError:
        _fail("TRADING2538_BOUND_FILE_OUTSIDE_REPOSITORY", relative_path)
    if not path.is_file() or path.is_symlink():
        _fail("TRADING2538_BOUND_FILE_MISSING", relative_path)
    if sha256(path.read_bytes()).hexdigest() != expected:
        _fail("TRADING2538_BOUND_FILE_DRIFT", relative_path)


def _verify_bound_json_content(
    project_root: Path,
    relative_path: object,
    expected_file: object,
    expected_content: object,
) -> None:
    _bound_file_hash(project_root, relative_path, expected_file)
    path = project_root / str(relative_path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        _fail("TRADING2538_BOUND_JSON_INVALID", str(exc))
    if not isinstance(payload, dict) or payload.get("content_sha256") != expected_content:
        _fail("TRADING2538_BOUND_JSON_CONTENT_DRIFT", str(relative_path))


def _verify_proposal_package(project_root: Path, policy: Mapping[str, Any]) -> None:
    package_root = (project_root / str(policy["proposal_package_root"])).resolve()
    built = proposal_v1.load_attribution_proposal_package(
        project_root=project_root, package_root=package_root
    )
    loaded_policy = proposal_v1.load_attribution_proposal_policy(
        policy_path=project_root / str(policy["proposal_policy_path"]),
        project_root=project_root,
    )
    if (
        loaded_policy.file_sha256 != policy["proposal_policy_file_sha256"]
        or loaded_policy.canonical_sha256 != policy["proposal_policy_canonical_sha256"]
        or built.run_scope.content_sha256 != policy["run_scope_content_sha256"]
        or sha256(built.run_scope.canonical_bytes).hexdigest()
        != policy["run_scope_canonical_sha256"]
        or built.proposal.content_sha256 != policy["proposal_content_sha256"]
        or sha256(built.proposal.canonical_bytes).hexdigest()
        != policy["proposal_canonical_sha256"]
        or built.manifest.content_sha256 != policy["package_manifest_content_sha256"]
        or len(built.project_code_bytes) != policy["project_code_lf_byte_count"]
        or sha256(built.project_code_bytes).hexdigest() != policy["project_code_lf_sha256"]
    ):
        _fail("TRADING2538_PROPOSAL_PACKAGE_DRIFT")
    if b"\r" in built.project_code_bytes:
        _fail("TRADING2538_PROJECT_CODE_NOT_LF")
    for stem in ("source_evidence", "source_admission"):
        _verify_bound_json_content(
            project_root,
            policy[f"{stem}_path"],
            policy[f"{stem}_file_sha256"],
            policy[f"{stem}_content_sha256"],
        )
    _bound_file_hash(
        project_root,
        policy["staged_readiness_policy_path"],
        policy["staged_readiness_policy_file_sha256"],
    )
    _bound_file_hash(
        project_root,
        policy["staged_readiness_evaluator_path"],
        policy["staged_readiness_evaluator_file_sha256"],
    )
    _verify_bound_json_content(
        project_root,
        policy["predecessor_package_manifest_path"],
        policy["predecessor_package_manifest_file_sha256"],
        policy["predecessor_package_manifest_content_sha256"],
    )


def admit_owner_token(
    *,
    token: str,
    source: str,
    admitted_at_utc: str,
    local_main_sha: str,
    origin_main_sha: str,
    project_root: Path = PROJECT_ROOT,
) -> SealedRecord:
    root = project_root.resolve()
    policy = _policy(root)
    if "\r" in token or token.endswith("\n") or token.startswith("\n"):
        _fail("TRADING2538_OWNER_TOKEN_NONCANONICAL_NEWLINE")
    raw = token.encode("utf-8")
    if len(raw) != policy["owner_token_byte_count"]:
        _fail("TRADING2538_OWNER_TOKEN_BYTE_COUNT_INVALID")
    if sha256(raw).hexdigest() != policy["owner_token_sha256"]:
        _fail("TRADING2538_OWNER_TOKEN_SHA256_INVALID")
    lines = token.split("\n")
    if len(lines) != len(_TOKEN_KEYS):
        _fail("TRADING2538_OWNER_TOKEN_FIELD_COUNT_INVALID")
    parsed: dict[str, str] = {}
    for expected_key, line in zip(_TOKEN_KEYS, lines, strict=True):
        key, separator, value = line.partition(":")
        if separator != ":" or key != expected_key or key in parsed or not value:
            _fail("TRADING2538_OWNER_TOKEN_FIELD_INVALID", expected_key)
        parsed[key] = value
    if parsed != _expected_token_values(policy):
        mismatch = next(
            key for key in _TOKEN_KEYS if parsed[key] != _expected_token_values(policy)[key]
        )
        _fail("TRADING2538_OWNER_TOKEN_VALUE_INVALID", mismatch)
    admitted_at = _parse_utc(admitted_at_utc, field="admitted_at_utc")
    expires_at = _parse_utc(parsed["authorization_expires_at_utc"], field="expiry")
    if not admitted_at < expires_at <= admitted_at + timedelta(hours=168):
        _fail("TRADING2538_OWNER_TOKEN_EXPIRED_OR_HORIZON_INVALID")
    decision_date = date.fromisoformat(parsed["owner_decision"].split(":", 2)[1])
    tokyo_date = (admitted_at + timedelta(hours=9)).date()
    if decision_date != tokyo_date:
        _fail("TRADING2538_OWNER_TOKEN_DECISION_DATE_INVALID")
    if local_main_sha != policy["ordinary_pushed_main_sha"]:
        _fail("TRADING2538_LOCAL_MAIN_SHA_INVALID")
    if origin_main_sha != policy["ordinary_pushed_main_sha"]:
        _fail("TRADING2538_ORIGIN_MAIN_SHA_INVALID")
    _verify_proposal_package(root, policy)
    return _seal(
        {
            "schema_version": "qc_qqq_options_exact_date_attribution_owner_admission.v1",
            "task_id": TASK_ID,
            "status": "OWNER_AUTHORIZATION_ADMITTED_UNUSED",
            "owner_token_source": source,
            "owner_token_byte_count": len(raw),
            "owner_token_sha256": sha256(raw).hexdigest(),
            "admitted_at_utc": admitted_at_utc,
            "expires_at_utc": parsed["authorization_expires_at_utc"],
            "ordinary_pushed_main_sha": local_main_sha,
            "target_project_id": int(parsed["target_project_id"]),
            "project_code_lf_byte_count": int(parsed["project_code_lf_byte_count"]),
            "project_code_lf_sha256": parsed["project_code_lf_sha256"],
            "package_manifest_content_sha256": parsed[
                "package_manifest_content_sha256"
            ],
            "requested_range": parsed["requested_range"],
            "expected_session_count": int(parsed["expected_session_count"]),
            "expected_never_chain_session_count": int(
                parsed["expected_never_chain_session_count"]
            ),
            "maximum_provider_query_attempts": 1,
            "maximum_project_mutations": 1,
            "maximum_cloud_backtests": 1,
            "maximum_orders": 0,
            "maximum_fills": 0,
            "authorization_single_use": True,
            "authorization_invalidates_on_first_run_attempt": True,
            "authorization_consumed": False,
            "external_action_performed": False,
            "selection_authorized": False,
            "dq_pit_admission_authorized": False,
            "engine_status": "POLICY_BLOCKED_CASH_PRESERVATION",
            "production_effect": "none",
            "broker_action": "none",
        }
    )


def build_mutation_consumption_receipt(
    *,
    admission: SealedRecord,
    mutated_at_utc: str,
    project_id: int,
    project_code_lf_sha256: str,
) -> SealedRecord:
    verify_sealed_record(admission, label="authorization_admission")
    admitted = admission.payload
    _parse_utc(mutated_at_utc, field="mutated_at_utc")
    if (
        admitted.get("status") != "OWNER_AUTHORIZATION_ADMITTED_UNUSED"
        or project_id != admitted.get("target_project_id")
        or project_code_lf_sha256 != admitted.get("project_code_lf_sha256")
    ):
        _fail("TRADING2538_MUTATION_BINDING_INVALID")
    return _seal(
        {
            "schema_version": "qc_qqq_options_exact_date_attribution_mutation_consumption.v1",
            "task_id": TASK_ID,
            "status": "AUTHORIZATION_CONSUMED_BY_FIRST_PROJECT_MUTATION",
            "authorization_admission_content_sha256": admission.content_sha256,
            "authorization_consumed": True,
            "authorization_invalidated_for_additional_mutations": True,
            "mutated_at_utc": mutated_at_utc,
            "project_id": project_id,
            "project_code_lf_sha256": project_code_lf_sha256,
            "project_mutation_count": 1,
            "cloud_backtest_attempt_count": 0,
            "additional_project_mutation_authorized": False,
            "single_bound_cloud_run_still_authorized": True,
            "orders": 0,
            "fills": 0,
            "production_effect": "none",
            "broker_action": "none",
        }
    )


def build_run_attempt_receipt(
    *,
    admission: SealedRecord,
    mutation: SealedRecord,
    attempted_at_utc: str,
    backtest_id: str,
    attempt_status: str = "SUBMITTED",
) -> SealedRecord:
    verify_sealed_record(admission, label="authorization_admission")
    verify_sealed_record(mutation, label="mutation_consumption")
    attempted = _parse_utc(attempted_at_utc, field="attempted_at_utc")
    mutated = _parse_utc(str(mutation.payload["mutated_at_utc"]), field="mutated_at_utc")
    if attempted < mutated or not re.fullmatch(r"[0-9a-f]{32}", backtest_id):
        _fail("TRADING2538_RUN_ATTEMPT_BINDING_INVALID")
    if (
        mutation.payload.get("authorization_admission_content_sha256")
        != admission.content_sha256
        or mutation.payload.get("project_mutation_count") != 1
        or mutation.payload.get("cloud_backtest_attempt_count") != 0
    ):
        _fail("TRADING2538_RUN_ATTEMPT_BINDING_INVALID")
    return _seal(
        {
            "schema_version": "qc_qqq_options_exact_date_attribution_run_attempt.v1",
            "task_id": TASK_ID,
            "attempt_status": attempt_status,
            "attempted_at_utc": attempted_at_utc,
            "authorization_admission_content_sha256": admission.content_sha256,
            "mutation_consumption_content_sha256": mutation.content_sha256,
            "authorization_consumed": True,
            "authorization_invalidated_for_further_attempts": True,
            "backtest_id": backtest_id,
            "project_id": admission.payload["target_project_id"],
            "project_code_lf_sha256": admission.payload["project_code_lf_sha256"],
            "project_mutation_count": 1,
            "cloud_backtest_attempt_count": 1,
            "provider_query_maximum": 1,
            "second_cloud_run_authorized": False,
            "second_project_mutation_authorized": False,
            "orders": 0,
            "fills": 0,
            "selection_authorized": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    )


def build_failed_mutation_attempt_incident(
    *,
    admission: SealedRecord,
    preliminary_mutation_receipt: SealedRecord,
    attempted_at_utc: str,
    verification_failed_at_utc: str,
    observed_code_marker: str,
    screenshot_byte_count: int,
    screenshot_sha256: str,
) -> SealedRecord:
    """Seal a failed first mutation attempt without inventing an actual mutation."""

    verify_sealed_record(admission, label="authorization_admission")
    verify_sealed_record(preliminary_mutation_receipt, label="preliminary_mutation_receipt")
    attempted = _parse_utc(attempted_at_utc, field="attempted_at_utc")
    failed = _parse_utc(verification_failed_at_utc, field="verification_failed_at_utc")
    if failed < attempted:
        _fail("TRADING2538_MUTATION_INCIDENT_TIME_INVALID")
    if (
        preliminary_mutation_receipt.payload.get(
            "authorization_admission_content_sha256"
        )
        != admission.content_sha256
        or preliminary_mutation_receipt.payload.get("project_id")
        != admission.payload.get("target_project_id")
        or not re.fullmatch(r"[0-9a-f]{64}", screenshot_sha256)
        or screenshot_byte_count <= 0
        or not observed_code_marker
    ):
        _fail("TRADING2538_MUTATION_INCIDENT_BINDING_INVALID")
    return _seal(
        {
            "schema_version": "qc_qqq_options_exact_date_attribution_mutation_incident.v1",
            "task_id": TASK_ID,
            "status": "FIRST_PROJECT_MUTATION_ATTEMPT_FAILED_AUTHORIZATION_CONSUMED",
            "authorization_admission_content_sha256": admission.content_sha256,
            "preliminary_mutation_receipt_content_sha256": (
                preliminary_mutation_receipt.content_sha256
            ),
            "preliminary_mutation_receipt_disposition": (
                "SUPERSEDED_BY_POST_SAVE_READ_ONLY_VERIFICATION_FAILURE"
            ),
            "attempted_at_utc": attempted_at_utc,
            "verification_failed_at_utc": verification_failed_at_utc,
            "project_id": admission.payload["target_project_id"],
            "expected_project_code_lf_byte_count": admission.payload[
                "project_code_lf_byte_count"
            ],
            "expected_project_code_lf_sha256": admission.payload[
                "project_code_lf_sha256"
            ],
            "observed_code_marker": observed_code_marker,
            "verification_method": "READ_ONLY_VISIBLE_MONACO_TEXT_AND_SCREENSHOT_HASH",
            "screenshot_byte_count": screenshot_byte_count,
            "screenshot_sha256": screenshot_sha256,
            "project_mutation_attempt_count": 1,
            "verified_project_mutation_count": 0,
            "cloud_backtest_attempt_count": 0,
            "provider_query_attempt_count": 0,
            "authorization_consumed": True,
            "retry_authorized": False,
            "cloud_run_authorized_after_failure": False,
            "orders": 0,
            "fills": 0,
            "selection_authorized": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    )


def build_failed_mutation_action_ledger(
    *,
    admission: SealedRecord,
    incident: SealedRecord,
    login_observed_at_utc: str,
) -> SealedRecord:
    verify_sealed_record(admission, label="authorization_admission")
    verify_sealed_record(incident, label="mutation_attempt_incident")
    _parse_utc(login_observed_at_utc, field="login_observed_at_utc")
    if (
        incident.payload.get("authorization_admission_content_sha256")
        != admission.content_sha256
        or incident.payload.get("verified_project_mutation_count") != 0
        or incident.payload.get("cloud_backtest_attempt_count") != 0
    ):
        _fail("TRADING2538_FAILED_LEDGER_BINDING_INVALID")
    actions = [
        {"ordinal": 1, "action": "LOGIN_OBSERVED", "at_utc": login_observed_at_utc},
        {
            "ordinal": 2,
            "action": "PROJECT_MUTATION_ATTEMPTED",
            "at_utc": incident.payload["attempted_at_utc"],
        },
        {
            "ordinal": 3,
            "action": "POST_SAVE_READ_ONLY_VERIFICATION_FAILED",
            "at_utc": incident.payload["verification_failed_at_utc"],
        },
        {
            "ordinal": 4,
            "action": "AUTHORIZATION_CONSUMED_NO_RETRY_CLOUD_RUN_BLOCKED",
            "at_utc": incident.payload["verification_failed_at_utc"],
        },
    ]
    return _seal(
        {
            "schema_version": "qc_qqq_options_exact_date_attribution_action_ledger.v1",
            "task_id": TASK_ID,
            "status": "EXTERNAL_ACTION_BLOCKED_AFTER_FAILED_FIRST_MUTATION_ATTEMPT",
            "authorization_admission_content_sha256": admission.content_sha256,
            "mutation_attempt_incident_content_sha256": incident.content_sha256,
            "actions": actions,
            "project_mutation_attempt_count": 1,
            "verified_project_mutation_count": 0,
            "cloud_backtest_attempt_count": 0,
            "provider_query_attempt_count": 0,
            "orders": 0,
            "fills": 0,
            "authorization_consumed": True,
            "retry_authorized": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    )


def build_blocked_execution_manifest(
    *, records: Mapping[str, SealedRecord]
) -> SealedRecord:
    required = {
        "authorization_admission.json",
        "mutation_consumption_receipt.json",
        "mutation_attempt_incident.json",
        "external_action_ledger.json",
    }
    if set(records) != required:
        _fail("TRADING2538_BLOCKED_MANIFEST_INVENTORY_INVALID")
    artifacts: dict[str, str] = {}
    for name, record in sorted(records.items()):
        verify_sealed_record(record, label=name)
        artifacts[name] = record.content_sha256
    incident = records["mutation_attempt_incident.json"]
    ledger = records["external_action_ledger.json"]
    if (
        ledger.payload.get("mutation_attempt_incident_content_sha256")
        != incident.content_sha256
    ):
        _fail("TRADING2538_BLOCKED_MANIFEST_BINDING_INVALID")
    return _seal(
        {
            "schema_version": "qc_qqq_options_exact_date_attribution_execution_manifest.v1",
            "task_id": TASK_ID,
            "status": "EXECUTION_BLOCKED_AUTHORIZATION_CONSUMED_NO_CLOUD_RUN",
            "artifact_count": len(artifacts),
            "artifacts": artifacts,
            "project_mutation_attempt_count": 1,
            "verified_project_mutation_count": 0,
            "cloud_backtest_attempt_count": 0,
            "provider_query_attempt_count": 0,
            "orders": 0,
            "fills": 0,
            "raw_result_committed": False,
            "retry_authorized": False,
            "dq_pit_admission_authorized": False,
            "selection_authorized": False,
            "investment_conclusion_authorized": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    )


def _result_object(raw: bytes) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                _fail("TRADING2538_RESULTS_DUPLICATE_JSON_KEY", key)
            result[key] = value
        return result

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        _fail("TRADING2538_RESULTS_JSON_INVALID", str(exc))
    if not isinstance(value, dict):
        _fail("TRADING2538_RESULTS_JSON_INVALID")
    return value


def _mapping(value: object, *, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail("TRADING2538_RESULTS_MAPPING_REQUIRED", field)
    return value


def _reject_prohibited_result_keys(value: object, *, path: str = "result") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = re.sub(r"[^a-z0-9]", "", str(key).casefold())
            if normalized in _PROHIBITED_RESULT_KEYS:
                _fail("TRADING2538_RESULTS_PROHIBITED_CARRIER", f"{path}.{key}")
            _reject_prohibited_result_keys(child, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_prohibited_result_keys(child, path=f"{path}[{index}]")


def _nonnegative_integer(value: object, *, field: str) -> int:
    if isinstance(value, bool):
        _fail("TRADING2538_RESULTS_COUNT_INVALID", field)
    try:
        parsed = int(str(value))
    except ValueError:
        _fail("TRADING2538_RESULTS_COUNT_INVALID", field)
    if str(parsed) != str(value).strip() or parsed < 0:
        _fail("TRADING2538_RESULTS_COUNT_INVALID", field)
    return parsed


def _zero_currency(value: object, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        _fail("TRADING2538_RESULTS_ZERO_REQUIRED", field)
    try:
        parsed = float(str(value).strip().replace("$", "").replace(",", ""))
    except ValueError:
        _fail("TRADING2538_RESULTS_ZERO_REQUIRED", field)
    if parsed != 0:
        _fail("TRADING2538_RESULTS_ZERO_REQUIRED", field)


def _terminal_fields(
    value: object, *, expected_sessions: int, requested_range: str
) -> dict[str, str]:
    if not isinstance(value, str):
        _fail("TRADING2538_RESULTS_TERMINAL_INVALID")
    fields: dict[str, str] = {}
    for item in value.split("|"):
        key, separator, rendered = item.partition("=")
        if separator != "=" or not key or key in fields:
            _fail("TRADING2538_RESULTS_TERMINAL_INVALID")
        fields[key] = rendered
    expected_keys = {
        "status",
        "expected_sessions",
        "observed_sessions",
        "requested_range",
        "evaluated_range",
        "orders",
        "fills",
        "portfolio_invested",
        "raw_rows",
        "contract_identifiers_exported",
        "individual_fields_exported",
        "logs_as_data",
        "object_store",
    }
    if set(fields) != expected_keys:
        _fail("TRADING2538_RESULTS_TERMINAL_INVALID")
    if (
        fields["status"] != "COMPLETE"
        or fields["expected_sessions"] != str(expected_sessions)
        or _nonnegative_integer(fields["observed_sessions"], field="observed_sessions")
        != expected_sessions
        or fields["requested_range"] != requested_range
        or fields["evaluated_range"] != requested_range
        or any(
            fields[key] != expected
            for key, expected in {
                "orders": "0",
                "fills": "0",
                "portfolio_invested": "false",
                "raw_rows": "false",
                "contract_identifiers_exported": "false",
                "individual_fields_exported": "false",
                "logs_as_data": "false",
                "object_store": "false",
            }.items()
        )
    ):
        _fail("TRADING2538_RESULTS_TERMINAL_INVALID")
    return fields


def _validate_attribution(runtime: Mapping[str, Any], policy: Mapping[str, Any]) -> dict[str, Any]:
    counts = {
        key: _nonnegative_integer(runtime[key], field=key)
        for key in (
            "TRADING2537_TARGET_SESSION_COUNT",
            "TRADING2537_TARGET_SUBSCRIBED_CHAIN_EVENT_COUNT",
            "TRADING2537_PROVIDER_QUERY_ATTEMPT_COUNT",
            "TRADING2537_EXACT_DATE_RECORD_COUNT",
            "TRADING2537_EXACT_DATE_CONTRACT_COUNT",
            "TRADING2537_NON_TARGET_RECORD_COUNT",
        )
    }
    if (
        counts["TRADING2537_TARGET_SESSION_COUNT"] != 1
        or counts["TRADING2537_TARGET_SUBSCRIBED_CHAIN_EVENT_COUNT"] != 0
        or counts["TRADING2537_PROVIDER_QUERY_ATTEMPT_COUNT"] != 1
    ):
        _fail("TRADING2538_RESULTS_SCOPE_INVALID")
    target_date = str(runtime["TRADING2537_TARGET_SESSION_DATE"])
    try:
        parsed_date = date.fromisoformat(target_date)
    except ValueError:
        _fail("TRADING2538_RESULTS_TARGET_DATE_INVALID")
    start_text, end_text = str(policy["requested_range"]).split("..", 1)
    expected_position = (
        "START_BOUNDARY"
        if target_date == start_text
        else "END_BOUNDARY"
        if target_date == end_text
        else "INTERIOR"
    )
    if not date.fromisoformat(start_text) <= parsed_date <= date.fromisoformat(end_text):
        _fail("TRADING2538_RESULTS_TARGET_DATE_INVALID")
    if runtime["TRADING2537_TARGET_SESSION_POSITION"] != expected_position:
        _fail("TRADING2538_RESULTS_TARGET_POSITION_INVALID")
    if runtime["TRADING2537_TARGET_EQUITY_SLICE_PRESENT"] not in ("true", "false"):
        _fail("TRADING2538_RESULTS_TARGET_EQUITY_INVALID")
    cross_date = runtime["TRADING2537_CROSS_DATE_FALLBACK_DETECTED"]
    if cross_date not in ("true", "false"):
        _fail("TRADING2538_RESULTS_CROSS_DATE_INVALID")
    probe = str(runtime["TRADING2537_PROVIDER_PROBE_STATUS"])
    attribution = str(runtime["TRADING2537_ATTRIBUTION"])
    terminal = str(runtime["TRADING2537_ATTRIBUTION_TERMINAL"])
    exact_records = counts["TRADING2537_EXACT_DATE_RECORD_COUNT"]
    exact_contracts = counts["TRADING2537_EXACT_DATE_CONTRACT_COUNT"]
    non_target = counts["TRADING2537_NON_TARGET_RECORD_COUNT"]
    valid = False
    if probe == "ERROR":
        valid = attribution == "PROVIDER_PROBE_ERROR" and terminal == "ERROR"
    elif probe == "CROSS_DATE_FALLBACK":
        valid = (
            cross_date == "true"
            and non_target > 0
            and attribution == "NO_EXACT_DATE_PROVIDER_EVIDENCE"
            and terminal == "INDETERMINATE"
        )
    elif probe == "NO_EXACT_DATE_RECORD":
        valid = (
            cross_date == "false"
            and exact_records == 0
            and non_target == 0
            and attribution == "NO_EXACT_DATE_PROVIDER_EVIDENCE"
            and terminal == "INDETERMINATE"
        )
    elif probe == "INDETERMINATE":
        valid = (
            cross_date == "false"
            and exact_records != 1
            and attribution == "ATTRIBUTION_INDETERMINATE"
            and terminal == "INDETERMINATE"
        )
    elif probe == "EXACT_DATE_AVAILABLE":
        valid = (
            cross_date == "false"
            and exact_records == 1
            and exact_contracts > 0
            and non_target == 0
            and attribution == "EXACT_DATE_CATALOG_AVAILABLE_SUBSCRIPTION_MISSING"
            and terminal == "RESOLVED"
        )
    elif probe == "EXACT_DATE_EMPTY":
        valid = (
            cross_date == "false"
            and exact_records == 1
            and exact_contracts == 0
            and non_target == 0
            and attribution == "EXACT_DATE_CATALOG_EMPTY"
            and terminal == "RESOLVED"
        )
    if not valid or attribution not in policy["allowed_classifications"]:
        _fail("TRADING2538_RESULTS_ATTRIBUTION_INVALID")
    return {
        "target_session_date": target_date,
        "target_session_position": expected_position,
        "target_equity_slice_present": runtime[
            "TRADING2537_TARGET_EQUITY_SLICE_PRESENT"
        ]
        == "true",
        "provider_probe_status": probe,
        "provider_query_attempt_count": counts[
            "TRADING2537_PROVIDER_QUERY_ATTEMPT_COUNT"
        ],
        "exact_date_record_count": exact_records,
        "exact_date_contract_count": exact_contracts,
        "non_target_record_count": non_target,
        "cross_date_fallback_detected": cross_date == "true",
        "attribution": attribution,
        "attribution_terminal": terminal,
    }


def validate_results_json(
    *,
    result_bytes: bytes,
    admission: SealedRecord,
    mutation: SealedRecord,
    run_attempt: SealedRecord,
    collected_at_utc: str,
    backtest_id: str,
    project_root: Path = PROJECT_ROOT,
) -> SealedRecord:
    for record, label in (
        (admission, "authorization_admission"),
        (mutation, "mutation_consumption"),
        (run_attempt, "run_attempt"),
    ):
        verify_sealed_record(record, label=label)
    collected = _parse_utc(collected_at_utc, field="collected_at_utc")
    attempted = _parse_utc(str(run_attempt.payload["attempted_at_utc"]), field="attempted")
    if collected < attempted or run_attempt.payload.get("backtest_id") != backtest_id:
        _fail("TRADING2538_RESULTS_RUN_BINDING_INVALID")
    if (
        run_attempt.payload.get("authorization_admission_content_sha256")
        != admission.content_sha256
        or run_attempt.payload.get("mutation_consumption_content_sha256")
        != mutation.content_sha256
    ):
        _fail("TRADING2538_RESULTS_RUN_BINDING_INVALID")
    policy = _policy(project_root.resolve())
    payload = _result_object(result_bytes)
    _reject_prohibited_result_keys(payload)
    state = _mapping(payload.get("state"), field="state")
    if (
        state.get("Status") != "Completed"
        or state.get("RuntimeError") not in (None, "")
        or str(state.get("OrderCount")) != "0"
    ):
        _fail("TRADING2538_RESULTS_STATE_INVALID")
    if payload.get("orders") not in ({}, [], None):
        _fail("TRADING2538_RESULTS_ORDERS_NOT_EMPTY")
    statistics = _mapping(payload.get("statistics"), field="statistics")
    _zero_currency(statistics.get("Total Orders"), field="statistics.Total Orders")
    _zero_currency(statistics.get("Total Fees"), field="statistics.Total Fees")
    configuration = _mapping(payload.get("algorithmConfiguration"), field="configuration")
    start_text, end_text = str(policy["requested_range"]).split("..", 1)
    if not str(configuration.get("startDate", "")).startswith(start_text):
        _fail("TRADING2538_RESULTS_RANGE_INVALID", "start")
    if not str(configuration.get("endDate", "")).startswith(end_text):
        _fail("TRADING2538_RESULTS_RANGE_INVALID", "end")
    runtime = _mapping(payload.get("runtimeStatistics"), field="runtimeStatistics")
    observed = {key for key in runtime if key.startswith("TRADING2537_")}
    if observed != set(policy["allowed_runtime_statistics"]):
        _fail("TRADING2538_RESULTS_RUNTIME_KEYSET_INVALID")
    expected_identity = (
        "schema=qc_qqq_options_exact_date_provider_catalog_attribution_correction_runtime.v1"
        f"|source={policy['source_evidence_content_sha256']}"
        f"|admission={policy['source_admission_content_sha256']}"
        f"|staged_policy={policy['staged_readiness_policy_file_sha256']}"
        f"|predecessor={policy['predecessor_package_manifest_content_sha256']}"
    )
    if runtime["TRADING2537_IDENTITY"] != expected_identity:
        _fail("TRADING2538_RESULTS_IDENTITY_INVALID")
    terminal = _terminal_fields(
        runtime["TRADING2537_EXECUTION_TERMINAL"],
        expected_sessions=int(policy["expected_session_count"]),
        requested_range=str(policy["requested_range"]),
    )
    attribution = _validate_attribution(runtime, policy)
    return _seal(
        {
            "schema_version": "qc_qqq_options_exact_date_attribution_export_safe_evidence.v1",
            "task_id": TASK_ID,
            "status": "EXPORT_SAFE_EXACT_DATE_ATTRIBUTION_COLLECTED",
            "authorization_admission_content_sha256": admission.content_sha256,
            "mutation_consumption_content_sha256": mutation.content_sha256,
            "run_attempt_content_sha256": run_attempt.content_sha256,
            "source_result_file_sha256": sha256(result_bytes).hexdigest(),
            "source_result_byte_count": len(result_bytes),
            "collected_at_utc": collected_at_utc,
            "backtest_id": backtest_id,
            "project_id": admission.payload["target_project_id"],
            "project_code_lf_sha256": admission.payload["project_code_lf_sha256"],
            "requested_range": policy["requested_range"],
            "expected_session_count": policy["expected_session_count"],
            "observed_session_count": int(terminal["observed_sessions"]),
            **attribution,
            "orders": 0,
            "fills": 0,
            "raw_rows_collected": False,
            "contract_identifiers_collected": False,
            "individual_contract_fields_collected": False,
            "logs_as_data_collected": False,
            "object_store_used": False,
            "dq_pit_admission_authorized": False,
            "selection_authorized": False,
            "investment_conclusion_authorized": False,
            "engine_status": "POLICY_BLOCKED_CASH_PRESERVATION",
            "production_effect": "none",
            "broker_action": "none",
        }
    )


def build_external_action_ledger(
    *,
    admission: SealedRecord,
    login_observed_at_utc: str,
    mutation: SealedRecord,
    run_attempt: SealedRecord,
    evidence: SealedRecord | None = None,
) -> SealedRecord:
    actions: list[dict[str, Any]] = [
        {"ordinal": 1, "action": "LOGIN_OBSERVED", "at_utc": login_observed_at_utc},
        {
            "ordinal": 2,
            "action": "PROJECT_MUTATION_CONSUMED_AUTHORIZATION",
            "at_utc": mutation.payload["mutated_at_utc"],
        },
        {
            "ordinal": 3,
            "action": "CLOUD_RUN_ATTEMPT_SUBMITTED",
            "at_utc": run_attempt.payload["attempted_at_utc"],
            "backtest_id": run_attempt.payload["backtest_id"],
        },
    ]
    if evidence is not None:
        actions.append(
            {
                "ordinal": 4,
                "action": "EXPORT_SAFE_EVIDENCE_VALIDATED",
                "at_utc": evidence.payload["collected_at_utc"],
                "evidence_content_sha256": evidence.content_sha256,
            }
        )
    return _seal(
        {
            "schema_version": "qc_qqq_options_exact_date_attribution_action_ledger.v1",
            "task_id": TASK_ID,
            "status": "EXTERNAL_ACTION_LIFECYCLE_COMPLETE"
            if evidence is not None
            else "EXTERNAL_ACTION_RESULT_PENDING",
            "authorization_admission_content_sha256": admission.content_sha256,
            "mutation_consumption_content_sha256": mutation.content_sha256,
            "run_attempt_content_sha256": run_attempt.content_sha256,
            "actions": actions,
            "project_mutation_count": 1,
            "cloud_backtest_attempt_count": 1,
            "orders": 0,
            "fills": 0,
            "second_attempt_authorized": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    )


def build_execution_manifest(records: Mapping[str, SealedRecord]) -> SealedRecord:
    required = {
        "authorization_admission.json",
        "mutation_consumption_receipt.json",
        "run_attempt_receipt.json",
        "external_action_ledger.json",
    }
    if not required.issubset(records):
        _fail("TRADING2538_MANIFEST_INVENTORY_INVALID")
    artifacts: dict[str, str] = {}
    for name, record in sorted(records.items()):
        verify_sealed_record(record, label=name)
        artifacts[name] = record.content_sha256
    return _seal(
        {
            "schema_version": "qc_qqq_options_exact_date_attribution_execution_manifest.v1",
            "task_id": TASK_ID,
            "status": "EXECUTION_EVIDENCE_COMPLETE"
            if "export_safe_attribution_evidence.json" in records
            else "EXECUTION_RESULT_PENDING",
            "artifact_count": len(artifacts),
            "artifacts": artifacts,
            "project_mutation_count": 1,
            "cloud_backtest_attempt_count": 1,
            "orders": 0,
            "fills": 0,
            "raw_result_committed": False,
            "second_attempt_authorized": False,
            "dq_pit_admission_authorized": False,
            "selection_authorized": False,
            "investment_conclusion_authorized": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    )


__all__ = [
    "ExactDateAttributionExecutionError",
    "SealedRecord",
    "admit_owner_token",
    "build_blocked_execution_manifest",
    "build_execution_manifest",
    "build_external_action_ledger",
    "build_failed_mutation_action_ledger",
    "build_failed_mutation_attempt_incident",
    "build_mutation_consumption_receipt",
    "build_run_attempt_receipt",
    "load_sealed_record",
    "validate_results_json",
    "verify_sealed_record",
    "write_sealed_record",
]
