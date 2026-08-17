from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

from ai_trading_system.qqq_options_research.daily_transport_session_finalization import (
    validate_session_finalization_package,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_POLICY_PATH = Path(
    "config/research/qc_qqq_options_session_finalization_external_validation_admission_v1.yaml"
)

_FIELD_ORDER = (
    "proposal_publication_main_sha",
    "ordinary_pushed_admission_main_sha",
    "admission_identity_contract_content_sha256",
    "registration_base_repository_code_sha",
    "policy_file_sha256",
    "policy_canonical_sha256",
    "contract_content_sha256",
    "contract_canonical_sha256",
    "project_code_lf_byte_count",
    "project_code_lf_sha256",
    "predecessor_evidence_content_sha256",
    "predecessor_results_sha256",
    "package_manifest_content_sha256",
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
_DIAGNOSTICS = (
    "CHAINLESS_SLICE_EVENTS",
    "SESSIONS_WITH_CHAINLESS_SLICE",
    "SESSIONS_RECOVERED_AFTER_CHAINLESS",
    "SESSIONS_NEVER_CHAIN",
    "SESSIONS_WITH_CANONICAL_EQUITY_PRESENT",
    "SESSIONS_WITH_CANONICAL_EQUITY_MISSING",
    "SESSIONS_WITH_CANONICAL_EQUITY_INVALID",
    "SESSIONS_WITH_CONTRACT_ZERO_IGNORED",
    "SESSIONS_WITH_MULTIPLE_CHAIN_EVENTS",
)
_AXIS_KEYS = tuple(
    f"TRADING2531_{axis}_{status}_SESSIONS" for axis in _AXES for status in _STATUSES
)
_DIAGNOSTIC_KEYS = tuple(f"TRADING2531_{name}" for name in _DIAGNOSTICS)
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


class SessionFinalizationExternalValidationError(ValueError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(code if not detail else f"{code}: {detail}")
        self.code = code
        self.detail = detail


def _fail(code: str, detail: str = "") -> None:
    raise SessionFinalizationExternalValidationError(code, detail)


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _content_sha256(payload: Mapping[str, Any]) -> str:
    body = dict(payload)
    body.pop("content_sha256", None)
    return sha256(_canonical_bytes(body)).hexdigest()


def _verify_seal(payload: Mapping[str, Any], *, label: str) -> None:
    observed = payload.get("content_sha256")
    if not isinstance(observed, str) or observed != _content_sha256(payload):
        _fail("SESSION_FINALIZATION_EXTERNAL_SEAL_INVALID", label)


def _parse_utc(value: str, *, field: str) -> datetime:
    if not value.endswith("Z"):
        _fail("SESSION_FINALIZATION_EXTERNAL_UTC_REQUIRED", field)
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        _fail("SESSION_FINALIZATION_EXTERNAL_TIME_INVALID", field)
        raise AssertionError from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        _fail("SESSION_FINALIZATION_EXTERNAL_UTC_REQUIRED", field)
    return parsed.astimezone(UTC)


def _mapping(value: object, *, field: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        _fail("SESSION_FINALIZATION_EXTERNAL_MAPPING_REQUIRED", field)
    return value


def _load_policy(project_root: Path = PROJECT_ROOT) -> dict[str, Any]:
    path = (project_root.resolve() / DEFAULT_POLICY_PATH).resolve()
    value = safe_load_yaml_path(path)
    if not isinstance(value, dict):
        _fail("SESSION_FINALIZATION_EXTERNAL_POLICY_INVALID", "root")
    required = {
        "schema_version",
        "policy_id",
        "policy_version",
        "policy_status",
        "task_id",
        "predecessor_task_id",
        "ordinary_pushed_proposal_main_sha",
        "registration_base_repository_code_sha",
        "proposal_package_root",
        "proposal_scope_content_sha256",
        "proposal_package_content_sha256",
        "admission_identity_contract_path",
        "admission_identity_contract_content_sha256",
        "admission_identity_request_path",
        "admission_identity_request_sha256",
        "owner_decision",
        "owner_token_status",
        "owner_token_sha256",
        "owner_token_byte_count",
        "owner_token_source",
        "owner_decision_date",
        "authorization_expires_at_utc",
        "authorization_expires_after_hours_maximum",
        "policy_file_sha256",
        "policy_canonical_sha256",
        "contract_content_sha256",
        "contract_canonical_sha256",
        "project_code_lf_byte_count",
        "project_code_lf_sha256",
        "predecessor_evidence_content_sha256",
        "predecessor_results_sha256",
        "target_project_id",
        "requested_start",
        "requested_end",
        "expected_session_count",
        "maximum_project_mutations",
        "maximum_cloud_backtests",
        "maximum_orders",
        "maximum_fills",
        "collector_id",
        "independent_reviewer_id",
        "authorization_single_use",
        "authorization_invalidates_on_first_run_attempt",
        "result_carrier",
        "output_granularity",
        "allowed_actions_after_admission",
        "prohibited_actions",
        "safety",
    }
    if set(value) != required:
        _fail("SESSION_FINALIZATION_EXTERNAL_POLICY_KEYSET_INVALID")
    if (
        value["schema_version"]
        != "qc_qqq_options_session_finalization_external_validation_admission_policy.v2"
        or value["policy_version"] != "2.0.0"
        or value["maximum_project_mutations"] != 1
        or value["maximum_cloud_backtests"] != 1
        or value["maximum_orders"] != 0
        or value["maximum_fills"] != 0
        or value["authorization_single_use"] is not True
        or value["authorization_invalidates_on_first_run_attempt"] is not True
    ):
        _fail("SESSION_FINALIZATION_EXTERNAL_POLICY_LIMIT_INVALID")
    safety = _mapping(value["safety"], field="safety")
    if safety != {
        "engine_status": "POLICY_BLOCKED_CASH_PRESERVATION",
        "selection_authorized": False,
        "production_effect": "none",
        "broker_action": "none",
    }:
        _fail("SESSION_FINALIZATION_EXTERNAL_POLICY_SAFETY_INVALID")
    return value


def _load_json_object(path: Path, *, label: str) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                _fail("SESSION_FINALIZATION_EXTERNAL_JSON_DUPLICATE_KEY", f"{label}.{key}")
            result[key] = value
        return result

    try:
        value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicates)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        _fail("SESSION_FINALIZATION_EXTERNAL_JSON_INVALID", f"{label}:{exc}")
    return _mapping(value, field=label)


def _load_proposal_package(
    *, project_root: Path, policy: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    root = project_root.resolve()
    portable = str(policy["proposal_package_root"])
    package_root = (root / portable).resolve()
    try:
        if package_root.relative_to(root).as_posix() != portable:
            _fail("SESSION_FINALIZATION_EXTERNAL_PACKAGE_PATH_INVALID")
    except ValueError:
        _fail("SESSION_FINALIZATION_EXTERNAL_PACKAGE_PATH_INVALID")
    scope = _load_json_object(package_root / "execution_scope.json", label="scope")
    manifest = _load_json_object(package_root / "package_manifest.json", label="manifest")
    _verify_seal(scope, label="scope")
    _verify_seal(manifest, label="manifest")
    if scope["content_sha256"] != policy["proposal_scope_content_sha256"]:
        _fail("SESSION_FINALIZATION_EXTERNAL_SCOPE_IDENTITY_MISMATCH")
    if manifest["content_sha256"] != policy["proposal_package_content_sha256"]:
        _fail("SESSION_FINALIZATION_EXTERNAL_PACKAGE_IDENTITY_MISMATCH")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list) or manifest.get("artifact_count") != len(artifacts):
        _fail("SESSION_FINALIZATION_EXTERNAL_PACKAGE_ARTIFACT_INVALID")
    expected_names = {"execution_scope.json", "owner_decision_request.md"}
    observed_names: set[str] = set()
    for raw_identity in artifacts:
        identity = _mapping(raw_identity, field="artifact")
        name = identity.get("relative_path")
        if not isinstance(name, str) or name in observed_names or name not in expected_names:
            _fail("SESSION_FINALIZATION_EXTERNAL_PACKAGE_ARTIFACT_INVALID", str(name))
        observed_names.add(name)
        raw = (package_root / name).read_bytes()
        if (
            identity.get("byte_count") != len(raw)
            or identity.get("sha256") != sha256(raw).hexdigest()
        ):
            _fail("SESSION_FINALIZATION_EXTERNAL_PACKAGE_ARTIFACT_DRIFT", name)
    if observed_names != expected_names:
        _fail("SESSION_FINALIZATION_EXTERNAL_PACKAGE_ARTIFACT_INVALID")
    if (
        manifest.get("external_action_authorized") is not False
        or manifest.get("external_action_performed") is not False
    ):
        _fail("SESSION_FINALIZATION_EXTERNAL_PACKAGE_ACTION_INVALID")
    source = validate_session_finalization_package(project_root=root)
    frozen = _mapping(scope.get("frozen_inputs"), field="frozen_inputs")
    if (
        frozen.get("policy_file_sha256") != source.policy_file_sha256
        or frozen.get("policy_canonical_sha256") != source.policy_canonical_sha256
        or frozen.get("contract_content_sha256") != source.contract["content_sha256"]
        or frozen.get("project_code_lf_byte_count") != len(source.project_code_bytes)
        or frozen.get("project_code_lf_sha256") != sha256(source.project_code_bytes).hexdigest()
    ):
        _fail("SESSION_FINALIZATION_EXTERNAL_SOURCE_PACKAGE_DRIFT")
    return scope, manifest


def _load_admission_identity_contract(
    *, project_root: Path, policy: Mapping[str, Any]
) -> dict[str, Any]:
    root = project_root.resolve()

    def resolve_portable_file(policy_key: str) -> Path:
        portable = str(policy[policy_key])
        candidate = (root / portable).resolve()
        try:
            if candidate.relative_to(root).as_posix() != portable:
                _fail("SESSION_FINALIZATION_EXTERNAL_IDENTITY_PATH_INVALID", policy_key)
        except ValueError:
            _fail("SESSION_FINALIZATION_EXTERNAL_IDENTITY_PATH_INVALID", policy_key)
        if not candidate.is_file():
            _fail("SESSION_FINALIZATION_EXTERNAL_IDENTITY_PATH_INVALID", policy_key)
        return candidate

    contract_path = resolve_portable_file("admission_identity_contract_path")
    request_path = resolve_portable_file("admission_identity_request_path")
    contract = _load_json_object(contract_path, label="admission_identity_contract")
    _verify_seal(contract, label="admission_identity_contract")
    if contract.get("content_sha256") != policy["admission_identity_contract_content_sha256"]:
        _fail("SESSION_FINALIZATION_EXTERNAL_IDENTITY_CONTRACT_MISMATCH")
    request_bytes = request_path.read_bytes()
    if sha256(request_bytes).hexdigest() != policy["admission_identity_request_sha256"]:
        _fail("SESSION_FINALIZATION_EXTERNAL_IDENTITY_REQUEST_MISMATCH")
    binding = _mapping(contract.get("admission_main_binding"), field="admission_main_binding")
    expected_binding = {
        "field_name": "ordinary_pushed_admission_main_sha",
        "required_format": "LOWERCASE_GIT_SHA1_40",
        "required_relation": "LOCAL_MAIN_EQUALS_ORIGIN_MAIN_EQUALS_OWNER_TOKEN_FIELD",
        "must_differ_from_proposal_publication_main": True,
        "post_token_tracked_mutation_allowed": False,
    }
    if (
        contract.get("schema_version")
        != "qc_qqq_options_session_finalization_external_validation_admission_identity_contract.v2"
        or contract.get("task_id") != policy["task_id"]
        or contract.get("contract_status") != "FROZEN_PROPOSAL_IDENTITY_AND_DYNAMIC_ADMISSION_MAIN"
        or contract.get("proposal_publication_main_sha")
        != policy["ordinary_pushed_proposal_main_sha"]
        or contract.get("original_proposal_package_content_sha256")
        != policy["proposal_package_content_sha256"]
        or contract.get("owner_decision_exact") != policy["owner_decision"]
        or contract.get("owner_token_source") != policy["owner_token_source"]
        or binding != expected_binding
        or contract.get("maximum_project_mutations") != 1
        or contract.get("maximum_cloud_backtests") != 1
        or contract.get("maximum_orders") != 0
        or contract.get("maximum_fills") != 0
        or contract.get("authorization_single_use") is not True
        or contract.get("authorization_invalidates_on_first_run_attempt") is not True
        or contract.get("external_action_authorized") is not False
        or contract.get("production_effect") != "none"
        or contract.get("broker_action") != "none"
    ):
        _fail("SESSION_FINALIZATION_EXTERNAL_IDENTITY_CONTRACT_INVALID")
    baseline_main = contract.get("baseline_admission_implementation_main_sha")
    if not isinstance(baseline_main, str) or re.fullmatch(r"[0-9a-f]{40}", baseline_main) is None:
        _fail("SESSION_FINALIZATION_EXTERNAL_IDENTITY_CONTRACT_INVALID")
    return contract


def _expected_token_fields(policy: Mapping[str, Any]) -> dict[str, str]:
    return {
        "proposal_publication_main_sha": str(policy["ordinary_pushed_proposal_main_sha"]),
        "admission_identity_contract_content_sha256": str(
            policy["admission_identity_contract_content_sha256"]
        ),
        "registration_base_repository_code_sha": str(
            policy["registration_base_repository_code_sha"]
        ),
        "policy_file_sha256": str(policy["policy_file_sha256"]),
        "policy_canonical_sha256": str(policy["policy_canonical_sha256"]),
        "contract_content_sha256": str(policy["contract_content_sha256"]),
        "contract_canonical_sha256": str(policy["contract_canonical_sha256"]),
        "project_code_lf_byte_count": str(policy["project_code_lf_byte_count"]),
        "project_code_lf_sha256": str(policy["project_code_lf_sha256"]),
        "predecessor_evidence_content_sha256": str(policy["predecessor_evidence_content_sha256"]),
        "predecessor_results_sha256": str(policy["predecessor_results_sha256"]),
        "package_manifest_content_sha256": str(policy["proposal_package_content_sha256"]),
        "target_project_id": str(policy["target_project_id"]),
        "requested_range": f"{policy['requested_start']}..{policy['requested_end']}",
        "expected_session_count": str(policy["expected_session_count"]),
        "maximum_project_mutations": str(policy["maximum_project_mutations"]),
        "maximum_cloud_backtests": str(policy["maximum_cloud_backtests"]),
        "maximum_orders": str(policy["maximum_orders"]),
        "maximum_fills": str(policy["maximum_fills"]),
        "collector": str(policy["collector_id"]),
        "independent_reviewer": str(policy["independent_reviewer_id"]),
        "authorization_single_use": "true",
        "authorization_invalidates_on_first_run_attempt": "true",
    }


@dataclass(frozen=True)
class OwnerTokenCandidate:
    token_text: str
    fields: Mapping[str, str]
    token_sha256: str
    token_byte_count: int
    expires_at_utc: str


@dataclass(frozen=True)
class OwnerAuthorizationAdmissionReceipt:
    payload: Mapping[str, Any]

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.payload)

    @property
    def content_sha256(self) -> str:
        return _content_sha256(self.payload)


@dataclass(frozen=True)
class RunAttemptConsumptionReceipt:
    payload: Mapping[str, Any]

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.payload)

    @property
    def content_sha256(self) -> str:
        return _content_sha256(self.payload)


@dataclass(frozen=True)
class ExportSafeSessionFinalizationEvidence:
    payload: Mapping[str, Any]

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.payload)

    @property
    def content_sha256(self) -> str:
        return _content_sha256(self.payload)


@dataclass(frozen=True)
class ExternalActionLedger:
    payload: Mapping[str, Any]

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.payload)

    @property
    def content_sha256(self) -> str:
        return _content_sha256(self.payload)


@dataclass(frozen=True)
class ExecutionEvidenceManifest:
    payload: Mapping[str, Any]

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.payload)

    @property
    def content_sha256(self) -> str:
        return _content_sha256(self.payload)


def validate_owner_token_candidate(
    *, owner_token_bytes: bytes, project_root: Path = PROJECT_ROOT
) -> OwnerTokenCandidate:
    if owner_token_bytes.endswith(b"\n") or b"\r" in owner_token_bytes:
        _fail("SESSION_FINALIZATION_EXTERNAL_TOKEN_CANONICAL_LF_REQUIRED")
    try:
        text = owner_token_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        _fail("SESSION_FINALIZATION_EXTERNAL_TOKEN_UTF8_REQUIRED", str(exc))
    policy = _load_policy(project_root)
    _load_proposal_package(project_root=project_root, policy=policy)
    _load_admission_identity_contract(project_root=project_root, policy=policy)
    lines = text.split("\n")
    if len(lines) != 1 + len(_FIELD_ORDER) or lines[0] != policy["owner_decision"]:
        _fail("SESSION_FINALIZATION_EXTERNAL_TOKEN_DECISION_OR_LINE_COUNT_INVALID")
    fields: dict[str, str] = {}
    order: list[str] = []
    for line in lines[1:]:
        if ":" not in line:
            _fail("SESSION_FINALIZATION_EXTERNAL_TOKEN_FIELD_INVALID", line)
        key, value = line.split(":", 1)
        if not key or not value or key in fields:
            _fail("SESSION_FINALIZATION_EXTERNAL_TOKEN_FIELD_INVALID", key)
        order.append(key)
        fields[key] = value
    if tuple(order) != _FIELD_ORDER:
        _fail("SESSION_FINALIZATION_EXTERNAL_TOKEN_FIELD_ORDER_INVALID")
    expected = _expected_token_fields(policy)
    wrong = sorted(key for key, value in expected.items() if fields.get(key) != value)
    if wrong:
        _fail("SESSION_FINALIZATION_EXTERNAL_TOKEN_SCOPE_OR_HASH_MISMATCH", ",".join(wrong))
    admission_main = fields["ordinary_pushed_admission_main_sha"]
    if re.fullmatch(r"[0-9a-f]{40}", admission_main) is None:
        _fail("SESSION_FINALIZATION_EXTERNAL_ADMISSION_MAIN_FORMAT_INVALID")
    if admission_main == fields["proposal_publication_main_sha"]:
        _fail("SESSION_FINALIZATION_EXTERNAL_ADMISSION_MAIN_EQUALS_PROPOSAL")
    expires_at = _parse_utc(fields["authorization_expires_at_utc"], field="expiry")
    decision_start = datetime.fromisoformat(str(policy["owner_decision_date"])).replace(tzinfo=UTC)
    if expires_at <= decision_start or expires_at > decision_start + timedelta(
        hours=int(policy["authorization_expires_after_hours_maximum"])
    ):
        _fail("SESSION_FINALIZATION_EXTERNAL_TOKEN_EXPIRY_OUTSIDE_WINDOW")
    return OwnerTokenCandidate(
        token_text=text,
        fields=fields,
        token_sha256=sha256(owner_token_bytes).hexdigest(),
        token_byte_count=len(owner_token_bytes),
        expires_at_utc=fields["authorization_expires_at_utc"],
    )


def admit_owner_authorization(
    *,
    owner_token_bytes: bytes,
    owner_token_source: str,
    reviewed_at_utc: str,
    local_main_sha: str,
    origin_main_sha: str,
    project_root: Path = PROJECT_ROOT,
) -> OwnerAuthorizationAdmissionReceipt:
    policy = _load_policy(project_root)
    candidate = validate_owner_token_candidate(
        owner_token_bytes=owner_token_bytes, project_root=project_root
    )
    if (
        policy["policy_status"] != "AWAITING_EXACT_OWNER_TOKEN_DIRECT_ADMISSION"
        or policy["owner_token_status"] != "PENDING_DIRECT_RUNTIME_ADMISSION"
        or policy["owner_token_sha256"] is not None
        or policy["owner_token_byte_count"] is not None
        or policy["authorization_expires_at_utc"] is not None
    ):
        _fail("SESSION_FINALIZATION_EXTERNAL_DIRECT_ADMISSION_POLICY_INVALID")
    if owner_token_source != policy["owner_token_source"]:
        _fail("SESSION_FINALIZATION_EXTERNAL_OWNER_TOKEN_SOURCE_INVALID")
    expected_main = candidate.fields["ordinary_pushed_admission_main_sha"]
    if local_main_sha != expected_main or origin_main_sha != expected_main:
        _fail("SESSION_FINALIZATION_EXTERNAL_PUBLISHED_MAIN_MISMATCH")
    reviewed_at = _parse_utc(reviewed_at_utc, field="reviewed_at_utc")
    expires_at = _parse_utc(candidate.expires_at_utc, field="expiry")
    decision_start = datetime.fromisoformat(str(policy["owner_decision_date"])).replace(tzinfo=UTC)
    if reviewed_at < decision_start or reviewed_at >= expires_at:
        _fail("SESSION_FINALIZATION_EXTERNAL_REVIEW_TIME_OUTSIDE_WINDOW")
    body: dict[str, Any] = {
        "schema_version": (
            "qc_qqq_options_session_finalization_external_owner_admission_receipt.v1"
        ),
        "task_id": policy["task_id"],
        "predecessor_task_id": policy["predecessor_task_id"],
        "status": "OWNER_AUTHORIZATION_ADMITTED_UNUSED",
        "owner_token_source": owner_token_source,
        "owner_token_sha256": candidate.token_sha256,
        "owner_token_byte_count": candidate.token_byte_count,
        "reviewed_at_utc": reviewed_at_utc,
        "expires_at_utc": candidate.expires_at_utc,
        "proposal_publication_main_sha": policy["ordinary_pushed_proposal_main_sha"],
        "ordinary_pushed_admission_main_sha": expected_main,
        "admission_identity_contract_content_sha256": policy[
            "admission_identity_contract_content_sha256"
        ],
        "proposal_scope_content_sha256": policy["proposal_scope_content_sha256"],
        "proposal_package_content_sha256": policy["proposal_package_content_sha256"],
        "project_code_lf_sha256": policy["project_code_lf_sha256"],
        "target_project_id": policy["target_project_id"],
        "requested_range": candidate.fields["requested_range"],
        "expected_session_count": policy["expected_session_count"],
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
    body["content_sha256"] = _content_sha256(body)
    return OwnerAuthorizationAdmissionReceipt(body)


def consume_on_first_run_attempt(
    *,
    admission: OwnerAuthorizationAdmissionReceipt,
    attempted_at_utc: str,
    project_id: int,
    project_code_lf_sha256: str,
    backtest_id: str,
    attempt_status: str,
    prior_consumption: RunAttemptConsumptionReceipt | None = None,
) -> RunAttemptConsumptionReceipt:
    _verify_seal(admission.payload, label="authorization_admission")
    if prior_consumption is not None:
        _verify_seal(prior_consumption.payload, label="prior_run_attempt_consumption")
        _fail("SESSION_FINALIZATION_EXTERNAL_AUTHORIZATION_ALREADY_CONSUMED")
    admitted = admission.payload
    if admitted.get("status") != "OWNER_AUTHORIZATION_ADMITTED_UNUSED" or admitted.get(
        "authorization_consumed"
    ):
        _fail("SESSION_FINALIZATION_EXTERNAL_AUTHORIZATION_NOT_UNUSED")
    attempted_at = _parse_utc(attempted_at_utc, field="attempted_at_utc")
    if attempted_at >= _parse_utc(str(admitted["expires_at_utc"]), field="expiry"):
        _fail("SESSION_FINALIZATION_EXTERNAL_AUTHORIZATION_EXPIRED")
    if project_id != admitted.get("target_project_id"):
        _fail("SESSION_FINALIZATION_EXTERNAL_PROJECT_MISMATCH")
    if project_code_lf_sha256 != admitted.get("project_code_lf_sha256"):
        _fail("SESSION_FINALIZATION_EXTERNAL_PROJECT_CODE_MISMATCH")
    if not backtest_id or attempt_status not in {"SUBMITTED", "COMPLETED", "FAILED"}:
        _fail("SESSION_FINALIZATION_EXTERNAL_RUN_ATTEMPT_INVALID")
    body: dict[str, Any] = {
        "schema_version": (
            "qc_qqq_options_session_finalization_external_run_consumption_receipt.v1"
        ),
        "task_id": admitted["task_id"],
        "authorization_admission_content_sha256": admission.content_sha256,
        "attempted_at_utc": attempted_at_utc,
        "project_id": project_id,
        "project_code_lf_sha256": project_code_lf_sha256,
        "backtest_id": backtest_id,
        "attempt_status": attempt_status,
        "project_mutation_count": 1,
        "cloud_backtest_attempt_count": 1,
        "maximum_project_mutations": 1,
        "maximum_cloud_backtests": 1,
        "orders": 0,
        "fills": 0,
        "authorization_consumed": True,
        "authorization_invalidated_for_further_attempts": True,
        "second_project_mutation_authorized": False,
        "second_cloud_run_authorized": False,
        "selection_authorized": False,
        "engine_status": "POLICY_BLOCKED_CASH_PRESERVATION",
        "production_effect": "none",
        "broker_action": "none",
    }
    body["content_sha256"] = _content_sha256(body)
    return RunAttemptConsumptionReceipt(body)


def _result_object(raw: bytes) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                _fail("SESSION_FINALIZATION_RESULTS_DUPLICATE_JSON_KEY", key)
            result[key] = value
        return result

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        _fail("SESSION_FINALIZATION_RESULTS_JSON_INVALID", str(exc))
    return _mapping(value, field="result")


def _reject_prohibited_result_keys(value: object, *, path: str = "result") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = re.sub(r"[^a-z0-9]", "", str(key).casefold())
            if normalized in _PROHIBITED_RESULT_KEYS:
                _fail("SESSION_FINALIZATION_RESULTS_PROHIBITED_CARRIER", f"{path}.{key}")
            _reject_prohibited_result_keys(child, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_prohibited_result_keys(child, path=f"{path}[{index}]")


def _nonnegative_integer(value: object, *, field: str) -> int:
    if isinstance(value, bool):
        _fail("SESSION_FINALIZATION_RESULTS_COUNT_INVALID", field)
    try:
        parsed = int(str(value))
    except ValueError:
        _fail("SESSION_FINALIZATION_RESULTS_COUNT_INVALID", field)
    if str(parsed) != str(value).strip() or parsed < 0:
        _fail("SESSION_FINALIZATION_RESULTS_COUNT_INVALID", field)
    return parsed


def _zero_currency(value: object, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        _fail("SESSION_FINALIZATION_RESULTS_ZERO_REQUIRED", field)
    rendered = str(value).strip().replace("$", "").replace(",", "")
    try:
        parsed = float(rendered)
    except ValueError:
        _fail("SESSION_FINALIZATION_RESULTS_ZERO_REQUIRED", field)
    if parsed != 0:
        _fail("SESSION_FINALIZATION_RESULTS_ZERO_REQUIRED", field)


def _terminal_fields(value: object, *, expected_sessions: int) -> dict[str, str]:
    if not isinstance(value, str):
        _fail("SESSION_FINALIZATION_RESULTS_TERMINAL_INVALID")
    fields: dict[str, str] = {}
    for item in value.split("|"):
        if "=" not in item:
            _fail("SESSION_FINALIZATION_RESULTS_TERMINAL_INVALID")
        key, rendered = item.split("=", 1)
        if not key or key in fields:
            _fail("SESSION_FINALIZATION_RESULTS_TERMINAL_INVALID")
        fields[key] = rendered
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
        "stale_underlying_fallback",
    }
    if set(fields) != expected_keys:
        _fail("SESSION_FINALIZATION_RESULTS_TERMINAL_INVALID")
    observed = _nonnegative_integer(fields["observed_sessions"], field="terminal.observed_sessions")
    if (
        fields["status"] != "COMPLETE"
        or fields["expected_sessions"] != str(expected_sessions)
        or observed != expected_sessions
        or fields["orders"] != "0"
        or fields["fills"] != "0"
        or fields["portfolio_invested"] != "false"
        or fields["raw_rows"] != "false"
        or fields["logs_as_data"] != "false"
        or fields["object_store"] != "false"
        or fields["stale_underlying_fallback"] != "false"
    ):
        _fail("SESSION_FINALIZATION_RESULTS_TERMINAL_INVALID")
    return fields


def validate_results_json(
    *,
    result_bytes: bytes,
    admission: OwnerAuthorizationAdmissionReceipt,
    consumption: RunAttemptConsumptionReceipt,
    collected_at_utc: str,
    backtest_id: str,
    project_root: Path = PROJECT_ROOT,
) -> ExportSafeSessionFinalizationEvidence:
    _verify_seal(admission.payload, label="authorization_admission")
    _verify_seal(consumption.payload, label="run_attempt_consumption")
    policy = _load_policy(project_root)
    admitted = admission.payload
    consumed = consumption.payload
    if (
        admitted.get("status") != "OWNER_AUTHORIZATION_ADMITTED_UNUSED"
        or consumed.get("authorization_consumed") is not True
        or consumed.get("backtest_id") != backtest_id
        or consumed.get("project_id") != admitted.get("target_project_id")
        or consumed.get("project_code_lf_sha256") != admitted.get("project_code_lf_sha256")
    ):
        _fail("SESSION_FINALIZATION_RESULTS_CONSUMPTION_BINDING_INVALID")
    collected_at = _parse_utc(collected_at_utc, field="collected_at_utc")
    attempted_at = _parse_utc(str(consumed["attempted_at_utc"]), field="attempted_at_utc")
    if collected_at < attempted_at:
        _fail("SESSION_FINALIZATION_RESULTS_COLLECTION_PRECEDES_ATTEMPT")
    payload = _result_object(result_bytes)
    _reject_prohibited_result_keys(payload)
    state = _mapping(payload.get("state"), field="state")
    if (
        state.get("Status") != "Completed"
        or state.get("RuntimeError") not in (None, "")
        or str(state.get("OrderCount")) != "0"
    ):
        _fail("SESSION_FINALIZATION_RESULTS_STATE_INVALID")
    orders = payload.get("orders")
    if orders not in ({}, [], None):
        _fail("SESSION_FINALIZATION_RESULTS_ORDERS_NOT_EMPTY")
    statistics = _mapping(payload.get("statistics"), field="statistics")
    _zero_currency(statistics.get("Total Orders"), field="statistics.Total Orders")
    _zero_currency(statistics.get("Total Fees"), field="statistics.Total Fees")
    configuration = _mapping(payload.get("algorithmConfiguration"), field="algorithmConfiguration")
    if not str(configuration.get("startDate", "")).startswith(policy["requested_start"]):
        _fail("SESSION_FINALIZATION_RESULTS_RANGE_INVALID", "start")
    if not str(configuration.get("endDate", "")).startswith(policy["requested_end"]):
        _fail("SESSION_FINALIZATION_RESULTS_RANGE_INVALID", "end")
    runtime = _mapping(payload.get("runtimeStatistics"), field="runtimeStatistics")
    aggregate_keys = (
        set(_AXIS_KEYS)
        | set(_DIAGNOSTIC_KEYS)
        | {
            "TRADING2531_IDENTITY",
            "TRADING2531_TERMINAL",
        }
    )
    observed_aggregate_keys = {key for key in runtime if key.startswith("TRADING2531_")}
    if observed_aggregate_keys != aggregate_keys:
        _fail("SESSION_FINALIZATION_RESULTS_AGGREGATE_KEYSET_INVALID")
    expected_identity = (
        "schema=qc_qqq_options_daily_transport_per_axis_runtime.v2"
        f"|contract={policy['contract_content_sha256']}"
    )
    if runtime["TRADING2531_IDENTITY"] != expected_identity:
        _fail("SESSION_FINALIZATION_RESULTS_IDENTITY_INVALID")
    terminal = _terminal_fields(
        runtime["TRADING2531_TERMINAL"],
        expected_sessions=int(policy["expected_session_count"]),
    )
    axis_counts = {key: _nonnegative_integer(runtime[key], field=key) for key in _AXIS_KEYS}
    diagnostic_counts = {
        key: _nonnegative_integer(runtime[key], field=key) for key in _DIAGNOSTIC_KEYS
    }
    expected_sessions = int(policy["expected_session_count"])
    for axis in _AXES:
        total = sum(axis_counts[f"TRADING2531_{axis}_{status}_SESSIONS"] for status in _STATUSES)
        if total != expected_sessions:
            _fail("SESSION_FINALIZATION_RESULTS_AXIS_TOTAL_INVALID", axis)
    chain_present = axis_counts["TRADING2531_OPTION_CHAIN_PRESENCE_PRESENT_SESSIONS"]
    chain_missing = axis_counts["TRADING2531_OPTION_CHAIN_PRESENCE_MISSING_SESSIONS"]
    never_chain = diagnostic_counts["TRADING2531_SESSIONS_NEVER_CHAIN"]
    if (
        chain_present + chain_missing != expected_sessions
        or never_chain != chain_missing
        or axis_counts["TRADING2531_OPTION_CHAIN_PRESENCE_INVALID_SESSIONS"] != 0
        or axis_counts["TRADING2531_OPTION_CHAIN_PRESENCE_NOT_EVALUATED_SESSIONS"] != 0
    ):
        _fail("SESSION_FINALIZATION_RESULTS_CHAIN_PARTITION_INVALID")
    equity_total = sum(
        diagnostic_counts[f"TRADING2531_SESSIONS_WITH_CANONICAL_EQUITY_{status}"]
        for status in ("PRESENT", "MISSING", "INVALID")
    )
    if equity_total != chain_present:
        _fail("SESSION_FINALIZATION_RESULTS_EQUITY_PARTITION_INVALID")
    for status in ("PRESENT", "MISSING", "INVALID"):
        if (
            axis_counts[f"TRADING2531_UNDERLYING_PRICE_{status}_SESSIONS"]
            != diagnostic_counts[f"TRADING2531_SESSIONS_WITH_CANONICAL_EQUITY_{status}"]
        ):
            _fail("SESSION_FINALIZATION_RESULTS_EQUITY_BINDING_INVALID", status)
    if axis_counts["TRADING2531_UNDERLYING_PRICE_NOT_EVALUATED_SESSIONS"] != never_chain:
        _fail("SESSION_FINALIZATION_RESULTS_EQUITY_BINDING_INVALID", "NOT_EVALUATED")
    for axis in _AXES[2:]:
        if axis_counts[f"TRADING2531_{axis}_NOT_EVALUATED_SESSIONS"] != never_chain:
            _fail("SESSION_FINALIZATION_RESULTS_NOT_EVALUATED_PARTITION_INVALID", axis)
    if (
        diagnostic_counts["TRADING2531_CHAINLESS_SLICE_EVENTS"]
        < diagnostic_counts["TRADING2531_SESSIONS_WITH_CHAINLESS_SLICE"]
        or diagnostic_counts["TRADING2531_SESSIONS_RECOVERED_AFTER_CHAINLESS"]
        > min(
            chain_present,
            diagnostic_counts["TRADING2531_SESSIONS_WITH_CHAINLESS_SLICE"],
        )
        or diagnostic_counts["TRADING2531_SESSIONS_WITH_MULTIPLE_CHAIN_EVENTS"] > chain_present
        or diagnostic_counts["TRADING2531_SESSIONS_WITH_CONTRACT_ZERO_IGNORED"] > chain_present
    ):
        _fail("SESSION_FINALIZATION_RESULTS_DIAGNOSTIC_INVARIANT_INVALID")
    body: dict[str, Any] = {
        "schema_version": ("qc_qqq_options_session_finalization_v2_export_safe_evidence.v1"),
        "task_id": policy["task_id"],
        "status": "EXPORT_SAFE_SESSION_FINALIZATION_V2_AGGREGATES_COLLECTED",
        "authorization_admission_content_sha256": admission.content_sha256,
        "run_attempt_consumption_content_sha256": consumption.content_sha256,
        "source_result_file_sha256": sha256(result_bytes).hexdigest(),
        "source_result_byte_count": len(result_bytes),
        "collected_at_utc": collected_at_utc,
        "backtest_id": backtest_id,
        "project_id": admitted["target_project_id"],
        "project_code_lf_sha256": admitted["project_code_lf_sha256"],
        "requested_range": admitted["requested_range"],
        "expected_session_count": expected_sessions,
        "observed_session_count": int(terminal["observed_sessions"]),
        "per_axis_status_session_counts": axis_counts,
        "diagnostic_counts": diagnostic_counts,
        "orders": 0,
        "fills": 0,
        "raw_rows_collected": False,
        "logs_as_data_collected": False,
        "object_store_used": False,
        "stale_underlying_fallback_used": False,
        "dq_pit_admission_authorized": False,
        "selection_authorized": False,
        "investment_conclusion_authorized": False,
        "engine_status": "POLICY_BLOCKED_CASH_PRESERVATION",
        "production_effect": "none",
        "broker_action": "none",
    }
    body["content_sha256"] = _content_sha256(body)
    return ExportSafeSessionFinalizationEvidence(body)


def build_external_action_ledger(
    *,
    admission: OwnerAuthorizationAdmissionReceipt,
    login_observed_at_utc: str,
    mutation_started_at_utc: str,
    mutation_verified_at_utc: str,
    consumption: RunAttemptConsumptionReceipt,
    evidence: ExportSafeSessionFinalizationEvidence | None = None,
) -> ExternalActionLedger:
    _verify_seal(admission.payload, label="authorization_admission")
    _verify_seal(consumption.payload, label="run_attempt_consumption")
    if evidence is not None:
        _verify_seal(evidence.payload, label="export_safe_aggregate_evidence")
    login = _parse_utc(login_observed_at_utc, field="login_observed_at_utc")
    mutation_started = _parse_utc(mutation_started_at_utc, field="mutation_started_at_utc")
    mutation_verified = _parse_utc(mutation_verified_at_utc, field="mutation_verified_at_utc")
    attempted = _parse_utc(str(consumption.payload["attempted_at_utc"]), field="attempted_at_utc")
    if not login <= mutation_started <= mutation_verified <= attempted:
        _fail("SESSION_FINALIZATION_EXTERNAL_ACTION_ORDER_INVALID")
    if consumption.payload.get("authorization_admission_content_sha256") != (
        admission.content_sha256
    ):
        _fail("SESSION_FINALIZATION_EXTERNAL_ACTION_BINDING_INVALID")
    if (
        evidence is not None
        and evidence.payload.get("run_attempt_consumption_content_sha256")
        != consumption.content_sha256
    ):
        _fail("SESSION_FINALIZATION_EXTERNAL_ACTION_BINDING_INVALID")
    actions: list[dict[str, Any]] = [
        {"ordinal": 1, "action": "LOGIN_OBSERVED", "at_utc": login_observed_at_utc},
        {
            "ordinal": 2,
            "action": "PROJECT_MUTATION_STARTED",
            "at_utc": mutation_started_at_utc,
        },
        {
            "ordinal": 3,
            "action": "PROJECT_MUTATION_VERIFIED",
            "at_utc": mutation_verified_at_utc,
        },
        {
            "ordinal": 4,
            "action": "CLOUD_RUN_ATTEMPT_CONSUMED_AUTHORIZATION",
            "at_utc": consumption.payload["attempted_at_utc"],
            "backtest_id": consumption.payload["backtest_id"],
        },
    ]
    if evidence is not None:
        actions.append(
            {
                "ordinal": 5,
                "action": "EXPORT_SAFE_AGGREGATE_EVIDENCE_VALIDATED",
                "at_utc": evidence.payload["collected_at_utc"],
                "evidence_content_sha256": evidence.content_sha256,
            }
        )
    body: dict[str, Any] = {
        "schema_version": "qc_qqq_options_session_finalization_external_action_ledger.v1",
        "task_id": admission.payload["task_id"],
        "status": (
            "EXTERNAL_ACTION_LIFECYCLE_COMPLETE"
            if evidence is not None
            else "EXTERNAL_ACTION_ATTEMPT_CONSUMED_EVIDENCE_PENDING"
        ),
        "authorization_admission_content_sha256": admission.content_sha256,
        "run_attempt_consumption_content_sha256": consumption.content_sha256,
        "actions": actions,
        "project_mutation_count": 1,
        "cloud_backtest_attempt_count": 1,
        "orders": 0,
        "fills": 0,
        "second_attempt_authorized": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    body["content_sha256"] = _content_sha256(body)
    return ExternalActionLedger(body)


def build_execution_evidence_manifest(
    *,
    admission: OwnerAuthorizationAdmissionReceipt,
    consumption: RunAttemptConsumptionReceipt,
    action_ledger: ExternalActionLedger,
    evidence: ExportSafeSessionFinalizationEvidence | None = None,
) -> ExecutionEvidenceManifest:
    _verify_seal(admission.payload, label="authorization_admission")
    _verify_seal(consumption.payload, label="run_attempt_consumption")
    _verify_seal(action_ledger.payload, label="external_action_ledger")
    if evidence is not None:
        _verify_seal(evidence.payload, label="export_safe_aggregate_evidence")
    if action_ledger.payload.get("authorization_admission_content_sha256") != (
        admission.content_sha256
    ) or action_ledger.payload.get("run_attempt_consumption_content_sha256") != (
        consumption.content_sha256
    ):
        _fail("SESSION_FINALIZATION_EXTERNAL_MANIFEST_BINDING_INVALID")
    artifacts: dict[str, str] = {
        "authorization_admission.json": admission.content_sha256,
        "run_attempt_consumption_receipt.json": consumption.content_sha256,
        "external_action_ledger.json": action_ledger.content_sha256,
    }
    if evidence is not None:
        artifacts["export_safe_aggregate_evidence.json"] = evidence.content_sha256
    body: dict[str, Any] = {
        "schema_version": ("qc_qqq_options_session_finalization_external_execution_manifest.v1"),
        "task_id": admission.payload["task_id"],
        "status": (
            "EXECUTION_EVIDENCE_COMPLETE"
            if evidence is not None
            else "RUN_ATTEMPT_CONSUMED_RESULT_EVIDENCE_PENDING"
        ),
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
    body["content_sha256"] = _content_sha256(body)
    return ExecutionEvidenceManifest(body)


__all__ = [
    "DEFAULT_POLICY_PATH",
    "ExecutionEvidenceManifest",
    "ExportSafeSessionFinalizationEvidence",
    "ExternalActionLedger",
    "OwnerAuthorizationAdmissionReceipt",
    "OwnerTokenCandidate",
    "PROJECT_ROOT",
    "RunAttemptConsumptionReceipt",
    "SessionFinalizationExternalValidationError",
    "admit_owner_authorization",
    "build_execution_evidence_manifest",
    "build_external_action_ledger",
    "consume_on_first_run_attempt",
    "validate_owner_token_candidate",
    "validate_results_json",
]
