from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

TASK_ID = "TRADING-2465_POST_O1_ROUTE_DECISION_AND_BLIND_REENTRY_PREREGISTRATION"
SCHEMA_VERSION = "o1_relative_opportunity_reentry_preregistration_proposal.v1"
PROPOSAL_STATUS = "OWNER_REVIEW_REQUIRED_NOT_ACTIVE"
HISTORICAL_OWNER_GATE_STATUS = "BLOCKED_OWNER_INPUT"
CURRENT_TASK_STATUS = "BASELINE_DONE"
OWNER_DECISION_TOKEN = (
    "owner_decision:TRADING-2465:2026-07-30:"
    "select_o1_blind_calendar_reentry_with_generic_evidence_infrastructure_v1"
)
SUCCESSOR_TASK_ID = "TRADING-2467_O1_BLIND_CALENDAR_REENTRY_POLICY_SLOT_FREEZE"
PLANNING_SNAPSHOT = "fb4687244e04228ae2e5c4dd425f82cb1e35291c"
SOURCE_POLICY_ID = "TRADING_2464_O1_CAPABILITY_AUDIT_V1"
SOURCE_POLICY_STATUS = "CLOSED_INSUFFICIENT_COVERAGE_OR_DQ"
SOURCE_POLICY_PATH = "config/research/o1_relative_opportunity_capability_audit_v1.yaml"
REQUIREMENT_PATH = (
    "docs/requirements/TRADING-2465_Post_O1_Route_Decision_And_Blind_Reentry_Preregistration.md"
)
PROPOSAL_PATH = "config/research/o1_relative_opportunity_reentry_preregistration_v1_proposal.yaml"
TASK_REGISTER_PATH = "docs/task_register.md"

IMMUTABLE_SECTIONS = (
    "target_contract",
    "split_contract",
    "coverage_contract",
    "regime_contract",
    "event_contract",
    "model_feature_contract",
    "metric_contract",
    "falsification_contract",
    "classification_contract",
)

EMPIRICAL_AUTHORIZATION_KEYS = (
    "route_selected",
    "new_result_read_allowed",
    "data_acquisition_allowed",
    "dq_execution_allowed",
    "coverage_run_allowed",
    "model_training_allowed",
    "canonical_run_allowed",
    "falsification_execution_allowed",
    "o2_foundation_execution_allowed",
    "o3_foundation_execution_allowed",
)

DOWNSTREAM_FALSE_KEYS = (
    "prospective_accessed",
    "decision_value_audit_started",
    "risk_overlay_created",
    "candidate_family_created",
    "strategy_backtest_executed",
    "target_weights_generated",
    "qld_automatic_selection_enabled",
    "paper_shadow_changed",
    "promotion_allowed",
)


@dataclass(frozen=True)
class ValidationResult:
    status: str
    task_id: str
    proposal_path: str
    error_count: int
    errors: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "trading2465_reentry_preregistration_validation.v1",
            "status": self.status,
            "task_id": self.task_id,
            "proposal_path": self.proposal_path,
            "error_count": self.error_count,
            "errors": list(self.errors),
            "production_effect": "none",
            "broker_action": "none",
        }


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path.as_posix()} must contain one YAML mapping")
    return payload


def _mapping(
    payload: Mapping[str, Any],
    key: str,
    errors: list[str],
) -> Mapping[str, Any]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        errors.append(f"{key}: expected mapping")
        return {}
    return value


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _git_blob_sha1(payload: bytes) -> str:
    header = f"blob {len(payload)}\0".encode()
    return hashlib.sha1(header + payload, usedforsecurity=False).hexdigest()


def _section_sha256(payload: Any) -> str:
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return _sha256_bytes(serialized)


def _expect(
    errors: list[str],
    condition: bool,
    code: str,
) -> None:
    if not condition:
        errors.append(code)


def validate_reentry_preregistration(
    *,
    project_root: Path,
    proposal_path: Path | None = None,
) -> ValidationResult:
    root = project_root.resolve()
    proposal_file = proposal_path.resolve() if proposal_path is not None else root / PROPOSAL_PATH
    proposal = _load_yaml(proposal_file)
    source_file = root / SOURCE_POLICY_PATH
    source_bytes = source_file.read_bytes()
    source_policy = _load_yaml(source_file)
    requirement = (root / REQUIREMENT_PATH).read_text(encoding="utf-8")
    task_register = (root / TASK_REGISTER_PATH).read_text(encoding="utf-8")
    errors: list[str] = []

    _expect(
        errors,
        proposal.get("schema_version") == SCHEMA_VERSION,
        "PROPOSAL_SCHEMA_VERSION_MISMATCH",
    )
    _expect(
        errors,
        proposal.get("proposal_id") == "TRADING_2465_O1_BLIND_REENTRY_PREREGISTRATION_V1_PROPOSAL",
        "PROPOSAL_ID_MISMATCH",
    )
    _expect(errors, proposal.get("task_id") == TASK_ID, "TASK_ID_MISMATCH")
    _expect(
        errors,
        proposal.get("status") == PROPOSAL_STATUS,
        "PROPOSAL_NOT_INACTIVE",
    )
    _expect(
        errors,
        proposal.get("activation_allowed") is False,
        "PROPOSAL_ACTIVATION_ALLOWED",
    )
    _expect(
        errors,
        proposal.get("owner_route_decision") == "NOT_SELECTED",
        "OWNER_ROUTE_ALREADY_SELECTED",
    )

    authority = _mapping(proposal, "authority", errors)
    owner_instruction = _mapping(authority, "owner_instruction", errors)
    predecessor = _mapping(authority, "predecessor_policy", errors)
    _expect(
        errors,
        authority.get("planning_snapshot_commit") == PLANNING_SNAPSHOT,
        "PLANNING_SNAPSHOT_MISMATCH",
    )
    _expect(
        errors,
        owner_instruction.get("interpretation") == "PLANNING_AND_PREREGISTRATION_ONLY",
        "OWNER_INSTRUCTION_SCOPE_MISMATCH",
    )
    for key in (
        "route_selection_authorized",
        "numeric_policy_authorized",
        "empirical_action_authorized",
    ):
        _expect(
            errors,
            owner_instruction.get(key) is False,
            f"OWNER_INSTRUCTION_{key.upper()}",
        )

    _expect(
        errors,
        predecessor.get("path") == SOURCE_POLICY_PATH,
        "SOURCE_POLICY_PATH_MISMATCH",
    )
    _expect(
        errors,
        predecessor.get("policy_id") == SOURCE_POLICY_ID,
        "SOURCE_POLICY_ID_MISMATCH",
    )
    _expect(
        errors,
        predecessor.get("status") == SOURCE_POLICY_STATUS,
        "SOURCE_POLICY_STATUS_BINDING_MISMATCH",
    )
    _expect(
        errors,
        predecessor.get("file_sha256") == _sha256_bytes(source_bytes),
        "SOURCE_POLICY_SHA256_MISMATCH",
    )
    _expect(
        errors,
        predecessor.get("git_blob_sha1_at_planning_snapshot") == _git_blob_sha1(source_bytes),
        "SOURCE_POLICY_GIT_BLOB_SHA1_MISMATCH",
    )
    _expect(
        errors,
        source_policy.get("policy_id") == SOURCE_POLICY_ID,
        "LIVE_SOURCE_POLICY_ID_MISMATCH",
    )
    _expect(
        errors,
        source_policy.get("status") == SOURCE_POLICY_STATUS,
        "LIVE_SOURCE_POLICY_NOT_CLOSED",
    )

    frozen = _mapping(proposal, "frozen_o1_contract", errors)
    section_hashes = _mapping(frozen, "immutable_section_sha256", errors)
    _expect(
        errors,
        frozen.get("exact_reuse_required_for_route_a_plus_d") is True,
        "O1_EXACT_REUSE_NOT_REQUIRED",
    )
    _expect(
        errors,
        frozen.get("source_policy_path") == SOURCE_POLICY_PATH,
        "FROZEN_SOURCE_POLICY_PATH_MISMATCH",
    )
    _expect(
        errors,
        frozen.get("source_policy_file_sha256") == _sha256_bytes(source_bytes),
        "FROZEN_SOURCE_POLICY_SHA256_MISMATCH",
    )
    for section_name in IMMUTABLE_SECTIONS:
        _expect(
            errors,
            section_hashes.get(section_name) == _section_sha256(source_policy.get(section_name)),
            f"FROZEN_SECTION_HASH_MISMATCH:{section_name}",
        )

    identity = _mapping(frozen, "identity_summary", errors)
    target = _mapping(source_policy, "target_contract", errors)
    split = _mapping(source_policy, "split_contract", errors)
    coverage_contract = _mapping(source_policy, "coverage_contract", errors)
    model = _mapping(source_policy, "model_feature_contract", errors)
    metric = _mapping(source_policy, "metric_contract", errors)
    data_contract = _mapping(source_policy, "data_contract", errors)
    expected_identity = {
        "target_id": target.get("target_id"),
        "form": target.get("form"),
        "label": target.get("label"),
        "primary_horizon_common_sessions": target.get("primary_horizon_common_sessions"),
        "split_id": split.get("split_id"),
        "model_id": model.get("model_id"),
        "feature_family_prefix": model.get("family_prefix"),
        "primary_metric": metric.get("primary_metric"),
        "coverage_failure_class": coverage_contract.get("failure_class"),
        "primary_research_start": data_contract.get("primary_research_start"),
    }
    _expect(
        errors,
        dict(identity) == expected_identity,
        "FROZEN_IDENTITY_SUMMARY_MISMATCH",
    )

    prior = _mapping(proposal, "prior_attempt", errors)
    active_coverage = _mapping(source_policy, "coverage_evidence", errors)
    active_report = _mapping(active_coverage, "report", errors)
    active_gate = _mapping(active_coverage, "gate", errors)
    active_attempt = _mapping(source_policy, "attempt_ledger_contract", errors)
    _expect(
        errors,
        prior.get("attempt_id") == active_attempt.get("current_attempt_family_id"),
        "PRIOR_ATTEMPT_ID_MISMATCH",
    )
    _expect(
        errors,
        prior.get("source_commit_sha") == active_coverage.get("source_commit_sha"),
        "PRIOR_ATTEMPT_SOURCE_COMMIT_MISMATCH",
    )
    for proposal_key, source in (
        ("coverage_report", active_report),
        ("coverage_gate", active_gate),
    ):
        binding = _mapping(prior, proposal_key, errors)
        for field in ("sha256", "byte_size"):
            _expect(
                errors,
                binding.get(field) == source.get(field),
                f"PRIOR_{proposal_key.upper()}_{field.upper()}_MISMATCH",
            )
    _expect(
        errors,
        _mapping(prior, "coverage_report", errors).get("report_id")
        == active_report.get("report_id"),
        "PRIOR_COVERAGE_REPORT_ID_MISMATCH",
    )
    _expect(
        errors,
        _mapping(prior, "coverage_gate", errors).get("gate_id") == active_gate.get("gate_id"),
        "PRIOR_COVERAGE_GATE_ID_MISMATCH",
    )
    for key, expected in (
        ("append_only", True),
        ("single_run_consumed", True),
        ("result_driven_retry_allowed", False),
        ("model_training_executed", False),
        ("prediction_or_metric_generated", False),
        ("prospective_accessed", False),
        ("retained_evidence_bytes_read_by_this_task", False),
        ("retained_evidence_mutation_allowed", False),
        ("retained_evidence_cleanup_allowed", False),
    ):
        _expect(
            errors,
            prior.get(key) is expected,
            f"PRIOR_ATTEMPT_BOUNDARY_MISMATCH:{key}",
        )

    route = _mapping(proposal, "route_decision", errors)
    _expect(
        errors,
        route.get("selected_route") == "NOT_SELECTED",
        "ROUTE_SELECTED_WITHOUT_OWNER",
    )
    _expect(
        errors,
        route.get("owner_decision_token") == "NOT_SELECTED",
        "OWNER_ROUTE_TOKEN_ALREADY_SET",
    )
    _expect(
        errors,
        route.get("selection_allowed_without_owner_token") is False,
        "ROUTE_SELECTION_WITHOUT_OWNER_ALLOWED",
    )
    route_options = route.get("options")
    if not isinstance(route_options, list):
        errors.append("ROUTE_OPTIONS_NOT_LIST")
    else:
        for option in route_options:
            if not isinstance(option, Mapping):
                errors.append("ROUTE_OPTION_NOT_MAPPING")
                continue
            _expect(
                errors,
                option.get("empirical_action_allowed") is False,
                f"ROUTE_OPTION_EMPIRICAL_ACTION_ALLOWED:{option.get('route_id')}",
            )

    route_a = _mapping(proposal, "route_a_plus_d_policy_slots", errors)
    calendar = _mapping(route_a, "calendar_trigger", errors)
    vintage = _mapping(route_a, "data_vintage", errors)
    look_budget = _mapping(route_a, "look_budget", errors)
    future_attempt = _mapping(route_a, "future_attempt", errors)
    for key in ("trigger_type", "not_before_date", "rationale"):
        _expect(
            errors,
            calendar.get(key) == "NOT_SELECTED",
            f"CALENDAR_SLOT_ALREADY_SELECTED:{key}",
        )
    for key in ("source_publication_cutoff", "exact_vintage_identity"):
        _expect(
            errors,
            vintage.get(key) == "NOT_SELECTED",
            f"DATA_VINTAGE_SLOT_ALREADY_SELECTED:{key}",
        )
    _expect(
        errors,
        look_budget.get("maximum_reentry_coverage_looks") == "NOT_SELECTED",
        "LOOK_BUDGET_ALREADY_SELECTED",
    )
    _expect(
        errors,
        future_attempt.get("selected_attempt_id") == "NOT_SELECTED",
        "FUTURE_ATTEMPT_ALREADY_SELECTED",
    )
    for mapping, key, code in (
        (calendar, "count_based_or_dynamic_trigger_allowed", "DYNAMIC_TRIGGER_ALLOWED"),
        (vintage, "pre_2021_primary_data_allowed", "PRE_2021_PRIMARY_DATA_ALLOWED"),
        (look_budget, "automatic_retry_allowed", "AUTOMATIC_RETRY_ALLOWED"),
        (look_budget, "resume_or_overwrite_allowed", "RESUME_OR_OVERWRITE_ALLOWED"),
        (
            future_attempt,
            "pristine_first_attempt_claim_allowed",
            "PRISTINE_FIRST_ATTEMPT_CLAIM_ALLOWED",
        ),
    ):
        _expect(errors, mapping.get(key) is False, code)

    route_b = _mapping(proposal, "route_b_policy_slots", errors)
    route_c = _mapping(proposal, "route_c_policy_slots", errors)
    for key in (
        "owner_decision_token",
        "loss_budget",
        "target_form",
        "event_base_rate_policy",
        "false_negative_cost_policy",
        "calibration_gate",
        "independent_event_ledger",
    ):
        _expect(
            errors,
            route_b.get(key) == "NOT_SELECTED",
            f"ROUTE_B_SLOT_ALREADY_SELECTED:{key}",
        )
    for key in (
        "owner_decision_token",
        "action_templates",
        "execution_timing",
        "cost_model",
        "risk_penalty",
        "utility_unit",
        "sensitivity_policy",
    ):
        _expect(
            errors,
            route_c.get(key) == "NOT_SELECTED",
            f"ROUTE_C_SLOT_ALREADY_SELECTED:{key}",
        )

    authorization = _mapping(proposal, "authorization", errors)
    _expect(
        errors,
        authorization.get("planning_and_static_validation_allowed") is True,
        "PLANNING_VALIDATION_NOT_ALLOWED",
    )
    for key in EMPIRICAL_AUTHORIZATION_KEYS:
        _expect(
            errors,
            authorization.get(key) is False,
            f"UNAUTHORIZED_EMPIRICAL_FLAG:{key}",
        )

    safety = _mapping(proposal, "safety", errors)
    for key in DOWNSTREAM_FALSE_KEYS:
        _expect(
            errors,
            safety.get(key) is False,
            f"DOWNSTREAM_FLAG_ENABLED:{key}",
        )
    _expect(
        errors,
        safety.get("production_effect") == "none",
        "PRODUCTION_EFFECT_CHANGED",
    )
    _expect(
        errors,
        safety.get("broker_action") == "none",
        "BROKER_ACTION_CHANGED",
    )

    validation = _mapping(proposal, "validation_contract", errors)
    _expect(
        errors,
        validation.get("retained_runtime_evidence_access_required") is False,
        "RETAINED_RUNTIME_EVIDENCE_ACCESS_REQUIRED",
    )
    _expect(
        errors,
        validation.get("docs_system_flow_impact") == "NONE_INACTIVE_PLANNING_ONLY",
        "SYSTEM_FLOW_IMPACT_MISMATCH",
    )
    _expect(
        errors,
        validation.get("expected_status_before_owner_selection") == HISTORICAL_OWNER_GATE_STATUS,
        "EXPECTED_OWNER_GATE_STATUS_MISMATCH",
    )

    task_line = next(
        (line for line in task_register.splitlines() if line.startswith(f"|{TASK_ID}|")),
        None,
    )
    _expect(errors, task_line is not None, "TASK_REGISTER_ROW_MISSING")
    if task_line is not None:
        cells = task_line.split("|")
        _expect(
            errors,
            len(cells) > 4 and cells[4] == CURRENT_TASK_STATUS,
            "TASK_REGISTER_STATUS_NOT_BASELINE_DONE",
        )
        _expect(
            errors,
            REQUIREMENT_PATH in task_line,
            "TASK_REGISTER_REQUIREMENT_LINK_MISSING",
        )
    for required_text, code in (
        (TASK_ID, "REQUIREMENT_TASK_ID_MISSING"),
        ("状态：`BASELINE_DONE`", "REQUIREMENT_STATUS_MISMATCH"),
        (
            OWNER_DECISION_TOKEN,
            "REQUIREMENT_EXACT_OWNER_DECISION_MISSING",
        ),
        (
            SUCCESSOR_TASK_ID,
            "REQUIREMENT_SUCCESSOR_TASK_MISSING",
        ),
        (
            "CANNOT_VERIFY_EXACT_BACKEND_ROUTE=true",
            "REQUIREMENT_ROUTING_RISK_MISSING",
        ),
        (
            "INVALID_POST_RESULT_REDESIGN_CONTAMINATION",
            "REQUIREMENT_CONTAMINATION_STOP_MISSING",
        ),
    ):
        _expect(errors, required_text in requirement, code)

    errors_tuple = tuple(sorted(set(errors)))
    return ValidationResult(
        status="PASS" if not errors_tuple else "FAIL",
        task_id=TASK_ID,
        proposal_path=proposal_file.as_posix(),
        error_count=len(errors_tuple),
        errors=errors_tuple,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "验证 TRADING-2465 历史 inactive O1 blind-reentry preregistration"
            "及其 Owner 决策交接；不读取 runtime evidence，不运行 empirical action。"
        )
    )
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--proposal", type=Path)
    args = parser.parse_args()
    result = validate_reentry_preregistration(
        project_root=args.project_root,
        proposal_path=args.proposal,
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False, sort_keys=True))
    return 0 if result.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
