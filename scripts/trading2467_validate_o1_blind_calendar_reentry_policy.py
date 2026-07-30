from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

STABLE_TASK_ID = "TRADING-2467_O1_BLIND_CALENDAR_REENTRY_POLICY_SLOT_FREEZE"
POLICY_TASK_ID = "TRADING-2467_O1_BLIND_CALENDAR_REENTRY_POLICY_SLOT_FREEZE"
POLICY_PATH = (
    "config/research/o1_relative_opportunity_blind_calendar_reentry_policy_v1.yaml"
)
POLICY_SCHEMA_VERSION = "o1_relative_opportunity_blind_calendar_reentry_policy.v1"
POLICY_ID = "TRADING_2467_O1_BLIND_CALENDAR_REENTRY_POLICY_V1"
POLICY_STATUS = "OWNER_APPROVED_INACTIVE_POLICY_SLOT_FROZEN"
EXACT_SOURCE_COMMIT = "26e76d25b425957926a37ce8be5e55c58d356f37"

PREDECESSOR_REQUIREMENT_PATH = (
    "docs/requirements/"
    "TRADING-2465_Post_O1_Route_Decision_And_Blind_Reentry_Preregistration.md"
)
PREDECESSOR_PROPOSAL_PATH = (
    "config/research/o1_relative_opportunity_reentry_preregistration_v1_proposal.yaml"
)
V1_POLICY_PATH = "config/research/o1_relative_opportunity_capability_audit_v1.yaml"
V1_POLICY_ID = "TRADING_2464_O1_CAPABILITY_AUDIT_V1"
V1_POLICY_STATUS = "CLOSED_INSUFFICIENT_COVERAGE_OR_DQ"

ROUTE_OWNER_DECISION = (
    "owner_decision:TRADING-2465:2026-07-30:"
    "select_o1_blind_calendar_reentry_with_generic_evidence_infrastructure_v1"
)
ROUTE_OWNER_DECISION_TEMPLATE = (
    "owner_decision:TRADING-2465:YYYY-MM-DD:"
    "select_o1_blind_calendar_reentry_with_generic_evidence_infrastructure_v1"
)
POLICY_OWNER_DECISION = (
    "owner_decision:TRADING-2467:2026-07-30:"
    "authorize_inactive_o1_blind_calendar_reentry_policy_slot_freeze_v1"
)

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

# These values are reviewed policy, not validator-chosen heuristics. Their authority
# is TRADING-2467 requirement sections 3-5 and the two exact Owner decisions above.
EXPECTED_ROUTE_CONTRACT = {
    "route_id": (
        "A_PLUS_D_O1_BLIND_CALENDAR_REENTRY_WITH_GENERIC_EVIDENCE_INFRASTRUCTURE"
    ),
    "o1_blind_calendar_reentry_selected": True,
    "generic_evidence_infrastructure_selected": True,
    "o2_loss_budget_foundation_selected": False,
    "o3_utility_action_foundation_selected": False,
    "threshold_or_model_redesign_selected": False,
    "route_selection_is_capability_conclusion": False,
}

EXPECTED_CALENDAR_TRIGGER = {
    "trigger_type": "BLIND_CALENDAR_NOT_BEFORE",
    "not_before_date": "2027-02-01",
    "not_before_at": "2027-02-01T00:00:00-05:00",
    "timezone": "America/New_York",
    "rationale_basis": "SIX_COMPLETE_CALENDAR_MONTH_PUBLICATION_CYCLES",
    "count_based_trigger_allowed": False,
    "dynamic_trigger_allowed": False,
    "result_based_trigger_allowed": False,
    "automatic_execution_on_trigger": False,
    "date_effect": "OWNER_REVIEW_ELIGIBLE_NOT_RUN_AUTHORIZED",
}

EXPECTED_FUTURE_SESSIONS = (
    "2027-01-25",
    "2027-01-26",
    "2027-01-27",
    "2027-01-28",
    "2027-01-29",
)

EXPECTED_MANIFEST_BINDINGS = (
    "execution_commit",
    "policy_path",
    "policy_git_blob_sha1",
    "policy_content_sha256",
    "owner_decision_ids",
    "provider_endpoint_parameters",
    "publication_and_download_timestamps",
    "immutable_member_sha256_size_rows_and_range",
    "dq_receipt",
    "calendar_event_ledger",
    "requested_and_evaluated_range",
    "pit_contract",
    "runtime_package_lock",
)
EXPECTED_FUTURE_MATERIALIZATION = {
    "source_member_receipts": "PENDING_SEPARATE_RUN_AUTHORIZATION_AND_MATERIALIZATION",
    "download_receipts": "PENDING_SEPARATE_RUN_AUTHORIZATION_AND_MATERIALIZATION",
    "dq_receipt": "PENDING_SEPARATE_RUN_AUTHORIZATION_AND_MATERIALIZATION",
    "calendar_event_ledger": "PENDING_SEPARATE_RUN_AUTHORIZATION_AND_MATERIALIZATION",
    "runtime_package_lock": "PENDING_SEPARATE_RUN_AUTHORIZATION_AND_MATERIALIZATION",
}

EXPECTED_FUTURE_ATTEMPT = {
    "attempt_id": "O1_M1_RIDGE_CROSS_ASSET_STATE_V2_CALENDAR_REENTRY",
    "predecessor_attempt_id": "O1_M1_RIDGE_CROSS_ASSET_STATE_V1",
    "predecessor_single_run_consumed": True,
    "pristine_independent_first_attempt_claim_allowed": False,
    "append_only_attempt_ledger_required": True,
    "separate_real_run_owner_token_required": True,
    "separate_real_run_owner_token_status": "NOT_GRANTED",
    "look_consumption_event": (
        "FIRST_O1_V2_NON_SYNTHETIC_ELIGIBILITY_COVERAGE_OR_DERIVED_RESULT_READ_ATTEMPT"
    ),
    "look_must_be_atomically_appended_before_event": True,
    "crash_exception_or_partial_output_consumes_look": True,
    "materialized_real_output_consumes_look": True,
    "generic_acquisition_or_dq_authorized_by_this_policy": False,
}

EXPECTED_LOOK_BUDGET = {
    "maximum_reentry_coverage_looks": 1,
    "budget_window_start_inclusive": "2027-02-01T00:00:00-05:00",
    "budget_window_end_exclusive": "2028-02-01T00:00:00-05:00",
    "rolling_window": False,
    "automatic_rollover": False,
    "unused_budget_after_expiry": "EXPIRES",
    "automatic_retry_allowed": False,
    "resume_allowed": False,
    "overwrite_allowed": False,
    "second_candidate_allowed": False,
    "result_driven_rerun_allowed": False,
}

EXPECTED_AUTHORIZATION = {
    "inactive_policy_serialization_allowed": True,
    "independent_static_validation_allowed": True,
    "controlled_fixture_tests_allowed": True,
    "data_acquisition_allowed": False,
    "dq_execution_allowed": False,
    "eligibility_read_allowed": False,
    "coverage_read_allowed": False,
    "result_read_allowed": False,
    "model_training_allowed": False,
    "prediction_allowed": False,
    "metric_generation_allowed": False,
    "canonical_run_allowed": False,
    "falsification_execution_allowed": False,
    "decision_value_audit_allowed": False,
    "risk_overlay_allowed": False,
    "candidate_or_backtest_allowed": False,
    "target_weights_allowed": False,
    "paper_shadow_change_allowed": False,
    "promotion_allowed": False,
}

EXPECTED_SAFETY = {
    "retained_runtime_evidence_access_required": False,
    "retained_runtime_evidence_bytes_read_by_this_task": False,
    "retained_runtime_evidence_mutation_allowed": False,
    "retained_runtime_evidence_cleanup_allowed": False,
    "network_required": False,
    "cached_market_or_macro_data_required": False,
    "runtime_output_root_required": False,
    "docs_system_flow_impact": "NONE_INACTIVE_POLICY_ONLY",
    "first_stop_boundary": (
        "STATIC_POLICY_VALIDATOR_AND_TESTS_PASS_"
        "BEFORE_ANY_REAL_DATA_DQ_COVERAGE_OR_RESULT_READ"
    ),
}
EXPECTED_OUTCOME_CONTRACT = {
    "PASS": {
        "output_class": "COVERAGE_ELIGIBLE_PASS_ONLY",
        "requirements": [
            "STRICT_DQ_PASS_0_ERRORS_0_WARNINGS",
            "EXACT_VINTAGE_AND_LINEAGE_VALID",
            "ALL_FROZEN_MANDATORY_COVERAGE_FLOORS_PASS",
        ],
        "model_training_allowed": False,
        "next_action": "STOP_AND_REQUIRE_NEW_OWNER_CANONICAL_RUN_DECISION",
    },
    "FAIL": {
        "capability_class_allowed": False,
        "conditions": [
            "OWNER_REJECTS_ROUTE",
            "STATIC_POLICY_OR_VALIDATOR_FAILS",
            "EXACT_POLICY_CANNOT_BE_CONSTRUCTED",
        ],
        "next_action": "STOP",
    },
    "INSUFFICIENT": {
        "output_class": "INSUFFICIENT_COVERAGE_OR_DQ",
        "capability_class_allowed": False,
        "retry_allowed": False,
        "next_action": "CLOSE_V2_AND_STOP",
    },
    "INVALID": {
        "capability_class_allowed": False,
        "quarantine_required": True,
        "conditions": [
            "PIT_BREACH",
            "WRONG_VINTAGE",
            "LINEAGE_TAMPER",
            "EXACT_RECONSTRUCTION_MISMATCH",
            "UNAUTHORIZED_RESULT_READ",
            "POST_RESULT_CONTRACT_CHANGE",
            "RESUME_OR_OVERWRITE",
            "ATTEMPT_LEDGER_VIOLATION",
            "INDEPENDENT_VALIDATOR_ERROR",
        ],
        "next_action": "QUARANTINE_AND_STOP",
    },
}


@dataclass(frozen=True)
class ValidationResult:
    status: str
    task_id: str
    policy_path: str
    policy_sha256: str
    deterministic_serialization_sha256: str
    error_count: int
    errors: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": (
                "trading2467_o1_blind_calendar_reentry_policy_validation.v1"
            ),
            "status": self.status,
            "task_id": self.task_id,
            "policy_path": self.policy_path,
            "policy_sha256": self.policy_sha256,
            "deterministic_serialization_sha256": (
                self.deterministic_serialization_sha256
            ),
            "error_count": self.error_count,
            "errors": list(self.errors),
            "production_effect": "none",
            "broker_action": "none",
        }


def _load_yaml_bytes(payload: bytes, *, source: str) -> dict[str, Any]:
    parsed = yaml.safe_load(payload.decode("utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError(f"{source} must contain one YAML mapping")
    return parsed


def _load_yaml_path(path: Path) -> tuple[bytes, dict[str, Any]]:
    payload = path.read_bytes()
    return payload, _load_yaml_bytes(payload, source=path.as_posix())


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


def _expect(
    errors: list[str],
    condition: bool,
    code: str,
) -> None:
    if not condition:
        errors.append(code)


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _git_blob_sha1(payload: bytes) -> str:
    header = f"blob {len(payload)}\0".encode()
    return hashlib.sha1(header + payload, usedforsecurity=False).hexdigest()


def _section_sha256(payload: Any) -> str:
    return _sha256_bytes(canonical_policy_bytes(payload))


def canonical_policy_bytes(payload: Any) -> bytes:
    """Serialize policy-shaped data deterministically without writing an artifact."""
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _git_payload_at_commit(
    *,
    project_root: Path,
    commit: str,
    repository_path: str,
) -> tuple[str, bytes]:
    object_name = f"{commit}:{repository_path}"
    object_id = subprocess.run(
        ["git", "-C", str(project_root), "rev-parse", object_name],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    payload = subprocess.run(
        ["git", "-C", str(project_root), "cat-file", "blob", object_name],
        check=True,
        capture_output=True,
    ).stdout
    return object_id, payload


def _validate_source_binding(
    *,
    project_root: Path,
    binding: Mapping[str, Any],
    expected_path: str,
    binding_name: str,
    require_live_match: bool,
    errors: list[str],
) -> bytes:
    _expect(
        errors,
        binding.get("path") == expected_path,
        f"SOURCE_BINDING_PATH_MISMATCH:{binding_name}",
    )
    object_id, exact_payload = _git_payload_at_commit(
        project_root=project_root,
        commit=EXACT_SOURCE_COMMIT,
        repository_path=expected_path,
    )
    _expect(
        errors,
        object_id == _git_blob_sha1(exact_payload),
        f"EXACT_GIT_OBJECT_RECOMPUTE_MISMATCH:{binding_name}",
    )
    _expect(
        errors,
        binding.get("git_blob_sha1_at_exact_source_commit") == object_id,
        f"SOURCE_GIT_BLOB_MISMATCH:{binding_name}",
    )
    _expect(
        errors,
        binding.get("content_sha256_at_exact_source_commit")
        == _sha256_bytes(exact_payload),
        f"SOURCE_CONTENT_SHA256_MISMATCH:{binding_name}",
    )
    if require_live_match:
        live_payload = (project_root / expected_path).read_bytes()
        _expect(
            errors,
            live_payload == exact_payload,
            f"LIVE_SOURCE_DRIFT_FROM_EXACT_COMMIT:{binding_name}",
        )
    return exact_payload


def _not_selected_paths(payload: Any, prefix: str = "$") -> tuple[str, ...]:
    paths: list[str] = []
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            paths.extend(_not_selected_paths(value, f"{prefix}.{key}"))
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            paths.extend(_not_selected_paths(value, f"{prefix}[{index}]"))
    elif payload == "NOT_SELECTED":
        paths.append(prefix)
    return tuple(paths)


def _validate_temporal_contracts(
    *,
    calendar: Mapping[str, Any],
    vintage: Mapping[str, Any],
    look_budget: Mapping[str, Any],
    errors: list[str],
) -> None:
    try:
        not_before_date = date.fromisoformat(str(calendar.get("not_before_date")))
        not_before_at = datetime.fromisoformat(str(calendar.get("not_before_at")))
        cutoff = datetime.fromisoformat(
            str(vintage.get("source_publication_cutoff_inclusive"))
        )
        budget_start = datetime.fromisoformat(
            str(look_budget.get("budget_window_start_inclusive"))
        )
        budget_end = datetime.fromisoformat(
            str(look_budget.get("budget_window_end_exclusive"))
        )
        requested_end = date.fromisoformat(str(vintage.get("requested_end")))
        evaluated_end = date.fromisoformat(str(vintage.get("evaluated_end")))
        sessions = tuple(
            date.fromisoformat(str(value))
            for value in _mapping(
                vintage,
                "future_sessions_after_evaluated_end",
                errors,
            ).get("sessions", [])
        )
    except (TypeError, ValueError):
        errors.append("TEMPORAL_CONTRACT_PARSE_ERROR")
        return

    _expect(
        errors,
        not_before_date == not_before_at.date(),
        "TRIGGER_DATE_TIMESTAMP_MISMATCH",
    )
    _expect(
        errors,
        cutoff + timedelta(seconds=1) == not_before_at,
        "CUTOFF_TRIGGER_BOUNDARY_MISMATCH",
    )
    _expect(
        errors,
        budget_start == not_before_at,
        "LOOK_WINDOW_START_TRIGGER_MISMATCH",
    )
    _expect(
        errors,
        budget_end.isoformat() == "2028-02-01T00:00:00-05:00",
        "LOOK_WINDOW_END_MISMATCH",
    )
    _expect(
        errors,
        tuple(value.isoformat() for value in sessions) == EXPECTED_FUTURE_SESSIONS,
        "FUTURE_SESSION_SEQUENCE_MISMATCH",
    )
    _expect(
        errors,
        bool(sessions)
        and evaluated_end < sessions[0]
        and sessions[-1] == requested_end,
        "EVALUATED_REQUESTED_SESSION_BOUNDARY_MISMATCH",
    )


def validate_blind_calendar_reentry_policy(
    *,
    project_root: Path,
    policy_path: Path | None = None,
) -> ValidationResult:
    root = project_root.resolve()
    policy_file = policy_path.resolve() if policy_path is not None else root / POLICY_PATH
    policy_bytes, policy = _load_yaml_path(policy_file)
    errors: list[str] = []

    _expect(
        errors,
        policy.get("schema_version") == POLICY_SCHEMA_VERSION,
        "POLICY_SCHEMA_VERSION_MISMATCH",
    )
    _expect(errors, policy.get("policy_id") == POLICY_ID, "POLICY_ID_MISMATCH")
    _expect(errors, policy.get("task_id") == POLICY_TASK_ID, "POLICY_TASK_ID_MISMATCH")
    _expect(errors, policy.get("status") == POLICY_STATUS, "POLICY_STATUS_MISMATCH")
    _expect(
        errors,
        policy.get("effective_status") == "INACTIVE",
        "POLICY_EFFECTIVE_STATUS_NOT_INACTIVE",
    )
    for key in ("activation_allowed", "automatic_execution_allowed"):
        _expect(errors, policy.get(key) is False, f"POLICY_FLAG_ENABLED:{key}")
    _expect(
        errors,
        policy.get("production_effect") == "none",
        "PRODUCTION_EFFECT_CHANGED",
    )
    _expect(errors, policy.get("broker_action") == "none", "BROKER_ACTION_CHANGED")

    authority = _mapping(policy, "authority", errors)
    _expect(
        errors,
        authority.get("exact_source_commit") == EXACT_SOURCE_COMMIT,
        "EXACT_SOURCE_COMMIT_MISMATCH",
    )
    owner_instruction = _mapping(authority, "owner_instruction", errors)
    _expect(
        errors,
        owner_instruction.get("interpretation") == "INACTIVE_POLICY_SLOT_FREEZE_ONLY",
        "OWNER_INSTRUCTION_SCOPE_MISMATCH",
    )
    owner_decisions = _mapping(authority, "owner_decisions", errors)
    for key, expected_decision in (
        ("route_and_infrastructure", ROUTE_OWNER_DECISION),
        ("inactive_policy_freeze", POLICY_OWNER_DECISION),
    ):
        decision = _mapping(owner_decisions, key, errors)
        _expect(
            errors,
            decision.get("decision_id") == expected_decision,
            f"OWNER_DECISION_ID_MISMATCH:{key}",
        )
        _expect(
            errors,
            decision.get("accepted") is True,
            f"OWNER_DECISION_NOT_ACCEPTED:{key}",
        )

    bindings = _mapping(authority, "source_bindings", errors)
    requirement_payload = _validate_source_binding(
        project_root=root,
        binding=_mapping(bindings, "predecessor_requirement", errors),
        expected_path=PREDECESSOR_REQUIREMENT_PATH,
        binding_name="predecessor_requirement",
        require_live_match=False,
        errors=errors,
    )
    proposal_payload = _validate_source_binding(
        project_root=root,
        binding=_mapping(bindings, "predecessor_proposal", errors),
        expected_path=PREDECESSOR_PROPOSAL_PATH,
        binding_name="predecessor_proposal",
        require_live_match=True,
        errors=errors,
    )
    v1_payload = _validate_source_binding(
        project_root=root,
        binding=_mapping(bindings, "o1_v1_policy", errors),
        expected_path=V1_POLICY_PATH,
        binding_name="o1_v1_policy",
        require_live_match=True,
        errors=errors,
    )
    _expect(
        errors,
        ROUTE_OWNER_DECISION_TEMPLATE.encode("utf-8") in requirement_payload,
        "PREDECESSOR_REQUIREMENT_OWNER_DECISION_TEMPLATE_MISSING",
    )
    predecessor_proposal = _load_yaml_bytes(
        proposal_payload,
        source=PREDECESSOR_PROPOSAL_PATH,
    )
    predecessor_binding = _mapping(bindings, "predecessor_proposal", errors)
    _expect(
        errors,
        predecessor_binding.get("historical_status")
        == predecessor_proposal.get("status"),
        "PREDECESSOR_PROPOSAL_STATUS_MISMATCH",
    )
    _expect(
        errors,
        predecessor_binding.get("retained_as_immutable_pre_owner_history") is True,
        "PREDECESSOR_PROPOSAL_NOT_RETAINED",
    )

    v1_policy = _load_yaml_bytes(v1_payload, source=V1_POLICY_PATH)
    v1_binding = _mapping(bindings, "o1_v1_policy", errors)
    _expect(
        errors,
        v1_binding.get("policy_id") == V1_POLICY_ID == v1_policy.get("policy_id"),
        "V1_POLICY_ID_MISMATCH",
    )
    _expect(
        errors,
        v1_binding.get("status") == V1_POLICY_STATUS == v1_policy.get("status"),
        "V1_POLICY_STATUS_MISMATCH",
    )

    web_pro = _mapping(authority, "web_pro_planning_evidence", errors)
    _expect(
        errors,
        web_pro.get("classification") == "UI_PRO_AND_SELF_REPORT_PRO_ROUTE_UNVERIFIED",
        "WEB_PRO_ROUTE_CLASSIFICATION_MISMATCH",
    )
    _expect(
        errors,
        web_pro.get("cannot_verify_exact_backend_route") is True,
        "WEB_PRO_ROUTE_RISK_NOT_EXPOSED",
    )
    _expect(
        errors,
        web_pro.get("repository_authority") is False,
        "WEB_PRO_IMPROPERLY_MARKED_AUTHORITY",
    )

    route = _mapping(policy, "route_contract", errors)
    _expect(
        errors,
        dict(route) == EXPECTED_ROUTE_CONTRACT,
        "ROUTE_CONTRACT_MISMATCH",
    )
    calendar = _mapping(policy, "calendar_trigger", errors)
    _expect(
        errors,
        dict(calendar) == EXPECTED_CALENDAR_TRIGGER,
        "CALENDAR_TRIGGER_MISMATCH",
    )

    vintage = _mapping(policy, "data_vintage_contract", errors)
    _expect(
        errors,
        set(vintage)
        == {
            "source_publication_cutoff_inclusive",
            "primary_research_start",
            "pre_2021_02_22_primary_rows_allowed",
            "requested_start",
            "requested_end",
            "evaluated_start",
            "evaluated_end",
            "future_sessions_after_evaluated_end",
            "pit_requirements",
            "strict_data_quality_gate",
            "future_materialization",
            "canonical_manifest",
        },
        "DATA_VINTAGE_FIELD_SET_MISMATCH",
    )
    for key, expected in (
        ("source_publication_cutoff_inclusive", "2027-01-31T23:59:59-05:00"),
        ("primary_research_start", "2021-02-22"),
        ("pre_2021_02_22_primary_rows_allowed", False),
        ("requested_start", "2021-02-22"),
        ("requested_end", "2027-01-29"),
        ("evaluated_start", "2021-02-22"),
        ("evaluated_end", "2027-01-22"),
    ):
        _expect(
            errors,
            vintage.get(key) == expected,
            f"DATA_VINTAGE_FIELD_MISMATCH:{key}",
        )
    future_sessions = _mapping(vintage, "future_sessions_after_evaluated_end", errors)
    _expect(
        errors,
        dict(future_sessions)
        == {
            "count": 5,
            "sessions": list(EXPECTED_FUTURE_SESSIONS),
        },
        "FUTURE_SESSION_COUNT_MISMATCH",
    )
    _expect(
        errors,
        tuple(future_sessions.get("sessions", [])) == EXPECTED_FUTURE_SESSIONS,
        "FUTURE_SESSION_SEQUENCE_MISMATCH",
    )

    pit = _mapping(vintage, "pit_requirements", errors)
    expected_pit = {
        "source_published_at_not_after_cutoff": True,
        "known_at_not_after_cutoff": True,
        "available_at_not_after_cutoff": True,
        "unknown_historical_timestamp_allowed": False,
        "current_view_substitution_allowed": False,
        "silent_imputation_allowed": False,
        "post_cutoff_revision_allowed": False,
    }
    _expect(errors, dict(pit) == expected_pit, "PIT_REQUIREMENTS_MISMATCH")

    dq = _mapping(vintage, "strict_data_quality_gate", errors)
    _expect(
        errors,
        dict(dq)
        == {
            "required_status": "PASS",
            "required_error_count": 0,
            "required_warning_count": 0,
        },
        "STRICT_DQ_REQUIREMENT_MISMATCH",
    )

    materialization = _mapping(vintage, "future_materialization", errors)
    _expect(
        errors,
        dict(materialization) == EXPECTED_FUTURE_MATERIALIZATION,
        "FUTURE_MATERIALIZATION_BOUNDARY_MISMATCH",
    )

    manifest = _mapping(vintage, "canonical_manifest", errors)
    for key, expected in (
        ("required_before_first_real_coverage_read", True),
        ("deterministic_utf8_serialization", True),
        ("stable_key_and_member_sorting", True),
        (
            "exact_vintage_identity_formula",
            "O1_V2_VINTAGE_SHA256_ + SHA256(canonical_manifest_bytes)",
        ),
        (
            "exact_vintage_identity",
            "PENDING_SEPARATE_RUN_AUTHORIZATION_AND_MATERIALIZATION",
        ),
    ):
        _expect(
            errors,
            manifest.get(key) == expected,
            f"CANONICAL_MANIFEST_FIELD_MISMATCH:{key}",
        )
    _expect(
        errors,
        tuple(manifest.get("required_bindings", [])) == EXPECTED_MANIFEST_BINDINGS,
        "CANONICAL_MANIFEST_BINDINGS_MISMATCH",
    )
    _expect(
        errors,
        set(manifest)
        == {
            "required_before_first_real_coverage_read",
            "deterministic_utf8_serialization",
            "stable_key_and_member_sorting",
            "exact_vintage_identity_formula",
            "required_bindings",
            "exact_vintage_identity",
        },
        "CANONICAL_MANIFEST_FIELD_SET_MISMATCH",
    )

    future_attempt = _mapping(policy, "future_attempt_contract", errors)
    _expect(
        errors,
        dict(future_attempt) == EXPECTED_FUTURE_ATTEMPT,
        "FUTURE_ATTEMPT_CONTRACT_MISMATCH",
    )
    look_budget = _mapping(policy, "look_budget_contract", errors)
    _expect(
        errors,
        dict(look_budget) == EXPECTED_LOOK_BUDGET,
        "LOOK_BUDGET_CONTRACT_MISMATCH",
    )
    _validate_temporal_contracts(
        calendar=calendar,
        vintage=vintage,
        look_budget=look_budget,
        errors=errors,
    )

    v1 = _mapping(policy, "v1_immutable_contract", errors)
    _expect(
        errors,
        set(v1)
        == {
            "source_policy_path",
            "source_policy_id",
            "exact_reuse_required",
            "post_result_threshold_fold_horizon_regime_event_or_model_change_allowed",
            "immutable_section_sha256",
            "capability_status",
        },
        "V1_IMMUTABLE_CONTRACT_FIELD_SET_MISMATCH",
    )
    _expect(
        errors,
        v1.get("source_policy_path") == V1_POLICY_PATH,
        "V1_IMMUTABLE_SOURCE_PATH_MISMATCH",
    )
    _expect(
        errors,
        v1.get("source_policy_id") == V1_POLICY_ID,
        "V1_IMMUTABLE_SOURCE_ID_MISMATCH",
    )
    _expect(
        errors,
        v1.get("exact_reuse_required") is True,
        "V1_EXACT_REUSE_NOT_REQUIRED",
    )
    _expect(
        errors,
        v1.get(
            "post_result_threshold_fold_horizon_regime_event_or_model_change_allowed"
        )
        is False,
        "V1_POST_RESULT_CONTRACT_CHANGE_ALLOWED",
    )
    section_hashes = _mapping(v1, "immutable_section_sha256", errors)
    _expect(
        errors,
        set(section_hashes) == set(IMMUTABLE_SECTIONS),
        "V1_IMMUTABLE_SECTION_SET_MISMATCH",
    )
    for section_name in IMMUTABLE_SECTIONS:
        _expect(
            errors,
            section_hashes.get(section_name)
            == _section_sha256(v1_policy.get(section_name)),
            f"V1_IMMUTABLE_SECTION_HASH_MISMATCH:{section_name}",
        )
    capability = _mapping(v1, "capability_status", errors)
    _expect(
        errors,
        dict(capability)
        == {
            "value": "NOT_EVALUATED",
            "no_measurable_skill_allowed": False,
            "reason": (
                "V1_CLOSED_BEFORE_MODEL_TRAINING_DUE_TO_"
                "INSUFFICIENT_COVERAGE_OR_DQ"
            ),
        },
        "V1_CAPABILITY_STATUS_MISMATCH",
    )

    outcome = _mapping(policy, "outcome_contract", errors)
    _expect(
        errors,
        dict(outcome) == EXPECTED_OUTCOME_CONTRACT,
        "STOP_MATRIX_MISMATCH",
    )
    outcome_pass = _mapping(outcome, "PASS", errors)
    _expect(
        errors,
        outcome_pass.get("output_class") == "COVERAGE_ELIGIBLE_PASS_ONLY",
        "STOP_MATRIX_PASS_OUTPUT_MISMATCH",
    )
    _expect(
        errors,
        outcome_pass.get("model_training_allowed") is False,
        "STOP_MATRIX_PASS_MODEL_TRAINING_ALLOWED",
    )
    _expect(
        errors,
        outcome_pass.get("next_action")
        == "STOP_AND_REQUIRE_NEW_OWNER_CANONICAL_RUN_DECISION",
        "STOP_MATRIX_PASS_NEXT_ACTION_MISMATCH",
    )
    outcome_fail = _mapping(outcome, "FAIL", errors)
    _expect(
        errors,
        outcome_fail.get("capability_class_allowed") is False
        and outcome_fail.get("next_action") == "STOP",
        "STOP_MATRIX_FAIL_MISMATCH",
    )
    outcome_insufficient = _mapping(outcome, "INSUFFICIENT", errors)
    _expect(
        errors,
        dict(outcome_insufficient)
        == {
            "output_class": "INSUFFICIENT_COVERAGE_OR_DQ",
            "capability_class_allowed": False,
            "retry_allowed": False,
            "next_action": "CLOSE_V2_AND_STOP",
        },
        "STOP_MATRIX_INSUFFICIENT_MISMATCH",
    )
    outcome_invalid = _mapping(outcome, "INVALID", errors)
    _expect(
        errors,
        outcome_invalid.get("capability_class_allowed") is False
        and outcome_invalid.get("quarantine_required") is True
        and outcome_invalid.get("next_action") == "QUARANTINE_AND_STOP"
        and "INDEPENDENT_VALIDATOR_ERROR"
        in outcome_invalid.get("conditions", []),
        "STOP_MATRIX_INVALID_MISMATCH",
    )

    authorization = _mapping(policy, "authorization", errors)
    _expect(
        errors,
        dict(authorization) == EXPECTED_AUTHORIZATION,
        "AUTHORIZATION_BOUNDARY_MISMATCH",
    )
    safety = _mapping(policy, "safety", errors)
    _expect(errors, dict(safety) == EXPECTED_SAFETY, "SAFETY_BOUNDARY_MISMATCH")

    for path in _not_selected_paths(policy):
        errors.append(f"NOT_SELECTED_PRESENT:{path}")

    try:
        serialized_once = canonical_policy_bytes(policy)
        serialized_twice = canonical_policy_bytes(json.loads(serialized_once))
        _expect(
            errors,
            serialized_once == serialized_twice,
            "DETERMINISTIC_SERIALIZATION_MISMATCH",
        )
    except (TypeError, ValueError, json.JSONDecodeError):
        serialized_once = b""
        errors.append("DETERMINISTIC_SERIALIZATION_ERROR")

    error_tuple = tuple(sorted(set(errors)))
    return ValidationResult(
        status="PASS" if not error_tuple else "FAIL",
        task_id=STABLE_TASK_ID,
        policy_path=policy_file.as_posix(),
        policy_sha256=_sha256_bytes(policy_bytes),
        deterministic_serialization_sha256=_sha256_bytes(serialized_once),
        error_count=len(error_tuple),
        errors=error_tuple,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "静态验证 TRADING-2467 inactive O1 blind calendar re-entry policy；"
            "不访问 retained runtime evidence、cache 或市场数据，"
            "不执行 DQ、coverage、model、canonical 或 downstream action。"
        )
    )
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--policy", type=Path)
    args = parser.parse_args()
    result = validate_blind_calendar_reentry_policy(
        project_root=args.project_root,
        policy_path=args.policy,
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False, sort_keys=True))
    return 0 if result.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
