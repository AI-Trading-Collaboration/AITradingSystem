from __future__ import annotations

import copy
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_baseline_contract_decision_pack as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import load_report_registry
from ai_trading_system.research_quality import (
    growth_tilt_baseline_contract_decision_pack as decision_pack,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_real_decision_pack_is_ready_but_owner_decisions_and_m1d2_are_blocked() -> None:
    payload = _build()

    assert payload["status"] == decision_pack.READY_STATUS
    assert payload["m1d1_decision_complete"] is False
    assert payload["m1d2_implementation_allowed"] is False
    assert payload["m1d2_readiness_status"] == decision_pack.M1D2_BLOCKED_STATUS
    assert payload["m2_eligible_candidate_count"] == 0
    assert payload["strict_validation_errors"] == []


def test_offline_selection_result_is_not_treated_as_runtime_value() -> None:
    payload = _build()
    correction = payload["interpretation_correction"]

    assert correction["field"] == "do_not_de_risk_pass"
    assert correction["classification"] == "OFFLINE_SELECTION_RESULT_NOT_RUNTIME_VALUE"
    assert correction["offline_selection_pass"] is False
    assert correction["mapping_readiness_separate_from_runtime_activation"] is True
    assert correction["candidate_may_force_signal_true"] is False


def test_recovery_producer_is_callable_even_when_offline_selection_failed() -> None:
    recovery = _build()["recovery_persistence_decision"]

    assert recovery["producer_callable"] is True
    assert recovery["output_path_resolved"] is True
    assert recovery["semantics_registered"] is True
    assert recovery["offline_selection_pass"] is False
    assert recovery["offline_selection_role"] == (
        "OFFLINE_SELECTION_RESULT_NOT_RUNTIME_VALUE"
    )


def test_recovery_contract_remains_blocked_by_lineage_consumption_and_persistence() -> None:
    recovery = _build()["recovery_persistence_decision"]

    assert recovery["pit_lineage_valid"] is False
    assert recovery["compiler_consumes_recovery_permission"] is False
    assert recovery["baseline_required_consecutive_steps"] is None
    assert recovery["baseline_persistence_at_least_two"] is False
    assert recovery["contract_ready"] is False


def test_candidate_a_stays_approved_but_m2_ineligible() -> None:
    row = _candidate(_build(), "recovery_reentry_speedup_guard")

    assert row["decision"] == "APPROVE"
    assert row["m2_eligibility"] == "BLOCKED_PENDING_BASELINE_CONTRACTS"
    assert row["m2_eligible"] is False
    assert "RECOVERY_PERSISTENCE_CONTRACT_NOT_READY" in row["blocker_codes"]


def test_candidate_b_is_redefined_without_inventing_soft_confirmation() -> None:
    payload = _build()
    row = _candidate(payload, "false_risk_off_confirmation_relaxation")
    defensive = payload["defensive_entry_decision"]

    assert row["decision"] == "REDEFINE"
    assert row["proposed_candidate_id"] == (
        "non_hard_defensive_entry_persistence_guard"
    )
    assert defensive["existing_callable_soft_confirmation_found"] is False
    assert defensive["existing_callable_aggregate_non_hard_request_found"] is False
    assert defensive["compiler_accepts_defensive_hold_input"] is True
    assert defensive["defensive_hold_producer_callable"] is False
    assert defensive["withdraw_condition_met"] is True


def test_candidate_c_remains_redefined_and_excluded() -> None:
    row = _candidate(_build(), "missed_upside_reentry_accelerator")

    assert row["decision"] == "REDEFINE"
    assert row["m2_eligible"] is False
    assert row["second_owner_approval_required"] is True


def test_hard_veto_matrix_preserves_exact_baseline_identity_and_order() -> None:
    rows = _build()["hard_veto_resolution_matrix"]["components"]

    assert [row["veto_id"] for row in rows] == list(decision_pack.EXPECTED_VETO_IDS)
    assert all(row["required_by_baseline"] is True for row in rows)
    assert all(row["priority"] == "BEFORE_CANDIDATE_OVERLAY" for row in rows)


def test_callable_volatility_and_static_tqqq_guards_are_resolved() -> None:
    payload = _build()
    for veto_id in ("volatility_veto", "tqqq_veto"):
        row = _veto(payload, veto_id)
        assert row["resolution_status"] == "RESOLVED_CALLABLE"
        assert row["producer_callable"] is True
        assert row["ready"] is True


def test_unresolved_hard_veto_components_block_complete_aggregate() -> None:
    matrix = _build()["hard_veto_resolution_matrix"]

    assert matrix["complete_baseline_set"] is False
    assert matrix["unresolved_component_ids"] == [
        "risk_off_veto",
        "event_risk_veto",
        "trend_break_veto",
    ]
    assert matrix["missing_evidence_policy"] == "BLOCKED_NOT_FALSE"


def test_event_risk_veto_is_not_fabricated_or_marked_not_applicable() -> None:
    row = _veto(_build(), "event_risk_veto")

    assert row["resolution_status"] == "BLOCKED_NO_PIT_CONTRACT"
    assert row["producer_callable"] is False
    assert row["pit_lineage_ref"] is None
    assert row["not_applicable_rationale"] is None


def test_transition_states_exist_but_requested_applied_interface_does_not() -> None:
    transition = _build()["transition_exposure_decision"]["transition"]

    assert transition["canonical_state_ids"] == list(decision_pack.EXPECTED_STATES)
    assert transition["canonical_state_schema_ready"] is True
    assert transition["existing_same_row_label_mutation"] is True
    assert transition["requested_applied_split_requested"] is True
    assert transition["requested_applied_split_callable"] is False
    assert transition["contract_ready"] is False


def test_existing_qqq_equivalent_formula_is_cap_only_not_candidate_delta() -> None:
    exposure = _build()["transition_exposure_decision"]["exposure"]

    assert exposure["qqq_equivalent_supported"] is True
    assert exposure["qqq_equivalent_formula_callable"] is True
    assert exposure["qqq_equivalent_cap"] == 0.75
    assert exposure["qqq_equivalent_scope"] == (
        "EXISTING_CAP_ONLY_DERIVED_SCALAR_NOT_CANDIDATE_DELTA"
    )
    assert exposure["candidate_delta_may_use_qqq_equivalent"] is False
    assert exposure["tqqq_increase_allowed"] is False


def test_native_scalar_fields_remain_explicit_owner_decisions() -> None:
    exposure = _build()["transition_exposure_decision"]["exposure"]

    assert exposure["native_scalar_id"] is None
    assert exposure["minimum_increment"] is None
    assert exposure["current_scalar_field"] is None
    assert exposure["requested_target_scalar_field"] is None
    assert exposure["applied_target_scalar_field"] is None
    assert exposure["contract_ready"] is False


def test_owner_action_list_covers_all_unresolved_contract_areas() -> None:
    payload = _build()
    areas = {row["area"] for row in payload["owner_actions"]}

    assert payload["owner_action_count"] == len(payload["owner_actions"])
    assert {"recovery_persistence", "candidate_b", "hard_veto", "transition", "exposure"} <= areas


def test_decision_pack_has_no_replay_metric_or_candidate_behavior_side_effect() -> None:
    payload = _build()

    assert payload["data_quality_gate_executed"] is False
    assert payload["replay_run"] is False
    assert payload["runtime_metrics_generated"] is False
    assert payload["six_metric_runtime_artifact_generated"] is False
    assert payload["candidate_behavior_implemented"] is False
    assert payload["candidate_parameters_changed"] is False
    assert payload["threshold_values_changed"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"


def test_source_artifacts_have_absolute_paths_hashes_and_sizes() -> None:
    payload = _build()

    assert payload["source_artifacts"]
    assert all(Path(row["path"]).is_absolute() for row in payload["source_artifacts"])
    assert all(len(row["sha256"]) == 64 for row in payload["source_artifacts"])
    assert all(row["size_bytes"] > 0 for row in payload["source_artifacts"])


def test_source_schema_drift_fails_strict_contract() -> None:
    sources = _sources()
    sources["owner_review"]["schema_version"] = "wrong.v1"
    payload = _build(sources=sources)

    assert "source_schema_mismatch:owner_review" in payload["strict_validation_errors"]
    assert payload["status"] == decision_pack.BLOCKED_STATUS


def test_candidate_disposition_drift_fails_strict_contract() -> None:
    sources = _sources()
    sources["owner_review"]["candidate_disposition"][1]["decision"] = "APPROVE"
    payload = _build(sources=sources)

    assert "candidate_disposition_mismatch" in payload["strict_validation_errors"]


def test_dynamic_runner_writes_all_decision_artifacts(tmp_path: Path) -> None:
    payload = impl.run_growth_tilt_baseline_contract_decision_pack(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        as_of_date=date(2026, 7, 10),
        strict=True,
    )

    assert payload["status"] == decision_pack.READY_STATUS
    for filename in (
        "growth_tilt_baseline_contract_decision_pack.json",
        "growth_tilt_candidate_disposition_after_baseline_audit.json",
        "growth_tilt_hard_veto_resolution_matrix.json",
        "growth_tilt_transition_exposure_decision.json",
    ):
        path = tmp_path / "outputs" / filename
        assert path.exists()
        assert json.loads(path.read_text(encoding="utf-8"))
    assert (tmp_path / "docs" / "growth_tilt_baseline_contract_decision_pack.md").exists()


def test_dynamic_runner_strict_mode_rejects_missing_source(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="owner_review missing"):
        impl.run_growth_tilt_baseline_contract_decision_pack(
            owner_review_path=tmp_path / "missing.yaml",
            output_root=tmp_path / "outputs",
            docs_root=tmp_path / "docs",
            as_of_date=date(2026, 7, 10),
            strict=True,
        )


def test_cli_real_decision_pack_run(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-baseline-contract-decision-pack",
            "--output-root",
            str(tmp_path / "outputs"),
            "--docs-root",
            str(tmp_path / "docs"),
            "--as-of",
            "2026-07-10",
            "--strict",
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert decision_pack.READY_STATUS in result.output
    assert "m1d2_implementation_allowed=false" in result.output
    assert "m2_eligible_candidate_count=0" in result.output
    assert "runtime_metrics_generated=false" in result.output


def test_registry_catalog_flow_and_task_register_are_aligned() -> None:
    registry = load_report_registry(impl.DEFAULT_REPORT_REGISTRY_PATH)
    entry = next(
        row
        for row in registry["reports"]
        if row["report_id"] == decision_pack.REPORT_TYPE
    )

    assert entry["command"] == (
        "aits research strategies growth-tilt-baseline-contract-decision-pack"
    )
    assert entry["production_effect"] == "none"
    assert all(
        ref in impl.DEFAULT_ARTIFACT_CATALOG_PATH.read_text(encoding="utf-8")
        for ref in decision_pack.REQUIRED_CATALOG_REFERENCES
    )
    assert all(
        ref in impl.DEFAULT_SYSTEM_FLOW_PATH.read_text(encoding="utf-8")
        for ref in decision_pack.REQUIRED_FLOW_REFERENCES
    )
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "TRADING-2438M1D1_GROWTH_TILT_BASELINE_CONTRACT_DECISION_PACK" in task_register
    assert "TRADING-2438M1D2_GROWTH_TILT_BASELINE_CONTRACT_ADAPTERS_AND_READINESS" in task_register


def _build(*, sources: dict[str, Any] | None = None) -> dict[str, Any]:
    resolved = sources or _sources()
    return decision_pack.build_growth_tilt_baseline_contract_decision_pack(
        resolved,
        source_artifacts=impl._source_artifact_records(
            _all_paths(), resolved, _yaml_paths()
        ),
        report_registry=resolved["report_registry"],
        artifact_catalog_text=resolved["artifact_catalog_text"],
        system_flow_text=resolved["system_flow_text"],
        requirement_text=resolved["requirement_text"],
        as_of="2026-07-10",
    )


def _sources() -> dict[str, Any]:
    sources = {
        key: copy.deepcopy(safe_load_yaml_path(path))
        for key, path in _yaml_paths().items()
    }
    sources.update(
        {
            "channel_code_text": impl.DEFAULT_CHANNEL_CODE_PATH.read_text(
                encoding="utf-8"
            ),
            "compiler_code_text": impl.DEFAULT_COMPILER_CODE_PATH.read_text(
                encoding="utf-8"
            ),
            "m1c_report_text": impl.DEFAULT_M1C_REPORT_PATH.read_text(encoding="utf-8"),
            "requirement_text": impl.DEFAULT_REQUIREMENT_DOC_PATH.read_text(
                encoding="utf-8"
            ),
            "artifact_catalog_text": impl.DEFAULT_ARTIFACT_CATALOG_PATH.read_text(
                encoding="utf-8"
            ),
            "system_flow_text": impl.DEFAULT_SYSTEM_FLOW_PATH.read_text(
                encoding="utf-8"
            ),
        }
    )
    return sources


def _yaml_paths() -> dict[str, Path]:
    return {
        "channel_config": impl.DEFAULT_CHANNEL_CONFIG_PATH,
        "final_matrix": impl.DEFAULT_FINAL_MATRIX_PATH,
        "signal_usage_matrix": impl.DEFAULT_SIGNAL_USAGE_MATRIX_PATH,
        "base_policy": impl.DEFAULT_BASE_POLICY_PATH,
        "risk_veto_policy": impl.DEFAULT_RISK_VETO_POLICY_PATH,
        "probe_registry": impl.DEFAULT_PROBE_REGISTRY_PATH,
        "owner_review": impl.DEFAULT_OWNER_REVIEW_PATH,
        "report_registry": impl.DEFAULT_REPORT_REGISTRY_PATH,
    }


def _all_paths() -> list[Path]:
    return [
        *_yaml_paths().values(),
        impl.DEFAULT_CHANNEL_CODE_PATH,
        impl.DEFAULT_COMPILER_CODE_PATH,
        impl.DEFAULT_M1C_REPORT_PATH,
        impl.DEFAULT_REQUIREMENT_DOC_PATH,
        impl.DEFAULT_ARTIFACT_CATALOG_PATH,
        impl.DEFAULT_SYSTEM_FLOW_PATH,
    ]


def _candidate(payload: dict[str, Any], candidate_id: str) -> dict[str, Any]:
    return next(
        row
        for row in payload["candidate_disposition_after_baseline_audit"]["candidates"]
        if row["candidate_id"] == candidate_id
    )


def _veto(payload: dict[str, Any], veto_id: str) -> dict[str, Any]:
    return next(
        row
        for row in payload["hard_veto_resolution_matrix"]["components"]
        if row["veto_id"] == veto_id
    )
