from __future__ import annotations

import hashlib
from pathlib import Path

from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_PATH = Path("config/architecture/arch_004_refactor_policy.yaml")
RECONCILIATION_PATH = Path("inputs/architecture/arch_004_predecessor_reconciliation.yaml")
GLOSSARY_PATH = Path("config/architecture/research_semantic_glossary.yaml")
COMPATIBILITY_BASELINE_PATH = Path("inputs/architecture/arch_004_compatibility_baseline.yaml")
ATTRIBUTION_PATH = Path("inputs/architecture/arch_004_worktree_attribution.yaml")


def test_arch_004_phase_b_complete_policy_keeps_freeze_and_preserves_safety() -> None:
    policy = safe_load_yaml_path(POLICY_PATH)

    assert policy["schema_version"] == "arch_004_refactor_policy.v1"
    assert policy["status"] == "phase_b_complete_phase_c_ready"
    assert policy["program"]["current_phase"] == "ARCH-004B"
    assert policy["program"]["current_phase_status"] == "COMPLETE"
    assert policy["program"]["next_phase"] == "ARCH-004C"
    assert policy["program"]["next_phase_unblocked"] is True
    assert policy["feature_freeze"]["active"] is True
    assert "NEW_TASK_SHAPED_RESEARCH_MODULE" in policy["feature_freeze"]["forbidden_change_classes"]
    assert (
        "STRUCTURAL_REFACTOR_MIXED_WITH_STRATEGY_TUNING"
        in policy["feature_freeze"]["forbidden_change_classes"]
    )
    assert policy["behavior_preservation"]["strategy_logic_changed"] is False
    assert policy["behavior_preservation"]["threshold_changed"] is False
    enforcement = policy["semantic_kernel_enforcement"]
    assert enforcement["active_for_new_investment_facing_artifacts"] is True
    assert enforcement["schema_version"] == "research_evaluation_context.v1"
    assert enforcement["requested_range_may_substitute_actual_or_effective_range"] is False
    assert enforcement["legacy_flat_fields_require_exact_parity"] is True
    assert enforcement["reference_consumer"] == "first_layer_v2_effective_coverage_audit"
    assert enforcement["next_phase_unblocked"] is True
    assert policy["phase_b_completion"]["full_parallel_validation"]["passed"] == 5375
    assert policy["phase_b_completion"]["full_parallel_validation"]["failed"] == 0
    assert policy["safety_boundary"] == {
        "research_only": True,
        "architecture_governance_only": True,
        "production_effect": "none",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "data_quality_gate_bypass_allowed": False,
    }


def test_arch_004_reconciliation_reuses_predecessors_and_unblocks_phase_b() -> None:
    payload = safe_load_yaml_path(RECONCILIATION_PATH)

    assert payload["status"] == "ARCH_004_PREDECESSOR_RECONCILIATION_COMPLETE"
    assert payload["historical_baseline"]["closeout_status"] == "ENGINEERING_CLOSEOUT_READY"
    current = payload["current_control_plane_evidence"]
    assert current["engineering_surface_inventory"]["surface_count"] == 3812
    assert current["artifact_lifecycle"]["validation_status"] == "FAIL"
    assert current["engineering_stage_b"]["validation_status"] == "FAIL"
    assert current["canonical_system"]["doctor_status"] == "FAIL"
    assert current["reader_brief_consistency"]["native_template_gap_count"] == 1634

    dispositions = {
        row["capability_id"]: row["disposition"] for row in payload["predecessor_capabilities"]
    }
    assert dispositions["root_cli_modularization"] == "REUSE"
    assert dispositions["artifact_ref_and_workflow_types"] == "EXTEND"
    assert dispositions["reader_brief_effective_view_model"] == "CARRY_FORWARD"
    assert dispositions["clean_clone_release_acceptance"] == "REUSE"
    assert payload["phase_a_gate"] == {
        "predecessor_reconciliation_complete": True,
        "current_control_plane_evidence_captured": True,
        "full_parallel_validation_baseline_recorded": True,
        "existing_failures_have_root_cause_and_linked_task": True,
        "semantic_glossary_frozen": True,
        "command_and_artifact_compatibility_baseline_frozen": True,
        "shared_file_ownership_frozen": True,
        "clean_handoff_or_attributable_isolation_proven": True,
        "phase_b_unblocked": True,
    }
    assert current["full_parallel_validation"]["exit_validation"]["status"] == "PASS"
    assert current["full_parallel_validation"]["exit_validation"]["failed"] == 0
    assert payload["safety_boundary"]["waivers_added"] == 0


def test_arch_004_semantic_glossary_separates_regime_and_research_window() -> None:
    glossary = safe_load_yaml_path(GLOSSARY_PATH)
    terms = glossary["canonical_terms"]

    assert glossary["status"] == "frozen_phase_a"
    assert str(terms["anchor_event_date"]["canonical_value"]) == "2022-11-30"
    assert str(terms["market_regime_start"]["canonical_value"]) == "2022-12-01"
    assert terms["primary_research_window_id"]["canonical_value"] == ("exact_three_asset_validated")
    assert str(terms["primary_research_window_start"]["canonical_value"]) == ("2021-02-22")
    assert terms["primary_research_window_id"]["not_global_for_unrelated_asset_families"]
    assert glossary["resolution_rules"]["conflict_behavior"] == "FAIL_CLOSED"
    assert glossary["resolution_rules"]["implicit_date_aliasing_allowed"] is False
    assert (
        glossary["reporting_contract"]["market_regime_start_may_substitute_research_window_start"]
        is False
    )
    implementation = glossary["implementation_boundary"]
    assert implementation["runtime_enforcement_implemented"] is True
    assert implementation["context_schema_version"] == "research_evaluation_context.v1"
    assert implementation["existing_artifact_migration_status"] == "GOVERNED_WAVES_PENDING"


def test_arch_004_compatibility_baseline_freezes_surface_and_core_hashes() -> None:
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)

    assert baseline["status"] == "FROZEN_AFTER_ARCH_004A1_REMEDIATION"
    assert baseline["surface_inventory"]["total"] == 3812
    assert baseline["surface_inventory"]["types"]["cli_command"] == 1103
    assert baseline["surface_inventory"]["types"]["report_registry_entry"] == 1358
    assert baseline["repository_inventory"]["python_module_count"] == 752
    assert baseline["validation_baseline"]["full_after_fix"]["status"] == "PASS"
    assert baseline["validation_baseline"]["full_after_fix"]["failed"] == 0
    date_range_contract = baseline["explicit_cli_adapter_contracts"]["date_range_kwargs"]
    assert date_range_contract["positional_parameters"] == [
        "as_of",
        "start_date",
        "end_date",
    ]
    assert date_range_contract["exact_output_keys"] == [
        "as_of_date",
        "start_date",
        "end_date",
    ]
    assert str(date_range_contract["missing_start_default"]) == "2022-12-01"
    assert baseline["explicit_cli_adapter_contracts"]["as_of_kwargs"] == {
        "positional_parameters": ["as_of"],
        "exact_output_keys": ["as_of_date"],
    }
    for source in baseline["frozen_sources"]:
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["contract_id"]
    phase_b = baseline["phase_b_semantic_kernel"]
    assert phase_b["status"] == "COMPLETE_PHASE_C_READY"
    assert phase_b["contract_schema"] == "research_evaluation_context.v1"
    assert phase_b["repository_inventory"] == {
        "python_module_count": 757,
        "python_test_file_count": 1100,
    }
    assert phase_b["validation"]["focused"]["passed"] == 74
    assert phase_b["validation"]["contract_validation"]["passed"] == 197
    assert phase_b["validation"]["full_parallel"]["status"] == "PASS"
    assert phase_b["validation"]["full_parallel"]["passed"] == 5375
    assert phase_b["validation"]["full_parallel"]["failed"] == 0
    for source in phase_b["sources"]:
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]


def test_arch_004_worktree_attribution_excludes_concurrent_user_changes() -> None:
    attribution = safe_load_yaml_path(ATTRIBUTION_PATH)

    assert attribution["status"] == "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_B"
    excluded = set(attribution["excluded_user_or_other_task_paths"])
    assert excluded == {
        "docs/research/growth_tilt_owner_decision_resolution.md",
        "docs/research/indicator_family_only_model_review.md",
        "docs/research/layer1_selector_pause_or_continue_owner_pack.md",
    }
    assert attribution["staging_rule"]["exclude_user_or_other_task_paths"] is True
    assert attribution["safety_boundary"]["user_changes_preserved"] is True
