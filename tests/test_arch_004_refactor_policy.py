from __future__ import annotations

import hashlib
from pathlib import Path

from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_PATH = Path("config/architecture/arch_004_refactor_policy.yaml")
RECONCILIATION_PATH = Path("inputs/architecture/arch_004_predecessor_reconciliation.yaml")
GLOSSARY_PATH = Path("config/architecture/research_semantic_glossary.yaml")
COMPATIBILITY_BASELINE_PATH = Path("inputs/architecture/arch_004_compatibility_baseline.yaml")
ATTRIBUTION_PATH = Path("inputs/architecture/arch_004_worktree_attribution.yaml")
DEPENDENCY_POLICY_PATH = Path("config/architecture/arch_004c_dependency_policy.yaml")
DIRECT_WRITER_BASELINE_PATH = Path("inputs/architecture/arch_004c_direct_writer_baseline.yaml")


def test_arch_004_phase_g_in_progress_policy_keeps_freeze_and_preserves_safety() -> None:
    policy = safe_load_yaml_path(POLICY_PATH)

    assert policy["schema_version"] == "arch_004_refactor_policy.v1"
    assert policy["status"] == "phase_g_in_progress"
    assert policy["program"]["current_phase"] == "ARCH-004G"
    assert policy["program"]["current_phase_status"] == "IN_PROGRESS"
    assert policy["program"]["next_phase"] == "ARCH-004H"
    assert policy["program"]["next_phase_unblocked"] is False
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
    phase_c = policy["phase_c_execution"]
    assert phase_c["status"] == "COMPLETE"
    assert phase_c["stages"] == {
        "C1_pure_contracts": "COMPLETE",
        "C2_canonical_io_and_facades": "COMPLETE",
        "C3_typed_config_resolver_and_split": "COMPLETE",
        "C4_workflow_and_report_adapters": "COMPLETE",
        "C5_architecture_dependency_gate": "COMPLETE",
        "C6_reference_integration_and_closeout": "COMPLETE",
    }
    assert phase_c["new_direct_artifact_writer_allowed"] is False
    assert phase_c["domain_wide_migration_allowed"] is False
    assert phase_c["completion_validation"]["focused"]["passed"] == 120
    assert phase_c["completion_validation"]["architecture_gate"] == {
        "status": "PASS",
        "scanned_python_files": 770,
        "frozen_direct_writer_calls": 894,
        "current_direct_writer_calls": 893,
        "violations": 0,
    }
    assert phase_c["completion_validation"]["contract_validation"]["passed"] == 197
    assert phase_c["completion_validation"]["full_parallel_validation"]["passed"] == 5404
    assert phase_c["completion_validation"]["full_parallel_validation"]["failed"] == 0
    phase_d = policy["phase_d_execution"]
    assert phase_d["status"] == "COMPLETE"
    assert phase_d["reference_slice"] == "growth_tilt_candidate_family_closure"
    assert phase_d["stages"] == {
        "D1_characterization_and_typed_spec": "COMPLETE",
        "D2_generic_runner_and_plugin_interfaces": "COMPLETE",
        "D3_reference_plugin_and_legacy_facade": "COMPLETE",
        "D4_envelope_ledger_report_integration": "COMPLETE",
        "D5_parity_proof_and_closeout": "COMPLETE",
    }
    assert phase_d["new_task_id_python_module_allowed"] is False
    assert phase_d["strategy_or_research_conclusion_change_allowed"] is False
    assert phase_d["completion_validation"]["focused"]["passed"] == 77
    assert phase_d["completion_validation"]["architecture_gate"] == {
        "status": "PASS",
        "scanned_python_files": 775,
        "frozen_direct_writer_calls": 894,
        "current_direct_writer_calls": 893,
        "violations": 0,
    }
    assert phase_d["completion_validation"]["contract_validation"]["passed"] == 197
    assert phase_d["completion_validation"]["full_parallel_validation"]["passed"] == 5411
    assert phase_d["completion_validation"]["full_parallel_validation"]["failed"] == 0
    phase_e = policy["phase_e_execution"]
    assert phase_e["status"] == "COMPLETE"
    assert phase_e["stages"] == {
        "E1_ownership_policy_and_manifests": "COMPLETE",
        "E2_impact_selection": "COMPLETE",
        "E3_architecture_fitness": "COMPLETE",
        "E4_scaffold_and_aggregate_fragments": "COMPLETE",
        "E5_control_plane_integration_and_closeout": "COMPLETE",
    }
    assert phase_e["existing_aggregate_source_of_truth_changed"] is False
    assert phase_e["impact_selection_may_replace_full_validation"] is False
    assert phase_e["worker_may_edit_shared_aggregates"] is False
    completion = phase_e["completion_validation"]
    assert completion["generated_manifests"] == {
        "status": "PASS",
        "module_count": 777,
        "test_file_count": 1107,
        "orphan_count": 0,
        "specific_overlap_count": 0,
    }
    assert completion["aggregate_shadow"] == {
        "status": "SHADOW_COMPATIBILITY_PASS",
        "target_count": 3,
        "fragment_count": 4,
        "existing_source_of_truth_changed": False,
    }
    assert completion["architecture_fitness"]["status"] == "PASS"
    assert completion["architecture_fitness"]["violations"] == 0
    assert completion["architecture_tier"]["passed"] == 78
    assert completion["contract_validation"]["passed"] == 197
    assert completion["full_parallel_validation"]["passed"] == 5420
    assert completion["full_parallel_validation"]["failed"] == 0
    phase_f2 = policy["phase_f2_execution"]
    assert phase_f2["status"] == "COMPLETE"
    assert phase_f2["stages"] == {
        "F2_1_current_state_inventory_and_trace": "COMPLETE",
        "F2_2_authoritative_execution_chain_document": "COMPLETE",
        "F2_3_lifecycle_contract_and_review_boundary": "DOCUMENTED_BASELINE_COMPLETE",
        "F2_4_reference_integration_and_validation": "COMPLETE",
        "F2_5_generic_lifecycle_runtime_migration": "COMPLETE",
        "F2_5a_canonical_contract_and_state_machine": "COMPLETE",
        "F2_5b_legacy_campaign_compatibility_assessment": "COMPLETE",
        "F2_5c_optional_experiment_lifecycle_plugin": "COMPLETE",
        "F2_5d_growth_tilt_reference_sidecar_parity": "COMPLETE",
        "F2_5e_validation_and_closeout": "COMPLETE",
    }
    assert phase_f2["periodic_review_may_auto_tune"] is False
    assert phase_f2["result_visible_before_preregistration_freeze_allowed"] is False
    assert phase_f2["strategy_or_threshold_change_allowed_in_documentation_slice"] is False
    assert phase_f2["interim_evidence"]["market_regime_and_research_window_separated"] is True
    assert phase_f2["interim_evidence"]["production_effect"] == "none"
    assert phase_f2["documentation_validation"]["focused_docs_and_policy"]["passed"] == 23
    assert phase_f2["documentation_validation"]["architecture_fitness"]["passed"] == 80
    assert phase_f2["documentation_validation"]["contract_validation"]["passed"] == 197
    assert phase_f2["competing_campaign_runner_allowed"] is False
    runtime_validation = phase_f2["runtime_migration_completion_validation"]
    assert runtime_validation["focused"] == {"status": "PASS", "passed": 15}
    assert runtime_validation["scoped_mypy"] == {"status": "PASS"}
    assert runtime_validation["old_core_artifact_bytes_parity"] == "PASS"
    assert runtime_validation["legacy_campaign_missing_binding_behavior"] == "BLOCKED"
    assert runtime_validation["architecture_fitness"]["passed"] == 88
    assert runtime_validation["contract_validation"]["passed"] == 197
    assert runtime_validation["full_parallel"]["passed"] == 5430
    assert runtime_validation["full_parallel"]["failed"] == 0
    phase_f1 = policy["phase_f1_execution"]
    assert phase_f1["status"] == "COMPLETE"
    assert phase_f1["stages"] == {
        "F1_1_inventory_due_contract_and_compatibility_adapter": "COMPLETE",
        "F1_2_shadow_plan_and_daily_parity": "COMPLETE",
        "F1_3_lock_retry_idempotency_and_resume": "COMPLETE",
        "F1_4_daily_executor_adapter_cut_in": "COMPLETE",
        "F1_5_non_daily_controlled_due_dispatch": "COMPLETE",
        "F1_6_validation_and_closeout": "COMPLETE",
    }
    assert phase_f1["scheduled_task_inventory"] == {
        "daily": 37,
        "non_daily": 41,
        "total": 78,
    }
    assert phase_f1["unified_external_trigger"] == "aits ops daily-run"
    assert phase_f1["additional_external_scheduler_entry_allowed"] is False
    assert phase_f1["non_daily_automatic_dispatch_enabled"] is False
    assert phase_f1["non_daily_manual_dispatch_enabled"] is True
    assert phase_f1["legacy_dispatch_enabled_by_shadow_adapter"] is False
    runtime_control = phase_f1["runtime_control"]
    assert runtime_control["policy_id"] == "operations_runtime_control_v1"
    assert runtime_control["deterministic_idempotency_key"] is True
    assert runtime_control["concurrent_workflow_date_lock"] == "BLOCKED"
    assert runtime_control["stale_lock_recovery"] == "EXPIRED_ONLY"
    assert runtime_control["duplicate_completed_trigger"] == "ALREADY_COMPLETE"
    assert runtime_control["step_attempt_budget_from_workflow_spec"] is True
    assert runtime_control["non_idempotent_partial_resume"] == "BLOCKED"
    assert runtime_control["legacy_daily_executor_cut_in_enabled"] is True
    assert runtime_control["execution_ledger_schema"] == "run_ledger.v1"
    assert runtime_control["validate_data_failure_blocks_downstream"] is True
    assert runtime_control["non_daily_dispatch_enabled"] is True
    assert phase_f1["compatibility_findings"]["periodic_task_plan_count"] == 41
    assert phase_f1["compatibility_findings"]["periodic_automatic_command_dispatch"] is False
    assert phase_f1["compatibility_findings"]["trading_day_daily_plan"] == "PASS"
    assert phase_f1["compatibility_findings"]["closed_market_daily_plan"] == "PASS"
    assert phase_f1["compatibility_findings"]["conditional_step_contract"] == {
        "official_policy_sources": "closed_market_only"
    }
    assert phase_f1["compatibility_findings"]["additive_shadow_artifact_emission"] == ("COMPLETE")
    phase_f3 = policy["phase_f3_execution"]
    assert phase_f3["status"] == "COMPLETE"
    assert phase_f3["stages"] == {
        "F3_1_inventory_policy_and_characterization": "COMPLETE",
        "F3_2_pure_contracts_and_typed_catalog": "COMPLETE",
        "F3_3_owner_daily_brief_reference_cut_in": "COMPLETE",
        "F3_4_research_review_pack": "COMPLETE",
        "F3_5_audit_index_and_generated_fragments": "COMPLETE",
        "F3_6_cut_in_parity_and_closeout": "COMPLETE",
    }
    assert phase_f3["owner_daily_brief_core_section_limit"] == 10
    assert phase_f3["owner_queue_requires_due_and_actionable"] is True
    assert phase_f3["reporting_layer_may_recompute_investment_conclusion"] is False
    assert phase_f3["legacy_unclassified_disposition"] == (
        "AUDIT_INDEX_LIMITED_UNCLASSIFIED"
    )
    assert phase_f3["next_phase_unblocked"] is True
    assert phase_f3["completion_validation"]["full_parallel"] == {
        "status": "PASS",
        "passed": 5494,
        "failed": 0,
        "warnings": 642,
        "elapsed_seconds": 923.4,
        "runtime_artifact": (
            "outputs/validation_runtime/full_20260711T040642Z/test_runtime_summary.json"
        ),
    }
    phase_g = policy["phase_g_execution"]
    assert phase_g["status"] == "IN_PROGRESS"
    assert phase_g["stages"]["G0_inventory_deprecation_policy_and_removal_gate"] == (
        "COMPLETE"
    )
    assert phase_g["stages"]["G1_shared_platform_helper_migration"] == "COMPLETE"
    assert phase_g["stages"]["G2_interfaces_and_etf_cli_migration"] == "IN_PROGRESS"
    assert phase_g["permanent_dual_track_allowed"] is False
    assert phase_g["runtime_removal_allowed_in_g0"] is False
    assert phase_g["investment_semantics_change_allowed"] is False
    assert phase_g["historical_artifact_deletion_allowed"] is False
    assert phase_g["g0_evidence"] == {
        "policy_path": "config/architecture/arch_004g_deprecation_policy.yaml",
        "inventory_path": "inputs/architecture/arch_004g_deprecation_inventory.yaml",
        "module_count": 795,
        "test_file_count": 1112,
        "priority_target_count": 9,
        "active_target_count": 6,
        "deprecated_target_count": 3,
        "removal_ready_count": 0,
        "direct_writer_baseline": 894,
        "direct_writer_current": 893,
        "dynamic_strategy_wrapper_count": 99,
        "matching_research_quality_implementation_count": 48,
        "runtime_removal_performed": False,
    }
    assert phase_g["g0_validation"]["focused"] == {"status": "PASS", "passed": 6}
    assert phase_g["g0_validation"]["architecture_fitness"]["passed"] == 156
    assert phase_g["g0_validation"]["contract_validation"]["passed"] == 203
    assert phase_g["g1_slices"] == {
        "G1_1_three_governance_module_writer_implementation_migration": "COMPLETE",
        "G1_2_internal_caller_migration_and_private_wrapper_removal": "COMPLETE",
        "G1_3_next_shared_helper_family": "COMPLETE",
        "G1_3a_trading_engine_summary_writer_migration": "COMPLETE",
        "G1_3b_next_shared_helper_family": "COMPLETE",
        "G1_3b_notification_retry_writer_migration": "COMPLETE",
        "G1_3c_next_shared_helper_family": "COMPLETE",
        "G1_3c_checksum_helper_migration": "COMPLETE",
        "G1_3d_runtime_metadata_helper_inventory": "COMPLETE",
        "G1_3d_pit_replay_observe_only_metadata_migration": "COMPLETE",
        "G1_3e_data_quality_and_safety_helper_inventory": "COMPLETE",
        "G1_3e_growth_tilt_data_quality_gate_migration": "COMPLETE",
    }
    assert phase_g["g1_current_evidence"]["direct_writer_before"] == 893
    assert phase_g["g1_current_evidence"]["direct_writer_after"] == 861
    assert phase_g["g1_current_evidence"]["direct_writer_reduction"] == 32
    assert phase_g["g1_current_evidence"]["private_compatibility_wrappers_remaining"] == 0
    assert phase_g["g1_current_evidence"]["private_compatibility_wrappers_removed"] == 32
    assert phase_g["g1_current_evidence"]["internal_callers_using_canonical_writer"] is True
    assert phase_g["g1_current_evidence"]["runtime_removal_performed"] is False
    assert phase_g["g1_first_family_validation"]["focused"] == {
        "status": "PASS",
        "passed": 29,
    }
    assert phase_g["g1_first_family_validation"]["architecture_fitness"]["passed"] == 159
    assert phase_g["g1_first_family_validation"]["architecture_fitness"][
        "current_direct_writer_calls"
    ] == 887
    assert phase_g["g1_second_family_plan"]["direct_writer_after"] == 877
    assert phase_g["g1_second_family_plan"]["removed_private_writer_count"] == 10
    assert phase_g["g1_second_family_plan"]["json_sort_keys"] is False
    assert phase_g["g1_second_family_plan"]["focused_validation"] == {
        "status": "PASS",
        "passed": 95,
    }
    assert phase_g["g1_second_family_plan"]["architecture_fitness"]["passed"] == 161
    assert phase_g["g1_third_family_plan"]["direct_writer_after"] == 861
    assert phase_g["g1_third_family_plan"]["removed_private_writer_count"] == 16
    assert phase_g["g1_third_family_plan"]["focused_validation"] == {
        "status": "PASS",
        "passed": 139,
    }
    assert phase_g["g1_third_family_plan"]["architecture_fitness"]["passed"] == 162
    assert phase_g["g1_current_evidence"]["canonical_checksum_helper"] == "sha256_path"
    assert phase_g["g1_current_evidence"]["private_checksum_helpers_removed"] == 8
    assert phase_g["g1_fourth_family_plan"]["canonical_caller_count"] == 13
    assert phase_g["g1_fourth_family_plan"]["direct_writer_count_unchanged"] == 861
    assert phase_g["g1_fourth_family_plan"]["focused_validation"] == {
        "status": "PASS",
        "passed": 155,
    }
    assert phase_g["g1_fourth_family_plan"]["architecture_fitness"]["passed"] == 164
    assert phase_g["g1_current_evidence"]["private_runtime_metadata_helpers_removed"] == 10
    assert phase_g["g1_current_evidence"][
        "canonical_runtime_metadata_helper"
    ] == "with_pit_replay_observe_only_runtime_metadata"
    assert phase_g["g1_fifth_family_plan"]["inventory_ast_field_group_count"] == 14
    assert phase_g["g1_fifth_family_plan"]["canonical_safety_false_field_count"] == 39
    assert phase_g["g1_fifth_family_plan"]["private_metadata_helper_remaining_count"] == 0
    assert phase_g["g1_fifth_family_plan"]["focused_validation"] == {
        "status": "PASS",
        "passed": 182,
    }
    assert phase_g["g1_fifth_family_plan"]["architecture_fitness"]["passed"] == 166
    assert phase_g["g1_current_evidence"]["canonical_data_quality_gate"] == (
        "run_growth_tilt_data_quality_gate"
    )
    assert phase_g["g1_current_evidence"]["private_data_quality_gate_helpers_removed"] == 15
    assert phase_g["g1_current_evidence"]["private_secondary_price_helpers_removed"] == 15
    assert phase_g["g1_sixth_family_plan"]["direct_validate_data_cache_call_required"] is True
    assert phase_g["g1_sixth_family_plan"]["exception_downgrade_allowed"] is False
    assert phase_g["g1_sixth_family_plan"]["private_gate_helper_remaining_count"] == 0
    assert phase_g["g1_sixth_family_plan"]["focused_validation"] == {
        "status": "PASS",
        "passed": 242,
    }
    assert phase_g["g1_sixth_family_plan"]["architecture_fitness"]["passed"] == 168
    assert phase_g["stages"]["G1_shared_platform_helper_migration"] == "COMPLETE"
    assert phase_g["stages"]["G2_interfaces_and_etf_cli_migration"] == "IN_PROGRESS"
    assert phase_g["g1_closeout"] == {
        "status": "COMPLETE",
        "canonical_family_count": 6,
        "private_helper_removal_count": 80,
        "direct_writer_before": 893,
        "direct_writer_after": 861,
        "direct_writer_reduction": 32,
        "dynamic_wrapper_lines_before_g1_3d": 89805,
        "dynamic_wrapper_lines_after_g1_3e": 88315,
        "dynamic_wrapper_line_reduction": 1490,
        "dynamic_wrapper_functions_before_g1_3d": 2154,
        "dynamic_wrapper_functions_after_g1_3e": 2114,
        "dynamic_wrapper_function_reduction": 40,
        "safety_assertion_groups_audited": 29,
        "unsafe_cross_semantic_abstraction_avoided": True,
        "legacy_callers_for_selected_families": 0,
        "architecture_fitness": {
            "status": "PASS",
            "passed": 168,
            "current_direct_writer_calls": 861,
            "violation_count": 0,
            "runtime_artifact": (
                "outputs/validation_runtime/"
                "architecture-fitness_20260711T064010Z/test_runtime_summary.json"
            ),
        },
        "production_effect": "none",
    }
    assert phase_g["g2_current_plan"]["status"] == "IN_PROGRESS"
    assert phase_g["g2_current_plan"]["implementation_started_in_g1_closeout_slice"] is False
    assert phase_g["g2_current_plan"]["stages"] == {
        "G2_1_command_registry_and_golden_contract": "COMPLETE",
        "G2_2_registration_shell_and_shared_parameters": "IN_PROGRESS",
        "G2_3_data_operations_reporting_groups": "NOT_STARTED",
        "G2_4_research_shadow_portfolio_groups": "NOT_STARTED",
        "G2_5_freeze_deprecation_and_closeout": "NOT_STARTED",
    }
    assert phase_g["g2_current_plan"]["g2_1_contract"]["leaf_command_count"] == 993
    assert phase_g["g2_current_plan"]["g2_1_contract"]["duplicate_path_count"] == 0
    assert phase_g["g2_current_plan"]["g2_1_contract"]["callback_location_in_contract"] is False
    assert phase_g["g2_current_plan"]["g2_1_contract"]["architecture_fitness"]["passed"] == 171
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
        if source.get("historical_phase_a_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004D", "ARCH-004F1"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
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
        if source.get("historical_phase_b_hash"):
            assert source["superseded_by_phase"] == "ARCH-004C"
            assert source["current_hash_tracked_in"] == "phase_c_platform_contracts.sources"
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_c = baseline["phase_c_platform_contracts"]
    assert phase_c["status"] == "COMPLETE_PHASE_D_READY"
    assert phase_c["contract_schemas"] == [
        "artifact_envelope.v1",
        "data_quality_evidence.v1",
        "workflow_spec.v1",
        "run_ledger.v1",
        "report_spec.v1",
    ]
    assert phase_c["direct_writer_ratchet"] == {
        "baseline_path": "inputs/architecture/arch_004c_direct_writer_baseline.yaml",
        "baseline_call_count": 894,
        "current_call_count": 893,
        "violation_count": 0,
    }
    assert set(phase_c["parity"].values()) == {"PASS"}
    assert phase_c["validation"]["focused"]["passed"] == 120
    assert phase_c["validation"]["contract_validation"]["passed"] == 197
    assert phase_c["validation"]["full_parallel"]["passed"] == 5404
    assert phase_c["validation"]["full_parallel"]["failed"] == 0
    for source in phase_c["sources"]:
        if source.get("historical_phase_c_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004F3", "ARCH-004G1"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_d = baseline["phase_d_reference_vertical_slice"]
    assert phase_d["status"] == "COMPLETE_PHASE_E_READY"
    assert phase_d["reference_slice"] == "growth_tilt_candidate_family_closure"
    assert set(phase_d["parity"].values()) == {"PASS"}
    assert phase_d["additive_sidecars"] == {
        "artifact_envelope": "artifact_envelope.v1",
        "run_ledger": "run_ledger.v1",
        "data_quality_required": False,
        "data_quality_pass_fabricated": False,
    }
    assert phase_d["architecture"]["second_same_plugin_spec_without_python_module"] == "PASS"
    assert phase_d["validation"]["focused"]["passed"] == 77
    assert phase_d["validation"]["contract_validation"]["passed"] == 197
    assert phase_d["validation"]["full_parallel"]["passed"] == 5411
    assert phase_d["validation"]["full_parallel"]["failed"] == 0
    for source in phase_d["sources"]:
        if source.get("historical_phase_d_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004E", "ARCH-004F2_RUNTIME"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_e = baseline["phase_e_devex_ownership_generated_indexes"]
    assert phase_e["status"] == "COMPLETE_PHASE_F_READY"
    assert phase_e["repository_inventory"] == {
        "python_module_count_including_ignored": 777,
        "python_test_and_support_file_count": 1107,
    }
    assert phase_e["ownership"]["owner_roles"] == [
        "code_owner",
        "policy_owner",
        "data_owner",
        "artifact_owner",
        "runtime_owner",
    ]
    assert phase_e["ownership"]["module_orphan_count"] == 0
    assert phase_e["ownership"]["module_specific_overlap_count"] == 0
    assert phase_e["ownership"]["test_orphan_count"] == 0
    assert phase_e["ownership"]["test_specific_overlap_count"] == 0
    assert phase_e["aggregate_shadow"] == {
        "target_count": 3,
        "fragment_count": 4,
        "existing_source_of_truth_changed": False,
        "deterministic": "PASS",
    }
    assert phase_e["impact_selection"]["replaces_full_validation"] is False
    assert phase_e["architecture_fitness"] == {
        "status": "PASS",
        "direct_writer_baseline": 894,
        "direct_writer_current": 893,
        "violation_count": 0,
    }
    assert phase_e["validation"]["architecture_tier"]["passed"] == 78
    assert phase_e["validation"]["contract_validation"]["passed"] == 197
    assert phase_e["validation"]["full_parallel"]["passed"] == 5420
    assert phase_e["validation"]["full_parallel"]["failed"] == 0
    for source in phase_e["sources"]:
        if source.get("historical_phase_e_hash"):
            assert source["superseded_by_phase"] in {
                "ARCH-004F2",
                "ARCH-004F2_RUNTIME",
                "ARCH-004G",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_f2 = baseline["phase_f2_research_lifecycle_and_execution_chain"]
    assert phase_f2["status"] == "BASELINE_DONE_RUNTIME_MIGRATION_PENDING"
    assert phase_f2["execution_chain"]["state_classes"] == [
        "CANONICAL",
        "REFERENCE",
        "LEGACY",
        "BLOCKED",
        "PLANNED",
    ]
    assert str(phase_f2["execution_chain"]["market_regime_start"]) == "2022-12-01"
    assert str(phase_f2["execution_chain"]["primary_research_window_start"]) == "2021-02-22"
    assert phase_f2["execution_chain"]["periodic_review_auto_tuning_allowed"] is False
    assert phase_f2["repository_inventory"]["python_test_and_support_file_count"] == 1108
    assert phase_f2["validation"]["focused_document_contract"]["passed"] == 23
    assert phase_f2["validation"]["architecture_fitness"]["passed"] == 80
    assert phase_f2["validation"]["contract_validation"]["passed"] == 197
    for source in phase_f2["sources"]:
        if source.get("historical_phase_f2_documentation_hash"):
            assert source["superseded_by_phase"] == "ARCH-004F2_RUNTIME"
            assert source["current_hash_tracked_in"] == ("phase_f2_runtime_lifecycle.sources")
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    runtime = baseline["phase_f2_runtime_lifecycle"]
    assert runtime["status"] == "COMPLETE_F1_F3_READY"
    assert runtime["contracts"] == {
        "lifecycle_schema": "research_lifecycle.v1",
        "preregistration_schema": "research_preregistration.v1",
        "legacy_campaign_disposition": "REUSE_WITH_EXPLICIT_COMPATIBILITY_ASSESSMENT",
        "missing_binding_behavior": "BLOCKED",
        "periodic_review_auto_tuning_allowed": False,
        "lifecycle_sidecar_additive": True,
    }
    assert runtime["repository_inventory"] == {
        "python_module_count_including_ignored": 779,
        "python_test_and_support_file_count": 1109,
    }
    assert set(runtime["parity"].values()) == {"PASS"}
    assert runtime["validation"]["architecture_fitness"]["passed"] == 88
    assert runtime["validation"]["contract_validation"]["passed"] == 197
    assert runtime["validation"]["full_parallel"]["passed"] == 5430
    assert runtime["validation"]["full_parallel"]["failed"] == 0
    for source in runtime["sources"]:
        if source.get("historical_phase_f2_runtime_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004F1", "ARCH-004G1.3D"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_f1 = baseline["phase_f1_operations_control_plane"]
    assert phase_f1["status"] == "COMPLETE_F3_READY"
    assert phase_f1["contracts"]["shadow_execution_enabled"] is False
    assert phase_f1["contracts"]["additive_shadow_artifact_emission"] is True
    assert phase_f1["contracts"]["execution_state_schema"] == "operations_execution_state.v1"
    assert phase_f1["contracts"]["idempotent_only_resume"] is True
    assert phase_f1["contracts"]["legacy_daily_executor_cut_in_enabled"] is True
    assert phase_f1["contracts"]["execution_ledger_schema"] == "run_ledger.v1"
    assert phase_f1["contracts"]["non_daily_automatic_dispatch_enabled"] is False
    assert phase_f1["contracts"]["non_daily_manual_dispatch_enabled"] is True
    assert phase_f1["contracts"]["periodic_plan_schema"] == "periodic_operations_plan.v1"
    assert phase_f1["scheduled_task_inventory"] == {
        "daily": 37,
        "non_daily": 41,
        "total": 78,
    }
    assert phase_f1["parity"]["trading_day_fixture_1"] == "PASS"
    assert phase_f1["parity"]["closed_market_fixture_1"] == "PASS"
    assert phase_f1["parity"]["additive_shadow_artifact_emission"] == "PASS"
    assert phase_f1["parity"]["legacy_markdown_bytes"] == "PASS"
    assert phase_f1["parity"]["concurrent_lock"] == "BLOCKED"
    assert phase_f1["parity"]["stale_lock_recovery"] == "EXPIRED_ONLY"
    assert phase_f1["parity"]["duplicate_completed_trigger"] == "ALREADY_COMPLETE"
    assert phase_f1["parity"]["non_idempotent_partial_resume"] == "BLOCKED"
    assert phase_f1["parity"]["atomic_state_write"] == "PASS"
    assert phase_f1["parity"]["periodic_task_plan_count"] == 41
    assert phase_f1["parity"]["periodic_automatic_command_dispatch"] is False
    for source in phase_f1["sources"]:
        if source.get("historical_phase_f1_hash"):
            assert source["superseded_by_phase"] == "ARCH-004F3"
            assert source["current_hash_tracked_in"] == (
                "phase_f3_reporting_architecture.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_f3 = baseline["phase_f3_reporting_architecture"]
    assert phase_f3["status"] == "COMPLETE_G_READY"
    assert phase_f3["contracts"]["owner_daily_core_section_count"] == 10
    assert phase_f3["contracts"]["owner_queue_requires_due_and_actionable"] is True
    assert phase_f3["contracts"]["reporting_layer_recompute_allowed"] is False
    assert phase_f3["contracts"]["research_auto_tune_allowed"] is False
    assert phase_f3["contracts"]["proposal_may_equal_adoption"] is False
    assert phase_f3["contracts"]["reader_brief_native_cut_in_enabled"] is False
    assert phase_f3["parity"]["report_registry_coverage_count"] == 1358
    assert phase_f3["parity"]["report_registry_silent_drop_count"] == 0
    assert phase_f3["repository_inventory"] == {
        "python_module_count_including_ignored": 793,
        "python_test_and_support_file_count": 1111,
        "aggregate_fragment_count": 13,
        "report_fragment_count": 4,
    }
    assert phase_f3["validation"]["full_parallel"]["passed"] == 5494
    assert phase_f3["validation"]["full_parallel"]["failed"] == 0
    for source in phase_f3["sources"]:
        if source.get("historical_phase_f3_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G"
            assert source["current_hash_tracked_in"] == (
                "phase_g0_deprecation_inventory_and_policy.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g0 = baseline["phase_g0_deprecation_inventory_and_policy"]
    assert phase_g0["status"] == "COMPLETE_G1_IN_PROGRESS"
    assert phase_g0["contracts"] == {
        "deprecation_record_schema": "deprecation_record.v1",
        "lifecycle": ["EXPERIMENTAL", "ACTIVE", "DEPRECATED", "FROZEN", "REMOVED"],
        "required_removal_gate_count": 12,
        "permanent_dual_track_allowed": False,
        "runtime_removal_allowed_in_g0": False,
        "unknown_reachability_is_removal_ready": False,
        "artifact_retention_separate_from_code_removal": True,
    }
    assert phase_g0["target_inventory"] == {
        "target_count": 9,
        "active_count": 6,
        "deprecated_count": 3,
        "removal_ready_count": 0,
        "runtime_removal_performed": False,
    }
    assert phase_g0["validation"]["architecture_fitness"]["passed"] == 156
    assert phase_g0["validation"]["contract_validation"]["passed"] == 203
    for source in phase_g0["sources"]:
        if source.get("historical_phase_g0_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G1", "ARCH-004G2.1"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g1 = baseline["phase_g1_shared_writer_migration"]
    assert phase_g1["status"] == "FIRST_FAMILY_COMPLETE_G1_CONTINUES"
    assert phase_g1["family"] == {
        "canonical_json_writer": "write_json_atomic_without_trailing_newline",
        "canonical_text_writer": "write_text_atomic",
        "migrated_module_count": 3,
        "removed_private_wrapper_count": 6,
        "private_wrapper_remaining_count": 0,
        "internal_callers_use_canonical_writer": True,
        "direct_writer_before": 893,
        "direct_writer_after": 887,
        "direct_writer_reduction": 6,
    }
    assert phase_g1["parity"]["artifact_path_schema_status"] == "PASS"
    assert phase_g1["parity"]["data_quality_behavior_changed"] is False
    assert phase_g1["parity"]["production_effect"] == "none"
    assert phase_g1["validation"]["focused"]["passed"] == 29
    assert phase_g1["validation"]["architecture_fitness"]["passed"] == 159
    assert phase_g1["validation"]["architecture_fitness"][
        "current_direct_writer_calls"
    ] == 887
    for source in phase_g1["sources"]:
        if source.get("historical_phase_g1_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G1.3A", "ARCH-004G1.3C"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g1_3a = baseline["phase_g1_3a_trading_engine_summary_writer_migration"]
    assert phase_g1_3a["status"] == "SECOND_FAMILY_COMPLETE_G1_CONTINUES"
    assert phase_g1_3a["family"] == {
        "canonical_json_writer": "write_json_atomic",
        "canonical_text_writer": "write_text_atomic",
        "migrated_module_count": 5,
        "removed_private_writer_count": 10,
        "private_writer_remaining_count": 0,
        "direct_writer_before": 887,
        "direct_writer_after": 877,
        "direct_writer_reduction": 10,
    }
    assert phase_g1_3a["parity"]["sort_keys"] is False
    assert phase_g1_3a["parity"]["trailing_newline"] is True
    assert phase_g1_3a["parity"]["oserror_boundary"] == "PASS"
    assert phase_g1_3a["parity"]["investment_semantics_changed"] is False
    assert phase_g1_3a["validation"]["focused"] == {"status": "PASS", "passed": 95}
    assert phase_g1_3a["validation"]["architecture_fitness"]["passed"] == 161
    for source in phase_g1_3a["sources"]:
        if source.get("historical_phase_g1_3a_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G1.3B", "ARCH-004G1.3C"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g1_3b = baseline["phase_g1_3b_notification_retry_writer_migration"]
    assert phase_g1_3b["status"] == "THIRD_FAMILY_COMPLETE_G1_CONTINUES"
    assert phase_g1_3b["family"] == {
        "canonical_json_writer": "write_json_atomic",
        "canonical_text_writer": "write_text_atomic",
        "migrated_module_count": 8,
        "removed_private_writer_count": 16,
        "private_writer_remaining_count": 0,
        "direct_writer_before": 877,
        "direct_writer_after": 861,
        "direct_writer_reduction": 16,
    }
    assert phase_g1_3b["parity"]["sort_keys"] is False
    assert phase_g1_3b["parity"]["trailing_newline"] is True
    assert phase_g1_3b["parity"]["artifact_path_schema_status"] == "PASS"
    assert phase_g1_3b["parity"]["workflow_decisions_changed"] is False
    assert phase_g1_3b["validation"]["focused"] == {"status": "PASS", "passed": 139}
    assert phase_g1_3b["validation"]["architecture_fitness"]["passed"] == 162
    for source in phase_g1_3b["sources"]:
        if source.get("historical_phase_g1_3b_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G1.3C", "ARCH-004G1.3D"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g1_3c = baseline["phase_g1_3c_streaming_checksum_helper_migration"]
    assert phase_g1_3c["status"] == "FOURTH_FAMILY_COMPLETE_G1_CONTINUES"
    assert phase_g1_3c["family"] == {
        "canonical_checksum_helper": "sha256_path",
        "default_chunk_size_bytes": 1048576,
        "migrated_module_count": 8,
        "migrated_caller_count": 13,
        "removed_private_checksum_helper_count": 8,
        "private_checksum_helper_remaining_count": 0,
        "direct_writer_before": 861,
        "direct_writer_after": 861,
    }
    assert phase_g1_3c["parity"]["default_cross_chunk_digest"] == "PASS"
    assert phase_g1_3c["parity"]["missing_path_oserror"] == "PASS"
    assert phase_g1_3c["parity"]["workflow_decisions_changed"] is False
    assert phase_g1_3c["validation"]["focused"] == {"status": "PASS", "passed": 155}
    assert phase_g1_3c["validation"]["architecture_fitness"]["passed"] == 164
    for source in phase_g1_3c["sources"]:
        if source.get("historical_phase_g1_3c_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G1.3D"
            assert source["current_hash_tracked_in"] == (
                "phase_g1_3d_pit_replay_runtime_metadata_migration.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g1_3d = baseline["phase_g1_3d_pit_replay_runtime_metadata_migration"]
    assert phase_g1_3d["status"] == "FIFTH_FAMILY_COMPLETE_G1_CONTINUES"
    assert phase_g1_3d["family"] == {
        "canonical_helper": "with_pit_replay_observe_only_runtime_metadata",
        "canonical_safety_constant": "PIT_REPLAY_OBSERVE_ONLY_SAFETY_FALSE_FIELDS",
        "inventory_file_count": 42,
        "inventory_ast_field_group_count": 14,
        "migrated_module_count": 10,
        "migrated_caller_count": 10,
        "removed_private_metadata_helper_count": 10,
        "private_metadata_helper_remaining_count": 0,
        "safety_false_field_count": 39,
    }
    assert phase_g1_3d["parity"]["field_order"] == "PASS"
    assert phase_g1_3d["parity"]["module_safety_constant_alias"] == "PASS"
    assert phase_g1_3d["parity"]["generic_extra_fields_allowed"] is False
    assert phase_g1_3d["validation"]["focused"] == {"status": "PASS", "passed": 182}
    assert phase_g1_3d["validation"]["architecture_fitness"]["passed"] == 166
    for source in phase_g1_3d["sources"]:
        if source.get("historical_phase_g1_3d_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G1.3E"
            assert source["current_hash_tracked_in"] == (
                "phase_g1_3e_growth_tilt_data_quality_gate_migration.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g1_3e = baseline["phase_g1_3e_growth_tilt_data_quality_gate_migration"]
    assert phase_g1_3e["status"] == "SIXTH_FAMILY_COMPLETE_G1_COMPLETE_G2_IN_PROGRESS"
    assert phase_g1_3e["family"] == {
        "canonical_helper": "run_growth_tilt_data_quality_gate",
        "inventory_helper_count": 106,
        "inventory_group_count": 51,
        "migrated_module_count": 15,
        "migrated_caller_count": 15,
        "removed_private_gate_helper_count": 15,
        "removed_private_secondary_helper_count": 15,
        "private_gate_helper_remaining_count": 0,
    }
    assert phase_g1_3e["parity"]["direct_validate_data_cache_call"] == "PASS"
    assert phase_g1_3e["parity"]["marketstack_requirement"] == "PASS"
    assert phase_g1_3e["parity"]["exception_downgrade_allowed"] is False
    assert phase_g1_3e["parity"]["fabricated_pass_allowed"] is False
    assert phase_g1_3e["validation"]["focused"] == {"status": "PASS", "passed": 242}
    assert phase_g1_3e["validation"]["architecture_fitness"]["passed"] == 168
    for source in phase_g1_3e["sources"]:
        if source.get("historical_phase_g1_3e_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.1"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_1_etf_cli_contract_baseline.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_1 = baseline["phase_g2_1_etf_cli_contract_baseline"]
    assert phase_g2_1["status"] == "COMPLETE_G2_2_IN_PROGRESS"
    assert phase_g2_1["contract"] == {
        "schema_version": "arch_004g2_cli_contract.v1",
        "root_command_count": 41,
        "group_count": 291,
        "leaf_command_count": 993,
        "registered_leaf_count": 993,
        "unique_path_count": 1284,
        "duplicate_path_count": 0,
        "tree_sha256": "afa0760c82cf347bb135ecb12ae133bc16238fb53e28b7a0cf3c699f6ba1cec2",
        "callback_location_in_contract": False,
        "runtime_behavior_changed": False,
        "production_effect": "none",
    }
    assert phase_g2_1["validation"]["focused"] == {"status": "PASS", "passed": 3}
    assert phase_g2_1["validation"]["architecture_fitness"]["passed"] == 171
    for source in phase_g2_1["sources"]:
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]


def test_arch_004c_dependency_policy_uses_count_ratchet_without_waiver() -> None:
    policy = safe_load_yaml_path(DEPENDENCY_POLICY_PATH)
    direct = policy["direct_writer_ratchet"]
    baseline = safe_load_yaml_path(DIRECT_WRITER_BASELINE_PATH)

    assert policy["status"] == "active_phase_c"
    assert policy["canonical_writer_path"] == ("src/ai_trading_system/platform/artifacts/writer.py")
    assert direct["new_calls_allowed"] is False
    assert direct["baseline_status"] == "FROZEN_ARCH_004C_C2"
    assert baseline["status"] == "FROZEN_ARCH_004C_C2"
    assert baseline["direct_writer_call_count"] == 894
    assert baseline["entries"]


def test_arch_004_worktree_attribution_excludes_concurrent_user_changes() -> None:
    attribution = safe_load_yaml_path(ATTRIBUTION_PATH)

    assert attribution["status"] == (
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_1_COMPLETE_G2_2_IN_PROGRESS"
    )
    excluded = set(attribution["excluded_user_or_other_task_paths"])
    assert excluded == {
        "docs/research/growth_tilt_owner_decision_resolution.md",
        "docs/research/indicator_family_only_model_review.md",
        "docs/research/layer1_selector_pause_or_continue_owner_pack.md",
    }
    assert attribution["staging_rule"]["exclude_user_or_other_task_paths"] is True
    assert attribution["safety_boundary"]["user_changes_preserved"] is True
