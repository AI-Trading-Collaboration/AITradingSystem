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
    assert phase_f3["legacy_unclassified_disposition"] == ("AUDIT_INDEX_LIMITED_UNCLASSIFIED")
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
    assert phase_g["stages"]["G0_inventory_deprecation_policy_and_removal_gate"] == ("COMPLETE")
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
    assert (
        phase_g["g1_first_family_validation"]["architecture_fitness"]["current_direct_writer_calls"]
        == 887
    )
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
    assert (
        phase_g["g1_current_evidence"]["canonical_runtime_metadata_helper"]
        == "with_pit_replay_observe_only_runtime_metadata"
    )
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
        "G2_2_registration_shell_and_shared_parameters": "COMPLETE",
        "G2_3_data_operations_reporting_groups": "COMPLETE",
        "G2_4_research_shadow_portfolio_groups": "IN_PROGRESS",
        "G2_5_freeze_deprecation_and_closeout": "NOT_STARTED",
    }
    assert phase_g["g2_current_plan"]["g2_1_contract"]["leaf_command_count"] == 993
    assert phase_g["g2_current_plan"]["g2_1_contract"]["duplicate_path_count"] == 0
    assert phase_g["g2_current_plan"]["g2_1_contract"]["callback_location_in_contract"] is False
    assert phase_g["g2_current_plan"]["g2_1_contract"]["architecture_fitness"]["passed"] == 171
    g2_2 = phase_g["g2_current_plan"]["g2_2_registration_shell"]
    assert g2_2["typer_apps_moved"] == 291
    assert g2_2["add_typer_relationships_moved"] == 290
    assert g2_2["legacy_typer_app_definitions_remaining"] == 0
    assert g2_2["legacy_add_typer_relationships_remaining"] == 0
    assert g2_2["legacy_root_line_reduction"] == 1559
    assert g2_2["top_level_functions_unchanged"] == 1049
    assert g2_2["command_decorators_unchanged"] == 993
    assert g2_2["callback_functions_moved"] == 0
    assert g2_2["focused_validation"] == {"status": "PASS", "passed": 341, "file_count": 25}
    assert g2_2["contract_characterization"] == {"status": "PASS", "passed": 6}
    assert g2_2["architecture_fitness"]["passed"] == 174
    assert g2_2["runtime_behavior_changed"] is False
    g2_3_first = phase_g["g2_current_plan"]["g2_3_first_slice"]
    assert g2_3_first["callback_count"] == 3
    assert g2_3_first["legacy_callback_definitions_remaining"] == 0
    assert g2_3_first["legacy_helper_definitions_remaining"] == 0
    assert g2_3_first["compatibility_aliases_using_canonical_callbacks"] is True
    assert g2_3_first["legacy_root_top_level_functions_after"] == 1043
    assert g2_3_first["legacy_root_command_decorators_after"] == 990
    assert g2_3_first["focused_validation"] == {"status": "PASS", "passed": 72}
    assert g2_3_first["architecture_fitness"]["passed"] == 175
    g2_3_second = phase_g["g2_current_plan"]["g2_3_second_slice"]
    assert g2_3_second["callback_count"] == 3
    assert g2_3_second["legacy_callback_definitions_remaining"] == 0
    assert g2_3_second["direct_dispatch_using_canonical_callbacks"] is True
    assert g2_3_second["shared_helper_added"] is False
    assert g2_3_second["legacy_root_top_level_functions_after"] == 1040
    assert g2_3_second["legacy_root_command_decorators_after"] == 987
    assert g2_3_second["focused_validation"] == {"status": "PASS", "passed": 44}
    assert g2_3_second["architecture_fitness"]["passed"] == 176
    g2_3_third = phase_g["g2_current_plan"]["g2_3_third_slice"]
    assert g2_3_third["callback_count"] == 3
    assert g2_3_third["legacy_callback_definitions_remaining"] == 0
    assert g2_3_third["legacy_parser_definitions_remaining"] == 0
    assert g2_3_third["legacy_directory_constant_definitions_remaining"] == 0
    assert g2_3_third["direct_dispatch_using_canonical_callbacks"] is True
    assert g2_3_third["legacy_root_top_level_functions_after"] == 1036
    assert g2_3_third["legacy_root_command_decorators_after"] == 984
    assert g2_3_third["focused_validation"] == {"status": "PASS", "passed": 111}
    assert g2_3_third["architecture_fitness"]["passed"] == 177
    g2_3_fourth = phase_g["g2_current_plan"]["g2_3_fourth_slice"]
    assert g2_3_fourth["callback_count"] == 3
    assert g2_3_fourth["legacy_callback_definitions_remaining"] == 0
    assert g2_3_fourth["legacy_strategy_evidence_imports_remaining"] == 0
    assert g2_3_fourth["direct_dispatch_using_canonical_callbacks"] is True
    assert g2_3_fourth["legacy_root_top_level_functions_after"] == 1033
    assert g2_3_fourth["legacy_root_command_decorators_after"] == 981
    assert g2_3_fourth["focused_validation"] == {"status": "PASS", "passed": 44}
    assert g2_3_fourth["architecture_fitness"]["passed"] == 178
    g2_3_fifth = phase_g["g2_current_plan"]["g2_3_fifth_slice"]
    assert g2_3_fifth["callback_count"] == 4
    assert g2_3_fifth["legacy_callback_definitions_remaining"] == 0
    assert g2_3_fifth["legacy_helper_definitions_remaining"] == 0
    assert g2_3_fifth["legacy_weekly_review_imports_remaining"] == 0
    assert g2_3_fifth["legacy_callers_using_canonical_date_helper"] is True
    assert g2_3_fifth["legacy_root_top_level_functions_after"] == 1027
    assert g2_3_fifth["legacy_root_command_decorators_after"] == 977
    assert g2_3_fifth["focused_validation"] == {"status": "PASS", "passed": 84}
    assert g2_3_fifth["architecture_fitness"]["passed"] == 179
    g2_3_sixth = phase_g["g2_current_plan"]["g2_3_sixth_slice"]
    assert g2_3_sixth["callback_count"] == 4
    assert g2_3_sixth["legacy_callback_definitions_remaining"] == 0
    assert g2_3_sixth["legacy_helper_definitions_remaining"] == 0
    assert g2_3_sixth["legacy_parameter_review_imports_remaining"] == 0
    assert g2_3_sixth["canonical_date_helper_reused"] == "weekly_review_date"
    assert g2_3_sixth["legacy_root_top_level_functions_after"] == 1022
    assert g2_3_sixth["legacy_root_command_decorators_after"] == 973
    assert g2_3_sixth["focused_validation"] == {"status": "PASS", "passed": 65}
    assert g2_3_sixth["architecture_fitness"]["passed"] == 180
    g2_3_seventh = phase_g["g2_current_plan"]["g2_3_seventh_slice"]
    assert g2_3_seventh["callback_count"] == 3
    assert g2_3_seventh["legacy_callback_definitions_remaining"] == 0
    assert g2_3_seventh["legacy_helper_definitions_remaining"] == 0
    assert g2_3_seventh["legacy_satellite_attribution_imports_remaining"] == 0
    assert str(g2_3_seventh["default_ai_regime_start_unchanged"]) == "2022-12-01"
    assert g2_3_seventh["invalid_price_fixture_fail_closed"] is True
    assert g2_3_seventh["legacy_root_top_level_functions_after"] == 1017
    assert g2_3_seventh["legacy_root_command_decorators_after"] == 970
    assert g2_3_seventh["focused_validation"] == {"status": "PASS", "passed": 78}
    assert g2_3_seventh["architecture_fitness"]["passed"] == 181
    g2_3_eighth = phase_g["g2_current_plan"]["g2_3_eighth_slice"]
    assert g2_3_eighth["callback_count"] == 3
    assert g2_3_eighth["legacy_callback_definitions_remaining"] == 0
    assert g2_3_eighth["legacy_dq_helper_definitions_remaining"] == 0
    assert g2_3_eighth["legacy_trend_calibration_imports_remaining"] == 0
    assert g2_3_eighth["dq_gate_precedes_price_and_feature_build"] is True
    assert g2_3_eighth["dq_failure_fixture_fail_closed"] is True
    assert g2_3_eighth["legacy_root_top_level_functions_after"] == 1010
    assert g2_3_eighth["legacy_root_command_decorators_after"] == 967
    assert g2_3_eighth["focused_validation"] == {"status": "PASS", "passed": 54}
    assert g2_3_eighth["architecture_fitness"]["passed"] == 182
    g2_3_closeout = phase_g["g2_current_plan"]["g2_3_closeout"]
    assert g2_3_closeout["status"] == "COMPLETE"
    assert g2_3_closeout["slice_count"] == 8
    assert g2_3_closeout["canonical_module_count"] == 9
    assert g2_3_closeout["migrated_callback_count"] == 26
    assert g2_3_closeout["migrated_helper_count"] == 13
    assert g2_3_closeout["legacy_selected_callback_definitions_remaining"] == 0
    assert g2_3_closeout["legacy_selected_helper_definitions_remaining"] == 0
    assert g2_3_closeout["legacy_selected_domain_imports_remaining"] == 0
    assert g2_3_closeout["legacy_root_line_reduction"] == 1605
    assert g2_3_closeout["legacy_root_function_reduction"] == 39
    assert g2_3_closeout["legacy_root_command_decorator_reduction"] == 26
    assert g2_3_closeout["direct_writer_calls_after"] == 860
    assert g2_3_closeout["focused_closeout_validation"] == {"status": "PASS", "passed": 15}
    assert g2_3_closeout["architecture_fitness"]["passed"] == 183
    g2_4 = phase_g["g2_current_plan"]["g2_4_current_plan"]
    assert g2_4["status"] == "IN_PROGRESS"
    assert g2_4["first_slice"] == "baseline_review"
    assert g2_4["implementation_started_in_g2_3_closeout"] is False
    assert g2_4["callback_count"] == 7
    assert g2_4["owner_decision_semantics_sensitive"] is True
    assert g2_4["production_runtime_state_mutation_allowed"] is False
    assert g2_4["governance_journal_write_allowed"] is True
    assert g2_4["journal_link_optional"] is True
    g2_4_first = phase_g["g2_current_plan"]["g2_4_first_slice"]
    assert g2_4_first["status"] == "COMPLETE"
    assert g2_4_first["callback_count"] == 7
    assert g2_4_first["legacy_callback_definitions_remaining"] == 0
    assert g2_4_first["legacy_helper_definitions_remaining"] == 0
    assert g2_4_first["legacy_baseline_review_imports_remaining"] == 0
    assert g2_4_first["governance_journal_write_allowed"] is True
    assert g2_4_first["production_runtime_state_mutation_allowed"] is False
    assert g2_4_first["proposal_is_draft_only"] is True
    assert g2_4_first["direct_writer_calls_after"] == 858
    assert g2_4_first["legacy_root_lines_after"] == 33950
    assert g2_4_first["legacy_root_top_level_functions_after"] == 1002
    assert g2_4_first["legacy_root_command_decorators_after"] == 960
    assert g2_4_first["focused_validation"] == {"status": "PASS", "passed": 36}
    assert g2_4_first["architecture_fitness"]["passed"] == 184
    g2_4_second = phase_g["g2_current_plan"]["g2_4_second_slice"]
    assert g2_4_second["status"] == "COMPLETE"
    assert g2_4_second["callback_count"] == 4
    assert g2_4_second["legacy_callback_definitions_remaining"] == 0
    assert g2_4_second["legacy_shadow_ready_review_imports_remaining"] == 0
    assert g2_4_second["candidate_governance_artifact_write_allowed"] is True
    assert g2_4_second["decision_journal_write_allowed"] is False
    assert g2_4_second["approved_enrollment_artifact_write_allowed"] is True
    assert g2_4_second["automatic_paper_shadow_execution_allowed"] is False
    assert g2_4_second["runtime_registry_mutation_allowed"] is False
    assert g2_4_second["direct_writer_calls_after"] == 858
    assert g2_4_second["legacy_root_lines_after"] == 33656
    assert g2_4_second["legacy_root_top_level_functions_after"] == 998
    assert g2_4_second["legacy_root_command_decorators_after"] == 956
    assert g2_4_second["focused_validation"] == {"status": "PASS", "passed": 21}
    assert g2_4_second["architecture_fitness"]["passed"] == 185
    g2_4_third = phase_g["g2_current_plan"]["g2_4_third_slice"]
    assert g2_4_third["status"] == "COMPLETE"
    assert g2_4_third["callback_count"] == 3
    assert g2_4_third["legacy_callback_definitions_remaining"] == 0
    assert g2_4_third["legacy_helper_definitions_remaining"] == 0
    assert g2_4_third["candidate_decision_artifact_write_allowed"] is True
    assert g2_4_third["candidate_policy_registry_artifact_write_allowed"] is True
    assert g2_4_third["runtime_registry_mutation_allowed"] is False
    assert g2_4_third["official_target_weights_mutation_allowed"] is False
    assert g2_4_third["production_rebalance_allowed"] is False
    assert g2_4_third["direct_writer_calls_after"] == 858
    assert g2_4_third["legacy_root_lines_after"] == 33405
    assert g2_4_third["legacy_root_top_level_functions_after"] == 993
    assert g2_4_third["legacy_root_command_decorators_after"] == 953
    assert g2_4_third["focused_validation"] == {"status": "PASS", "passed": 21}
    assert g2_4_third["architecture_fitness"]["passed"] == 186
    g2_4_fourth = phase_g["g2_current_plan"]["g2_4_fourth_slice"]
    assert g2_4_fourth["status"] == "COMPLETE"
    assert g2_4_fourth["callback_count"] == 3
    assert g2_4_fourth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_fourth["research_cache_write_allowed"] is True
    assert g2_4_fourth["candidate_artifact_write_allowed"] is True
    assert g2_4_fourth["automatic_candidate_promotion_allowed"] is False
    assert g2_4_fourth["auto_enrollment_without_owner_approval_allowed"] is False
    assert g2_4_fourth["official_target_weights_mutation_allowed"] is False
    assert g2_4_fourth["validation_uses_canonical_cli_owner"] is True
    assert g2_4_fourth["direct_writer_calls_after"] == 858
    assert g2_4_fourth["legacy_root_lines_after"] == 33199
    assert g2_4_fourth["legacy_root_top_level_functions_after"] == 990
    assert g2_4_fourth["legacy_root_command_decorators_after"] == 950
    assert g2_4_fourth["focused_validation"] == {"status": "PASS", "passed": 24}
    assert g2_4_fourth["architecture_fitness"]["passed"] == 187
    g2_4_fifth = phase_g["g2_current_plan"]["g2_4_fifth_slice"]
    assert g2_4_fifth["status"] == "COMPLETE"
    assert g2_4_fifth["callback_count"] == 2
    assert g2_4_fifth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_fifth["cached_dq_gate_precedes_standard_price_validation"] is True
    assert g2_4_fifth["standard_price_validation_precedes_robustness"] is True
    assert g2_4_fifth["dq_failure_fail_closed"] is True
    assert g2_4_fifth["latest_mode_read_only"] is True
    assert g2_4_fifth["shadow_enrollment_allowed"] is False
    assert g2_4_fifth["direct_writer_calls_after"] == 858
    assert g2_4_fifth["legacy_root_lines_after"] == 32979
    assert g2_4_fifth["legacy_root_top_level_functions_after"] == 988
    assert g2_4_fifth["legacy_root_command_decorators_after"] == 948
    assert g2_4_fifth["focused_validation"] == {"status": "PASS", "passed": 24}
    assert g2_4_fifth["architecture_fitness"]["passed"] == 188
    g2_4_sixth = phase_g["g2_current_plan"]["g2_4_sixth_slice"]
    assert g2_4_sixth["status"] == "COMPLETE"
    assert g2_4_sixth["callback_count"] == 3
    assert g2_4_sixth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_sixth["cached_dq_gate_precedes_standard_price_validation"] is True
    assert g2_4_sixth["standard_price_validation_precedes_rescue_comparison"] is True
    assert g2_4_sixth["dq_failure_fail_closed"] is True
    assert g2_4_sixth["bounded_rescue_candidate_artifact_write_allowed"] is True
    assert g2_4_sixth["automatic_candidate_enrollment_allowed"] is False
    assert g2_4_sixth["owner_approval_executed"] is False
    assert g2_4_sixth["automatic_candidate_promotion_allowed"] is False
    assert g2_4_sixth["official_target_weights_mutation_allowed"] is False
    assert g2_4_sixth["broker_action_allowed"] is False
    assert g2_4_sixth["validation_uses_canonical_cli_owner"] is True
    assert g2_4_sixth["direct_writer_calls_after"] == 858
    assert g2_4_sixth["legacy_root_lines_after"] == 32713
    assert g2_4_sixth["legacy_root_top_level_functions_after"] == 985
    assert g2_4_sixth["legacy_root_command_decorators_after"] == 945
    assert g2_4_sixth["focused_validation"] == {"status": "PASS", "passed": 25}
    assert g2_4_sixth["architecture_fitness"]["passed"] == 189
    g2_4_seventh = phase_g["g2_current_plan"]["g2_4_seventh_slice"]
    assert g2_4_seventh["status"] == "COMPLETE"
    assert g2_4_seventh["callback_count"] == 3
    assert g2_4_seventh["legacy_callback_definitions_remaining"] == 0
    assert g2_4_seventh["existing_rescue_and_robustness_artifacts_read_only"] is True
    assert g2_4_seventh["market_backtest_reexecution_allowed"] is False
    assert g2_4_seventh["mandatory_source_missing_fail_closed"] is True
    assert g2_4_seventh["optional_shadow_missing_is_warning"] is True
    assert g2_4_seventh["latest_report_mode_read_only"] is True
    assert g2_4_seventh["review_artifact_write_allowed"] is True
    assert g2_4_seventh["owner_approval_executed"] is False
    assert g2_4_seventh["shadow_enrollment_allowed"] is False
    assert g2_4_seventh["automatic_candidate_promotion_allowed"] is False
    assert g2_4_seventh["official_target_weights_mutation_allowed"] is False
    assert g2_4_seventh["validation_uses_canonical_cli_owner"] is True
    assert g2_4_seventh["direct_writer_calls_after"] == 858
    assert g2_4_seventh["legacy_root_lines_after"] == 32546
    assert g2_4_seventh["legacy_root_top_level_functions_after"] == 982
    assert g2_4_seventh["legacy_root_command_decorators_after"] == 942
    assert g2_4_seventh["focused_validation"] == {"status": "PASS", "passed": 27}
    assert g2_4_seventh["architecture_fitness"]["passed"] == 190
    g2_4_eighth = phase_g["g2_current_plan"]["g2_4_eighth_slice"]
    assert g2_4_eighth["status"] == "COMPLETE"
    assert g2_4_eighth["callback_count"] == 3
    assert g2_4_eighth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_eighth["remaining_dynamic_v3_commands_stay_legacy_owned"] is True
    assert g2_4_eighth["v0_4_review_package_read_only"] is True
    assert g2_4_eighth["base_candidate_must_match_reviewed_policy"] is True
    assert g2_4_eighth["latest_report_mode_read_only"] is True
    assert g2_4_eighth["candidate_evaluation_artifact_write_allowed"] is True
    assert g2_4_eighth["owner_approval_executed"] is False
    assert g2_4_eighth["shadow_enrollment_allowed"] is False
    assert g2_4_eighth["automatic_candidate_promotion_allowed"] is False
    assert g2_4_eighth["official_target_weights_mutation_allowed"] is False
    assert g2_4_eighth["validation_checks_canonical_and_legacy_owners"] is True
    assert g2_4_eighth["direct_writer_calls_after"] == 858
    assert g2_4_eighth["legacy_root_lines_after"] == 32389
    assert g2_4_eighth["legacy_root_top_level_functions_after"] == 979
    assert g2_4_eighth["legacy_root_command_decorators_after"] == 939
    assert g2_4_eighth["focused_validation"] == {"status": "PASS", "passed": 28}
    assert g2_4_eighth["architecture_fitness"]["passed"] == 191
    g2_4_ninth = phase_g["g2_current_plan"]["g2_4_ninth_slice"]
    assert g2_4_ninth["status"] == "COMPLETE"
    assert g2_4_ninth["callback_count"] == 3
    assert g2_4_ninth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_ninth["cached_dq_gate_precedes_standard_price_validation"] is True
    assert g2_4_ninth["standard_price_validation_precedes_pit_evaluation"] is True
    assert g2_4_ninth["dq_failure_fail_closed"] is True
    assert g2_4_ninth["pit_no_lookahead_preserved"] is True
    assert g2_4_ninth["requested_range_and_ai_regime_separate"] is True
    assert g2_4_ninth["pre_regime_primary_conclusion_allowed"] is False
    assert g2_4_ninth["latest_report_mode_read_only"] is True
    assert g2_4_ninth["promotion_gate_executes_promotion"] is False
    assert g2_4_ninth["owner_approval_executed"] is False
    assert g2_4_ninth["shadow_enrollment_allowed"] is False
    assert g2_4_ninth["official_target_weights_mutation_allowed"] is False
    assert g2_4_ninth["validation_uses_canonical_cli_owner"] is True
    assert g2_4_ninth["direct_writer_calls_after"] == 858
    assert g2_4_ninth["legacy_root_lines_after"] == 32166
    assert g2_4_ninth["legacy_root_top_level_functions_after"] == 976
    assert g2_4_ninth["legacy_root_command_decorators_after"] == 936
    assert g2_4_ninth["focused_validation"] == {"status": "PASS", "passed": 28}
    assert g2_4_ninth["architecture_fitness"]["passed"] == 192
    g2_4_tenth = phase_g["g2_current_plan"]["g2_4_tenth_slice"]
    assert g2_4_tenth["status"] == "COMPLETE"
    assert g2_4_tenth["callback_count"] == 3
    assert g2_4_tenth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_tenth["real_evaluation_lineage_loaded_before_dq"] is True
    assert g2_4_tenth["dq_as_of_inherits_explicit_or_source_end"] is True
    assert g2_4_tenth["cached_dq_gate_precedes_standard_price_validation"] is True
    assert g2_4_tenth["standard_price_validation_precedes_pit_attribution"] is True
    assert g2_4_tenth["dq_failure_fail_closed"] is True
    assert g2_4_tenth["source_artifact_mutation_allowed"] is False
    assert g2_4_tenth["requested_range_and_ai_regime_separate"] is True
    assert g2_4_tenth["latest_report_mode_read_only"] is True
    assert g2_4_tenth["review_or_recommendation_executes_promotion"] is False
    assert g2_4_tenth["owner_approval_executed"] is False
    assert g2_4_tenth["shadow_enrollment_allowed"] is False
    assert g2_4_tenth["official_target_weights_mutation_allowed"] is False
    assert g2_4_tenth["validation_uses_canonical_cli_owner"] is True
    assert g2_4_tenth["direct_writer_calls_after"] == 858
    assert g2_4_tenth["legacy_root_lines_after"] == 31876
    assert g2_4_tenth["legacy_root_top_level_functions_after"] == 973
    assert g2_4_tenth["legacy_root_command_decorators_after"] == 933
    assert g2_4_tenth["focused_validation"] == {"status": "PASS", "passed": 28}
    assert g2_4_tenth["architecture_fitness"]["passed"] == 193
    g2_4_eleventh = phase_g["g2_current_plan"]["g2_4_eleventh_slice"]
    assert g2_4_eleventh["status"] == "COMPLETE"
    assert g2_4_eleventh["callback_count"] == 2
    assert g2_4_eleventh["legacy_callback_definitions_remaining"] == 0
    assert g2_4_eleventh["reviewed_config_read_only"] is True
    assert g2_4_eleventh["schema_and_safety_validation_only"] is True
    assert g2_4_eleventh["stable_candidate_id_enumeration"] is True
    assert g2_4_eleventh["evaluator_execution_allowed"] is False
    assert g2_4_eleventh["backtest_or_pit_execution_allowed"] is False
    assert g2_4_eleventh["fresh_data_quality_gate_required"] is False
    assert g2_4_eleventh["runtime_artifact_write_allowed"] is False
    assert g2_4_eleventh["production_candidate_generated"] is False
    assert g2_4_eleventh["preview_limit_changes_candidate_universe"] is False
    assert g2_4_eleventh["runtime_commands_deferred_to_next_slice"] is True
    assert g2_4_eleventh["direct_writer_calls_after"] == 858
    assert g2_4_eleventh["legacy_root_lines_after"] == 31833
    assert g2_4_eleventh["legacy_root_top_level_functions_after"] == 971
    assert g2_4_eleventh["legacy_root_command_decorators_after"] == 931
    assert g2_4_eleventh["focused_validation"] == {"status": "PASS", "passed": 43}
    assert g2_4_eleventh["architecture_fitness"]["passed"] == 194
    g2_4_twelfth = phase_g["g2_current_plan"]["g2_4_twelfth_slice"]
    assert g2_4_twelfth["status"] == "COMPLETE"
    assert g2_4_twelfth["callback_count"] == 8
    assert g2_4_twelfth["helper_count"] == 1
    assert g2_4_twelfth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_twelfth["legacy_helper_definitions_remaining"] == 0
    assert g2_4_twelfth["profile_list_and_validate_read_only"] is True
    assert g2_4_twelfth["research_runtime_artifact_write_allowed"] is True
    assert g2_4_twelfth["real_evaluator_uses_dq_and_pit_path"] is True
    assert g2_4_twelfth["tiny_fixture_not_for_investment_decision"] is True
    assert g2_4_twelfth["resume_evaluator_mode_mutation_allowed"] is False
    assert g2_4_twelfth["resume_worker_override_recorded"] is True
    assert g2_4_twelfth["derived_leaderboard_or_report_materialization_allowed"] is True
    assert g2_4_twelfth["production_candidate_generated"] is False
    assert g2_4_twelfth["automatic_candidate_promotion_allowed"] is False
    assert g2_4_twelfth["shadow_enrollment_allowed"] is False
    assert g2_4_twelfth["official_target_weights_mutation_allowed"] is False
    assert g2_4_twelfth["direct_writer_calls_after"] == 858
    assert g2_4_twelfth["legacy_root_lines_after"] == 31548
    assert g2_4_twelfth["legacy_root_top_level_functions_after"] == 962
    assert g2_4_twelfth["legacy_root_command_decorators_after"] == 923
    assert g2_4_twelfth["focused_validation"] == {"status": "PASS", "passed": 44}
    assert g2_4_twelfth["architecture_fitness"]["passed"] == 195
    g2_4_thirteenth = phase_g["g2_current_plan"]["g2_4_thirteenth_slice"]
    assert g2_4_thirteenth["status"] == "COMPLETE"
    assert g2_4_thirteenth["callback_count"] == 3
    assert g2_4_thirteenth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_thirteenth["explicit_as_of_and_end_required"] is True
    assert g2_4_thirteenth["same_validate_data_path_used"] is True
    assert g2_4_thirteenth["failed_quality_evidence_write_allowed"] is True
    assert g2_4_thirteenth["dq_failure_may_be_reported_as_pass"] is False
    assert g2_4_thirteenth["checksum_and_provenance_artifacts_required"] is True
    assert g2_4_thirteenth["pit_coverage_and_data_gap_artifacts_required"] is True
    assert g2_4_thirteenth["latest_report_mode_read_only"] is True
    assert g2_4_thirteenth["validation_requires_dq_non_fail"] is True
    assert g2_4_thirteenth["cache_or_download_manifest_mutation_allowed"] is False
    assert g2_4_thirteenth["candidate_generation_allowed"] is False
    assert g2_4_thirteenth["backtest_or_pit_evaluation_allowed"] is False
    assert g2_4_thirteenth["direct_writer_calls_after"] == 858
    assert g2_4_thirteenth["legacy_root_lines_after"] == 31464
    assert g2_4_thirteenth["legacy_root_top_level_functions_after"] == 959
    assert g2_4_thirteenth["legacy_root_command_decorators_after"] == 920
    assert g2_4_thirteenth["focused_validation"] == {"status": "PASS", "passed": 45}
    assert g2_4_thirteenth["architecture_fitness"]["passed"] == 196
    g2_4_fourteenth = phase_g["g2_current_plan"]["g2_4_fourteenth_slice"]
    assert g2_4_fourteenth["status"] == "COMPLETE"
    assert g2_4_fourteenth["callback_count"] == 3
    assert g2_4_fourteenth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_fourteenth["inspect_and_validate_read_only"] is True
    assert g2_4_fourteenth["manifest_repair_requires_explicit_mode"] is True
    assert g2_4_fourteenth["supported_repair_mode"] == "reconstruct-from-cache"
    assert g2_4_fourteenth["repair_requires_all_cache_files"] is True
    assert g2_4_fourteenth["reconstructed_rows_use_current_file_checksums"] is True
    assert g2_4_fourteenth["reconstructed_source_label_required"] == (
        "cache_rebuild_from_existing_file"
    )
    assert g2_4_fourteenth["reconstructed_provenance_status_required"] == ("RECONSTRUCTED_MANIFEST")
    assert g2_4_fourteenth["original_download_event_unavailable_disclosed"] is True
    assert g2_4_fourteenth["provider_or_endpoint_invention_allowed"] is False
    assert g2_4_fourteenth["reconstructed_may_claim_primary_download_provenance"] is False
    assert g2_4_fourteenth["candidate_or_backtest_execution_allowed"] is False
    assert g2_4_fourteenth["direct_writer_calls_after"] == 858
    assert g2_4_fourteenth["legacy_root_lines_after"] == 31379
    assert g2_4_fourteenth["legacy_root_top_level_functions_after"] == 956
    assert g2_4_fourteenth["legacy_root_command_decorators_after"] == 917
    assert g2_4_fourteenth["focused_validation"] == {"status": "PASS", "passed": 46}
    assert g2_4_fourteenth["architecture_fitness"]["passed"] == 197
    g2_4_fifteenth = phase_g["g2_current_plan"]["g2_4_fifteenth_slice"]
    assert g2_4_fifteenth["status"] in {"VALIDATING", "COMPLETE"}
    assert g2_4_fifteenth["callback_count"] == 4
    assert g2_4_fifteenth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_fifteenth["as_of_option_semantics"] == "requested_start"
    assert g2_4_fifteenth["configured_requested_actual_ranges_distinct"] is True
    assert str(g2_4_fifteenth["ai_regime_default_start"]) == "2022-12-01"
    assert g2_4_fifteenth["pre_regime_actual_range_inherently_invalid"] is False
    assert g2_4_fifteenth["research_window_role_automatically_validated"] is False
    assert g2_4_fifteenth["missing_or_invalid_range_fails_closed"] is True
    assert g2_4_fifteenth["late_start_or_early_end_blocks_promotion"] is True
    assert g2_4_fifteenth["report_and_inspect_read_only"] is True
    assert g2_4_fifteenth["candidate_or_backtest_execution_allowed"] is False
    assert g2_4_fifteenth["direct_writer_calls_after"] == 858
    assert g2_4_fifteenth["legacy_root_lines_after"] == 31277
    assert g2_4_fifteenth["legacy_root_top_level_functions_after"] == 952
    assert g2_4_fifteenth["legacy_root_command_decorators_after"] == 913
    assert g2_4_fifteenth["focused_validation"] == {"status": "PASS", "passed": 48}
    assert g2_4_fifteenth["architecture_fitness"]["passed"] == 198
    g2_4_sixteenth = phase_g["g2_current_plan"]["g2_4_sixteenth_slice"]
    assert g2_4_sixteenth["status"] in {"VALIDATING", "COMPLETE"}
    assert g2_4_sixteenth["callback_count"] == 3
    assert g2_4_sixteenth["legacy_callback_definitions_remaining"] == 0
    assert g2_4_sixteenth["deterministic_base_plus_ofat_pairs"] is True
    assert g2_4_sixteenth["grid_prefix_may_prove_parameter_effect"] is False
    assert g2_4_sixteenth["parameter_effect_uses_matched_pairs_only"] is True
    assert g2_4_sixteenth["declared_mapping_alone_proves_consumption"] is False
    assert g2_4_sixteenth["independent_parameter_effect_artifact_required"] is True
    assert g2_4_sixteenth["insufficient_pair_status"] == ("INSUFFICIENT_MATCHED_PAIR_EVIDENCE")
    assert g2_4_sixteenth["insufficient_pair_coverage_audit_status"] == "INCOMPLETE"
    assert g2_4_sixteenth["validation_fails_on_incomplete_pair_coverage"] is True
    assert g2_4_sixteenth["real_evaluation_uses_dq_and_pit_context"] is True
    assert g2_4_sixteenth["latest_report_mode_read_only"] is True
    assert g2_4_sixteenth["automatic_candidate_promotion_allowed"] is False
    assert g2_4_sixteenth["direct_writer_calls_after"] == 858
    assert g2_4_sixteenth["legacy_root_lines_after"] == 31183
    assert g2_4_sixteenth["legacy_root_top_level_functions_after"] == 949
    assert g2_4_sixteenth["legacy_root_command_decorators_after"] == 910
    assert g2_4_sixteenth["focused_validation"] == {"status": "PASS", "passed": 53}
    assert g2_4_sixteenth["architecture_fitness"]["passed"] == 199
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
            assert source["superseded_by_phase"] in {
                "ARCH-004D",
                "ARCH-004F1",
                "ARCH-004G2.4P",
            }
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
            assert source["superseded_by_phase"] in {
                "ARCH-004F1",
                "ARCH-004G1.3D",
                "ARCH-004G2.4O",
            }
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
            assert source["superseded_by_phase"] in {
                "ARCH-004F3",
                "ARCH-004G2.3C",
                "ARCH-004G2.4P",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
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
            assert source["superseded_by_phase"] in {
                "ARCH-004G",
                "ARCH-004G2.4P",
                "ARCH-004G2.4AH",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
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
            assert source["superseded_by_phase"] in {
                "ARCH-004G1",
                "ARCH-004G2.1",
                "ARCH-004G2.4P",
            }
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
    assert phase_g1["validation"]["architecture_fitness"]["current_direct_writer_calls"] == 887
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
            assert source["superseded_by_phase"] in {
                "ARCH-004G2.1",
                "ARCH-004G2.4AQ",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
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
        if source.get("historical_phase_g2_1_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.2"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_2_etf_cli_registration_shell.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_2 = baseline["phase_g2_2_etf_cli_registration_shell"]
    assert phase_g2_2["status"] == "COMPLETE_G2_3_IN_PROGRESS"
    assert phase_g2_2["migration"] == {
        "typer_apps_moved": 291,
        "add_typer_relationships_moved": 290,
        "legacy_typer_app_definitions_remaining": 0,
        "legacy_add_typer_relationships_remaining": 0,
        "legacy_root_lines_before": 37604,
        "legacy_root_lines_after": 36045,
        "legacy_root_line_reduction": 1559,
        "top_level_functions_unchanged": 1049,
        "command_decorators_unchanged": 993,
        "tree_sha256": "afa0760c82cf347bb135ecb12ae133bc16238fb53e28b7a0cf3c699f6ba1cec2",
        "node_contracts_equal": True,
        "callback_functions_moved": 0,
        "runtime_behavior_changed": False,
        "production_effect": "none",
    }
    assert phase_g2_2["validation"]["cli_consumer_focused"] == {
        "status": "PASS",
        "passed": 341,
        "file_count": 25,
    }
    assert phase_g2_2["validation"]["architecture_fitness"]["passed"] == 174
    for source in phase_g2_2["sources"]:
        if source.get("historical_phase_g2_2_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.3A"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_3a_etf_cli_data_features.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_3a = baseline["phase_g2_3a_etf_cli_data_features"]
    assert phase_g2_3a["status"] == "COMPLETE_G2_3_CONTINUES"
    assert phase_g2_3a["migration"]["callback_count"] == 3
    assert phase_g2_3a["migration"]["shared_helper_count"] == 3
    assert phase_g2_3a["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_3a["migration"]["compatibility_aliases_using_canonical_callbacks"] is True
    assert phase_g2_3a["migration"]["data_quality_behavior_changed"] is False
    assert phase_g2_3a["validation"]["focused"] == {"status": "PASS", "passed": 72}
    assert phase_g2_3a["validation"]["architecture_fitness"]["passed"] == 175
    for source in phase_g2_3a["sources"]:
        if source.get("historical_phase_g2_3a_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.3B", "ARCH-004G2.3G"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_3b = baseline["phase_g2_3b_etf_cli_data_quality"]
    assert phase_g2_3b["status"] == "COMPLETE_G2_3_CONTINUES"
    assert phase_g2_3b["migration"]["callback_count"] == 3
    assert phase_g2_3b["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_3b["migration"]["direct_dispatch_using_canonical_callbacks"] is True
    assert phase_g2_3b["migration"]["data_quality_behavior_changed"] is False
    assert phase_g2_3b["validation"]["focused"] == {"status": "PASS", "passed": 44}
    assert phase_g2_3b["validation"]["architecture_fitness"]["passed"] == 176
    for source in phase_g2_3b["sources"]:
        if source.get("historical_phase_g2_3b_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.3C", "ARCH-004G2.3H"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_3c = baseline["phase_g2_3c_etf_cli_operations"]
    assert phase_g2_3c["status"] == "COMPLETE_G2_3_CONTINUES"
    assert phase_g2_3c["migration"]["callback_count"] == 3
    assert phase_g2_3c["migration"]["shared_parser_count"] == 1
    assert phase_g2_3c["migration"]["directory_constant_count"] == 3
    assert phase_g2_3c["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_3c["migration"]["legacy_parser_definitions_remaining"] == 0
    assert phase_g2_3c["migration"]["legacy_directory_constant_definitions_remaining"] == 0
    assert phase_g2_3c["migration"]["direct_dispatch_using_canonical_callbacks"] is True
    assert phase_g2_3c["migration"]["operations_behavior_changed"] is False
    assert phase_g2_3c["validation"]["focused"] == {"status": "PASS", "passed": 111}
    assert phase_g2_3c["validation"]["architecture_fitness"]["passed"] == 177
    for source in phase_g2_3c["sources"]:
        if source.get("historical_phase_g2_3c_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.3D"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_3d_etf_cli_evidence_dashboard.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_3d = baseline["phase_g2_3d_etf_cli_evidence_dashboard"]
    assert phase_g2_3d["status"] == "COMPLETE_G2_3_CONTINUES"
    assert phase_g2_3d["migration"]["callback_count"] == 3
    assert phase_g2_3d["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_3d["migration"]["legacy_strategy_evidence_imports_remaining"] == 0
    assert phase_g2_3d["migration"]["direct_dispatch_using_canonical_callbacks"] is True
    assert phase_g2_3d["migration"]["direct_writer_calls_after"] == 860
    assert phase_g2_3d["migration"]["reporting_behavior_changed"] is False
    assert phase_g2_3d["validation"]["focused"] == {"status": "PASS", "passed": 44}
    assert phase_g2_3d["validation"]["architecture_fitness"]["passed"] == 178
    for source in phase_g2_3d["sources"]:
        if source.get("historical_phase_g2_3d_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.3E"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_3e_etf_cli_weekly_review.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_3e = baseline["phase_g2_3e_etf_cli_weekly_review"]
    assert phase_g2_3e["status"] == "COMPLETE_G2_3_CONTINUES"
    assert phase_g2_3e["migration"]["callback_count"] == 4
    assert phase_g2_3e["migration"]["shared_helper_count"] == 2
    assert phase_g2_3e["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_3e["migration"]["legacy_helper_definitions_remaining"] == 0
    assert phase_g2_3e["migration"]["legacy_weekly_review_imports_remaining"] == 0
    assert phase_g2_3e["migration"]["legacy_callers_using_canonical_date_helper"] is True
    assert phase_g2_3e["migration"]["reporting_behavior_changed"] is False
    assert phase_g2_3e["validation"]["focused"] == {"status": "PASS", "passed": 84}
    assert phase_g2_3e["validation"]["architecture_fitness"]["passed"] == 179
    for source in phase_g2_3e["sources"]:
        if source.get("historical_phase_g2_3e_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.3F"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_3f_etf_cli_parameter_review.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_3f = baseline["phase_g2_3f_etf_cli_parameter_review"]
    assert phase_g2_3f["status"] == "COMPLETE_G2_3_CONTINUES"
    assert phase_g2_3f["migration"]["callback_count"] == 4
    assert phase_g2_3f["migration"]["shared_helper_count"] == 1
    assert phase_g2_3f["migration"]["canonical_date_helper_reused"] == "weekly_review_date"
    assert phase_g2_3f["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_3f["migration"]["legacy_helper_definitions_remaining"] == 0
    assert phase_g2_3f["migration"]["legacy_parameter_review_imports_remaining"] == 0
    assert phase_g2_3f["migration"]["reporting_behavior_changed"] is False
    assert phase_g2_3f["validation"]["focused"] == {"status": "PASS", "passed": 65}
    assert phase_g2_3f["validation"]["architecture_fitness"]["passed"] == 180
    for source in phase_g2_3f["sources"]:
        if source.get("historical_phase_g2_3f_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.3G"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_3g_etf_cli_satellite_attribution.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_3g = baseline["phase_g2_3g_etf_cli_satellite_attribution"]
    assert phase_g2_3g["status"] == "COMPLETE_G2_3_CONTINUES"
    assert phase_g2_3g["migration"]["callback_count"] == 3
    assert phase_g2_3g["migration"]["shared_helper_count"] == 2
    assert phase_g2_3g["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_3g["migration"]["legacy_helper_definitions_remaining"] == 0
    assert str(phase_g2_3g["migration"]["default_ai_regime_start"]) == "2022-12-01"
    assert phase_g2_3g["migration"]["invalid_price_fixture_fail_closed"] is True
    assert phase_g2_3g["migration"]["data_quality_behavior_changed"] is False
    assert phase_g2_3g["migration"]["regime_interpretation_changed"] is False
    assert phase_g2_3g["validation"]["focused"] == {"status": "PASS", "passed": 78}
    assert phase_g2_3g["validation"]["architecture_fitness"]["passed"] == 181
    for source in phase_g2_3g["sources"]:
        if source.get("historical_phase_g2_3g_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.3H", "ARCH-004G2.4A"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_3h = baseline["phase_g2_3h_etf_cli_trend_calibration"]
    assert phase_g2_3h["status"] == "COMPLETE_G2_3_CONTINUES"
    assert phase_g2_3h["migration"]["callback_count"] == 3
    assert phase_g2_3h["migration"]["shared_dq_helper_count"] == 4
    assert phase_g2_3h["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_3h["migration"]["legacy_dq_helper_definitions_remaining"] == 0
    assert phase_g2_3h["migration"]["dq_gate_precedes_price_and_feature_build"] is True
    assert phase_g2_3h["migration"]["dq_failure_fixture_fail_closed"] is True
    assert phase_g2_3h["migration"]["data_quality_behavior_changed"] is False
    assert phase_g2_3h["migration"]["regime_interpretation_changed"] is False
    assert phase_g2_3h["migration"]["strategy_or_threshold_changed"] is False
    assert phase_g2_3h["validation"]["focused"] == {"status": "PASS", "passed": 54}
    assert phase_g2_3h["validation"]["architecture_fitness"]["passed"] == 182
    for source in phase_g2_3h["sources"]:
        if source.get("historical_phase_g2_3h_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.3_CLOSEOUT", "ARCH-004G2.4A"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    closeout = baseline["phase_g2_3_closeout_g2_4_start"]
    assert closeout["status"] == "COMPLETE_G2_4_IN_PROGRESS"
    assert closeout["g2_3_closeout"]["slice_count"] == 8
    assert closeout["g2_3_closeout"]["canonical_module_count"] == 9
    assert closeout["g2_3_closeout"]["migrated_callback_count"] == 26
    assert closeout["g2_3_closeout"]["migrated_helper_count"] == 13
    assert closeout["g2_3_closeout"]["legacy_selected_definitions_remaining"] == 0
    assert closeout["g2_3_closeout"]["legacy_selected_domain_imports_remaining"] == 0
    assert closeout["g2_4_start"]["first_slice"] == "baseline_review"
    assert closeout["g2_4_start"]["implementation_started"] is False
    assert closeout["validation"]["architecture_fitness"]["passed"] == 183
    for source in closeout["sources"]:
        if source.get("historical_phase_g2_3_closeout_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4A"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4a_etf_cli_baseline_review.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4a = baseline["phase_g2_4a_etf_cli_baseline_review"]
    assert phase_g2_4a["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4a["migration"]["callback_count"] == 7
    assert phase_g2_4a["migration"]["shared_helper_count"] == 1
    assert phase_g2_4a["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_4a["migration"]["legacy_helper_definitions_remaining"] == 0
    assert phase_g2_4a["migration"]["legacy_baseline_review_imports_remaining"] == 0
    assert phase_g2_4a["migration"]["governance_journal_write_allowed"] is True
    assert phase_g2_4a["migration"]["production_runtime_state_mutation_allowed"] is False
    assert phase_g2_4a["migration"]["proposal_is_draft_only"] is True
    assert phase_g2_4a["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4a["validation"]["focused"] == {"status": "PASS", "passed": 36}
    assert phase_g2_4a["validation"]["architecture_fitness"]["passed"] == 184
    for source in phase_g2_4a["sources"]:
        if source.get("historical_phase_g2_4a_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.4B", "ARCH-004G2.4C"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4b = baseline["phase_g2_4b_etf_cli_shadow_review"]
    assert phase_g2_4b["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4b["migration"]["callback_count"] == 4
    assert phase_g2_4b["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_4b["migration"]["legacy_shadow_ready_review_imports_remaining"] == 0
    assert phase_g2_4b["migration"]["candidate_governance_artifact_write_allowed"] is True
    assert phase_g2_4b["migration"]["decision_journal_write_allowed"] is False
    assert phase_g2_4b["migration"]["automatic_paper_shadow_execution_allowed"] is False
    assert phase_g2_4b["migration"]["runtime_registry_mutation_allowed"] is False
    assert phase_g2_4b["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4b["validation"]["focused"] == {"status": "PASS", "passed": 21}
    assert phase_g2_4b["validation"]["architecture_fitness"]["passed"] == 185
    for source in phase_g2_4b["sources"]:
        if source.get("historical_phase_g2_4b_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4C"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4c_etf_cli_dynamic_allocation.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4c = baseline["phase_g2_4c_etf_cli_dynamic_allocation"]
    assert phase_g2_4c["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4c["migration"]["callback_count"] == 3
    assert phase_g2_4c["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_4c["migration"]["legacy_helper_definitions_remaining"] == 0
    assert phase_g2_4c["migration"]["candidate_decision_artifact_write_allowed"] is True
    assert phase_g2_4c["migration"]["runtime_registry_mutation_allowed"] is False
    assert phase_g2_4c["migration"]["official_target_weights_mutation_allowed"] is False
    assert phase_g2_4c["migration"]["production_rebalance_allowed"] is False
    assert phase_g2_4c["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4c["validation"]["focused"] == {"status": "PASS", "passed": 21}
    assert phase_g2_4c["validation"]["architecture_fitness"]["passed"] == 186
    for source in phase_g2_4c["sources"]:
        if source.get("historical_phase_g2_4c_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4D"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4d_etf_cli_dynamic_calibration.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4d = baseline["phase_g2_4d_etf_cli_dynamic_calibration"]
    assert phase_g2_4d["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4d["migration"]["callback_count"] == 3
    assert phase_g2_4d["migration"]["legacy_callback_definitions_remaining"] == 0
    assert phase_g2_4d["migration"]["research_cache_write_allowed"] is True
    assert phase_g2_4d["migration"]["automatic_candidate_promotion_allowed"] is False
    assert phase_g2_4d["migration"]["auto_enrollment_without_owner_approval_allowed"] is False
    assert phase_g2_4d["migration"]["official_target_weights_mutation_allowed"] is False
    assert phase_g2_4d["migration"]["validation_uses_canonical_cli_owner"] is True
    assert phase_g2_4d["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4d["validation"]["focused"] == {"status": "PASS", "passed": 24}
    assert phase_g2_4d["validation"]["architecture_fitness"]["passed"] == 187
    for source in phase_g2_4d["sources"]:
        if source.get("historical_phase_g2_4d_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4E"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4e_etf_cli_dynamic_robustness.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4e = baseline["phase_g2_4e_etf_cli_dynamic_robustness"]
    assert phase_g2_4e["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4e["migration"]["callback_count"] == 2
    assert phase_g2_4e["migration"]["cached_dq_gate_precedes_standard_price_validation"] is True
    assert phase_g2_4e["migration"]["standard_price_validation_precedes_robustness"] is True
    assert phase_g2_4e["migration"]["dq_failure_fail_closed"] is True
    assert phase_g2_4e["migration"]["latest_mode_read_only"] is True
    assert phase_g2_4e["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4e["validation"]["focused"] == {"status": "PASS", "passed": 24}
    assert phase_g2_4e["validation"]["architecture_fitness"]["passed"] == 188
    for source in phase_g2_4e["sources"]:
        if source.get("historical_phase_g2_4e_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4F"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4f_etf_cli_dynamic_rescue.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4f = baseline["phase_g2_4f_etf_cli_dynamic_rescue"]
    assert phase_g2_4f["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4f["migration"]["callback_count"] == 3
    assert phase_g2_4f["migration"]["cached_dq_gate_precedes_standard_price_validation"] is True
    assert phase_g2_4f["migration"]["standard_price_validation_precedes_rescue_comparison"] is True
    assert phase_g2_4f["migration"]["dq_failure_fail_closed"] is True
    assert phase_g2_4f["migration"]["bounded_rescue_candidate_artifact_write_allowed"] is True
    assert phase_g2_4f["migration"]["automatic_candidate_enrollment_allowed"] is False
    assert phase_g2_4f["migration"]["owner_approval_executed"] is False
    assert phase_g2_4f["migration"]["automatic_candidate_promotion_allowed"] is False
    assert phase_g2_4f["migration"]["official_target_weights_mutation_allowed"] is False
    assert phase_g2_4f["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4f["validation"]["focused"] == {"status": "PASS", "passed": 25}
    assert phase_g2_4f["validation"]["architecture_fitness"]["passed"] == 189
    for source in phase_g2_4f["sources"]:
        if source.get("historical_phase_g2_4f_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4G"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4g_etf_cli_dynamic_v2_review.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4g = baseline["phase_g2_4g_etf_cli_dynamic_v2_review"]
    assert phase_g2_4g["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4g["migration"]["callback_count"] == 3
    assert phase_g2_4g["migration"]["market_backtest_reexecution_allowed"] is False
    assert phase_g2_4g["migration"]["mandatory_source_missing_fail_closed"] is True
    assert phase_g2_4g["migration"]["optional_shadow_missing_is_warning"] is True
    assert phase_g2_4g["migration"]["latest_report_mode_read_only"] is True
    assert phase_g2_4g["migration"]["shadow_enrollment_allowed"] is False
    assert phase_g2_4g["migration"]["owner_approval_executed"] is False
    assert phase_g2_4g["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4g["validation"]["focused"] == {"status": "PASS", "passed": 27}
    assert phase_g2_4g["validation"]["architecture_fitness"]["passed"] == 190
    for source in phase_g2_4g["sources"]:
        if source.get("historical_phase_g2_4g_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4H"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4h_etf_cli_dynamic_v3_rescue_base.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4h = baseline["phase_g2_4h_etf_cli_dynamic_v3_rescue_base"]
    assert phase_g2_4h["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4h["migration"]["callback_count"] == 3
    assert phase_g2_4h["migration"]["remaining_dynamic_v3_commands_stay_legacy_owned"] is True
    assert phase_g2_4h["migration"]["v0_4_review_package_read_only"] is True
    assert phase_g2_4h["migration"]["base_candidate_must_match_reviewed_policy"] is True
    assert phase_g2_4h["migration"]["latest_report_mode_read_only"] is True
    assert phase_g2_4h["migration"]["shadow_enrollment_allowed"] is False
    assert phase_g2_4h["migration"]["owner_approval_executed"] is False
    assert phase_g2_4h["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4h["validation"]["focused"] == {"status": "PASS", "passed": 28}
    assert phase_g2_4h["validation"]["architecture_fitness"]["passed"] == 191
    for source in phase_g2_4h["sources"]:
        if source.get("historical_phase_g2_4h_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4I"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4i_etf_cli_dynamic_v3_real_evaluation.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4i = baseline["phase_g2_4i_etf_cli_dynamic_v3_real_evaluation"]
    assert phase_g2_4i["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4i["migration"]["callback_count"] == 3
    assert phase_g2_4i["migration"]["cached_dq_gate_precedes_standard_price_validation"] is True
    assert phase_g2_4i["migration"]["standard_price_validation_precedes_pit_evaluation"] is True
    assert phase_g2_4i["migration"]["dq_failure_fail_closed"] is True
    assert phase_g2_4i["migration"]["requested_range_and_ai_regime_separate"] is True
    assert phase_g2_4i["migration"]["pre_regime_primary_conclusion_allowed"] is False
    assert phase_g2_4i["migration"]["promotion_gate_executes_promotion"] is False
    assert phase_g2_4i["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4i["validation"]["focused"] == {"status": "PASS", "passed": 28}
    assert phase_g2_4i["validation"]["architecture_fitness"]["passed"] == 192
    for source in phase_g2_4i["sources"]:
        if source.get("historical_phase_g2_4i_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4J"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4j_etf_cli_dynamic_v3_failure_attribution.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4j = baseline["phase_g2_4j_etf_cli_dynamic_v3_failure_attribution"]
    assert phase_g2_4j["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4j["migration"]["callback_count"] == 3
    assert phase_g2_4j["migration"]["real_evaluation_lineage_loaded_before_dq"] is True
    assert phase_g2_4j["migration"]["dq_as_of_inherits_explicit_or_source_end"] is True
    assert phase_g2_4j["migration"]["cached_dq_gate_precedes_standard_price_validation"] is True
    assert phase_g2_4j["migration"]["standard_price_validation_precedes_pit_attribution"] is True
    assert phase_g2_4j["migration"]["source_artifact_mutation_allowed"] is False
    assert phase_g2_4j["migration"]["review_or_recommendation_executes_promotion"] is False
    assert phase_g2_4j["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4j["validation"]["focused"] == {"status": "PASS", "passed": 28}
    assert phase_g2_4j["validation"]["architecture_fitness"]["passed"] == 193
    for source in phase_g2_4j["sources"]:
        if source.get("historical_phase_g2_4j_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4K"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4k_etf_cli_dynamic_v3_sweep_config.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4k = baseline["phase_g2_4k_etf_cli_dynamic_v3_sweep_config"]
    assert phase_g2_4k["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4k["migration"]["callback_count"] == 2
    assert phase_g2_4k["migration"]["reviewed_config_read_only"] is True
    assert phase_g2_4k["migration"]["stable_candidate_id_enumeration"] is True
    assert phase_g2_4k["migration"]["evaluator_execution_allowed"] is False
    assert phase_g2_4k["migration"]["runtime_artifact_write_allowed"] is False
    assert phase_g2_4k["migration"]["production_candidate_generated"] is False
    assert phase_g2_4k["migration"]["preview_limit_changes_candidate_universe"] is False
    assert phase_g2_4k["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4k["validation"]["focused"] == {"status": "PASS", "passed": 43}
    assert phase_g2_4k["validation"]["architecture_fitness"]["passed"] == 194
    for source in phase_g2_4k["sources"]:
        if source.get("historical_phase_g2_4k_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.4L", "ARCH-004G2.4O"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4l = baseline["phase_g2_4l_etf_cli_dynamic_v3_sweep_runtime"]
    assert phase_g2_4l["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4l["migration"]["callback_count"] == 8
    assert phase_g2_4l["migration"]["helper_count"] == 1
    assert phase_g2_4l["migration"]["real_evaluator_uses_dq_and_pit_path"] is True
    assert phase_g2_4l["migration"]["tiny_fixture_not_for_investment_decision"] is True
    assert phase_g2_4l["migration"]["resume_evaluator_mode_mutation_allowed"] is False
    assert phase_g2_4l["migration"]["resume_worker_override_recorded"] is True
    assert phase_g2_4l["migration"]["production_candidate_generated"] is False
    assert phase_g2_4l["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4l["validation"]["focused"] == {"status": "PASS", "passed": 44}
    assert phase_g2_4l["validation"]["architecture_fitness"]["passed"] == 195
    for source in phase_g2_4l["sources"]:
        if source.get("historical_phase_g2_4l_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.4M", "ARCH-004G2.4O"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4m = baseline["phase_g2_4m_etf_cli_dynamic_v3_data_audit"]
    assert phase_g2_4m["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4m["migration"]["callback_count"] == 3
    assert phase_g2_4m["migration"]["same_validate_data_path_used"] is True
    assert phase_g2_4m["migration"]["failed_quality_evidence_write_allowed"] is True
    assert phase_g2_4m["migration"]["dq_failure_may_be_reported_as_pass"] is False
    assert phase_g2_4m["migration"]["checksum_and_provenance_artifacts_required"] is True
    assert phase_g2_4m["migration"]["validation_requires_dq_non_fail"] is True
    assert phase_g2_4m["migration"]["cache_or_download_manifest_mutation_allowed"] is False
    assert phase_g2_4m["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4m["validation"]["focused"] == {"status": "PASS", "passed": 45}
    assert phase_g2_4m["validation"]["architecture_fitness"]["passed"] == 196
    for source in phase_g2_4m["sources"]:
        if source.get("historical_phase_g2_4m_hash"):
            assert source["superseded_by_phase"] in {"ARCH-004G2.4N", "ARCH-004G2.4O"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4n = baseline["phase_g2_4n_etf_cli_dynamic_v3_data_provenance"]
    assert phase_g2_4n["status"] == "COMPLETE_G2_4_CONTINUES"
    assert phase_g2_4n["migration"]["callback_count"] == 3
    assert phase_g2_4n["migration"]["inspect_and_validate_read_only"] is True
    assert phase_g2_4n["migration"]["supported_repair_mode"] == "reconstruct-from-cache"
    assert phase_g2_4n["migration"]["repair_requires_all_cache_files"] is True
    assert phase_g2_4n["migration"]["original_download_event_unavailable_disclosed"] is True
    assert phase_g2_4n["migration"]["provider_or_endpoint_invention_allowed"] is False
    assert phase_g2_4n["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4n["validation"]["focused"] == {"status": "PASS", "passed": 46}
    assert phase_g2_4n["validation"]["architecture_fitness"]["passed"] == 197
    for source in phase_g2_4n["sources"]:
        if source.get("historical_phase_g2_4n_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4O"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4o_etf_cli_dynamic_v3_window_audit.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4o = baseline["phase_g2_4o_etf_cli_dynamic_v3_window_audit"]
    assert phase_g2_4o["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    assert phase_g2_4o["migration"]["callback_count"] == 4
    assert phase_g2_4o["migration"]["as_of_option_semantics"] == "requested_start"
    assert phase_g2_4o["migration"]["configured_requested_actual_ranges_distinct"] is True
    assert str(phase_g2_4o["migration"]["ai_regime_default_start"]) == "2022-12-01"
    assert phase_g2_4o["migration"]["pre_regime_actual_range_inherently_invalid"] is False
    assert phase_g2_4o["migration"]["research_window_role_automatically_validated"] is False
    assert phase_g2_4o["migration"]["missing_or_invalid_range_fails_closed"] is True
    assert phase_g2_4o["migration"]["late_start_or_early_end_blocks_promotion"] is True
    assert phase_g2_4o["migration"]["report_and_inspect_read_only"] is True
    assert phase_g2_4o["migration"]["candidate_or_backtest_execution_allowed"] is False
    assert phase_g2_4o["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4o["validation"]["focused"] == {"status": "PASS", "passed": 48}
    assert phase_g2_4o["validation"]["architecture_fitness"]["passed"] == 198
    for source in phase_g2_4o["sources"]:
        if source.get("historical_phase_g2_4o_hash"):
            assert source["superseded_by_phase"] == "ARCH-004G2.4P"
            assert source["current_hash_tracked_in"] == (
                "phase_g2_4p_etf_cli_dynamic_v3_injection_audit.sources"
            )
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4p = baseline["phase_g2_4p_etf_cli_dynamic_v3_injection_audit"]
    assert phase_g2_4p["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    assert phase_g2_4p["migration"]["callback_count"] == 3
    assert phase_g2_4p["migration"]["deterministic_base_plus_ofat_pairs"] is True
    assert phase_g2_4p["migration"]["grid_prefix_may_prove_parameter_effect"] is False
    assert phase_g2_4p["migration"]["parameter_effect_uses_matched_pairs_only"] is True
    assert phase_g2_4p["migration"]["declared_mapping_alone_proves_consumption"] is False
    assert phase_g2_4p["migration"]["independent_parameter_effect_artifact_required"] is True
    assert phase_g2_4p["migration"]["insufficient_pair_status"] == (
        "INSUFFICIENT_MATCHED_PAIR_EVIDENCE"
    )
    assert phase_g2_4p["migration"]["validation_fails_on_incomplete_pair_coverage"] is True
    assert phase_g2_4p["migration"]["real_evaluation_uses_dq_and_pit_context"] is True
    assert phase_g2_4p["migration"]["latest_report_mode_read_only"] is True
    assert phase_g2_4p["migration"]["automatic_candidate_promotion_allowed"] is False
    assert phase_g2_4p["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4p["validation"]["focused"] == {"status": "PASS", "passed": 53}
    assert phase_g2_4p["validation"]["architecture_fitness"]["passed"] == 199
    superseded_g2_4p = set(phase_g2_4p["superseded_source_paths"])
    for source in phase_g2_4p["sources"]:
        if source["path"] in superseded_g2_4p:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4q = baseline["phase_g2_4q_etf_cli_dynamic_v3_weight_path"]
    assert phase_g2_4q["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    assert phase_g2_4q["migration"]["callback_count"] == 2
    assert phase_g2_4q["migration"]["shared_read_only_content_inspection"] is True
    assert phase_g2_4q["migration"]["metadata_declaration_alone_proves_completeness"] is False
    assert phase_g2_4q["migration"]["unique_evaluation_directory_required"] is True
    assert phase_g2_4q["migration"]["daily_weight_sum_validation_required"] is True
    assert phase_g2_4q["migration"]["metadata_content_parity_required"] is True
    assert phase_g2_4q["migration"]["event_and_turnover_content_validation_required"] is True
    assert phase_g2_4q["migration"]["invalid_core_observed_status"] == "INCOMPLETE"
    assert phase_g2_4q["migration"]["valid_minimal_observed_status"] == "PARTIAL"
    assert phase_g2_4q["migration"]["complete_requires_no_missing_fields"] is True
    assert phase_g2_4q["migration"]["complete_requires_parseable_detail_fields"] is True
    assert phase_g2_4q["migration"]["declared_observed_mismatch_fails_validation"] is True
    assert phase_g2_4q["migration"]["source_artifact_mutation_allowed"] is False
    assert phase_g2_4q["migration"]["automatic_candidate_promotion_allowed"] is False
    assert phase_g2_4q["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4q["validation"]["focused"] == {"status": "PASS", "passed": 59}
    assert phase_g2_4q["validation"]["architecture_fitness"]["passed"] == 200
    superseded_g2_4q = set(phase_g2_4q["superseded_source_paths"])
    for source in phase_g2_4q["sources"]:
        if source["path"] in superseded_g2_4q:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4r = baseline["phase_g2_4r_etf_cli_dynamic_v3_candidate_evidence"]
    assert phase_g2_4r["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    assert phase_g2_4r["migration"]["callback_count"] == 3
    assert phase_g2_4r["migration"]["explicit_candidate_report_prerequisite"] is True
    assert phase_g2_4r["migration"]["attribution_source_mutation_allowed"] is False
    assert phase_g2_4r["migration"]["candidate_results_and_report_checksums_required"] is True
    assert phase_g2_4r["migration"]["real_artifact_sweep_candidate_ownership_required"] is True
    assert phase_g2_4r["migration"]["observed_weight_path_completeness_required"] is True
    assert phase_g2_4r["migration"]["weight_delta_recomputed_from_daily_paths"] is True
    assert phase_g2_4r["migration"]["current_weight_reference"] == "static_base_candidate"
    assert phase_g2_4r["migration"]["dynamic_v0_4_weights_may_be_inferred_from_summary"] is False
    assert phase_g2_4r["migration"]["current_attribution_method"] == "path_and_aggregate_v2"
    assert phase_g2_4r["migration"]["complete_attribution_allowed"] is False
    assert (
        phase_g2_4r["migration"]["validation_recomputes_lineage_delta_status_and_checksums"] is True
    )
    assert phase_g2_4r["migration"]["sweep_or_real_evaluation_execution_allowed"] is False
    assert phase_g2_4r["migration"]["automatic_candidate_promotion_allowed"] is False
    assert phase_g2_4r["migration"]["direct_writer_calls_after"] == 858
    assert phase_g2_4r["validation"]["focused"] == {"status": "PASS", "passed": 62}
    assert phase_g2_4r["validation"]["architecture_fitness"]["passed"] == 201
    superseded_g2_4r = set(phase_g2_4r["superseded_source_paths"])
    for source in phase_g2_4r["sources"]:
        if source["path"] in superseded_g2_4r:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4s = baseline["phase_g2_4s_etf_cli_dynamic_v3_validation_evidence"]
    assert phase_g2_4s["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4s = phase_g2_4s["migration"]
    assert migration_g2_4s["callback_count"] == 6
    assert migration_g2_4s["full_period_metrics_used_as_window_evidence"] is False
    assert migration_g2_4s["stable_hash_used_as_window_evidence"] is False
    assert migration_g2_4s["global_gate_pre_filters_train_candidates"] is False
    assert migration_g2_4s["rejected_or_unscored_train_candidate_may_be_selected"] is False
    assert migration_g2_4s["real_daily_path_window_recomputation_required"] is True
    assert migration_g2_4s["profile_config_source_lineage_checks_required"] is True
    assert migration_g2_4s["source_and_output_checksums_required"] is True
    assert migration_g2_4s["tiny_fixture_true_walk_forward_pass_allowed"] is False
    assert migration_g2_4s["partial_walk_forward_pass_allowed"] is False
    assert migration_g2_4s["current_walk_forward_status"] == "INCOMPLETE"
    assert migration_g2_4s["current_walk_forward_no_eligible_window_count"] == 2
    assert migration_g2_4s["current_overfit_method"] == "path_and_aggregate_overfit_v2"
    assert migration_g2_4s["partial_or_proxy_low_risk_allowed"] is False
    assert migration_g2_4s["content_recomputation_validation_required"] is True
    assert migration_g2_4s["source_sweep_execution_allowed"] is False
    assert migration_g2_4s["automatic_candidate_promotion_allowed"] is False
    assert migration_g2_4s["automatic_shadow_enrollment_allowed"] is False
    assert migration_g2_4s["direct_writer_calls_after"] == 858
    assert migration_g2_4s["python_module_count"] == 830
    assert phase_g2_4s["real_smoke"]["walk_forward_validation"] == "PASS"
    assert phase_g2_4s["real_smoke"]["selected_candidate_count"] == 0
    assert phase_g2_4s["real_smoke"]["superseded_intermediate_validation"] == "FAIL"
    assert phase_g2_4s["real_smoke"]["overfit_validation"] == "PASS"
    assert phase_g2_4s["validation"]["focused"] == {"status": "PASS", "passed": 65}
    assert phase_g2_4s["validation"]["architecture_fitness"]["passed"] == 202
    assert phase_g2_4s["sources"]
    superseded_g2_4s = set(phase_g2_4s["superseded_source_paths"])
    for source in phase_g2_4s["sources"]:
        if source["path"] in superseded_g2_4s:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4t = baseline["phase_g2_4t_etf_cli_dynamic_v3_legacy_validation"]
    assert phase_g2_4t["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4t = phase_g2_4t["migration"]
    assert migration_g2_4t["callback_count"] == 6
    assert migration_g2_4t["legacy_full_period_metrics_used_as_window_evidence"] is False
    assert migration_g2_4t["legacy_stable_hash_used_as_window_evidence"] is False
    assert migration_g2_4t["shared_real_daily_path_window_recomputation_required"] is True
    assert migration_g2_4t["tiny_fixture_evidence_completeness"] == "PROXY_ONLY"
    assert migration_g2_4t["tiny_fixture_walk_forward_pass_allowed"] is False
    assert migration_g2_4t["source_and_output_checksums_required"] is True
    assert migration_g2_4t["content_recomputation_validation_required"] is True
    assert migration_g2_4t["real_neighbor_path_report_identity_required"] is True
    assert migration_g2_4t["aggregate_stress_may_be_dedicated_bucket_pass"] is False
    assert migration_g2_4t["regime_observation_may_be_stability_pass"] is False
    assert migration_g2_4t["old_manifest_complete_shadow_basis_allowed"] is False
    assert migration_g2_4t["validator_automatic_registry_repair_allowed"] is False
    assert migration_g2_4t["automatic_candidate_promotion_allowed"] is False
    assert migration_g2_4t["automatic_shadow_enrollment_allowed"] is False
    assert migration_g2_4t["direct_writer_calls_after"] == 858
    assert migration_g2_4t["legacy_root_lines_after"] == 30762
    assert migration_g2_4t["legacy_root_top_level_functions_after"] == 932
    assert migration_g2_4t["legacy_root_command_decorators_after"] == 893
    assert migration_g2_4t["parameter_research_g6_decomposition_debt_recorded"] is True
    assert migration_g2_4t["python_module_count"] == 831
    assert phase_g2_4t["real_smoke"]["walk_forward_validation"] == "PASS"
    assert phase_g2_4t["real_smoke"]["walk_forward_result_row_count"] == 40
    assert phase_g2_4t["real_smoke"]["robustness_validation"] == "PASS"
    assert phase_g2_4t["real_smoke"]["superseded_walk_forward_validation"] == "FAIL"
    assert phase_g2_4t["real_smoke"]["superseded_robustness_validation"] == "FAIL"
    assert phase_g2_4t["real_smoke"]["current_shadow_registry_validation"] == "FAIL"
    assert phase_g2_4t["real_smoke"]["automatic_registry_mutation_performed"] is False
    assert phase_g2_4t["sources"]
    superseded_g2_4t = set(phase_g2_4t["superseded_source_paths"])
    for source in phase_g2_4t["sources"]:
        if source["path"] in superseded_g2_4t:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4u = baseline["phase_g2_4u_etf_cli_dynamic_v3_shadow_registry"]
    assert phase_g2_4u["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4u = phase_g2_4u["migration"]
    assert migration_g2_4u["callback_count"] == 4
    assert migration_g2_4u["implicit_mtime_evidence_selection_allowed"] is False
    assert migration_g2_4u["explicit_evidence_ids_must_be_paired"] is True
    assert migration_g2_4u["content_recomputation_validation_required"] is True
    assert migration_g2_4u["candidate_and_sweep_ownership_required"] is True
    assert migration_g2_4u["promotion_latest_fallback_allowed"] is False
    assert migration_g2_4u["validator_automatic_registry_repair_allowed"] is False
    assert migration_g2_4u["automatic_candidate_promotion_allowed"] is False
    assert migration_g2_4u["automatic_shadow_enrollment_allowed"] is False
    assert migration_g2_4u["legacy_root_lines_after"] == 30628
    assert migration_g2_4u["legacy_root_top_level_functions_after"] == 928
    assert migration_g2_4u["legacy_root_command_decorators_after"] == 889
    assert migration_g2_4u["python_module_count"] == 832
    assert phase_g2_4u["real_smoke"]["temporary_explicit_registration"] == "PASS"
    assert phase_g2_4u["real_smoke"]["current_shadow_registry_validation"] == "FAIL"
    assert phase_g2_4u["real_smoke"]["automatic_registry_mutation_performed"] is False
    assert phase_g2_4u["sources"]
    superseded_g2_4u = set(phase_g2_4u["superseded_source_paths"])
    for source in phase_g2_4u["sources"]:
        if source["path"] in superseded_g2_4u:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4v = baseline["phase_g2_4v_etf_cli_dynamic_v3_research_control"]
    assert phase_g2_4v["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4v = phase_g2_4v["migration"]
    assert migration_g2_4v["callback_count"] == 11
    assert migration_g2_4v["governance_diff_read_only"] is True
    assert migration_g2_4v["research_query_compare_history_read_only"] is True
    assert migration_g2_4v["research_index_rebuild_only"] is True
    assert migration_g2_4v["artifact_latest_validate_stale_read_only"] is True
    assert migration_g2_4v["repair_latest_is_only_pointer_writer"] is True
    assert migration_g2_4v["repair_latest_canonical_root_only"] is True
    assert migration_g2_4v["source_artifact_mutation_allowed"] is False
    assert migration_g2_4v["research_or_candidate_execution_allowed"] is False
    assert migration_g2_4v["automatic_candidate_promotion_allowed"] is False
    assert migration_g2_4v["legacy_root_lines_after"] == 30391
    assert migration_g2_4v["legacy_root_top_level_functions_after"] == 917
    assert migration_g2_4v["legacy_root_command_decorators_after"] == 878
    assert migration_g2_4v["python_module_count"] == 833
    assert phase_g2_4v["sources"]
    superseded_g2_4v = set(phase_g2_4v["superseded_source_paths"])
    for source in phase_g2_4v["sources"]:
        if source["path"] in superseded_g2_4v:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4w = baseline["phase_g2_4w_etf_cli_dynamic_v3_observation_lifecycle"]
    assert phase_g2_4w["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4w = phase_g2_4w["migration"]
    assert migration_g2_4w["callback_count"] == 7
    assert migration_g2_4w["shadow_monitor_observe_only"] is True
    assert migration_g2_4w["scheduled_observe_lightweight_gate_only"] is True
    assert migration_g2_4w["scheduled_observe_research_execution_allowed"] is False
    assert migration_g2_4w["promotion_pack_manual_review_only"] is True
    assert migration_g2_4w["promotion_pack_pass_means_production_approval"] is False
    assert migration_g2_4w["automatic_candidate_promotion_allowed"] is False
    assert migration_g2_4w["automatic_shadow_enrollment_allowed"] is False
    assert migration_g2_4w["official_weight_or_broker_mutation_allowed"] is False
    assert migration_g2_4w["legacy_root_lines_after"] == 30163
    assert migration_g2_4w["legacy_root_top_level_functions_after"] == 910
    assert migration_g2_4w["legacy_root_command_decorators_after"] == 871
    assert migration_g2_4w["python_module_count"] == 834
    assert phase_g2_4w["sources"]
    superseded_g2_4w = set(phase_g2_4w["superseded_source_paths"])
    for source in phase_g2_4w["sources"]:
        if source["path"] in superseded_g2_4w:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4x = baseline["phase_g2_4x_etf_cli_dynamic_v3_evidence_readiness"]
    assert phase_g2_4x["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4x = phase_g2_4x["migration"]
    assert migration_g2_4x["callback_count"] == 17
    assert migration_g2_4x["evidence_summary_is_promotion"] is False
    assert migration_g2_4x["medium_real_report_runs_sweep"] is False
    assert migration_g2_4x["regime_coverage_requires_price_input"] is True
    assert migration_g2_4x["interpretation_is_manual_review_material"] is True
    assert migration_g2_4x["observe_pool_writes_shadow_registry"] is False
    assert migration_g2_4x["overnight_readiness_runs_overnight_sweep"] is False
    assert migration_g2_4x["ready_or_usable_means_production_ready"] is False
    assert migration_g2_4x["automatic_candidate_promotion_allowed"] is False
    assert migration_g2_4x["official_weight_or_broker_mutation_allowed"] is False
    assert migration_g2_4x["legacy_root_lines_after"] == 29703
    assert migration_g2_4x["legacy_root_top_level_functions_after"] == 893
    assert migration_g2_4x["legacy_root_command_decorators_after"] == 854
    assert migration_g2_4x["python_module_count"] == 835
    assert phase_g2_4x["sources"]
    superseded_g2_4x = set(phase_g2_4x["superseded_source_paths"])
    for source in phase_g2_4x["sources"]:
        if source["path"] in superseded_g2_4x:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4y = baseline["phase_g2_4y_etf_cli_dynamic_v3_evidence_governance"]
    assert phase_g2_4y["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4y = phase_g2_4y["migration"]
    assert migration_g2_4y["callback_count"] == 19
    assert migration_g2_4y["gate_impact_simulation_only"] is True
    assert migration_g2_4y["policy_apply_requires_reviewed_policy"] is True
    assert migration_g2_4y["hard_blocker_downgrade_allowed"] is False
    assert migration_g2_4y["candidate_recovery_observe_only"] is True
    assert migration_g2_4y["observe_pool_rebuild_writes_shadow_registry"] is False
    assert migration_g2_4y["research_decision_executes_promotion"] is False
    assert migration_g2_4y["automatic_candidate_promotion_allowed"] is False
    assert migration_g2_4y["official_weight_or_broker_mutation_allowed"] is False
    assert migration_g2_4y["legacy_root_lines_after"] == 29165
    assert migration_g2_4y["legacy_root_top_level_functions_after"] == 874
    assert migration_g2_4y["legacy_root_command_decorators_after"] == 835
    assert migration_g2_4y["python_module_count"] == 836
    assert phase_g2_4y["sources"]
    superseded_g2_4y = set(phase_g2_4y["superseded_source_paths"])
    for source in phase_g2_4y["sources"]:
        if source["path"] in superseded_g2_4y:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4z = baseline["phase_g2_4z_etf_cli_dynamic_v3_candidate_observation"]
    assert phase_g2_4z["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4z = phase_g2_4z["migration"]
    assert migration_g2_4z["callback_count"] == 13
    assert migration_g2_4z["shortlist_is_manual_review_set"] is True
    assert migration_g2_4z["cluster_is_similarity_diagnostic"] is True
    assert migration_g2_4z["shadow_shortlist_writes_legacy_registry"] is False
    assert migration_g2_4z["monitoring_activation_is_enrollment"] is False
    assert migration_g2_4z["monitoring_active_means_position_advisory"] is False
    assert migration_g2_4z["automatic_candidate_promotion_allowed"] is False
    assert migration_g2_4z["official_weight_or_broker_mutation_allowed"] is False
    assert migration_g2_4z["legacy_root_lines_after"] == 28802
    assert migration_g2_4z["legacy_root_top_level_functions_after"] == 861
    assert migration_g2_4z["legacy_root_command_decorators_after"] == 822
    assert migration_g2_4z["python_module_count"] == 837
    assert phase_g2_4z["sources"]
    superseded_g2_4z = set(phase_g2_4z["superseded_source_paths"])
    for source in phase_g2_4z["sources"]:
        if source["path"] in superseded_g2_4z:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4aa = baseline["phase_g2_4aa_etf_cli_dynamic_v3_portfolio_intake"]
    assert phase_g2_4aa["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4aa = phase_g2_4aa["migration"]
    assert migration_g2_4aa["callback_count"] == 7
    assert migration_g2_4aa["snapshot_source_must_be_explicit"] is True
    assert migration_g2_4aa["normalization_infers_missing_positions"] is False
    assert migration_g2_4aa["intake_triggers_downstream_risk_chain"] is False
    assert migration_g2_4aa["normalized_pass_means_portfolio_approval"] is False
    assert migration_g2_4aa["official_weight_or_broker_mutation_allowed"] is False
    assert migration_g2_4aa["legacy_root_lines_after"] == 28604
    assert migration_g2_4aa["legacy_root_top_level_functions_after"] == 854
    assert migration_g2_4aa["legacy_root_command_decorators_after"] == 815
    assert migration_g2_4aa["python_module_count"] == 838
    assert phase_g2_4aa["sources"]
    superseded_g2_4aa = set(phase_g2_4aa["superseded_source_paths"])
    for source in phase_g2_4aa["sources"]:
        if source["path"] in superseded_g2_4aa:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4ab = baseline["phase_g2_4ab_etf_cli_dynamic_v3_portfolio_risk_controls"]
    assert phase_g2_4ab["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ab = phase_g2_4ab["migration"]
    assert migration_g2_4ab["callback_count"] == 9
    assert migration_g2_4ab["requires_explicit_upstream_artifacts"] is True
    assert migration_g2_4ab["runs_portfolio_intake"] is False
    assert migration_g2_4ab["builds_manual_execution_review"] is False
    assert migration_g2_4ab["recommended_action_is_execution_authorization"] is False
    assert migration_g2_4ab["source_or_policy_mutation_allowed"] is False
    assert migration_g2_4ab["official_weight_or_broker_mutation_allowed"] is False
    assert migration_g2_4ab["legacy_root_lines_after"] == 28335
    assert migration_g2_4ab["legacy_root_top_level_functions_after"] == 845
    assert migration_g2_4ab["legacy_root_command_decorators_after"] == 806
    assert migration_g2_4ab["python_module_count"] == 839
    assert phase_g2_4ab["sources"]
    superseded_g2_4ab = set(phase_g2_4ab["superseded_source_paths"])
    for source in phase_g2_4ab["sources"]:
        if source["path"] in superseded_g2_4ab:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4ac = baseline["phase_g2_4ac_etf_cli_dynamic_v3_manual_execution_review"]
    assert phase_g2_4ac["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ac = phase_g2_4ac["migration"]
    assert migration_g2_4ac["callback_count"] == 3
    assert migration_g2_4ac["requires_explicit_source_ids"] is True
    assert migration_g2_4ac["manual_execution_decision_is_owner_record"] is False
    assert migration_g2_4ac["runs_upstream_risk_controls"] is False
    assert migration_g2_4ac["records_owner_approval"] is False
    assert migration_g2_4ac["order_ticket_generation_allowed"] is False
    assert migration_g2_4ac["official_weight_or_broker_mutation_allowed"] is False
    assert migration_g2_4ac["legacy_root_lines_after"] == 28226
    assert migration_g2_4ac["legacy_root_top_level_functions_after"] == 842
    assert migration_g2_4ac["legacy_root_command_decorators_after"] == 803
    assert migration_g2_4ac["python_module_count"] == 840
    assert phase_g2_4ac["sources"]
    superseded_g2_4ac = set(phase_g2_4ac["superseded_source_paths"])
    for source in phase_g2_4ac["sources"]:
        if source["path"] in superseded_g2_4ac:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4ad = baseline["phase_g2_4ad_etf_cli_dynamic_v3_real_snapshot_intake"]
    assert phase_g2_4ad["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ad = phase_g2_4ad["migration"]
    assert migration_g2_4ad["callback_count"] == 5
    assert migration_g2_4ad["redaction_required_before_intake"] is True
    assert migration_g2_4ad["sensitive_broker_or_account_fields_allowed"] is False
    assert migration_g2_4ad["manual_snapshot_link_is_broker_sync"] is False
    assert migration_g2_4ad["runs_real_snapshot_dry_run"] is False
    assert migration_g2_4ad["real_portfolio_or_broker_mutation_allowed"] is False
    assert migration_g2_4ad["legacy_root_lines_after"] == 28115
    assert migration_g2_4ad["legacy_root_top_level_functions_after"] == 837
    assert migration_g2_4ad["legacy_root_command_decorators_after"] == 798
    assert migration_g2_4ad["python_module_count"] == 841
    assert phase_g2_4ad["sources"]
    superseded_g2_4ad = set(phase_g2_4ad["superseded_source_paths"])
    for source in phase_g2_4ad["sources"]:
        if source["path"] in superseded_g2_4ad:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4ae = baseline["phase_g2_4ae_etf_cli_dynamic_v3_real_snapshot_dry_run"]
    assert phase_g2_4ae["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ae = phase_g2_4ae["migration"]
    assert migration_g2_4ae["callback_count"] == 3
    assert migration_g2_4ae["explicit_operator_trigger_required"] is True
    assert migration_g2_4ae["writes_risk_control_and_review_artifacts"] is True
    assert migration_g2_4ae["runs_snapshot_intake"] is False
    assert migration_g2_4ae["creates_owner_decision_or_paper_action"] is False
    assert migration_g2_4ae["order_real_portfolio_or_broker_mutation_allowed"] is False
    assert migration_g2_4ae["legacy_root_lines_after"] == 28032
    assert migration_g2_4ae["legacy_root_top_level_functions_after"] == 834
    assert migration_g2_4ae["legacy_root_command_decorators_after"] == 795
    assert migration_g2_4ae["python_module_count"] == 842
    assert phase_g2_4ae["sources"]
    superseded_g2_4ae = set(phase_g2_4ae["superseded_source_paths"])
    for source in phase_g2_4ae["sources"]:
        if source["path"] in superseded_g2_4ae:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4af = baseline["phase_g2_4af_etf_cli_dynamic_v3_real_execution_owner_review"]
    assert phase_g2_4af["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4af = phase_g2_4af["migration"]
    assert migration_g2_4af["callback_count"] == 4
    assert migration_g2_4af["owner_decision_recording_allowed"] is True
    assert migration_g2_4af["pending_is_recordable_final_decision"] is False
    assert migration_g2_4af["sensitive_owner_notes_allowed"] is False
    assert migration_g2_4af["auto_applies_paper_action"] is False
    assert migration_g2_4af["order_portfolio_or_broker_mutation_allowed"] is False
    assert migration_g2_4af["legacy_root_lines_after"] == 27937
    assert migration_g2_4af["legacy_root_top_level_functions_after"] == 830
    assert migration_g2_4af["legacy_root_command_decorators_after"] == 791
    assert migration_g2_4af["python_module_count"] == 843
    assert phase_g2_4af["sources"]
    superseded_g2_4af = set(phase_g2_4af["superseded_source_paths"])
    for source in phase_g2_4af["sources"]:
        if source["path"] in superseded_g2_4af:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4ag = baseline["phase_g2_4ag_etf_cli_dynamic_v3_real_snapshot_paper_action"]
    assert phase_g2_4ag["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ag = phase_g2_4ag["migration"]
    assert migration_g2_4ag["callback_count"] == 3
    assert migration_g2_4ag["requires_validated_final_owner_decision"] is True
    assert migration_g2_4ag["pending_owner_decision_allowed"] is False
    assert migration_g2_4ag["content_derived_validation"] is True
    assert migration_g2_4ag["source_checksum_binding"] is True
    assert migration_g2_4ag["mutates_existing_paper_portfolio"] is False
    assert migration_g2_4ag["real_portfolio_order_or_broker_mutation_allowed"] is False
    assert migration_g2_4ag["legacy_root_lines_after"] == 27855
    assert migration_g2_4ag["legacy_root_top_level_functions_after"] == 827
    assert migration_g2_4ag["legacy_root_command_decorators_after"] == 788
    assert migration_g2_4ag["python_module_count"] == 844
    assert phase_g2_4ag["sources"]
    superseded_g2_4ag = set(phase_g2_4ag["superseded_source_paths"])
    for source in phase_g2_4ag["sources"]:
        if source["path"] in superseded_g2_4ag:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4ah = baseline["phase_g2_4ah_etf_cli_dynamic_v3_weekly_real_snapshot_review"]
    assert phase_g2_4ah["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ah = phase_g2_4ah["migration"]
    assert migration_g2_4ah["callback_count"] == 3
    assert migration_g2_4ah["week_ending_cutoff_enforced"] is True
    assert migration_g2_4ah["owner_chain_anchor"] is True
    assert migration_g2_4ah["cross_chain_latest_allowed"] is False
    assert migration_g2_4ah["source_checksum_and_inventory_binding"] is True
    assert migration_g2_4ah["content_derived_render_validation"] is True
    assert migration_g2_4ah["runs_upstream_workflow"] is False
    assert migration_g2_4ah["portfolio_order_or_broker_mutation_allowed"] is False
    assert migration_g2_4ah["legacy_root_lines_after"] == 27770
    assert migration_g2_4ah["legacy_root_top_level_functions_after"] == 824
    assert migration_g2_4ah["legacy_root_command_decorators_after"] == 785
    assert migration_g2_4ah["python_module_count"] == 845
    assert phase_g2_4ah["sources"]
    superseded_g2_4ah = set(phase_g2_4ah["superseded_source_paths"])
    for source in phase_g2_4ah["sources"]:
        if source["path"] in superseded_g2_4ah:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4ai = baseline["phase_g2_4ai_etf_cli_dynamic_v3_position_advisory"]
    assert phase_g2_4ai["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ai = phase_g2_4ai["migration"]
    assert migration_g2_4ai["callback_count"] == 3
    assert migration_g2_4ai["requires_validated_shadow_shortlist"] is True
    assert migration_g2_4ai["requires_complete_candidate_weight_paths"] is True
    assert migration_g2_4ai["source_checksum_binding"] is True
    assert migration_g2_4ai["content_derived_validation"] is True
    assert migration_g2_4ai["snapshot_optional_target_only"] is True
    assert migration_g2_4ai["advisory_is_execution_authorization"] is False
    assert migration_g2_4ai["portfolio_order_or_broker_mutation_allowed"] is False
    assert migration_g2_4ai["legacy_root_lines_after"] == 27682
    assert migration_g2_4ai["legacy_root_top_level_functions_after"] == 821
    assert migration_g2_4ai["legacy_root_command_decorators_after"] == 782
    assert migration_g2_4ai["python_module_count"] == 846
    assert phase_g2_4ai["sources"]
    superseded_g2_4ai = set(phase_g2_4ai["superseded_source_paths"])
    for source in phase_g2_4ai["sources"]:
        if source["path"] in superseded_g2_4ai:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4aj = baseline["phase_g2_4aj_etf_cli_dynamic_v3_position_advisory_daily"]
    assert phase_g2_4aj["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4aj = phase_g2_4aj["migration"]
    assert migration_g2_4aj["callback_count"] == 3
    assert migration_g2_4aj["requires_validated_monitor"] is True
    assert migration_g2_4aj["candidate_weight_invariants_required"] is True
    assert migration_g2_4aj["agreement_uses_policy_tolerance"] is True
    assert migration_g2_4aj["filesystem_mtime_selection_allowed"] is False
    assert migration_g2_4aj["same_chain_drift_validation_required"] is True
    assert migration_g2_4aj["future_snapshot_allowed"] is False
    assert migration_g2_4aj["source_checksum_and_inventory_binding"] is True
    assert migration_g2_4aj["content_derived_render_validation"] is True
    assert migration_g2_4aj["advisory_is_execution_authorization"] is False
    assert migration_g2_4aj["legacy_root_lines_after"] == 27580
    assert migration_g2_4aj["legacy_root_top_level_functions_after"] == 818
    assert migration_g2_4aj["legacy_root_command_decorators_after"] == 779
    assert migration_g2_4aj["python_module_count"] == 847
    assert phase_g2_4aj["sources"]
    superseded_g2_4aj = set(phase_g2_4aj["superseded_source_paths"])
    for source in phase_g2_4aj["sources"]:
        if source["path"] in superseded_g2_4aj:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4ak = baseline["phase_g2_4ak_etf_cli_dynamic_v3_consensus_drift"]
    assert phase_g2_4ak["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ak = phase_g2_4ak["migration"]
    assert migration_g2_4ak["callback_count"] == 3
    assert migration_g2_4ak["requires_validated_current_monitor"] is True
    assert migration_g2_4ak["candidate_weight_invariants_required"] is True
    assert migration_g2_4ak["generated_cutoff_enforced"] is True
    assert migration_g2_4ak["semantic_previous_monitor_selection"] is True
    assert migration_g2_4ak["filesystem_mtime_selection_allowed"] is False
    assert migration_g2_4ak["latest_relevant_invalid_fails_closed"] is True
    assert migration_g2_4ak["all_non_consensus_requires_manual_review"] is True
    assert migration_g2_4ak["previous_source_id_is_content_derived"] is True
    assert migration_g2_4ak["previous_delta_uses_symbol_union"] is True
    assert migration_g2_4ak["source_checksum_and_inventory_binding"] is True
    assert migration_g2_4ak["content_derived_render_validation"] is True
    assert migration_g2_4ak["drift_is_execution_authorization"] is False
    assert migration_g2_4ak["legacy_root_lines_after"] == 27497
    assert migration_g2_4ak["legacy_root_top_level_functions_after"] == 815
    assert migration_g2_4ak["legacy_root_command_decorators_after"] == 776
    assert migration_g2_4ak["python_module_count"] == 848
    assert phase_g2_4ak["sources"]
    superseded_g2_4ak = set(phase_g2_4ak["superseded_source_paths"])
    for source in phase_g2_4ak["sources"]:
        if source["path"] in superseded_g2_4ak:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4al = baseline["phase_g2_4al_etf_cli_dynamic_v3_owner_review_journal"]
    assert phase_g2_4al["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4al = phase_g2_4al["migration"]
    assert migration_g2_4al["callback_count"] == 5
    assert migration_g2_4al["requires_validated_daily_advisory"] is True
    assert migration_g2_4al["append_only_checksum_event_chain"] is True
    assert migration_g2_4al["single_final_decision_enforced"] is True
    assert migration_g2_4al["shared_owner_notes_privacy_gate"] is True
    assert migration_g2_4al["daily_source_checksum_binding"] is True
    assert migration_g2_4al["materialized_views_derived_from_events"] is True
    assert migration_g2_4al["legacy_unchained_mutation_allowed"] is False
    assert migration_g2_4al["paper_action_content_bound"] is True
    assert migration_g2_4al["content_derived_render_validation"] is True
    assert migration_g2_4al["owner_decision_is_execution_authorization"] is False
    assert migration_g2_4al["legacy_root_lines_after"] == 27373
    assert migration_g2_4al["legacy_root_top_level_functions_after"] == 810
    assert migration_g2_4al["legacy_root_command_decorators_after"] == 771
    assert migration_g2_4al["python_module_count"] == 850
    assert phase_g2_4al["sources"]
    superseded_g2_4al = set(phase_g2_4al["superseded_source_paths"])
    for source in phase_g2_4al["sources"]:
        if source["path"] in superseded_g2_4al:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4am = baseline["phase_g2_4am_etf_cli_dynamic_v3_paper_portfolio"]
    assert phase_g2_4am["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4am = phase_g2_4am["migration"]
    assert migration_g2_4am["callback_count"] == 5
    assert migration_g2_4am["requires_validated_initial_config_snapshot"] is True
    assert migration_g2_4am["append_only_checksum_event_chain"] is True
    assert migration_g2_4am["one_review_one_action"] is True
    assert migration_g2_4am["requires_validated_owner_review"] is True
    assert migration_g2_4am["requires_frozen_daily_source"] is True
    assert migration_g2_4am["manual_deltas_finite_zero_sum"] is True
    assert migration_g2_4am["policy_limited_content_replay"] is True
    assert migration_g2_4am["materialized_views_derived_from_events"] is True
    assert migration_g2_4am["legacy_unchained_mutation_allowed"] is False
    assert migration_g2_4am["paper_state_is_real_portfolio_mutation"] is False
    assert migration_g2_4am["legacy_root_lines_after"] == 27212
    assert migration_g2_4am["legacy_root_top_level_functions_after"] == 805
    assert migration_g2_4am["legacy_root_command_decorators_after"] == 766
    assert migration_g2_4am["python_module_count"] == 851
    assert phase_g2_4am["sources"]
    superseded_g2_4am = set(phase_g2_4am["superseded_source_paths"])
    for source in phase_g2_4am["sources"]:
        if source["path"] in superseded_g2_4am:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4an = baseline["phase_g2_4an_etf_cli_dynamic_v3_advisory_outcome"]
    assert phase_g2_4an["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4an = phase_g2_4an["migration"]
    assert migration_g2_4an["callback_count"] == 4
    assert migration_g2_4an["immutable_decision_time_event"] is True
    assert migration_g2_4an["append_only_checksum_update_chain"] is True
    assert migration_g2_4an["requires_validated_daily_and_paper_sources"] is True
    assert migration_g2_4an["future_paper_action_lookahead_allowed"] is False
    assert migration_g2_4an["required_symbol_complete_date_windows"] is True
    assert migration_g2_4an["fixed_share_and_piecewise_paths"] is True
    assert migration_g2_4an["transaction_and_slippage_costs_applied"] is True
    assert migration_g2_4an["non_available_metrics_are_null"] is True
    assert migration_g2_4an["content_derived_source_snapshot_replay"] is True
    assert migration_g2_4an["legacy_unchained_update_allowed"] is False
    assert migration_g2_4an["portfolio_or_execution_effect"] is False
    assert migration_g2_4an["legacy_root_lines_after"] == 27086
    assert migration_g2_4an["legacy_root_top_level_functions_after"] == 801
    assert migration_g2_4an["legacy_root_command_decorators_after"] == 762
    assert migration_g2_4an["python_module_count"] == 852
    assert phase_g2_4an["sources"]
    superseded_g2_4an = set(phase_g2_4an["superseded_source_paths"])
    for source in phase_g2_4an["sources"]:
        if source["path"] in superseded_g2_4an:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4ao = baseline["phase_g2_4ao_etf_cli_dynamic_v3_owner_attribution"]
    assert phase_g2_4ao["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ao = phase_g2_4ao["migration"]
    assert migration_g2_4ao["callback_count"] == 3
    assert migration_g2_4ao["requires_validated_owner_reviews"] is True
    assert migration_g2_4ao["requires_validated_advisory_outcomes"] is True
    assert migration_g2_4ao["generated_cutoff_enforced"] is True
    assert migration_g2_4ao["one_daily_zero_or_one_outcome"] is True
    assert migration_g2_4ao["immutable_source_snapshots"] is True
    assert migration_g2_4ao["review_outcome_window_units_separated"] is True
    assert migration_g2_4ao["missing_horizon_metrics_are_null"] is True
    assert migration_g2_4ao["content_derived_snapshot_validation"] is True
    assert migration_g2_4ao["legacy_unsnapshotted_is_current_evidence"] is False
    assert migration_g2_4ao["attribution_is_causal_evidence"] is False
    assert migration_g2_4ao["portfolio_or_execution_effect"] is False
    assert migration_g2_4ao["legacy_root_lines_after"] == 27004
    assert migration_g2_4ao["legacy_root_top_level_functions_after"] == 798
    assert migration_g2_4ao["legacy_root_command_decorators_after"] == 759
    assert migration_g2_4ao["python_module_count"] == 853
    assert phase_g2_4ao["sources"]
    superseded_g2_4ao = set(phase_g2_4ao["superseded_source_paths"])
    for source in phase_g2_4ao["sources"]:
        if source["path"] in superseded_g2_4ao:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4ap = baseline["phase_g2_4ap_etf_cli_dynamic_v3_shadow_aging"]
    assert phase_g2_4ap["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ap = phase_g2_4ap["migration"]
    assert migration_g2_4ap["callback_count"] == 3
    assert migration_g2_4ap["requires_validated_shortlist"] is True
    assert migration_g2_4ap["requires_validated_monitor_drift_outcome"] is True
    assert migration_g2_4ap["generated_cutoff_enforced"] is True
    assert migration_g2_4ap["duplicate_source_binding_allowed"] is False
    assert migration_g2_4ap["immutable_source_snapshot"] is True
    assert migration_g2_4ap["true_weight_change_rebalance_count"] is True
    assert migration_g2_4ap["candidate_specific_outcome_replay"] is True
    assert migration_g2_4ap["missing_outcome_score_is_null"] is True
    assert migration_g2_4ap["reviewed_policy_thresholds"] is True
    assert migration_g2_4ap["selective_outcome_update_event_bound"] is True
    assert migration_g2_4ap["eligible_is_automatic_promotion"] is False
    assert migration_g2_4ap["portfolio_or_execution_effect"] is False
    assert migration_g2_4ap["legacy_root_lines_after"] == 26911
    assert migration_g2_4ap["legacy_root_top_level_functions_after"] == 795
    assert migration_g2_4ap["legacy_root_command_decorators_after"] == 756
    assert migration_g2_4ap["python_module_count"] == 854
    assert phase_g2_4ap["sources"]
    superseded_g2_4ap = set(phase_g2_4ap["superseded_source_paths"])
    for source in phase_g2_4ap["sources"]:
        if source["path"] in superseded_g2_4ap:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4aq = baseline["phase_g2_4aq_etf_cli_dynamic_v3_weekly_advisory_review"]
    assert phase_g2_4aq["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4aq = phase_g2_4aq["migration"]
    assert migration_g2_4aq["callback_count"] == 3
    assert migration_g2_4aq["calendar_week_and_generated_cutoff_enforced"] is True
    assert migration_g2_4aq["validated_daily_monitor_anchor"] is True
    assert migration_g2_4aq["owner_paper_outcome_cutoff_prefix_replay"] is True
    assert migration_g2_4aq["ambiguous_source_binding_allowed"] is False
    assert migration_g2_4aq["immutable_source_snapshot"] is True
    assert migration_g2_4aq["missing_outcome_metrics_are_null"] is True
    assert migration_g2_4aq["reviewed_policy_coverage_and_precedence"] is True
    assert migration_g2_4aq["content_derived_all_views_validation"] is True
    assert migration_g2_4aq["independent_scheduler_added"] is False
    assert migration_g2_4aq["portfolio_or_execution_effect"] is False
    assert migration_g2_4aq["legacy_root_lines_after"] == 26802
    assert migration_g2_4aq["legacy_root_top_level_functions_after"] == 792
    assert migration_g2_4aq["legacy_root_command_decorators_after"] == 753
    assert migration_g2_4aq["python_module_count"] == 855
    assert phase_g2_4aq["sources"]
    superseded_g2_4aq = set(phase_g2_4aq["superseded_source_paths"])
    for source in phase_g2_4aq["sources"]:
        if source["path"] in superseded_g2_4aq:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4ar = baseline["phase_g2_4ar_etf_cli_dynamic_v3_replay_inventory"]
    assert phase_g2_4ar["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ar = phase_g2_4ar["migration"]
    assert migration_g2_4ar["callback_count"] == 3
    assert migration_g2_4ar["valid_range_and_generated_cutoff_enforced"] is True
    assert migration_g2_4ar["ambiguous_daily_or_cutoff_binding_allowed"] is False
    assert migration_g2_4ar["semantic_drift_selection_without_mtime"] is True
    assert migration_g2_4ar["immutable_source_snapshot"] is True
    assert migration_g2_4ar["price_is_outcome_availability_only"] is True
    assert migration_g2_4ar["hard_pit_limitations_ineligible"] is True
    assert migration_g2_4ar["content_derived_all_views_validation"] is True
    assert migration_g2_4ar["legacy_missing_artifact_accepted"] is False
    assert migration_g2_4ar["historical_replay_or_backfill_executed"] is False
    assert migration_g2_4ar["portfolio_or_execution_effect"] is False
    assert migration_g2_4ar["legacy_root_lines_after"] == 26686
    assert migration_g2_4ar["legacy_root_top_level_functions_after"] == 789
    assert migration_g2_4ar["legacy_root_command_decorators_after"] == 750
    assert migration_g2_4ar["python_module_count"] == 856
    assert phase_g2_4ar["sources"]
    superseded_g2_4ar = set(phase_g2_4ar["superseded_source_paths"])
    for source in phase_g2_4ar["sources"]:
        if source["path"] in superseded_g2_4ar:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4as = baseline["phase_g2_4as_etf_cli_dynamic_v3_historical_replay"]
    assert phase_g2_4as["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4as = phase_g2_4as["migration"]
    assert migration_g2_4as["callback_count"] == 3
    assert migration_g2_4as["requires_full_validated_inventory"] is True
    assert migration_g2_4as["generated_cutoff_ordering_enforced"] is True
    assert migration_g2_4as["hard_pit_override_allowed"] is False
    assert migration_g2_4as["weight_simplex_enforced"] is True
    assert migration_g2_4as["fallback_source_status_explicit"] is True
    assert migration_g2_4as["one_way_l1_turnover_explicit"] is True
    assert migration_g2_4as["immutable_source_snapshot"] is True
    assert migration_g2_4as["content_derived_all_views_validation"] is True
    assert migration_g2_4as["outcome_price_read"] is False
    assert migration_g2_4as["portfolio_or_execution_effect"] is False
    assert migration_g2_4as["legacy_root_lines_after"] == 26602
    assert migration_g2_4as["legacy_root_top_level_functions_after"] == 786
    assert migration_g2_4as["legacy_root_command_decorators_after"] == 747
    assert migration_g2_4as["python_module_count"] == 857
    assert phase_g2_4as["sources"]
    superseded_g2_4as = set(phase_g2_4as.get("superseded_source_paths", []))
    for source in phase_g2_4as["sources"]:
        if source["path"] in superseded_g2_4as:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4at = baseline["phase_g2_4at_etf_cli_dynamic_v3_backfilled_outcome"]
    assert phase_g2_4at["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4at = phase_g2_4at["migration"]
    assert migration_g2_4at["callback_count"] == 3
    assert migration_g2_4at["requires_full_validated_replay"] is True
    assert migration_g2_4at["generated_cutoff_ordering_enforced"] is True
    assert migration_g2_4at["cached_data_quality_gate_required"] is True
    assert migration_g2_4at["non_available_metrics_are_null"] is True
    assert migration_g2_4at["fixed_share_path_explicit"] is True
    assert migration_g2_4at["versioned_initial_turnover_cost_explicit"] is True
    assert migration_g2_4at["immutable_source_snapshot"] is True
    assert migration_g2_4at["content_derived_all_views_validation"] is True
    assert migration_g2_4at["portfolio_or_execution_effect"] is False
    assert migration_g2_4at["legacy_root_lines_after"] == 26504
    assert migration_g2_4at["legacy_root_top_level_functions_after"] == 783
    assert migration_g2_4at["legacy_root_command_decorators_after"] == 744
    assert migration_g2_4at["python_module_count"] == 858
    assert phase_g2_4at["sources"]
    superseded_g2_4at = set(phase_g2_4at.get("superseded_source_paths", []))
    for source in phase_g2_4at["sources"]:
        if source["path"] in superseded_g2_4at:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4au = baseline["phase_g2_4au_etf_cli_dynamic_v3_historical_paper_sim"]
    assert phase_g2_4au["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4au = phase_g2_4au["migration"]
    assert migration_g2_4au["callback_count"] == 3
    assert migration_g2_4au["requires_full_validated_replay"] is True
    assert migration_g2_4au["cached_data_quality_gate_required"] is True
    assert migration_g2_4au["fixed_share_total_and_risk_path_consistent"] is True
    assert migration_g2_4au["missing_price_return_zero_allowed"] is False
    assert migration_g2_4au["event_target_reset_cost_explicit"] is True
    assert migration_g2_4au["immutable_source_snapshot"] is True
    assert migration_g2_4au["content_derived_all_views_validation"] is True
    assert migration_g2_4au["portfolio_or_execution_effect"] is False
    assert migration_g2_4au["legacy_root_lines_after"] == 26414
    assert migration_g2_4au["legacy_root_top_level_functions_after"] == 780
    assert migration_g2_4au["legacy_root_command_decorators_after"] == 741
    assert migration_g2_4au["python_module_count"] == 859
    assert phase_g2_4au["sources"]
    superseded_g2_4au = set(phase_g2_4au.get("superseded_source_paths", []))
    for source in phase_g2_4au["sources"]:
        if source["path"] in superseded_g2_4au:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4av = baseline["phase_g2_4av_etf_cli_dynamic_v3_replay_performance_review"]
    assert phase_g2_4av["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4av = phase_g2_4av["migration"]
    assert migration_g2_4av["callback_count"] == 3
    assert migration_g2_4av["requires_full_validated_backfill_and_simulation"] is True
    assert migration_g2_4av["same_replay_and_time_ordering_enforced"] is True
    assert migration_g2_4av["unsupported_classification_metrics_are_null"] is True
    assert migration_g2_4av["reviewed_sample_floor_policy_required"] is True
    assert migration_g2_4av["automatic_config_or_promotion_allowed"] is False
    assert migration_g2_4av["immutable_source_snapshot"] is True
    assert migration_g2_4av["content_derived_all_views_validation"] is True
    assert migration_g2_4av["portfolio_or_execution_effect"] is False
    assert migration_g2_4av["legacy_root_lines_after"] == 26322
    assert migration_g2_4av["legacy_root_top_level_functions_after"] == 777
    assert migration_g2_4av["legacy_root_command_decorators_after"] == 738
    assert migration_g2_4av["python_module_count"] == 860
    assert phase_g2_4av["sources"]
    superseded_g2_4av = set(phase_g2_4av.get("superseded_source_paths", []))
    for source in phase_g2_4av["sources"]:
        if source["path"] in superseded_g2_4av:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4aw = baseline["phase_g2_4aw_etf_cli_dynamic_v3_replay_diagnosis"]
    assert phase_g2_4aw["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4aw = phase_g2_4aw["migration"]
    assert migration_g2_4aw["callback_count"] == 3
    assert migration_g2_4aw["requires_five_full_validated_sources"] is True
    assert migration_g2_4aw["full_lineage_and_time_ordering_enforced"] is True
    assert migration_g2_4aw["unit_aware_pending_reasons"] is True
    assert migration_g2_4aw["healthy_unknown_blocker_allowed"] is False
    assert migration_g2_4aw["reviewed_comparison_readiness_gate"] is True
    assert migration_g2_4aw["immutable_source_snapshot"] is True
    assert migration_g2_4aw["content_derived_all_views_validation"] is True
    assert migration_g2_4aw["repair_or_calibration_executed"] is False
    assert migration_g2_4aw["portfolio_or_execution_effect"] is False
    assert migration_g2_4aw["legacy_root_lines_after"] == 26200
    assert migration_g2_4aw["legacy_root_top_level_functions_after"] == 774
    assert migration_g2_4aw["legacy_root_command_decorators_after"] == 735
    assert migration_g2_4aw["python_module_count"] == 861
    assert phase_g2_4aw["sources"]
    superseded_g2_4aw = set(phase_g2_4aw.get("superseded_source_paths", []))
    for source in phase_g2_4aw["sources"]:
        if source["path"] in superseded_g2_4aw:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4ax = baseline["phase_g2_4ax_etf_cli_dynamic_v3_backfill_repair"]
    assert phase_g2_4ax["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ax = phase_g2_4ax["migration"]
    assert migration_g2_4ax["callback_count"] == 3
    assert migration_g2_4ax["requires_three_full_validated_sources"] is True
    assert migration_g2_4ax["full_lineage_and_time_ordering_enforced"] is True
    assert migration_g2_4ax["cached_data_quality_gate_required"] is True
    assert migration_g2_4ax["original_available_rows_immutable"] is True
    assert migration_g2_4ax["repair_count_unit"] == "event_variant_window"
    assert migration_g2_4ax["immutable_source_snapshot"] is True
    assert migration_g2_4ax["content_derived_all_views_validation"] is True
    assert migration_g2_4ax["comparison_or_calibration_executed"] is False
    assert migration_g2_4ax["portfolio_or_execution_effect"] is False
    assert migration_g2_4ax["legacy_root_lines_after"] == 26099
    assert migration_g2_4ax["legacy_root_top_level_functions_after"] == 771
    assert migration_g2_4ax["legacy_root_command_decorators_after"] == 732
    assert migration_g2_4ax["python_module_count"] == 862
    assert phase_g2_4ax["sources"]
    superseded_g2_4ax = set(phase_g2_4ax.get("superseded_source_paths", []))
    for source in phase_g2_4ax["sources"]:
        if source["path"] in superseded_g2_4ax:
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4ay = baseline["phase_g2_4ay_etf_cli_dynamic_v3_variant_comparison"]
    assert phase_g2_4ay["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ay = phase_g2_4ay["migration"]
    assert migration_g2_4ay["callback_count"] == 3
    assert migration_g2_4ay["requires_validated_backfill_and_optional_repair"] is True
    assert migration_g2_4ay["full_lineage_and_time_ordering_enforced"] is True
    assert migration_g2_4ay["duplicate_variant_window_keys_allowed"] is False
    assert migration_g2_4ay["missing_metrics_are_null"] is True
    assert migration_g2_4ay["same_event_primary_window_ranking"] is True
    assert migration_g2_4ay["reviewed_sample_floor_policy_required"] is True
    assert migration_g2_4ay["immutable_source_snapshot"] is True
    assert migration_g2_4ay["content_derived_all_views_validation"] is True
    assert migration_g2_4ay["automatic_calibration_allowed"] is False
    assert migration_g2_4ay["portfolio_or_execution_effect"] is False
    assert migration_g2_4ay["legacy_root_lines_after"] == 26007
    assert migration_g2_4ay["legacy_root_top_level_functions_after"] == 768
    assert migration_g2_4ay["legacy_root_command_decorators_after"] == 729
    assert migration_g2_4ay["python_module_count"] == 863
    assert phase_g2_4ay["sources"]
    for source in phase_g2_4ay["sources"]:
        if source["path"] in set(phase_g2_4ay.get("superseded_source_paths", [])):
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4az = baseline["phase_g2_4az_etf_cli_dynamic_v3_rule_calibration"]
    assert phase_g2_4az["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4az = phase_g2_4az["migration"]
    assert migration_g2_4az["callback_count"] == 3
    assert migration_g2_4az["requires_validated_variant_comparison"] is True
    assert migration_g2_4az["source_and_policy_snapshot_required"] is True
    assert migration_g2_4az["evidence_action_is_policy_proposal"] is False
    assert migration_g2_4az["insufficient_data_policy_change_allowed"] is False
    assert migration_g2_4az["directional_missing_metrics_are_null"] is True
    assert migration_g2_4az["content_derived_all_views_validation"] is True
    assert migration_g2_4az["automatic_policy_apply_allowed"] is False
    assert migration_g2_4az["portfolio_or_execution_effect"] is False
    assert migration_g2_4az["legacy_root_lines_after"] == 25921
    assert migration_g2_4az["legacy_root_top_level_functions_after"] == 765
    assert migration_g2_4az["legacy_root_command_decorators_after"] == 726
    assert migration_g2_4az["python_module_count"] == 864
    assert phase_g2_4az["sources"]
    for source in phase_g2_4az["sources"]:
        if source["path"] in set(phase_g2_4az.get("superseded_source_paths", [])):
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4ba = baseline["phase_g2_4ba_etf_cli_dynamic_v3_replay_forward_bridge"]
    assert phase_g2_4ba["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ba = phase_g2_4ba["migration"]
    assert migration_g2_4ba["callback_count"] == 3
    assert migration_g2_4ba["requires_three_content_derived_sources"] is True
    assert migration_g2_4ba["full_lineage_and_time_ordering_enforced"] is True
    assert migration_g2_4ba["source_and_policy_snapshot_required"] is True
    assert migration_g2_4ba["evidence_action_is_policy_proposal"] is False
    assert migration_g2_4ba["unknown_reason_injected_when_empty"] is False
    assert migration_g2_4ba["content_derived_all_views_validation"] is True
    assert migration_g2_4ba["automatic_upstream_or_policy_apply_allowed"] is False
    assert migration_g2_4ba["portfolio_or_execution_effect"] is False
    assert migration_g2_4ba["legacy_root_lines_after"] == 25823
    assert migration_g2_4ba["legacy_root_top_level_functions_after"] == 762
    assert migration_g2_4ba["legacy_root_command_decorators_after"] == 723
    assert migration_g2_4ba["python_module_count"] == 865
    assert phase_g2_4ba["sources"]
    for source in phase_g2_4ba["sources"]:
        if source["path"] in set(phase_g2_4ba.get("superseded_source_paths", [])):
            continue
        actual = hashlib.sha256(Path(source["path"]).read_bytes()).hexdigest()
        assert actual == source["sha256"], source["path"]
    phase_g2_4bb = baseline["phase_g2_4bb_etf_cli_dynamic_v3_outcome_due"]
    assert phase_g2_4bb["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bb = phase_g2_4bb["migration"]
    assert migration_g2_4bb["callback_count"] == 4
    assert migration_g2_4bb["pre_output_data_quality_and_source_validation"] is True
    assert migration_g2_4bb["duplicate_daily_window_allowed"] is False
    assert migration_g2_4bb["cutoff_price_date_snapshot_required"] is True
    assert migration_g2_4bb["content_derived_scan_validation"] is True
    assert migration_g2_4bb["update_ready_single_use"] is True
    assert migration_g2_4bb["allowed_window_days_explicit"] is True
    assert migration_g2_4bb["not_due_or_price_missing_update_allowed"] is False
    assert migration_g2_4bb["portfolio_or_execution_effect"] is False
    assert migration_g2_4bb["legacy_root_lines_after"] == 25692
    assert migration_g2_4bb["legacy_root_top_level_functions_after"] == 758
    assert migration_g2_4bb["legacy_root_command_decorators_after"] == 719
    assert migration_g2_4bb["python_module_count"] == 866
    assert phase_g2_4bb["sources"]
    for source in phase_g2_4bb["sources"]:
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

    assert attribution["status"] in {
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AL_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AL_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AM_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AM_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AN_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AN_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AO_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AO_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AP_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AP_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AQ_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AQ_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AR_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AR_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AS_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AS_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AT_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AT_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AU_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AU_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AV_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AV_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AW_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AW_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AX_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AX_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AY_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AY_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AZ_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4AZ_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BA_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BA_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BB_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BB_COMPLETE_G2_4_CONTINUES",
    }
    excluded = set(attribution["excluded_user_or_other_task_paths"])
    assert excluded == {
        "docs/requirements/ARCH-004G2_Parallel_Readiness_Gate.md",
        "docs/requirements/ARCH-004H_Cutover_and_Legacy_Removal.md",
        "docs/requirements/ARCH-005_Parallel_Development_Control_Plane.md",
        "docs/requirements/DATA-GOV-001_Unified_Data_Foundation_Governance.md",
        "docs/requirements/KNOWLEDGE-001_Knowledge_Insight_and_Multi_Carrier_Publishing.md",
        "docs/requirements/PLATFORM-UX-001_System_Understanding_Workbench.md",
        "docs/research/growth_tilt_owner_decision_resolution.md",
        "docs/research/indicator_family_only_model_review.md",
        "docs/research/layer1_selector_pause_or_continue_owner_pack.md",
    }
    assert attribution["staging_rule"]["exclude_user_or_other_task_paths"] is True
    assert attribution["safety_boundary"]["user_changes_preserved"] is True
