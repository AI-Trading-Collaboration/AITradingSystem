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


def _source_sha256(source: dict[str, object]) -> str:
    payload = Path(str(source["path"])).read_bytes()
    normalization = source.get("hash_normalization")
    if normalization == "git_eol_lf":
        payload = payload.replace(b"\r\n", b"\n")
    elif normalization is not None:
        raise AssertionError(f"unsupported hash normalization: {normalization}")
    return hashlib.sha256(payload).hexdigest()


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
    assert baseline["checkout_hash_normalization"] == {
        "schema_version": "arch_004_checkout_hash_normalization.v1",
        "status": "PASS",
        "policy": "git_eol_lf",
        "normalized_source_count": 6,
        "source_content_changed": False,
        "production_effect": "none",
        "paths": [
            "docs/artifact_catalog.md",
            "src/ai_trading_system/cli_commands/reports.py",
            "src/ai_trading_system/cli_commands/research_execution_common.py",
            "src/ai_trading_system/etf_portfolio/dynamic_v3_system_target.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/"
            "dynamic_v3_system_target_smoothed_freshness.py",
            "tests/test_cli_direct.py",
        ],
    }
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
    execution_cli_source = next(
        source
        for source in baseline["frozen_sources"]
        if source["contract_id"] == "execution_cli_date_adapters_after_arch_004a1"
    )
    assert execution_cli_source["hash_normalization"] == "git_eol_lf"
    assert execution_cli_source["previous_worktree_sha256"] == (
        "188c9318e0a530a0b269b8c11bb6bd594f51bb96a8650b95cd1433750a48cbcc"
    )
    for source in baseline["frozen_sources"]:
        if source.get("historical_phase_a_hash"):
            assert source["superseded_by_phase"] in {
                "ARCH-004D",
                "ARCH-004F1",
                "ARCH-004G2.4P",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
            assert source["superseded_by_phase"] in {
                "ARCH-004F3",
                "ARCH-004G1",
                "TRADING-2443",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
                "ARCH-004G2.4_EB0",
                "ARCH-005-PB1",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
                "TRADING-2443",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
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
                "TRADING-2444",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
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
                "ARCH-004G2.4_EB0",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
            assert source["superseded_by_phase"] in {
                "ARCH-004G2.2",
                "ARCH-004G2.4CX1",
                "ARCH-004G2_EB0_S2C",
            }
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
            assert source["superseded_by_phase"] in {"ARCH-004G2.3D", "ARCH-004G2.4CM"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
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
            assert source["superseded_by_phase"] in {"ARCH-004G2.3E", "ARCH-004G2.4CM"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
            assert source["superseded_by_phase"] in {
                "ARCH-004G2.4D",
                "ARCH-004G2.4-EB6",
            }
            expected_current = (
                "phase_g2_4eb6_weight_calibration_and_research_interfaces.sources"
                if source["superseded_by_phase"] == "ARCH-004G2.4-EB6"
                else "phase_g2_4d_etf_cli_dynamic_calibration.sources"
            )
            assert source["current_hash_tracked_in"] == expected_current
            continue
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
            assert source["superseded_by_phase"] in {"ARCH-004G2.4I", "ARCH-004G2.4BG"}
            assert str(source["current_hash_tracked_in"]).endswith(".sources")
            continue
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        actual = _source_sha256(source)
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
        if source["path"] in set(phase_g2_4bb.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4bc = baseline["phase_g2_4bc_etf_cli_dynamic_v3_replay_sample_expansion"]
    assert phase_g2_4bc["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bc = phase_g2_4bc["migration"]
    assert migration_g2_4bc["callback_count"] == 3
    assert migration_g2_4bc["pre_output_range_time_data_quality_gate"] is True
    assert migration_g2_4bc["daily_owner_replay_full_validation"] is True
    assert migration_g2_4bc["duplicate_or_conflicting_event_allowed"] is False
    assert migration_g2_4bc["source_policy_price_snapshot_required"] is True
    assert migration_g2_4bc["pit_safety_separate_from_price_evaluability"] is True
    assert migration_g2_4bc["content_derived_all_views_validation"] is True
    assert migration_g2_4bc["automatic_replay_execution_allowed"] is False
    assert migration_g2_4bc["portfolio_or_execution_effect"] is False
    assert migration_g2_4bc["legacy_root_lines_after"] == 25589
    assert migration_g2_4bc["legacy_root_top_level_functions_after"] == 755
    assert migration_g2_4bc["legacy_root_command_decorators_after"] == 716
    assert migration_g2_4bc["python_module_count"] == 867
    assert phase_g2_4bc["sources"]
    for source in phase_g2_4bc["sources"]:
        if source["path"] in set(phase_g2_4bc.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4bd = baseline["phase_g2_4bd_etf_cli_dynamic_v3_outcome_dashboard"]
    assert phase_g2_4bd["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bd = phase_g2_4bd["migration"]
    assert migration_g2_4bd["callback_count"] == 3
    assert migration_g2_4bd["all_selected_sources_content_validated"] is True
    assert migration_g2_4bd["semantic_latest_and_cutoff_enforced"] is True
    assert migration_g2_4bd["duplicate_or_cross_lineage_sample_allowed"] is False
    assert migration_g2_4bd["source_and_pending_policy_snapshot_required"] is True
    assert migration_g2_4bd["mode_specific_sample_units_explicit"] is True
    assert migration_g2_4bd["content_derived_all_views_validation"] is True
    assert migration_g2_4bd["automatic_upstream_run_allowed"] is False
    assert migration_g2_4bd["portfolio_or_execution_effect"] is False
    assert migration_g2_4bd["legacy_root_lines_after"] == 25492
    assert migration_g2_4bd["legacy_root_top_level_functions_after"] == 752
    assert migration_g2_4bd["legacy_root_command_decorators_after"] == 713
    assert migration_g2_4bd["python_module_count"] == 868
    assert phase_g2_4bd["sources"]
    for source in phase_g2_4bd["sources"]:
        if source["path"] in set(phase_g2_4bd.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4be = baseline["phase_g2_4be_etf_cli_dynamic_v3_limited_vs_notrade"]
    assert phase_g2_4be["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4be = phase_g2_4be["migration"]
    assert migration_g2_4be["callback_count"] == 3
    assert migration_g2_4be["all_selected_sources_content_validated"] is True
    assert migration_g2_4be["semantic_latest_and_cutoff_enforced"] is True
    assert migration_g2_4be["strict_unique_pairing_required"] is True
    assert migration_g2_4be["missing_metrics_remain_null"] is True
    assert migration_g2_4be["reviewed_policy_snapshot_required"] is True
    assert migration_g2_4be["real_regime_labels_only"] is True
    assert migration_g2_4be["content_derived_all_views_validation"] is True
    assert migration_g2_4be["automatic_policy_apply_allowed"] is False
    assert migration_g2_4be["portfolio_or_execution_effect"] is False
    assert migration_g2_4be["legacy_root_lines_after"] == 25399
    assert migration_g2_4be["legacy_root_top_level_functions_after"] == 749
    assert migration_g2_4be["legacy_root_command_decorators_after"] == 710
    assert migration_g2_4be["python_module_count"] == 869
    assert phase_g2_4be["sources"]
    for source in phase_g2_4be["sources"]:
        if source["path"] in set(phase_g2_4be.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4bf = baseline["phase_g2_4bf_etf_cli_dynamic_v3_consensus_risk"]
    assert phase_g2_4bf["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bf = phase_g2_4bf["migration"]
    assert migration_g2_4bf["callback_count"] == 3
    assert migration_g2_4bf["all_selected_sources_content_validated"] is True
    assert migration_g2_4bf["semantic_latest_cutoff_and_replay_lineage_enforced"] is True
    assert migration_g2_4bf["distinct_decision_date_exposure_required"] is True
    assert migration_g2_4bf["candidate_target_fallback_allowed"] is False
    assert migration_g2_4bf["strict_paired_drawdown_required"] is True
    assert migration_g2_4bf["distinct_event_turnover_required"] is True
    assert migration_g2_4bf["missing_metrics_remain_null"] is True
    assert migration_g2_4bf["reviewed_risk_policy_snapshot_required"] is True
    assert migration_g2_4bf["content_derived_all_views_validation"] is True
    assert migration_g2_4bf["default_execution_or_policy_apply_allowed"] is False
    assert migration_g2_4bf["portfolio_or_execution_effect"] is False
    assert migration_g2_4bf["legacy_root_lines_after"] == 25301
    assert migration_g2_4bf["legacy_root_top_level_functions_after"] == 746
    assert migration_g2_4bf["legacy_root_command_decorators_after"] == 707
    assert migration_g2_4bf["python_module_count"] == 870
    assert phase_g2_4bf["sources"]
    for source in phase_g2_4bf["sources"]:
        if source["path"] in set(phase_g2_4bf.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4bg = baseline["phase_g2_4bg_etf_cli_dynamic_v3_outcome_update_review"]
    assert phase_g2_4bg["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bg = phase_g2_4bg["migration"]
    assert migration_g2_4bg["callback_count"] == 3
    assert migration_g2_4bg["explicit_due_source_content_validated"] is True
    assert migration_g2_4bg["due_id_and_cutoff_enforced"] is True
    assert migration_g2_4bg["unique_outcome_window_identity_required"] is True
    assert migration_g2_4bg["ready_status_deterministically_derived"] is True
    assert migration_g2_4bg["no_future_data_date_proof_required"] is True
    assert migration_g2_4bg["full_due_bundle_snapshot_required"] is True
    assert migration_g2_4bg["content_derived_all_views_validation"] is True
    assert migration_g2_4bg["empty_artifact_is_insufficient_data"] is True
    assert migration_g2_4bg["outcome_update_or_data_refresh_allowed"] is False
    assert migration_g2_4bg["portfolio_or_execution_effect"] is False
    assert migration_g2_4bg["legacy_root_lines_after"] == 25221
    assert migration_g2_4bg["legacy_root_top_level_functions_after"] == 743
    assert migration_g2_4bg["legacy_root_command_decorators_after"] == 704
    assert migration_g2_4bg["python_module_count"] == 871
    assert phase_g2_4bg["sources"]
    for source in phase_g2_4bg["sources"]:
        if source["path"] in set(phase_g2_4bg.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4bh = baseline["phase_g2_4bh_etf_cli_dynamic_v3_outcome_update"]
    assert phase_g2_4bh["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bh = phase_g2_4bh["migration"]
    assert migration_g2_4bh["callback_count"] == 3
    assert migration_g2_4bh["explicit_review_content_validated"] is True
    assert migration_g2_4bh["single_committed_update_per_review"] is True
    assert migration_g2_4bh["unique_identity_and_live_pre_state_required"] is True
    assert migration_g2_4bh["isolated_full_batch_preflight_required"] is True
    assert migration_g2_4bh["transaction_states"] == [
        "PREPARED",
        "COMMITTED",
        "ROLLED_BACK",
    ]
    assert migration_g2_4bh["all_or_rollback_required"] is True
    assert migration_g2_4bh["full_review_pre_post_bundles_required"] is True
    assert migration_g2_4bh["selected_cohort_delta_required"] is True
    assert migration_g2_4bh["content_derived_all_views_validation"] is True
    assert migration_g2_4bh["automatic_downstream_refresh_allowed"] is False
    assert migration_g2_4bh["portfolio_or_execution_effect"] is False
    assert migration_g2_4bh["legacy_root_lines_after"] == 25111
    assert migration_g2_4bh["legacy_root_top_level_functions_after"] == 740
    assert migration_g2_4bh["legacy_root_command_decorators_after"] == 701
    assert migration_g2_4bh["python_module_count"] == 872
    assert phase_g2_4bh["sources"]
    for source in phase_g2_4bh["sources"]:
        if source["path"] in set(phase_g2_4bh.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4bi = baseline["phase_g2_4bi_etf_cli_dynamic_v3_rolling_evidence_refresh"]
    assert phase_g2_4bi["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bi = phase_g2_4bi["migration"]
    assert migration_g2_4bi["callback_count"] == 3
    assert migration_g2_4bi["explicit_committed_update_content_validated"] is True
    assert migration_g2_4bi["single_committed_refresh_per_update"] is True
    assert migration_g2_4bi["transaction_states"] == [
        "PREPARED",
        "COMMITTED",
        "ROLLED_BACK",
    ]
    assert migration_g2_4bi["partial_artifact_and_pointer_rollback_required"] is True
    assert migration_g2_4bi["all_downstream_content_validations_required"] is True
    assert migration_g2_4bi["full_update_baseline_post_bundles_required"] is True
    assert migration_g2_4bi["selected_cohort_forward_delta_required"] is True
    assert migration_g2_4bi["consumed_due_reuse_allowed"] is False
    assert migration_g2_4bi["reader_brief_section_is_global_update"] is False
    assert migration_g2_4bi["content_derived_all_views_validation"] is True
    assert migration_g2_4bi["portfolio_or_execution_effect"] is False
    assert migration_g2_4bi["legacy_root_lines_after"] == 25020
    assert migration_g2_4bi["legacy_root_top_level_functions_after"] == 737
    assert migration_g2_4bi["legacy_root_command_decorators_after"] == 698
    assert migration_g2_4bi["python_module_count"] == 873
    assert phase_g2_4bi["sources"]
    for source in phase_g2_4bi["sources"]:
        if source["path"] in set(phase_g2_4bi.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]
    phase_g2_4bj = baseline["phase_g2_4bj_etf_cli_dynamic_v3_evidence_trend"]
    assert phase_g2_4bj["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bj = phase_g2_4bj["migration"]
    assert migration_g2_4bj["callback_count"] == 3
    assert migration_g2_4bj["validated_committed_refreshes_only"] is True
    assert migration_g2_4bj["unique_refresh_and_update_identity_required"] is True
    assert migration_g2_4bj["excluded_refresh_reason_evidence_required"] is True
    assert migration_g2_4bj["prepared_or_invalid_committed_blocks"] is True
    assert migration_g2_4bj["full_refresh_and_policy_snapshot_required"] is True
    assert migration_g2_4bj["full_dashboard_state_comparison_required"] is True
    assert migration_g2_4bj["null_preserving_metrics_required"] is True
    assert migration_g2_4bj["reviewed_trend_policy_required"] is True
    assert migration_g2_4bj["content_derived_all_views_validation"] is True
    assert migration_g2_4bj["automatic_upstream_or_policy_apply_allowed"] is False
    assert migration_g2_4bj["portfolio_or_execution_effect"] is False
    assert migration_g2_4bj["legacy_root_lines_after"] == 24948
    assert migration_g2_4bj["legacy_root_top_level_functions_after"] == 734
    assert migration_g2_4bj["legacy_root_command_decorators_after"] == 695
    assert migration_g2_4bj["python_module_count"] == 874
    if phase_g2_4bj["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bj["sources"]
        for source in phase_g2_4bj["sources"]:
            if source["path"] in set(phase_g2_4bj.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bk = baseline["phase_g2_4bk_etf_cli_dynamic_v3_forward_outcome_decision"]
    assert phase_g2_4bk["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bk = phase_g2_4bk["migration"]
    assert migration_g2_4bk["callback_count"] == 3
    assert migration_g2_4bk["cutoff_bound_zero_or_one_source_selection"] is True
    assert migration_g2_4bk["explicit_source_ids_supported"] is True
    assert migration_g2_4bk["all_selected_sources_content_validated"] is True
    assert migration_g2_4bk["update_refresh_trend_lineage_required"] is True
    assert migration_g2_4bk["full_source_and_policy_snapshot_required"] is True
    assert migration_g2_4bk["missing_and_invalid_sources_distinct"] is True
    assert migration_g2_4bk["null_preserving_full_dashboard_state_required"] is True
    assert migration_g2_4bk["reviewed_decision_policy_required"] is True
    assert migration_g2_4bk["content_derived_all_views_validation"] is True
    assert migration_g2_4bk["automatic_upstream_or_policy_apply_allowed"] is False
    assert migration_g2_4bk["portfolio_or_execution_effect"] is False
    assert migration_g2_4bk["legacy_root_lines_after"] == 24861
    assert migration_g2_4bk["legacy_root_top_level_functions_after"] == 731
    assert migration_g2_4bk["legacy_root_command_decorators_after"] == 692
    assert migration_g2_4bk["python_module_count"] == 875
    if phase_g2_4bk["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bk["sources"]
        for source in phase_g2_4bk["sources"]:
            if source["path"] in set(phase_g2_4bk.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bl = baseline["phase_g2_4bl_etf_cli_dynamic_v3_backtest_sim_events"]
    assert phase_g2_4bl["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bl = phase_g2_4bl["migration"]
    assert migration_g2_4bl["callback_count"] == 4
    assert migration_g2_4bl["strict_governed_config_validation"] is True
    assert migration_g2_4bl["pre_output_timezone_range_and_data_quality_gate"] is True
    assert migration_g2_4bl["zero_partial_artifact_on_preflight_failure"] is True
    assert migration_g2_4bl["full_governed_source_snapshot_required"] is True
    assert migration_g2_4bl["cutoff_bound_price_rate_rows_required"] is True
    assert migration_g2_4bl["candidate_identity_and_source_validation_required"] is True
    assert migration_g2_4bl["legal_empty_schedule_is_insufficient_data"] is True
    assert migration_g2_4bl["content_derived_all_views_validation"] is True
    assert migration_g2_4bl["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bl["portfolio_or_execution_effect"] is False
    assert migration_g2_4bl["legacy_root_lines_after"] == 24761
    assert migration_g2_4bl["legacy_root_top_level_functions_after"] == 727
    assert migration_g2_4bl["legacy_root_command_decorators_after"] == 688
    assert migration_g2_4bl["python_module_count"] == 876
    if phase_g2_4bl["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bl["sources"]
        for source in phase_g2_4bl["sources"]:
            if source["path"] in set(phase_g2_4bl.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bm = baseline["phase_g2_4bm_etf_cli_dynamic_v3_backtest_sim_variants"]
    assert phase_g2_4bm["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bm = phase_g2_4bm["migration"]
    assert migration_g2_4bm["callback_count"] == 3
    assert migration_g2_4bm["pre_output_event_content_validation"] is True
    assert migration_g2_4bm["source_generated_cutoff_required"] is True
    assert migration_g2_4bm["full_event_config_validation_snapshot_required"] is True
    assert migration_g2_4bm["exact_enabled_variant_coverage_required"] is True
    assert migration_g2_4bm["unique_event_variant_identity_required"] is True
    assert migration_g2_4bm["state_weight_delta_turnover_invariants_required"] is True
    assert migration_g2_4bm["content_derived_all_views_validation"] is True
    assert migration_g2_4bm["automatic_outcome_or_paper_run_allowed"] is False
    assert migration_g2_4bm["portfolio_or_execution_effect"] is False
    assert migration_g2_4bm["legacy_root_lines_after"] == 24676
    assert migration_g2_4bm["legacy_root_top_level_functions_after"] == 724
    assert migration_g2_4bm["legacy_root_command_decorators_after"] == 685
    assert migration_g2_4bm["python_module_count"] == 877
    if phase_g2_4bm["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bm["sources"]
        for source in phase_g2_4bm["sources"]:
            if source["path"] in set(phase_g2_4bm.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bn = baseline["phase_g2_4bn_etf_cli_dynamic_v3_backtest_sim_outcome"]
    assert phase_g2_4bn["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bn = phase_g2_4bn["migration"]
    assert migration_g2_4bn["callback_count"] == 3
    assert migration_g2_4bn["canonical_owner"].endswith("dynamic_v3_backtest_sim_outcome.py")
    assert migration_g2_4bn["pre_output_variant_content_validation"] is True
    assert migration_g2_4bn["pre_output_data_quality_gate"] is True
    assert migration_g2_4bn["full_variant_cache_dq_snapshot_required"] is True
    assert migration_g2_4bn["unknown_metrics_must_be_null"] is True
    assert migration_g2_4bn["content_derived_all_views_validation"] is True
    assert migration_g2_4bn["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bn["legacy_root_lines_after"] == 24581
    assert migration_g2_4bn["legacy_root_top_level_functions_after"] == 721
    assert migration_g2_4bn["legacy_root_command_decorators_after"] == 682
    assert migration_g2_4bn["python_module_count"] == 878
    assert migration_g2_4bn["production_effect"] == "none"
    if phase_g2_4bn["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bn["validation"]["focused"]["passed"] == 398
        assert phase_g2_4bn["validation"]["architecture_fitness"]["passed"] == 249
        assert phase_g2_4bn["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bn["sources"]
        for source in phase_g2_4bn["sources"]:
            if source["path"] in set(phase_g2_4bn.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bo = baseline["phase_g2_4bo_etf_cli_dynamic_v3_backtest_sim_paper"]
    assert phase_g2_4bo["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bo = phase_g2_4bo["migration"]
    assert migration_g2_4bo["callback_count"] == 3
    assert migration_g2_4bo["canonical_owner"].endswith("dynamic_v3_backtest_sim_paper.py")
    assert migration_g2_4bo["pre_output_variant_content_validation"] is True
    assert migration_g2_4bo["pre_output_data_quality_gate"] is True
    assert migration_g2_4bo["full_variant_cache_dq_snapshot_required"] is True
    assert migration_g2_4bo["unknown_metrics_must_be_null"] is True
    assert migration_g2_4bo["gross_before_costs_disclosure_required"] is True
    assert migration_g2_4bo["content_derived_all_views_validation"] is True
    assert migration_g2_4bo["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bo["legacy_root_lines_after"] == 24483
    assert migration_g2_4bo["legacy_root_top_level_functions_after"] == 718
    assert migration_g2_4bo["legacy_root_command_decorators_after"] == 679
    assert migration_g2_4bo["python_module_count"] == 879
    assert migration_g2_4bo["production_effect"] == "none"
    if phase_g2_4bo["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bo["validation"]["focused"]["passed"] == 411
        assert phase_g2_4bo["validation"]["architecture_fitness"]["passed"] == 250
        assert phase_g2_4bo["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bo["sources"]
        for source in phase_g2_4bo["sources"]:
            if source["path"] in set(phase_g2_4bo.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bp = baseline["phase_g2_4bp_etf_cli_dynamic_v3_backtest_sim_regime"]
    assert phase_g2_4bp["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bp = phase_g2_4bp["migration"]
    assert migration_g2_4bp["callback_count"] == 3
    assert migration_g2_4bp["pre_output_outcome_content_validation"] is True
    assert migration_g2_4bp["full_outcome_validation_snapshot_required"] is True
    assert migration_g2_4bp["event_and_window_count_units_distinct"] is True
    assert migration_g2_4bp["missing_metrics_must_be_null"] is True
    assert migration_g2_4bp["content_derived_all_views_validation"] is True
    assert migration_g2_4bp["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bp["legacy_root_lines_after"] == 24394
    assert migration_g2_4bp["legacy_root_top_level_functions_after"] == 715
    assert migration_g2_4bp["legacy_root_command_decorators_after"] == 676
    assert migration_g2_4bp["python_module_count"] == 880
    if phase_g2_4bp["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bp["validation"]["focused"]["passed"] == 423
        assert phase_g2_4bp["validation"]["architecture_fitness"]["passed"] == 251
        assert phase_g2_4bp["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bp["sources"]
        for source in phase_g2_4bp["sources"]:
            if source["path"] in set(phase_g2_4bp.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bq = baseline["phase_g2_4bq_etf_cli_dynamic_v3_backtest_sim_sensitivity"]
    assert phase_g2_4bq["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bq = phase_g2_4bq["migration"]
    assert migration_g2_4bq["callback_count"] == 3
    assert migration_g2_4bq["pre_output_outcome_content_validation"] is True
    assert migration_g2_4bq["full_outcome_validation_snapshot_required"] is True
    assert migration_g2_4bq["single_frozen_variant_event_config_cache_lineage"] is True
    assert migration_g2_4bq["exact_unique_policy_grids_required"] is True
    assert migration_g2_4bq["missing_metrics_must_be_null"] is True
    assert migration_g2_4bq["event_window_and_result_units_distinct"] is True
    assert migration_g2_4bq["missing_dispersion_must_be_excluded"] is True
    assert migration_g2_4bq["strong_calibration_low_risk_only"] is True
    assert migration_g2_4bq["content_derived_all_views_validation"] is True
    assert migration_g2_4bq["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bq["legacy_root_lines_after"] == 24296
    assert migration_g2_4bq["legacy_root_top_level_functions_after"] == 712
    assert migration_g2_4bq["legacy_root_command_decorators_after"] == 673
    assert migration_g2_4bq["python_module_count"] == 881
    if phase_g2_4bq["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bq["validation"]["focused"]["passed"] == 437
        assert phase_g2_4bq["validation"]["architecture_fitness"]["passed"] == 252
        assert phase_g2_4bq["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bq["sources"]
        for source in phase_g2_4bq["sources"]:
            if source["path"] in set(phase_g2_4bq.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4br = baseline["phase_g2_4br_etf_cli_dynamic_v3_backtest_sim_calibration"]
    assert phase_g2_4br["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4br = phase_g2_4br["migration"]
    assert migration_g2_4br["callback_count"] == 3
    assert migration_g2_4br["four_source_content_validation_required"] is True
    assert migration_g2_4br["full_source_bundles_and_validations_snapshot_required"] is True
    assert migration_g2_4br["cross_source_lineage_required"] is True
    assert migration_g2_4br["missing_metrics_must_be_null"] is True
    assert migration_g2_4br["positive_proposal_low_risk_only"] is True
    assert migration_g2_4br["content_derived_all_views_validation"] is True
    assert migration_g2_4br["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4br["legacy_root_lines_after"] == 24180
    assert migration_g2_4br["legacy_root_top_level_functions_after"] == 709
    assert migration_g2_4br["legacy_root_command_decorators_after"] == 670
    assert migration_g2_4br["python_module_count"] == 882
    if phase_g2_4br["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4br["validation"]["focused"]["passed"] == 451
        assert phase_g2_4br["validation"]["architecture_fitness"]["passed"] == 253
        assert phase_g2_4br["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4br["sources"]
        for source in phase_g2_4br["sources"]:
            if source["path"] in set(phase_g2_4br.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bs = baseline["phase_g2_4bs_etf_cli_dynamic_v3_backtest_sim_forward_bridge"]
    assert phase_g2_4bs["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bs = phase_g2_4bs["migration"]
    assert migration_g2_4bs["callback_count"] == 3
    assert migration_g2_4bs["calibration_content_validation_required"] is True
    assert migration_g2_4bs["full_calibration_bundle_and_validation_snapshot_required"] is True
    assert migration_g2_4bs["reviewed_forward_policy_required"] is True
    assert migration_g2_4bs["policy_numeric_fallback_allowed"] is False
    assert migration_g2_4bs["tracking_plan_only"] is True
    assert migration_g2_4bs["content_derived_all_views_validation"] is True
    assert migration_g2_4bs["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bs["legacy_root_lines_after"] == 24096
    assert migration_g2_4bs["legacy_root_top_level_functions_after"] == 706
    assert migration_g2_4bs["legacy_root_command_decorators_after"] == 667
    assert migration_g2_4bs["python_module_count"] == 883
    if phase_g2_4bs["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bs["validation"]["focused"]["passed"] > 451
        assert phase_g2_4bs["validation"]["architecture_fitness"]["passed"] > 253
        assert phase_g2_4bs["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bs["sources"]
        for source in phase_g2_4bs["sources"]:
            if source["path"] in set(phase_g2_4bs.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bt = baseline["phase_g2_4bt_etf_cli_dynamic_v3_sim_interpretation"]
    assert phase_g2_4bt["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bt = phase_g2_4bt["migration"]
    assert migration_g2_4bt["callback_count"] == 3
    assert migration_g2_4bt["three_source_content_validation_required"] is True
    assert migration_g2_4bt["same_outcome_lineage_required"] is True
    assert migration_g2_4bt["full_source_bundles_and_validations_snapshot_required"] is True
    assert migration_g2_4bt["paired_available_finite_cohort_required"] is True
    assert migration_g2_4bt["missing_metrics_must_be_null"] is True
    assert migration_g2_4bt["tracking_plan_is_not_forward_success"] is True
    assert migration_g2_4bt["content_derived_all_views_validation"] is True
    assert migration_g2_4bt["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bt["legacy_root_lines_after"] == 23994
    assert migration_g2_4bt["legacy_root_top_level_functions_after"] == 703
    assert migration_g2_4bt["legacy_root_command_decorators_after"] == 664
    assert migration_g2_4bt["python_module_count"] == 884
    if phase_g2_4bt["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bt["validation"]["focused"]["passed"] > 464
        assert phase_g2_4bt["validation"]["architecture_fitness"]["passed"] > 254
        assert phase_g2_4bt["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bt["sources"]
        for source in phase_g2_4bt["sources"]:
            if source["path"] in set(phase_g2_4bt.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bu = baseline["phase_g2_4bu_etf_cli_dynamic_v3_sim_risk_return"]
    assert phase_g2_4bu["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bu = phase_g2_4bu["migration"]
    assert migration_g2_4bu["callback_count"] == 3
    assert migration_g2_4bu["outcome_content_validation_required"] is True
    assert migration_g2_4bu["full_outcome_bundle_and_validation_snapshot_required"] is True
    assert migration_g2_4bu["same_event_20d_available_finite_pairs_required"] is True
    assert migration_g2_4bu["paired_event_and_window_counts_required"] is True
    assert migration_g2_4bu["missing_metrics_and_undefined_ratios_must_be_null"] is True
    assert migration_g2_4bu["content_derived_all_views_validation"] is True
    assert migration_g2_4bu["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bu["legacy_root_lines_after"] == 23911
    assert migration_g2_4bu["legacy_root_top_level_functions_after"] == 700
    assert migration_g2_4bu["legacy_root_command_decorators_after"] == 661
    assert migration_g2_4bu["python_module_count"] == 885
    if phase_g2_4bu["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bu["validation"]["focused"]["passed"] > 475
        assert phase_g2_4bu["validation"]["architecture_fitness"]["passed"] > 255
        assert phase_g2_4bu["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bu["sources"]
        for source in phase_g2_4bu["sources"]:
            if source["path"] in set(phase_g2_4bu.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bv = baseline["phase_g2_4bv_etf_cli_dynamic_v3_sim_defensive_validation"]
    assert phase_g2_4bv["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bv = phase_g2_4bv["migration"]
    assert migration_g2_4bv["callback_count"] == 3
    assert migration_g2_4bv["outcome_content_validation_required"] is True
    assert migration_g2_4bv["full_outcome_bundle_validation_and_policy_snapshot_required"] is True
    assert migration_g2_4bv["same_regime_event_window_available_finite_pairs_required"] is True
    assert migration_g2_4bv["paired_event_and_window_counts_required"] is True
    assert migration_g2_4bv["missing_metrics_must_be_null"] is True
    assert migration_g2_4bv["reviewed_defensive_policy_required"] is True
    assert migration_g2_4bv["content_derived_all_views_validation"] is True
    assert migration_g2_4bv["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bv["legacy_root_lines_after"] == 23815
    assert migration_g2_4bv["legacy_root_top_level_functions_after"] == 697
    assert migration_g2_4bv["legacy_root_command_decorators_after"] == 658
    assert migration_g2_4bv["python_module_count"] == 886
    if phase_g2_4bv["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bv["validation"]["focused"]["passed"] > 486
        assert phase_g2_4bv["validation"]["architecture_fitness"]["passed"] > 256
        assert phase_g2_4bv["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bv["sources"]
        for source in phase_g2_4bv["sources"]:
            if source["path"] in set(phase_g2_4bv.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bw = baseline["phase_g2_4bw_etf_cli_dynamic_v3_advisory_proposal_review"]
    assert phase_g2_4bw["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bw = phase_g2_4bw["migration"]
    assert migration_g2_4bw["callback_count"] == 3
    assert migration_g2_4bw["four_source_content_validation_required"] is True
    assert migration_g2_4bw["source_generated_cutoff_and_same_outcome_lineage_required"] is True
    assert migration_g2_4bw["full_source_bundles_validations_and_policy_snapshot_required"] is True
    assert migration_g2_4bw["fabricated_proposal_or_confidence_allowed"] is False
    assert migration_g2_4bw["empty_proposals_are_insufficient_data"] is True
    assert migration_g2_4bw["reviewed_proposal_policy_required"] is True
    assert migration_g2_4bw["content_derived_all_views_validation"] is True
    assert migration_g2_4bw["automatic_downstream_simulation_allowed"] is False
    assert migration_g2_4bw["legacy_root_lines_after"] == 23688
    assert migration_g2_4bw["legacy_root_top_level_functions_after"] == 694
    assert migration_g2_4bw["legacy_root_command_decorators_after"] == 655
    assert migration_g2_4bw["python_module_count"] == 887
    if phase_g2_4bw["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bw["validation"]["focused"]["passed"] > 500
        assert phase_g2_4bw["validation"]["architecture_fitness"]["passed"] > 257
        assert phase_g2_4bw["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bw["sources"]
        for source in phase_g2_4bw["sources"]:
            if source["path"] in set(phase_g2_4bw.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bx = baseline["phase_g2_4bx_etf_cli_dynamic_v3_forward_confirmation_plan"]
    assert phase_g2_4bx["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bx = phase_g2_4bx["migration"]
    assert migration_g2_4bx["callback_count"] == 3
    assert migration_g2_4bx["two_source_content_validation_required"] is True
    assert migration_g2_4bx["source_generated_cutoff_and_same_calibration_lineage_required"] is True
    assert migration_g2_4bx["full_source_bundles_validations_and_policy_snapshot_required"] is True
    assert migration_g2_4bx["fabricated_target_or_numeric_criterion_allowed"] is False
    assert migration_g2_4bx["empty_or_unmatched_proposals_are_insufficient_data"] is True
    assert migration_g2_4bx["bridge_criteria_exact_inheritance_required"] is True
    assert migration_g2_4bx["reviewed_semantic_policy_required"] is True
    assert migration_g2_4bx["content_derived_all_views_validation"] is True
    assert migration_g2_4bx["automatic_target_registration_or_forward_run_allowed"] is False
    assert migration_g2_4bx["legacy_root_lines_after"] == 23584
    assert migration_g2_4bx["legacy_root_top_level_functions_after"] == 691
    assert migration_g2_4bx["legacy_root_command_decorators_after"] == 652
    assert migration_g2_4bx["python_module_count"] == 888
    if phase_g2_4bx["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bx["validation"]["focused"]["passed"] > 514
        assert phase_g2_4bx["validation"]["architecture_fitness"]["passed"] > 258
        assert phase_g2_4bx["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bx["sources"]
        for source in phase_g2_4bx["sources"]:
            if source["path"] in set(phase_g2_4bx.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4by = baseline["phase_g2_4by_etf_cli_dynamic_v3_confirmation_targets"]
    assert phase_g2_4by["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4by = phase_g2_4by["migration"]
    assert migration_g2_4by["callback_count"] == 4
    assert migration_g2_4by["plan_content_validation_and_available_status_required"] is True
    assert migration_g2_4by["source_generated_cutoff_required"] is True
    assert (
        migration_g2_4by["full_plan_bundle_validation_and_registry_preimage_snapshot_required"]
        is True
    )
    assert migration_g2_4by["source_exact_targets_status_criteria_and_failures_required"] is True
    assert migration_g2_4by["duplicate_plan_registration_allowed"] is False
    assert migration_g2_4by["canonical_atomic_materialized_registry_required"] is True
    assert migration_g2_4by["content_derived_all_views_validation"] is True
    assert migration_g2_4by["automatic_progress_or_evaluation_allowed"] is False
    assert migration_g2_4by["legacy_root_lines_after"] == 23439
    assert migration_g2_4by["legacy_root_top_level_functions_after"] == 687
    assert migration_g2_4by["legacy_root_command_decorators_after"] == 648
    assert migration_g2_4by["python_module_count"] == 889
    if phase_g2_4by["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4by["validation"]["focused"]["passed"] > 529
        assert phase_g2_4by["validation"]["architecture_fitness"]["passed"] > 259
        assert phase_g2_4by["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4by["sources"]
        for source in phase_g2_4by["sources"]:
            if source["path"] in set(phase_g2_4by.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4bz = baseline["phase_g2_4bz_etf_cli_dynamic_v3_confirmation_progress"]
    assert phase_g2_4bz["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4bz = phase_g2_4bz["migration"]
    assert migration_g2_4bz["callback_count"] == 3
    assert migration_g2_4bz["registry_content_validation_and_cutoff_required"] is True
    assert migration_g2_4bz["deterministic_validated_evidence_selection_required"] is True
    assert migration_g2_4bz["full_source_bundle_validation_snapshot_required"] is True
    assert migration_g2_4bz["source_exact_events_windows_and_criteria_required"] is True
    assert migration_g2_4bz["cross_window_event_double_count_allowed"] is False
    assert migration_g2_4bz["missing_metrics_must_remain_null"] is True
    assert migration_g2_4bz["ungoverned_near_ready_threshold_allowed"] is False
    assert migration_g2_4bz["content_derived_all_views_validation"] is True
    assert migration_g2_4bz["automatic_evaluation_allowed"] is False
    assert migration_g2_4bz["legacy_root_lines_after"] == 23336
    assert migration_g2_4bz["legacy_root_top_level_functions_after"] == 684
    assert migration_g2_4bz["legacy_root_command_decorators_after"] == 645
    assert migration_g2_4bz["python_module_count"] == 890
    if phase_g2_4bz["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4bz["validation"]["focused"]["passed"] > 540
        assert phase_g2_4bz["validation"]["architecture_fitness"]["passed"] > 260
        assert phase_g2_4bz["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4bz["sources"]
        for source in phase_g2_4bz["sources"]:
            if source["path"] in set(phase_g2_4bz.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4ca = baseline["phase_g2_4ca_etf_cli_dynamic_v3_confirmation_evaluation"]
    assert phase_g2_4ca["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ca = phase_g2_4ca["migration"]
    assert migration_g2_4ca["callback_count"] == 3
    assert migration_g2_4ca["progress_content_validation_and_cutoff_required"] is True
    assert migration_g2_4ca["full_progress_bundle_validation_snapshot_required"] is True
    assert migration_g2_4ca["not_ready_partial_criteria_evaluation_allowed"] is False
    assert migration_g2_4ca["ready_source_exact_finite_criteria_required"] is True
    assert migration_g2_4ca["all_criteria_pass_and_no_failure_required_for_success"] is True
    assert migration_g2_4ca["failure_boundary_must_derive_from_source_criterion"] is True
    assert migration_g2_4ca["unknown_failure_condition_allowed"] is False
    assert migration_g2_4ca["content_derived_all_views_validation"] is True
    assert migration_g2_4ca["automatic_rule_review_allowed"] is False
    assert migration_g2_4ca["legacy_root_lines_after"] == 23246
    assert migration_g2_4ca["legacy_root_top_level_functions_after"] == 681
    assert migration_g2_4ca["legacy_root_command_decorators_after"] == 642
    assert migration_g2_4ca["python_module_count"] == 891
    if phase_g2_4ca["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4ca["validation"]["focused"]["passed"] > 550
        assert phase_g2_4ca["validation"]["architecture_fitness"]["passed"] > 261
        assert phase_g2_4ca["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4ca["sources"]
        for source in phase_g2_4ca["sources"]:
            if source["path"] in set(phase_g2_4ca.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4cb = baseline["phase_g2_4cb_etf_cli_dynamic_v3_rule_review_cycle"]
    assert phase_g2_4cb["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cb = phase_g2_4cb["migration"]
    assert migration_g2_4cb["callback_count"] == 3
    assert migration_g2_4cb["three_source_content_validation_and_cutoff_required"] is True
    assert (
        migration_g2_4cb["strict_registry_progress_evaluation_lineage_and_chronology_required"]
        is True
    )
    assert migration_g2_4cb["exact_target_coverage_required"] is True
    assert migration_g2_4cb["bounded_full_byte_commitment_bundle_required"] is True
    assert migration_g2_4cb["source_failure_actions_preserved"] is True
    assert migration_g2_4cb["target_id_semantic_override_allowed"] is False
    assert migration_g2_4cb["content_derived_all_views_validation"] is True
    assert migration_g2_4cb["automatic_owner_decision_allowed"] is False
    assert migration_g2_4cb["legacy_root_lines_after"] == 23136
    assert migration_g2_4cb["legacy_root_top_level_functions_after"] == 678
    assert migration_g2_4cb["legacy_root_command_decorators_after"] == 639
    assert migration_g2_4cb["python_module_count"] == 892
    if phase_g2_4cb["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cb["validation"]["focused"]["passed"] > 563
        assert phase_g2_4cb["validation"]["architecture_fitness"]["passed"] > 262
        assert phase_g2_4cb["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4cb["sources"]
        for source in phase_g2_4cb["sources"]:
            if source["path"] in set(phase_g2_4cb.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4cc = baseline["phase_g2_4cc_etf_cli_dynamic_v3_rule_owner_decision"]
    assert phase_g2_4cc["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cc = phase_g2_4cc["migration"]
    assert migration_g2_4cc["callback_count"] == 5
    assert migration_g2_4cc["rule_review_content_validation_and_cutoff_required"] is True
    assert migration_g2_4cc["bounded_cycle_snapshot_and_exact_scope_required"] is True
    assert migration_g2_4cc["one_decision_per_cycle_required"] is True
    assert migration_g2_4cc["append_only_event_sha256_chain_required"] is True
    assert (
        migration_g2_4cc["pending_single_final_transition_and_strict_chronology_required"] is True
    )
    assert migration_g2_4cc["evidence_bound_decision_eligibility_required"] is True
    assert migration_g2_4cc["legacy_unsnapshotted_write_allowed"] is False
    assert migration_g2_4cc["content_derived_all_views_validation"] is True
    assert migration_g2_4cc["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cc["legacy_root_lines_after"] == 22981
    assert migration_g2_4cc["legacy_root_top_level_functions_after"] == 673
    assert migration_g2_4cc["legacy_root_command_decorators_after"] == 634
    assert migration_g2_4cc["python_module_count"] == 893
    if phase_g2_4cc["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cc["validation"]["focused"]["passed"] > 570
        assert phase_g2_4cc["validation"]["architecture_fitness"]["passed"] > 263
        assert phase_g2_4cc["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4cc["sources"]
        for source in phase_g2_4cc["sources"]:
            if source["path"] in set(phase_g2_4cc.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4cd = baseline["phase_g2_4cd_etf_cli_dynamic_v3_confirmation_operations"]
    assert phase_g2_4cd["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cd = phase_g2_4cd["migration"]
    assert migration_g2_4cd["callback_count"] == 16
    assert migration_g2_4cd["pre_output_timezone_and_cutoff_required"] is True
    assert migration_g2_4cd["schedule_config_and_source_content_validation_required"] is True
    assert migration_g2_4cd["semantic_latest_selection_without_mtime_required"] is True
    assert migration_g2_4cd["bounded_source_commitment_snapshots_required"] is True
    assert migration_g2_4cd["weekly_step_chain_validation_required"] is True
    assert migration_g2_4cd["optional_absence_and_invalid_source_distinct"] is True
    assert migration_g2_4cd["dashboard_progress_readiness_override_allowed"] is False
    assert migration_g2_4cd["queue_cross_cycle_owner_decision_allowed"] is False
    assert migration_g2_4cd["pressure_pending_outcome_counts_as_defensive_evidence"] is False
    assert migration_g2_4cd["content_derived_all_views_validation"] is True
    assert migration_g2_4cd["default_weekly_mode"] == "dry_run"
    assert migration_g2_4cd["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cd["legacy_root_lines_after"] == 22538
    assert migration_g2_4cd["legacy_root_top_level_functions_after"] == 657
    assert migration_g2_4cd["legacy_root_command_decorators_after"] == 618
    assert migration_g2_4cd["python_module_count"] == 894
    if phase_g2_4cd["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cd["validation"]["focused"]["passed"] > 578
        assert phase_g2_4cd["validation"]["architecture_fitness"]["passed"] > 264
        assert phase_g2_4cd["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4cd["sources"]
        for source in phase_g2_4cd["sources"]:
            if source["path"] in set(phase_g2_4cd.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4ce = baseline["phase_g2_4ce_etf_cli_dynamic_v3_pressure_validation"]
    assert phase_g2_4ce["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ce = phase_g2_4ce["migration"]
    assert migration_g2_4ce["callback_count"] == 15
    assert migration_g2_4ce["pre_output_timezone_and_cutoff_required"] is True
    assert migration_g2_4ce["semantic_latest_selection_without_mtime_required"] is True
    assert migration_g2_4ce["content_derived_consumer_validation_required"] is True
    assert migration_g2_4ce["bounded_source_and_policy_commitments_required"] is True
    assert migration_g2_4ce["available_finite_unique_paired_evidence_only"] is True
    assert migration_g2_4ce["missing_metrics_must_remain_null"] is True
    assert migration_g2_4ce["policy_governed_distinct_event_floor_and_boundaries"] is True
    assert (
        migration_g2_4ce["all_configured_pressure_regimes_required_for_source_conclusion"] is True
    )
    assert migration_g2_4ce["weekly_distinct_forward_event_count_required"] is True
    assert migration_g2_4ce["downstream_pressure_capture_source_roots_explicit"] is True
    assert migration_g2_4ce["simulation_and_historical_evidence_research_only"] is True
    assert migration_g2_4ce["weekly_exact_lineage_and_chronology_required"] is True
    assert migration_g2_4ce["rule_approval_allowed"] is False
    assert migration_g2_4ce["auto_apply_allowed"] is False
    assert migration_g2_4ce["content_derived_all_views_validation"] is True
    assert migration_g2_4ce["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4ce["legacy_root_lines_after"] == 22069
    assert migration_g2_4ce["legacy_root_top_level_functions_after"] == 642
    assert migration_g2_4ce["legacy_root_command_decorators_after"] == 603
    assert migration_g2_4ce["python_module_count"] == 895
    if phase_g2_4ce["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4ce["validation"]["focused"]["passed"] > 580
        assert phase_g2_4ce["validation"]["architecture_fitness"]["passed"] > 265
        assert phase_g2_4ce["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4ce["sources"]
        for source in phase_g2_4ce["sources"]:
            if source["path"] in set(phase_g2_4ce.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4cf = baseline["phase_g2_4cf_etf_cli_dynamic_v3_defensive_research_synthesis"]
    assert phase_g2_4cf["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cf = phase_g2_4cf["migration"]
    assert migration_g2_4cf["callback_count"] == 15
    assert migration_g2_4cf["pre_output_timezone_and_cutoff_required"] is True
    assert migration_g2_4cf["content_derived_consumer_validation_required"] is True
    assert migration_g2_4cf["strict_same_lineage_and_chronology_required"] is True
    assert migration_g2_4cf["bounded_source_and_policy_commitments_required"] is True
    assert migration_g2_4cf["missing_metrics_must_remain_null"] is True
    assert migration_g2_4cf["policy_governed_research_interpretation"] is True
    assert migration_g2_4cf["simulation_can_support_rule_approval"] is False
    assert migration_g2_4cf["rename_or_mitigation_auto_apply_allowed"] is False
    assert migration_g2_4cf["content_derived_all_views_validation"] is True
    assert migration_g2_4cf["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cf["legacy_root_lines_after"] == 21696
    assert migration_g2_4cf["legacy_root_top_level_functions_after"] == 627
    assert migration_g2_4cf["legacy_root_command_decorators_after"] == 588
    assert migration_g2_4cf["python_module_count"] == 897
    if phase_g2_4cf["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cf["validation"]["focused"]["passed"] > 590
        assert phase_g2_4cf["validation"]["downstream_compatibility"]["passed"] >= 5
        assert phase_g2_4cf["validation"]["architecture_fitness"]["passed"] >= 266
        assert phase_g2_4cf["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4cf["sources"]
        for source in phase_g2_4cf["sources"]:
            if source["path"] in set(phase_g2_4cf.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4cg = baseline["phase_g2_4cg_etf_cli_dynamic_v3_forward_pressure_evidence"]
    assert phase_g2_4cg["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cg = phase_g2_4cg["migration"]
    assert migration_g2_4cg["callback_count"] == 15
    assert migration_g2_4cg["reviewed_capture_policy_required"] is True
    assert migration_g2_4cg["pre_output_data_quality_and_cutoff_required"] is True
    assert migration_g2_4cg["trigger_live_cache_and_policy_recompute_required"] is True
    assert migration_g2_4cg["exact_trigger_tag_backfill_compare_lineage_required"] is True
    assert migration_g2_4cg["semantic_cutoff_source_selection_required"] is True
    assert migration_g2_4cg["distinct_source_event_sample_unit_required"] is True
    assert migration_g2_4cg["bounded_source_policy_cache_snapshots_required"] is True
    assert migration_g2_4cg["explicit_test_fixture_dq_skip_only"] is True
    assert migration_g2_4cg["content_derived_all_views_validation"] is True
    assert migration_g2_4cg["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cg["portfolio_or_execution_effect"] is False
    assert migration_g2_4cg["legacy_root_lines_after"] == 21286
    assert migration_g2_4cg["legacy_root_top_level_functions_after"] == 612
    assert migration_g2_4cg["legacy_root_command_decorators_after"] == 573
    assert migration_g2_4cg["python_module_count"] == 899
    assert migration_g2_4cg["python_test_file_count"] == 1114
    if phase_g2_4cg["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cg["validation"]["focused"]["passed"] > 601
        assert phase_g2_4cg["validation"]["current_slice_and_cli_contract"]["passed"] >= 117
        assert phase_g2_4cg["validation"]["architecture_fitness"]["passed"] >= 268
        assert phase_g2_4cg["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4cg["sources"]
        for source in phase_g2_4cg["sources"]:
            if source["path"] in set(phase_g2_4cg.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4ch = baseline["phase_g2_4ch_etf_cli_dynamic_v3_system_target_portfolio"]
    assert phase_g2_4ch["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ch = phase_g2_4ch["migration"]
    assert migration_g2_4ch["callback_count"] == 17
    assert migration_g2_4ch["reviewed_target_and_paper_policy_required"] is True
    assert migration_g2_4ch["pre_output_source_validation_and_cutoff_required"] is True
    assert migration_g2_4ch["semantic_zero_or_one_selection_required"] is True
    assert migration_g2_4ch["fabricated_target_fallback_allowed"] is False
    assert migration_g2_4ch["immutable_paper_source_state_required"] is True
    assert migration_g2_4ch["append_only_rebalance_post_state_required"] is True
    assert migration_g2_4ch["duplicate_paper_target_allowed"] is False
    assert migration_g2_4ch["common_finite_date_performance_required"] is True
    assert migration_g2_4ch["missing_metrics_must_remain_null"] is True
    assert migration_g2_4ch["data_quality_and_cache_commitments_required"] is True
    assert migration_g2_4ch["exact_target_paper_performance_lineage_required"] is True
    assert migration_g2_4ch["performance_winner_may_equal_observation_priority"] is False
    assert migration_g2_4ch["bounded_source_policy_cache_snapshots_required"] is True
    assert migration_g2_4ch["content_derived_all_views_validation"] is True
    assert migration_g2_4ch["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4ch["portfolio_or_execution_effect"] is False
    assert migration_g2_4ch["legacy_root_lines_after"] == 20841
    assert migration_g2_4ch["legacy_root_top_level_functions_after"] == 595
    assert migration_g2_4ch["legacy_root_command_decorators_after"] == 556
    assert migration_g2_4ch["legacy_domain_lines_after"] == 27838
    assert migration_g2_4ch["legacy_domain_top_level_functions_after"] == 804
    assert migration_g2_4ch["python_module_count"] == 901
    assert migration_g2_4ch["python_test_file_count"] == 1114
    if phase_g2_4ch["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4ch["validation"]["focused"]["passed"] >= 225
        assert phase_g2_4ch["validation"]["core_positive_and_negative"]["passed"] >= 11
        assert phase_g2_4ch["validation"]["architecture_fitness"]["passed"] >= 269
        assert phase_g2_4ch["validation"]["contract_validation"]["passed"] == 203
        assert phase_g2_4ch["sources"]
        for source in phase_g2_4ch["sources"]:
            if source["path"] in set(phase_g2_4ch.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4ci = baseline["phase_g2_4ci_etf_cli_dynamic_v3_system_target_history"]
    assert phase_g2_4ci["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ci = phase_g2_4ci["migration"]
    assert migration_g2_4ci["callback_count"] == 16
    assert migration_g2_4ci["validated_model_target_only"] is True
    assert migration_g2_4ci["mutable_latest_or_baseline_fallback_allowed"] is False
    assert migration_g2_4ci["common_finite_duplicate_free_dates_required"] is True
    assert migration_g2_4ci["versioned_costs_applied"] is True
    assert migration_g2_4ci["current_definition_replayed_historically"] is True
    assert migration_g2_4ci["pit_safe"] is False
    assert migration_g2_4ci["missing_metrics_must_remain_null"] is True
    assert migration_g2_4ci["regime_and_rank_thresholds_policy_governed"] is True
    assert migration_g2_4ci["exact_same_backfill_lineage_required"] is True
    assert migration_g2_4ci["reference_only_recommendation_allowed"] is False
    assert migration_g2_4ci["bounded_source_policy_cache_snapshots_required"] is True
    assert migration_g2_4ci["content_derived_all_views_validation"] is True
    assert migration_g2_4ci["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4ci["portfolio_or_execution_effect"] is False
    assert migration_g2_4ci["legacy_root_lines_after"] == 20437
    assert migration_g2_4ci["legacy_root_top_level_functions_after"] == 579
    assert migration_g2_4ci["legacy_root_command_decorators_after"] == 540
    assert migration_g2_4ci["legacy_domain_lines_after"] == 27034
    assert migration_g2_4ci["legacy_domain_top_level_functions_after"] == 806
    assert migration_g2_4ci["python_module_count"] == 903
    assert migration_g2_4ci["python_test_file_count"] == 1114
    if phase_g2_4ci["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4ci["validation"]["focused"]["passed"] >= 33
        assert phase_g2_4ci["validation"]["core_positive_and_negative"]["passed"] >= 10
        assert phase_g2_4ci["validation"]["architecture_fitness"]["passed"] >= 270
        assert phase_g2_4ci["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4ci["sources"]
        for source in phase_g2_4ci["sources"]:
            if source["path"] in set(phase_g2_4ci.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]
    phase_g2_4cj = baseline["phase_g2_4cj_etf_cli_dynamic_v3_system_target_hardening"]
    assert phase_g2_4cj["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cj = phase_g2_4cj["migration"]
    assert migration_g2_4cj["callback_count"] == 15
    assert migration_g2_4cj["pre_output_live_source_validation_required"] is True
    assert migration_g2_4cj["implicit_upstream_run_allowed"] is False
    assert migration_g2_4cj["missing_metrics_must_remain_null"] is True
    assert migration_g2_4cj["data_quality_penalty_scored"] is False
    assert migration_g2_4cj["exact_same_backfill_lineage_required"] is True
    assert migration_g2_4cj["warning_detail_missing_is_unknown"] is True
    assert migration_g2_4cj["hardening_same_selection_lineage_required"] is True
    assert migration_g2_4cj["workflow_pass_is_investment_conclusion"] is False
    assert migration_g2_4cj["bounded_source_policy_snapshots_required"] is True
    assert migration_g2_4cj["content_derived_all_views_validation"] is True
    assert migration_g2_4cj["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cj["portfolio_or_execution_effect"] is False
    assert migration_g2_4cj["legacy_root_lines_after"] == 20017
    assert migration_g2_4cj["legacy_root_top_level_functions_after"] == 564
    assert migration_g2_4cj["legacy_root_command_decorators_after"] == 525
    assert migration_g2_4cj["legacy_domain_lines_after"] == 26087
    assert migration_g2_4cj["legacy_domain_top_level_functions_after"] == 801
    assert migration_g2_4cj["python_module_count"] == 905
    assert migration_g2_4cj["python_test_file_count"] == 1114
    if phase_g2_4cj["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cj["validation"]["focused"]["passed"] >= 12
        assert phase_g2_4cj["validation"]["core_positive_and_negative"]["passed"] >= 7
        assert phase_g2_4cj["validation"]["downstream_compatibility"]["passed"] >= 5
        assert phase_g2_4cj["validation"]["architecture_fitness"]["passed"] >= 271
        assert phase_g2_4cj["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cj["sources"]
        for source in phase_g2_4cj["sources"]:
            if source["path"] in set(phase_g2_4cj.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4ck = baseline["phase_g2_4ck_etf_cli_dynamic_v3_system_target_refinement"]
    assert phase_g2_4ck["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ck = phase_g2_4ck["migration"]
    assert migration_g2_4ck["callback_count"] == 15
    assert migration_g2_4ck["pre_output_live_source_validation_required"] is True
    assert migration_g2_4ck["implicit_upstream_run_allowed"] is False
    assert migration_g2_4ck["exact_same_backfill_and_selection_lineage_required"] is True
    assert migration_g2_4ck["source_chronology_required"] is True
    assert migration_g2_4ck["reviewed_method_refinement_policy_required"] is True
    assert migration_g2_4ck["risk_data_quality_and_cache_commitments_required"] is True
    assert migration_g2_4ck["common_finite_price_dates_required"] is True
    assert migration_g2_4ck["first_or_missing_return_may_be_filled_zero"] is False
    assert migration_g2_4ck["missing_metrics_must_remain_null"] is True
    assert migration_g2_4ck["conceptual_metrics_must_remain_null_or_unknown"] is True
    assert migration_g2_4ck["overlapping_risk_windows_are_independent_samples"] is False
    assert migration_g2_4ck["bounded_source_policy_cache_snapshots_required"] is True
    assert migration_g2_4ck["content_derived_all_views_validation"] is True
    assert migration_g2_4ck["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4ck["portfolio_or_execution_effect"] is False
    assert migration_g2_4ck["legacy_root_lines_after"] == 19586
    assert migration_g2_4ck["legacy_root_top_level_functions_after"] == 549
    assert migration_g2_4ck["legacy_root_command_decorators_after"] == 510
    assert migration_g2_4ck["legacy_domain_lines_after"] == 25367
    assert migration_g2_4ck["legacy_domain_top_level_functions_after"] == 801
    assert migration_g2_4ck["python_module_count"] == 907
    assert migration_g2_4ck["python_test_file_count"] == 1114
    if phase_g2_4ck["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4ck["validation"]["focused"]["passed"] >= 7
        assert phase_g2_4ck["validation"]["current_slice_and_cli_contract"]["passed"] >= 110
        assert phase_g2_4ck["validation"]["architecture_fitness"]["passed"] >= 272
        assert phase_g2_4ck["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4ck["sources"]
        for source in phase_g2_4ck["sources"]:
            if source["path"] in set(phase_g2_4ck.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cl = baseline["phase_g2_4cl_etf_cli_dynamic_v3_system_target_risk_capped"]
    assert phase_g2_4cl["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cl = phase_g2_4cl["migration"]
    assert migration_g2_4cl["callback_count"] == 15
    assert migration_g2_4cl["pre_output_live_source_validation_required"] is True
    assert migration_g2_4cl["canonical_paper_backfill_orchestration_allowed"] is True
    assert migration_g2_4cl["all_other_implicit_upstream_run_allowed"] is False
    assert migration_g2_4cl["exact_same_backfill_lineage_required"] is True
    assert migration_g2_4cl["source_chronology_required"] is True
    assert migration_g2_4cl["reviewed_evaluation_policy_required"] is True
    assert migration_g2_4cl["risk_data_quality_and_cache_commitments_required"] is True
    assert migration_g2_4cl["duplicate_risk_method_observations_allowed"] is False
    assert migration_g2_4cl["missing_regime_metrics_must_remain_null"] is True
    assert migration_g2_4cl["bounded_source_policy_cache_snapshots_required"] is True
    assert len(migration_g2_4cl["snapshot_schemas"]) == 5
    assert migration_g2_4cl["content_derived_all_views_validation"] is True
    assert migration_g2_4cl["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cl["portfolio_or_execution_effect"] is False
    assert migration_g2_4cl["legacy_root_lines_after"] == 19197
    assert migration_g2_4cl["legacy_root_top_level_functions_after"] == 534
    assert migration_g2_4cl["legacy_root_command_decorators_after"] == 495
    assert migration_g2_4cl["legacy_domain_lines_after"] == 24598
    assert migration_g2_4cl["legacy_domain_top_level_functions_after"] == 802
    assert migration_g2_4cl["python_module_count"] == 909
    assert migration_g2_4cl["python_test_file_count"] == 1115
    fixture_g2_4cl = phase_g2_4cl["fixture"]
    assert fixture_g2_4cl["date_start"].isoformat() == "2022-12-01"
    assert fixture_g2_4cl["date_end"].isoformat() == "2024-02-29"
    assert fixture_g2_4cl["workflow_pass_is_not_investment_conclusion"] is True
    if phase_g2_4cl["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cl["validation"]["focused"]["passed"] >= 11
        assert phase_g2_4cl["validation"]["current_slice_and_cli_contract"]["passed"] >= 116
        assert phase_g2_4cl["validation"]["architecture_fitness"]["passed"] >= 273
        assert phase_g2_4cl["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cl["sources"]
        for source in phase_g2_4cl["sources"]:
            if source["path"] in set(phase_g2_4cl.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cm = baseline["phase_g2_4cm_etf_cli_dynamic_v3_system_target_experiment_factory"]
    assert phase_g2_4cm["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cm = phase_g2_4cm["migration"]
    assert migration_g2_4cm["callback_count"] == 21
    assert migration_g2_4cm["pre_output_live_source_validation_required"] is True
    assert migration_g2_4cm["exact_full_chain_lineage_required"] is True
    assert migration_g2_4cm["source_chronology_required"] is True
    assert migration_g2_4cm["common_finite_price_dates_required"] is True
    assert migration_g2_4cm["first_or_missing_return_may_be_filled_zero"] is False
    assert migration_g2_4cm["candidate_selection_must_execute"] is True
    assert migration_g2_4cm["missing_regime_metrics_must_remain_null"] is True
    assert migration_g2_4cm["zero_effect_transform_must_defer"] is True
    assert migration_g2_4cm["reviewed_complete_triage_policy_required"] is True
    assert migration_g2_4cm["expected_and_observed_evidence_are_separate"] is True
    assert len(migration_g2_4cm["snapshot_schemas"]) == 7
    assert migration_g2_4cm["content_derived_all_views_validation"] is True
    assert migration_g2_4cm["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cm["portfolio_or_execution_effect"] is False
    assert migration_g2_4cm["legacy_root_lines_after"] == 18568
    assert migration_g2_4cm["legacy_root_top_level_functions_after"] == 513
    assert migration_g2_4cm["legacy_root_command_decorators_after"] == 474
    assert migration_g2_4cm["legacy_domain_lines_after"] == 23375
    assert migration_g2_4cm["legacy_domain_top_level_functions_after"] == 803
    assert migration_g2_4cm["python_module_count"] == 911
    assert migration_g2_4cm["python_test_file_count"] == 1116
    fixture_g2_4cm = phase_g2_4cm["fixture"]
    assert fixture_g2_4cm["requested_start_date"].isoformat() == "2022-12-01"
    assert fixture_g2_4cm["actual_return_start_date"].isoformat() == "2022-12-02"
    assert fixture_g2_4cm["promote_count"] == 0
    assert fixture_g2_4cm["keep_testing_count"] == 3
    assert fixture_g2_4cm["reject_count"] == 7
    assert fixture_g2_4cm["defer_count"] == 5
    assert fixture_g2_4cm["promotion_plan_status"] == "DEFER"
    assert fixture_g2_4cm["workflow_pass_is_not_investment_conclusion"] is True
    if phase_g2_4cm["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cm["validation"]["focused"]["passed"] >= 12
        assert phase_g2_4cm["validation"]["current_slice_and_cli_contract"]["passed"] >= 118
        assert phase_g2_4cm["validation"]["architecture_fitness"]["passed"] >= 274
        assert phase_g2_4cm["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cm["sources"]
        for source in phase_g2_4cm["sources"]:
            if source["path"] in set(phase_g2_4cm.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cn = baseline["phase_g2_4cn_etf_cli_dynamic_v3_system_target_smoothed_method"]
    assert phase_g2_4cn["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cn = phase_g2_4cn["migration"]
    assert migration_g2_4cn["callback_count"] == 15
    assert migration_g2_4cn["pre_output_live_source_validation_required"] is True
    assert migration_g2_4cn["canonical_paper_backfill_orchestration_allowed"] is True
    assert migration_g2_4cn["all_other_implicit_upstream_run_allowed"] is False
    assert migration_g2_4cn["exact_same_backfill_lineage_required"] is True
    assert migration_g2_4cn["source_chronology_required"] is True
    assert migration_g2_4cn["reviewed_evaluation_policy_required"] is True
    assert migration_g2_4cn["duplicate_method_observations_allowed"] is False
    assert migration_g2_4cn["missing_metrics_must_remain_null"] is True
    assert migration_g2_4cn["evidence_driven_method_selection_required"] is True
    assert migration_g2_4cn["fixed_method_recommendation_allowed"] is False
    assert len(migration_g2_4cn["snapshot_schemas"]) == 5
    assert migration_g2_4cn["content_derived_all_views_validation"] is True
    assert migration_g2_4cn["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cn["portfolio_or_execution_effect"] is False
    assert migration_g2_4cn["legacy_root_lines_after"] == 18164
    assert migration_g2_4cn["legacy_root_top_level_functions_after"] == 498
    assert migration_g2_4cn["legacy_root_command_decorators_after"] == 459
    assert migration_g2_4cn["legacy_domain_lines_after"] == 22551
    assert migration_g2_4cn["legacy_domain_top_level_functions_after"] == 804
    assert migration_g2_4cn["python_module_count"] == 913
    assert migration_g2_4cn["python_test_file_count"] == 1117
    fixture_g2_4cn = phase_g2_4cn["fixture"]
    assert fixture_g2_4cn["date_start"].isoformat() == "2022-12-01"
    assert fixture_g2_4cn["date_end"].isoformat() == "2024-02-29"
    assert fixture_g2_4cn["comparison_observation_count"] == 326
    assert fixture_g2_4cn["decision"] == "CONTINUE_OBSERVATION"
    assert fixture_g2_4cn["recommended_method"] is None
    assert fixture_g2_4cn["secondary_method"] is None
    assert fixture_g2_4cn["workflow_pass_is_not_investment_conclusion"] is True
    if phase_g2_4cn["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cn["validation"]["focused"]["passed"] >= 45
        assert phase_g2_4cn["validation"]["current_slice_and_cli_contract"]["passed"] >= 152
        assert phase_g2_4cn["validation"]["architecture_fitness"]["passed"] >= 275
        assert phase_g2_4cn["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cn["sources"]
        for source in phase_g2_4cn["sources"]:
            if source["path"] in set(phase_g2_4cn.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4co = baseline["phase_g2_4co_etf_cli_dynamic_v3_system_target_smoothed_evidence"]
    assert phase_g2_4co["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4co = phase_g2_4co["migration"]
    assert migration_g2_4co["callback_count"] == 15
    assert migration_g2_4co["pre_output_live_source_validation_required"] is True
    assert migration_g2_4co["exact_review_comparison_backfill_lineage_required"] is True
    assert migration_g2_4co["source_chronology_required"] is True
    assert migration_g2_4co["reviewed_evidence_policy_required"] is True
    assert migration_g2_4co["missing_metrics_must_remain_null"] is True
    assert migration_g2_4co["per_method_evidence_required"] is True
    assert migration_g2_4co["fixed_method_roles_allowed"] is False
    assert migration_g2_4co["confirmation_requires_unique_eligible_recommendation"] is True
    assert migration_g2_4co["zero_targets_when_no_candidate_required"] is True
    assert len(migration_g2_4co["snapshot_schemas"]) == 5
    assert migration_g2_4co["content_derived_all_views_validation"] is True
    assert migration_g2_4co["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4co["portfolio_or_execution_effect"] is False
    assert migration_g2_4co["legacy_root_lines_after"] == 17739
    assert migration_g2_4co["legacy_root_top_level_functions_after"] == 483
    assert migration_g2_4co["legacy_root_command_decorators_after"] == 444
    assert migration_g2_4co["legacy_domain_lines_after"] == 21897
    assert migration_g2_4co["legacy_domain_top_level_functions_after"] == 805
    assert migration_g2_4co["python_module_count"] == 915
    assert migration_g2_4co["python_test_file_count"] == 1118
    fixture_g2_4co = phase_g2_4co["fixture"]
    assert fixture_g2_4co["date_start"].isoformat() == "2022-12-01"
    assert fixture_g2_4co["date_end"].isoformat() == "2024-02-29"
    assert fixture_g2_4co["common_return_observation_count"] == 326
    assert fixture_g2_4co["sideways_observation_count"] == 283
    assert fixture_g2_4co["recovery_observation_count"] == 7
    assert fixture_g2_4co["decision"] == "CONTINUE_OBSERVATION"
    assert fixture_g2_4co["candidate_method"] is None
    assert fixture_g2_4co["confirmation_status"] == "INSUFFICIENT_EVIDENCE"
    assert fixture_g2_4co["confirmation_target_count"] == 0
    assert fixture_g2_4co["workflow_pass_is_not_investment_conclusion"] is True
    if phase_g2_4co["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4co["validation"]["focused"]["passed"] >= 12
        assert phase_g2_4co["validation"]["current_slice_and_cli_contract"]["passed"] >= 120
        assert phase_g2_4co["validation"]["architecture_fitness"]["passed"] >= 276
        assert phase_g2_4co["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4co["sources"]
        for source in phase_g2_4co["sources"]:
            if source["path"] in set(phase_g2_4co.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cp = baseline["phase_g2_4cp_etf_cli_dynamic_v3_system_target_smoothed_readiness"]
    assert phase_g2_4cp["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cp = phase_g2_4cp["migration"]
    assert migration_g2_4cp["callback_count"] == 15
    assert migration_g2_4cp["pre_output_live_source_validation_required"] is True
    assert migration_g2_4cp["exact_review_comparison_backfill_lineage_required"] is True
    assert migration_g2_4cp["exact_same_backfill_lineage_required"] is True
    assert migration_g2_4cp["source_chronology_required"] is True
    assert migration_g2_4cp["reviewed_readiness_policy_required"] is True
    assert migration_g2_4cp["explicit_delta_or_zero_turnover_identity_required"] is True
    assert migration_g2_4cp["duplicate_method_observations_allowed"] is False
    assert migration_g2_4cp["missing_metrics_must_remain_null"] is True
    assert migration_g2_4cp["missing_evidence_positive_score_allowed"] is False
    assert migration_g2_4cp["confirmation_candidate_is_only_candidate_authority"] is True
    assert migration_g2_4cp["fixed_method_roles_allowed"] is False
    assert migration_g2_4cp["candidate_less_promotion_allowed"] is False
    assert len(migration_g2_4cp["snapshot_schemas"]) == 5
    assert migration_g2_4cp["content_derived_all_views_validation"] is True
    assert migration_g2_4cp["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cp["portfolio_or_execution_effect"] is False
    assert migration_g2_4cp["legacy_root_lines_after"] == 17328
    assert migration_g2_4cp["legacy_root_top_level_functions_after"] == 468
    assert migration_g2_4cp["legacy_root_command_decorators_after"] == 429
    assert migration_g2_4cp["legacy_domain_lines_after"] == 19856
    assert migration_g2_4cp["legacy_domain_top_level_functions_after"] == 760
    assert migration_g2_4cp["python_module_count"] == 917
    assert migration_g2_4cp["python_test_file_count"] == 1119
    fixture_g2_4cp = phase_g2_4cp["fixture"]
    assert fixture_g2_4cp["date_start"].isoformat() == "2022-12-01"
    assert fixture_g2_4cp["date_end"].isoformat() == "2024-02-29"
    assert fixture_g2_4cp["candidate_method"] is None
    assert fixture_g2_4cp["gap_tradeoff_can_be_resolved_by_backfill"] is False
    assert fixture_g2_4cp["gap_requires_forward_observation"] is True
    assert fixture_g2_4cp["scorecard_decision"] == "CONTINUE_OBSERVATION"
    assert fixture_g2_4cp["scorecard_evidence_status"] == "INSUFFICIENT_EVIDENCE"
    assert fixture_g2_4cp["recommended_method"] is None
    assert fixture_g2_4cp["secondary_method"] is None
    assert fixture_g2_4cp["owner_recommended_action"] == "request_additional_evidence"
    assert fixture_g2_4cp["owner_promotion_recommended"] is False
    assert fixture_g2_4cp["workflow_pass_is_not_investment_conclusion"] is True
    if phase_g2_4cp["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cp["validation"]["focused_integration"]["passed"] >= 1
        assert phase_g2_4cp["validation"]["hardening"]["passed"] >= 7
        assert phase_g2_4cp["validation"]["current_slice_and_cli_contract"]["passed"] >= 9
        assert (
            phase_g2_4cp["validation"]["downstream_compatibility"]["status"]
            == "FAIL_CLOSED_NEXT_SLICE"
        )
        assert phase_g2_4cp["validation"]["downstream_compatibility"]["passed"] >= 1
        assert phase_g2_4cp["validation"]["architecture_fitness"]["passed"] >= 277
        assert phase_g2_4cp["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cp["sources"]
        for source in phase_g2_4cp["sources"]:
            if source["path"] in set(phase_g2_4cp.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cq = baseline["phase_g2_4cq_etf_cli_dynamic_v3_system_target_smoothed_promotion"]
    assert phase_g2_4cq["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cq = phase_g2_4cq["migration"]
    assert migration_g2_4cq["callback_count"] == 16
    assert migration_g2_4cq["pre_output_live_source_validation_required"] is True
    assert migration_g2_4cq["source_chronology_required"] is True
    assert migration_g2_4cq["exact_candidate_lineage_required"] is True
    assert migration_g2_4cq["reviewed_promotion_policy_required"] is True
    assert migration_g2_4cq["confirmation_candidate_is_only_candidate_authority"] is True
    assert migration_g2_4cq["fixed_method_fallback_allowed"] is False
    assert migration_g2_4cq["candidate_less_target_fabrication_allowed"] is False
    assert migration_g2_4cq["candidate_less_switch_proposal_allowed"] is False
    assert migration_g2_4cq["candidate_less_promote_record_allowed"] is False
    assert migration_g2_4cq["candidate_less_continue_observation_valid"] is True
    assert migration_g2_4cq["bounded_source_bundle_recursive_input_snapshot_allowed"] is False
    assert migration_g2_4cq["bounded_source_bundle_live_validator_required"] is True
    assert migration_g2_4cq["content_derived_evidence_criteria_targets_switch_journal"] is True
    assert migration_g2_4cq["owner_record_atomic_all_view_rebuild"] is True
    assert len(migration_g2_4cq["snapshot_schemas"]) == 5
    assert migration_g2_4cq["content_derived_all_views_validation"] is True
    assert migration_g2_4cq["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cq["portfolio_or_execution_effect"] is False
    assert migration_g2_4cq["legacy_root_lines_after"] == 16761
    assert migration_g2_4cq["legacy_root_top_level_functions_after"] == 452
    assert migration_g2_4cq["legacy_root_command_decorators_after"] == 413
    assert migration_g2_4cq["legacy_domain_lines_after"] == 18248
    assert migration_g2_4cq["legacy_domain_top_level_functions_after"] == 740
    assert migration_g2_4cq["legacy_domain_compatibility_wrapper_count"] == 11
    assert migration_g2_4cq["python_module_count"] == 919
    assert migration_g2_4cq["python_test_file_count"] == 1120
    fixture_g2_4cq = phase_g2_4cq["fixture"]
    assert fixture_g2_4cq["candidate_method"] is None
    assert fixture_g2_4cq["promotion_can_enter_owner_review"] is False
    assert fixture_g2_4cq["promotion_can_become_primary_candidate"] == "NOT_ELIGIBLE"
    assert fixture_g2_4cq["gate_decision"] == "CONTINUE_OBSERVATION"
    assert fixture_g2_4cq["binding_status"] == "NOT_REGISTERED"
    assert fixture_g2_4cq["bound_target_count"] == 0
    assert fixture_g2_4cq["proposed_primary_research_candidate"] is None
    assert fixture_g2_4cq["switch_decision"] == "NO_ELIGIBLE_CANDIDATE"
    assert fixture_g2_4cq["owner_recommended_action"] == "request_more_forward_data"
    assert fixture_g2_4cq["invalid_candidate_less_promote_rejected"] is True
    assert fixture_g2_4cq["continue_observation_validation"] == "PASS"
    assert fixture_g2_4cq["actual_switch_executed"] is False
    assert fixture_g2_4cq["workflow_pass_is_not_investment_conclusion"] is True
    if phase_g2_4cq["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cq["validation"]["hardening"]["passed"] >= 7
        assert phase_g2_4cq["validation"]["five_stage_integration"]["passed"] >= 5
        assert phase_g2_4cq["validation"]["architecture_fitness"]["passed"] >= 278
        assert phase_g2_4cq["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cq["sources"]
        for source in phase_g2_4cq["sources"]:
            if source["path"] in set(phase_g2_4cq.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cr = baseline["phase_g2_4cr_etf_cli_dynamic_v3_system_target_smoothed_operations"]
    assert phase_g2_4cr["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cr = phase_g2_4cr["migration"]
    assert migration_g2_4cr["callback_count"] == 15
    assert migration_g2_4cr["pre_output_live_source_validation_required"] is True
    assert migration_g2_4cr["source_chronology_required"] is True
    assert migration_g2_4cr["binding_candidate_and_targets_are_only_authority"] is True
    assert migration_g2_4cr["explicit_evidence_ids_and_exact_target_lineage_required"] is True
    assert migration_g2_4cr["cross_lineage_directory_scan_allowed"] is False
    assert migration_g2_4cr["progress_is_only_operations_lineage"] is True
    assert migration_g2_4cr["dashboard_monitor_same_progress_required"] is True
    assert migration_g2_4cr["recheck_exact_progress_candidate_lineage_required"] is True
    assert migration_g2_4cr["renewal_exact_recheck_owner_lineage_required"] is True
    assert migration_g2_4cr["fixed_method_fallback_allowed"] is False
    assert migration_g2_4cr["candidate_less_requirement_fabrication_allowed"] is False
    assert migration_g2_4cr["bounded_source_bundle_recursive_input_snapshot_allowed"] is False
    assert migration_g2_4cr["bounded_source_bundle_live_validator_required"] is True
    assert migration_g2_4cr["content_fingerprint_validation_session"] is True
    assert migration_g2_4cr["complete_file_fingerprint_required"] is True
    assert migration_g2_4cr["validation_cache_pass_only"] is True
    assert migration_g2_4cr["validation_cache_failure_reuse_allowed"] is False
    assert migration_g2_4cr["validation_cache_return_mutation_isolated"] is True
    assert len(migration_g2_4cr["snapshot_schemas"]) == 5
    assert migration_g2_4cr["content_derived_all_views_validation"] is True
    assert migration_g2_4cr["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cr["portfolio_or_execution_effect"] is False
    assert migration_g2_4cr["legacy_root_lines_after"] == 16359
    assert migration_g2_4cr["legacy_root_top_level_functions_after"] == 437
    assert migration_g2_4cr["legacy_root_command_decorators_after"] == 398
    assert migration_g2_4cr["legacy_domain_lines_after"] == 16659
    assert migration_g2_4cr["legacy_domain_top_level_functions_after"] == 713
    assert migration_g2_4cr["legacy_domain_compatibility_wrapper_count"] == 15
    assert migration_g2_4cr["python_module_count"] == 922
    assert migration_g2_4cr["python_test_file_count"] == 1122
    fixture_g2_4cr = phase_g2_4cr["fixture"]
    assert fixture_g2_4cr["candidate_method"] is None
    assert fixture_g2_4cr["bound_target_count"] == 0
    assert fixture_g2_4cr["progress_status"] == "NOT_REGISTERED"
    assert fixture_g2_4cr["progress_requirement_count"] == 0
    assert fixture_g2_4cr["progress_forward_event_count"] == 0
    assert fixture_g2_4cr["dashboard_status"] == "NOT_REGISTERED"
    assert fixture_g2_4cr["dashboard_ready_for_switch_recheck"] is False
    assert fixture_g2_4cr["event_monitor_status"] == "NOT_REGISTERED"
    assert fixture_g2_4cr["sideways_event_count"] == 0
    assert fixture_g2_4cr["recovery_event_count"] == 0
    assert fixture_g2_4cr["recheck_decision"] == "NO_ELIGIBLE_CANDIDATE"
    assert fixture_g2_4cr["recheck_criteria_count"] == 0
    assert fixture_g2_4cr["owner_decision_required"] is False
    assert fixture_g2_4cr["can_execute_switch"] is False
    assert fixture_g2_4cr["renewal_promote_available"] is False
    assert fixture_g2_4cr["renewal_recommended_action"] == "request_more_forward_data"
    assert fixture_g2_4cr["workflow_pass_is_not_investment_conclusion"] is True
    performance_g2_4cr = phase_g2_4cr["performance"]
    assert performance_g2_4cr["progress_test_baseline_seconds"] == 557.27
    assert performance_g2_4cr["progress_test_bounded_source_seconds"] == 13.60
    assert performance_g2_4cr["elapsed_reduction_percent"] >= 97.5
    assert performance_g2_4cr["largest_snapshot_reduction_percent"] >= 98.5
    if phase_g2_4cr["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cr["validation"]["operations_hardening"]["passed"] >= 1
        assert phase_g2_4cr["validation"]["five_stage_integration"]["passed"] >= 5
        assert phase_g2_4cr["validation"]["architecture_fitness"]["passed"] >= 279
        assert phase_g2_4cr["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cr["sources"]
        for source in phase_g2_4cr["sources"]:
            if source["path"] in set(phase_g2_4cr.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cs = baseline["phase_g2_4cs_etf_cli_dynamic_v3_smoothed_forward_sample_bootstrap"]
    assert phase_g2_4cs["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cs = phase_g2_4cs["migration"]
    assert migration_g2_4cs["callback_count"] == 15
    assert migration_g2_4cs["binding_candidate_and_targets_are_only_authority"] is True
    assert migration_g2_4cs["fixed_candidate_or_method_role_allowed"] is False
    assert migration_g2_4cs["candidate_less_sample_fabrication_allowed"] is False
    assert migration_g2_4cs["pre_output_live_source_validation_required"] is True
    assert migration_g2_4cs["source_chronology_required"] is True
    assert migration_g2_4cs["bounded_market_data_and_validate_data_required"] is True
    assert migration_g2_4cs["exact_emission_window_lineage_required"] is True
    assert migration_g2_4cs["cross_lineage_directory_scan_allowed"] is False
    assert migration_g2_4cs["null_preserving_calculation_required"] is True
    assert migration_g2_4cs["dynamic_candidate_baseline_required"] is True
    assert migration_g2_4cs["reporting_thresholds_are_investment_gates"] is False
    assert migration_g2_4cs["weekly_exact_nine_step_binding_required"] is True
    assert migration_g2_4cs["content_derived_all_views_validation"] is True
    assert migration_g2_4cs["source_and_output_tamper_fail_closed"] is True
    assert len(migration_g2_4cs["snapshot_schemas"]) == 5
    assert migration_g2_4cs["automatic_policy_or_execution_allowed"] is False
    assert migration_g2_4cs["portfolio_or_execution_effect"] is False
    assert migration_g2_4cs["legacy_root_lines_after"] == 15842
    assert migration_g2_4cs["legacy_root_top_level_functions_after"] == 422
    assert migration_g2_4cs["legacy_root_command_decorators_after"] == 383
    assert migration_g2_4cs["legacy_domain_lines_after"] == 14895
    assert migration_g2_4cs["legacy_domain_top_level_functions_after"] == 678
    assert migration_g2_4cs["legacy_domain_compatibility_wrapper_count"] == 15
    assert migration_g2_4cs["python_module_count"] == 924
    assert migration_g2_4cs["python_test_file_count"] == 1123
    fixture_g2_4cs = phase_g2_4cs["fixture"]
    assert fixture_g2_4cs["candidate_method"] is None
    assert fixture_g2_4cs["binding_status"] == "NOT_REGISTERED"
    assert fixture_g2_4cs["bound_target_count"] == 0
    assert fixture_g2_4cs["emitted_event_count"] == 0
    assert fixture_g2_4cs["event_status"] == "NOT_REGISTERED"
    assert fixture_g2_4cs["due_window_count"] == 0
    assert fixture_g2_4cs["updated_window_count"] == 0
    assert fixture_g2_4cs["classified_event_count"] == 0
    assert fixture_g2_4cs["recheck_decision"] == "NO_ELIGIBLE_CANDIDATE"
    assert fixture_g2_4cs["renewal_recommended_action"] == "request_more_forward_data"
    assert fixture_g2_4cs["can_execute_switch"] is False
    assert fixture_g2_4cs["workflow_pass_is_not_investment_conclusion"] is True
    performance_g2_4cs = phase_g2_4cs["performance"]
    assert performance_g2_4cs["smoothed_regression_before_seconds"] == 270.04
    assert performance_g2_4cs["smoothed_regression_after_seconds"] == 100.98
    assert performance_g2_4cs["elapsed_reduction_percent"] >= 62.6
    assert performance_g2_4cs["readiness_reduction_percent"] >= 64.9
    assert performance_g2_4cs["validation_gate_skipped"] is False
    if phase_g2_4cs["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cs["validation"]["focused_formula_lineage_and_chain"]["passed"] >= 15
        assert phase_g2_4cs["validation"]["smoothed_regression"]["passed"] >= 41
        assert phase_g2_4cs["validation"]["cli_contract"]["passed"] >= 117
        assert phase_g2_4cs["validation"]["architecture_fitness"]["passed"] >= 280
        assert phase_g2_4cs["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cs["validation"]["full_validation"]["passed"] >= 6012
        assert phase_g2_4cs["sources"]
        for source in phase_g2_4cs["sources"]:
            if source["path"] in set(phase_g2_4cs.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4ct = baseline["phase_g2_4ct_etf_cli_dynamic_v3_smoothed_data_freshness"]
    assert phase_g2_4ct["status"] in {"VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4ct = phase_g2_4ct["migration"]
    assert migration_g2_4ct["callback_count"] == 15
    assert migration_g2_4ct["legacy_domain_is_lazy_wrapper_only"] is True
    assert len(migration_g2_4ct["snapshot_schemas"]) == 5
    assert migration_g2_4ct["same_validate_data_path_and_live_replay_required"] is True
    assert migration_g2_4ct["strict_unique_cutoff_model_target_required"] is True
    assert migration_g2_4ct["invalid_or_ambiguous_target_silently_null_allowed"] is False
    assert migration_g2_4ct["exact_preflight_latest_explain_refresh_lineage_required"] is True
    assert migration_g2_4ct["retry_validated_weekly_or_latest_child_required"] is True
    assert migration_g2_4ct["blocked_retry_downstream_execution_allowed"] is False
    assert migration_g2_4ct["ready_explain_zero_blockers_allowed"] is True
    assert migration_g2_4ct["current_null_candidate_emission_count"] == 0
    assert migration_g2_4ct["current_null_candidate_event_status"] == "NOT_REGISTERED"
    assert migration_g2_4ct["content_derived_all_views_validation"] is True
    assert migration_g2_4ct["source_and_output_tamper_fail_closed"] is True
    assert migration_g2_4ct["validation_session_cache_reuse_enabled"] is True
    assert migration_g2_4ct["validation_gate_reduced_for_performance"] is False
    assert migration_g2_4ct["automatic_refresh_policy_or_execution_allowed"] is False
    assert migration_g2_4ct["portfolio_or_execution_effect"] is False
    assert migration_g2_4ct["legacy_root_lines_after"] == 15373
    assert migration_g2_4ct["legacy_root_top_level_functions_after"] == 407
    assert migration_g2_4ct["legacy_root_command_decorators_after"] == 368
    assert migration_g2_4ct["legacy_domain_lines_after"] == 13978
    assert migration_g2_4ct["legacy_domain_top_level_functions_after"] == 679
    assert migration_g2_4ct["legacy_domain_compatibility_wrapper_count"] == 15
    assert migration_g2_4ct["python_module_count"] == 926
    assert migration_g2_4ct["python_test_file_count"] == 1124
    fixture_g2_4ct = phase_g2_4ct["fixture"]
    assert fixture_g2_4ct["stale_freshness_status"] == "BLOCKED_STALE_DATA"
    assert str(fixture_g2_4ct["stale_latest_valid_as_of"]) == "2026-01-08"
    assert fixture_g2_4ct["latest_emitted_event_count"] == 0
    assert fixture_g2_4ct["latest_event_status"] == "NOT_REGISTERED"
    assert fixture_g2_4ct["ready_blocked_command_count"] == 0
    assert fixture_g2_4ct["cross_preflight_refresh_allowed"] is False
    assert fixture_g2_4ct["weekly_child_tamper_fails_retry"] is True
    assert fixture_g2_4ct["can_execute_switch"] is False
    if phase_g2_4ct["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4ct["validation"]["combined_focused"]["passed"] >= 125
        assert phase_g2_4ct["validation"]["architecture_fitness"]["passed"] >= 281
        assert phase_g2_4ct["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4ct["validation"]["full_validation"]["passed"] >= 6012
        assert phase_g2_4ct["sources"]
        smoothed_interface_source = next(
            source
            for source in phase_g2_4ct["sources"]
            if source["path"].endswith("dynamic_v3_system_target_smoothed_freshness.py")
            and "/interfaces/" in source["path"]
        )
        assert smoothed_interface_source["hash_normalization"] == "git_eol_lf"
        assert smoothed_interface_source["previous_worktree_sha256"] == (
            "bc268a1292730b9751c5febe2702dd1a456b85b7e74b6b56d7efc4508fe4b8d7"
        )
        for source in phase_g2_4ct["sources"]:
            if source["path"] in set(phase_g2_4ct.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cu = baseline["phase_g2_4cu_etf_cli_dynamic_v3_smoothed_data_refresh_operations"]
    assert phase_g2_4cu["status"] in {"IN_PROGRESS", "VALIDATING", "COMPLETE_G2_4_CONTINUES"}
    migration_g2_4cu = phase_g2_4cu["migration"]
    assert migration_g2_4cu["callback_count"] == 16
    assert migration_g2_4cu["domain_entrypoint_count"] == 15
    assert len(migration_g2_4cu["snapshot_schemas"]) == 5
    assert (
        migration_g2_4cu[
            "exact_refresh_plan_preflight_refresh_post_resume_growth_readiness_lineage_required"
        ]
        is True
    )
    assert migration_g2_4cu["explicit_execute_authorization_required"] is True
    assert migration_g2_4cu["dry_run_before_after_identity_required"] is True
    assert migration_g2_4cu["execute_before_after_commitments_required"] is True
    assert migration_g2_4cu["validator_provider_side_effect_replay_allowed"] is False
    assert migration_g2_4cu["blocked_downstream_child_generation_allowed"] is False
    assert migration_g2_4cu["content_derived_all_views_validation_required"] is True
    assert migration_g2_4cu["real_provider_blocker_may_be_fabricated_away"] is False
    assert migration_g2_4cu["can_execute_switch"] is False
    assert migration_g2_4cu["production_effect"] == "none"
    performance_g2_4cu = phase_g2_4cu["performance"]
    assert performance_g2_4cu["validation_session_nested_source_cache_reuse"] is True
    assert performance_g2_4cu["full_gate_reduced_for_performance"] is False
    assert performance_g2_4cu["observed_single_run_reduction_percent"] >= 22.8
    assert performance_g2_4cu["stable_full_improvement_claimed"] is False
    assert performance_g2_4cu["duration_and_peak_memory_aware_sharding_required"] is True
    assert performance_g2_4cu["active_node_heartbeat_required"] is True
    if phase_g2_4cu["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cu["validation"]["focused"]["passed"] >= 123
        assert phase_g2_4cu["validation"]["architecture_fitness"]["passed"] >= 282
        assert phase_g2_4cu["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cu["validation"]["full_validation"]["passed"] >= 6023
        assert phase_g2_4cu["sources"]
        for source in phase_g2_4cu["sources"]:
            if source["path"] in set(phase_g2_4cu.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cv1 = baseline["phase_g2_4cv1_etf_cli_dynamic_v3_weight_search_foundation"]
    assert phase_g2_4cv1["status"] in {
        "IN_PROGRESS",
        "VALIDATING",
        "COMPLETE_G2_4_CONTINUES",
    }
    migration_g2_4cv1 = phase_g2_4cv1["migration"]
    assert migration_g2_4cv1["callback_count"] == 10
    assert migration_g2_4cv1["domain_entrypoint_count"] == 12
    assert len(migration_g2_4cv1["snapshot_schemas"]) == 3
    assert migration_g2_4cv1["exact_search_to_matrix_to_backfill_lineage_required"] is True
    assert migration_g2_4cv1["exact_paper_backfill_binding_required"] is True
    assert migration_g2_4cv1["same_validate_data_path_required"] is True
    assert migration_g2_4cv1["live_config_and_cache_commitments_required"] is True
    assert migration_g2_4cv1["content_derived_all_views_validation_required"] is True
    assert migration_g2_4cv1["resume_requires_existing_artifact_pass"] is True
    assert migration_g2_4cv1["broker_action_allowed"] is False
    assert migration_g2_4cv1["production_effect"] == "none"
    subtraction_g2_4cv1 = phase_g2_4cv1["subtraction"]
    assert subtraction_g2_4cv1["legacy_cli_lines_after"] == 14551
    assert subtraction_g2_4cv1["legacy_cli_top_level_functions_after"] == 381
    assert subtraction_g2_4cv1["legacy_cli_callback_reduction"] == 10
    hardening_g2_4cv1 = phase_g2_4cv1["hardening"]
    assert hardening_g2_4cv1["search_emitted_views_tamper_checked"] == 4
    assert hardening_g2_4cv1["matrix_emitted_views_tamper_checked"] == 4
    assert hardening_g2_4cv1["backfill_emitted_views_tamper_checked"] == 10
    assert hardening_g2_4cv1["snapshot_schema_tamper_checked"] == 3
    assert hardening_g2_4cv1["tampered_progress_blocks_resume"] is True
    performance_g2_4cv1 = phase_g2_4cv1["performance"]
    assert performance_g2_4cv1["full_gate_reduced_for_performance"] is False
    assert performance_g2_4cv1["observed_regression_percent"] >= 28.9
    assert performance_g2_4cv1["stable_full_improvement_claimed"] is False
    assert performance_g2_4cv1["current_slice_hardening_in_slowest_50"] is False
    assert performance_g2_4cv1["duration_and_peak_memory_aware_sharding_required"] is True
    assert performance_g2_4cv1["active_node_heartbeat_and_eta_required"] is True
    if phase_g2_4cv1["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cv1["validation"]["focused"]["passed"] >= 124
        assert phase_g2_4cv1["validation"]["architecture_fitness"]["passed"] >= 282
        assert phase_g2_4cv1["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cv1["validation"]["full_validation"]["passed"] >= 6023
        assert phase_g2_4cv1["sources"]
        for source in phase_g2_4cv1["sources"]:
            if source["path"] in set(phase_g2_4cv1.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cv2 = baseline["phase_g2_4cv2_etf_cli_dynamic_v3_weight_search_evaluation"]
    assert phase_g2_4cv2["status"] in {
        "IN_PROGRESS",
        "VALIDATING",
        "COMPLETE_G2_4_CONTINUES",
    }
    migration_g2_4cv2 = phase_g2_4cv2["migration"]
    assert migration_g2_4cv2["callback_count"] == 11
    assert migration_g2_4cv2["domain_entrypoint_count"] == 11
    assert len(migration_g2_4cv2["snapshot_schemas"]) == 3
    assert migration_g2_4cv2["exact_backfill_matrix_to_scorecard_lineage_required"] is True
    assert migration_g2_4cv2["exact_scorecard_backfill_to_robustness_lineage_required"] is True
    assert migration_g2_4cv2["same_lineage_scorecard_robustness_to_adaptive_required"] is True
    assert migration_g2_4cv2["validated_branch_authorizes_expanded_matrix_required"] is True
    assert (
        migration_g2_4cv2["canonical_matrix_and_data_quality_backfill_delegation_required"] is True
    )
    assert migration_g2_4cv2["content_derived_all_views_validation_required"] is True
    assert migration_g2_4cv2["broker_action_allowed"] is False
    assert migration_g2_4cv2["production_effect"] == "none"
    subtraction_g2_4cv2 = phase_g2_4cv2["subtraction"]
    assert subtraction_g2_4cv2["legacy_cli_lines_after"] == 14262
    assert subtraction_g2_4cv2["legacy_cli_top_level_functions_after"] == 370
    assert subtraction_g2_4cv2["legacy_cli_callback_reduction"] == 11
    hardening_g2_4cv2 = phase_g2_4cv2["hardening"]
    assert hardening_g2_4cv2["scorecard_emitted_views_tamper_checked"] == 5
    assert hardening_g2_4cv2["robustness_emitted_views_tamper_checked"] == 6
    assert hardening_g2_4cv2["adaptive_emitted_views_tamper_checked"] == 3
    assert hardening_g2_4cv2["snapshot_schema_tamper_checked"] == 3
    assert hardening_g2_4cv2["cross_lineage_adaptive_fails"] is True
    assert hardening_g2_4cv2["tampered_branch_blocks_expanded_run"] is True
    if phase_g2_4cv2["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cv2["validation"]["focused"]["passed"] >= 125
        assert phase_g2_4cv2["validation"]["architecture_fitness"]["passed"] >= 284
        assert phase_g2_4cv2["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cv2["validation"]["full_validation"]["passed"] >= 6026
        assert phase_g2_4cv2["sources"]
        for source in phase_g2_4cv2["sources"]:
            if source["path"] in set(phase_g2_4cv2.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cv3 = baseline["phase_g2_4cv3_etf_cli_dynamic_v3_weight_search_decision"]
    assert phase_g2_4cv3["status"] in {
        "IN_PROGRESS",
        "VALIDATING",
        "COMPLETE_G2_4_CONTINUES",
    }
    migration_g2_4cv3 = phase_g2_4cv3["migration"]
    assert migration_g2_4cv3["callback_count"] == 18
    assert migration_g2_4cv3["domain_entrypoint_count"] == 18
    assert len(migration_g2_4cv3["snapshot_schemas"]) == 6
    assert migration_g2_4cv3["same_lineage_scorecard_robustness_to_cluster_required"] is True
    assert (
        migration_g2_4cv3["exact_cluster_to_interpretation_to_gate_to_plan_lineage_required"]
        is True
    )
    assert (
        migration_g2_4cv3["same_lineage_scorecard_adaptive_optional_gate_to_dashboard_required"]
        is True
    )
    assert migration_g2_4cv3["exact_dashboard_to_owner_pack_lineage_required"] is True
    assert migration_g2_4cv3["content_derived_all_views_validation_required"] is True
    assert migration_g2_4cv3["formal_plan_implemented"] is False
    assert migration_g2_4cv3["owner_options_manual_only"] is True
    assert migration_g2_4cv3["broker_action_allowed"] is False
    assert migration_g2_4cv3["production_effect"] == "none"
    subtraction_g2_4cv3 = phase_g2_4cv3["subtraction"]
    assert subtraction_g2_4cv3["legacy_cli_lines_after"] == 13828
    assert subtraction_g2_4cv3["legacy_cli_top_level_functions_after"] == 352
    assert subtraction_g2_4cv3["legacy_cli_callback_reduction"] == 18
    hardening_g2_4cv3 = phase_g2_4cv3["hardening"]
    assert hardening_g2_4cv3["total_emitted_views_tamper_checked"] == 27
    assert hardening_g2_4cv3["snapshot_schema_tamper_checked"] == 6
    assert hardening_g2_4cv3["cross_lineage_tamper_checked"] == 3
    performance_g2_4cv3 = phase_g2_4cv3["performance"]
    assert performance_g2_4cv3["immutable_fixture_built_once"] is True
    assert performance_g2_4cv3["recursive_baseline_validation_replay_still_present"] is True
    assert performance_g2_4cv3["full_gate_reduced_for_performance"] is False
    if phase_g2_4cv3["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cv3["validation"]["focused"]["passed"] >= 132
        assert phase_g2_4cv3["validation"]["architecture_fitness"]["passed"] >= 285
        assert phase_g2_4cv3["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cv3["validation"]["full_validation"]["passed"] >= 6029
        assert phase_g2_4cv3["sources"]
        for source in phase_g2_4cv3["sources"]:
            if source["path"] in set(phase_g2_4cv3.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cw1 = baseline["phase_g2_4cw1_etf_cli_dynamic_v3_weight_search_diagnostics"]
    assert phase_g2_4cw1["status"] in {
        "IN_PROGRESS",
        "VALIDATING",
        "COMPLETE_G2_4_CONTINUES",
    }
    migration_g2_4cw1 = phase_g2_4cw1["migration"]
    assert migration_g2_4cw1["callback_count"] == 12
    assert migration_g2_4cw1["domain_entrypoint_count"] == 12
    assert len(migration_g2_4cw1["snapshot_schemas"]) == 4
    assert (
        migration_g2_4cw1["exact_scorecard_to_review_to_near_miss_to_attribution_lineage_required"]
        is True
    )
    assert migration_g2_4cw1["exact_search_space_to_coverage_lineage_required"] is True
    assert migration_g2_4cw1["same_scorecard_and_policy_binding_required"] is True
    assert migration_g2_4cw1["content_derived_all_views_validation_required"] is True
    assert migration_g2_4cw1["live_source_and_policy_replay_required"] is True
    assert migration_g2_4cw1["broker_action_allowed"] is False
    assert migration_g2_4cw1["production_effect"] == "none"
    subtraction_g2_4cw1 = phase_g2_4cw1["subtraction"]
    assert subtraction_g2_4cw1["legacy_cli_lines_after"] == 13522
    assert subtraction_g2_4cw1["legacy_cli_top_level_functions_after"] == 340
    assert subtraction_g2_4cw1["legacy_cli_callback_reduction"] == 12
    assert subtraction_g2_4cw1["legacy_domain_lazy_wrapper_count"] == 12
    hardening_g2_4cw1 = phase_g2_4cw1["hardening"]
    assert hardening_g2_4cw1["total_emitted_views_tamper_checked"] == 21
    assert hardening_g2_4cw1["snapshot_schema_tamper_checked"] == 4
    assert hardening_g2_4cw1["cross_lineage_tamper_checked"] == 3
    assert hardening_g2_4cw1["policy_binding_tamper_checked"] is True
    performance_g2_4cw1 = phase_g2_4cw1["performance"]
    assert performance_g2_4cw1["stable_improvement_claimed"] is False
    assert performance_g2_4cw1["full_gate_reduced_for_performance"] is False
    if phase_g2_4cw1["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cw1["validation"]["focused"]["passed"] >= 132
        assert phase_g2_4cw1["validation"]["architecture_fitness"]["passed"] >= 286
        assert phase_g2_4cw1["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cw1["validation"]["full_validation"]["passed"] >= 6030
        assert phase_g2_4cw1["sources"]
        for source in phase_g2_4cw1["sources"]:
            if source["path"] in set(phase_g2_4cw1.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cw2 = baseline["phase_g2_4cw2_etf_cli_dynamic_v3_weight_search_targeted"]
    assert phase_g2_4cw2["status"] in {
        "IN_PROGRESS",
        "VALIDATING",
        "COMPLETE_G2_4_CONTINUES",
    }
    migration_g2_4cw2 = phase_g2_4cw2["migration"]
    assert migration_g2_4cw2["callback_count"] == 10
    assert migration_g2_4cw2["domain_entrypoint_count"] == 10
    assert len(migration_g2_4cw2["snapshot_schemas"]) == 3
    assert (
        migration_g2_4cw2["exact_coverage_near_miss_scorecard_to_matrix_lineage_required"] is True
    )
    assert migration_g2_4cw2["exact_matrix_weight_paper_backfill_lineage_required"] is True
    assert (
        migration_g2_4cw2["exact_backfill_matrix_near_miss_scorecard_to_ab_lineage_required"]
        is True
    )
    assert migration_g2_4cw2["reviewed_targeted_policy_required"] is True
    assert migration_g2_4cw2["pre_output_data_quality_gate_required"] is True
    assert migration_g2_4cw2["backfill_resume_prior_pass_required"] is True
    assert migration_g2_4cw2["live_source_policy_cache_and_dq_replay_required"] is True
    assert migration_g2_4cw2["broker_action_allowed"] is False
    assert migration_g2_4cw2["production_effect"] == "none"
    subtraction_g2_4cw2 = phase_g2_4cw2["subtraction"]
    assert subtraction_g2_4cw2["legacy_cli_lines_after"] == 13269
    assert subtraction_g2_4cw2["legacy_cli_top_level_functions_after"] == 330
    assert subtraction_g2_4cw2["legacy_cli_callback_reduction"] == 10
    assert subtraction_g2_4cw2["legacy_domain_lazy_wrapper_count"] == 10
    hardening_g2_4cw2 = phase_g2_4cw2["hardening"]
    assert hardening_g2_4cw2["total_emitted_views_tamper_checked"] == 16
    assert hardening_g2_4cw2["snapshot_schema_tamper_checked"] == 3
    assert hardening_g2_4cw2["cross_lineage_tamper_checked"] == 3
    assert hardening_g2_4cw2["policy_binding_tamper_checked"] is True
    assert hardening_g2_4cw2["price_and_rates_binding_tamper_checked"] == 2
    assert hardening_g2_4cw2["resume_tampered_or_incomplete_source_fail_closed_checked"] is True
    performance_g2_4cw2 = phase_g2_4cw2["performance"]
    assert performance_g2_4cw2["stable_improvement_claimed"] is False
    assert performance_g2_4cw2["full_gate_reduced_for_performance"] is False
    if phase_g2_4cw2["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cw2["validation"]["focused"]["passed"] >= 132
        assert phase_g2_4cw2["validation"]["architecture_fitness"]["passed"] >= 286
        assert phase_g2_4cw2["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cw2["validation"]["full_validation"]["passed"] >= 6030
        assert phase_g2_4cw2["sources"]
        for source in phase_g2_4cw2["sources"]:
            if source["path"] in set(phase_g2_4cw2.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cw3 = baseline["phase_g2_4cw3_etf_cli_dynamic_v3_weight_search_followup"]
    assert phase_g2_4cw3["status"] in {
        "IN_PROGRESS",
        "VALIDATING",
        "COMPLETE_G2_4_CONTINUES",
    }
    migration_g2_4cw3 = phase_g2_4cw3["migration"]
    assert migration_g2_4cw3["callback_count"] == 9
    assert migration_g2_4cw3["domain_entrypoint_count"] == 9
    assert len(migration_g2_4cw3["snapshot_schemas"]) == 3
    assert (
        migration_g2_4cw3["exact_backfill_matrix_ab_scorecard_near_miss_lineage_required"] is True
    )
    assert migration_g2_4cw3["exact_sensitivity_to_promotion_lineage_required"] is True
    assert migration_g2_4cw3["exact_promotion_to_next_plan_lineage_required"] is True
    assert migration_g2_4cw3["reviewed_followup_policy_required"] is True
    assert migration_g2_4cw3["relaxed_threshold_diagnostic_only_required"] is True
    assert migration_g2_4cw3["owner_review_required"] is True
    assert migration_g2_4cw3["implemented"] is False
    assert migration_g2_4cw3["formal_method_task_created"] is False
    assert migration_g2_4cw3["broker_action_allowed"] is False
    assert migration_g2_4cw3["production_effect"] == "none"
    subtraction_g2_4cw3 = phase_g2_4cw3["subtraction"]
    assert subtraction_g2_4cw3["legacy_cli_lines_after"] == 13016
    assert subtraction_g2_4cw3["legacy_cli_top_level_functions_after"] == 321
    assert subtraction_g2_4cw3["legacy_cli_callback_reduction"] == 9
    assert subtraction_g2_4cw3["legacy_domain_lazy_wrapper_count"] == 9
    hardening_g2_4cw3 = phase_g2_4cw3["hardening"]
    assert hardening_g2_4cw3["total_emitted_views_tamper_checked"] == 18
    assert hardening_g2_4cw3["snapshot_schema_tamper_checked"] == 3
    assert hardening_g2_4cw3["cross_lineage_tamper_checked"] == 3
    assert hardening_g2_4cw3["policy_binding_tamper_checked"] is True
    assert hardening_g2_4cw3["live_price_binding_cache_invalidation_checked"] is True
    performance_g2_4cw3 = phase_g2_4cw3["performance"]
    assert performance_g2_4cw3["pass_only_content_addressed_validation_session"] is True
    assert performance_g2_4cw3["cache_key_includes_recursive_live_bindings"] is True
    assert performance_g2_4cw3["fail_results_cached"] is False
    assert performance_g2_4cw3["observed_minimum_wall_time_improvement_percent"] >= 78.5
    assert performance_g2_4cw3["stable_full_suite_improvement_claimed"] is False
    assert performance_g2_4cw3["full_gate_reduced_for_performance"] is False
    if phase_g2_4cw3["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cw3["validation"]["focused"]["passed"] >= 136
        assert phase_g2_4cw3["validation"]["architecture_fitness"]["passed"] >= 289
        assert phase_g2_4cw3["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cw3["validation"]["full_validation"]["passed"] >= 6035
        assert phase_g2_4cw3["sources"]
        for source in phase_g2_4cw3["sources"]:
            if source["path"] in set(phase_g2_4cw3.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cx1 = baseline["phase_g2_4cx1_etf_cli_dynamic_v3_signal_diagnosis_foundation"]
    assert phase_g2_4cx1["status"] in {
        "IN_PROGRESS",
        "VALIDATING",
        "COMPLETE_G2_4_CONTINUES",
    }
    migration_g2_4cx1 = phase_g2_4cx1["migration"]
    assert migration_g2_4cx1["callback_count"] == 12
    assert migration_g2_4cx1["domain_entrypoint_count"] == 12
    assert len(migration_g2_4cx1["snapshot_schemas"]) == 4
    assert migration_g2_4cx1["no_dated_signal_event_fabrication_required"] is True
    assert migration_g2_4cx1["no_consensus_variant_fallback_required"] is True
    assert migration_g2_4cx1["missing_observation_null_and_insufficient_data_required"] is True
    assert migration_g2_4cx1["broker_action_allowed"] is False
    assert migration_g2_4cx1["production_effect"] == "none"
    subtraction_g2_4cx1 = phase_g2_4cx1["subtraction"]
    assert subtraction_g2_4cx1["legacy_cli_lines_after"] == 12693
    assert subtraction_g2_4cx1["legacy_cli_top_level_functions_after"] == 309
    assert subtraction_g2_4cx1["legacy_cli_callback_decorators_after"] == 270
    assert subtraction_g2_4cx1["legacy_cli_callback_reduction"] == 12
    assert subtraction_g2_4cx1["legacy_domain_lazy_wrapper_count"] == 12
    matrix_g2_4cx1 = phase_g2_4cx1["callback_matrix"]
    assert matrix_g2_4cx1["migrated_callback_count"] == 697
    assert matrix_g2_4cx1["pending_callback_count"] == 270
    assert matrix_g2_4cx1["phase_exit_ready"] is False
    hardening_g2_4cx1 = phase_g2_4cx1["hardening"]
    assert hardening_g2_4cx1["output_artifact_family_tamper_checked"] == 4
    assert hardening_g2_4cx1["snapshot_schema_tamper_checked"] == 4
    assert hardening_g2_4cx1["live_source_tamper_fail_closed_checked"] is True
    performance_g2_4cx1 = phase_g2_4cx1["performance"]
    assert performance_g2_4cx1["observed_wall_time_improvement_percent"] >= 61.0
    assert performance_g2_4cx1["test_policy_required_family_count"] == 6
    assert performance_g2_4cx1["production_policy_min_variant_count"] == 60
    assert performance_g2_4cx1["production_policy_max_variant_count"] == 120
    assert performance_g2_4cx1["stable_full_suite_improvement_claimed"] is False
    assert performance_g2_4cx1["full_gate_reduced_for_performance"] is False
    if phase_g2_4cx1["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cx1["validation"]["focused"]["passed"] >= 166
        assert phase_g2_4cx1["validation"]["architecture_fitness"]["passed"] >= 292
        assert phase_g2_4cx1["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cx1["validation"]["full_validation"]["passed"] >= 6040
        assert phase_g2_4cx1["sources"]
        for source in phase_g2_4cx1["sources"]:
            if source["path"] in set(phase_g2_4cx1.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cx2 = baseline["phase_g2_4cx2_etf_cli_dynamic_v3_micro_search_foundation"]
    assert phase_g2_4cx2["status"] in {
        "IN_PROGRESS",
        "VALIDATING",
        "COMPLETE_G2_4_CONTINUES",
    }
    migration_g2_4cx2 = phase_g2_4cx2["migration"]
    assert migration_g2_4cx2["callback_count"] == 12
    assert migration_g2_4cx2["domain_entrypoint_count"] == 12
    assert len(migration_g2_4cx2["snapshot_schemas"]) == 4
    assert migration_g2_4cx2["reviewed_micro_search_policy_required"] is True
    assert migration_g2_4cx2["exact_cx1_scorecard_matrix_backfill_lineage_required"] is True
    assert migration_g2_4cx2["exact_design_backfill_gate_lineage_required"] is True
    assert (
        migration_g2_4cx2["historical_calculation_and_current_quality_cache_roles_required"] is True
    )
    assert migration_g2_4cx2["insufficient_dated_evidence_inconclusive_required"] is True
    assert migration_g2_4cx2["market_regime_default_attribution_forbidden"] is True
    assert migration_g2_4cx2["broker_action_allowed"] is False
    assert migration_g2_4cx2["production_effect"] == "none"
    subtraction_g2_4cx2 = phase_g2_4cx2["subtraction"]
    assert subtraction_g2_4cx2["legacy_cli_lines_after"] == 12339
    assert subtraction_g2_4cx2["legacy_cli_top_level_functions_after"] == 297
    assert subtraction_g2_4cx2["legacy_cli_callback_decorators_after"] == 258
    assert subtraction_g2_4cx2["legacy_cli_callback_reduction"] == 12
    assert subtraction_g2_4cx2["legacy_domain_lazy_wrapper_count"] == 12
    matrix_g2_4cx2 = phase_g2_4cx2["callback_matrix"]
    assert matrix_g2_4cx2["migrated_callback_count"] == 709
    assert matrix_g2_4cx2["pending_callback_count"] == 258
    assert matrix_g2_4cx2["phase_exit_ready"] is False
    hardening_g2_4cx2 = phase_g2_4cx2["hardening"]
    assert hardening_g2_4cx2["output_artifact_family_tamper_checked"] == 4
    assert hardening_g2_4cx2["snapshot_policy_binding_tamper_checked"] == 4
    assert hardening_g2_4cx2["cross_lineage_fail_closed_checked"] is True
    performance_g2_4cx2 = phase_g2_4cx2["performance"]
    assert performance_g2_4cx2["observed_wall_time_improvement_percent"] >= 92.2
    assert performance_g2_4cx2["observed_speedup_ratio"] >= 12.8
    assert performance_g2_4cx2["stable_full_suite_improvement_claimed"] is False
    assert performance_g2_4cx2["full_gate_reduced_for_performance"] is False
    if phase_g2_4cx2["status"] == "COMPLETE_G2_4_CONTINUES":
        assert phase_g2_4cx2["validation"]["focused"]["passed"] >= 169
        assert phase_g2_4cx2["validation"]["architecture_fitness"]["passed"] >= 298
        assert phase_g2_4cx2["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cx2["validation"]["full_validation"]["passed"] >= 6047
        assert phase_g2_4cx2["sources"]
        for source in phase_g2_4cx2["sources"]:
            if source["path"] in set(phase_g2_4cx2.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    phase_g2_4cx3 = baseline["phase_g2_4cx3_etf_cli_dynamic_v3_research_direction_foundation"]
    assert phase_g2_4cx3["status"] in {
        "IN_PROGRESS",
        "VALIDATING",
        "COMPLETE_G2_4_CONTINUES",
    }
    migration_g2_4cx3 = phase_g2_4cx3["migration"]
    assert migration_g2_4cx3["callback_count"] == 6
    assert migration_g2_4cx3["domain_entrypoint_count"] == 6
    assert migration_g2_4cx3["snapshot_schemas"] == [
        "next_research_direction_input_snapshot.v2",
        "owner_research_roadmap_input_snapshot.v2",
    ]
    assert migration_g2_4cx3["reviewed_direction_policy_required"] is True
    assert migration_g2_4cx3["exact_attribution_to_direction_to_roadmap_lineage_required"] is True
    assert migration_g2_4cx3["insufficient_evidence_defer_mapping_required"] is True
    assert migration_g2_4cx3["unknown_shift_default_forbidden"] is True
    assert migration_g2_4cx3["historical_downstream_is_not_current_evidence_required"] is True
    assert migration_g2_4cx3["proposed_owner_review_only_required"] is True
    assert migration_g2_4cx3["broker_action_allowed"] is False
    assert migration_g2_4cx3["production_effect"] == "none"
    if phase_g2_4cx3["status"] == "COMPLETE_G2_4_CONTINUES":
        subtraction_g2_4cx3 = phase_g2_4cx3["subtraction"]
        assert subtraction_g2_4cx3["legacy_cli_lines_after"] == 12196
        assert subtraction_g2_4cx3["legacy_cli_top_level_functions_after"] == 291
        assert subtraction_g2_4cx3["legacy_cli_callback_decorators_after"] == 252
        assert subtraction_g2_4cx3["legacy_cli_callback_reduction"] == 6
        assert subtraction_g2_4cx3["legacy_domain_lazy_wrapper_count"] == 6
        matrix_g2_4cx3 = phase_g2_4cx3["callback_matrix"]
        assert matrix_g2_4cx3["migrated_callback_count"] == 715
        assert matrix_g2_4cx3["pending_callback_count"] == 252
        assert matrix_g2_4cx3["phase_exit_ready"] is False
        hardening_g2_4cx3 = phase_g2_4cx3["hardening"]
        assert hardening_g2_4cx3["output_view_tamper_checked"] == 10
        assert hardening_g2_4cx3["snapshot_policy_binding_tamper_checked"] == 2
        assert hardening_g2_4cx3["snapshot_safety_tamper_checked"] == 2
        assert hardening_g2_4cx3["source_binding_file_set_tamper_checked"] == 2
        assert hardening_g2_4cx3["source_binding_path_tamper_checked"] == 2
        assert hardening_g2_4cx3["cross_lineage_fail_closed_checked"] is True
        assert hardening_g2_4cx3["evidence_status_shift_mismatch_fail_closed_checked"] is True
        assert hardening_g2_4cx3["sufficient_evidence_checklist_branch_checked"] is True
        assert hardening_g2_4cx3["all_materialized_views_byte_rebuild_required"] is True
        performance_g2_4cx3 = phase_g2_4cx3["performance"]
        assert performance_g2_4cx3["stable_full_suite_improvement_claimed"] is False
        assert performance_g2_4cx3["full_gate_reduced_for_performance"] is False
        assert phase_g2_4cx3["validation"]["focused"]["passed"] >= 28
        assert phase_g2_4cx3["validation"]["architecture_fitness"]["passed"] >= 300
        assert phase_g2_4cx3["validation"]["contract_validation"]["passed"] >= 203
        assert phase_g2_4cx3["validation"]["full_validation"]["passed"] >= 6050
        assert phase_g2_4cx3["sources"]
        for source in phase_g2_4cx3["sources"]:
            if source["path"] in set(phase_g2_4cx3.get("superseded_source_paths", [])):
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    eb0_s2b = baseline["integrated_change_arch_004g2_eb0_s2b"]
    assert eb0_s2b["status"] == "VALIDATING"
    assert eb0_s2b["task_id"] == "ARCH-004G2_REMAINING_PHASE_EFFICIENCY_EXECUTION"
    assert eb0_s2b["behavior"] == "bounded_high_node_compatibility_fingerprint_reuse"
    assert eb0_s2b["root_cause"] == {
        "snapshot_size_bytes": 9_531_096,
        "snapshot_json_node_count": 226_197,
        "snapshot_bound_path_count": 45,
        "previous_json_node_limit": 100_000,
        "current_json_node_limit": 500_000,
        "pre_fix_observed_read_gib": 171.05,
        "pre_fix_elapsed_lower_bound_seconds": 947.08,
    }
    assert eb0_s2b["safety"] == {
        "max_document_size_bytes": 64 * 1024 * 1024,
        "max_bound_path_count": 4_096,
        "pass_only_cache": True,
        "before_after_fingerprint_required": True,
        "link_and_topology_gate_preserved": True,
        "above_node_limit_bypasses_cache": True,
        "production_effect": "none",
    }
    assert eb0_s2b["validation"]["cache_hardening"]["passed"] == 78
    assert eb0_s2b["validation"]["same_node_same_command"] == {
        "status": "PASS",
        "passed": 1,
        "elapsed_seconds": 172.23,
        "slowest_call_seconds": 168.66,
    }
    assert eb0_s2b["validation"]["smoothed_focused"] == {
        "status": "PASS",
        "passed": 27,
        "elapsed_seconds": 498.56,
    }
    assert eb0_s2b["validation"]["confirmation_targets_focused"] == {
        "status": "PASS",
        "passed": 10,
        "elapsed_seconds": 85.37,
        "pre_fix_elapsed_lower_bound_seconds": 665.0,
        "reduction_lower_bound_percent": 87.2,
    }
    assert eb0_s2b["validation"]["advisory_proposal_review_focused"] == {
        "status": "PASS",
        "passed": 13,
        "elapsed_seconds": 119.35,
        "isolated_worker_elapsed_seconds": 450.0,
        "reduction_percent_approx": 73.5,
    }
    assert eb0_s2b["validation"]["forward_plan_and_rule_review_focused"] == {
        "status": "PASS",
        "passed": 22,
        "elapsed_seconds": 209.97,
        "forward_plan_reduction_percent_approx": 65.0,
        "rule_review_reduction_percent_approx": 70.0,
    }
    assert eb0_s2b["validation"]["confirmation_direct_chain_focused"] == {
        "status": "PASS",
        "passed": 45,
        "elapsed_seconds": 235.97,
    }
    assert eb0_s2b["validation"]["correctness_shards"] == {
        "status": "PASS",
        "passed": 5_782,
        "skipped": 1,
        "failed": 0,
        "junit_wall_seconds": [246.411, 371.109, 313.790, 249.942],
    }
    assert eb0_s2b["validation"]["historical_top45_diagnostic"] == {
        "status": "DIAGNOSTIC_STOP",
        "elapsed_seconds": 1_253,
        "full_pass_claimed": False,
        "peak_working_set_gib": 6.33,
        "peak_private_gib": 17.84,
        "available_memory_gib": 62.0,
        "residual_owner": "weight_search",
        "active_file_count": 6,
        "active_files": [
            "tests/test_weight_expanded_search.py",
            "tests/test_formal_method_auto_plan.py",
            "tests/test_near_miss_ab_comparison.py",
            "tests/test_search_coverage_gap.py",
            "tests/test_weight_top_candidate_interpretation.py",
            "tests/test_weight_candidate_cluster.py",
        ],
    }
    assert eb0_s2b["validation"]["formal_focused"] == {
        "status": "PASS",
        "passed": 274,
        "skipped": 1,
        "elapsed_seconds": 248.21,
        "junit_artifact": (
            "outputs/validation_runtime/arch004g2-eb0-focused_20260717T073027Z/junit.xml"
        ),
    }
    assert eb0_s2b["validation"]["architecture_fitness_first_clean_candidate"] == {
        "status": "FAIL",
        "passed": 310,
        "failed": 1,
        "elapsed_seconds": 62.70,
        "runtime_artifact": (
            "outputs/validation_runtime/architecture-fitness_20260717T073451Z/"
            "test_runtime_summary.json"
        ),
        "reason": "deprecation_inventory_raw_worktree_eol_hash_drift",
    }
    assert eb0_s2b["validation"]["architecture_fitness"]["status"] == "PASS"
    assert eb0_s2b["validation"]["architecture_fitness"]["passed"] == 312
    assert eb0_s2b["validation"]["contract_validation"]["status"] == "PASS"
    assert eb0_s2b["validation"]["contract_validation"]["passed"] == 204
    assert eb0_s2b["validation"]["full_validation"] == {
        "status": "PASS",
        "passed": 6_195,
        "skipped": 2,
        "warnings": 642,
        "elapsed_seconds": 2_138.84,
        "runtime_artifact": (
            "outputs/validation_runtime/full_20260717T075427Z/test_runtime_summary.json"
        ),
    }
    assert eb0_s2b["checkout_reproducibility"] == {
        "first_architecture_gate_status": "FAIL",
        "first_architecture_gate_passed": 302,
        "first_architecture_gate_failed": 2,
        "first_architecture_gate_elapsed_seconds": 55.60,
        "absolute_root_drift_node_count": 987,
        "project_relative_token": "<PROJECT_ROOT>",
        "runtime_defaults_changed": False,
        "cli_surface_changed": False,
        "deprecation_inventory_text_hash_policy": "universal_newline_lf",
        "deprecation_inventory_raw_byte_count_before": 528_634,
        "deprecation_inventory_canonical_byte_count": 515_757,
        "deprecation_inventory_source_content_changed": False,
        "production_effect": "none",
    }
    for source in eb0_s2b["sources"]:
        if source["path"] in set(eb0_s2b.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]

    eb0_s3a = baseline["integrated_change_arch_004g2_eb0_s3a_weight_search_tail"]
    assert eb0_s3a["status"] == "COMPLETE_RUNTIME_TASK_CONTINUES"
    assert eb0_s3a["task_id"] == "ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE"
    assert eb0_s3a["behavior"] == "weight_search_tail_pass_only_validation_reuse"
    assert eb0_s3a["pre_change_full"] == {
        "passed": 6_195,
        "skipped": 2,
        "warnings": 642,
        "elapsed_seconds": 2_138.84,
        "runtime_artifact": (
            "outputs/validation_runtime/full_20260717T075427Z/test_runtime_summary.json"
        ),
    }
    assert eb0_s3a["same_command"] == {
        "node_count": 5,
        "before_elapsed_seconds": 889.01,
        "after_elapsed_seconds": 254.19,
        "reduction_percent": 71.4,
    }
    assert eb0_s3a["isolated_nodes"] == {
        "followup": {
            "before_seconds": 884.24,
            "after_seconds": 352.36,
            "reduction_percent": 60.1,
        },
        "decision": {
            "before_seconds": 500.55,
            "after_seconds": 102.93,
            "reduction_percent": 79.4,
        },
    }
    assert eb0_s3a["post_change_full"]["passed"] == 6_195
    assert eb0_s3a["post_change_full"]["elapsed_seconds"] == 1_789.86
    assert eb0_s3a["post_change_full"]["reduction_percent"] == 16.3
    assert eb0_s3a["post_change_full"]["followup_seconds"] == 408.82
    assert eb0_s3a["post_change_full"]["decision_hardening_seconds"] == 157.40
    assert eb0_s3a["safety"] == {
        "first_full_builder_preserved": True,
        "all_view_rebuild_preserved": True,
        "tamper_matrix_preserved": True,
        "cross_lineage_fail_closed_preserved": True,
        "pass_only_cache": True,
        "fail_not_cached": True,
        "strategy_logic_changed": False,
        "cached_data_mutated": False,
        "production_effect": "none",
    }
    assert eb0_s3a["scope"]["research_foundation_started"] is False
    assert eb0_s3a["scope"]["eb1_started"] is False
    assert eb0_s3a["validation"]["same_command"]["status"] == "PASS"
    assert eb0_s3a["validation"]["decision_isolated"]["status"] == "PASS"
    assert eb0_s3a["validation"]["scoped_ruff"]["status"] == "PASS"
    assert eb0_s3a["validation"]["formal_focused"] == {
        "status": "PASS",
        "passed": 291,
        "skipped": 1,
        "elapsed_seconds": 269.09,
        "junit_artifact": (
            "outputs/validation_runtime/arch004g2-eb0-s3a-focused_20260717T221426Z/junit.xml"
        ),
    }
    assert eb0_s3a["validation"]["architecture_fitness"]["status"] == "PASS"
    assert eb0_s3a["validation"]["architecture_fitness"]["passed"] == 312
    assert eb0_s3a["validation"]["contract_validation"]["status"] == "PASS"
    assert eb0_s3a["validation"]["contract_validation"]["passed"] == 204
    assert eb0_s3a["validation"]["full_validation"]["status"] == "PASS"
    assert eb0_s3a["validation"]["full_validation"]["passed"] == 6_195
    assert "s3n_adaptive_equal_risk_tail_closeout" not in phase_b
    s3n = eb0_s3a["s3n_adaptive_equal_risk_tail_closeout"]
    assert s3n["status"] == "COMPLETE_RUNTIME_TASK_CONTINUES"
    assert s3n["base_commit"] == "13d85f1e"
    assert set(s3n["lanes"]) == {
        "weight_adaptive_outer_session",
        "equal_risk_restart_cli_canonical_json_reuse",
        "equal_risk_tilt_cli_dag_canonical_json_reuse",
    }
    restart_lane = s3n["lanes"]["equal_risk_restart_cli_canonical_json_reuse"]
    assert restart_lane["real_cli_count_before"] == 1
    assert restart_lane["real_cli_count_after"] == 1
    assert restart_lane["real_cli_count_preserved"] is True
    tilt_lane = s3n["lanes"]["equal_risk_tilt_cli_dag_canonical_json_reuse"]
    assert tilt_lane["real_cli_count_before"] == 4
    assert tilt_lane["real_cli_count_after"] == 4
    assert tilt_lane["real_cli_count_preserved"] is True
    assert s3n["validation"]["full_validation"]["run_count"] == 1
    assert s3n["validation"]["full_validation"]["node_count"] == 6_248
    assert s3n["validation"]["full_validation"]["file_count"] == 1_068
    assert s3n["validation"]["active_source_count"] == 77
    assert len(eb0_s3a["sources"]) >= s3n["validation"]["active_source_count"]
    assert s3n["validation"]["worktree_attribution"] == {
        "status": "PASS",
        "changed_tracked_path_count": 12,
        "declared_changed_tracked_path_count": 12,
        "excluded_user_path_count": 3,
    }
    assert s3n["validation"]["post_full_tracked_state_pending"] is False
    assert s3n["next_work"]["post_full_pass_satisfied"] is True
    assert s3n["next_work"]["second_full_allowed"] is False
    assert s3n["next_phase_or_slice_unblocked"] is False
    s4 = eb0_s3a["s4_full_trigger_provenance"]
    assert s4["status"] == "COMPLETE_RUNTIME_TASK_CONTINUES"
    assert s4["base_commit"] == "2962e02f"
    assert s4["owner_authorization"] == {
        "selected_option": "A",
        "authorized_increment": "S4_FULL_TRIGGER_PROVENANCE",
        "return_to_g2_4_coordination_point_after_closeout": True,
        "eb1_requires_new_explicit_owner_instruction": True,
    }
    assert s4["contract"]["cli_over_environment_precedence"] == "whole_envelope"
    assert s4["contract"]["profile_binding_status_required_for_performance_pass"] is True
    assert s4["contract"]["full_benchmark_runtime_profile_status"] == "NOT_APPLICABLE"
    assert "runtime_profile_sha256" in s4["contract"]["failure_fix_parent_binding"]["binds"]
    assert s4["contract"]["failure_fix_parent_binding"]["formal_parent_proof"][-1] == (
        "inventory_sha_size_fresh"
    )
    assert s4["contract"]["benchmark_inherited_formal_profile_provenance_env_removed"] is True
    assert s4["contract"]["malformed_json_types_fail_closed_without_validator_exception"] is True
    assert s4["validation"]["full_run_count"] == 0
    assert s4["validation"]["full_validation_required_for_s4"] is False
    assert s4["validation"]["architecture_fitness"]["status"] == ("PASS_AFTER_FRESHNESS_CORRECTION")
    assert s4["validation"]["contract_validation"]["status"] == "PASS"
    assert s4["validation"]["active_source_count"] == len(eb0_s3a["sources"])
    assert s4["next_phase_or_slice_unblocked"] is False
    for source in eb0_s3a["sources"]:
        if source["path"] in set(eb0_s3a.get("superseded_source_paths", [])):
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]

    eb1 = baseline["phase_g2_4eb1_etf_cli_dynamic_v3_signal_filter_foundation"]
    assert eb1["status"] in {
        "VALIDATING_G2_4_CONTINUES",
        "COMPLETE_G2_4_CONTINUES",
    }
    assert eb1["base_commit"] == "ee604385"
    assert eb1["boundary_id"] == "ARCH-004G2.4-EB1"
    assert eb1["migration"]["callback_count"] == 15
    assert eb1["migration"]["domain_public_entrypoint_count"] == 15
    assert len(eb1["migration"]["callback_ids"]) == 15
    assert len(set(eb1["migration"]["callback_ids"])) == 15
    assert eb1["contract"]["input_snapshot_schemas"] == [
        "signal_failure_taxonomy_input_snapshot.v2",
        "candidate_signal_ledger_input_snapshot.v2",
        "signal_churn_root_cause_input_snapshot.v2",
        "regime_mismatch_attribution_input_snapshot.v2",
        "candidate_quality_filter_design_input_snapshot.v2",
    ]
    assert eb1["contract"]["materialized_view_count"] == 23
    assert eb1["contract"]["missing_dated_rows"] == {
        "events": "empty",
        "method_count": None,
        "method_return": None,
        "downstream_status": "INSUFFICIENT_DATA",
        "mitigations_or_filters_allowed": False,
    }
    assert eb1["contract"]["aggregate_proxy_may_create_dated_evidence"] is False
    assert eb1["subtraction"] == {
        "legacy_cli_lines_before": 12_196,
        "legacy_cli_lines_after": 11_837,
        "legacy_cli_top_level_functions_before": 291,
        "legacy_cli_top_level_functions_after": 276,
        "legacy_cli_decorators_before": 252,
        "legacy_cli_decorators_after": 237,
        "legacy_domain_lines_before": 7_010,
        "legacy_domain_lines_after": 5_668,
        "compatibility_wrapper_count": 15,
        "duplicate_implementation_retained": False,
    }
    assert eb1["callback_matrix"] == {
        "baseline_callback_count": 967,
        "migrated_callback_count": 730,
        "pending_callback_count": 237,
        "unresolved_callback_count": 0,
        "duplicate_registration_count": 0,
        "phase_exit_criteria_passed": False,
    }
    assert eb1["cli_contract"]["leaf_command_count"] == 993
    assert eb1["cli_contract"]["duplicate_path_count"] == 0
    assert eb1["hardening"]["no_fabricated_events_or_forward_returns"] is True
    assert eb1["safety"]["production_effect"] == "none"
    assert eb1["next_work"]["eb2_requires_new_explicit_owner_instruction"] is True
    assert eb1["next_work"]["phase_exit_or_handoff_triggered"] is False
    assert eb1["next_phase_or_slice_unblocked"] is False
    assert eb1["sources"]
    eb1_superseded = set(eb1["superseded_source_paths"])
    for source in eb1["sources"]:
        if source["path"] in eb1_superseded:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]

    eb2 = baseline["phase_g2_4eb2_etf_cli_dynamic_v3_filtered_candidate_pipeline"]
    assert eb2["status"] in {
        "VALIDATING_G2_4_CONTINUES",
        "COMPLETE_G2_4_CONTINUES",
    }
    assert eb2["base_commit"] == "9c0025e4"
    assert eb2["boundary_id"] == "ARCH-004G2.4-EB2"
    assert eb2["migration"]["callback_count"] == 15
    assert eb2["migration"]["domain_public_entrypoint_count"] == 15
    assert len(eb2["migration"]["callback_ids"]) == 15
    assert len(set(eb2["migration"]["callback_ids"])) == 15
    assert eb2["contract"]["input_snapshot_schemas"] == [
        "filtered_candidate_backfill_input_snapshot.v2",
        "filtered_vs_original_comparison_input_snapshot.v2",
        "signal_gate_experiment_input_snapshot.v2",
        "filtered_candidate_promotion_review_input_snapshot.v2",
        "owner_signal_roadmap_input_snapshot.v2",
    ]
    assert eb2["contract"]["materialized_view_count"] == 24
    assert eb2["contract"]["empty_or_missing_evidence"] == {
        "variant_rows": "empty",
        "performance_rows": "empty",
        "comparison_rows": "empty",
        "gate_rows": "empty",
        "winner": None,
        "candidate": None,
        "confidence": None,
        "downstream_status": "INSUFFICIENT_DATA",
    }
    assert eb2["contract"]["synthesized_performance_or_denominator_allowed"] is False
    assert eb2["subtraction"] == {
        "legacy_cli_lines_before": 11_837,
        "legacy_cli_lines_after": 11_456,
        "legacy_cli_top_level_functions_before": 276,
        "legacy_cli_top_level_functions_after": 261,
        "legacy_cli_decorators_before": 237,
        "legacy_cli_decorators_after": 222,
        "legacy_domain_lines_before": 5_668,
        "legacy_domain_lines_after": 4_554,
        "compatibility_wrapper_count": 15,
        "duplicate_implementation_retained": False,
    }
    assert eb2["callback_matrix"] == {
        "baseline_callback_count": 967,
        "migrated_callback_count": 745,
        "pending_callback_count": 222,
        "unresolved_callback_count": 0,
        "duplicate_registration_count": 0,
        "phase_exit_criteria_passed": False,
    }
    assert eb2["cli_contract"]["leaf_command_count"] == 993
    assert eb2["cli_contract"]["duplicate_path_count"] == 0
    assert eb2["hardening"]["no_fabricated_filtered_outcomes_or_winner"] is True
    assert eb2["safety"]["production_effect"] == "none"
    assert eb2["next_work"] == {
        "pre_bootstrap_requires_eb2_integration_gate": False,
        "pre_bootstrap_unblocked": True,
        "eb3_or_formal_arch_005_s0_unblocked": False,
        "phase_exit_or_handoff_triggered": False,
    }
    assert eb2["next_work"]["phase_exit_or_handoff_triggered"] is False
    assert eb2["next_phase_or_slice_unblocked"] is False
    assert eb2["sources"]
    eb2_superseded = set(eb2["superseded_source_paths"])
    for source in eb2["sources"]:
        if source["path"] in eb2_superseded:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]

    eb3 = baseline["phase_g2_4eb3_etf_cli_dynamic_v3_filtered_candidate_readiness"]
    assert eb3["status"] in {
        "VALIDATING_G2_4_CONTINUES",
        "COMPLETE_G2_4_CONTINUES",
    }
    assert eb3["base_commit"] == "073a0c57"
    assert eb3["boundary_id"] == "ARCH-004G2.4-EB3"
    assert eb3["migration"]["callback_count"] == 30
    assert eb3["migration"]["domain_public_entrypoint_count"] == 30
    assert len(eb3["migration"]["callback_ids"]) == 30
    assert len(set(eb3["migration"]["callback_ids"])) == 30
    assert eb3["contract"]["input_snapshot_schemas"] == [
        "filtered_candidate_evidence_input_snapshot.v2",
        "median_regime_filter_spec_input_snapshot.v2",
        "filtered_candidate_stress_backfill_input_snapshot.v2",
        "drawdown_mismatch_reduction_input_snapshot.v2",
        "flip_rotation_reduction_input_snapshot.v2",
        "filtered_candidate_ab_review_input_snapshot.v2",
        "signal_gate_confirmation_input_snapshot.v2",
        "filtered_formalization_readiness_input_snapshot.v2",
        "owner_filtered_candidate_review_input_snapshot.v2",
        "filtered_next_decision_input_snapshot.v2",
    ]
    assert eb3["contract"]["policy_schema"] == "filtered_formalization_policy.v1"
    assert eb3["contract"]["materialized_view_count"] == 47
    assert eb3["contract"]["empty_or_missing_dated_evidence"] == {
        "observed_rows": "empty",
        "confirmation_targets": "empty",
        "rates_and_metrics": None,
        "winner_and_confidence": None,
        "specification_status": "RESEARCH_SPEC_ONLY",
        "downstream_status": "INSUFFICIENT_DATA",
        "formal_research_method_status": "NOT_READY",
        "promotion_state": "NEEDS_MORE_EVIDENCE",
        "next_decision": "COLLECT_DATED_EVIDENCE",
    }
    assert eb3["subtraction"] == {
        "legacy_cli_lines_before": 11_456,
        "legacy_cli_lines_after": 10_725,
        "legacy_cli_top_level_functions_before": 261,
        "legacy_cli_top_level_functions_after": 231,
        "legacy_cli_decorators_before": 222,
        "legacy_cli_decorators_after": 192,
        "legacy_domain_lines_before": 6_155,
        "legacy_domain_lines_after": 4_114,
        "compatibility_wrapper_count": 30,
        "duplicate_implementation_retained": False,
    }
    assert eb3["callback_matrix"] == {
        "baseline_callback_count": 967,
        "migrated_callback_count": 775,
        "pending_callback_count": 192,
        "unresolved_callback_count": 0,
        "duplicate_registration_count": 0,
        "phase_exit_criteria_passed": False,
    }
    assert eb3["cli_contract"]["leaf_command_count"] == 993
    assert eb3["cli_contract"]["duplicate_path_count"] == 0
    assert eb3["hardening"]["no_synthetic_stress_performance_or_confirmation_targets"] is True
    assert eb3["safety"]["production_effect"] == "none"
    assert eb3["next_work"] == {
        "eb4_requires_new_explicit_owner_instruction": True,
        "formal_arch_005_s0_unblocked": False,
        "g2_5_unblocked": False,
        "phase_exit_or_handoff_triggered": False,
    }
    assert eb3["next_phase_or_slice_unblocked"] is False
    assert eb3["sources"]
    eb3_superseded = set(eb3["superseded_source_paths"])
    for source in eb3["sources"]:
        if source["path"] in eb3_superseded:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]

    eb4 = baseline["phase_g2_4eb4_etf_cli_evidence_materialization_and_input_readiness"]
    assert eb4["status"] in {
        "VALIDATING_G2_4_CONTINUES",
        "COMPLETE_G2_4_CONTINUES",
    }
    assert eb4["base_commit"] == "26a45e0d"
    assert eb4["boundary_id"] == "ARCH-004G2.4-EB4"
    assert eb4["owner_authorization"] == {
        "instruction": "先按照这个顺序推进到可以考虑开始推进研究策略前把",
        "eb4_through_handoff_authorized": True,
        "arch_005_s0_s1_after_handoff_authorized": True,
        "g2_5_requires_later_explicit_instruction": True,
    }
    assert eb4["migration"]["callback_count"] == 39
    assert eb4["migration"]["app_callback_count"] == 26
    assert eb4["migration"]["matching_validator_callback_count"] == 13
    assert eb4["migration"]["domain_public_entrypoint_count"] == 39
    assert len(eb4["migration"]["canonical_interfaces"]) == 3
    assert len(eb4["migration"]["callback_ids"]) == 39
    assert len(set(eb4["migration"]["callback_ids"])) == 39
    assert len(eb4["contract"]["input_snapshot_schemas"]) == 14
    assert eb4["contract"]["materialized_view_count"] == 63
    assert eb4["contract"]["missing_or_unqualified_evidence"] == {
        "observed_metrics": None,
        "observed_rows": "empty",
        "status": "INSUFFICIENT_DATA",
        "promotion_ready": False,
        "automatic_promotion": False,
    }
    assert eb4["subtraction"] == {
        "legacy_cli_lines_before": 10_725,
        "legacy_cli_lines_after": 9_065,
        "legacy_cli_top_level_functions_before": 231,
        "legacy_cli_top_level_functions_after": 192,
        "legacy_cli_decorators_before": 192,
        "legacy_cli_decorators_after": 153,
        "legacy_readiness_domain_lines_before": 4_114,
        "legacy_readiness_domain_lines_after": 2_978,
        "compatibility_wrapper_count": 6,
        "duplicate_implementation_retained": False,
    }
    assert eb4["callback_matrix"] == {
        "baseline_callback_count": 967,
        "migrated_callback_count": 814,
        "pending_callback_count": 153,
        "unresolved_callback_count": 0,
        "duplicate_registration_count": 0,
        "phase_exit_criteria_passed": False,
    }
    assert eb4["cli_contract"] == {
        "root_command_count": 41,
        "group_count": 291,
        "leaf_command_count": 993,
        "duplicate_path_count": 0,
        "command_tree_sha256": ("01c78550ae58b38c2d8cca0683376643e2934f93e324710612c87d39eea7302d"),
    }
    assert eb4["focused_validation"]["performance_triggered"] is False
    assert eb4["formal_validation"]["architecture_fitness"]["status"] == "PASS"
    assert eb4["formal_validation"]["contract_validation"]["status"] == "PASS"
    full_validation = eb4["formal_validation"]["full_validation"]
    assert full_validation["status"] == "PASS_AFTER_AUDITED_FAILURE_FIX_RERUN"
    assert full_validation["actual_natural_run_count"] == 1
    assert full_validation["natural_run"]["status"] == "FAIL"
    assert full_validation["natural_run"]["failed"] == 7
    assert full_validation["failure_fix_rerun"]["status"] == "PASS"
    assert full_validation["failure_fix_rerun"]["passed"] == 6_373
    assert full_validation["failure_fix_rerun"]["failed"] == 0
    assert full_validation["failure_fix_rerun"]["scheduler_fallback"] is False
    assert full_validation["failure_fix_rerun"]["production_effect"] == "none"
    assert full_validation["duration_profile_refresh"]["version"] == 11
    assert eb4["formal_validation"]["architecture_devex"]["violation_count"] == 0
    assert eb4["formal_validation"]["deprecation_inventory"]["test_file_count"] == 1_130
    assert eb4["safety"]["production_effect"] == "none"
    assert eb4["next_work"] == {
        "eb5_after_eb4_integration_gate": True,
        "eb5_unblocked": True,
        "whole_g2_4_phase_exit_passed": False,
        "formal_arch_005_s0_unblocked": False,
        "g2_5_unblocked": False,
        "phase_exit_or_handoff_triggered": False,
    }
    assert eb4["next_phase_or_slice_unblocked"] is True
    assert len(eb4["sources"]) == 57
    eb4_superseded = set(eb4["superseded_source_paths"])
    for source in eb4["sources"]:
        if source["path"] in eb4_superseded:
            continue
        actual = _source_sha256(source)
        assert actual == source["sha256"], source["path"]

    eb5 = baseline["phase_g2_4eb5_paper_shadow_health_recovery_and_decision_support"]
    assert eb5["status"] in {
        "VALIDATING_G2_4_CONTINUES",
        "COMPLETE_G2_4_CONTINUES",
    }
    assert eb5["base_commit"] == ("8332e9613e3fabd542f61689c899bb90dc1bd995")
    assert eb5["boundary_id"] == "ARCH-004G2.4-EB5"
    assert eb5["owner_authorization"] == {
        "instruction": "先按照这个顺序推进到可以考虑开始推进研究策略前把",
        "eb5_through_handoff_authorized": True,
        "arch_005_s0_s1_after_handoff_authorized": True,
        "g2_5_requires_later_explicit_instruction": True,
    }
    assert eb5["migration"]["callback_count"] == 37
    assert eb5["migration"]["app_callback_count"] == 24
    assert eb5["migration"]["matching_validator_callback_count"] == 13
    assert eb5["migration"]["domain_public_entrypoint_count"] == 37
    assert len(eb5["migration"]["canonical_interfaces"]) == 3
    assert len(eb5["contract"]["input_snapshot_schemas"]) == 13
    assert eb5["contract"]["input_snapshot_sha256_sealed"] is True
    assert eb5["contract"]["validation_cache_pass_only"] is True
    assert eb5["contract"]["validation_cache_content_fingerprint_bound"] is True
    assert eb5["subtraction"] == {
        "legacy_cli_lines_before": 9_065,
        "legacy_cli_lines_after": 6_572,
        "legacy_cli_top_level_functions_before": 192,
        "legacy_cli_top_level_functions_after": 142,
        "legacy_cli_decorators_before": 153,
        "legacy_cli_decorators_after": 116,
        "duplicate_implementation_retained": False,
    }
    assert eb5["callback_matrix"] == {
        "baseline_callback_count": 967,
        "migrated_callback_count": 851,
        "pending_callback_count": 116,
        "unresolved_callback_count": 0,
        "duplicate_registration_count": 0,
        "phase_exit_criteria_passed": False,
    }
    assert eb5["cli_contract"] == {
        "root_command_count": 41,
        "group_count": 291,
        "leaf_command_count": 993,
        "duplicate_path_count": 0,
        "command_tree_sha256": ("01c78550ae58b38c2d8cca0683376643e2934f93e324710612c87d39eea7302d"),
    }
    assert eb5["focused_validation"]["corrected_family_run"]["passed"] == 50
    assert eb5["focused_validation"]["elapsed_reduction_percent"] == 45.3
    assert eb5["focused_validation"]["performance_triggered"] is False
    assert eb5["hardening"]["source_tamper_fail_closed"] is True
    assert eb5["hardening"]["snapshot_tamper_fail_closed"] is True
    assert eb5["hardening"]["output_tamper_fail_closed"] is True
    assert eb5["hardening"]["cross_lineage_tamper_fail_closed"] is True
    assert eb5["safety"]["production_effect"] == "none"
    assert len(eb5["sources"]) == 38
    if eb5["status"] == "VALIDATING_G2_4_CONTINUES":
        assert eb5["formal_validation"]["architecture_fitness"]["status"] == "PENDING"
        assert eb5["formal_validation"]["contract_validation"]["status"] == "PENDING"
        assert eb5["formal_validation"]["full_validation"]["status"] == "PENDING"
        assert eb5["next_phase_or_slice_unblocked"] is False
        assert all(source["sha256"] == 0 for source in eb5["sources"])
    else:
        assert eb5["formal_validation"]["architecture_fitness"]["status"] == "PASS"
        assert eb5["formal_validation"]["contract_validation"]["status"] == "PASS"
        assert eb5["formal_validation"]["full_validation"]["status"] in {
            "PASS",
            "PASS_AFTER_AUDITED_FAILURE_FIX_RERUN",
        }
        assert eb5["next_phase_or_slice_unblocked"] is True
        eb5_superseded = set(eb5["superseded_source_paths"])
        assert len(eb5_superseded) == 18
        for source in eb5["sources"]:
            if source["path"] in eb5_superseded:
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    eb6 = baseline["phase_g2_4eb6_weight_calibration_and_research_interfaces"]
    assert eb6["status"] in {
        "VALIDATING_G2_4_CONTINUES",
        "COMPLETE_G2_4_CONTINUES",
    }
    assert eb6["base_commit"] == ("e5fc456c4f2c67466f6f8f78f551e7dc69801644")
    assert eb6["boundary_id"] == "ARCH-004G2.4-EB6"
    assert eb6["migration"] == {
        "callback_count": 40,
        "weight_calibration_callback_count": 20,
        "weight_research_callback_count": 20,
        "canonical_interfaces": [
            "src/ai_trading_system/interfaces/cli/etf_portfolio/weight_calibration.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/weight_research.py",
        ],
        "compatibility_app_reexports": True,
        "compatibility_callback_wrappers": False,
    }
    assert eb6["contract"] == {
        "interface_ownership_only": True,
        "weight_or_threshold_semantics_changed": False,
        "sample_holdout_regime_or_date_semantics_changed": False,
        "ranking_recommendation_or_promotion_semantics_changed": False,
        "data_quality_and_policy_provenance_preserved": True,
        "cli_path_help_default_exit_and_output_preserved": True,
        "latest_json_helper_has_no_investment_semantics": True,
    }
    assert eb6["subtraction"] == {
        "legacy_cli_lines_before": 6572,
        "legacy_cli_lines_after": 4038,
        "legacy_cli_top_level_functions_before": 142,
        "legacy_cli_top_level_functions_after": 100,
        "legacy_cli_decorators_before": 116,
        "legacy_cli_decorators_after": 76,
        "duplicate_implementation_retained": False,
    }
    assert eb6["callback_matrix"] == {
        "baseline_callback_count": 967,
        "migrated_callback_count": 891,
        "pending_callback_count": 76,
        "unresolved_callback_count": 0,
        "duplicate_registration_count": 0,
        "phase_exit_criteria_passed": False,
    }
    assert eb6["cli_contract"] == {
        "root_command_count": 41,
        "group_count": 291,
        "leaf_command_count": 993,
        "duplicate_path_count": 0,
        "command_tree_sha256": ("01c78550ae58b38c2d8cca0683376643e2934f93e324710612c87d39eea7302d"),
    }
    assert eb6["focused_validation"]["pre_migration_baseline"]["passed"] == 231
    assert eb6["focused_validation"]["corrected_post_migration_run"]["passed"] == 231
    assert eb6["focused_validation"]["performance_triggered"] is False
    assert eb6["safety"]["strategy_logic_changed"] is False
    assert eb6["safety"]["production_effect"] == "none"
    assert len(eb6["sources"]) == 42
    if eb6["status"] == "VALIDATING_G2_4_CONTINUES":
        assert eb6["formal_validation"]["architecture_fitness"]["status"] == "PENDING"
        assert eb6["formal_validation"]["contract_validation"]["status"] == "PENDING"
        assert eb6["formal_validation"]["full_validation"]["status"] == "PENDING"
        assert eb6["next_phase_or_slice_unblocked"] is False
        assert all(source["sha256"] == 0 for source in eb6["sources"])
    else:
        assert eb6["formal_validation"]["architecture_fitness"]["status"] == "PASS"
        assert eb6["formal_validation"]["contract_validation"]["status"] == "PASS"
        assert eb6["formal_validation"]["full_validation"]["status"] in {
            "PASS",
            "PASS_AFTER_AUDITED_FAILURE_FIX_RERUN",
        }
        assert eb6["next_phase_or_slice_unblocked"] is True
        eb6_superseded = set(eb6.get("superseded_source_paths", []))
        for source in eb6["sources"]:
            if source["path"] in eb6_superseded:
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    eb7 = baseline["phase_g2_4eb7_residual_cli_interfaces"]
    assert eb7["status"] in {
        "VALIDATING_G2_4_CONTINUES",
        "COMPLETE_G2_4_CONTINUES",
    }
    assert eb7["base_commit"] == "4371b419cf9b18c7a8445658c06bddb073eca004"
    assert eb7["boundary_id"] == "ARCH-004G2.4-EB7"
    assert eb7["migration"] == {
        "callback_count": 40,
        "dynamic_shadow_callback_count": 6,
        "satellite_callback_count": 6,
        "experiments_callback_count": 7,
        "p2_callback_count": 18,
        "simulation_callback_count": 3,
        "canonical_interfaces": [
            "src/ai_trading_system/interfaces/cli/etf_portfolio/dynamic_shadow.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/satellite.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/experiments.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/p2.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/simulation.py",
        ],
        "compatibility_app_reexports": True,
        "compatibility_callback_reexports": 6,
        "compatibility_callback_wrappers": False,
    }
    assert eb7["subtraction"] == {
        "legacy_cli_lines_before": 4038,
        "legacy_cli_lines_after": 1781,
        "legacy_cli_top_level_functions_before": 100,
        "legacy_cli_top_level_functions_after": 41,
        "legacy_cli_decorators_before": 76,
        "legacy_cli_decorators_after": 36,
        "duplicate_implementation_retained": False,
    }
    assert eb7["callback_matrix"] == {
        "baseline_callback_count": 967,
        "migrated_callback_count": 931,
        "pending_callback_count": 36,
        "unresolved_callback_count": 0,
        "duplicate_registration_count": 0,
        "phase_exit_criteria_passed": False,
    }
    assert eb7["cli_contract"] == {
        "root_command_count": 41,
        "group_count": 291,
        "leaf_command_count": 993,
        "duplicate_path_count": 0,
        "command_tree_sha256": ("01c78550ae58b38c2d8cca0683376643e2934f93e324710612c87d39eea7302d"),
    }
    assert eb7["focused_validation"]["post_migration_run"]["passed"] == 257
    assert eb7["focused_validation"]["performance_triggered"] is False
    assert eb7["architecture_devex"]["module_count"] == 970
    assert eb7["deprecation_inventory"]["module_count"] == 970
    assert eb7["safety"]["strategy_logic_changed"] is False
    assert eb7["safety"]["production_effect"] == "none"
    assert len(eb7["sources"]) == 29
    if eb7["status"] == "VALIDATING_G2_4_CONTINUES":
        assert eb7["formal_validation"]["architecture_fitness"]["status"] == "PENDING"
        assert eb7["formal_validation"]["contract_validation"]["status"] == "PENDING"
        assert eb7["formal_validation"]["full_validation"]["status"] == "PENDING"
        assert eb7["next_phase_or_slice_unblocked"] is False
        assert all(source["sha256"] == 0 for source in eb7["sources"])
    else:
        assert eb7["formal_validation"]["architecture_fitness"]["status"] == "PASS"
        assert eb7["formal_validation"]["contract_validation"]["status"] == "PASS"
        assert eb7["formal_validation"]["full_validation"]["status"] in {
            "PASS",
            "PASS_AFTER_AUDITED_FAILURE_FIX_RERUN",
        }
        assert eb7["next_phase_or_slice_unblocked"] is True
        eb7_superseded = set(eb7.get("superseded_source_paths", []))
        for source in eb7["sources"]:
            if source["path"] in eb7_superseded:
                continue
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    eb8 = baseline["phase_g2_4eb8_final_cli_compatibility_facade"]
    assert eb8["status"] in {
        "VALIDATING_G2_4_CONTINUES",
        "COMPLETE_G2_4_CONTINUES",
    }
    assert eb8["base_commit"] == "bfbb38cf1ae0f9fbdd6fcefb10749bc5e59f03dc"
    assert eb8["boundary_id"] == "ARCH-004G2.4-EB8"
    assert eb8["migration"] == {
        "callback_count": 36,
        "ai_attribution_callback_count": 3,
        "ai_confirmation_callback_count": 4,
        "backtest_callback_count": 3,
        "decision_journal_callback_count": 8,
        "forward_callback_count": 5,
        "p1_callback_count": 6,
        "workflow_callback_count": 7,
        "canonical_interfaces": [
            "src/ai_trading_system/interfaces/cli/etf_portfolio/ai_attribution.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/ai_confirmation.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/backtest.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/decision_journal.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/forward.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/p1.py",
            "src/ai_trading_system/interfaces/cli/etf_portfolio/workflow.py",
        ],
        "compatibility_app_reexports": True,
        "compatibility_callback_reexports": 8,
        "compatibility_callback_wrappers": False,
    }
    assert eb8["subtraction"] == {
        "legacy_cli_lines_before": 1781,
        "legacy_cli_lines_after": 146,
        "legacy_cli_top_level_functions_before": 41,
        "legacy_cli_top_level_functions_after": 0,
        "legacy_cli_decorators_before": 36,
        "legacy_cli_decorators_after": 0,
        "duplicate_implementation_retained": False,
    }
    assert eb8["callback_matrix"] == {
        "baseline_callback_count": 967,
        "migrated_callback_count": 967,
        "pending_callback_count": 0,
        "unresolved_callback_count": 0,
        "duplicate_registration_count": 0,
        "phase_exit_criteria_passed": True,
    }
    assert eb8["cli_contract"] == {
        "root_command_count": 41,
        "group_count": 291,
        "leaf_command_count": 993,
        "duplicate_path_count": 0,
        "command_tree_sha256": ("01c78550ae58b38c2d8cca0683376643e2934f93e324710612c87d39eea7302d"),
    }
    assert eb8["focused_validation"]["post_migration_run"]["passed"] == 269
    assert eb8["focused_validation"]["performance_triggered"] is False
    assert eb8["safety"]["strategy_logic_changed"] is False
    assert eb8["safety"]["production_effect"] == "none"
    assert len(eb8["sources"]) == 26
    if eb8["status"] == "VALIDATING_G2_4_CONTINUES":
        assert eb8["formal_validation"]["architecture_fitness"]["status"] == "PENDING"
        assert eb8["formal_validation"]["contract_validation"]["status"] == "PENDING"
        assert eb8["formal_validation"]["full_validation"]["status"] == "PENDING"
        assert eb8["next_phase_or_slice_unblocked"] is False
        assert all(source["sha256"] == 0 for source in eb8["sources"])
    else:
        assert eb8["formal_validation"]["architecture_fitness"]["status"] == "PASS"
        assert eb8["formal_validation"]["contract_validation"]["status"] == "PASS"
        assert eb8["formal_validation"]["full_validation"]["status"] in {
            "PASS",
            "PASS_AFTER_AUDITED_FAILURE_FIX_RERUN",
        }
        assert eb8["next_phase_or_slice_unblocked"] is True
        for source in eb8["sources"]:
            actual = _source_sha256(source)
            assert actual == source["sha256"], source["path"]

    prebootstrap = baseline["arch_005_prebootstrap_primitives"]
    assert prebootstrap["status"] in {
        "IN_PROGRESS_NON_CUTOVER",
        "COMPLETE_NON_CUTOVER_G2_4_CONTINUES",
    }
    assert prebootstrap["task_id"] == ("ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE")
    assert prebootstrap["slice_id"] == "ARCH-005-PB1"
    assert prebootstrap["base_commit"] == ("fe0e19b943e7ca2f49c091a50536a6b022657566")
    assert prebootstrap["owner_authorization"] == {
        "instruction": "继续按照这个思路实现",
        "boundary_id": "ARCH-005-PB1",
        "formal_s0_authorized": False,
    }
    assert prebootstrap["contracts"] == {
        "change_manifest_schema": "change_manifest.v1",
        "validation_evidence_schema": "validation_evidence.v1",
        "lane_plan_schema": "lane_plan.v1",
        "canonical_serialization_and_hash": True,
        "base_drift_fail_closed": True,
        "owned_shared_path_conflict": True,
        "module_conflict": True,
        "contract_access_and_version_conflict": True,
        "coordinator_only_guard": True,
        "explicit_lane_capacity_required": True,
        "deterministic_domain_waves": True,
        "coordinator_final_integration_wave": True,
        "evidence_manifest_base_tier_status_artifact_binding": True,
    }
    assert prebootstrap["non_cutover_boundary"] == {
        "dispatch_allowed": False,
        "lease_acquisition_allowed": False,
        "task_registry_mutated": False,
        "generated_task_views_written": False,
        "markdown_source_of_truth_changed": False,
        "production_effect": "none",
    }
    assert prebootstrap["phase_lock"]["eb2_integration_gate_passed"] is True
    assert prebootstrap["phase_lock"]["pre_bootstrap_unblocked"] is True
    assert prebootstrap["phase_lock"]["pre_bootstrap_complete"] is True
    assert prebootstrap["phase_lock"]["eb3_unblocked"] is False
    assert prebootstrap["phase_lock"]["formal_arch_005_s0_unblocked"] is False
    assert prebootstrap["phase_lock"]["g2_5_unblocked"] is False
    assert prebootstrap["phase_lock"]["next_phase_or_slice_unblocked"] is False
    assert prebootstrap["validation"]["architecture_devex"] == {
        "status": "PASS",
        "module_count": 954,
        "test_file_count": 1129,
        "direct_writer_count": 858,
        "violation_count": 0,
    }
    assert prebootstrap["validation"]["deprecation_inventory"] == {
        "status": "FRESH_REFRESHED",
        "inventory_id": "arch_004g_deprecation_inventory_a2ab38dc563643dacc6e",
        "module_count": 954,
        "test_file_count": 1129,
    }
    assert len(prebootstrap["sources"]) == 20
    prebootstrap_superseded = set(prebootstrap["superseded_source_paths"])
    for source in prebootstrap["sources"]:
        if source["path"] in prebootstrap_superseded:
            continue
        if prebootstrap["status"] == "IN_PROGRESS_NON_CUTOVER":
            assert source["sha256"] == 0
            continue
        actual = _source_sha256(source)
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
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BC_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BC_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BD_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BD_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BE_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BE_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BF_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BF_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BG_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BG_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BG_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BH_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BH_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BH_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BI_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BI_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BI_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BJ_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BJ_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BJ_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BK_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BK_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BK_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BL_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BL_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BL_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BM_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BM_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BM_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BN_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BN_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BN_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BO_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BO_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BO_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BP_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BP_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BP_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BQ_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BQ_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BQ_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BR_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BR_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BR_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BS_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BS_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BS_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BT_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BT_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BT_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BU_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BU_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BU_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BV_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BV_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BV_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BW_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BW_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BW_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BX_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BX_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BX_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BY_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BY_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BY_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BZ_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BZ_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4BZ_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CA_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CA_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CA_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CB_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CB_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CB_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CC_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CC_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CC_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CD_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CD_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CD_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CE_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CE_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CE_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CF_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CF_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CF_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CG_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CG_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CG_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CH_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CH_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CH_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CI_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CJ_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CK_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CK_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CL_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CL_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CM_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CM_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CN_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CN_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CO_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CO_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CO_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CP_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CP_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CQ_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CQ_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CR_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CR_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CR_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CS_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CT_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CT_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CU_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CU_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CU_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CV1_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CV1_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CV1_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CV2_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CV2_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CV2_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CV3_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CV3_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CV3_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CW1_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CW1_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CW2_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CW2_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CW3_VALIDATING",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CW3_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CX2_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CX2_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CX3_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CX3_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4CX3_COMPLETE_G2_4_CONTINUES",
        "INTEGRATED_WORKTREE_ATTRIBUTION_PROVEN_PHASE_G2_4CX3_VALIDATING_G2_4_CONTINUES",
        "INTEGRATED_WORKTREE_ATTRIBUTION_PROVEN_PHASE_G2_4CX3_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB1_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB1_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB2_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB2_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB3_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB3_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB4_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB4_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB5_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB5_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB6_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB6_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB6_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB7_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB7_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB7_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB8_IN_PROGRESS_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB8_VALIDATING_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_G2_4_EB8_COMPLETE_G2_4_CONTINUES",
        "ATTRIBUTABLE_ISOLATION_PROVEN_ARCH_005_PREBOOTSTRAP_IN_PROGRESS",
        "ATTRIBUTABLE_ISOLATION_PROVEN_ARCH_005_PREBOOTSTRAP_COMPLETE_G2_4_CONTINUES",
    }
    current_authority = attribution["current_staging_authority"]
    assert current_authority["task_id"] == ("ARCH-004G2_REMAINING_PHASE_EFFICIENCY_EXECUTION")
    assert current_authority["increment"] == "ARCH-004G2.4-EB8"
    assert current_authority["status"] in {
        "IN_PROGRESS_G2_4_CONTINUES",
        "VALIDATING_G2_4_CONTINUES",
        "COMPLETE_G2_4_CONTINUES",
    }
    assert current_authority["base_commit"] == ("bfbb38cf1ae0f9fbdd6fcefb10749bc5e59f03dc")
    assert current_authority["declared_path_set_current"] is True
    owner_authorization = current_authority["owner_authorization"]
    assert owner_authorization["instruction"] == (
        "先按照这个顺序推进到可以考虑开始推进研究策略前把"
    )
    assert str(owner_authorization["authorized_at"]) == "2026-07-19"
    assert owner_authorization["boundary_id"] == "ARCH-004G2.4-EB8"
    expected_complete = current_authority["status"] == "COMPLETE_G2_4_CONTINUES"
    assert current_authority["phase_lock"] == {
        "next_phase_or_slice_unblocked": expected_complete,
        "eb2_authorized": True,
        "eb2_integration_gate_passed": True,
        "pre_bootstrap_after_integration_gate_authorized": True,
        "pre_bootstrap_unblocked": True,
        "pre_bootstrap_complete": True,
        "eb3_unblocked": True,
        "eb3_complete": True,
        "eb4_authorized": True,
        "eb4_unblocked": True,
        "eb5_unblocked": True,
        "eb5_authorized": True,
        "eb5_complete": True,
        "eb6_unblocked": True,
        "eb6_authorized": True,
        "eb6_complete": True,
        "eb7_unblocked": True,
        "eb7_authorized": True,
        "eb7_complete": True,
        "eb8_unblocked": True,
        "eb8_authorized": True,
        "eb8_complete": expected_complete,
        "formal_arch_005_s0_unblocked": False,
        "g2_5_unblocked": False,
    }
    assert len(current_authority["declared_changed_paths"]) == 27
    assert attribution["current_staging_authority"]["base_commit"] == attribution["base_commit"]
    assert attribution["current_staging_authority"]["task_id"] in attribution["integrated_task_ids"]
    assert {
        "tests/test_weight_adaptive_branch.py",
        "tests/test_equal_risk_growth_research_restart.py",
        "tests/test_equal_risk_growth_tilt.py",
    }.issubset(set(attribution["arch_004_owned_paths"]))
    excluded = set(attribution["excluded_user_or_other_task_paths"])
    assert excluded == {
        "docs/research/growth_tilt_owner_decision_resolution.md",
        "docs/research/indicator_family_only_model_review.md",
        "docs/research/layer1_selector_pause_or_continue_owner_pack.md",
    }
    assert attribution["staging_rule"]["exclude_user_or_other_task_paths"] is True
    assert attribution["safety_boundary"]["user_changes_preserved"] is True
