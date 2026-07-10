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


def test_arch_004_phase_f2_validating_policy_keeps_freeze_and_preserves_safety() -> None:
    policy = safe_load_yaml_path(POLICY_PATH)

    assert policy["schema_version"] == "arch_004_refactor_policy.v1"
    assert policy["status"] == "phase_f2_in_progress"
    assert policy["program"]["current_phase"] == "ARCH-004F2"
    assert policy["program"]["current_phase_status"] == "IN_PROGRESS"
    assert policy["program"]["next_phase"] == "ARCH-004F1_OR_F3"
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
    assert phase_f2["status"] == "BASELINE_DONE_RUNTIME_MIGRATION_PENDING"
    assert phase_f2["stages"] == {
        "F2_1_current_state_inventory_and_trace": "COMPLETE",
        "F2_2_authoritative_execution_chain_document": "COMPLETE",
        "F2_3_lifecycle_contract_and_review_boundary": "DOCUMENTED_BASELINE_COMPLETE",
        "F2_4_reference_integration_and_validation": "COMPLETE",
        "F2_5_generic_lifecycle_runtime_migration": "NOT_STARTED",
    }
    assert phase_f2["periodic_review_may_auto_tune"] is False
    assert phase_f2["result_visible_before_preregistration_freeze_allowed"] is False
    assert phase_f2["strategy_or_threshold_change_allowed_in_documentation_slice"] is False
    assert phase_f2["interim_evidence"]["market_regime_and_research_window_separated"] is True
    assert phase_f2["interim_evidence"]["production_effect"] == "none"
    assert phase_f2["documentation_validation"]["focused_docs_and_policy"]["passed"] == 23
    assert phase_f2["documentation_validation"]["architecture_fitness"]["passed"] == 80
    assert phase_f2["documentation_validation"]["contract_validation"]["passed"] == 197
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
            assert source["superseded_by_phase"] == "ARCH-004D"
            assert source["current_hash_tracked_in"] == ("phase_d_reference_vertical_slice.sources")
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
            assert source["superseded_by_phase"] == "ARCH-004E"
            assert source["current_hash_tracked_in"] == (
                "phase_e_devex_ownership_generated_indexes.sources"
            )
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
            assert source["superseded_by_phase"] == "ARCH-004F2"
            assert source["current_hash_tracked_in"] == (
                "phase_f2_research_lifecycle_and_execution_chain.sources"
            )
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
        "ATTRIBUTABLE_ISOLATION_PROVEN_PHASE_F2_DOCUMENTATION_BASELINE"
    )
    excluded = set(attribution["excluded_user_or_other_task_paths"])
    assert excluded == {
        "docs/research/growth_tilt_owner_decision_resolution.md",
        "docs/research/indicator_family_only_model_review.md",
        "docs/research/layer1_selector_pause_or_continue_owner_pack.md",
    }
    assert attribution["staging_rule"]["exclude_user_or_other_task_paths"] is True
    assert attribution["safety_boundary"]["user_changes_preserved"] is True
