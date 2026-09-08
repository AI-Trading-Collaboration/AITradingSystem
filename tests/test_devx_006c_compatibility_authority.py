from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
import yaml

from ai_trading_system.platform.architecture import compatibility_authority as authority
from ai_trading_system.platform.architecture.compatibility_authority import (
    CompatibilityAuthorityError,
    load_compatibility_authority,
    load_compatibility_policy,
    render_fragment,
    render_index,
    validate_repository_authority,
)
from ai_trading_system.platform.artifacts.writer import canonical_json_bytes
from ai_trading_system.yaml_loader import safe_load_yaml_text

REAL_LEGACY_PATH = Path("inputs/architecture/arch_004_compatibility_baseline.yaml")
DEVX_006C_SECTION = "phase_devx_006c_compatibility_authority_fragmentation"
DEVX_006D_SECTION = "phase_devx_006d_report_catalog_flow_lossless_fragmentation"
ARCH_005_S5_SECTION = "phase_arch_005_s5_canonical_task_source_cutover"
DEVX_007_V2_SECTION = "phase_devx_007_web_pro_git_review_skill_explicit_submission_v2"
TRADING_2542C_SECTION = (
    "phase_trading_2542c_growth_action_value_independent_review_remediation_and_freeze_readiness_v1"
)
DEVX_009_SECTION = (
    "phase_devx_009_parallel_integration_publication_fence_and_generated_state_rebuild_v1"
)
TRADING_2542D_SECTION = (
    "phase_trading_2542d_growth_action_value_dq_pit_and_sample_semantics_freeze_correction_v1"
)
PROD_004_SECTION = "phase_prod_004_pit_cumulative_archive_consumption_v1"
DEVX_011_SECTION = "phase_devx_011_governed_workflow_health_control_loop_v1"
DEVX_012_SECTION = "phase_devx_012_automatic_workflow_health_trigger_and_outcome_review_v1"
RISK_012_SECTION = "phase_risk_012_unknown_risk_event_id_fail_closed_v1"
OPS_077_SECTION = "phase_ops_077_atomic_release_scheduler_binding_and_canary_v1"
OPS_078_SECTION = "phase_ops_078_daily_automation_isolation_and_same_day_rescue_v1"
TRADING_2564_S2A_SECTION = "phase_trading_2564_s2a_named_immutable_snapshot_v1"
OPS_079_SECTION = "phase_ops_079_historical_daily_gap_recovery_executor_v1"
DEVX_014_SECTION = "phase_devx_014_dirty_source_preservation_and_os_lease_arbiter_v1"
TRADING_2564_S2B_SECTION = "phase_trading_2564_s2b_named_dq_execution_v1"
TRADING_2564_S2C_SECTION = "phase_trading_2564_s2c_equal_risk_price_consumer_scope_v1"
TRADING_2564_S2C2_SECTION = "phase_trading_2564_s2c2_five_candidate_preview_v1"
TRADING_2564_S3A_SECTION = "phase_trading_2564_s3a_prospective_event_time_v1"
TRADING_2564_S3B_SECTION = "phase_trading_2564_s3b_prospective_capture_execution_v1"
DEVX_014_SOURCE_PATHS = frozenset(
    {
        "config/architecture/arch_005_source_preservation.yaml",
        "config/architecture/devx_006d_report_catalog_flow_authority.yaml",
        "docs/requirements/DEVX-014_Dirty_Source_Preservation_Recovery_V1.md",
        "docs/system_flow.md",
        "docs/task_register.md",
        "docs/task_register_completed.md",
        "inputs/architecture/arch_004e_aggregate_shadow_index.yaml",
        "inputs/architecture/arch_004e_architecture_fitness.yaml",
        "inputs/architecture/arch_004e_module_manifest.yaml",
        "inputs/architecture/arch_004e_test_manifest.yaml",
        "inputs/architecture/arch_004g_deprecation_inventory.yaml",
        "inputs/architecture/arch_005_s5_consumer_inventory.yaml",
        "inputs/architecture/arch_005_task_registry_index.yaml",
        "inputs/architecture/devx_006d_report_catalog_flow_authority_index.json",
        "inputs/architecture/devx_006d_report_catalog_flow_consumer_inventory.json",
        (
            "registry/development_tasks/7a/"
            "7aa82ac8ab6fa54a137b6972521b5ba8c7f1d0c4033f6fe84dac77fa2b5267e1.yaml"
        ),
        "scripts/architecture_arch005_lease_arbiter.py",
        "scripts/architecture_arch005_source_preservation.py",
        "src/ai_trading_system/platform/architecture/compatibility_authority.py",
        "src/ai_trading_system/platform/architecture/lease_arbiter.py",
        "src/ai_trading_system/platform/architecture/parallel_control_kernel.py",
        "src/ai_trading_system/platform/architecture/source_preservation.py",
        "tests/test_arch_004_refactor_policy.py",
        "tests/test_arch_004g_deprecation.py",
        "tests/test_arch_005_integration_publication_fence.py",
        "tests/test_arch_005_lease_arbiter.py",
        "tests/test_arch_005_s2_kernel.py",
        "tests/test_arch_005_s4d_checkout_guard.py",
        "tests/test_arch_005_s5_task_source_cutover.py",
        "tests/test_arch_005_source_preservation.py",
        "tests/test_architecture_wave_readiness.py",
        "tests/test_devx_006c_compatibility_authority.py",
        "tests/test_devx_006d_report_catalog_flow_authority.py",
        "tests/test_trading2452_architecture_contract.py",
    }
)
TRADING_2564_S2B_SOURCE_PATHS = frozenset(
    {
        "inputs/data_quality/dq_issue_attribution_readiness_inventory_v1.json",
        "inputs/data_quality/dq_issue_attribution_readiness_inventory_v1.validation.json",
        "docs/data_quality/dq_issue_attribution_readiness_inventory_v1.md",
        "inputs/data_quality/rate_issue_attribution_review_pack_v1.json",
        "inputs/data_quality/rate_issue_attribution_review_pack_v1.validation.json",
        "docs/data_quality/rate_issue_attribution_review_pack_v1.md",
        "config/data_quality/rate_row_issue_attribution_decision_v1.yaml",
        "src/ai_trading_system/contracts/rate_data_quality_attribution.py",
        "tests/test_rate_issue_attribution_contract.py",
        "tests/test_data_quality_issue_attribution_inventory.py",
        "tests/test_trading2452_architecture_contract.py",
        "src/ai_trading_system/contracts/named_data_quality_execution.py",
        "src/ai_trading_system/contracts/named_execution_context.py",
        "src/ai_trading_system/data/quality_provenance.py",
        "src/ai_trading_system/data/named_quality_execution.py",
        "src/ai_trading_system/data/quality.py",
        "src/ai_trading_system/data/quality_execution.py",
        "scripts/run_named_data_quality.py",
        "config/data_governance/named_data_quality_execution_sources_v1.json",
        "config/architecture/devx_006d_report_catalog_flow_authority.yaml",
        "tests/test_named_data_quality_execution_contract.py",
        "tests/test_named_data_quality_execution.py",
        "tests/test_named_data_quality_bootstrap.py",
        "tests/test_named_data_quality_candidate.py",
        "tests/named_data_quality_support.py",
        "tests/test_data_quality.py",
        "tests/test_data_quality_execution.py",
        "tests/test_qqq_options_signal_package.py",
        "tests/test_research_input_readiness.py",
        "tests/test_arch_004_refactor_policy.py",
        "tests/test_arch_004g_deprecation.py",
        "tests/test_devx_006c_compatibility_authority.py",
        "tests/test_devx_006d_report_catalog_flow_authority.py",
        "src/ai_trading_system/platform/architecture/compatibility_authority.py",
        "docs/requirements/TRADING-2564_Long_Term_Research_Capability_Improvement_V1.md",
        "docs/requirements/TRADING-2564_S2b_Named_DQ_Execution_Contract_V1.md",
        "docs/system_flow.md",
        "docs/artifact_catalog.md",
        "registry/development_tasks/c8/c8c1f96abee465a20184abbf6558c5183466d30eb4b1581e8fc922a6276b5a00.yaml",
        "inputs/architecture/arch_005_task_registry_index.yaml",
    }
)

TRADING_2564_S2C_SOURCE_PATHS = frozenset(
    {
        "src/ai_trading_system/contracts/named_data_quality_execution.py",
        "src/ai_trading_system/data_foundation.py",
        "src/ai_trading_system/simple_baseline_portfolio_control.py",
        "config/research/simple_baseline_strategy_registry.yaml",
        "config/data_governance/named_data_quality_execution_sources_v1.json",
        "config/data_governance/named_equal_risk_price_consumer_sources_v1.json",
        "tests/test_named_data_quality_execution_contract.py",
        "tests/test_named_data_quality_execution.py",
        "tests/test_named_data_quality_candidate.py",
        "tests/test_named_data_quality_bootstrap.py",
        "tests/named_data_quality_support.py",
        "docs/requirements/TRADING-2564_Long_Term_Research_Capability_Improvement_V1.md",
        "docs/requirements/TRADING-2564_S2b_Named_DQ_Execution_Contract_V1.md",
        "docs/requirements/TRADING-2564_S2c_Equal_Risk_Price_Consumer_Scope_V1.md",
        "docs/system_flow.md",
        "docs/artifact_catalog.md",
        "config/architecture/devx_006d_report_catalog_flow_authority.yaml",
        "src/ai_trading_system/platform/architecture/compatibility_authority.py",
        "tests/test_arch_004_refactor_policy.py",
        "tests/test_arch_004g_deprecation.py",
        "tests/test_arch_005_s5_task_source_cutover.py",
        "tests/test_devx_006c_compatibility_authority.py",
        "tests/test_devx_006d_report_catalog_flow_authority.py",
        "tests/test_trading2452_architecture_contract.py",
        "registry/development_tasks/c8/c8c1f96abee465a20184abbf6558c5183466d30eb4b1581e8fc922a6276b5a00.yaml",
        "inputs/architecture/arch_005_task_registry_index.yaml",
    }
)

TRADING_2564_S2C2_SOURCE_PATHS = TRADING_2564_S2C_SOURCE_PATHS | frozenset(
    {
        "src/ai_trading_system/data/named_quality_execution.py",
        "src/ai_trading_system/contracts/named_simple_baseline_preview.py",
        "src/ai_trading_system/simple_baseline_named_preview.py",
        "config/data_governance/named_simple_baseline_preview_sources_v1.json",
        "tests/test_named_simple_baseline_preview.py",
        "tests/test_named_simple_baseline_preview_candidate.py",
        "docs/requirements/TRADING-2564_S2c2_Five_Candidate_Read_Only_Preview_V1.md",
    }
)

TRADING_2564_S3A_SOURCE_PATHS = TRADING_2564_S2C2_SOURCE_PATHS | frozenset(
    {
        "src/ai_trading_system/contracts/prospective_event_time_evidence.py",
        "src/ai_trading_system/prospective_event_time_evidence.py",
        "config/research/prospective_event_time_evidence_v1.yaml",
        "tests/test_prospective_event_time_evidence.py",
        "tests/test_prospective_event_time_evidence_contract.py",
        "docs/requirements/TRADING-2564_S3a_Prospective_Event_Time_Evidence_V1.md",
    }
)

TRADING_2564_S3B_SOURCE_PATHS = TRADING_2564_S3A_SOURCE_PATHS | frozenset(
    {
        "src/ai_trading_system/contracts/prospective_capture_execution.py",
        "src/ai_trading_system/prospective_capture_execution.py",
        "src/ai_trading_system/data/named_quality_dispatch.py",
        "config/data_governance/named_prospective_five_candidate_sources_v1.json",
        "config/research/prospective_capture_execution_v1.yaml",
        "config/research/prospective_capture_execution_v2.yaml",
        "config/research/prospective_event_time_evidence_v2.yaml",
        "config/research/host_clock_evidence_v1.yaml",
        "src/ai_trading_system/contracts/host_clock_evidence.py",
        "src/ai_trading_system/host_clock_evidence.py",
        "tests/test_host_clock_evidence_contract.py",
        "tests/test_host_clock_evidence.py",
        "docs/requirements/TRADING-2564_S3b_Clock_Evidence_Correction_V1.md",
        "tests/test_named_quality_dispatch.py",
        "tests/test_named_data_quality_actual_candidate.py",
        "tests/test_prospective_capture_execution.py",
        "tests/test_prospective_capture_execution_contract.py",
        "tests/test_simple_baseline_named_preview.py",
        "docs/requirements/TRADING-2564_S3b_Prospective_Capture_Execution_V1.md",
    }
)

TRADING_2564_S2A_SOURCE_PATHS = frozenset(
    {
        "src/ai_trading_system/data/immutable_publish.py",
        "src/ai_trading_system/data/download_publication.py",
        "src/ai_trading_system/platform/architecture/compatibility_authority.py",
        "tests/test_named_immutable_publication.py",
        "tests/test_arch_004_refactor_policy.py",
        "tests/test_devx_006c_compatibility_authority.py",
        "tests/test_devx_006d_report_catalog_flow_authority.py",
        "docs/requirements/TRADING-2564_Long_Term_Research_Capability_Improvement_V1.md",
        (
            "registry/development_tasks/c8/"
            "c8c1f96abee465a20184abbf6558c5183466d30eb4b1581e8fc922a6276b5a00.yaml"
        ),
        "inputs/architecture/arch_005_task_registry_index.yaml",
    }
)


def _write_fixture_authority(
    root: Path,
    *,
    sections: list[tuple[str, dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    legacy_path = root / REAL_LEGACY_PATH
    legacy_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_bytes = b"schema_version: fixture.v1\nlegacy_section:\n  status: PASS\n"
    legacy_path.write_bytes(legacy_bytes)
    legacy = safe_load_yaml_text(legacy_bytes.decode("utf-8"))
    legacy_ids = list(legacy)
    legacy_seal = {
        "path": REAL_LEGACY_PATH.as_posix(),
        "byte_count": len(legacy_bytes),
        "file_sha256": hashlib.sha256(legacy_bytes).hexdigest(),
        "lf_sha256": hashlib.sha256(legacy_bytes).hexdigest(),
        "git_blob": authority._git_blob_id(legacy_bytes),
        "top_level_entry_count": len(legacy_ids),
        "ordered_entry_ids_sha256": hashlib.sha256(
            ("\n".join(legacy_ids) + "\n").encode("utf-8")
        ).hexdigest(),
        "mapping_replay_sha256": hashlib.sha256(
            authority._canonical_mapping_bytes(legacy, sort_keys=False)
        ).hexdigest(),
        "grandfathered_duplicate_key_count": 0,
        "grandfathered_duplicate_key_behavior": "NONE",
    }
    policy = {
        "schema_version": "devx_006c_compatibility_authority_policy.v1",
        "status": "ACTIVE",
        "task_id": "DEVX-006C_COMPATIBILITY_AUTHORITY_FRAGMENTATION",
        "exact_start_base": "0" * 40,
        "owner_decision": "fixture-owner-decision",
        "legacy_prefix": legacy_seal,
        "fragment_root": "registry/architecture_compatibility_authority/fragments",
        "index_path": "inputs/architecture/devx_006c_compatibility_authority_index.json",
        "consumer_inventory_path": (
            "inputs/architecture/devx_006c_compatibility_consumer_inventory.json"
        ),
        "contract": {
            "legacy_append_allowed": False,
            "fragment_source_active": True,
            "dual_write": False,
            "fragment_identity": "CANONICAL_SECTION_SHA256",
            "index_chain": "SHA256",
            "rollback_mode": "FROZEN_LEGACY_PREFIX_ONLY",
        },
        "production_effect": "none",
        "broker_action": "none",
    }
    policy_path = root / authority.DEFAULT_POLICY_PATH
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.write_text(
        yaml.safe_dump(policy, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    loaded_policy = load_compatibility_policy(root)
    requested = sections or [("new_section", {"status": "PASS"})]
    fragments: list[tuple[str, str, dict[str, Any], bytes]] = []
    fragment_paths: list[Path] = []
    for section_id, section in requested:
        relative, record, content = render_fragment(
            section_id=section_id,
            section=section,
        )
        fragment_path = root / Path(loaded_policy["fragment_root"]) / Path(relative)
        fragment_path.parent.mkdir(parents=True, exist_ok=True)
        fragment_path.write_bytes(content)
        fragment_paths.append(fragment_path)
        fragments.append((section_id, relative, record, content))
    index, index_bytes = render_index(policy=loaded_policy, fragments=fragments)
    index_path = root / Path(loaded_policy["index_path"])
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_bytes(index_bytes)
    return {
        "legacy_path": legacy_path,
        "legacy_bytes": legacy_bytes,
        "policy": loaded_policy,
        "fragment_paths": fragment_paths,
        "index_path": index_path,
        "index": index,
    }


def _write_index(path: Path, index: dict[str, Any]) -> None:
    path.write_bytes(canonical_json_bytes(index, sort_keys=True, indent=2, ensure_ascii=False))


def _rehash_entry(index: dict[str, Any], position: int) -> None:
    entry = index["entries"][position]
    entry_without_hash = {key: entry[key] for key in authority._ENTRY_FIELDS - {"entry_sha256"}}
    entry["entry_sha256"] = authority._entry_hash(entry_without_hash)
    index["final_chain_sha256"] = index["entries"][-1]["entry_sha256"]


def test_repository_authority_is_fresh_and_cut_over() -> None:
    result = validate_repository_authority()
    merged = load_compatibility_authority()
    legacy_only = load_compatibility_authority(include_fragments=False)

    assert result["status"] == "PASS"
    assert len(legacy_only) == 306
    assert len(merged) == 327
    assert result["fragment_count"] == 21
    assert next(reversed(legacy_only)) == (
        "phase_trading_2504_qqq_options_owner_decision_manifest_v1"
    )
    assert next(reversed(merged)) == TRADING_2564_S3B_SECTION
    assert DEVX_006C_SECTION in merged
    assert DEVX_006D_SECTION in merged
    assert merged[ARCH_005_S5_SECTION]["task_registry_authority"]["source_of_truth"] == (
        "ARCH_005_TASK_REGISTRY"
    )
    assert merged[DEVX_007_V2_SECTION]["submission_authorization"] == {
        "explicit_current_request_is_submission_authority": True,
        "non_sensitive_public_or_authorized_exact_commit_only": True,
        "repeat_send_confirmation_required": False,
        "second_submission_requires_separate_recovery_or_authorization": True,
        "sensitive_private_unscoped_fail_closed": True,
        "scope_expansion_is_new_authority_scope": True,
    }
    assert merged[TRADING_2542C_SECTION]["research_boundary"] == {
        "threshold_bundle_frozen": False,
        "real_dq_or_empirical_run_authorized": False,
        "second_independent_review_required": True,
        "owner_exact_value_approval_required": True,
    }
    assert merged[DEVX_009_SECTION]["publication_contract"] == {
        "single_active_coordinator": True,
        "expected_main_compare_and_set": True,
        "generated_state_rebuild_once": True,
        "full_dispatch_claim_is_atomic": True,
        "closeout_receipt_is_replayable": True,
    }
    assert merged[TRADING_2542D_SECTION]["engineering_contract"] == {
        "target_session_count": 1202,
        "target_session_inventory_lf_sha256": (
            "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0"
        ),
        "pre_window_prior_session": "2021-02-19",
        "excluded_invalid_propagates": True,
        "zero_expected_terminal": "FAIL",
        "expected_nonempty_zero_observed_terminal": "INVALID",
        "right_censor_after_transitive_cluster_merge": True,
        "cost_reconciliation_session_keyed": True,
    }
    assert merged[PROD_004_SECTION]["pit_consumption_contract"] == {
        "primary_research_start": "2021-02-22",
        "default_manifest_mode": "CUMULATIVE_ARCHIVE_DISCOVERY",
        "explicit_manifest_compatibility_preserved": True,
        "daily_capture_date_directories_must_be_iso": True,
        "conflicting_snapshot_id_fails_closed": True,
        "valuation_history_recursive_pattern_restricted": True,
        "duplicate_valuation_snapshot_id_fails_closed": True,
        "strict_pit_grade_a_inferred": False,
    }
    assert merged[DEVX_011_SECTION]["workflow_health_contract"] == {
        "window_days": 7,
        "window_timezone": "UTC",
        "telemetry_sources": [
            "validation_runtime",
            "publication_transactions",
            "git_main_history",
        ],
        "candidate_fingerprint_stable_across_dates": True,
        "candidate_review_only": True,
        "weekly_self_trigger_enabled": True,
        "automatic_dispatch_enabled": False,
        "task_or_code_mutation_allowed": False,
        "validation_gate_change_allowed": False,
    }
    assert merged[DEVX_012_SECTION]["workflow_health_automatic_cycle"] == {
        "existing_automation_id": "aitradingsystem-pit",
        "second_scheduler_created": False,
        "iso_week_validated_bundle_deduplication": True,
        "failed_or_blocked_retry_on_next_existing_invocation": True,
        "main_origin_head_identity_required": True,
        "automatic_report_generation_enabled": True,
        "automatic_optimization_execution_enabled": False,
        "candidate_task_or_code_mutation_allowed": False,
        "validation_gate_change_allowed": False,
        "prior_validated_week_metric_comparison": True,
        "candidate_lifecycle_reported": True,
    }
    assert merged[RISK_012_SECTION]["risk_event_admission_contract"] == {
        "reviewed_config_required": True,
        "unknown_matched_id_is_error": True,
        "batch_error_writes_zero_occurrences": True,
        "batch_error_writes_zero_attestations": True,
        "known_match_preferred_from_mixed_list": True,
        "validator_relaxed": False,
    }
    assert merged[DEVX_006C_SECTION]["authority_contract"] == {
        "dual_write": False,
        "fragment_identity": "CANONICAL_SECTION_SHA256",
        "fragment_source_active": True,
        "index_chain": "SHA256",
        "legacy_append_allowed": False,
        "rollback_mode": "FROZEN_LEGACY_PREFIX_ONLY",
    }


def _assert_devx_014_source_closure(phase: dict[str, Any]) -> None:
    paths = [row["path"] for row in phase["sources"]]
    assert len(DEVX_014_SOURCE_PATHS) == 34
    assert paths == sorted(DEVX_014_SOURCE_PATHS, key=str.casefold)
    assert phase["superseded_live_source_paths"] == paths
    for row in phase["sources"]:
        assert set(row) == {"path", "sha256", "hash_normalization"}
        assert row["hash_normalization"] == "git_eol_lf"
        content = Path(row["path"]).read_bytes().replace(b"\r\n", b"\n")
        assert hashlib.sha256(content).hexdigest() == row["sha256"], row["path"]


def test_devx_014_is_exact_source_preservation_and_os_arbiter_successor() -> None:
    merged = load_compatibility_authority()
    assert list(merged).index(TRADING_2564_S2B_SECTION) == list(merged).index(DEVX_014_SECTION) + 1
    assert list(merged).index(DEVX_014_SECTION) == list(merged).index(OPS_079_SECTION) + 1
    phase = merged[DEVX_014_SECTION]
    assert set(phase) == {
        "schema_version",
        "task_id",
        "status",
        "owner_decision",
        "authority_contract",
        "sources",
        "superseded_live_source_paths",
        "supersession",
        "source_preservation_contract",
        "os_arbiter_contract",
        "migration_contract",
        "safety",
        "production_effect",
        "broker_action",
    }
    assert phase["schema_version"] == "devx_014_dirty_source_preservation_and_os_lease_arbiter.v1"
    assert phase["task_id"] == "DEVX-014_DIRTY_SOURCE_PRESERVATION_RECOVERY_V1"
    assert phase["status"] == "VALIDATING"
    assert phase["owner_decision"] == "owner_instruction:DEVX-014:2026-09-06:arbiter-safety-fix"
    assert phase["authority_contract"] == load_compatibility_policy()["contract"]
    assert phase["supersession"] == {
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": OPS_079_SECTION,
        "current_hash_authority": f"{DEVX_014_SECTION}.sources",
    }
    _assert_devx_014_source_closure(phase)
    assert phase["source_preservation_contract"] == {
        "snapshot_profile": "RAW_BYTES_SOURCE_ONLY_UNVALIDATED",
        "implementation_profile": "COMMITTED_SOURCE_GIT_EOL_LF",
        "tracked_unstaged_regular_modified_only": True,
        "raw_source_bytes_preserved": True,
        "real_index_and_worktree_unchanged": True,
        "canonical_history_append_only": True,
        "create_only_snapshot_ref": True,
        "ordinary_integration_required": True,
        "git_configuration_profile": "source_preservation_git_configuration.v1",
        "trusted_and_source_worktree_config_checked": True,
        "configuration_drift_before_mutation_rejected": True,
        "configuration_raw_values_retained": False,
    }
    assert phase["os_arbiter_contract"] == {
        "protocol": "execution_lease_os_arbiter.v2",
        "sole_existing_arbiter": True,
        "stable_regular_file": True,
        "normal_anchor_rename_or_unlink_allowed": False,
        "live_owner_ttl_takeover_allowed": False,
        "owner_safe_handle_release": True,
        "logical_lease_contract_changed": False,
    }
    assert phase["migration_contract"] == {
        "legacy_conversion_mode": "EXPLICIT_QUIESCENT_ONLY",
        "create_only_anchor": True,
        "partial_failure_blocks_replay": True,
        "arbitrary_store_target_allowed": False,
        "implementation_profiles": {
            "current_root_bootstrap": "REVIEWED_WORKING_SOURCE_ENGINEERING_ONLY",
            "published_source_migration": "COMMITTED_PUBLISHED_SOURCE_GIT_EOL_LF",
        },
    }
    assert phase["safety"] == {
        "snapshot_grants_task_source_write": False,
        "snapshot_grants_generator": False,
        "snapshot_grants_formal_validation": False,
        "snapshot_grants_full": False,
        "snapshot_grants_main_ff": False,
        "snapshot_grants_push": False,
        "snapshot_grants_research": False,
        "snapshot_grants_data_action": False,
        "snapshot_grants_trading": False,
        "ordinary_publication_fence_changed": False,
        "research_or_data_authorization_expanded": False,
        "broker_or_trading_authorization_expanded": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    assert phase["production_effect"] == phase["broker_action"] == "none"


@pytest.mark.parametrize(
    "mutation",
    ["extra_source", "extra_in_both", "missing_source", "missing_from_both", "wrong_hash"],
)
def test_devx_014_source_closure_rejects_unreviewed_changes(mutation: str) -> None:
    phase = deepcopy(load_compatibility_authority()[DEVX_014_SECTION])
    if mutation in {"extra_source", "extra_in_both"}:
        phase["sources"].append(
            {"path": "unreviewed.py", "sha256": "0" * 64, "hash_normalization": "git_eol_lf"}
        )
        if mutation == "extra_in_both":
            phase["superseded_live_source_paths"].append("unreviewed.py")
    elif mutation == "missing_source":
        phase["sources"].pop()
    elif mutation == "missing_from_both":
        removed = "src/ai_trading_system/platform/architecture/parallel_control_kernel.py"
        phase["sources"] = [row for row in phase["sources"] if row["path"] != removed]
        phase["superseded_live_source_paths"] = [
            path for path in phase["superseded_live_source_paths"] if path != removed
        ]
    else:
        phase["sources"][0]["sha256"] = "0" * 64
    with pytest.raises(AssertionError):
        _assert_devx_014_source_closure(phase)


def test_devx_014_validator_rejects_changed_source_without_rebuild(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = Path("src/ai_trading_system/platform/architecture/source_preservation.py").resolve()
    original = Path.read_bytes

    def altered_read(path: Path) -> bytes:
        content = original(path)
        return content + b"\n# synthetic DEVX-014 source drift\n" if path == target else content

    monkeypatch.setattr(Path, "read_bytes", altered_read)
    with pytest.raises(CompatibilityAuthorityError) as caught:
        validate_repository_authority()
    assert caught.value.code == "AUTHORITY_GENERATED_STALE"
    assert caught.value.detail == "inputs/architecture/devx_006c_compatibility_authority_index.json"


def _assert_s2a_source_closure(phase: dict[str, Any]) -> None:
    paths = [row["path"] for row in phase["sources"]]
    assert paths == sorted(TRADING_2564_S2A_SOURCE_PATHS, key=str.casefold)
    assert phase["superseded_live_source_paths"] == paths
    for row in phase["sources"]:
        assert row["hash_normalization"] == "git_eol_lf"
        content = Path(row["path"]).read_bytes().replace(b"\r\n", b"\n")
        assert hashlib.sha256(content).hexdigest() == row["sha256"], row["path"]


def test_s2a_is_exact_named_snapshot_successor_authority() -> None:
    merged = load_compatibility_authority()
    assert list(merged).index(TRADING_2564_S2A_SECTION) < list(merged).index(OPS_079_SECTION)
    assert list(merged).index(OPS_078_SECTION) < list(merged).index(TRADING_2564_S2A_SECTION)
    phase = merged[TRADING_2564_S2A_SECTION]
    assert phase["schema_version"] == "trading_2564_s2a_named_immutable_snapshot.v1"
    assert phase["task_id"] == "TRADING-2564_LONG_TERM_RESEARCH_CAPABILITY_IMPROVEMENT_V1"
    assert phase["status"] == "VALIDATING"
    assert phase["owner_decision"] == (
        "owner_instruction:TRADING-2564:2026-09-05:long-term-capability"
    )
    assert phase["supersession"] == {
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": OPS_078_SECTION,
        "current_hash_authority": f"{TRADING_2564_S2A_SECTION}.sources",
    }
    _assert_s2a_source_closure(phase)
    assert phase["named_snapshot_contract"] == {
        "exact_pointer_and_transaction_identity_required": True,
        "committed_chain_membership_required": True,
        "current_selects_replacement_input": False,
        "validation_scope": "STRUCTURAL_PUBLICATION_ONLY",
        "legacy_projection_status": "NOT_EVALUATED",
    }
    assert phase["safety"] == {
        "dq_validation_executed": False,
        "consumer_cutover_allowed": False,
        "dispatch_allowed": False,
        "historical_receipt_rewritten": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    assert phase["production_effect"] == phase["broker_action"] == "none"


@pytest.mark.parametrize(
    "mutation",
    ["extra_source", "missing_source", "tail_test_removed_from_both", "wrong_hash"],
)
def test_s2a_source_closure_rejects_unreviewed_changes(mutation: str) -> None:
    phase = deepcopy(load_compatibility_authority()[TRADING_2564_S2A_SECTION])
    if mutation == "extra_source":
        phase["sources"].append({"path": "unreviewed.py", "sha256": "0" * 64})
    elif mutation == "missing_source":
        phase["sources"].pop()
    elif mutation == "tail_test_removed_from_both":
        removed = "tests/test_devx_006d_report_catalog_flow_authority.py"
        phase["sources"] = [row for row in phase["sources"] if row["path"] != removed]
        phase["superseded_live_source_paths"] = [
            path for path in phase["superseded_live_source_paths"] if path != removed
        ]
    else:
        phase["sources"][0]["sha256"] = "0" * 64
    with pytest.raises(AssertionError):
        _assert_s2a_source_closure(phase)


@pytest.mark.parametrize(
    ("portable", "expected_code"),
    [
        (
            "src/ai_trading_system/data/immutable_publish.py",
            "AUTHORITY_GENERATED_STALE",
        ),
        (
            "src/ai_trading_system/data/download_publication.py",
            "AUTHORITY_GENERATED_STALE",
        ),
        (
            "tests/test_devx_006d_report_catalog_flow_authority.py",
            "AUTHORITY_FILE_MISSING",
        ),
    ],
)
def test_s2a_validator_rejects_changed_source_without_rebuild(
    monkeypatch: pytest.MonkeyPatch, portable: str, expected_code: str
) -> None:
    original = Path.read_bytes
    target = Path(portable).resolve()

    def altered_read(path: Path) -> bytes:
        content = original(path)
        return content + b"\n# synthetic source drift\n" if path == target else content

    monkeypatch.setattr(Path, "read_bytes", altered_read)
    with pytest.raises(CompatibilityAuthorityError) as caught:
        validate_repository_authority()
    assert caught.value.code == expected_code
    if expected_code == "AUTHORITY_GENERATED_STALE":
        assert (
            caught.value.detail
            == "inputs/architecture/devx_006c_compatibility_authority_index.json"
        )
    else:
        assert caught.value.detail.startswith(
            "registry/architecture_compatibility_authority/fragments/"
        )


def _assert_s2b_source_closure(phase: dict[str, Any]) -> None:
    paths = [row["path"] for row in phase["sources"]]
    assert len(TRADING_2564_S2B_SOURCE_PATHS) == 40
    assert paths == sorted(TRADING_2564_S2B_SOURCE_PATHS, key=str.casefold)
    assert phase["superseded_live_source_paths"] == paths
    for row in phase["sources"]:
        assert set(row) == {"path", "sha256", "hash_normalization"}
        assert row["hash_normalization"] == "git_eol_lf"
        content = Path(row["path"]).read_bytes().replace(b"\r\n", b"\n")
        assert hashlib.sha256(content).hexdigest() == row["sha256"], row["path"]


def test_s2b_is_exact_named_execution_successor_authority() -> None:
    from test_trading2452_architecture_contract import (
        TRADING_2480_CAPABILITY_DISCOVERY_SUCCESSOR_CURRENT_AUTHORITY_PATHS,
        TRADING_2564_S2B_RESTRICTED_CURRENT_AUTHORITY_PATHS,
    )

    assert TRADING_2564_S2B_RESTRICTED_CURRENT_AUTHORITY_PATHS == (
        TRADING_2564_S2B_SOURCE_PATHS
        & TRADING_2480_CAPABILITY_DISCOVERY_SUCCESSOR_CURRENT_AUTHORITY_PATHS
    )
    merged = load_compatibility_authority()
    assert (
        list(merged).index(TRADING_2564_S2C_SECTION)
        == list(merged).index(TRADING_2564_S2B_SECTION) + 1
    )
    assert list(merged).index(TRADING_2564_S2A_SECTION) < list(merged).index(
        TRADING_2564_S2B_SECTION
    )
    assert list(merged).index(TRADING_2564_S2B_SECTION) == list(merged).index(DEVX_014_SECTION) + 1
    phase = merged[TRADING_2564_S2B_SECTION]
    assert phase["authority_contract"] == load_compatibility_policy()["contract"]
    assert phase["schema_version"] == "trading_2564_s2b_named_dq_execution.v1"
    assert phase["task_id"] == "TRADING-2564_LONG_TERM_RESEARCH_CAPABILITY_IMPROVEMENT_V1"
    assert phase["status"] == "VALIDATING"
    assert (
        phase["owner_decision"] == "owner_instruction:TRADING-2564:2026-09-05:long-term-capability"
    )
    assert phase["supersession"] == {
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": DEVX_014_SECTION,
        "current_hash_authority": f"{TRADING_2564_S2B_SECTION}.sources",
    }
    _assert_s2b_source_closure(phase)
    assert phase["named_dq_contract"] == {
        "source_kind": "GIT_COMMIT_BYTES_COMPILED",
        "original_manifest_full_row_binding_required": True,
        "separate_source_publication_execution_evidence_roots": True,
        "canonical_dq_calls_per_runner": 1,
        "canonical_dq_calls_per_verifier": 0,
        "successful_terminal_dispatch_binding_required": True,
        "complete_report_projection_exact_bytes_required": True,
        "verified_bytes_bound_to_current_pid_and_context": True,
        "verified_seal_export_allowed": False,
        "coverage_semantics": "CANONICAL_DQ_RULES_ONLY",
    }
    assert phase["safety"] == {
        "real_dq_or_research_executed": False,
        "dq_numeric_rules_changed": False,
        "consumer_cutover_allowed": False,
        "dispatch_allowed": False,
        "historical_receipt_rewritten": False,
        "publication_fence_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


@pytest.mark.parametrize(
    "mutation", ["extra", "extra_in_both", "missing", "both_missing_bootstrap", "wrong_hash"]
)
def test_s2b_source_closure_rejects_rebuilt_but_incomplete_authority(mutation: str) -> None:
    phase = deepcopy(load_compatibility_authority()[TRADING_2564_S2B_SECTION])
    if mutation in {"extra", "extra_in_both"}:
        phase["sources"].append(
            {"path": "unreviewed.py", "sha256": "0" * 64, "hash_normalization": "git_eol_lf"}
        )
        if mutation == "extra_in_both":
            phase["superseded_live_source_paths"].append("unreviewed.py")
    elif mutation == "missing":
        phase["sources"].pop()
    elif mutation == "both_missing_bootstrap":
        removed = "scripts/run_named_data_quality.py"
        phase["sources"] = [row for row in phase["sources"] if row["path"] != removed]
        phase["superseded_live_source_paths"] = [
            path for path in phase["superseded_live_source_paths"] if path != removed
        ]
    else:
        phase["sources"][0]["sha256"] = "0" * 64
    with pytest.raises(AssertionError):
        _assert_s2b_source_closure(phase)


@pytest.mark.parametrize(
    "portable,expected_code",
    [
        ("scripts/run_named_data_quality.py", "AUTHORITY_GENERATED_STALE"),
        (
            "src/ai_trading_system/data/named_quality_execution.py",
            "AUTHORITY_FILE_MISSING",
        ),
        ("tests/test_named_data_quality_candidate.py", "AUTHORITY_FILE_MISSING"),
    ],
)
def test_s2b_validator_rejects_changed_source_without_rebuild(
    monkeypatch: pytest.MonkeyPatch, portable: str, expected_code: str
) -> None:
    original = Path.read_bytes
    target = Path(portable).resolve()

    def altered_read(path: Path) -> bytes:
        content = original(path)
        return content + b"\n# synthetic S2b drift\n" if path == target else content

    monkeypatch.setattr(Path, "read_bytes", altered_read)
    with pytest.raises(CompatibilityAuthorityError) as caught:
        validate_repository_authority()
    assert caught.value.code == expected_code
    if expected_code == "AUTHORITY_GENERATED_STALE":
        # Retained S2b-only sources change the index; the S2c fragment is stable.
        assert (
            caught.value.detail
            == "inputs/architecture/devx_006c_compatibility_authority_index.json"
        )
    else:
        # This source is also in the exact current S2c closure.
        assert caught.value.detail.startswith(
            "registry/architecture_compatibility_authority/fragments/"
        )


def _assert_s2c2_source_closure(phase: dict[str, Any]) -> None:
    paths = [row["path"] for row in phase["sources"]]
    assert len(TRADING_2564_S2C2_SOURCE_PATHS) == 33
    assert paths == sorted(TRADING_2564_S2C2_SOURCE_PATHS, key=str.casefold)
    assert phase["superseded_live_source_paths"] == paths
    for row in phase["sources"]:
        assert set(row) == {"path", "sha256", "hash_normalization"}
        assert row["hash_normalization"] == "git_eol_lf"
        assert (
            hashlib.sha256(Path(row["path"]).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
            == row["sha256"]
        )


def test_s2c2_exact_preview_successor_preserves_closed_execution_boundary() -> None:
    from test_trading2452_architecture_contract import (
        TRADING_2480_CAPABILITY_DISCOVERY_SUCCESSOR_CURRENT_AUTHORITY_PATHS,
        TRADING_2564_S2C2_RESTRICTED_CURRENT_AUTHORITY_PATHS,
    )

    merged = load_compatibility_authority()
    assert next(reversed(merged)) == TRADING_2564_S3B_SECTION
    assert (
        list(merged).index(TRADING_2564_S2C2_SECTION)
        == list(merged).index(TRADING_2564_S2C_SECTION) + 1
    )
    assert TRADING_2564_S2C2_RESTRICTED_CURRENT_AUTHORITY_PATHS == (
        TRADING_2564_S2C2_SOURCE_PATHS
        & TRADING_2480_CAPABILITY_DISCOVERY_SUCCESSOR_CURRENT_AUTHORITY_PATHS
    )
    phase = merged[TRADING_2564_S2C2_SECTION]
    _assert_s2c2_source_closure(phase)
    assert phase["supersession"] == {
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": TRADING_2564_S2C_SECTION,
        "current_hash_authority": f"{TRADING_2564_S2C2_SECTION}.sources",
    }
    assert phase["preview_contract"] == {
        "source_manifest_path": (
            "config/data_governance/named_simple_baseline_preview_sources_v1.json"
        ),
        "compiled_module_count": 59,
        "execution_dependency_count": 8,
        "candidate_count": 5,
        "registry_semantics": "COMPLETE_CAPTURED_REVIEWED_BYTES",
        "calendar_semantics": "VERIFIER_SEALED_CANONICAL_SESSIONS",
        "consumer_io_allowed": False,
        "original_algorithms_reused": True,
        "original_common_evaluated_window_preserved": True,
        "legacy_55_7_and_57_8_profiles_preserved": True,
    }
    assert phase["safety"] == {
        "real_dq_or_research_executed": False,
        "dq_numeric_rules_changed": False,
        "strategy_policy_changed": False,
        "pit_or_oos_established": False,
        "observation_created": False,
        "returns_computed": False,
        "consumer_cutover_allowed": False,
        "dispatch_allowed": False,
        "historical_receipt_rewritten": False,
        "publication_fence_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


@pytest.mark.parametrize("mutation", ["missing", "missing_both", "extra_both", "wrong_hash"])
def test_s2c2_source_identity_is_not_self_reported_lists(mutation: str) -> None:
    phase = deepcopy(load_compatibility_authority()[TRADING_2564_S2C2_SECTION])
    if mutation in {"missing", "missing_both"}:
        removed = "src/ai_trading_system/simple_baseline_named_preview.py"
        phase["sources"] = [row for row in phase["sources"] if row["path"] != removed]
        if mutation == "missing_both":
            phase["superseded_live_source_paths"].remove(removed)
    elif mutation == "extra_both":
        phase["sources"].append({"path": "unknown.py", "sha256": "0" * 64})
        phase["superseded_live_source_paths"].append("unknown.py")
    else:
        phase["sources"][0]["sha256"] = "0" * 64
    with pytest.raises(AssertionError):
        _assert_s2c2_source_closure(phase)


def _assert_s2c_source_closure(phase: dict[str, Any]) -> None:
    paths = [row["path"] for row in phase["sources"]]
    assert len(TRADING_2564_S2C_SOURCE_PATHS) == 26
    assert paths == sorted(TRADING_2564_S2C_SOURCE_PATHS, key=str.casefold)
    assert phase["superseded_live_source_paths"] == paths
    for row in phase["sources"]:
        assert set(row) == {"path", "sha256", "hash_normalization"}
        assert row["hash_normalization"] == "git_eol_lf"
        assert (
            hashlib.sha256(Path(row["path"]).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
            == row["sha256"]
        )


def test_s2c_is_exact_price_scope_successor_not_strategy_promotion() -> None:
    from test_trading2452_architecture_contract import (
        TRADING_2480_CAPABILITY_DISCOVERY_SUCCESSOR_CURRENT_AUTHORITY_PATHS,
        TRADING_2564_S2C_RESTRICTED_CURRENT_AUTHORITY_PATHS,
    )

    merged = load_compatibility_authority()
    assert next(reversed(merged)) == TRADING_2564_S3B_SECTION
    assert (
        list(merged).index(TRADING_2564_S2C_SECTION)
        == list(merged).index(TRADING_2564_S2B_SECTION) + 1
    )
    assert TRADING_2564_S2C_RESTRICTED_CURRENT_AUTHORITY_PATHS == (
        TRADING_2564_S2C_SOURCE_PATHS
        & TRADING_2480_CAPABILITY_DISCOVERY_SUCCESSOR_CURRENT_AUTHORITY_PATHS
    )
    phase = merged[TRADING_2564_S2C_SECTION]
    assert phase["schema_version"] == "trading_2564_s2c_equal_risk_price_consumer_scope.v1"
    assert phase["task_id"] == "TRADING-2564_LONG_TERM_RESEARCH_CAPABILITY_IMPROVEMENT_V1"
    assert phase["status"] == "VALIDATING"
    assert phase["authority_contract"] == load_compatibility_policy()["contract"]
    assert phase["supersession"] == {
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": TRADING_2564_S2B_SECTION,
        "current_hash_authority": f"{TRADING_2564_S2C_SECTION}.sources",
    }
    _assert_s2c_source_closure(phase)
    assert phase["price_range_contract"] == {
        "consumer_id": "simple_baseline_forward_aging_preview@1.0.0",
        "source_manifest_path": (
            "config/data_governance/named_equal_risk_price_consumer_sources_v1.json"
        ),
        "compiled_module_count": 57,
        "execution_dependency_count": 8,
        "prices_role": "FEATURE_INPUT",
        "rates_role": "DQ_GUARD_ONLY",
        "canonical_full_requested_price_coverage_required": True,
        "registry_binding_semantics": "CAPTURED_IDENTITY_ONLY",
        "original_common_evaluated_window_preserved": True,
        "original_scope_and_accessor_semantics_preserved": True,
    }
    assert phase["safety"] == {
        "real_dq_or_research_executed": False,
        "dq_numeric_rules_changed": False,
        "strategy_semantics_validated": False,
        "consumer_cutover_allowed": False,
        "dispatch_allowed": False,
        "historical_receipt_rewritten": False,
        "publication_fence_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


@pytest.mark.parametrize("mutation", ["missing_both", "extra_both", "wrong_hash"])
def test_s2c_source_closure_cannot_be_redefined_by_matching_lists(mutation: str) -> None:
    phase = deepcopy(load_compatibility_authority()[TRADING_2564_S2C_SECTION])
    if mutation == "missing_both":
        removed = "config/data_governance/named_equal_risk_price_consumer_sources_v1.json"
        phase["sources"] = [row for row in phase["sources"] if row["path"] != removed]
        phase["superseded_live_source_paths"].remove(removed)
    elif mutation == "extra_both":
        phase["sources"].append(
            {"path": "unreviewed.py", "sha256": "0" * 64, "hash_normalization": "git_eol_lf"}
        )
        phase["superseded_live_source_paths"].append("unreviewed.py")
    else:
        phase["sources"][0]["sha256"] = "0" * 64
    with pytest.raises(AssertionError):
        _assert_s2c_source_closure(phase)


def test_legacy_prefix_bytes_equal_exact_start_base() -> None:
    current = REAL_LEGACY_PATH.read_bytes()
    frozen = authority._git_text(
        Path(".").resolve(),
        "cb437a4d4be178180f60cb3ee2d2994c1be45f94",
        REAL_LEGACY_PATH.as_posix(),
    ).encode("utf-8")

    assert current == frozen
    assert hashlib.sha256(current).hexdigest() == (
        "253b976b2740f0097e1d8949ec8eaf3846c82809f88fdf3a47fb76fae6023842"
    )


def test_consumer_inventory_has_no_growth_reader_or_runtime_writer() -> None:
    inventory = json.loads(
        Path("inputs/architecture/devx_006c_compatibility_consumer_inventory.json").read_text(
            encoding="utf-8"
        )
    )

    assert inventory["status"] == "PASS"
    assert inventory["consumer_count"] == 8
    assert inventory["growth_assuming_direct_consumer_count"] == 0
    assert inventory["runtime_legacy_append_writer_count"] == 0
    assert inventory["fixture_legacy_writer_count"] == 1
    assert sum(row["base_growth_assuming_read_count"] for row in inventory["consumers"]) == 135
    assert all(row["migration_status"] == "MIGRATED" for row in inventory["consumers"])


def test_fragment_and_index_render_are_repeatable() -> None:
    policy = load_compatibility_policy()
    first = render_fragment(section_id="repeatable_section", section={"value": [2, 1]})
    second = render_fragment(section_id="repeatable_section", section={"value": [2, 1]})
    first_index = render_index(
        policy=policy,
        fragments=[("repeatable_section", first[0], first[1], first[2])],
    )
    second_index = render_index(
        policy=policy,
        fragments=[("repeatable_section", second[0], second[1], second[2])],
    )

    assert first == second
    assert first_index == second_index


def test_legacy_one_fragment_and_multi_fragment_preserve_order(tmp_path: Path) -> None:
    fixture = _write_fixture_authority(
        tmp_path,
        sections=[("fragment_one", {"value": 1}), ("fragment_two", {"value": 2})],
    )
    merged = load_compatibility_authority(tmp_path)

    assert list(merged) == [
        "schema_version",
        "legacy_section",
        "fragment_one",
        "fragment_two",
    ]
    assert fixture["legacy_path"].read_bytes() == fixture["legacy_bytes"]


def test_new_fragment_does_not_change_existing_fragment_or_legacy(tmp_path: Path) -> None:
    first = _write_fixture_authority(
        tmp_path,
        sections=[("fragment_one", {"value": 1})],
    )
    legacy_before = first["legacy_path"].read_bytes()
    fragment_before = first["fragment_paths"][0].read_bytes()
    _write_fixture_authority(
        tmp_path,
        sections=[("fragment_one", {"value": 1}), ("fragment_two", {"value": 2})],
    )

    assert first["legacy_path"].read_bytes() == legacy_before
    assert first["fragment_paths"][0].read_bytes() == fragment_before


def test_missing_fragment_fails_closed(tmp_path: Path) -> None:
    fixture = _write_fixture_authority(tmp_path)
    fixture["fragment_paths"][0].unlink()

    with pytest.raises(CompatibilityAuthorityError) as caught:
        load_compatibility_authority(tmp_path)
    assert caught.value.code == "AUTHORITY_FILE_MISSING"


def test_fragment_tamper_fails_closed(tmp_path: Path) -> None:
    fixture = _write_fixture_authority(tmp_path)
    fragment = fixture["fragment_paths"][0]
    fragment.write_bytes(fragment.read_bytes() + b" ")

    with pytest.raises(CompatibilityAuthorityError) as caught:
        load_compatibility_authority(tmp_path)
    assert caught.value.code == "AUTHORITY_FRAGMENT_HASH_DRIFT"


def test_fragment_unknown_field_fails_closed(tmp_path: Path) -> None:
    fixture = _write_fixture_authority(tmp_path)
    fragment_path = fixture["fragment_paths"][0]
    fragment = json.loads(fragment_path.read_text(encoding="utf-8"))
    fragment["unexpected"] = True
    fragment_bytes = canonical_json_bytes(fragment, sort_keys=True, indent=2, ensure_ascii=False)
    fragment_path.write_bytes(fragment_bytes)
    index = fixture["index"]
    index["entries"][0]["fragment_sha256"] = hashlib.sha256(fragment_bytes).hexdigest()
    _rehash_entry(index, 0)
    _write_index(fixture["index_path"], index)

    with pytest.raises(CompatibilityAuthorityError) as caught:
        load_compatibility_authority(tmp_path)
    assert caught.value.code == "AUTHORITY_FIELDS_INVALID"


def test_duplicate_section_and_fragment_path_fail_closed(tmp_path: Path) -> None:
    fixture = _write_fixture_authority(tmp_path)
    relative, record, content = render_fragment(section_id="same", section={"value": 1})

    with pytest.raises(CompatibilityAuthorityError) as duplicate_section:
        render_index(
            policy=fixture["policy"],
            fragments=[
                ("same", relative, record, content),
                ("same", relative, record, content),
            ],
        )
    assert duplicate_section.value.code == "AUTHORITY_DUPLICATE_SECTION_ID"

    other_record = dict(record)
    other_record["section_id"] = "other"
    with pytest.raises(CompatibilityAuthorityError) as duplicate_path:
        render_index(
            policy=fixture["policy"],
            fragments=[
                ("same", relative, record, content),
                ("other", relative, other_record, content),
            ],
        )
    assert duplicate_path.value.code == "AUTHORITY_DUPLICATE_FRAGMENT_PATH"


def test_reorder_and_broken_chain_fail_closed(tmp_path: Path) -> None:
    fixture = _write_fixture_authority(
        tmp_path,
        sections=[("fragment_one", {"value": 1}), ("fragment_two", {"value": 2})],
    )
    reordered = fixture["index"]
    reordered["entries"] = list(reversed(reordered["entries"]))
    _write_index(fixture["index_path"], reordered)

    with pytest.raises(CompatibilityAuthorityError) as reorder_error:
        load_compatibility_authority(tmp_path)
    assert reorder_error.value.code == "AUTHORITY_INDEX_ORDER_INVALID"

    fixture = _write_fixture_authority(tmp_path)
    fixture["index"]["entries"][0]["previous_entry_sha256"] = "f" * 64
    _write_index(fixture["index_path"], fixture["index"])
    with pytest.raises(CompatibilityAuthorityError) as chain_error:
        load_compatibility_authority(tmp_path)
    assert chain_error.value.code == "AUTHORITY_INDEX_CHAIN_BROKEN"


def test_path_escape_fails_closed(tmp_path: Path) -> None:
    fixture = _write_fixture_authority(tmp_path)
    index = fixture["index"]
    index["entries"][0]["fragment_path"] = "../outside.json"
    _rehash_entry(index, 0)
    _write_index(fixture["index_path"], index)

    with pytest.raises(CompatibilityAuthorityError) as caught:
        load_compatibility_authority(tmp_path)
    assert caught.value.code == "AUTHORITY_PATH_INVALID"


def test_symlink_fragment_fails_closed(tmp_path: Path) -> None:
    fixture = _write_fixture_authority(tmp_path)
    fragment_path = fixture["fragment_paths"][0]
    content = fragment_path.read_bytes()
    outside = tmp_path / "outside.json"
    outside.write_bytes(content)
    fragment_path.unlink()
    try:
        fragment_path.symlink_to(outside)
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")

    with pytest.raises(CompatibilityAuthorityError) as caught:
        load_compatibility_authority(tmp_path)
    assert caught.value.code == "AUTHORITY_PATH_SYMLINK"


def test_legacy_drift_and_rollback_are_explicit(tmp_path: Path) -> None:
    fixture = _write_fixture_authority(tmp_path)
    legacy_only = load_compatibility_authority(tmp_path, include_fragments=False)
    assert list(legacy_only) == ["schema_version", "legacy_section"]

    fixture["legacy_path"].write_bytes(fixture["legacy_bytes"] + b"# drift\n")
    with pytest.raises(CompatibilityAuthorityError) as caught:
        load_compatibility_authority(tmp_path)
    assert caught.value.code == "AUTHORITY_LEGACY_SIZE_DRIFT"


def _assert_s3a_source_closure(phase: dict[str, Any]) -> None:
    paths = [row["path"] for row in phase["sources"]]
    assert len(TRADING_2564_S3A_SOURCE_PATHS) == 39
    assert paths == sorted(TRADING_2564_S3A_SOURCE_PATHS, key=str.casefold)
    assert phase["superseded_live_source_paths"] == paths
    for row in phase["sources"]:
        assert set(row) == {"path", "sha256", "hash_normalization"}
        assert row["hash_normalization"] == "git_eol_lf"
        assert (
            hashlib.sha256(Path(row["path"]).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
            == row["sha256"]
        )


def test_s3a_exact_temporal_successor_preserves_admission_boundaries() -> None:
    from test_trading2452_architecture_contract import (
        TRADING_2480_CAPABILITY_DISCOVERY_SUCCESSOR_CURRENT_AUTHORITY_PATHS,
        TRADING_2564_S3A_RESTRICTED_CURRENT_AUTHORITY_PATHS,
    )

    merged = load_compatibility_authority()
    assert next(reversed(merged)) == TRADING_2564_S3B_SECTION
    assert (
        list(merged).index(TRADING_2564_S3A_SECTION)
        == list(merged).index(TRADING_2564_S2C2_SECTION) + 1
    )
    assert TRADING_2564_S3A_RESTRICTED_CURRENT_AUTHORITY_PATHS == (
        TRADING_2564_S3A_SOURCE_PATHS
        & TRADING_2480_CAPABILITY_DISCOVERY_SUCCESSOR_CURRENT_AUTHORITY_PATHS
    )
    phase = merged[TRADING_2564_S3A_SECTION]
    _assert_s3a_source_closure(phase)
    assert phase["supersession"] == {
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": TRADING_2564_S2C2_SECTION,
        "current_hash_authority": f"{TRADING_2564_S3A_SECTION}.sources",
    }
    assert phase["event_time_contract"]["timing_version"] == "NEXT_XNYS_CLOSE_FORWARD_V1"
    assert phase["event_time_contract"]["legacy_five_candidate_return_equivalence"] is False
    assert phase["event_time_contract"]["incomplete_event_resigning_allowed"] is False
    assert phase["event_time_contract"]["future_session_recovery_allowed"] is True
    assert phase["safety"] == {
        "real_activation_adopted": False,
        "real_dq_or_research_executed": False,
        "provider_available_at_established": False,
        "pit_or_oos_established": False,
        "witness_own_durability_time_established": False,
        "parent_executor_acknowledgement": "NOT_PRESENT",
        "observation_authorized": False,
        "returns_computed": False,
        "legacy_execution_profiles_changed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


@pytest.mark.parametrize("mutation", ["missing", "missing_both", "extra_both", "wrong_hash"])
def test_s3a_source_identity_is_not_self_reported_lists(mutation: str) -> None:
    phase = deepcopy(load_compatibility_authority()[TRADING_2564_S3A_SECTION])
    if mutation in {"missing", "missing_both"}:
        removed = "src/ai_trading_system/prospective_event_time_evidence.py"
        phase["sources"] = [row for row in phase["sources"] if row["path"] != removed]
        if mutation == "missing_both":
            phase["superseded_live_source_paths"].remove(removed)
    elif mutation == "extra_both":
        phase["sources"].append({"path": "unknown.py", "sha256": "0" * 64})
        phase["superseded_live_source_paths"].append("unknown.py")
    else:
        phase["sources"][0]["sha256"] = "0" * 64
    with pytest.raises(AssertionError):
        _assert_s3a_source_closure(phase)


def _assert_s3b_source_closure(phase: dict[str, Any]) -> None:
    paths = [row["path"] for row in phase["sources"]]
    assert len(TRADING_2564_S3B_SOURCE_PATHS) == 58
    assert paths == sorted(TRADING_2564_S3B_SOURCE_PATHS, key=str.casefold)
    assert phase["superseded_live_source_paths"] == paths
    for row in phase["sources"]:
        assert set(row) == {"path", "sha256", "hash_normalization"}
        assert row["hash_normalization"] == "git_eol_lf"
        assert (
            hashlib.sha256(Path(row["path"]).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
            == row["sha256"]
        )


def test_s3b_exact_capture_successor_preserves_real_research_boundary() -> None:
    from test_trading2452_architecture_contract import (
        TRADING_2480_CAPABILITY_DISCOVERY_SUCCESSOR_CURRENT_AUTHORITY_PATHS,
        TRADING_2564_S3B_RESTRICTED_CURRENT_AUTHORITY_PATHS,
    )

    merged = load_compatibility_authority()
    assert next(reversed(merged)) == TRADING_2564_S3B_SECTION
    assert (
        list(merged).index(TRADING_2564_S3B_SECTION)
        == list(merged).index(TRADING_2564_S3A_SECTION) + 1
    )
    assert TRADING_2564_S3B_RESTRICTED_CURRENT_AUTHORITY_PATHS == (
        TRADING_2564_S3B_SOURCE_PATHS
        & TRADING_2480_CAPABILITY_DISCOVERY_SUCCESSOR_CURRENT_AUTHORITY_PATHS
    )
    phase = merged[TRADING_2564_S3B_SECTION]
    _assert_s3b_source_closure(phase)
    assert phase["supersession"] == {
        "historical_hashes_rewritten": False,
        "inherited_supersession_authority": TRADING_2564_S3A_SECTION,
        "current_hash_authority": f"{TRADING_2564_S3B_SECTION}.sources",
    }
    assert phase["capture_contract"] == {
        "source_manifest_path": (
            "config/data_governance/named_prospective_five_candidate_sources_v1.json"
        ),
        "module_count": 87,
        "dependency_count": 14,
        "parent_canonical_dq_calls": 0,
        "child_canonical_dq_calls_per_capture": 1,
        "input_closure": "ALL_DQ_MEMBERS_PROVENANCE_AND_EXECUTION_DEPENDENCIES",
        "acknowledgement": "V2_ORIGINAL_ANCHOR_ALL_CHILD_RETURN_BOUNDS_AND_POSTGUARDS",
        "terminal_commit": "V2_SINGLE_PRECHECKED_COMMIT_ORIGINAL_LEASE_AND_CLOCK_PREFIX",
        "legacy_clock_evidence": "V1_RETAINED_ORIGINAL_INNER_SEMANTICS_NO_REINTERPRETATION",
        "retained_signal_verification": "COMPLETE_RECOMPUTATION_FROM_CLOSED_INPUTS",
        "expired_existing_key": "READ_ONLY_ORIGINAL_RESULT_OR_INCOMPLETE",
        "timing_version": "NEXT_XNYS_CLOSE_FORWARD_V1",
    }
    assert phase["safety"] == {
        "real_activation_adopted": False,
        "real_dq_or_research_executed": False,
        "synthetic_evidence_admitted_to_research": False,
        "provider_available_at_established": False,
        "pit_or_oos_established": False,
        "outcome_access_authorized": False,
        "returns_computed": False,
        "legacy_execution_profiles_changed": False,
        "legacy_observation_ledger_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


@pytest.mark.parametrize("mutation", ["missing", "missing_both", "extra_both", "wrong_hash"])
def test_s3b_source_identity_is_not_self_reported_lists(mutation: str) -> None:
    phase = deepcopy(load_compatibility_authority()[TRADING_2564_S3B_SECTION])
    if mutation in {"missing", "missing_both"}:
        removed = "src/ai_trading_system/prospective_capture_execution.py"
        phase["sources"] = [row for row in phase["sources"] if row["path"] != removed]
        if mutation == "missing_both":
            phase["superseded_live_source_paths"].remove(removed)
    elif mutation == "extra_both":
        phase["sources"].append({"path": "unknown.py", "sha256": "0" * 64})
        phase["superseded_live_source_paths"].append("unknown.py")
    else:
        phase["sources"][0]["sha256"] = "0" * 64
    with pytest.raises(AssertionError):
        _assert_s3b_source_closure(phase)
