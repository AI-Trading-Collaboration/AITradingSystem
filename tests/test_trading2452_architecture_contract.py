from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.platform.architecture.compatibility_authority import (
    load_compatibility_authority,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_PATH = Path("config/architecture/arch_004_refactor_policy.yaml")
ACTIVE_GLOSSARY_PATH = Path("config/architecture/research_semantic_glossary_v2.yaml")
COMPATIBILITY_BASELINE_PATH = Path("inputs/architecture/arch_004_compatibility_baseline.yaml")
WAVE11_PHASE_KEY = "phase_arch_004_g2_5_wave11"
WAVE11_CURRENT_HASH_AUTHORITY = f"{WAVE11_PHASE_KEY}.sources"
TRADING_2480_CAPABILITY_DISCOVERY_EVIDENCE_PHASE_KEY = (
    "phase_trading_2480_qc_qqq_options_capability_discovery_evidence_v1"
)
TRADING_2480_CAPABILITY_DISCOVERY_REVIEW_PHASE_KEY = (
    "phase_trading_2480_qc_qqq_options_capability_discovery_review_v1"
)
TRADING_2492_BOUNDED_PILOT_OWNER_REVIEW_PROPOSAL_PHASE_KEY = (
    "phase_trading_2492_qc_qqq_options_bounded_pilot_owner_review_proposal_v1"
)
TRADING_2492_BOUNDED_PILOT_TERMINAL_NO_GO_PHASE_KEY = (
    "phase_trading_2492_qc_qqq_options_bounded_pilot_terminal_no_go_v1"
)
TRADING_2493_OWNER_STAGE_GATE_SIGNOFF_PHASE_KEY = (
    "phase_trading_2493_qc_qqq_options_owner_stage_gate_signoff_v1"
)
TRADING_2497_LICENSE_EXPORT_DUE_DILIGENCE_PHASE_KEY = (
    "phase_trading_2497_qc_qqq_options_license_export_due_diligence_v1"
)
TRADING_2497_LICENSE_EXPORT_OWNER_REVIEW_PHASE_KEY = (
    "phase_trading_2497_qc_qqq_options_license_export_owner_review_proposal_v1"
)
TRADING_2498_DAILY_CAPABILITY_GATE_PHASE_KEY = (
    "phase_trading_2498_qc_qqq_options_daily_capability_gate_v1"
)
TRADING_2496_OWNER_VISUAL_ACCEPTANCE_PHASE_KEY = (
    "phase_trading_2496_atlas_reader_status_explanation_owner_visual_acceptance_v1"
)
TRADING_2500_DAILY_CAPABILITY_GATE_RETRY_PHASE_KEY = (
    "phase_trading_2500_qc_qqq_options_daily_capability_gate_retry_v1"
)
TRADING_2500_DAILY_CAPABILITY_GATE_RETRY_EVIDENCE_REVIEW_PHASE_KEY = (
    "phase_trading_2500_qc_qqq_options_daily_capability_gate_retry_evidence_review_v1"
)
TRADING_2500_DAILY_CAPABILITY_GATE_RETRY_TERMINAL_REVIEW_PHASE_KEY = (
    "phase_trading_2500_qc_qqq_options_daily_capability_gate_retry_terminal_review_v1"
)
TRADING_2499_DAILY_PRIMARY_BACKTEST_CONTRACT_PHASE_KEY = (
    "phase_trading_2499_qqq_options_daily_primary_backtest_contract_v1"
)
TRADING_2501_ATLAS_OWNER_REVIEW_PACK_PHASE_KEY = (
    "phase_trading_2501_atlas_qqq_options_projection_owner_review_pack_v1"
)
TRADING_2503_ATLAS_PROJECTION_RENDERER_PHASE_KEY = (
    "phase_trading_2503_atlas_qqq_options_projection_renderer_v1"
)
TRADING_2502_OWNER_DECISION_PACK_PHASE_KEY = (
    "phase_trading_2502_qqq_options_owner_reviewed_backtest_policy_decision_pack_v1"
)
TRADING_2504_OWNER_DECISION_MANIFEST_PHASE_KEY = (
    "phase_trading_2504_qqq_options_owner_decision_manifest_v1"
)
DEVX_006C_COMPATIBILITY_AUTHORITY_PHASE_KEY = (
    "phase_devx_006c_compatibility_authority_fragmentation"
)
DEVX_006D_REPORT_CATALOG_FLOW_AUTHORITY_PHASE_KEY = (
    "phase_devx_006d_report_catalog_flow_lossless_fragmentation"
)
ARCH_005_S5_CANONICAL_TASK_SOURCE_PHASE_KEY = "phase_arch_005_s5_canonical_task_source_cutover"
DEVX_007_EXPLICIT_SUBMISSION_PHASE_KEY = (
    "phase_devx_007_web_pro_git_review_skill_explicit_submission_v2"
)
TRADING_2542C_REMEDIATION_PHASE_KEY = (
    "phase_trading_2542c_growth_action_value_independent_review_remediation_and_freeze_readiness_v1"
)
DEVX_009_PUBLICATION_FENCE_PHASE_KEY = (
    "phase_devx_009_parallel_integration_publication_fence_and_generated_state_rebuild_v1"
)
TRADING_2542D_DQ_PIT_SAMPLE_SEMANTICS_PHASE_KEY = (
    "phase_trading_2542d_growth_action_value_dq_pit_and_sample_semantics_freeze_correction_v1"
)
PROD_004_PIT_CUMULATIVE_CONSUMPTION_PHASE_KEY = (
    "phase_prod_004_pit_cumulative_archive_consumption_v1"
)
DEVX_011_WORKFLOW_HEALTH_PHASE_KEY = "phase_devx_011_governed_workflow_health_control_loop_v1"
DEVX_012_WORKFLOW_HEALTH_AUTOMATIC_CYCLE_PHASE_KEY = (
    "phase_devx_012_automatic_workflow_health_trigger_and_outcome_review_v1"
)
RISK_012_UNKNOWN_RISK_EVENT_ID_FAIL_CLOSED_PHASE_KEY = (
    "phase_risk_012_unknown_risk_event_id_fail_closed_v1"
)
OPS_077_ATOMIC_RELEASE_SCHEDULER_BINDING_PHASE_KEY = (
    "phase_ops_077_atomic_release_scheduler_binding_and_canary_v1"
)
OPS_078_DAILY_AUTOMATION_ISOLATION_PHASE_KEY = (
    "phase_ops_078_daily_automation_isolation_and_same_day_rescue_v1"
)
OPS_079_HISTORICAL_GAP_RECOVERY_PHASE_KEY = (
    "phase_ops_079_historical_daily_gap_recovery_executor_v1"
)
TRADING_2480_CAPABILITY_DISCOVERY_SUCCESSOR_CURRENT_AUTHORITY_PATHS = frozenset(
    {
        ("docs/requirements/TRADING-2492_QC_QQQ_Options_Bounded_Free_Cloud_Pilot_V1.md"),
        "docs/system_flow.md",
        "docs/task_register.md",
        "inputs/architecture/arch_004e_aggregate_shadow_index.yaml",
        "inputs/architecture/arch_004e_architecture_fitness.yaml",
        "inputs/architecture/arch_004e_module_manifest.yaml",
        "inputs/architecture/arch_004e_test_manifest.yaml",
        "inputs/architecture/arch_004g_deprecation_inventory.yaml",
        "inputs/architecture/arch_005_task_registry_baseline.yaml",
        "inputs/architecture/arch_005_task_shadow_index.yaml",
        "inputs/architecture/arch_005_task_shadow_v2_index.yaml",
        (
            "registry/development_tasks_shadow/active/10/"
            "10dbf6411f9224d8bb7715ca376f641792a9e86f7559657e0c1c9dc574f930ef.yaml"
        ),
        (
            "registry/development_tasks_shadow_v2/10/"
            "10dbf6411f9224d8bb7715ca376f641792a9e86f7559657e0c1c9dc574f930ef.yaml"
        ),
        (
            "registry/development_tasks_shadow_v2/a4/"
            "a483ee8d81b729624a31da998ea1e890cd4e2b302f23231664d39d1bc0907e8b.yaml"
        ),
        "tests/test_arch_004_refactor_policy.py",
        "tests/test_arch_004g_deprecation.py",
        "tests/test_trading2452_architecture_contract.py",
    }
)


def _sha256_path(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _source_sha256_path(path: Path, source: dict[str, Any]) -> str:
    payload = path.read_bytes()
    normalization = source.get("hash_normalization")
    if normalization == "git_eol_lf":
        payload = payload.replace(b"\r\n", b"\n")
    elif normalization is not None:
        raise AssertionError(f"unsupported hash normalization: {normalization}")
    return sha256(payload).hexdigest()


def _assert_historical_source_is_current_or_superseded(
    baseline: dict[str, Any],
    source: dict[str, str],
    *,
    repository_root: Path = Path("."),
) -> None:
    source_path = source["path"]
    live_path = repository_root / source_path
    assert live_path.is_file(), live_path

    live_hash = _source_sha256_path(live_path, source)
    if source["sha256"] == live_hash:
        return

    latest_authority: tuple[str, dict[str, Any], dict[str, str]] | None = None
    for section_key, section in reversed(tuple(baseline.items())):
        if not isinstance(section, dict):
            continue
        superseded_paths = section.get("superseded_live_source_paths")
        section_sources = section.get("sources")
        if not isinstance(superseded_paths, list) or source_path not in superseded_paths:
            continue
        if not isinstance(section_sources, list):
            continue
        matching_sources = [
            item
            for item in section_sources
            if isinstance(item, dict) and item.get("path") == source_path
        ]
        if matching_sources:
            latest_authority = (section_key, section, matching_sources[-1])
            break

    assert latest_authority is not None, (
        f"{source_path}: historical hash drift is not declared in an append-only "
        "supersession section that also contains its current source hash"
    )
    section_key, section, current_source = latest_authority
    supersession = section["supersession"]
    assert (
        supersession["historical_hashes_rewritten"] is False
    ), f"{section_key} must preserve historical source hashes"
    expected_authority = f"{section_key}.sources"
    assert (
        supersession["current_hash_authority"] == expected_authority
    ), f"{section_key} current hash authority must be {expected_authority}"
    current_live_hash = _source_sha256_path(live_path, current_source)
    if (
        source_path in TRADING_2480_CAPABILITY_DISCOVERY_SUCCESSOR_CURRENT_AUTHORITY_PATHS
        and section_key
        not in {
            TRADING_2501_ATLAS_OWNER_REVIEW_PACK_PHASE_KEY,
            TRADING_2503_ATLAS_PROJECTION_RENDERER_PHASE_KEY,
            TRADING_2502_OWNER_DECISION_PACK_PHASE_KEY,
            TRADING_2504_OWNER_DECISION_MANIFEST_PHASE_KEY,
            DEVX_006C_COMPATIBILITY_AUTHORITY_PHASE_KEY,
            DEVX_006D_REPORT_CATALOG_FLOW_AUTHORITY_PHASE_KEY,
            ARCH_005_S5_CANONICAL_TASK_SOURCE_PHASE_KEY,
            DEVX_007_EXPLICIT_SUBMISSION_PHASE_KEY,
            TRADING_2542C_REMEDIATION_PHASE_KEY,
            DEVX_009_PUBLICATION_FENCE_PHASE_KEY,
            TRADING_2542D_DQ_PIT_SAMPLE_SEMANTICS_PHASE_KEY,
            PROD_004_PIT_CUMULATIVE_CONSUMPTION_PHASE_KEY,
            DEVX_011_WORKFLOW_HEALTH_PHASE_KEY,
            DEVX_012_WORKFLOW_HEALTH_AUTOMATIC_CYCLE_PHASE_KEY,
            RISK_012_UNKNOWN_RISK_EVENT_ID_FAIL_CLOSED_PHASE_KEY,
            OPS_077_ATOMIC_RELEASE_SCHEDULER_BINDING_PHASE_KEY,
            OPS_078_DAILY_AUTOMATION_ISOLATION_PHASE_KEY,
            OPS_079_HISTORICAL_GAP_RECOVERY_PHASE_KEY,
        }
    ):
        section_ids = list(baseline)
        assert TRADING_2492_BOUNDED_PILOT_OWNER_REVIEW_PROPOSAL_PHASE_KEY in section_ids
        assert TRADING_2492_BOUNDED_PILOT_TERMINAL_NO_GO_PHASE_KEY in section_ids
        assert TRADING_2493_OWNER_STAGE_GATE_SIGNOFF_PHASE_KEY in section_ids
        assert TRADING_2497_LICENSE_EXPORT_DUE_DILIGENCE_PHASE_KEY in section_ids
        assert TRADING_2497_LICENSE_EXPORT_OWNER_REVIEW_PHASE_KEY in section_ids
        assert TRADING_2498_DAILY_CAPABILITY_GATE_PHASE_KEY in section_ids
        assert TRADING_2496_OWNER_VISUAL_ACCEPTANCE_PHASE_KEY in section_ids
        assert TRADING_2500_DAILY_CAPABILITY_GATE_RETRY_PHASE_KEY in section_ids
        assert TRADING_2500_DAILY_CAPABILITY_GATE_RETRY_EVIDENCE_REVIEW_PHASE_KEY in section_ids
        assert TRADING_2500_DAILY_CAPABILITY_GATE_RETRY_TERMINAL_REVIEW_PHASE_KEY in section_ids
        assert TRADING_2499_DAILY_PRIMARY_BACKTEST_CONTRACT_PHASE_KEY in section_ids
        assert TRADING_2501_ATLAS_OWNER_REVIEW_PACK_PHASE_KEY in section_ids
        assert section_ids.index(section_key) <= section_ids.index(
            TRADING_2501_ATLAS_OWNER_REVIEW_PACK_PHASE_KEY
        )
        return
    assert (
        current_source.get("sha256") == current_live_hash
    ), f"{source_path}: latest authority hash does not match live bytes"


def test_trading2452_active_glossary_supersedes_frozen_v1_without_rewriting_it() -> None:
    policy = safe_load_yaml_path(POLICY_PATH)
    glossary = safe_load_yaml_path(ACTIVE_GLOSSARY_PATH)
    terms = glossary["canonical_terms"]

    assert policy["program"]["current_baseline"]["semantic_glossary_path"] == str(
        ACTIVE_GLOSSARY_PATH
    ).replace("\\", "/")
    assert glossary["schema_version"] == "arch_004_research_semantic_glossary.v2"
    assert glossary["supersedes"] == "config/architecture/research_semantic_glossary.yaml"
    assert terms["market_regime"]["canonical_value"] == "unified_primary_2021"
    assert str(terms["market_regime_start"]["canonical_value"]) == "2021-02-22"
    assert terms["primary_research_window_id"]["canonical_value"] == ("exact_three_asset_validated")
    assert str(terms["primary_research_window_start"]["canonical_value"]) == "2021-02-22"
    legacy = terms["legacy_comparison_window_id"]
    assert legacy["market_regime_id"] == "ai_after_chatgpt"
    assert str(legacy["start"]) == "2022-12-01"
    assert legacy["active_default_allowed"] is False
    assert glossary["resolution_rules"]["conflict_behavior"] == "FAIL_CLOSED"
    assert glossary["implementation_boundary"]["production_effect"] == "none"


def test_trading2452_compatibility_sources_are_current_and_auditable() -> None:
    baseline = load_compatibility_authority()
    change = baseline["integrated_change_trading_2452"]

    assert change["status"] == "DONE"
    assert change["task_id"] == ("TRADING-2452_UNIFIED_2021_PRIMARY_WINDOW_AND_CLEAN_RUN")
    assert change["active_primary_start"] == "2021-02-22"
    assert change["historical_seen_result"] == "INCOMPLETE_NO_ELIGIBLE_CANDIDATE"
    assert change["production_effect"] == "none"
    for source in change["sources"]:
        _assert_historical_source_is_current_or_superseded(baseline, source)


def test_trading2453_w8e1_compatibility_sources_are_current_and_auditable() -> None:
    baseline = load_compatibility_authority()
    change = baseline["integrated_change_trading_2453_w8e1"]

    assert change["status"] == "BLOCKED_OWNER_INPUT"
    assert change["task_id"] == ("TRADING-2453_CONSTRAINT_HIT_REJECTION_DIAGNOSIS_AND_OWNER_REVIEW")
    assert change["diagnosis_status"] == "PASS"
    assert change["diagnosis_failed_check_count"] == 0
    assert change["default_decision"] == "KILL_PAUSE"
    assert change["recommended_option_id"] == ("A_KEEP_KILL_AND_CLOSE_CURRENT_PACKAGE")
    assert change["w8e1_status"] == "CLOSED_REVERTED_EXIT_GATE_NOT_MET"
    assert change["w8e1_code_retained"] is False
    assert change["production_effect"] == "none"
    for source in change["sources"]:
        _assert_historical_source_is_current_or_superseded(baseline, source)


def test_historical_source_drift_requires_explicit_wave11_supersession(
    tmp_path: Path,
) -> None:
    live_path = tmp_path / "unsuperseded_source.txt"
    live_path.write_bytes(b"current")
    source_path = live_path.as_posix()
    baseline = {
        WAVE11_PHASE_KEY: {
            "supersession": {
                "historical_hashes_rewritten": False,
                "current_hash_authority": WAVE11_CURRENT_HASH_AUTHORITY,
            },
            "superseded_live_source_paths": [],
            "sources": [{"path": source_path, "sha256": _sha256_path(live_path)}],
        }
    }
    historical_source = {
        "path": source_path,
        "sha256": sha256(b"historical").hexdigest(),
    }

    with pytest.raises(AssertionError, match="drift is not declared"):
        _assert_historical_source_is_current_or_superseded(baseline, historical_source)


def test_historical_source_drift_accepts_a_later_append_only_authority(
    tmp_path: Path,
) -> None:
    live_path = tmp_path / "future_authority_source.txt"
    live_path.write_bytes(b"current")
    source_path = live_path.as_posix()
    future_phase_key = "phase_future_append_only"
    baseline = {
        WAVE11_PHASE_KEY: {
            "supersession": {
                "historical_hashes_rewritten": False,
                "current_hash_authority": WAVE11_CURRENT_HASH_AUTHORITY,
            },
            "superseded_live_source_paths": [source_path],
            "sources": [{"path": source_path, "sha256": sha256(b"wave11").hexdigest()}],
        },
        future_phase_key: {
            "supersession": {
                "historical_hashes_rewritten": False,
                "current_hash_authority": f"{future_phase_key}.sources",
            },
            "superseded_live_source_paths": [source_path],
            "sources": [{"path": source_path, "sha256": _sha256_path(live_path)}],
        },
    }
    historical_source = {
        "path": source_path,
        "sha256": sha256(b"historical").hexdigest(),
    }

    _assert_historical_source_is_current_or_superseded(baseline, historical_source)


def test_historical_source_drift_rejects_stale_wave11_authority(tmp_path: Path) -> None:
    live_path = tmp_path / "stale_authority_source.txt"
    live_path.write_bytes(b"current")
    source_path = live_path.as_posix()
    baseline = {
        WAVE11_PHASE_KEY: {
            "supersession": {
                "historical_hashes_rewritten": False,
                "current_hash_authority": WAVE11_CURRENT_HASH_AUTHORITY,
            },
            "superseded_live_source_paths": [source_path],
            "sources": [{"path": source_path, "sha256": sha256(b"stale").hexdigest()}],
        }
    }
    historical_source = {
        "path": source_path,
        "sha256": sha256(b"historical").hexdigest(),
    }

    with pytest.raises(AssertionError, match="authority hash does not match live bytes"):
        _assert_historical_source_is_current_or_superseded(baseline, historical_source)
