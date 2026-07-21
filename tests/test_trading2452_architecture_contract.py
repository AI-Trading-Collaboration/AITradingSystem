from __future__ import annotations

from hashlib import sha256
from pathlib import Path

from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_PATH = Path("config/architecture/arch_004_refactor_policy.yaml")
ACTIVE_GLOSSARY_PATH = Path("config/architecture/research_semantic_glossary_v2.yaml")
COMPATIBILITY_BASELINE_PATH = Path("inputs/architecture/arch_004_compatibility_baseline.yaml")


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
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    change = baseline["integrated_change_trading_2452"]

    assert change["status"] == "DONE"
    assert change["task_id"] == ("TRADING-2452_UNIFIED_2021_PRIMARY_WINDOW_AND_CLEAN_RUN")
    assert change["active_primary_start"] == "2021-02-22"
    assert change["historical_seen_result"] == "INCOMPLETE_NO_ELIGIBLE_CANDIDATE"
    assert change["production_effect"] == "none"
    for source in change["sources"]:
        path = Path(source["path"])
        assert path.is_file(), path
        assert sha256(path.read_bytes()).hexdigest() == source["sha256"], path


def test_trading2453_w8e1_compatibility_sources_are_current_and_auditable() -> None:
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
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
        path = Path(source["path"])
        assert path.is_file(), path
        assert sha256(path.read_bytes()).hexdigest() == source["sha256"], path
