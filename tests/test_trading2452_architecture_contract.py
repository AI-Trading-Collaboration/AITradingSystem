from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_PATH = Path("config/architecture/arch_004_refactor_policy.yaml")
ACTIVE_GLOSSARY_PATH = Path("config/architecture/research_semantic_glossary_v2.yaml")
COMPATIBILITY_BASELINE_PATH = Path("inputs/architecture/arch_004_compatibility_baseline.yaml")
WAVE11_PHASE_KEY = "phase_arch_004_g2_5_wave11"
WAVE11_CURRENT_HASH_AUTHORITY = f"{WAVE11_PHASE_KEY}.sources"


def _sha256_path(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _assert_historical_source_is_current_or_superseded(
    baseline: dict[str, Any],
    source: dict[str, str],
    *,
    repository_root: Path = Path("."),
) -> None:
    source_path = source["path"]
    live_path = repository_root / source_path
    assert live_path.is_file(), live_path

    live_hash = _sha256_path(live_path)
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
    assert (
        current_source.get("sha256") == live_hash
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
    baseline = safe_load_yaml_path(COMPATIBILITY_BASELINE_PATH)
    change = baseline["integrated_change_trading_2452"]

    assert change["status"] == "DONE"
    assert change["task_id"] == ("TRADING-2452_UNIFIED_2021_PRIMARY_WINDOW_AND_CLEAN_RUN")
    assert change["active_primary_start"] == "2021-02-22"
    assert change["historical_seen_result"] == "INCOMPLETE_NO_ELIGIBLE_CANDIDATE"
    assert change["production_effect"] == "none"
    for source in change["sources"]:
        _assert_historical_source_is_current_or_superseded(baseline, source)


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
