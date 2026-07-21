from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.data_foundation import (
    AI_CYCLE_COMPARISON_START,
    AI_REGIME_START,
    PRIMARY_RESEARCH_START,
)
from ai_trading_system.etf_portfolio.ai_attribution import (
    AI_ATTRIBUTION_MARKET_REGIME,
    AI_ATTRIBUTION_REGIME_START,
)
from ai_trading_system.etf_portfolio.dynamic_v3_system_target import (
    DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH,
    DEFAULT_PAPER_SHADOW_CONFIG_PATH,
)
from ai_trading_system.etf_portfolio.satellite_attribution import (
    SATELLITE_ATTRIBUTION_MARKET_REGIME,
    SATELLITE_ATTRIBUTION_REGIME_START,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY
from ai_trading_system.market_feedback_optimization import (
    DEFAULT_MARKET_FEEDBACK_REPLAY_START,
)
from ai_trading_system.platform.config.market_regimes import (
    load_market_regimes,
    market_regime_by_id,
)

PRIMARY_START = "2021-02-22"
LEGACY_COMPARISON_START = "2022-12-01"
ACTIVE_DEFAULT_FIELDS = {
    "default_backtest_start",
    "default_decision_start",
    "default_evaluation_start",
    "default_start",
    "default_start_date",
    "minimum_requested_start_date",
}
LEGACY_REGIME_ID = "ai_after_chatgpt"
LEGACY_POLICY_FIELD_ALLOWLIST = {
    (
        Path("config/etf_portfolio/dynamic_v3_rescue/weight_search_targeted_v1.yaml"),
        "minimum_requested_start_date",
    ),
}


def _walk_mappings(value: Any) -> list[Mapping[str, Any]]:
    mappings: list[Mapping[str, Any]] = []
    if isinstance(value, Mapping):
        mappings.append(value)
        for nested in value.values():
            mappings.extend(_walk_mappings(nested))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for nested in value:
            mappings.extend(_walk_mappings(nested))
    return mappings


def test_market_regime_keeps_active_primary_distinct_from_legacy_comparison() -> None:
    config = load_market_regimes()

    assert config.default_backtest_regime == "unified_primary_2021"
    primary = market_regime_by_id(config, config.default_backtest_regime)
    legacy = market_regime_by_id(config, LEGACY_REGIME_ID)
    assert primary.start_date == date(2021, 2, 22)
    assert primary.primary is True
    assert legacy.start_date == date(2022, 12, 1)
    assert legacy.primary is False


def test_compatibility_constant_does_not_supply_active_primary_default() -> None:
    assert PRIMARY_RESEARCH_START == PRIMARY_START
    assert AI_CYCLE_COMPARISON_START == LEGACY_COMPARISON_START
    assert AI_REGIME_START == AI_CYCLE_COMPARISON_START
    assert PRIMARY_RESEARCH_START != AI_REGIME_START


def test_all_active_yaml_default_fields_use_unified_primary_start() -> None:
    failures: list[str] = []
    for path in sorted(Path("config").rglob("*.yaml")):
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        for mapping in _walk_mappings(payload):
            active_fields = ACTIVE_DEFAULT_FIELDS.intersection(mapping)
            for field in active_fields:
                value = mapping[field]
                actual = value.isoformat() if isinstance(value, date) else str(value)
                if actual != PRIMARY_START and (path, field) not in (LEGACY_POLICY_FIELD_ALLOWLIST):
                    failures.append(f"{path}:{field}={actual}")
            if not active_fields:
                continue
            declared_ids = {
                str(mapping[key])
                for key in ("regime_id", "id", "market_regime", "default_regime_id")
                if key in mapping
            }
            if LEGACY_REGIME_ID in declared_ids and any(
                (path, field) not in LEGACY_POLICY_FIELD_ALLOWLIST for field in active_fields
            ):
                failures.append(f"{path}:active_default_mixed_with={LEGACY_REGIME_ID}")

    assert failures == []


def test_active_runtime_defaults_resolve_to_unified_primary_window() -> None:
    assert AI_REGIME_SUMMARY == {
        "market_regime": "unified_primary_2021",
        "research_window_id": "exact_three_asset_validated",
        "anchor_event": "validated QQQ/SGOV/TQQQ common history start",
        "anchor_date": PRIMARY_START,
        "default_backtest_start": PRIMARY_START,
    }
    assert DEFAULT_MARKET_FEEDBACK_REPLAY_START == date(2021, 2, 22)
    assert AI_ATTRIBUTION_MARKET_REGIME == "unified_primary_2021"
    assert AI_ATTRIBUTION_REGIME_START == date(2021, 2, 22)
    assert SATELLITE_ATTRIBUTION_MARKET_REGIME == "unified_primary_2021"
    assert SATELLITE_ATTRIBUTION_REGIME_START == date(2021, 2, 22)


def test_active_walk_forward_and_restart_policy_exclude_legacy_default() -> None:
    walk_forward = yaml.safe_load(
        Path("config/research/dynamic_walk_forward_policy.yaml").read_text(encoding="utf-8")
    )
    restart = yaml.safe_load(
        Path("config/research/strategy_research_restart_policy.yaml").read_text(encoding="utf-8")
    )

    assert walk_forward["policy_id"] == "dynamic_walk_forward_validation_policy_v2"
    assert str(walk_forward["validation_splits"][0]["start_date"]) == PRIMARY_START
    assert walk_forward["validation_splits"][-1]["split_id"] == ("prospective_untouched_holdout")
    assert walk_forward["validation_splits"][-1]["access_allowed"] is False
    project = restart["window_semantics"]["project_ai_cycle_conclusion"]
    assert project["regime_id"] == "unified_primary_2021"
    assert str(project["start"]) == PRIMARY_START
    assert restart["research_lane"]["source_window_id"] == "exact_three_asset_validated"


def test_active_policy_windows_and_protocols_use_unified_primary_start() -> None:
    backtest = yaml.safe_load(Path("config/etf_portfolio/backtest.yaml").read_text("utf-8"))
    refresh = yaml.safe_load(
        Path("config/etf_portfolio/dynamic_v3_rescue/smoothed_source_refresh_v1.yaml").read_text(
            "utf-8"
        )
    )
    controlled = yaml.safe_load(
        Path("config/research/controlled_strategy_next_stage_research.yaml").read_text("utf-8")
    )
    indicator = yaml.safe_load(
        Path("config/research/indicator_research_registry.yaml").read_text("utf-8")
    )
    requested_window_paths = (
        "config/research/first_layer_current_state_policy.yaml",
        "config/research/first_layer_objective_validation_policy.yaml",
        "config/research/first_layer_proxy_challenger_experiments_policy.yaml",
        "config/research/first_layer_proxy_coverage_audit_policy.yaml",
    )
    protocol_paths = (
        "config/research/protocols/dynamic_trend_thresholds.yaml",
        "config/research/protocols/valuation_crowding_masking.yaml",
        "config/research/protocols/portfolio_decision_problem_v1.yaml",
    )

    assert backtest["backtest"]["regime"] == "unified_primary_2021"
    assert str(backtest["backtest"]["start_date"]) == PRIMARY_START
    assert str(refresh["refresh"]["start"]) == PRIMARY_START
    segments = {
        row["segment_id"]: row for row in controlled["value_surface_expansion"]["regime_segments"]
    }
    assert str(segments["unified_primary_2021_full"]["start_date"]) == PRIMARY_START
    assert str(segments["ai_after_chatgpt_full"]["start_date"]) == LEGACY_COMPARISON_START
    assert indicator["market_regime"]["regime_id"] == "unified_primary_2021"
    assert indicator["market_regime"]["requested_date_range"] == f"{PRIMARY_START}..present"
    for path in requested_window_paths:
        payload = yaml.safe_load(Path(path).read_text("utf-8"))
        assert payload["market_regime"] == "unified_primary_2021", path
        assert str(payload["requested_window"]["start"]) == PRIMARY_START, path
    for path in protocol_paths:
        payload = yaml.safe_load(Path(path).read_text("utf-8"))
        assert payload["market_regime"]["regime_id"] == "unified_primary_2021", path
        assert payload["market_regime"]["requested_date_range"] == (
            f"{PRIMARY_START}..present"
        ), path
    targeted = yaml.safe_load(
        Path("config/etf_portfolio/dynamic_v3_rescue/weight_search_targeted_v2.yaml").read_text(
            "utf-8"
        )
    )
    paper_shadow = yaml.safe_load(DEFAULT_PAPER_SHADOW_CONFIG_PATH.read_text("utf-8"))
    paper_backfill = yaml.safe_load(DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH.read_text("utf-8"))
    assert targeted["schema_version"] == "dynamic_v3_weight_search_targeted_policy.v2"
    assert targeted["backfill"]["market_regime"] == "unified_primary_2021"
    assert str(targeted["backfill"]["minimum_requested_start_date"]) == PRIMARY_START
    assert DEFAULT_PAPER_SHADOW_CONFIG_PATH.name == "paper_shadow_account_v2.yaml"
    assert DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH.name == "paper_shadow_backfill_v2.yaml"
    assert str(paper_shadow["paper_shadow_account"]["start_date"]) == PRIMARY_START
    assert str(paper_backfill["date_range"]["start"]) == PRIMARY_START
    assert paper_backfill["source"]["paper_shadow_config"].endswith("paper_shadow_account_v2.yaml")
