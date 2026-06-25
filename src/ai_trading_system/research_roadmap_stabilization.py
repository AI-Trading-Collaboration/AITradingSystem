from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso, write_foundation_artifact_pair
from ai_trading_system.layer1_simple_rule_meta_policy import (
    DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    run_layer1_selector_low_turnover_owner_decision_pack,
    run_layer1_selector_low_turnover_ranking,
    run_layer1_selector_result_review_master,
    run_layer1_selector_turnover_source_diagnosis,
    run_layer1_selector_vs_simple_components_final_gate,
)
from ai_trading_system.layer2_strategy_component_readiness import (
    DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
    DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
)
from ai_trading_system.simple_baseline_forward_aging import (
    DEFAULT_FORWARD_AGING_OBSERVATION_ROOT,
    PRIMARY_CANDIDATE_ID,
    run_simple_baseline_forward_aging_scoreboard,
    run_simple_baseline_forward_aging_update_maturity,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    _data_quality_gate,
    _load_registry,
    _mapping,
    _read_json_or_empty,
    _records,
)
from ai_trading_system.trading_calendar import us_equity_market_session
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_LAYER1_SELECTOR_DRY_RUN_ARCHIVE_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "layer1_selector_dry_run_archive_report.md"
)
DEFAULT_RESEARCH_ROADMAP_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_strategies"
DEFAULT_RESEARCH_ROADMAP_MASTER_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "research_roadmap_master_review.md"
)

SAFETY_BOUNDARY: dict[str, Any] = {
    "production_effect": "none",
    "broker_action": "none",
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "manual_review_required": True,
    "research_only": True,
    "observe_only": True,
}

AI_REGIME_SUMMARY = {
    "market_regime": "ai_after_chatgpt",
    "anchor_event": "ChatGPT public launch",
    "anchor_date": "2022-11-30",
    "default_backtest_start": "2022-12-01",
}


def run_layer1_selector_dry_run_archive_report(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Path = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
    archive_doc_path: Path = DEFAULT_LAYER1_SELECTOR_DRY_RUN_ARCHIVE_DOC_PATH,
    source_doc_root: Path | None = None,
) -> dict[str, Any]:
    docs_root = source_doc_root or output_root / "layer1_selector_archive_source_docs"
    result = run_layer1_selector_result_review_master(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
        owner_doc_path=docs_root / "layer1_selector_owner_watchlist_review.md",
        master_doc_path=docs_root / "layer1_selector_result_review_master.md",
    )
    low_turnover_owner = run_layer1_selector_low_turnover_owner_decision_pack(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
        owner_doc_path=docs_root / "layer1_selector_low_turnover_owner_decision_pack.md",
    )
    turnover = run_layer1_selector_turnover_source_diagnosis(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    ranking = run_layer1_selector_low_turnover_ranking(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )
    final_gate = run_layer1_selector_vs_simple_components_final_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        registry_config_path=registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
        layer2_output_root=layer2_output_root,
    )

    final_summary = _mapping(final_gate.get("summary"))
    result_summary = _mapping(result.get("summary"))
    low_summary = _mapping(low_turnover_owner.get("summary"))
    turnover_summary = _mapping(turnover.get("summary"))
    ranking_summary = _mapping(ranking.get("summary"))
    blocked = _source_blocked([result, low_turnover_owner, turnover, ranking, final_gate])
    final_gate_passed = final_gate.get("status") == "SELECTOR_FINAL_GATE_PASS"
    low_turnover_solved = bool(final_summary.get("forward_aging_gate_allowed"))
    strongest_selector = (
        final_summary.get("best_low_turnover_selector")
        or ranking_summary.get("recommended_low_turnover_candidate")
        or result_summary.get("top_selector_id")
    )
    answers = {
        "1_current_strongest_selector": strongest_selector,
        "2_why_not_forward_aging_watchlist": (
            "strict_switch_count_contract_failed_and_selector_does_not_beat_100_qqq"
            if not final_gate_passed
            else "final_gate_passed_but_owner_review_still_required"
        ),
        "3_too_much_turnover_core_blocker": (
            turnover.get("status") == "TURNOVER_NOISE_DOMINANT"
            or not bool(final_summary.get("forward_aging_gate_allowed"))
        ),
        "4_low_turnover_refinement_solved_problem": low_turnover_solved,
        "5_selector_beats_always_equal_risk": bool(
            final_summary.get("selector_beats_equal_risk")
        ),
        "6_selector_beats_always_100_qqq": bool(final_summary.get("selector_beats_100_qqq")),
        "7_cost_after_advantage_exists": bool(
            final_summary.get("selector_beats_equal_risk")
            or final_summary.get("selector_beats_100_qqq")
        )
        and not bool(final_summary.get("selector_only_beats_equal_risk")),
        "8_continue_ban_ml_selector": True,
        "9_current_recommendation": "archive_dry_run_only_pause_layer1_selector",
    }
    if blocked:
        status = "LAYER1_SELECTOR_ARCHIVE_BLOCKED"
    elif final_gate_passed:
        status = "LAYER1_SELECTOR_ARCHIVE_NEEDS_REVIEW"
    else:
        status = "LAYER1_SELECTOR_ARCHIVED_DRY_RUN_ONLY"
    payload = _payload(
        report_type="layer1_selector_dry_run_archive_report",
        title="Layer-1 Selector Dry-Run Archive Report",
        status=status,
        summary={
            "archive_decision": status,
            "strongest_selector": strongest_selector,
            "top_selector_from_result_review": result_summary.get("top_selector_id"),
            "low_turnover_candidate": low_summary.get("recommended_low_turnover_candidate"),
            "turnover_status": turnover.get("status"),
            "noise_switch_count": turnover_summary.get("noise_switch_count"),
            "near_200dma_switch_share": turnover_summary.get("near_200dma_switch_share"),
            "selector_beats_equal_risk": answers["5_selector_beats_always_equal_risk"],
            "selector_beats_100_qqq": answers["6_selector_beats_always_100_qqq"],
            "low_turnover_solved": low_turnover_solved,
            "data_quality_status": _first_summary_value(
                [result, low_turnover_owner, turnover, ranking, final_gate],
                "data_quality_status",
            ),
            "actual_requested_date_range": _first_summary_value(
                [result, low_turnover_owner, turnover, ranking, final_gate],
                "actual_requested_date_range",
            ),
            **_safety_summary(),
        },
        required_answers=answers,
        source_artifacts=_artifact_paths_by_report(
            {
                "layer1_selector_result_review_master": result,
                "layer1_selector_low_turnover_owner_decision_pack": low_turnover_owner,
                "layer1_selector_turnover_source_diagnosis": turnover,
                "layer1_selector_low_turnover_ranking": ranking,
                "layer1_selector_vs_simple_components_final_gate": final_gate,
            }
        ),
        source_statuses={
            "layer1_selector_result_review_master": result.get("status"),
            "layer1_selector_low_turnover_owner_decision_pack": low_turnover_owner.get(
                "status"
            ),
            "layer1_selector_turnover_source_diagnosis": turnover.get("status"),
            "layer1_selector_low_turnover_ranking": ranking.get("status"),
            "layer1_selector_vs_simple_components_final_gate": final_gate.get("status"),
        },
        blockers=blocked,
        owner_next_action="review_and_accept_layer1_selector_archive_dry_run_only",
        report_registry_entry=_report_registry_entry(
            "layer1_selector_dry_run_archive_report",
            "Layer-1 Selector Dry-Run Archive Report",
            "aits research strategies layer1-selector-dry-run-archive-report",
            [
                "outputs/research_strategies/layer1_meta_policy/"
                "layer1_selector_dry_run_archive_report.json",
                "outputs/research_strategies/layer1_meta_policy/"
                "layer1_selector_dry_run_archive_report.md",
                "docs/research/layer1_selector_dry_run_archive_report.md",
            ],
        ),
    )
    _write_pair(payload, output_root, "layer1_selector_dry_run_archive_report")
    _copy_markdown_artifact(payload, archive_doc_path)
    return payload


def run_layer1_selector_restart_condition_contract(
    *,
    output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    simple_output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    layer2_config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
) -> dict[str, Any]:
    layer2_config = _load_mapping(layer2_config_path)
    simple_config = _load_registry(simple_registry_config_path)
    forward_policy = _mapping(_mapping(simple_config.get("research_policy")).get("forward_aging"))
    minimum_120d_matured = _int(
        forward_policy.get("minimum_120d_matured_observations_for_paper_shadow_review")
    )
    scoreboard = _read_json_or_empty(
        simple_output_root / "simple_baseline_forward_aging_scoreboard.json"
    )
    primary_scoreboard = _scoreboard_primary_row(scoreboard)
    matured_120d_count = _int(primary_scoreboard.get("matured_120d_count"))
    mature_observation_floor_satisfied = (
        minimum_120d_matured > 0 and matured_120d_count >= minimum_120d_matured
    )
    restart_conditions = [
        _condition(
            "layer2_material_growth_component_added",
            False,
            "需要新的 Layer-2 material growth component 且通过 owner review。",
        ),
        _condition(
            "qqq_plus_growth_cost_after_edge_vs_100_qqq",
            False,
            "QQQ-plus growth 仍是 research_only_inactive_reference。",
        ),
        _condition(
            "equal_risk_forward_aging_mature_observations",
            mature_observation_floor_satisfied,
            (
                "需要满足 simple baseline forward-aging policy 中的 "
                f"120d mature observation floor：{minimum_120d_matured}。"
            ),
        ),
        _condition(
            "layer1_history_backfilled_beyond_recent_ai_regime",
            False,
            "当前 Layer-1 结论仍主要覆盖 AI-after-ChatGPT recent regime。",
        ),
        _condition(
            "new_selector_beats_100_qqq_under_strict_turnover_contract",
            False,
            "1015～1023 final gate 未证明 selector 在 strict turnover contract 下优于 100 QQQ。",
        ),
        _condition(
            "owner_manual_approval_to_restart",
            False,
            "当前没有 owner 手动批准重启 Layer-1 selector。",
        ),
    ]
    any_restart_condition_satisfied = any(
        row["currently_satisfied"] for row in restart_conditions
    )
    restart_allowed_now = False
    blockers = [
        "layer1_selector_archived_dry_run_only",
        "strict_turnover_contract_not_satisfied",
        "no_component_ready_growth_candidate",
        "owner_manual_approval_missing",
    ]
    status = "RESTART_NOT_ALLOWED_NOW" if not restart_allowed_now else "RESTART_CONTRACT_READY"
    payload = _payload(
        report_type="layer1_selector_restart_condition_contract",
        title="Layer-1 Selector Restart Condition Contract",
        status=status,
        summary={
            "restart_allowed_now": restart_allowed_now,
            "any_restart_condition_satisfied": any_restart_condition_satisfied,
            "restart_blocker_count": len(blockers),
            "owner_approval_required": True,
            "layer2_component_pool_version": layer2_config.get("component_pool_version"),
            "matured_120d_count": matured_120d_count,
            "minimum_120d_matured_required": minimum_120d_matured,
            **_safety_summary(),
        },
        restart_allowed_now=restart_allowed_now,
        restart_blockers=blockers,
        restart_conditions=restart_conditions,
        minimum_required_evidence=[
            "material Layer-2 growth component with definition_hash locked",
            "cost-after edge versus 100_qqq under strict turnover contract",
            "data-quality PASS/PASS_WITH_WARNINGS visible in reports",
            "history coverage or mature forward-aging observations sufficient for review",
            "mature observation floor comes from simple_baseline_forward_aging_contract_v1",
            "owner manual approval before any restart",
        ],
        prohibited_restart_paths=[
            "lower_switch_count_controlled_standard",
            "direct_ml_selector_enablement",
            "auto_add_qqq_plus_growth_to_selectable",
            "forward_aging_before_turnover_problem_resolved",
        ],
        owner_approval_required=True,
        source_artifacts={
            "layer2_component_pool_config": str(layer2_config_path),
            "simple_baseline_registry_config": str(simple_registry_config_path),
            "scoreboard": str(simple_output_root / "simple_baseline_forward_aging_scoreboard.json"),
        },
        report_registry_entry=_report_registry_entry(
            "layer1_selector_restart_condition_contract",
            "Layer-1 Selector Restart Condition Contract",
            "aits research strategies layer1-selector-restart-condition-contract",
            [
                "outputs/research_strategies/layer1_meta_policy/"
                "layer1_selector_restart_condition_contract.json",
                "outputs/research_strategies/layer1_meta_policy/"
                "layer1_selector_restart_condition_contract.md",
            ],
        ),
    )
    _write_pair(payload, output_root, "layer1_selector_restart_condition_contract")
    return payload


def run_equal_risk_forward_aging_daily_run_health_check(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    observations = _observation_payload_records(output_root)
    observed_dates = [item["decision_date"] for item in observations if item["decision_date"]]
    latest_date = max(observed_dates) if observed_dates else None
    duplicate_count = _duplicate_observation_count(observations)
    price_dates = _qqq_price_dates(prices_path)
    resolved_as_of = _safe_date(data_gate.get("as_of")) or as_of_date
    missing_dates = _missing_observation_dates(observed_dates, price_dates, resolved_as_of)
    non_trading_observation_dates = [
        item.isoformat()
        for item in sorted(set(observed_dates))
        if not us_equity_market_session(item).is_trading_day
    ]
    unsafe_rows = _unsafe_observation_rows(observations)
    warnings: list[str] = []
    blockers: list[str] = []
    if not observations:
        warnings.append("no_forward_aging_observation_files")
    if missing_dates:
        warnings.append("missing_trading_day_observations")
    if duplicate_count:
        blockers.append("duplicate_observation_keys")
    if unsafe_rows:
        blockers.append("unsafe_observation_safety_fields")
    if not bool(data_gate.get("passed")):
        blockers.append("validate_data_cache_failed")
    if non_trading_observation_dates:
        blockers.append("non_trading_day_observation_present")
    if blockers:
        status = "EQUAL_RISK_FORWARD_AGING_BLOCKED"
    elif warnings or _int(data_gate.get("warning_count")):
        status = "EQUAL_RISK_FORWARD_AGING_WARN"
    else:
        status = "EQUAL_RISK_FORWARD_AGING_HEALTHY"
    payload = _payload(
        report_type="equal_risk_forward_aging_daily_run_health_check",
        title="Equal-Risk Forward-Aging Daily Run Health Check",
        status=status,
        summary={
            "latest_observation_date": latest_date.isoformat() if latest_date else None,
            "observation_count": len(set(observed_dates)),
            "observation_file_count": len(observations),
            "observation_row_count": sum(len(item["rows"]) for item in observations),
            "duplicate_count": duplicate_count,
            "missing_trading_day_count": len(missing_dates),
            "data_quality_status": data_gate.get("status"),
            "duplicate_guard_normal": duplicate_count == 0,
            "non_trading_day_skip_ok": not non_trading_observation_dates,
            **_safety_summary(),
        },
        latest_observation_date=latest_date.isoformat() if latest_date else None,
        observation_count=len(set(observed_dates)),
        duplicate_count=duplicate_count,
        missing_trading_day_count=len(missing_dates),
        missing_trading_day_dates=[item.isoformat() for item in missing_dates],
        data_quality_status=data_gate.get("status"),
        data_quality=data_gate,
        health_warnings=warnings,
        health_blockers=blockers,
        non_trading_observation_dates=non_trading_observation_dates,
        unsafe_observation_rows=unsafe_rows,
        observation_files=[str(item["path"]) for item in observations],
        research_only_observation=True,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        report_registry_entry=_report_registry_entry(
            "equal_risk_forward_aging_daily_run_health_check",
            "Equal-Risk Forward-Aging Daily Run Health Check",
            "aits research strategies equal-risk-forward-aging-daily-run-health-check",
            [
                "outputs/research_strategies/simple_baselines/"
                "equal_risk_forward_aging_daily_run_health_check.json",
                "outputs/research_strategies/simple_baselines/"
                "equal_risk_forward_aging_daily_run_health_check.md",
            ],
        ),
    )
    _write_pair(payload, output_root, "equal_risk_forward_aging_daily_run_health_check")
    return payload


def run_equal_risk_forward_aging_maturity_update_check(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    before_hashes = _observation_core_hashes_by_path(output_root)
    updater = run_simple_baseline_forward_aging_update_maturity(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
    )
    after_hashes = _observation_core_hashes_by_path(output_root)
    rewritten = _core_hash_rewrite_rows(before_hashes, after_hashes)
    observations = _load_observation_rows(output_root)
    primary_counts = _maturity_counts(observations, PRIMARY_CANDIDATE_ID)
    per_strategy_counts = {
        strategy_id: _maturity_counts(observations, strategy_id)
        for strategy_id in sorted({str(row.get("strategy_id")) for row in observations})
    }
    updater_summary = _mapping(updater.get("summary"))
    data_missing_count = _int(updater_summary.get("missing_window_count"))
    pending_window_count = _int(updater_summary.get("pending_window_count"))
    blockers = []
    warnings = []
    if rewritten:
        blockers.append("original_observation_core_fields_rewritten")
    if updater.get("status") == "MATURITY_BLOCKED":
        blockers.append("maturity_updater_blocked")
    if data_missing_count:
        warnings.append("maturity_window_data_missing")
    if not observations:
        warnings.append("no_observations_to_update")
    if blockers:
        status = "MATURITY_UPDATE_BLOCKED"
    elif warnings and data_missing_count:
        status = "MATURITY_UPDATE_WARN"
    elif _sum_matured(primary_counts) == 0:
        status = "MATURITY_UPDATE_PENDING"
    else:
        status = "MATURITY_UPDATE_HEALTHY"
    payload = _payload(
        report_type="equal_risk_forward_aging_maturity_update_check",
        title="Equal-Risk Forward-Aging Maturity Update Check",
        status=status,
        summary={
            **primary_counts,
            "pending_window_count": pending_window_count,
            "data_missing_count": data_missing_count,
            "maturity_update_timestamp": updater.get("generated_at"),
            "original_target_weights_rewritten": any(
                row["target_weights_hash_changed"] for row in rewritten
            ),
            "original_signal_inputs_rewritten": any(
                row["signal_inputs_hash_changed"] for row in rewritten
            ),
            "definition_hash_rewritten": any(
                row["policy_definition_hash_changed"] for row in rewritten
            ),
            **_safety_summary(),
        },
        **primary_counts,
        pending_window_count=pending_window_count,
        data_missing_count=data_missing_count,
        maturity_update_timestamp=updater.get("generated_at"),
        original_target_weights_rewritten=any(
            row["target_weights_hash_changed"] for row in rewritten
        ),
        original_signal_inputs_rewritten=any(
            row["signal_inputs_hash_changed"] for row in rewritten
        ),
        definition_hash_rewritten=any(
            row["policy_definition_hash_changed"] for row in rewritten
        ),
        per_strategy_maturity_counts=per_strategy_counts,
        core_hash_rewrite_rows=rewritten,
        updater_status=updater.get("status"),
        updater_artifact_paths=updater.get("artifact_paths"),
        warnings=warnings,
        blockers=blockers,
        report_registry_entry=_report_registry_entry(
            "equal_risk_forward_aging_maturity_update_check",
            "Equal-Risk Forward-Aging Maturity Update Check",
            "aits research strategies equal-risk-forward-aging-maturity-update-check",
            [
                "outputs/research_strategies/simple_baselines/"
                "equal_risk_forward_aging_maturity_update_check.json",
                "outputs/research_strategies/simple_baselines/"
                "equal_risk_forward_aging_maturity_update_check.md",
            ],
        ),
    )
    _write_pair(payload, output_root, "equal_risk_forward_aging_maturity_update_check")
    return payload


def run_equal_risk_forward_aging_scoreboard_first_window_review(
    *,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    scoreboard = run_simple_baseline_forward_aging_scoreboard(
        config_path=config_path,
        output_root=output_root,
    )
    primary = _scoreboard_primary_row(scoreboard)
    policy = _mapping(_mapping(config.get("research_policy")).get("forward_aging"))
    min_20d = _int(policy.get("minimum_20d_matured_observations_for_initial_review"))
    min_60d = _int(policy.get("minimum_60d_matured_observations_for_weak_review"))
    min_120d = _int(policy.get("minimum_120d_matured_observations_for_paper_shadow_review"))
    insufficient = (
        _int(primary.get("matured_20d_count")) < min_20d
        or _int(primary.get("matured_60d_count")) < min_60d
        or _int(primary.get("matured_120d_count")) < min_120d
    )
    if not _records(scoreboard.get("scoreboard")):
        status = "EQUAL_RISK_SCOREBOARD_PENDING"
    elif insufficient:
        status = "EQUAL_RISK_SCOREBOARD_INSUFFICIENT"
    elif scoreboard.get("status") == "FORWARD_SCOREBOARD_READY":
        status = "EQUAL_RISK_SCOREBOARD_READY_FOR_RESEARCH_ONLY"
    else:
        status = "EQUAL_RISK_SCOREBOARD_BLOCKED"
    payload = _payload(
        report_type="equal_risk_forward_aging_scoreboard_first_window_review",
        title="Equal-Risk Forward-Aging Scoreboard First Window Review",
        status=status,
        summary={
            "scoreboard_status": status,
            "source_scoreboard_status": scoreboard.get("status"),
            "matured_5d_count": _int(primary.get("matured_5d_count")),
            "matured_10d_count": _int(primary.get("matured_10d_count")),
            "matured_20d_count": _int(primary.get("matured_20d_count")),
            "matured_60d_count": _int(primary.get("matured_60d_count")),
            "matured_120d_count": _int(primary.get("matured_120d_count")),
            "minimum_20d_required": min_20d,
            "minimum_60d_required": min_60d,
            "minimum_120d_required": min_120d,
            **_safety_summary(),
        },
        matured_5d_count=_int(primary.get("matured_5d_count")),
        matured_10d_count=_int(primary.get("matured_10d_count")),
        matured_20d_count=_int(primary.get("matured_20d_count")),
        matured_60d_count=_int(primary.get("matured_60d_count")),
        matured_120d_count=_int(primary.get("matured_120d_count")),
        avg_forward_return_by_window=primary.get("avg_forward_return_by_window", {}),
        avg_forward_drawdown_by_window=primary.get("avg_forward_drawdown_by_window", {}),
        win_rate_vs_100_qqq=primary.get("win_rate_vs_100_qqq", 0.0),
        win_rate_vs_qqq_50_sgov_50=primary.get("win_rate_vs_qqq_50_sgov_50", 0.0),
        win_rate_vs_qqq_60_sgov_40=primary.get("win_rate_vs_qqq_60_sgov_40", 0.0),
        scoreboard_status=status,
        paper_shadow_readiness_output=False,
        source_scoreboard=scoreboard,
        source_artifacts={"scoreboard": scoreboard.get("artifact_paths", {})},
        report_registry_entry=_report_registry_entry(
            "equal_risk_forward_aging_scoreboard_first_window_review",
            "Equal-Risk Forward-Aging Scoreboard First Window Review",
            "aits research strategies equal-risk-forward-aging-scoreboard-first-window-review",
            [
                "outputs/research_strategies/simple_baselines/"
                "equal_risk_forward_aging_scoreboard_first_window_review.json",
                "outputs/research_strategies/simple_baselines/"
                "equal_risk_forward_aging_scoreboard_first_window_review.md",
            ],
        ),
    )
    _write_pair(payload, output_root, "equal_risk_forward_aging_scoreboard_first_window_review")
    return payload


def run_layer2_growth_component_gap_review(
    *,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_mapping(config_path)
    selectable = _records(config.get("selectable_components"))
    inactive = _records(config.get("inactive_research_reference_candidates"))
    excluded = _records(config.get("excluded_components"))
    growth_ready = [
        row
        for row in selectable
        if "growth" in str(row.get("strategy_role", "")).lower()
        or "growth" in str(row.get("strategy_id", "")).lower()
    ]
    current_growth_candidates = [
        {
            "strategy_id": row.get("strategy_id"),
            "status": row.get("strategy_role"),
            "selectable_by_layer1": row.get("selectable_by_layer1"),
            "why_not_component_ready": row.get("inactive_reason"),
            "owner_decision": row.get("owner_decision"),
        }
        for row in inactive
    ]
    current_growth_candidates.extend(
        {
            "strategy_id": row.get("strategy_id"),
            "status": "excluded",
            "why_not_component_ready": row.get("exclusion_reason"),
        }
        for row in excluded
        if any(token in str(row.get("strategy_id", "")).lower() for token in ("tqqq", "leaps"))
        or "Wheel" in str(row.get("strategy_id"))
    )
    answers = {
        "1_component_ready_growth_component_exists": bool(growth_ready),
        "2_why_qqq_plus_growth_inactive_reference": (
            current_growth_candidates[0].get("why_not_component_ready")
            if current_growth_candidates
            else "no_inactive_growth_reference_found"
        ),
        "3_layer1_failure_related_to_narrow_component_pool": True,
        "4_need_continue_controlled_growth_component_search": True,
        "5_minimum_entry_conditions": _growth_component_minimum_conditions(),
        "6_continue_pause_tqqq_heavy": True,
        "7_continue_block_leaps_wheel": True,
    }
    status = (
        "GROWTH_COMPONENT_RESEARCH_NEEDED"
        if growth_ready
        else "GROWTH_COMPONENT_GAP_CONFIRMED"
    )
    payload = _payload(
        report_type="layer2_growth_component_gap_review",
        title="Layer-2 Growth Component Gap Review",
        status=status,
        summary={
            "growth_component_gap_status": status,
            "component_ready_growth_component_exists": bool(growth_ready),
            "inactive_growth_reference_count": len(inactive),
            "formal_selectable_component_count": len(selectable),
            "recommended_next_direction": "restart_layer2_controlled_growth_component_research",
            **_safety_summary(),
        },
        growth_component_gap_status=status,
        current_growth_candidates=current_growth_candidates,
        why_not_component_ready=[
            candidate.get("why_not_component_ready") for candidate in current_growth_candidates
        ],
        required_next_research=[
            "controlled_growth_component_candidate_search",
            "cost_after_edge_vs_100_qqq_review",
            "drawdown_and_risk_adjusted_metric_review",
            "anti_beta_only_attribution",
            "definition_hash_lock",
            "owner_manual_review",
        ],
        recommended_next_direction="restart_layer2_controlled_growth_component_research",
        required_answers=answers,
        growth_component_minimum_conditions=_growth_component_minimum_conditions(),
        source_artifacts={"layer2_component_pool_config": str(config_path)},
        report_registry_entry=_report_registry_entry(
            "layer2_growth_component_gap_review",
            "Layer-2 Growth Component Gap Review",
            "aits research strategies layer2-growth-component-gap-review",
            [
                "outputs/research_strategies/layer2_components/"
                "layer2_growth_component_gap_review.json",
                "outputs/research_strategies/layer2_components/"
                "layer2_growth_component_gap_review.md",
            ],
        ),
    )
    _write_pair(payload, output_root, "layer2_growth_component_gap_review")
    return payload


def run_research_roadmap_master_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    layer1_output_root: Path = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    simple_output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    layer2_output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
    output_root: Path = DEFAULT_RESEARCH_ROADMAP_OUTPUT_ROOT,
    roadmap_doc_path: Path = DEFAULT_RESEARCH_ROADMAP_MASTER_DOC_PATH,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    archive = run_layer1_selector_dry_run_archive_report(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        output_root=layer1_output_root,
        layer2_output_root=layer2_output_root,
        archive_doc_path=roadmap_doc_path.parent / "layer1_selector_dry_run_archive_report.md",
    )
    restart = run_layer1_selector_restart_condition_contract(
        output_root=layer1_output_root,
        simple_output_root=simple_output_root,
    )
    health = run_equal_risk_forward_aging_daily_run_health_check(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=simple_output_root,
        as_of_date=as_of_date,
    )
    maturity = run_equal_risk_forward_aging_maturity_update_check(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=simple_output_root,
        as_of_date=as_of_date,
    )
    scoreboard = run_equal_risk_forward_aging_scoreboard_first_window_review(
        output_root=simple_output_root,
    )
    growth_gap = run_layer2_growth_component_gap_review(output_root=layer2_output_root)
    blocked = [
        report_id
        for report_id, source in {
            "layer1_selector_dry_run_archive_report": archive,
            "layer1_selector_restart_condition_contract": restart,
            "equal_risk_forward_aging_daily_run_health_check": health,
            "equal_risk_forward_aging_maturity_update_check": maturity,
            "equal_risk_forward_aging_scoreboard_first_window_review": scoreboard,
            "layer2_growth_component_gap_review": growth_gap,
        }.items()
        if str(source.get("status", "")).endswith("BLOCKED")
        or str(source.get("status")) == "BLOCKED"
    ]
    final_conclusions = [
        "CONTINUE_EQUAL_RISK_FORWARD_AGING",
        "PAUSE_LAYER1_SELECTOR",
        "RESTART_LAYER2_GROWTH_RESEARCH",
        "KEEP_ALL_RESEARCH_ONLY",
    ]
    answers = {
        "1_layer1_selector_formally_paused": archive.get("status")
        == "LAYER1_SELECTOR_ARCHIVED_DRY_RUN_ONLY",
        "2_equal_risk_continues_forward_aging_mainline": True,
        "3_selector_forward_aging_candidate_exists": False,
        "4_continue_optimizing_200dma_selector_needed": False,
        "5_growth_direction_should_return_to_layer2": True,
        "6_need_new_growth_component_search": True,
        "7_tail_risk_fallback_quarantined": True,
        "8_leaps_wheel_blocked": True,
        "9_next_minimum_task": "controlled_layer2_growth_component_candidate_search",
        "10_no_paper_shadow_no_production_no_broker": True,
    }
    status = "BLOCKED" if blocked else "CONTINUE_EQUAL_RISK_FORWARD_AGING"
    payload = _payload(
        report_type="research_roadmap_master_review",
        title="Research Roadmap Master Review",
        status=status,
        summary={
            "primary_conclusion": status,
            "layer1_selector_status": archive.get("status"),
            "restart_allowed_now": restart.get("restart_allowed_now"),
            "equal_risk_health_status": health.get("status"),
            "maturity_update_status": maturity.get("status"),
            "scoreboard_status": scoreboard.get("status"),
            "growth_component_gap_status": growth_gap.get("status"),
            "blocked_source_count": len(blocked),
            **_safety_summary(),
        },
        final_conclusions=final_conclusions if not blocked else ["BLOCKED"],
        required_answers=answers,
        source_statuses={
            "layer1_selector_dry_run_archive_report": archive.get("status"),
            "layer1_selector_restart_condition_contract": restart.get("status"),
            "equal_risk_forward_aging_daily_run_health_check": health.get("status"),
            "equal_risk_forward_aging_maturity_update_check": maturity.get("status"),
            "equal_risk_forward_aging_scoreboard_first_window_review": scoreboard.get("status"),
            "layer2_growth_component_gap_review": growth_gap.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "layer1_selector_dry_run_archive_report": archive,
                "layer1_selector_restart_condition_contract": restart,
                "equal_risk_forward_aging_daily_run_health_check": health,
                "equal_risk_forward_aging_maturity_update_check": maturity,
                "equal_risk_forward_aging_scoreboard_first_window_review": scoreboard,
                "layer2_growth_component_gap_review": growth_gap,
            }
        ),
        blockers=blocked,
        owner_next_action="review_roadmap_and_pick_next_layer2_growth_research_task",
        report_registry_entry=_report_registry_entry(
            "research_roadmap_master_review",
            "Research Roadmap Master Review",
            "aits research strategies research-roadmap-master-review",
            [
                "outputs/research_strategies/research_roadmap_master_review.json",
                "outputs/research_strategies/research_roadmap_master_review.md",
                "docs/research/research_roadmap_master_review.md",
            ],
        ),
    )
    _write_pair(payload, output_root, "research_roadmap_master_review")
    _copy_markdown_artifact(payload, roadmap_doc_path)
    return payload


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        **AI_REGIME_SUMMARY,
        "summary": {
            **AI_REGIME_SUMMARY,
            **dict(summary),
        },
        **SAFETY_BOUNDARY,
        **extra,
    }


def _write_pair(payload: dict[str, Any], output_root: Path, artifact_id: str) -> None:
    payload["artifact_paths"] = {
        "json_path": str(output_root / f"{artifact_id}.json"),
        "markdown_path": str(output_root / f"{artifact_id}.md"),
    }
    write_foundation_artifact_pair(payload, output_root=output_root, artifact_id=artifact_id)


def _copy_markdown_artifact(payload: Mapping[str, Any], target_path: Path) -> None:
    source = Path(str(_mapping(payload.get("artifact_paths")).get("markdown_path", "")))
    if not source.exists():
        return
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")


def _report_registry_entry(
    report_id: str,
    title: str,
    command: str,
    artifact_globs: list[str],
) -> dict[str, Any]:
    return {
        "report_id": report_id,
        "title": title,
        "group": "research",
        "cadence": "ad_hoc",
        "audience": "project_owner",
        "owner": "research_governance",
        "command": command,
        "artifact_globs": artifact_globs,
        "artifact_selection_policy": "latest_available",
        "freshness_sla_days": 30,
        "freshness_rationale": (
            "TRADING-1024 to 1030 artifacts summarize research-only archive, "
            "forward-aging health, and roadmap state after Layer-1 final gate."
        ),
        "owner_action": "review_research_only_roadmap_state",
        "include_in_reader_brief": False,
        "include_in_daily_task_dashboard": False,
        "required_for_daily_reading": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _load_mapping(path: Path) -> dict[str, Any]:
    loaded = safe_load_yaml_path(path)
    if not isinstance(loaded, dict):
        raise ValueError(f"expected YAML mapping: {path}")
    return loaded


def _safety_summary() -> dict[str, Any]:
    return {
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "manual_review_required": True,
    }


def _source_blocked(sources: list[Mapping[str, Any]]) -> list[str]:
    blocked = []
    for source in sources:
        status = str(source.get("status") or "")
        if "BLOCKED" in status or status == "FAIL":
            blocked.append(str(source.get("report_type") or status))
    return blocked


def _artifact_paths_by_report(sources: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    return {report_id: source.get("artifact_paths", {}) for report_id, source in sources.items()}


def _first_summary_value(sources: list[Mapping[str, Any]], key: str) -> Any:
    for source in sources:
        summary = _mapping(source.get("summary"))
        if key in summary:
            return summary[key]
    return None


def _condition(
    condition_id: str,
    currently_satisfied: bool,
    evidence_required: str,
) -> dict[str, Any]:
    return {
        "condition_id": condition_id,
        "currently_satisfied": currently_satisfied,
        "evidence_required": evidence_required,
    }


def _observation_payload_records(output_root: Path) -> list[dict[str, Any]]:
    observation_root = output_root / "forward_aging_observations"
    if observation_root == DEFAULT_FORWARD_AGING_OBSERVATION_ROOT:
        observation_root = DEFAULT_FORWARD_AGING_OBSERVATION_ROOT
    records = []
    for path in sorted(observation_root.glob("simple_baseline_forward_aging_observation_*.json")):
        payload = _read_json_or_empty(path)
        decision_date = _safe_date(
            payload.get("decision_date") or _mapping(payload.get("summary")).get("decision_date")
        )
        records.append(
            {
                "path": path,
                "payload": payload,
                "decision_date": decision_date,
                "rows": _records(payload.get("observations")),
            }
        )
    return records


def _load_observation_rows(output_root: Path) -> list[dict[str, Any]]:
    rows = []
    for item in _observation_payload_records(output_root):
        rows.extend(dict(row) for row in item["rows"])
    return rows


def _duplicate_observation_count(observations: list[Mapping[str, Any]]) -> int:
    keys = []
    for item in observations:
        for row in item["rows"]:
            decision_date = row.get("decision_date") or (
                item["decision_date"].isoformat() if item["decision_date"] else None
            )
            keys.append((decision_date, row.get("strategy_id")))
    counts = Counter(keys)
    return sum(count - 1 for count in counts.values() if count > 1)


def _qqq_price_dates(prices_path: Path) -> set[date]:
    if not prices_path.exists():
        return set()
    try:
        frame = pd.read_csv(prices_path, usecols=["date", "ticker"])
    except Exception:
        return set()
    if frame.empty:
        return set()
    rows = frame.loc[frame["ticker"].astype(str) == "QQQ", "date"].astype(str)
    return {item for value in rows if (item := _safe_date(value)) is not None}


def _missing_observation_dates(
    observed_dates: list[date],
    price_dates: set[date],
    as_of_date: date | None,
) -> list[date]:
    if not observed_dates or as_of_date is None:
        return []
    start = min(observed_dates)
    expected = {
        item
        for item in price_dates
        if start <= item <= as_of_date and us_equity_market_session(item).is_trading_day
    }
    return sorted(expected - set(observed_dates))


def _unsafe_observation_rows(observations: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    unsafe = []
    for item in observations:
        for row in item["rows"]:
            if (
                row.get("paper_shadow_allowed") is not False
                or row.get("production_allowed") is not False
                or row.get("broker_action") != "none"
            ):
                unsafe.append(
                    {
                        "path": str(item["path"]),
                        "decision_date": row.get("decision_date"),
                        "strategy_id": row.get("strategy_id"),
                    }
                )
    return unsafe


def _observation_core_hashes_by_path(output_root: Path) -> dict[str, dict[str, str]]:
    hashes = {}
    for item in _observation_payload_records(output_root):
        path = str(item["path"])
        primary = next(
            (row for row in item["rows"] if row.get("strategy_id") == PRIMARY_CANDIDATE_ID),
            item["rows"][0] if item["rows"] else {},
        )
        hashes[path] = {
            "target_weights_hash": _stable_hash(primary.get("target_weights")),
            "signal_inputs_hash": _stable_hash(primary.get("signal_inputs_used")),
            "policy_definition_hash": str(primary.get("policy_definition_hash") or ""),
        }
    return hashes


def _core_hash_rewrite_rows(
    before: Mapping[str, Mapping[str, str]],
    after: Mapping[str, Mapping[str, str]],
) -> list[dict[str, Any]]:
    rows = []
    for path, before_hashes in before.items():
        after_hashes = after.get(path, {})
        row = {
            "path": path,
            "target_weights_hash_changed": before_hashes.get("target_weights_hash")
            != after_hashes.get("target_weights_hash"),
            "signal_inputs_hash_changed": before_hashes.get("signal_inputs_hash")
            != after_hashes.get("signal_inputs_hash"),
            "policy_definition_hash_changed": before_hashes.get("policy_definition_hash")
            != after_hashes.get("policy_definition_hash"),
        }
        if any(
            row[key]
            for key in (
                "target_weights_hash_changed",
                "signal_inputs_hash_changed",
                "policy_definition_hash_changed",
            )
        ):
            rows.append(row)
    return rows


def _maturity_counts(observations: list[Mapping[str, Any]], strategy_id: str) -> dict[str, int]:
    selected = [row for row in observations if row.get("strategy_id") == strategy_id]
    result = {
        "matured_5d_count": 0,
        "matured_10d_count": 0,
        "matured_20d_count": 0,
        "matured_60d_count": 0,
        "matured_120d_count": 0,
    }
    for row in selected:
        windows = _mapping(row.get("forward_windows"))
        for label in ("5d", "10d", "20d", "60d", "120d"):
            if _mapping(windows.get(label)).get("status") == "MATURED":
                result[f"matured_{label}_count"] += 1
    return result


def _sum_matured(counts: Mapping[str, Any]) -> int:
    return sum(_int(value) for key, value in counts.items() if key.startswith("matured_"))


def _scoreboard_primary_row(scoreboard: Mapping[str, Any]) -> dict[str, Any]:
    for row in _records(scoreboard.get("scoreboard")):
        if row.get("strategy_id") == PRIMARY_CANDIDATE_ID:
            return row
    return {}


def _growth_component_minimum_conditions() -> list[str]:
    return [
        "historical_return_materially_above_100_qqq",
        "cost_after_edge_survives",
        "max_drawdown_not_materially_worse",
        "calmar_or_sharpe_not_weaker_than_100_qqq",
        "not_pure_effective_beta_above_one",
        "not_only_ai_rally_effective",
        "switch_and_turnover_controlled",
        "definition_hash_locked",
        "data_quality_gate_passed",
        "owner_manual_review",
    ]


def _safe_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default
