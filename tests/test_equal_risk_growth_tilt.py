from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.equal_risk_growth_tilt import (
    DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    run_equal_risk_cap_floor_tilt_search,
    run_equal_risk_growth_tilt_objective_contract,
    run_equal_risk_growth_tilt_ranking_tiering,
    run_equal_risk_growth_tilt_registry_review,
    run_equal_risk_growth_tilt_tradeoff_frontier,
    run_equal_risk_missed_upside_compensation_search,
    run_equal_risk_risk_budget_tilt_search,
    run_equal_risk_small_tqqq_overlay_search,
    run_equal_risk_trend_on_qqq_boost_search,
    run_equal_risk_vol_target_growth_tilt_search,
    run_growth_exploration_master_review,
    run_growth_research_framing_correction,
    run_growth_tilt_beta_risk_budget_attribution,
    run_growth_tilt_cost_turnover_sensitivity,
    run_growth_tilt_definition_lock_versioning,
    run_growth_tilt_forward_aging_readiness_gate,
    run_growth_tilt_owner_decision_pack,
    run_growth_tilt_period_drawdown_replay,
    run_growth_tilt_reader_brief_safety_preview,
    run_roadmap_update_after_growth_tilt_review,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

GROWTH_TILT_REPORT_IDS = {
    "growth_research_framing_correction",
    "equal_risk_growth_tilt_objective_contract",
    "equal_risk_growth_tilt_registry_review",
    "equal_risk_cap_floor_tilt_search",
    "equal_risk_risk_budget_tilt_search",
    "equal_risk_trend_on_qqq_boost_search",
    "equal_risk_missed_upside_compensation_search",
    "equal_risk_small_tqqq_overlay_search",
    "equal_risk_vol_target_growth_tilt_search",
    "equal_risk_growth_tilt_ranking_tiering",
    "growth_tilt_beta_risk_budget_attribution",
    "growth_tilt_period_drawdown_replay",
    "growth_tilt_cost_turnover_sensitivity",
    "equal_risk_growth_tilt_tradeoff_frontier",
    "growth_tilt_definition_lock_versioning",
    "growth_tilt_forward_aging_readiness_gate",
    "growth_tilt_owner_decision_pack",
    "growth_exploration_master_review",
    "roadmap_update_after_growth_tilt_review",
    "growth_tilt_reader_brief_safety_preview",
}


def test_equal_risk_growth_tilt_builders_preserve_research_only_boundary(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_growth_caches(tmp_path)
    config_path = _write_small_growth_config(tmp_path)
    growth_root = tmp_path / "outputs" / "research_strategies" / "growth_components"
    roadmap_root = tmp_path / "outputs" / "research_strategies" / "roadmap"
    docs_root = tmp_path / "docs" / "research"
    owner_docs_path = docs_root / "growth_tilt_owner_decision_pack.md"
    master_docs_path = docs_root / "growth_exploration_master_review.md"
    data_kwargs = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_path,
        "rates_path": rates_path,
        "config_path": config_path,
        "output_root": growth_root,
        "as_of_date": as_of,
    }

    payloads = [
        run_growth_research_framing_correction(output_root=growth_root),
        run_equal_risk_growth_tilt_objective_contract(
            config_path=config_path,
            output_root=growth_root,
        ),
        run_equal_risk_growth_tilt_registry_review(
            config_path=config_path,
            output_root=growth_root,
        ),
    ]
    payloads.extend(
        builder(**data_kwargs)
        for builder in (
            run_equal_risk_cap_floor_tilt_search,
            run_equal_risk_risk_budget_tilt_search,
            run_equal_risk_trend_on_qqq_boost_search,
            run_equal_risk_missed_upside_compensation_search,
            run_equal_risk_small_tqqq_overlay_search,
            run_equal_risk_vol_target_growth_tilt_search,
            run_equal_risk_growth_tilt_ranking_tiering,
            run_growth_tilt_beta_risk_budget_attribution,
            run_growth_tilt_period_drawdown_replay,
            run_growth_tilt_cost_turnover_sensitivity,
            run_equal_risk_growth_tilt_tradeoff_frontier,
            run_growth_tilt_definition_lock_versioning,
            run_growth_tilt_forward_aging_readiness_gate,
            run_growth_tilt_reader_brief_safety_preview,
        )
    )
    payloads.extend(
        [
            run_growth_tilt_owner_decision_pack(
                **data_kwargs,
                docs_path=owner_docs_path,
            ),
            run_growth_exploration_master_review(
                **data_kwargs,
                docs_path=master_docs_path,
                owner_docs_path=owner_docs_path,
            ),
            run_roadmap_update_after_growth_tilt_review(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_path,
                rates_path=rates_path,
                config_path=config_path,
                growth_output_root=growth_root,
                output_root=roadmap_root,
                growth_master_docs_path=master_docs_path,
                growth_owner_docs_path=owner_docs_path,
                as_of_date=as_of,
            ),
        ]
    )

    payloads_by_type = {payload["report_type"]: payload for payload in payloads}
    ranking = payloads_by_type["equal_risk_growth_tilt_ranking_tiering"]
    cap_floor = payloads_by_type["equal_risk_cap_floor_tilt_search"]
    gate = payloads_by_type["growth_tilt_forward_aging_readiness_gate"]
    owner = payloads_by_type["growth_tilt_owner_decision_pack"]
    master = payloads_by_type["growth_exploration_master_review"]
    reader_preview = payloads_by_type["growth_tilt_reader_brief_safety_preview"]

    assert cap_floor["summary"]["candidate_count"] >= 1
    assert cap_floor["data_quality"]["passed"] is True
    assert str(cap_floor["requested_date_range"]).startswith("2022-12-01..")
    assert ranking["status"] in {
        "GROWTH_TILT_CANDIDATES_RANKED",
        "NO_GROWTH_TILT_EDGE",
    }
    assert ranking["summary"]["candidate_count"] >= 1
    assert gate["forward_aging_watchlist_allowed"] is False
    assert gate["status"] in {
        "GROWTH_TILT_FORWARD_AGING_REVIEWABLE",
        "GROWTH_TILT_RESEARCH_ONLY",
        "NO_GROWTH_TILT_CANDIDATE",
    }
    assert owner["owner_recommendation"] in {
        "OWNER_REVIEW_GROWTH_TILT_FORWARD_AGING_CANDIDATE",
        "KEEP_GROWTH_TILT_RESEARCH_ONLY",
        "NEED_MORE_HISTORY",
        "NO_USEFUL_GROWTH_TILT",
        "BLOCKED",
    }
    assert owner_docs_path.exists()
    assert master["status"] in {
        "GROWTH_TILT_FOUND",
        "BALANCED_CORE_CANDIDATE_FOUND",
        "CONTINUE_STRUCTURED_GROWTH_EXPLORATION",
    }
    assert master_docs_path.exists()
    assert reader_preview["prohibited_phrase_hits"] == []

    for payload in payloads:
        _assert_research_only_payload(payload)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "equal-risk-cap-floor-tilt-search",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--config",
            str(config_path),
            "--as-of",
            as_of.isoformat(),
            "--output-root",
            str(growth_root),
        ],
    )

    assert result.exit_code == 0, result.output
    written = json.loads(
        (growth_root / "equal_risk_cap_floor_tilt_search.json").read_text(
            encoding="utf-8"
        )
    )
    assert written["summary"]["broker_action"] == "none"


def test_equal_risk_growth_tilt_reports_and_registry_contracts() -> None:
    config = yaml.safe_load(DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH.read_text())
    safety = config["safety_boundary"]
    excluded_paths = set(config["excluded_paths"])
    families = config["candidate_families"]
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}

    assert safety["production_effect"] == "none"
    assert safety["broker_action"] == "none"
    assert safety["paper_shadow_allowed"] is False
    assert safety["production_allowed"] is False
    assert safety["manual_review_required"] is True
    assert config["market_regime"]["regime_id"] == "ai_after_chatgpt"
    assert str(config["market_regime"]["default_backtest_start"]) == "2022-12-01"
    assert "modify_original_equal_risk_qqq_sgov" in excluded_paths
    assert "tail_risk_fallback" in excluded_paths
    assert "LEAPS" in excluded_paths
    assert "Wheel" in excluded_paths
    assert {item["candidate_family"] for item in families} == {
        "cap_floor_tilt",
        "risk_budget_tilt",
        "trend_on_qqq_boost",
        "missed_upside_compensation",
        "small_tqqq_overlay",
        "vol_target_growth_tilt",
    }
    assert all(item["forward_aging_allowed"] is False for item in families)
    assert all(item["paper_shadow_allowed"] is False for item in families)
    assert all(item["production_allowed"] is False for item in families)
    assert all(item["broker_action"] == "none" for item in families)

    assert GROWTH_TILT_REPORT_IDS <= set(entries)
    for report_id in GROWTH_TILT_REPORT_IDS:
        entry = entries[report_id]
        assert entry["artifact_selection_policy"] == "latest_available"
        assert entry["required_for_daily_reading"] is False
        assert entry["production_effect"] == "none"
        assert entry["broker_action"] == "none"
        assert entry["command"].startswith("aits research strategies ")
        assert entry["artifact_globs"]


def _assert_research_only_payload(payload: dict[str, object]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["manual_review_required"] is True
    assert payload["market_regime"] == "ai_after_chatgpt"
    assert Path(payload["artifact_paths"]["json_path"]).exists()
    assert Path(payload["artifact_paths"]["markdown_path"]).exists()


def _write_small_growth_config(tmp_path: Path) -> Path:
    config = yaml.safe_load(DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH.read_text())
    policy = config["research_policy"]
    grids = policy["search_grids"]
    grids["cap_floor_tilt"] = {
        "qqq_max_weight": [0.70],
        "sgov_min_weight": [0.30],
        "rebalance": ["monthly"],
    }
    grids["risk_budget_tilt"] = {
        "qqq_risk_budget": [0.65],
        "sgov_risk_budget": [0.35],
        "vol_lookback": [60],
        "rebalance": ["monthly"],
    }
    grids["trend_on_qqq_boost"] = {
        "boost_amount": [0.10],
        "rebalance": ["monthly"],
    }
    policy["missed_upside_policy"]["thresholds"] = [0.05]
    policy["missed_upside_policy"]["compensation_amounts"] = [0.10]
    policy["missed_upside_policy"]["ramp_days"] = [10]
    grids["small_tqqq_overlay"] = {
        "max_tqqq_weight": [0.05],
        "rebalance": ["monthly"],
    }
    grids["vol_target_growth_tilt"] = {
        "target_vol_absolute": [0.15],
        "target_vol_additive_pp": [0.02],
        "vol_lookback": [60],
        "qqq_max_weight": [0.80],
        "sgov_min_weight": [0.20],
    }
    config_path = tmp_path / "equal_risk_growth_tilt_candidate_registry.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return config_path


def _write_growth_caches(tmp_path: Path) -> tuple[Path, Path, Path, date]:
    dates = _business_dates(date(2022, 12, 1), 760)
    prices_path = tmp_path / "prices_daily.csv"
    marketstack_path = tmp_path / "prices_marketstack_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    price_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    secondary_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    levels = {"QQQ": 280.0, "TQQQ": 22.0, "SGOV": 100.0}

    for day_index, row_date in enumerate(dates):
        qqq_return = 0.00065 + 0.0018 * math.sin(day_index / 19.0)
        if 90 <= day_index <= 125:
            qqq_return -= 0.006
        if 126 <= day_index <= 185:
            qqq_return += 0.004
        if 430 <= day_index <= 470:
            qqq_return -= 0.004
        levels["QQQ"] *= 1.0 + qqq_return
        levels["TQQQ"] *= 1.0 + qqq_return * 3.0 - 0.00025
        levels["SGOV"] *= 1.0 + 0.00016
        for ticker in ("QQQ", "TQQQ", "SGOV"):
            close = levels[ticker]
            row = (
                f"{row_date.isoformat()},{ticker},{close * 0.999:.4f},"
                f"{close * 1.002:.4f},{close * 0.998:.4f},{close:.4f},"
                f"{close:.4f},{1000000 + day_index}\n"
            )
            price_rows.append(row)
            secondary_rows.append(row)

    rate_rows = ["date,series,value\n"]
    for day_index, row_date in enumerate(dates):
        rate_rows.append(f"{row_date.isoformat()},DGS2,{4.0 + day_index * 0.0004:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DGS10,{4.2 + day_index * 0.0003:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DTWEXBGS,{120.0 + day_index * 0.01:.4f}\n")

    prices_path.write_text("".join(price_rows), encoding="utf-8")
    marketstack_path.write_text("".join(secondary_rows), encoding="utf-8")
    rates_path.write_text("".join(rate_rows), encoding="utf-8")
    return prices_path, marketstack_path, rates_path, dates[-1]


def _business_dates(start: date, count: int) -> list[date]:
    result = []
    current = start
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current)
        current += timedelta(days=1)
    return result
