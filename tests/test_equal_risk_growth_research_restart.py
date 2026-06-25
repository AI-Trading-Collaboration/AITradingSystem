from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.controlled_growth_component_research import (
    DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    run_beta_adjusted_growth_edge_contract,
    run_controlled_growth_component_registry_v2_review,
    run_drawdown_guarded_growth_component_search,
    run_equal_risk_and_growth_dual_track_roadmap,
    run_growth_component_beta_exposure_attribution,
    run_growth_component_cost_turnover_sensitivity,
    run_growth_component_owner_decision_pack,
    run_growth_component_period_drawdown_validation,
    run_growth_component_readiness_gate,
    run_layer2_growth_component_restart_contract,
    run_low_turnover_controlled_growth_search,
    run_research_roadmap_v2_master_review,
    run_volatility_targeted_growth_component_search,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_roadmap_stabilization import (
    run_equal_risk_first_maturity_monitor,
    run_equal_risk_forward_aging_scheduler_integration,
    run_equal_risk_forward_aging_scoreboard_safety_gate,
    run_equal_risk_observation_continuity_check,
    run_equal_risk_reader_brief_live_summary,
)

NEW_RESEARCH_REPORT_IDS = {
    "equal_risk_forward_aging_scheduler_integration",
    "equal_risk_observation_continuity_check",
    "equal_risk_first_maturity_monitor",
    "equal_risk_forward_aging_scoreboard_safety_gate",
    "equal_risk_reader_brief_live_summary",
    "layer2_growth_component_restart_contract",
    "controlled_growth_component_registry_v2_review",
    "beta_adjusted_growth_edge_contract",
    "low_turnover_controlled_growth_search",
    "volatility_targeted_growth_component_search",
    "drawdown_guarded_growth_component_search",
    "growth_component_beta_exposure_attribution",
    "growth_component_period_drawdown_validation",
    "growth_component_cost_turnover_sensitivity",
    "growth_component_readiness_gate",
    "growth_component_owner_decision_pack",
    "equal_risk_and_growth_dual_track_roadmap",
    "research_roadmap_v2_master_review",
}


def test_equal_risk_forward_aging_stabilization_outputs_preserve_safety(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_growth_caches(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "simple_baselines"

    scheduler = run_equal_risk_forward_aging_scheduler_integration(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=as_of,
        decision_date=date(2022, 12, 1),
    )
    continuity = run_equal_risk_observation_continuity_check(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=as_of,
    )
    maturity = run_equal_risk_first_maturity_monitor(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=as_of,
    )
    scoreboard = run_equal_risk_forward_aging_scoreboard_safety_gate(output_root=output_root)
    reader_summary = run_equal_risk_reader_brief_live_summary(output_root=output_root)

    assert scheduler["status"] in {
        "SCHEDULER_INTEGRATION_READY",
        "SCHEDULER_INTEGRATION_WARN",
    }
    assert scheduler["observation_write_status"] == "OBSERVATION_WRITTEN"
    assert scheduler["observation_written"] is True
    assert continuity["status"] != "OBSERVATION_CONTINUITY_BLOCKED"
    assert continuity["summary"]["duplicate_observation_date_count"] == 0
    assert continuity["summary"]["invalid_artifact_count"] == 0
    assert maturity["matured_20d_count"] >= 1
    assert maturity["matured_120d_count"] >= 1
    assert scoreboard["scoreboard_status"] in {"INSUFFICIENT", "RESEARCH_ONLY_READY"}
    assert reader_summary["equal_risk_forward_aging_summary"]["broker_action"] == "none"
    assert reader_summary["prohibited_phrase_hits"] == []

    for payload in (scheduler, continuity, maturity, scoreboard, reader_summary):
        _assert_research_only_payload(payload)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "equal-risk-observation-continuity-check",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            as_of.isoformat(),
            "--output-root",
            str(output_root),
        ],
    )

    assert result.exit_code == 0, result.output


def test_controlled_growth_component_restart_builders_and_cli(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_growth_caches(tmp_path)
    growth_root = tmp_path / "outputs" / "research_strategies" / "growth_components"
    simple_root = tmp_path / "outputs" / "research_strategies" / "simple_baselines"
    roadmap_root = tmp_path / "outputs" / "research_strategies" / "roadmap"
    owner_docs_path = tmp_path / "docs" / "research" / "growth_component_owner_decision_pack.md"
    roadmap_docs_path = tmp_path / "docs" / "research" / "research_roadmap_v2_master_review.md"

    payloads = [
        run_layer2_growth_component_restart_contract(output_root=growth_root),
        run_controlled_growth_component_registry_v2_review(output_root=growth_root),
        run_beta_adjusted_growth_edge_contract(output_root=growth_root),
        run_low_turnover_controlled_growth_search(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_path,
            rates_path=rates_path,
            output_root=growth_root,
            as_of_date=as_of,
        ),
        run_volatility_targeted_growth_component_search(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_path,
            rates_path=rates_path,
            output_root=growth_root,
            as_of_date=as_of,
        ),
        run_drawdown_guarded_growth_component_search(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_path,
            rates_path=rates_path,
            output_root=growth_root,
            as_of_date=as_of,
        ),
        run_growth_component_beta_exposure_attribution(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_path,
            rates_path=rates_path,
            output_root=growth_root,
            as_of_date=as_of,
        ),
        run_growth_component_period_drawdown_validation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_path,
            rates_path=rates_path,
            output_root=growth_root,
            as_of_date=as_of,
        ),
        run_growth_component_cost_turnover_sensitivity(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_path,
            rates_path=rates_path,
            output_root=growth_root,
            as_of_date=as_of,
        ),
        run_growth_component_readiness_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_path,
            rates_path=rates_path,
            output_root=growth_root,
            as_of_date=as_of,
        ),
        run_growth_component_owner_decision_pack(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_path,
            rates_path=rates_path,
            output_root=growth_root,
            docs_path=owner_docs_path,
            as_of_date=as_of,
        ),
        run_equal_risk_and_growth_dual_track_roadmap(output_root=roadmap_root),
        run_research_roadmap_v2_master_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_path,
            rates_path=rates_path,
            simple_output_root=simple_root,
            growth_output_root=growth_root,
            output_root=roadmap_root,
            owner_docs_path=owner_docs_path,
            docs_path=roadmap_docs_path,
            as_of_date=as_of,
        ),
    ]

    low_turnover = next(
        item for item in payloads if item["report_type"] == "low_turnover_controlled_growth_search"
    )
    attribution = next(
        item
        for item in payloads
        if item["report_type"] == "growth_component_beta_exposure_attribution"
    )
    owner = next(
        item for item in payloads if item["report_type"] == "growth_component_owner_decision_pack"
    )
    roadmap_v2 = next(
        item for item in payloads if item["report_type"] == "research_roadmap_v2_master_review"
    )

    assert low_turnover["summary"]["candidate_count"] >= 1
    assert low_turnover["candidate_results"]
    assert attribution["summary"]["candidate_count"] >= 1
    assert owner["owner_recommendation"] in {
        "PROMOTE_TO_COMPONENT_REVIEW",
        "KEEP_GROWTH_RESEARCH_ONLY",
        "NO_MATERIAL_GROWTH_EDGE",
        "BLOCKED",
    }
    assert owner_docs_path.exists()
    assert roadmap_v2["summary"]["primary_conclusion"]
    assert roadmap_docs_path.exists()

    for payload in payloads:
        _assert_research_only_payload(payload)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "low-turnover-controlled-growth-search",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            as_of.isoformat(),
            "--output-root",
            str(growth_root),
        ],
    )

    assert result.exit_code == 0, result.output
    written = json.loads(
        (growth_root / "low_turnover_controlled_growth_search.json").read_text(
            encoding="utf-8"
        )
    )
    assert written["summary"]["broker_action"] == "none"


def test_new_research_reports_are_registered_with_latest_available_policy() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}

    assert NEW_RESEARCH_REPORT_IDS <= set(entries)
    for report_id in NEW_RESEARCH_REPORT_IDS:
        entry = entries[report_id]
        assert entry["artifact_selection_policy"] == "latest_available"
        assert entry["required_for_daily_reading"] is False
        assert entry["production_effect"] == "none"
        assert entry["broker_action"] == "none"
        assert entry["command"].startswith("aits research strategies ")
        assert entry["artifact_globs"]


def test_controlled_growth_registry_v2_keeps_old_growth_paths_excluded() -> None:
    config = yaml.safe_load(DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH.read_text())
    safety = config["safety_boundary"]
    excluded = config["excluded_paths"]

    assert safety["production_effect"] == "none"
    assert safety["broker_action"] == "none"
    assert safety["paper_shadow_allowed"] is False
    assert safety["production_allowed"] is False
    assert "restore_old_qqq_plus_growth_as_selectable" in excluded
    assert "tqqq_heavy_mainline" in excluded
    assert "LEAPS" in excluded
    assert "Wheel" in excluded


def _assert_research_only_payload(payload: dict[str, object]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["manual_review_required"] is True
    assert Path(payload["artifact_paths"]["json_path"]).exists()
    assert Path(payload["artifact_paths"]["markdown_path"]).exists()


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
