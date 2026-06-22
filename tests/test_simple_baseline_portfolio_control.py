from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import render_reader_brief_html
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    run_options_next_stage_gate,
    run_qqq_sgov_baseline_backtest,
    run_simple_baseline_cost_sensitivity,
    run_simple_baseline_daily_reader_safety_summary,
    run_simple_baseline_dominance_ranking,
    run_simple_baseline_forward_aging_tracker,
    run_simple_baseline_master_review,
    run_simple_baseline_paper_shadow_readiness,
    run_simple_baseline_pit_boundary_audit,
    run_simple_baseline_portfolio_dry_run_mapper,
    run_simple_baseline_regime_review,
    run_simple_baseline_registry_review,
    run_tqqq_sgov_risk_controlled_baseline,
    run_trend_vol_allocation_policy_search,
)

TEST_AS_OF = date(2024, 7, 10)
SIMPLE_BASELINE_REPORT_IDS = {
    "simple_baseline_strategy_registry_review",
    "qqq_sgov_baseline_backtest",
    "tqqq_sgov_risk_controlled_baseline",
    "trend_vol_allocation_policy_search",
    "simple_baseline_dominance_ranking",
    "simple_baseline_pit_boundary_audit",
    "simple_baseline_cost_sensitivity",
    "simple_baseline_regime_review",
    "simple_baseline_forward_aging_tracker",
    "simple_baseline_paper_shadow_readiness",
    "daily_reader_portfolio_control_safety_summary",
    "simple_baseline_portfolio_dry_run_mapper",
    "simple_baseline_master_review",
    "options_next_stage_gate",
}


def test_simple_baseline_research_functions_write_auditable_artifacts(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_simple_baseline_caches(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "simple_baselines"
    docs_path = tmp_path / "docs" / "research" / "simple_baseline_master_review.md"

    registry = run_simple_baseline_registry_review(output_root=output_root)
    qqq = run_qqq_sgov_baseline_backtest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    tqqq = run_tqqq_sgov_risk_controlled_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    policy = run_trend_vol_allocation_policy_search(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    ranking = run_simple_baseline_dominance_ranking(output_root=output_root)
    pit = run_simple_baseline_pit_boundary_audit(output_root=output_root)
    cost = run_simple_baseline_cost_sensitivity(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    regime = run_simple_baseline_regime_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    forward = run_simple_baseline_forward_aging_tracker(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    readiness = run_simple_baseline_paper_shadow_readiness(output_root=output_root)
    daily = run_simple_baseline_daily_reader_safety_summary(output_root=output_root)
    mapper = run_simple_baseline_portfolio_dry_run_mapper(output_root=output_root)
    master = run_simple_baseline_master_review(output_root=output_root, master_doc_path=docs_path)
    options = run_options_next_stage_gate(output_root=output_root)

    payloads = [
        registry,
        qqq,
        tqqq,
        policy,
        ranking,
        pit,
        cost,
        regime,
        forward,
        readiness,
        daily,
        mapper,
        master,
        options,
    ]
    assert {payload["report_type"] for payload in payloads} == SIMPLE_BASELINE_REPORT_IDS
    assert registry["status"] == "BASELINE_REGISTRY_READY"
    assert qqq["data_quality"]["passed"] is True
    assert qqq["data_quality"]["price_row_count"] > 0
    assert qqq["data_quality"]["price_checksum"]
    assert qqq["requested_date_range"].startswith("2022-12-01")
    assert tqqq["status"] in {
        "TQQQ_BASELINE_RESEARCH_READY",
        "TQQQ_BASELINE_TOO_RISKY",
    }
    assert policy["allowed_inputs"]
    assert ranking["recommended_research_candidates"]
    assert pit["status"] == "PIT_BOUNDARY_PASS"
    assert cost["data_quality"]["status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert regime["return_by_regime"]
    assert forward["summary"]["matured_20d_count"] >= 20
    assert readiness["promotion_allowed"] is False
    assert readiness["paper_shadow_allowed"] is False
    assert daily["portfolio_control_research_status"]["broker_action"] == "none"
    assert mapper["broker_read_performed"] is False
    assert master["master_review_doc_path"] == str(docs_path)
    assert docs_path.exists()
    assert options["options_research_allowed"] is False

    for payload in payloads:
        assert payload["market_regime"] == "ai_after_chatgpt"
        assert payload["production_effect"] == "none"
        assert payload["broker_action"] == "none"
        assert payload["promotion_allowed"] is False
        assert payload["paper_shadow_allowed"] is False
        assert payload["production_allowed"] is False
        assert Path(payload["artifact_paths"]["json_path"]).exists()
        assert Path(payload["artifact_paths"]["markdown_path"]).exists()


def test_simple_baseline_cli_smoke_and_report_registry(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_simple_baseline_caches(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "simple_baselines"
    docs_path = tmp_path / "docs" / "research" / "simple_baseline_master_review.md"
    runner = CliRunner()

    data_args = [
        "--prices-path",
        str(prices_path),
        "--marketstack-prices-path",
        str(marketstack_path),
        "--rates-path",
        str(rates_path),
        "--as-of",
        TEST_AS_OF.isoformat(),
        "--output-root",
        str(output_root),
    ]
    commands = [
        [
            "research",
            "strategies",
            "simple-baseline-registry-review",
            "--output-root",
            str(output_root),
        ],
        ["research", "strategies", "qqq-sgov-baseline-backtest", *data_args],
        ["research", "strategies", "tqqq-sgov-risk-controlled-baseline", *data_args],
        ["research", "strategies", "trend-vol-allocation-policy-search", *data_args],
        [
            "research",
            "strategies",
            "simple-baseline-dominance-ranking",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "simple-baseline-pit-boundary-audit",
            "--output-root",
            str(output_root),
        ],
        ["research", "strategies", "simple-baseline-cost-sensitivity", *data_args],
        ["research", "strategies", "simple-baseline-regime-review", *data_args],
        ["research", "strategies", "simple-baseline-forward-aging-tracker", *data_args],
        [
            "research",
            "strategies",
            "simple-baseline-paper-shadow-readiness",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "daily-reader-portfolio-control-safety-summary",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "simple-baseline-portfolio-dry-run-mapper",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "simple-baseline-master-review",
            "--output-root",
            str(output_root),
            "--docs-path",
            str(docs_path),
        ],
        ["research", "strategies", "options-next-stage-gate", "--output-root", str(output_root)],
    ]
    for command in commands:
        result = runner.invoke(app, command)
        assert result.exit_code == 0, result.output

    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    registered = {item["report_id"] for item in registry["reports"]}
    assert SIMPLE_BASELINE_REPORT_IDS <= registered
    daily_entry = next(
        item
        for item in registry["reports"]
        if item["report_id"] == "daily_reader_portfolio_control_safety_summary"
    )
    assert daily_entry["include_in_reader_brief"] is True
    assert daily_entry["required_for_daily_reading"] is False


def test_reader_brief_renders_portfolio_control_research_summary(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    artifact_path = (
        tmp_path
        / "outputs"
        / "research_strategies"
        / "simple_baselines"
        / "daily_reader_portfolio_control_safety_summary.json"
    )
    _write_json(
        artifact_path,
        {
            "report_type": "daily_reader_portfolio_control_safety_summary",
            "status": "DAILY_SUMMARY_SAFE",
            "production_effect": "none",
            "broker_action": "none",
            "portfolio_control_research_status": {
                "top_simple_baseline_candidate": "qqq_80_sgov_20",
                "current_research_only_target_weights": {"QQQ": 0.8, "SGOV": 0.2},
                "promotion_allowed": False,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
                "major_blockers": [{"reason": "manual owner review still required"}],
            },
        },
    )
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)

    summary = reader_brief._portfolio_control_research_summary()
    html = render_reader_brief_html({"portfolio_control_research": summary})

    assert summary["status"] == "DAILY_SUMMARY_SAFE"
    assert summary["top_simple_baseline_candidate"] == "qqq_80_sgov_20"
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert summary["major_blocker_count"] == 1
    assert "Portfolio Control Research" in html
    assert "qqq_80_sgov_20" in html
    assert "broker_action" in html


def _write_simple_baseline_caches(tmp_path: Path) -> tuple[Path, Path, Path]:
    dates = _business_dates(date(2022, 12, 1), 420)
    prices_path = tmp_path / "prices_daily.csv"
    marketstack_path = tmp_path / "prices_marketstack_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    price_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    secondary_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    levels = {"QQQ": 280.0, "TQQQ": 22.0, "SGOV": 100.0}
    for day_index, row_date in enumerate(dates):
        qqq_return = 0.0008 + 0.002 * math.sin(day_index / 17.0)
        if 145 <= day_index <= 165:
            qqq_return -= 0.004
        if 260 <= day_index <= 275:
            qqq_return -= 0.003
        levels["QQQ"] *= 1.0 + qqq_return
        levels["TQQQ"] *= 1.0 + qqq_return * 3.0 - 0.0002
        levels["SGOV"] *= 1.0 + 0.00015
        for ticker in ("QQQ", "TQQQ", "SGOV"):
            close = levels[ticker]
            row = (
                f"{row_date.isoformat()},{ticker},{close * 0.999:.4f},{close * 1.002:.4f},"
                f"{close * 0.998:.4f},{close:.4f},{close:.4f},{1000000 + day_index}\n"
            )
            price_rows.append(row)
            secondary_rows.append(row)
    rate_rows = ["date,series,value\n"]
    for day_index, row_date in enumerate(dates):
        rate_rows.append(f"{row_date.isoformat()},DGS2,{4.0 + day_index * 0.0005:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DGS10,{4.2 + day_index * 0.0004:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DTWEXBGS,{120.0 + day_index * 0.01:.4f}\n")
    prices_path.write_text("".join(price_rows), encoding="utf-8")
    marketstack_path.write_text("".join(secondary_rows), encoding="utf-8")
    rates_path.write_text("".join(rate_rows), encoding="utf-8")
    return prices_path, marketstack_path, rates_path


def _business_dates(start: date, count: int) -> list[date]:
    values: list[date] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(current)
        current += timedelta(days=1)
    return values


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
