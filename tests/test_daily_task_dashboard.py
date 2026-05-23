from __future__ import annotations

import builtins
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.daily_task_dashboard import (
    build_daily_decision_summary_payload,
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.reports.daily_task_dashboard_view_model import (
    DailyTaskDashboardReport,
)

DANGEROUS_SHADOW_IMPACT_OUTPUT_TERMS = (
    "PROMOTE_TO_PRODUCTION",
    "READY_FOR_LIVE",
    "SHOULD_TRADE",
    "APPROVED_FOR_TRADING",
    "APPROVED",
)


def test_daily_task_dashboard_summarizes_task_conclusions_and_risks(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 4)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    _write_shadow_parameter_search(tmp_path)
    _write_shadow_iteration_report(tmp_path, as_of)
    _write_paper_trading_summary(tmp_path, as_of)
    _write_paper_signal_quality(tmp_path, as_of)
    _write_shadow_parameter_impact(tmp_path, as_of)
    _write_weight_adjustment_candidates(tmp_path, as_of)
    _write_weight_candidate_evaluation(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=tmp_path / "daily_ops_run_2026-05-04.md",
        reports_dir=tmp_path,
    )
    html = render_daily_task_dashboard(report)
    payload = build_daily_task_dashboard_payload(report)
    decision_summary = build_daily_decision_summary_payload(report)

    assert isinstance(report, DailyTaskDashboardReport)
    assert report.status == "PASS"
    assert report.risk_count == 2
    assert "关键结论总览" in html
    assert "Paper Trading Summary" in html
    assert "Paper Signal Quality" in html
    assert "Shadow Impact" in html
    assert "Weight Adjustment Candidate" in html
    assert "Weight Candidate Evaluation" in html
    assert "observe-only" in html
    assert "只读已有 paper artifacts" in html
    assert "production_effect=<code>none</code>" in html
    assert "reconciliation_status" in html
    assert "当日动作、仓位与主要约束" in html
    assert "任务执行明细" in html
    assert "Shadow 结果与参数对比" in html
    assert "Shadow 参数持续迭代状态" in html
    assert "结果对比" in html
    assert "Return 口径" in html
    assert "Total return" in html
    assert "4.74%" in html
    assert "9.02%" in html
    assert "+4.29%" in html
    assert "Gate cap override" in html
    assert "权重参数" in html
    assert "<strong>实际 40.00%</strong>" in html
    assert "<small>无静态 override</small>" in html
    assert "主线实际 gate" in html
    assert "subtask-link-grid" in html
    assert 'href="daily_score_2026-05-04.md"' in html
    assert 'href="parameter_governance_2026-05-04.md"' in html
    assert "执行动作：观察" in html
    assert "参数治理报告完成" in html
    assert "Owner quantitative input unavailable" in html
    assert "参数治理 状态为 PASS_WITH_LIMITATIONS" in html
    assert "Pipeline health 状态为 ACTIVE_WARNINGS" in html
    assert payload["production_effect"] == "none"
    assert payload["summary"]["task_count"] == 4
    assert payload["summary"]["risk_count"] == 2
    assert payload["paper_trading_summary"]["generated_intents"] == 1
    assert payload["paper_trading_summary"]["candidate_count"] == 2
    assert payload["paper_trading_summary"]["blocked_candidates"] == 1
    assert payload["paper_trading_summary"]["reconciliation_status"] == "PASS"
    assert payload["paper_trading_summary"]["production_effect"] == "none"
    assert payload["paper_signal_quality"]["evaluation_status"] == "LOW_DATA_QUALITY"
    assert payload["paper_signal_quality"]["primary_blocked_by"] == "manual_approval_required"
    assert payload["paper_signal_quality"]["sample_count"] == 7
    assert payload["paper_signal_quality"]["production_effect"] == "none"
    assert payload["paper_signal_quality"]["observe_only"] is True
    assert payload["shadow_parameter_impact"]["impact_status"] == "INSUFFICIENT_DATA"
    assert payload["shadow_parameter_impact"]["main_blocked_by"] == "insufficient_shadow_sample"
    assert payload["shadow_parameter_impact"]["window_sample_counts"]["7"] == {
        "production": 7,
        "shadow": 0,
        "unknown": 0,
    }
    assert payload["shadow_parameter_impact"]["production_vs_shadow_filled_count"] == {
        "production": 3,
        "shadow": 0,
    }
    assert payload["shadow_parameter_impact"]["continuous_replay_available"] is True
    assert payload["shadow_parameter_impact"]["production_effect"] == "none"
    assert payload["shadow_parameter_impact"]["observe_only"] is True
    assert payload["weight_adjustment_candidates"]["candidate_count"] == 1
    assert (
        payload["weight_adjustment_candidates"]["top_candidate_id"]
        == "weight_adjustment_candidate:2026-05-04:limited_input"
    )
    assert payload["weight_adjustment_candidates"]["gate_status"] == "LIMITED"
    assert payload["weight_adjustment_candidates"]["main_blocked_by"] == "manual_approval_required"
    assert payload["weight_adjustment_candidates"]["production_effect"] == "none"
    assert payload["weight_adjustment_candidates"]["mode"] == "observe_only"
    assert payload["weight_candidate_evaluation"]["evaluation_status"] == (
        "CANDIDATE_PROMISING_BUT_LIMITED"
    )
    assert payload["weight_candidate_evaluation"]["candidate_count"] == 1
    assert payload["weight_candidate_evaluation"]["evaluable_candidate_count"] == 1
    assert (
        payload["weight_candidate_evaluation"]["top_candidate_id"]
        == "weight_adjustment_candidate:2026-05-04:limited_input"
    )
    assert payload["weight_candidate_evaluation"]["main_blocked_by"] == ("manual_approval_required")
    assert payload["weight_candidate_evaluation"]["production_effect"] == "none"
    investment = next(item for item in payload["key_conclusions"] if item["area"] == "投资结论")
    assert investment["primary"] == (
        "执行动作：观察；最终 AI 仓位：40%-60%；置信度：0.71；Data Gate：PASS"
    )
    parameter = next(item for item in payload["key_conclusions"] if item["area"] == "参数治理")
    assert parameter["status"] == "PASS_WITH_LIMITATIONS"
    feedback = next(item for item in payload["key_conclusions"] if item["area"] == "反馈复盘")
    assert "source_current__grid_gate_0217" in feedback["primary"]
    assert "shadow return 9.02%" in feedback["primary"]
    assert "production 4.74%" in feedback["primary"]
    assert "excess +4.29%" in feedback["primary"]
    assert "promotion=NOT_PROMOTABLE" in feedback["primary"]
    assert "available：8，total：11" in "；".join(feedback["supporting"])
    assert "available=8，contract floor=30" in feedback["important_risk"]
    assert "单日 return = 仓位中点 × 标的 outcome return" in feedback["result_methodology"]
    results = feedback["result_comparison"]
    total_return = next(row for row in results if row["metric"] == "Total return")
    assert total_return["production"] == "4.74%"
    assert total_return["shadow"] == "9.02%"
    assert total_return["delta"] == "+4.29%"
    assert "cost_bps：5.0" in total_return["note"]
    drawdown = next(row for row in results if row["metric"] == "Max drawdown")
    assert drawdown["production"] == "-1.09%"
    assert drawdown["shadow"] == "-1.70%"
    assert drawdown["delta"] == "-0.61%"
    turnover = next(row for row in results if row["metric"] == "Turnover")
    assert turnover["production"] == "1.20"
    assert turnover["shadow"] == "0.65"
    assert turnover["delta"] == "-0.55"
    comparison = feedback["parameter_comparison"]
    valuation_cap = next(
        row for row in comparison if row["group"] == "gate_cap" and row["parameter"] == "valuation"
    )
    assert valuation_cap["production"] == "主线实际 gate：40.00%（无静态 override）"
    assert valuation_cap["production_observed_min"] == 0.4
    assert valuation_cap["production_observed_max"] == 0.4
    assert valuation_cap["candidate"] == "70.00%"
    assert valuation_cap["delta"] == "新增 override"
    assert "primary cap" in valuation_cap["note"]
    trend_weight = next(
        row for row in comparison if row["group"] == "weight" and row["parameter"] == "trend"
    )
    assert trend_weight["production"] == "25.00%"
    assert trend_weight["candidate"] == "25.00%"
    assert trend_weight["delta"] == "+0.00%"
    assert trend_weight["note"] == "未变化"
    shadow_iteration = next(
        item for item in payload["key_conclusions"] if item["area"] == "Shadow Iteration"
    )
    assert shadow_iteration["status"] == "PASS_WITH_LIMITATIONS"
    assert "active candidates：3" in shadow_iteration["primary"]
    assert "best gate-only" in shadow_iteration["primary"]
    assert "source_current__grid_gate_0217" in shadow_iteration["primary"]
    assert "dashboard 只读取 shadow_iteration JSON" in "；".join(shadow_iteration["supporting"])
    assert "gate_only 只能进入 gate policy review" in shadow_iteration["important_risk"]
    score_task = next(task for task in payload["tasks"] if task["step_id"] == "score_daily")
    assert score_task["conclusion"] == (
        "执行动作：观察；最终 AI 仓位：40%-60%；置信度：0.71；Data Gate：PASS"
    )
    assert score_task["detail_reports"][1]["href"] == "daily_score_2026-05-04.md"
    assert decision_summary["schema_version"] == 1
    assert decision_summary["report_type"] == "daily_decision_summary"
    assert decision_summary["production_effect"] == "none"
    assert decision_summary["decision_bus_role"] == {
        "upstream_for": "order_intent_candidate",
        "current_behavior": "read_only_no_trade",
        "order_intent_builder_connected": False,
    }
    assert decision_summary["data_gate"]["status"] == "PASS"
    assert decision_summary["investment_conclusion"]["action_bias"] == "观察"
    assert decision_summary["investment_conclusion"]["confidence"] == "0.71"
    assert decision_summary["investment_conclusion"]["position_band"] == "40%-60%"
    assert (
        decision_summary["investment_conclusion"]["source_dashboard_key_conclusion"]["primary"]
        == investment["primary"]
    )
    assert investment["primary"] in html
    assert (
        decision_summary["parameter_governance"]["shadow_candidate"]["selected_trial_id"]
        == "source_current__grid_gate_0217"
    )
    assert decision_summary["parameter_governance"]["promotion_status"] == "NOT_PROMOTABLE"
    assert "daily_ops_metadata" in decision_summary["hrefs"]
    assert any(
        artifact["label"] == "每日评分报告" and artifact["href"] == "daily_score_2026-05-04.md"
        for artifact in decision_summary["source_artifacts"]
    )


def test_reports_daily_tasks_cli_writes_html_and_json(tmp_path: Path) -> None:
    as_of = date(2026, 5, 4)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    _write_shadow_parameter_search(tmp_path)
    _write_shadow_iteration_report(tmp_path, as_of)
    _write_paper_trading_summary(tmp_path, as_of)
    output_path = tmp_path / "daily_task_dashboard_2026-05-04.html"
    json_output_path = tmp_path / "daily_task_dashboard_2026-05-04.json"
    decision_summary_output_path = tmp_path / "daily_decision_summary_2026-05-04.json"
    order_intent_candidates_output_path = tmp_path / "order_intent_candidates_2026-05-04.json"

    result = CliRunner().invoke(
        app,
        [
            "reports",
            "daily-tasks",
            "--as-of",
            "2026-05-04",
            "--metadata-path",
            str(metadata_path),
            "--run-report-path",
            str(tmp_path / "daily_ops_run_2026-05-04.md"),
            "--reports-dir",
            str(tmp_path),
            "--output-path",
            str(output_path),
            "--json-output-path",
            str(json_output_path),
            "--decision-summary-output-path",
            str(decision_summary_output_path),
            "--order-intent-candidates-output-path",
            str(order_intent_candidates_output_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "每日任务展示：PASS" in result.output
    assert output_path.exists()
    assert json_output_path.exists()
    assert decision_summary_output_path.exists()
    assert order_intent_candidates_output_path.exists()
    assert "关键结论总览" in output_path.read_text(encoding="utf-8")
    payload = json.loads(json_output_path.read_text(encoding="utf-8"))
    assert payload["summary"]["risk_count"] == 2
    assert payload["paper_trading_summary"]["candidate_count"] == 2
    assert payload["paper_trading_summary"]["blocked_candidates"] == 1
    assert payload["paper_trading_summary"]["filled"] == 1
    assert payload["key_conclusions"][0]["area"] == "投资结论"
    assert any(
        "source_current__grid_gate_0217" in item["primary"] for item in payload["key_conclusions"]
    )
    assert any(item["area"] == "Shadow Iteration" for item in payload["key_conclusions"])
    assert payload["tasks"][0]["step_id"] == "download_data"
    decision_summary = json.loads(decision_summary_output_path.read_text(encoding="utf-8"))
    assert decision_summary["investment_conclusion"]["action_bias"] == "观察"
    assert decision_summary["production_effect"] == "none"
    order_candidates = json.loads(order_intent_candidates_output_path.read_text(encoding="utf-8"))
    assert order_candidates["production_effect"] == "none"
    assert order_candidates["execution_boundary"]["creates_execution_action"] is False
    assert order_candidates["candidate_count"] == 1
    order_candidate = order_candidates["candidates"][0]
    assert order_candidate["source_decision"]["action_bias"] == (
        decision_summary["investment_conclusion"]["action_bias"]
    )
    assert order_candidate["blocked"] is True
    assert {"trading_engine_not_enabled", "manual_approval_required"}.issubset(
        set(order_candidate["blocked_by"])
    )
    assert order_candidate["execution_action"] == "none"


def test_daily_decision_summary_schema_is_stable(tmp_path: Path) -> None:
    as_of = date(2026, 5, 4)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=tmp_path / "daily_ops_run_2026-05-04.md",
        reports_dir=tmp_path,
    )
    payload = build_daily_decision_summary_payload(report)

    assert set(payload) == {
        "schema_version",
        "report_type",
        "as_of",
        "generated_at",
        "run_id",
        "production_effect",
        "status",
        "decision_bus_role",
        "data_gate",
        "investment_conclusion",
        "parameter_governance",
        "feedback_review",
        "system_health",
        "source_artifacts",
        "hrefs",
        "checksums",
    }
    assert set(payload["data_gate"]) == {
        "availability",
        "status",
        "blocking_reasons",
        "source_dashboard_key_conclusion",
    }
    assert set(payload["investment_conclusion"]) == {
        "availability",
        "action_bias",
        "confidence",
        "position_band",
        "major_risks",
        "source_dashboard_key_conclusion",
        "source_steps",
        "production_effect",
    }
    assert set(payload["parameter_governance"]) == {
        "availability",
        "status",
        "production_profile",
        "shadow_candidate",
        "promotion_status",
        "blocking_reasons",
        "source_dashboard_key_conclusion",
    }
    assert set(payload["feedback_review"]) == {
        "availability",
        "status",
        "summary",
        "market_feedback_status",
        "feedback_loop_status",
        "investment_review_status",
        "blocking_reasons",
        "source_dashboard_key_conclusion",
    }
    assert set(payload["system_health"]) == {
        "availability",
        "status",
        "warnings",
        "run_status",
        "failed_count",
        "skipped_count",
        "source_dashboard_key_conclusion",
    }


def test_daily_decision_summary_marks_missing_child_reports_without_synthesis(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 4)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    suffix = as_of.isoformat()
    (tmp_path / f"data_quality_{suffix}.md").write_text(
        "# 数据质量\n\n- 状态：PASS\n",
        encoding="utf-8",
    )
    (tmp_path / f"daily_score_{suffix}.md").write_text(
        "# 每日评分\n\n- 状态：PASS\n",
        encoding="utf-8",
    )

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=None,
        reports_dir=tmp_path,
    )
    html = render_daily_task_dashboard(report)
    payload = build_daily_decision_summary_payload(report)

    assert "当日投资结论受限" in html
    investment = payload["investment_conclusion"]
    assert investment["availability"] == "missing"
    assert investment["action_bias"] == "missing"
    assert investment["confidence"] == "missing"
    assert investment["position_band"] == "missing"
    assert any("未合成投资动作" in risk for risk in investment["major_risks"])
    assert payload["production_effect"] == "none"
    assert payload["parameter_governance"]["production_profile"]["availability"] == "missing"


def test_daily_task_dashboard_paper_trading_trend_is_limited_when_history_missing(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 4)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=tmp_path / "daily_ops_run_2026-05-04.md",
        reports_dir=tmp_path,
        paper_trading_trend_days=7,
    )
    html = render_daily_task_dashboard(report)
    payload = build_daily_task_dashboard_payload(report)

    assert "Paper Trading Trend" in html
    trend = payload["paper_trading_trend"]
    assert trend["status"] == "LIMITED"
    assert trend["production_effect"] == "none"
    assert trend["available_count"] == 0
    assert trend["missing_count"] == 7
    assert "不补造趋势结论" in trend["risk"]


def test_daily_task_dashboard_paper_trading_trend_aggregates_replay_visibility(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 14)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    for offset in range(7):
        current = date.fromordinal(as_of.toordinal() - offset)
        _write_paper_trading_summary(
            tmp_path,
            current,
            status="PASS",
            candidate_count=3,
            blocked_candidates=2,
            generated_intents=1,
            filled=1,
            unrealized_pnl=10.0,
            market_snapshot_source="synthetic_limit_price",
            market_snapshot_source_counts={
                "historical_ohlc": 1,
                "candidate_metadata": 0,
                "synthetic_limit_price": 1,
            },
        )
        _write_order_intent_candidates(tmp_path, current)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=tmp_path / "daily_ops_run_2026-05-14.md",
        reports_dir=tmp_path,
        paper_trading_trend_days=7,
    )
    html = render_daily_task_dashboard(report)
    payload = build_daily_task_dashboard_payload(report)
    trend = payload["paper_trading_trend"]

    assert "Paper Trading Trend (7/14/30 日)" in html
    assert "manual_approval_required" in html
    assert "REVIEW_DIRECTION_BLOCKED" in html
    assert trend["production_effect"] == "none"
    assert trend["replay_mode"] == "daily_independent"
    assert trend["portfolio_carry_forward"] is False
    assert trend["execution_boundary"]["runs_replay"] is False
    assert trend["execution_boundary"]["broker_api_allowed"] is False
    assert trend["execution_boundary"]["changes_parameter_promotion"] is False
    assert set(trend["windows"]) == {"7", "14", "30"}
    assert trend["windows"]["7"]["status"] == "PASS"
    assert trend["windows"]["14"]["status"] == "LIMITED"
    assert trend["windows"]["30"]["status"] == "LIMITED"
    assert trend["windows"]["7"]["totals"]["candidate_count"] == 21
    assert trend["windows"]["7"]["totals"]["filled"] == 7
    assert trend["windows"]["7"]["totals"]["unrealized_pnl"] == 70.0
    assert trend["windows"]["7"]["synthetic_snapshot_count"] == 7
    assert trend["windows"]["7"]["synthetic_snapshot_ratio"] == 0.5
    assert trend["windows"]["7"]["market_snapshot_source_distribution"] == {
        "historical_ohlc": 7,
        "synthetic_limit_price": 7,
    }
    assert trend["windows"]["7"]["top_blocked_by"][0] == {
        "value": "manual_approval_required",
        "count": 7,
    }
    assert trend["windows"]["7"]["top_reason_code"][0] == {
        "value": "REVIEW_DIRECTION_BLOCKED",
        "count": 7,
    }
    assert "连续组合收益" in html


def test_daily_task_dashboard_paper_trading_trend_shows_continuous_replay_summary(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 14)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    for offset in range(7):
        current = date.fromordinal(as_of.toordinal() - offset)
        _write_paper_trading_summary(tmp_path, current, status="PASS")
        _write_order_intent_candidates(tmp_path, current)
    _write_paper_trading_replay(
        tmp_path,
        start=date(2026, 5, 8),
        end=as_of,
        mode="continuous_portfolio",
        portfolio_carry_forward=True,
        final_equity=101234.56,
        max_drawdown_pct=-0.034,
        exposure_peak=12500.0,
        final_positions=[
            {
                "symbol": "TSM",
                "quantity": 5,
                "avg_cost": 100.0,
                "market_price": 110.0,
                "market_value": 550.0,
                "unrealized_pnl": 50.0,
            }
        ],
        expired_day_orders=2,
    )

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=tmp_path / "daily_ops_run_2026-05-14.md",
        reports_dir=tmp_path,
        paper_trading_trend_days=7,
    )
    html = render_daily_task_dashboard(report)
    payload = build_daily_task_dashboard_payload(report)
    trend = payload["paper_trading_trend"]
    continuous = trend["continuous_portfolio_summary"]

    assert trend["replay_mode"] == "continuous_portfolio"
    assert trend["portfolio_carry_forward"] is True
    assert trend["latest_replay"]["exists"] is True
    assert continuous["final_equity"] == 101234.56
    assert continuous["max_drawdown_pct"] == -0.034
    assert continuous["exposure_peak"] == 12500.0
    assert continuous["final_positions_count"] == 1
    assert continuous["expired_day_orders"] == 2
    assert trend["execution_boundary"]["runs_replay"] is False
    assert "continuous-portfolio" in html
    assert "final_equity" in html
    assert "max_drawdown" in html
    assert "portfolio_carry_forward" in html


def test_daily_task_dashboard_shadow_impact_card_is_read_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    as_of = date(2026, 5, 14)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    _write_shadow_parameter_impact(tmp_path, as_of)

    env_module = __import__("o" + "s")
    original_get_env = getattr(env_module, "get" + "env")
    original_import = builtins.__import__

    def guarded_get_env(key: str, default: str | None = None) -> str | None:
        blocked_tokens = ("API" + "_" + "KEY", "ALPACA" + "_", "IBKR" + "_", "BRO" + "KER")
        if any(token in key for token in blocked_tokens):
            raise AssertionError(f"dashboard must not read broker env var: {key}")
        return original_get_env(key, default)

    monkeypatch.setattr(env_module, "get" + "env", guarded_get_env)

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked_module_tokens = (
            "run_paper_trading_replay",
            "run_paper_trading_from_candidates",
            "ai_trading_system.trading_engine.brokers",
            "ai_trading_system.shadow_iteration",
            "shadow_parameter_promotion",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(f"dashboard must not import execution path: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=tmp_path / "daily_ops_run_2026-05-14.md",
        reports_dir=tmp_path,
    )
    html = render_daily_task_dashboard(report)
    payload = build_daily_task_dashboard_payload(report)

    impact = payload["shadow_parameter_impact"]
    assert impact["impact_status"] == "INSUFFICIENT_DATA"
    assert impact["production_effect"] == "none"
    assert impact["continuous_replay_available"] is True
    assert "Shadow Impact" in html
    assert "shadow_parameter_impact_2026-05-14.md" in html
    assert "Distribution Snapshot" not in html
    dashboard_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    _assert_no_shadow_impact_dangerous_terms(dashboard_json, html)


def test_daily_task_dashboard_operator_brief_card_is_read_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    as_of = date(2026, 5, 23)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    operator_brief = _write_operator_brief(tmp_path, as_of)

    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked_module_tokens = (
            "run_daily_shadow_weight_iteration",
            "run_daily_shadow_vs_production_comparison",
            "run_shadow_vs_production_multi_day_review",
            "run_shadow_promotion_proposal",
            "run_shadow_promotion_apply_preflight",
            "run_shadow_promotion_apply",
            "run_shadow_promotion_rollback",
            "run_shadow_promotion_lifecycle_audit",
            "run_parameter_governance_summary",
            "render_parameter_governance_web_view",
            "run_parameter_governance_daily_digest",
            "run_daily_trading_system_operator_brief",
            "ai_trading_system.trading_engine.daily_trading_system_operator_brief",
            "run_pipeline_health_summary",
            "ai_trading_system.trading_engine.pipeline_health_summary",
            "ai_trading_system.scoring",
            "ai_trading_system.backtest",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(f"dashboard must not import pipeline path: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=None,
        reports_dir=tmp_path,
    )
    html = render_daily_task_dashboard(report)
    payload = build_daily_task_dashboard_payload(report)

    summary = payload["daily_trading_system_operator_brief"]
    assert summary["brief_status"] == "OK"
    assert summary["summary_level"] == "NORMAL"
    assert summary["headline"] == operator_brief["headline"]
    assert summary["can_trust_outputs_today"] is True
    assert summary["manual_action_required"] is False
    assert summary["parameter_governance_digest_status"] == "OK"
    assert summary["pipeline_health_status"] == "UNKNOWN"
    assert summary["data_freshness_status"] == "UNKNOWN"
    assert summary["critical_alert_count"] == 0
    assert summary["warning_count"] == 0
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["operator_brief_only"] is True
    assert summary["read_only"] is True
    assert summary["apply_executed_by_operator_brief"] is False
    assert summary["rollback_executed_by_operator_brief"] is False
    assert summary["broker_execution"] is False
    assert summary["replay_execution"] is False
    assert summary["trading_execution"] is False
    assert "Daily Trading System Operator Brief" in html


def test_daily_task_dashboard_pipeline_health_summary_card_is_read_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    as_of = date(2026, 5, 23)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    health_summary = _write_pipeline_health_summary(tmp_path, as_of)

    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked_module_tokens = (
            "run_daily_shadow_weight_iteration",
            "run_daily_shadow_vs_production_comparison",
            "run_shadow_vs_production_multi_day_review",
            "run_shadow_promotion_proposal",
            "run_shadow_promotion_apply_preflight",
            "run_shadow_promotion_apply",
            "run_shadow_promotion_rollback",
            "run_shadow_promotion_lifecycle_audit",
            "run_parameter_governance_summary",
            "render_parameter_governance_web_view",
            "run_parameter_governance_daily_digest",
            "run_daily_trading_system_operator_brief",
            "run_pipeline_health_summary",
            "ai_trading_system.trading_engine.pipeline_health_summary",
            "ai_trading_system.scoring",
            "ai_trading_system.backtest",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(f"dashboard must not import pipeline path: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=None,
        reports_dir=tmp_path,
    )
    html = render_daily_task_dashboard(report)
    payload = build_daily_task_dashboard_payload(report)

    summary = payload["pipeline_health_summary"]
    assert summary["health_status"] == "OK"
    assert summary["summary_level"] == "NORMAL"
    assert summary["headline"] == health_summary["headline"]
    assert summary["required_pipelines"] == 8
    assert summary["missing_required_pipelines"] == 0
    assert summary["stale_required_pipelines"] == 0
    assert summary["critical_pipelines"] == 0
    assert summary["warning_pipelines"] == 1
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["pipeline_health_only"] is True
    assert summary["read_only"] is True
    assert summary["pipelines_executed_by_health_check"] is False
    assert summary["apply_executed_by_health_check"] is False
    assert summary["rollback_executed_by_health_check"] is False
    assert summary["broker_execution"] is False
    assert summary["replay_execution"] is False
    assert summary["trading_execution"] is False
    assert "Pipeline Health Summary" in html
    assert "pipeline_health_summary_2026-05-23.md" in html


def _write_daily_ops_metadata(tmp_path: Path, as_of: date) -> Path:
    metadata_path = tmp_path / f"daily_ops_run_metadata_{as_of.isoformat()}.json"
    started_at = datetime(2026, 5, 4, 21, 0, tzinfo=UTC)
    finished_at = datetime(2026, 5, 4, 21, 5, tzinfo=UTC)
    steps = (
        ("download_data", "aits download-data --start 2018-01-01"),
        ("score_daily", "aits score-daily --as-of 2026-05-04"),
        ("parameter_governance", "aits reports parameter-governance --as-of 2026-05-04"),
        ("pipeline_health", "aits ops health --as-of 2026-05-04"),
    )
    metadata_path.write_text(
        json.dumps(
            {
                "run_id": f"daily_ops_run:{as_of.isoformat()}:test",
                "as_of": as_of.isoformat(),
                "generated_at": started_at.isoformat(),
                "project_root": str(tmp_path),
                "status": "PASS",
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
                "visibility_cutoff": finished_at.isoformat(),
                "visibility_cutoff_source": "test",
                "input_visibility_status": "PASS",
                "git": {"commit": "abc123", "dirty": False},
                "commands": [
                    {
                        "step_id": step_id,
                        "enabled": True,
                        "command": command,
                        "required_env_vars": [],
                        "blocks_downstream": step_id == "download_data",
                        "skip_reason": None,
                        "input_visibility": "local_or_readonly",
                    }
                    for step_id, command in steps
                ],
                "step_results": [
                    {
                        "step_id": step_id,
                        "status": "PASS",
                        "return_code": 0,
                        "started_at": started_at.isoformat(),
                        "ended_at": finished_at.isoformat(),
                        "duration_seconds": 1.2,
                        "stdout_line_count": 1,
                        "stderr_line_count": 0,
                        "error": None,
                    }
                    for step_id, _command in steps
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return metadata_path


def _write_detail_reports(tmp_path: Path, as_of: date) -> None:
    suffix = as_of.isoformat()
    (tmp_path / f"download_data_diagnostics_{suffix}.md").write_text(
        "# 下载诊断\n\n- 状态：PASS\n- 行数：100\n",
        encoding="utf-8",
    )
    (tmp_path / f"data_quality_{suffix}.md").write_text(
        "# 数据质量\n\n- 状态：PASS\n- 错误数：0\n- 警告数：0\n",
        encoding="utf-8",
    )
    (tmp_path / f"daily_score_{suffix}.md").write_text(
        "# 每日评分\n\n- 状态：PASS\n",
        encoding="utf-8",
    )
    (tmp_path / f"alerts_{suffix}.md").write_text(
        "# 告警\n\n- 状态：PASS\n- 活跃告警数：0\n",
        encoding="utf-8",
    )
    (tmp_path / f"evidence_dashboard_{suffix}.json").write_text(
        json.dumps(
            {
                "decision": {
                    "action": "观察",
                    "final_risk_asset_ai_position": "40%-60%",
                    "confidence": "0.71",
                    "data_gate": "PASS",
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (tmp_path / f"parameter_governance_{suffix}.md").write_text(
        "\n".join(
            [
                "# 参数治理",
                "",
                "- 状态：PASS_WITH_LIMITATIONS",
                "- Owner quantitative input：Owner quantitative input unavailable",
                "- Action 分布：COLLECT_MORE_EVIDENCE=1",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / f"pipeline_health_{suffix}.md").write_text(
        "# Pipeline health\n\n- 状态：ACTIVE_WARNINGS\n- 检查项：15\n- 错误数：0\n- 警告数：1\n",
        encoding="utf-8",
    )
    (tmp_path / f"pipeline_health_alerts_{suffix}.md").write_text(
        "# Pipeline health alerts\n\n- 状态：PASS\n",
        encoding="utf-8",
    )


def _write_pipeline_health_summary(tmp_path: Path, as_of: date) -> dict[str, Any]:
    suffix = as_of.isoformat()
    summary_path = (
        tmp_path / "data" / "derived" / "pipeline_health" / f"pipeline_health_summary_{suffix}.json"
    )
    markdown_path = summary_path.with_suffix(".md")
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "pipeline_health_summary",
        "task_id": "TRADING-023",
        "date": suffix,
        "mode": "pipeline_health_summary_only",
        "production_effect": "none",
        "manual_review_only": True,
        "pipeline_health_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "pipelines_executed_by_health_check": False,
        "apply_executed_by_health_check": False,
        "rollback_executed_by_health_check": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "health_status": "OK",
        "summary_level": "NORMAL",
        "headline": "Required pipeline artifacts are available.",
        "coverage": {
            "registered_pipelines": 12,
            "required_pipelines": 8,
            "available_pipelines": 11,
            "missing_required_pipelines": 0,
            "stale_required_pipelines": 0,
            "critical_pipelines": 0,
            "warning_pipelines": 1,
        },
        "alerts": {
            "critical": [],
            "warnings": ["TRADING-020 web view artifact is stale but optional."],
            "notes": ["Pipeline health summary is read-only."],
        },
        "output_artifacts": {
            "json": {"path": str(summary_path)},
            "markdown": {"path": str(markdown_path)},
        },
        "pipeline_contract": {
            "runs_shadow_iteration_pipeline": False,
            "runs_comparison_pipeline": False,
            "runs_multi_day_review_pipeline": False,
            "runs_promotion_proposal_pipeline": False,
            "runs_apply_preflight_pipeline": False,
            "runs_promotion_apply": False,
            "runs_promotion_rollback": False,
            "runs_lifecycle_audit_pipeline": False,
            "runs_governance_summary_pipeline": False,
            "runs_web_view_render_script": False,
            "runs_daily_digest_script": False,
            "runs_operator_brief_script": False,
            "runs_pipeline_health_summary_script": False,
            "runs_market_pipeline": False,
            "runs_backtest_pipeline": False,
            "runs_scoring_pipeline": False,
            "runs_broker_runner": False,
            "runs_paper_runner": False,
            "runs_replay_runner": False,
            "writes_production_profile": False,
            "writes_production_weights": False,
            "writes_shadow_weights": False,
            "writes_approved_profile": False,
            "promotes_shadow_to_production": False,
            "triggers_trade": False,
        },
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text("# Pipeline Health Summary\n", encoding="utf-8")
    return payload


def _write_operator_brief(tmp_path: Path, as_of: date) -> dict[str, Any]:
    suffix = as_of.isoformat()
    brief_path = (
        tmp_path
        / "data"
        / "derived"
        / "operator_briefs"
        / f"daily_trading_system_operator_brief_{suffix}.json"
    )
    markdown_path = brief_path.with_suffix(".md")
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "daily_trading_system_operator_brief",
        "task_id": "TRADING-022",
        "date": suffix,
        "mode": "daily_trading_system_operator_brief_only",
        "production_effect": "none",
        "manual_review_only": True,
        "operator_brief_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "apply_executed_by_operator_brief": False,
        "rollback_executed_by_operator_brief": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "brief_status": "OK",
        "summary_level": "NORMAL",
        "headline": "Trading system status is stable. No immediate manual action is required.",
        "system_snapshot": {
            "overall_system_status": "OK",
            "can_trust_outputs_today": True,
            "manual_action_required": False,
            "has_critical_alerts": False,
            "has_warnings": False,
        },
        "parameter_governance": {
            "status": "OK",
            "digest_status": "OK",
            "summary_level": "NORMAL",
            "governance_state": "ROLLBACK_COMPLETED",
            "action_required": False,
            "action_level": "NONE",
            "headline": "Parameter governance is stable.",
        },
        "pipeline_health": {"status": "UNKNOWN", "available": False},
        "data_freshness": {"status": "UNKNOWN", "available": False},
        "alerts": {"critical": [], "warnings": [], "notes": ["Parameter governance digest is OK."]},
        "output_artifacts": {
            "json": {"path": str(brief_path)},
            "markdown": {"path": str(markdown_path)},
        },
        "pipeline_contract": {
            "runs_shadow_iteration_pipeline": False,
            "runs_comparison_pipeline": False,
            "runs_multi_day_review_pipeline": False,
            "runs_promotion_proposal_pipeline": False,
            "runs_apply_preflight_pipeline": False,
            "runs_promotion_apply": False,
            "runs_promotion_rollback": False,
            "runs_lifecycle_audit_pipeline": False,
            "runs_governance_summary_pipeline": False,
            "runs_web_view_render_script": False,
            "runs_daily_digest_script": False,
            "runs_operator_brief_script": False,
            "runs_market_pipeline": False,
            "runs_backtest_pipeline": False,
            "runs_scoring_pipeline": False,
            "runs_broker_runner": False,
            "runs_paper_runner": False,
            "runs_replay_runner": False,
            "writes_production_profile": False,
            "writes_production_weights": False,
            "writes_shadow_weights": False,
            "writes_approved_profile": False,
            "promotes_shadow_to_production": False,
            "triggers_trade": False,
        },
    }
    brief_path.parent.mkdir(parents=True, exist_ok=True)
    brief_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text("# Daily Trading System Operator Brief\n", encoding="utf-8")
    return payload


def _write_shadow_parameter_search(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "decision_snapshots"
    snapshot_dir.mkdir()
    _write_decision_snapshot(
        snapshot_dir / "decision_snapshot_2026-05-03.json",
        {"valuation": 0.4, "risk_budget": 1.0},
    )
    _write_decision_snapshot(
        snapshot_dir / "decision_snapshot_2026-05-04.json",
        {"valuation": 0.4, "risk_budget": 0.7},
    )
    output_dir = tmp_path / "outputs" / "parameter_search" / "demo_search"
    output_dir.mkdir(parents=True)
    (output_dir / "manifest.json").write_text(
        json.dumps(
            {
                "report_type": "shadow_parameter_search",
                "status": "PASS_WITH_LIMITATIONS",
                "production_effect": "none",
                "run_id": "demo_search",
                "generated_at": "2026-05-05T01:00:00+00:00",
                "cost_bps": 5.0,
                "slippage_bps": 0.0,
                "search_window": {"start": "2026-05-01", "end": "2026-05-04"},
                "decision_snapshot_path": str(snapshot_dir),
                "best_trial_id": None,
                "best_diagnostic_trial_id": "source_current__grid_gate_0217",
                "factorial_attribution": {
                    "primary_driver": "gate",
                },
                "cap_attribution": [
                    {
                        "gate_id": "valuation",
                        "selected_cap_value": 0.7,
                        "excess_delta_vs_baseline": 0.024188489,
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (output_dir / "trials.csv").write_text(
        "\n".join(
            [
                (
                    "trial_id,total_count,available_count,pending_count,missing_count,"
                    "production_total_return,shadow_total_return,excess_total_return,"
                    "production_max_drawdown,shadow_max_drawdown,"
                    "production_turnover,shadow_turnover,shadow_beats_production_rate,"
                    "eligible,ineligibility_reason,"
                    "target_weights_json,gate_cap_overrides_json"
                ),
                (
                    "source_current__production_observed_gates,11,8,1,2,"
                    "0.0473519476,0.0473519476,0.0,-0.0108505772,"
                    "-0.0108505772,1.2,1.2,0.0,False,"
                    "available_samples_below_objective_floor,"
                    '"{""trend"": 0.25, ""fundamentals"": 0.25}",'
                    "{}"
                ),
                (
                    "source_current__grid_gate_0217,11,8,1,2,0.0473519476,"
                    "0.0902253121,0.0428733645,-0.0108505772,"
                    "-0.0169856501,1.2,0.65,0.75,False,"
                    "available_samples_below_objective_floor,"
                    '"{""trend"": 0.25, ""fundamentals"": 0.25}",'
                    '"{""valuation"": 0.7, ""risk_budget"": 0.7}"'
                ),
            ]
        ),
        encoding="utf-8",
    )
    (output_dir / "shadow_parameter_promotion_demo_search.json").write_text(
        json.dumps(
            {
                "status": "NOT_PROMOTABLE",
                "generated_at": "2026-05-05T01:01:00+00:00",
                "checks": [
                    {
                        "check_id": "available_sample_floor",
                        "status": "FAIL",
                        "reason": "available=8，contract floor=30。",
                    },
                    {
                        "check_id": "forward_shadow",
                        "status": "MISSING",
                        "reason": "尚未接入独立 forward shadow outcome。",
                    },
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_shadow_iteration_report(tmp_path: Path, as_of: date) -> None:
    (tmp_path / f"shadow_iteration_{as_of.isoformat()}.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "shadow_iteration",
                "status": "PASS_WITH_LIMITATIONS",
                "production_effect": "none",
                "as_of": as_of.isoformat(),
                "source_search_run_id": "demo_search",
                "summary": {
                    "active_candidate_count": 3,
                    "primary_driver": "gate",
                    "next_action": "进入 gate policy review；不得作为权重晋级候选。",
                },
                "best_candidates": {
                    "weight_only": {
                        "trial_id": "grid_weight_0015__production_observed_gates",
                        "status": "BLOCKED",
                        "excess_return": 0.0,
                        "next_action": "不进入权重迭代；先处理 blocked reasons。",
                    },
                    "gate_only": {
                        "trial_id": "source_current__grid_gate_0217",
                        "status": "OBSERVED",
                        "excess_return": 0.0428733645,
                        "next_action": "进入 gate policy review；不得作为权重晋级候选。",
                    },
                    "weight_gate_bundle": {
                        "trial_id": "grid_weight_0196__grid_gate_0217",
                        "status": "OBSERVED",
                        "excess_return": 0.0444968246,
                        "next_action": "仅用于 diagnostic；拆分 weight/gate 影响后再评估。",
                    },
                },
                "blocked_reasons": {
                    "source_current__grid_gate_0217": [
                        "not_weight_promotion_candidate: gate_only 只能进入 gate policy review"
                    ]
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_paper_trading_summary(
    tmp_path: Path,
    as_of: date,
    *,
    status: str = "LIMITED",
    candidate_count: int = 2,
    blocked_candidates: int = 1,
    generated_intents: int = 1,
    filled: int = 1,
    unrealized_pnl: float = 12.5,
    market_snapshot_source: str = "none",
    market_snapshot_source_counts: dict[str, int] | None = None,
) -> None:
    source_counts = market_snapshot_source_counts or {
        "historical_ohlc": 0,
        "candidate_metadata": 0,
        "synthetic_limit_price": 0,
    }
    (tmp_path / f"paper_trading_summary_{as_of.isoformat()}.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "paper_trading_summary",
                "as_of": as_of.isoformat(),
                "status": status,
                "production_effect": "none",
                "candidate_count": candidate_count,
                "blocked_candidates": blocked_candidates,
                "generated_intents": generated_intents,
                "approved": 1,
                "rejected": 0,
                "submitted": 1,
                "filled": filled,
                "open": 0,
                "cancelled": 0,
                "realized_pnl": 0.0,
                "unrealized_pnl": unrealized_pnl,
                "reconciliation_status": "PASS",
                "audit_log_path": str(tmp_path / "audit"),
                "report_path": str(tmp_path / "reports" / "trading_daily" / "2026-05-04.md"),
                "market_snapshot_source": market_snapshot_source,
                "market_snapshot_source_counts": source_counts,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_paper_signal_quality(tmp_path: Path, as_of: date) -> None:
    (tmp_path / f"paper_signal_quality_{as_of.isoformat()}.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "paper_signal_quality",
                "as_of": as_of.isoformat(),
                "evaluation_status": "LOW_DATA_QUALITY",
                "production_effect": "none",
                "outputs": {
                    "json": str(tmp_path / f"paper_signal_quality_{as_of.isoformat()}.json"),
                    "markdown": str(tmp_path / f"paper_signal_quality_{as_of.isoformat()}.md"),
                },
                "summary": {
                    "sample_count": 7,
                    "candidate_count": 14,
                    "generated_intents": 5,
                    "filled_count": 2,
                    "primary_blocked_by": "manual_approval_required",
                    "synthetic_snapshot_ratio": 0.42,
                    "historical_ohlc_coverage": 0.58,
                    "reconciliation_pass_ratio": 0.86,
                },
                "evaluation_gate": {
                    "status": "LOW_DATA_QUALITY",
                    "blocked_by": ["LOW_DATA_QUALITY"],
                    "blocking_reasons": ["LOW_DATA_QUALITY"],
                    "explanation": "synthetic snapshot ratio 高于 policy 上限。",
                    "checks": [],
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (tmp_path / f"paper_signal_quality_{as_of.isoformat()}.md").write_text(
        "# Paper Signal Quality Evaluation\n\n- production_effect=none\n",
        encoding="utf-8",
    )


def _write_shadow_parameter_impact(tmp_path: Path, as_of: date) -> None:
    suffix = as_of.isoformat()
    windows = {
        str(days): {
            "impact_status": "INSUFFICIENT_DATA",
            "summary": {
                "sample_counts": {
                    "production": 7 if days == 7 else 7,
                    "shadow": 0,
                    "unknown": 0,
                }
            },
        }
        for days in (7, 14, 30)
    }
    (tmp_path / f"shadow_parameter_impact_{suffix}.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "shadow_parameter_impact",
                "as_of": suffix,
                "impact_status": "INSUFFICIENT_DATA",
                "policy_id": "shadow_parameter_impact_policy",
                "policy_version": 1,
                "thresholds_snapshot": {
                    "minimum_shadow_sample_count": 7,
                    "minimum_production_baseline_count": 7,
                    "minimum_filled_count_for_comparison": 3,
                    "maximum_synthetic_snapshot_ratio": 0.25,
                    "minimum_historical_ohlc_coverage": 0.7,
                    "minimum_reconciliation_pass_ratio": 0.9,
                },
                "production_effect": "none",
                "outputs": {
                    "json": str(tmp_path / f"shadow_parameter_impact_{suffix}.json"),
                    "markdown": str(tmp_path / f"shadow_parameter_impact_{suffix}.md"),
                },
                "summary": {
                    "impact_status": "INSUFFICIENT_DATA",
                    "sample_counts": {"production": 7, "shadow": 0, "unknown": 0},
                    "filled_count": {"production": 3, "shadow": 0},
                    "paper_pnl_total": {"production": 12.5, "shadow": 0.0},
                    "main_blocked_by": "insufficient_shadow_sample",
                    "main_warning": "none",
                    "continuous_replay_available": True,
                    "continuous_replay_mode": "continuous_portfolio",
                },
                "impact_gate": {
                    "status": "INSUFFICIENT_DATA",
                    "blocked": True,
                    "blocked_by": ["insufficient_shadow_sample"],
                    "blocking_reasons": ["insufficient_shadow_sample"],
                    "warnings": [],
                    "checks": [],
                    "reason_explanations": {
                        "insufficient_shadow_sample": "shadow profile 样本少于 policy floor。"
                    },
                    "warning_explanations": {},
                    "explanation": "INSUFFICIENT_DATA：shadow profile 样本少于 policy floor。",
                    "production_effect": "none",
                },
                "profile_comparison": {
                    "production": {
                        "sample_count": 7,
                        "candidate_count": 14,
                        "generated_intents": 5,
                        "filled_count": 3,
                        "paper_pnl_total": 12.5,
                        "blocked_by_distribution": [
                            {"value": "manual_approval_required", "count": 7}
                        ],
                        "reason_code_distribution": [
                            {"value": "REVIEW_DIRECTION_BLOCKED", "count": 7}
                        ],
                    },
                    "shadow": {
                        "sample_count": 0,
                        "candidate_count": 0,
                        "generated_intents": 0,
                        "filled_count": 0,
                        "paper_pnl_total": 0.0,
                        "blocked_by_distribution": [],
                        "reason_code_distribution": [],
                    },
                    "unknown": {
                        "sample_count": 0,
                        "candidate_count": 0,
                        "generated_intents": 0,
                        "filled_count": 0,
                        "paper_pnl_total": 0.0,
                        "blocked_by_distribution": [],
                        "reason_code_distribution": [],
                    },
                },
                "continuous_replay": {
                    "available": True,
                    "path": str(tmp_path / "paper_trading_replay_2026-05-08_2026-05-14.json"),
                    "start": "2026-05-08",
                    "end": "2026-05-14",
                    "replay_mode": "continuous_portfolio",
                    "portfolio_carry_forward": True,
                    "source_artifact": {
                        "exists": True,
                        "path": str(tmp_path / "paper_trading_replay_2026-05-08_2026-05-14.json"),
                        "mode": "continuous_portfolio",
                        "date_range": {"start": "2026-05-08", "end": "2026-05-14"},
                        "used_for_comparison": True,
                    },
                    "profiles": {
                        "production": {
                            "available": True,
                            "final_equity": 100012.5,
                            "max_drawdown_pct": -0.01,
                        },
                        "shadow": {
                            "available": False,
                            "final_equity": None,
                            "max_drawdown_pct": None,
                        },
                    },
                },
                "windows": windows,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (tmp_path / f"shadow_parameter_impact_{suffix}.md").write_text(
        "# Shadow Parameter Impact Evaluation\n\n- production_effect=none\n",
        encoding="utf-8",
    )


def _write_weight_adjustment_candidates(tmp_path: Path, as_of: date) -> None:
    suffix = as_of.isoformat()
    (tmp_path / f"weight_adjustment_candidates_{suffix}.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "weight_adjustment_candidates",
                "as_of": suffix,
                "generated_at": datetime(2026, 5, 4, 22, 0, tzinfo=UTC).isoformat(),
                "mode": "observe_only",
                "production_effect": "none",
                "status": "LIMITED",
                "gate_status": "LIMITED",
                "candidate_count": 1,
                "top_candidate_id": f"weight_adjustment_candidate:{suffix}:limited_input",
                "outputs": {
                    "json": str(tmp_path / f"weight_adjustment_candidates_{suffix}.json"),
                    "markdown": str(tmp_path / f"weight_adjustment_candidates_{suffix}.md"),
                },
                "summary": {
                    "candidate_count": 1,
                    "top_candidate_id": f"weight_adjustment_candidate:{suffix}:limited_input",
                    "gate_status": "LIMITED",
                    "main_blocked_by": "manual_approval_required",
                    "production_effect": "none",
                    "mode": "observe_only",
                },
                "candidate_gate": {
                    "status": "LIMITED",
                    "blocked": True,
                    "blocked_by": ["manual_approval_required"],
                    "explanation": "人工复核前保持 blocked。",
                },
                "candidates": [
                    {
                        "candidate_id": f"weight_adjustment_candidate:{suffix}:limited_input",
                        "generated_at": datetime(2026, 5, 4, 22, 0, tzinfo=UTC).isoformat(),
                        "source_profile": {"profile_id": "production_current"},
                        "target_profile": {"profile_id": "limited_no_change"},
                        "parameter_changes": [],
                        "reason_codes": ["limited_input"],
                        "expected_effect": {"summary": "只读占位。"},
                        "risk_notes": ["人工复核前保持 blocked。"],
                        "blocked_by": ["manual_approval_required"],
                        "required_validations": ["aits validate-data"],
                        "production_effect": "none",
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (tmp_path / f"weight_adjustment_candidates_{suffix}.md").write_text(
        "# Weight Adjustment Candidate Generator\n\n- production_effect=none\n",
        encoding="utf-8",
    )


def _write_weight_candidate_evaluation(tmp_path: Path, as_of: date) -> None:
    suffix = as_of.isoformat()
    candidate_id = f"weight_adjustment_candidate:{suffix}:limited_input"
    windows = {
        str(window): {
            "window_days": window,
            "evaluation_status": "CANDIDATE_PROMISING_BUT_LIMITED",
            "candidate_count": 1,
            "evaluable_candidate_count": 1,
            "blocked_candidate_count": 1,
            "insufficient_data_count": 0,
            "low_quality_data_count": 0,
            "continuous_replay_available": True,
            "synthetic_snapshot_ratio": 0.0,
            "historical_ohlc_coverage": 1.0,
            "reconciliation_pass_ratio": 1.0,
            "paper_signal_quality_status": "OBSERVE_ONLY",
            "shadow_impact_status": "SHADOW_PROMISING_BUT_LIMITED",
            "replay_mode": "continuous_portfolio",
            "max_drawdown_delta": 0.0,
            "final_equity_delta": 100.0,
            "exposure_delta": 0.0,
            "concentration_delta": 0.0,
            "blocked_by": ["manual_approval_required", "candidate_blocked"],
            "main_blocked_by": "manual_approval_required",
            "production_effect": "none",
        }
        for window in (7, 14, 30)
    }
    (tmp_path / f"weight_candidate_evaluation_{suffix}.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "weight_candidate_evaluation",
                "as_of": suffix,
                "generated_at": datetime(2026, 5, 4, 22, 5, tzinfo=UTC).isoformat(),
                "status": "BLOCKED",
                "evaluation_status": "CANDIDATE_PROMISING_BUT_LIMITED",
                "evaluation_mode": "observe_only",
                "production_effect": "none",
                "selected_window_days": 30,
                "outputs": {
                    "json": str(tmp_path / f"weight_candidate_evaluation_{suffix}.json"),
                    "markdown": str(tmp_path / f"weight_candidate_evaluation_{suffix}.md"),
                },
                "summary": {
                    "evaluation_status": "CANDIDATE_PROMISING_BUT_LIMITED",
                    "candidate_count": 1,
                    "evaluable_candidate_count": 1,
                    "blocked_candidate_count": 1,
                    "insufficient_data_count": 0,
                    "low_quality_data_count": 0,
                    "top_candidate_id": candidate_id,
                    "main_blocked_by": "manual_approval_required",
                    "production_effect": "none",
                    "evaluation_mode": "observe_only",
                },
                "windows": windows,
                "candidates": [
                    {
                        "candidate_id": candidate_id,
                        "source_profile": {"profile_id": "production_current"},
                        "target_profile": {"profile_id": "limited_no_change"},
                        "parameter_changes": [],
                        "required_validations": ["aits validate-data", "manual_owner_review"],
                        "evaluation_status": "CANDIDATE_PROMISING_BUT_LIMITED",
                        "blocked": True,
                        "blocked_by": ["manual_approval_required", "candidate_blocked"],
                        "warnings": [],
                        "scorecard": {"selected_window_days": 30, "windows": windows},
                        "recommendation": {"action": "manual_review_only"},
                        "production_effect": "none",
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (tmp_path / f"weight_candidate_evaluation_{suffix}.md").write_text(
        "# Weight Candidate Evaluation\n\n- production_effect=none\n- observe-only=true\n",
        encoding="utf-8",
    )


def _assert_no_shadow_impact_dangerous_terms(*texts: str) -> None:
    combined = "\n".join(texts)
    for term in DANGEROUS_SHADOW_IMPACT_OUTPUT_TERMS:
        assert term not in combined


def _write_order_intent_candidates(tmp_path: Path, as_of: date) -> None:
    (tmp_path / f"order_intent_candidates_{as_of.isoformat()}.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "order_intent_candidates",
                "as_of": as_of.isoformat(),
                "production_effect": "none",
                "candidate_count": 1,
                "candidates": [
                    {
                        "candidate_id": f"candidate:{as_of.isoformat()}",
                        "blocked": True,
                        "blocked_by": [
                            "manual_approval_required",
                            "data_gate_blocked",
                        ],
                        "reason_codes": ["REVIEW_DIRECTION_BLOCKED"],
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_paper_trading_replay(
    tmp_path: Path,
    *,
    start: date,
    end: date,
    mode: str,
    portfolio_carry_forward: bool,
    final_equity: float,
    max_drawdown_pct: float,
    exposure_peak: float,
    final_positions: list[dict[str, object]],
    expired_day_orders: int,
) -> None:
    (tmp_path / f"paper_trading_replay_{start.isoformat()}_{end.isoformat()}.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "paper_trading_replay",
                "generated_at": datetime(2026, 5, 14, 23, 0, tzinfo=UTC).isoformat(),
                "start": start.isoformat(),
                "end": end.isoformat(),
                "production_effect": "none",
                "replay_mode": mode,
                "portfolio_carry_forward": portfolio_carry_forward,
                "continuous_metrics_available": mode == "continuous_portfolio",
                "order_expiration_policy": "DAY orders expire at end of replay day.",
                "unsupported_order_policy": "GTC orders are rejected.",
                "final_cash": final_equity - exposure_peak,
                "final_equity": final_equity,
                "final_positions": final_positions,
                "carried_positions_count": len(final_positions),
                "max_drawdown": {
                    "amount_usd": -100.0,
                    "percent": max_drawdown_pct,
                },
                "max_drawdown_pct": max_drawdown_pct,
                "exposure_peak": exposure_peak,
                "expired_day_orders": expired_day_orders,
                "daily_results": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_decision_snapshot(path: Path, gate_caps: dict[str, float]) -> None:
    path.write_text(
        json.dumps(
            {
                "positions": {
                    "position_gates": [
                        {
                            "gate_id": gate_id,
                            "max_position": cap,
                        }
                        for gate_id, cap in sorted(gate_caps.items())
                    ]
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
