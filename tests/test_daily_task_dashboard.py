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
            "run_data_freshness_summary",
            "ai_trading_system.trading_engine.data_freshness_summary",
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
    assert summary["pipeline_health_status"] == "OK"
    assert summary["pipeline_health_health_status"] == "OK"
    assert summary["data_freshness_status"] == "OK"
    assert summary["data_freshness_freshness_status"] == "OK"
    assert summary["missing_required_pipelines"] == 1
    assert summary["stale_required_pipelines"] == 2
    assert summary["missing_required_sources"] == 3
    assert summary["stale_required_sources"] == 4
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
    assert "pipeline_health.health_status" in html
    assert "data_freshness.freshness_status" in html
    assert "missing_required_pipelines" in html
    assert "stale_required_sources" in html


def test_daily_task_dashboard_operator_brief_scheduler_dry_run_card_is_read_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    as_of = date(2026, 5, 23)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    dry_run = _write_operator_brief_scheduler_dry_run(tmp_path, as_of)

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
            "run_pipeline_health_summary",
            "run_data_freshness_summary",
            "run_daily_trading_system_operator_brief",
            "run_daily_operator_brief_scheduler_dry_run",
            "generate_daily_operator_brief_scheduler_templates",
            "ai_trading_system.trading_engine.daily_trading_system_operator_brief",
            "ai_trading_system.trading_engine.pipeline_health_summary",
            "ai_trading_system.trading_engine.data_freshness_summary",
            "ai_trading_system.trading_engine.daily_operator_brief_scheduler_dry_run",
            "ai_trading_system.trading_engine.daily_operator_brief_scheduler_templates",
            "ai_trading_system.data.download",
            "ai_trading_system.scoring",
            "ai_trading_system.backtest",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(f"dashboard must not import scheduler or pipeline path: {name}")
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

    summary = payload["daily_operator_brief_scheduler_dry_run"]
    assert summary["dry_run_decision"] == "READY"
    assert summary["dry_run_status"] == "OK"
    assert summary["summary_level"] == "NORMAL"
    assert summary["expected_run_time_local"] == "09:00"
    assert summary["dependency_check_status"] == "PASS"
    assert summary["safety_check_status"] == "PASS"
    assert summary["missing_required_inputs_count"] == 0
    assert summary["missing_optional_inputs_count"] == 1
    assert summary["stale_inputs_count"] == 1
    assert summary["latest_dry_run_markdown_path"].endswith(
        "daily_operator_brief_scheduler_dry_run_2026-05-23.md"
    )
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["scheduler_dry_run_only"] is True
    assert summary["read_only"] is True
    assert summary["scheduler_created"] is False
    assert summary["operator_brief_executed_by_scheduler_dry_run"] is False
    assert summary["pipelines_executed_by_scheduler_dry_run"] is False
    assert summary["data_downloaded_by_scheduler_dry_run"] is False
    assert summary["apply_executed_by_scheduler_dry_run"] is False
    assert summary["rollback_executed_by_scheduler_dry_run"] is False
    assert summary["broker_execution"] is False
    assert summary["replay_execution"] is False
    assert summary["trading_execution"] is False
    assert "Daily Operator Brief Scheduler Dry Run" in html
    assert "dependency_check.status" in html
    assert "safety_check.status" in html
    assert "missing_optional_inputs_count" in html
    assert dry_run["output_artifacts"]["markdown"]["path"] in html


def test_daily_task_dashboard_scheduler_templates_card_is_read_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    templates = _write_operator_brief_scheduler_templates(tmp_path, as_of)

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
            "run_pipeline_health_summary",
            "run_data_freshness_summary",
            "run_daily_trading_system_operator_brief",
            "run_daily_operator_brief_scheduler_dry_run",
            "generate_daily_operator_brief_scheduler_templates",
            "ai_trading_system.trading_engine.daily_trading_system_operator_brief",
            "ai_trading_system.trading_engine.pipeline_health_summary",
            "ai_trading_system.trading_engine.data_freshness_summary",
            "ai_trading_system.trading_engine.daily_operator_brief_scheduler_dry_run",
            "ai_trading_system.trading_engine.daily_operator_brief_scheduler_templates",
            "ai_trading_system.data.download",
            "ai_trading_system.scoring",
            "ai_trading_system.backtest",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(f"dashboard must not import scheduler or pipeline path: {name}")
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

    summary = payload["daily_operator_brief_scheduler_templates"]
    assert summary["template_generation_status"] == "GENERATED"
    assert summary["scheduler_created"] is False
    assert summary["scheduler_installed"] is False
    assert summary["scheduler_enabled"] is False
    assert summary["manual_review_required"] is True
    assert summary["generated_template_count"] == 5
    assert summary["windows_template_path"].endswith(
        "windows/daily_operator_brief_task_2026-05-24.xml.template"
    )
    assert summary["cron_template_path"].endswith(
        "cron/daily_operator_brief_cron_2026-05-24.txt.template"
    )
    assert summary["github_actions_template_path"].endswith(
        "github_actions/daily_operator_brief_workflow_2026-05-24.yml.template"
    )
    assert summary["summary_markdown_path"].endswith(
        "daily_operator_brief_scheduler_templates_2026-05-24.md"
    )
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["scheduler_template_only"] is True
    assert summary["read_only"] is True
    assert summary["operator_brief_executed_by_template_generator"] is False
    assert summary["pipelines_executed_by_template_generator"] is False
    assert summary["data_downloaded_by_template_generator"] is False
    assert summary["apply_executed_by_template_generator"] is False
    assert summary["rollback_executed_by_template_generator"] is False
    assert summary["broker_execution"] is False
    assert summary["replay_execution"] is False
    assert summary["trading_execution"] is False
    assert "Scheduler Configuration Templates" in html
    assert "template_generation_status" in html
    assert "scheduler_installed" in html
    assert templates["summary_markdown_path"] in html


def test_daily_task_dashboard_scheduler_template_validation_card_is_read_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    validation = _write_operator_brief_scheduler_template_validation(tmp_path, as_of)

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
            "run_pipeline_health_summary",
            "run_data_freshness_summary",
            "run_daily_trading_system_operator_brief",
            "run_daily_operator_brief_scheduler_dry_run",
            "generate_daily_operator_brief_scheduler_templates",
            "validate_daily_operator_brief_scheduler_templates",
            "ai_trading_system.trading_engine.daily_trading_system_operator_brief",
            "ai_trading_system.trading_engine.pipeline_health_summary",
            "ai_trading_system.trading_engine.data_freshness_summary",
            "ai_trading_system.trading_engine.daily_operator_brief_scheduler_dry_run",
            "ai_trading_system.trading_engine.daily_operator_brief_scheduler_templates",
            "ai_trading_system.trading_engine.daily_operator_brief_scheduler_template_validation",
            "ai_trading_system.data.download",
            "ai_trading_system.scoring",
            "ai_trading_system.backtest",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(f"dashboard must not import scheduler or pipeline path: {name}")
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

    summary = payload["daily_operator_brief_scheduler_template_validation"]
    assert summary["validation_status"] == "PASS_WITH_WARNINGS"
    assert summary["summary_level"] == "WATCH"
    assert summary["templates_declared"] == 5
    assert summary["templates_found"] == 5
    assert summary["templates_passed"] == 4
    assert summary["templates_with_warnings"] == 1
    assert summary["templates_failed"] == 0
    assert summary["critical_findings_count"] == 0
    assert summary["warnings_count"] == 1
    assert summary["validation_markdown_path"].endswith(
        "daily_operator_brief_scheduler_template_validation_2026-05-24.md"
    )
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["scheduler_template_validation_only"] is True
    assert summary["read_only"] is True
    assert summary["scheduler_created"] is False
    assert summary["scheduler_installed"] is False
    assert summary["scheduler_enabled"] is False
    assert summary["templates_executed_by_validator"] is False
    assert summary["operator_brief_executed_by_validator"] is False
    assert summary["pipelines_executed_by_validator"] is False
    assert summary["data_downloaded_by_validator"] is False
    assert summary["apply_executed_by_validator"] is False
    assert summary["rollback_executed_by_validator"] is False
    assert summary["broker_execution"] is False
    assert summary["replay_execution"] is False
    assert summary["trading_execution"] is False
    assert "Scheduler Template Validation Report" in html
    assert "validation_status" in html
    assert "templates_with_warnings" in html
    assert validation["output_artifacts"]["validation_markdown"]["path"] in html


def test_daily_task_dashboard_operator_brief_notification_draft_card_is_read_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    notification = _write_operator_brief_notification_draft(tmp_path, as_of)

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
            "run_pipeline_health_summary",
            "run_data_freshness_summary",
            "run_daily_trading_system_operator_brief",
            "run_daily_operator_brief_scheduler_dry_run",
            "generate_daily_operator_brief_scheduler_templates",
            "validate_daily_operator_brief_scheduler_templates",
            "generate_operator_brief_notification_draft",
            "ai_trading_system.trading_engine.daily_trading_system_operator_brief",
            "ai_trading_system.trading_engine.pipeline_health_summary",
            "ai_trading_system.trading_engine.data_freshness_summary",
            "ai_trading_system.trading_engine.daily_operator_brief_scheduler_dry_run",
            "ai_trading_system.trading_engine.daily_operator_brief_scheduler_templates",
            "ai_trading_system.trading_engine.daily_operator_brief_scheduler_template_validation",
            "ai_trading_system.trading_engine.operator_brief_notification_draft",
            "smtplib",
            "slack_sdk",
            "discord",
            "gmail",
            "ai_trading_system.data.download",
            "ai_trading_system.scoring",
            "ai_trading_system.backtest",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(
                f"dashboard must not import notification or execution path: {name}"
            )
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

    summary = payload["operator_brief_notification_draft"]
    assert summary["draft_status"] == "GENERATED"
    assert summary["notification_severity"] == "NORMAL"
    assert summary["headline"] == notification["headline"]
    assert summary["email_draft_path"].endswith("operator_brief_email_draft_2026-05-24.md")
    assert summary["chat_draft_path"].endswith("operator_brief_chat_draft_2026-05-24.md")
    assert summary["mobile_summary_path"].endswith("operator_brief_mobile_summary_2026-05-24.md")
    assert summary["manual_review_required"] is True
    assert summary["email_sent"] is False
    assert summary["slack_sent"] is False
    assert summary["discord_sent"] is False
    assert summary["mobile_push_sent"] is False
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["notification_draft_only"] is True
    assert summary["read_only"] is True
    assert summary["operator_brief_executed_by_notification_draft"] is False
    assert summary["pipelines_executed_by_notification_draft"] is False
    assert summary["data_downloaded_by_notification_draft"] is False
    assert summary["apply_executed_by_notification_draft"] is False
    assert summary["rollback_executed_by_notification_draft"] is False
    assert summary["broker_execution"] is False
    assert summary["replay_execution"] is False
    assert summary["trading_execution"] is False
    assert "Operator Brief Notification Draft" in html
    assert "notification_severity" in html
    assert "email_sent" in html
    assert notification["draft_outputs"]["summary_markdown"]["path"] in html


def test_daily_task_dashboard_operator_brief_notification_delivery_preflight_card_is_read_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    preflight = _write_operator_brief_notification_delivery_preflight(tmp_path, as_of)

    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked_module_tokens = (
            "run_daily_trading_system_operator_brief",
            "generate_operator_brief_notification_draft",
            "run_operator_brief_notification_delivery_preflight",
            "ai_trading_system.trading_engine.daily_trading_system_operator_brief",
            "ai_trading_system.trading_engine.operator_brief_notification_draft",
            "ai_trading_system.trading_engine.operator_brief_notification_delivery_preflight",
            "smtplib",
            "slack_sdk",
            "discord",
            "gmail",
            "webhook",
            "ai_trading_system.data.download",
            "ai_trading_system.scoring",
            "ai_trading_system.backtest",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(
                f"dashboard must not import delivery preflight or execution path: {name}"
            )
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

    summary = payload["operator_brief_notification_delivery_preflight"]
    assert summary["preflight_status"] == "PASS"
    assert summary["delivery_readiness"] == "READY_FOR_MANUAL_REVIEW"
    assert summary["notification_severity"] == "NORMAL"
    assert summary["email_channel_status"] == "READY_FOR_MANUAL_REVIEW"
    assert summary["chat_channel_status"] == "READY_FOR_MANUAL_REVIEW"
    assert summary["mobile_channel_status"] == "READY_FOR_MANUAL_REVIEW"
    assert summary["approval_required"] is False
    assert summary["critical_alert_count"] == 0
    assert summary["warning_count"] == 1
    assert summary["email_sent"] is False
    assert summary["gmail_draft_created"] is False
    assert summary["gmail_draft_modified"] is False
    assert summary["slack_sent"] is False
    assert summary["discord_sent"] is False
    assert summary["webhook_called"] is False
    assert summary["mobile_push_sent"] is False
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["notification_delivery_preflight_only"] is True
    assert summary["read_only"] is True
    assert summary["operator_brief_executed_by_delivery_preflight"] is False
    assert summary["notification_draft_executed_by_delivery_preflight"] is False
    assert summary["pipelines_executed_by_delivery_preflight"] is False
    assert summary["data_downloaded_by_delivery_preflight"] is False
    assert summary["apply_executed_by_delivery_preflight"] is False
    assert summary["rollback_executed_by_delivery_preflight"] is False
    assert summary["broker_execution"] is False
    assert summary["replay_execution"] is False
    assert summary["trading_execution"] is False
    assert "Operator Brief Notification Delivery Preflight" in html
    assert "delivery_readiness" in html
    assert "webhook_called" in html
    assert preflight["output_artifacts"]["preflight_markdown"]["path"] in html


def test_daily_task_dashboard_operator_brief_notification_dispatch_preview_card_is_read_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    preview = _write_operator_brief_notification_dispatch_preview(tmp_path, as_of)

    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked_module_tokens = (
            "run_daily_trading_system_operator_brief",
            "generate_operator_brief_notification_draft",
            "run_operator_brief_notification_delivery_preflight",
            "run_operator_brief_notification_dispatch_preview",
            "ai_trading_system.trading_engine.daily_trading_system_operator_brief",
            "ai_trading_system.trading_engine.operator_brief_notification_draft",
            "ai_trading_system.trading_engine.operator_brief_notification_delivery_preflight",
            "ai_trading_system.trading_engine.operator_brief_notification_dispatch_preview",
            "smtplib",
            "slack_sdk",
            "telegram",
            "discord",
            "gmail",
            "webhook",
            "ai_trading_system.data.download",
            "ai_trading_system.scoring",
            "ai_trading_system.backtest",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(
                f"dashboard must not import dispatch preview or execution path: {name}"
            )
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

    summary = payload["operator_brief_notification_dispatch_preview"]
    assert summary["final_status"] == "WOULD_SEND"
    assert summary["preflight_status"] == "PASS"
    assert summary["dispatch_status"] == "WOULD_SEND"
    assert summary["channel_count"] == 2
    assert summary["would_send_channel_count"] == 1
    assert summary["human_action_required"] is True
    assert summary["next_recommended_action"] == (
        "Manual reviewer may approve a future real dispatch task; this run sent nothing."
    )
    assert summary["generated_at"] == "2026-05-24T00:00:00Z"
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["dispatch_preview_only"] is True
    assert summary["read_only"] is True
    assert summary["external_side_effects"] is False
    assert summary["network_access_required"] is False
    assert summary["secrets_required"] is False
    assert summary["email_sent"] is False
    assert summary["gmail_draft_created"] is False
    assert summary["gmail_draft_modified"] is False
    assert summary["slack_sent"] is False
    assert summary["telegram_sent"] is False
    assert summary["discord_sent"] is False
    assert summary["webhook_called"] is False
    assert summary["mobile_push_sent"] is False
    assert summary["operator_brief_executed_by_dispatch_preview"] is False
    assert summary["notification_draft_executed_by_dispatch_preview"] is False
    assert summary["delivery_preflight_executed_by_dispatch_preview"] is False
    assert summary["pipelines_executed_by_dispatch_preview"] is False
    assert summary["data_downloaded_by_dispatch_preview"] is False
    assert summary["apply_executed_by_dispatch_preview"] is False
    assert summary["rollback_executed_by_dispatch_preview"] is False
    assert summary["broker_execution"] is False
    assert summary["replay_execution"] is False
    assert summary["trading_execution"] is False
    assert "Operator Brief Notification Dispatch Preview" in html
    assert "would_send_channel_count" in html
    assert "latest artifact path" in html
    assert preview["output_artifacts"]["dispatch_preview_markdown"]["path"] in html


def test_daily_task_dashboard_operator_brief_notification_approval_gate_card_is_read_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    gate = _write_operator_brief_notification_approval_gate(tmp_path, as_of)

    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked_module_tokens = (
            "run_daily_trading_system_operator_brief",
            "generate_operator_brief_notification_draft",
            "run_operator_brief_notification_delivery_preflight",
            "run_operator_brief_notification_dispatch_preview",
            "run_operator_brief_notification_approval_gate",
            "ai_trading_system.trading_engine.daily_trading_system_operator_brief",
            "ai_trading_system.trading_engine.operator_brief_notification_draft",
            "ai_trading_system.trading_engine.operator_brief_notification_delivery_preflight",
            "ai_trading_system.trading_engine.operator_brief_notification_dispatch_preview",
            "ai_trading_system.trading_engine.operator_brief_notification_approval_gate",
            "smtplib",
            "slack_sdk",
            "telegram",
            "discord",
            "gmail",
            "webhook",
            "ai_trading_system.data.download",
            "ai_trading_system.scoring",
            "ai_trading_system.backtest",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(
                f"dashboard must not import approval gate or execution path: {name}"
            )
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

    summary = payload["operator_brief_notification_approval_gate"]
    assert summary["approval_gate_status"] == "APPROVED"
    assert summary["allowed_to_enter_dispatch"] is True
    assert summary["human_action_required"] is False
    assert summary["dispatch_preview_status"] == "WOULD_SEND"
    assert summary["approval_marker_exists"] is True
    assert summary["hash_matches"] is True
    assert summary["expired"] is False
    assert summary["generated_at"] == "2026-05-24T00:00:00Z"
    assert summary["next_recommended_action"] == (
        "Future real dispatch may read this approval gate artifact; TRADING-033 sent nothing."
    )
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["approval_gate_only"] is True
    assert summary["read_only"] is True
    assert summary["external_side_effects"] is False
    assert summary["network_access_required"] is False
    assert summary["secrets_required"] is False
    assert summary["email_sent"] is False
    assert summary["gmail_draft_created"] is False
    assert summary["gmail_draft_modified"] is False
    assert summary["slack_sent"] is False
    assert summary["telegram_sent"] is False
    assert summary["discord_sent"] is False
    assert summary["webhook_called"] is False
    assert summary["mobile_push_sent"] is False
    assert summary["operator_brief_executed_by_approval_gate"] is False
    assert summary["notification_draft_executed_by_approval_gate"] is False
    assert summary["delivery_preflight_executed_by_approval_gate"] is False
    assert summary["dispatch_preview_executed_by_approval_gate"] is False
    assert summary["pipelines_executed_by_approval_gate"] is False
    assert summary["data_downloaded_by_approval_gate"] is False
    assert summary["apply_executed_by_approval_gate"] is False
    assert summary["rollback_executed_by_approval_gate"] is False
    assert summary["broker_execution"] is False
    assert summary["replay_execution"] is False
    assert summary["trading_execution"] is False
    assert "Operator Brief Notification Approval Gate" in html
    assert "allowed_to_enter_dispatch" in html
    assert "hash_matches" in html
    assert gate["output_artifacts"]["approval_gate_markdown"]["path"] in html


def test_daily_task_dashboard_operator_brief_notification_draft_dispatch_card_is_read_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    draft_dispatch = _write_operator_brief_notification_draft_dispatch(tmp_path, as_of)

    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked_module_tokens = (
            "run_daily_trading_system_operator_brief",
            "generate_operator_brief_notification_draft",
            "run_operator_brief_notification_delivery_preflight",
            "run_operator_brief_notification_dispatch_preview",
            "run_operator_brief_notification_approval_gate",
            "run_operator_brief_notification_draft_dispatch",
            "ai_trading_system.trading_engine.daily_trading_system_operator_brief",
            "ai_trading_system.trading_engine.operator_brief_notification_draft",
            "ai_trading_system.trading_engine.operator_brief_notification_delivery_preflight",
            "ai_trading_system.trading_engine.operator_brief_notification_dispatch_preview",
            "ai_trading_system.trading_engine.operator_brief_notification_approval_gate",
            "ai_trading_system.trading_engine.operator_brief_notification_draft_dispatch",
            "smtplib",
            "slack_sdk",
            "telegram",
            "discord",
            "gmail",
            "webhook",
            "ai_trading_system.data.download",
            "ai_trading_system.scoring",
            "ai_trading_system.backtest",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(
                f"dashboard must not import draft dispatch or execution path: {name}"
            )
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

    summary = payload["operator_brief_notification_draft_dispatch"]
    assert summary["final_status"] == "DRAFT_READY"
    assert summary["ready_for_actual_dispatch"] is True
    assert summary["approval_gate_status"] == "APPROVED"
    assert summary["channel_count"] == 2
    assert summary["draft_ready_channel_count"] == 1
    assert summary["draft_hash"] == "sha256:abc123"
    assert summary["generated_at"] == "2026-05-24T00:00:00Z"
    assert summary["next_recommended_action"] == (
        "Review this local draft dispatch artifact before any future actual dispatch task; "
        "TRADING-034 sent nothing."
    )
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["draft_dispatch_only"] is True
    assert summary["read_only"] is True
    assert summary["external_side_effects"] is False
    assert summary["network_access_required"] is False
    assert summary["secrets_required"] is False
    assert summary["email_sent"] is False
    assert summary["gmail_draft_created"] is False
    assert summary["gmail_draft_modified"] is False
    assert summary["smtp_called"] is False
    assert summary["slack_sent"] is False
    assert summary["telegram_sent"] is False
    assert summary["discord_sent"] is False
    assert summary["webhook_called"] is False
    assert summary["mobile_push_sent"] is False
    assert summary["operator_brief_executed_by_draft_dispatch"] is False
    assert summary["notification_draft_executed_by_draft_dispatch"] is False
    assert summary["delivery_preflight_executed_by_draft_dispatch"] is False
    assert summary["dispatch_preview_executed_by_draft_dispatch"] is False
    assert summary["approval_gate_executed_by_draft_dispatch"] is False
    assert summary["pipelines_executed_by_draft_dispatch"] is False
    assert summary["data_downloaded_by_draft_dispatch"] is False
    assert summary["apply_executed_by_draft_dispatch"] is False
    assert summary["rollback_executed_by_draft_dispatch"] is False
    assert summary["operator_brief_executed_by_dispatch"] is False
    assert summary["pipelines_executed_by_dispatch"] is False
    assert summary["data_downloaded_by_dispatch"] is False
    assert summary["apply_executed_by_dispatch"] is False
    assert summary["rollback_executed_by_dispatch"] is False
    assert summary["broker_execution"] is False
    assert summary["replay_execution"] is False
    assert summary["trading_execution"] is False
    assert "Operator Brief Notification Draft Dispatch" in html
    assert "ready_for_actual_dispatch" in html
    assert "draft_ready_channel_count" in html
    assert draft_dispatch["output_artifacts"]["draft_dispatch_markdown"]["path"] in html


def test_daily_task_dashboard_retry_execution_dry_run_card_is_read_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    as_of = date(2026, 5, 26)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    dry_run = _write_retry_execution_dry_run(tmp_path, as_of)

    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked_module_tokens = (
            "run_retry_execution_dry_run",
            "ai_trading_system.trading_engine.retry_execution_dry_run",
            "run_retry_candidate_queue",
            "ai_trading_system.trading_engine.retry_candidate_queue",
            "run_notification_delivery_failure_classification",
            "ai_trading_system.trading_engine.notification_delivery_failure_classification",
            "run_notification_delivery_audit_summary",
            "ai_trading_system.trading_engine.notification_delivery_audit_summary",
            "smtplib",
            "slack_sdk",
            "discord",
            "gmail",
            "webhook",
            "ai_trading_system.data.download",
            "ai_trading_system.scoring",
            "ai_trading_system.backtest",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(f"dashboard must not import retry dry-run path: {name}")
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

    summary = payload["retry_execution_dry_run"]
    assert summary["dry_run_status"] == "READY_FOR_DRY_RUN"
    assert summary["total_candidates"] == 1
    assert summary["approved_for_dry_run"] == 1
    assert summary["blocked_from_dry_run"] == 0
    assert summary["simulated_retry_actions"] == 1
    assert summary["real_retry_allowed"] is False
    assert summary["external_delivery_allowed"] is False
    assert summary["production_state_mutation_allowed"] is False
    assert summary["source_queue_status"] == "PENDING_APPROVAL"
    assert summary["approval_record_available"] is True
    assert summary["approval_parse_status"] == "OK"
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["retry_execution_dry_run_only"] is True
    assert summary["dry_run_only"] is True
    assert summary["read_only"] is True
    assert summary["approval_record_modified"] is False
    assert summary["retry_executed"] is False
    assert summary["actual_retry_executed"] is False
    assert summary["external_delivery_executed"] is False
    assert summary["delivery_state_mutated"] is False
    assert summary["state_mutation_executed"] is False
    assert "Retry Execution Dry Run" in html
    assert "dry_run_status" in html
    assert dry_run["output_artifacts"]["retry_execution_dry_run_markdown"]["path"] in html


def test_daily_task_dashboard_retry_execution_dry_run_handles_missing_report_gracefully(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 26)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=None,
        reports_dir=tmp_path,
    )
    html = render_daily_task_dashboard(report)
    payload = build_daily_task_dashboard_payload(report)

    summary = payload["retry_execution_dry_run"]
    assert summary["exists"] is False
    assert summary["dry_run_status"] == "MISSING"
    assert summary["real_retry_allowed"] is False
    assert "No retry execution dry-run report available." in html


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


def test_daily_task_dashboard_data_freshness_summary_card_is_read_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    as_of = date(2026, 5, 23)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    freshness_summary = _write_data_freshness_summary(tmp_path, as_of)

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
            "run_data_freshness_summary",
            "ai_trading_system.trading_engine.pipeline_health_summary",
            "ai_trading_system.trading_engine.data_freshness_summary",
            "ai_trading_system.data.download",
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

    summary = payload["data_freshness_summary"]
    assert summary["freshness_status"] == "OK"
    assert summary["summary_level"] == "NORMAL"
    assert summary["headline"] == freshness_summary["headline"]
    assert summary["required_sources"] == 3
    assert summary["missing_required_sources"] == 0
    assert summary["stale_required_sources"] == 0
    assert summary["critical_sources"] == 0
    assert summary["warning_sources"] == 1
    assert summary["production_effect"] == "none"
    assert summary["manual_review_only"] is True
    assert summary["data_freshness_only"] is True
    assert summary["read_only"] is True
    assert summary["data_downloaded_by_freshness_check"] is False
    assert summary["pipelines_executed_by_freshness_check"] is False
    assert summary["apply_executed_by_freshness_check"] is False
    assert summary["rollback_executed_by_freshness_check"] is False
    assert summary["broker_execution"] is False
    assert summary["replay_execution"] is False
    assert summary["trading_execution"] is False
    assert "Data Freshness Summary" in html
    assert "data_freshness_summary_2026-05-23.md" in html


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


def _write_data_freshness_summary(tmp_path: Path, as_of: date) -> dict[str, Any]:
    suffix = as_of.isoformat()
    summary_path = (
        tmp_path / "data" / "derived" / "data_freshness" / f"data_freshness_summary_{suffix}.json"
    )
    markdown_path = summary_path.with_suffix(".md")
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "data_freshness_summary",
        "task_id": "TRADING-024",
        "date": suffix,
        "mode": "data_freshness_summary_only",
        "production_effect": "none",
        "manual_review_only": True,
        "data_freshness_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "data_downloaded_by_freshness_check": False,
        "pipelines_executed_by_freshness_check": False,
        "apply_executed_by_freshness_check": False,
        "rollback_executed_by_freshness_check": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "freshness_status": "OK",
        "summary_level": "NORMAL",
        "headline": "Required data sources are fresh enough for today's system outputs.",
        "coverage": {
            "registered_sources": 8,
            "required_sources": 3,
            "available_sources": 7,
            "missing_required_sources": 0,
            "stale_required_sources": 0,
            "critical_sources": 0,
            "warning_sources": 1,
        },
        "alerts": {
            "critical": [],
            "warnings": ["Optional market report artifact was not found."],
            "notes": ["Data freshness summary is read-only."],
        },
        "output_artifacts": {
            "json": {"path": str(summary_path)},
            "markdown": {"path": str(markdown_path)},
        },
        "freshness_contract": {
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
            "runs_data_freshness_summary_script": False,
            "runs_market_pipeline": False,
            "runs_backtest_pipeline": False,
            "runs_scoring_pipeline": False,
            "runs_data_download": False,
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
    markdown_path.write_text("# Data Freshness Summary\n", encoding="utf-8")
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
        "pipeline_health": {
            "status": "OK",
            "available": True,
            "health_status": "OK",
            "missing_required_pipelines": 1,
            "stale_required_pipelines": 2,
        },
        "data_freshness": {
            "status": "OK",
            "available": True,
            "freshness_status": "OK",
            "missing_required_sources": 3,
            "stale_required_sources": 4,
        },
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


def _write_operator_brief_scheduler_dry_run(tmp_path: Path, as_of: date) -> dict[str, Any]:
    suffix = as_of.isoformat()
    dry_run_path = (
        tmp_path
        / "data"
        / "derived"
        / "operator_briefs"
        / "scheduler_dry_run"
        / f"daily_operator_brief_scheduler_dry_run_{suffix}.json"
    )
    markdown_path = dry_run_path.with_suffix(".md")
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "daily_operator_brief_scheduler_dry_run",
        "task_id": "TRADING-026",
        "date": suffix,
        "mode": "daily_operator_brief_scheduler_dry_run_only",
        "production_effect": "none",
        "manual_review_only": True,
        "scheduler_dry_run_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "safe_for_scheduled_generation": True,
        "scheduler_created": False,
        "operator_brief_executed_by_scheduler_dry_run": False,
        "pipelines_executed_by_scheduler_dry_run": False,
        "data_downloaded_by_scheduler_dry_run": False,
        "apply_executed_by_scheduler_dry_run": False,
        "rollback_executed_by_scheduler_dry_run": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "dry_run_decision": "READY",
        "dry_run_status": "OK",
        "summary_level": "NORMAL",
        "headline": "Daily operator brief scheduler dry run is ready.",
        "schedule_plan": {
            "intended_frequency": "DAILY",
            "expected_run_time_local": "09:00",
            "timezone": "Asia/Tokyo",
            "scheduler_target": "manual_or_future_scheduler",
            "actual_scheduler_created": False,
        },
        "dependency_check": {
            "status": "PASS",
            "required_inputs_available": True,
            "optional_inputs_available": False,
            "missing_required_inputs": [],
            "missing_optional_inputs": ["pipeline_health_summary"],
            "invalid_inputs": [],
            "stale_inputs": ["data_freshness_summary"],
            "blocking_reasons": [],
        },
        "safety_check": {
            "status": "PASS",
            "digest_safe": True,
            "pipeline_health_safe": True,
            "data_freshness_safe": True,
            "existing_operator_brief_safe": True,
            "no_scheduler_created": True,
            "no_pipeline_execution": True,
            "no_data_download": True,
            "no_broker_execution": True,
            "no_replay_execution": True,
            "no_trading_execution": True,
            "blocking_reasons": [],
        },
        "output_artifacts": {
            "json": {"path": str(dry_run_path)},
            "markdown": {"path": str(markdown_path)},
        },
        "scheduler_contract": {
            "runs_daily_digest_script": False,
            "runs_pipeline_health_summary_script": False,
            "runs_data_freshness_summary_script": False,
            "runs_operator_brief_script": False,
            "creates_windows_task_scheduler_task": False,
            "creates_cron_job": False,
            "creates_github_actions_workflow": False,
            "runs_market_pipeline": False,
            "runs_backtest_pipeline": False,
            "runs_scoring_pipeline": False,
            "runs_data_download": False,
            "runs_broker_runner": False,
            "runs_replay_runner": False,
            "triggers_trade": False,
            "writes_production_profile": False,
            "writes_production_weights": False,
            "writes_shadow_weights": False,
            "writes_approved_profile": False,
            "promotes_shadow_to_production": False,
        },
    }
    dry_run_path.parent.mkdir(parents=True, exist_ok=True)
    dry_run_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text("# Daily Operator Brief Scheduler Dry Run\n", encoding="utf-8")
    return payload


def _write_operator_brief_scheduler_templates(tmp_path: Path, as_of: date) -> dict[str, Any]:
    suffix = as_of.isoformat()
    template_root = tmp_path / "data" / "derived" / "operator_briefs" / "scheduler_templates"
    json_path = template_root / f"daily_operator_brief_scheduler_templates_{suffix}.json"
    markdown_path = template_root / f"daily_operator_brief_scheduler_templates_{suffix}.md"
    output_templates = {
        "windows_task_xml": {
            "enabled": True,
            "generated": True,
            "path": (
                "data/derived/operator_briefs/scheduler_templates/windows/"
                f"daily_operator_brief_task_{suffix}.xml.template"
            ),
        },
        "powershell_wrapper": {
            "enabled": True,
            "generated": True,
            "path": (
                "data/derived/operator_briefs/scheduler_templates/windows/"
                f"run_daily_operator_brief_{suffix}.ps1.template"
            ),
        },
        "batch_wrapper": {
            "enabled": True,
            "generated": True,
            "path": (
                "data/derived/operator_briefs/scheduler_templates/windows/"
                f"run_daily_operator_brief_{suffix}.bat.template"
            ),
        },
        "cron_line": {
            "enabled": True,
            "generated": True,
            "path": (
                "data/derived/operator_briefs/scheduler_templates/cron/"
                f"daily_operator_brief_cron_{suffix}.txt.template"
            ),
        },
        "github_actions_workflow": {
            "enabled": True,
            "generated": True,
            "path": (
                "data/derived/operator_briefs/scheduler_templates/github_actions/"
                f"daily_operator_brief_workflow_{suffix}.yml.template"
            ),
        },
    }
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "daily_operator_brief_scheduler_templates",
        "task_id": "TRADING-028",
        "date": suffix,
        "mode": "daily_operator_brief_scheduler_template_generation_only",
        "production_effect": "none",
        "manual_review_only": True,
        "scheduler_template_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "scheduler_created": False,
        "scheduler_installed": False,
        "scheduler_enabled": False,
        "operator_brief_executed_by_template_generator": False,
        "pipelines_executed_by_template_generator": False,
        "data_downloaded_by_template_generator": False,
        "apply_executed_by_template_generator": False,
        "rollback_executed_by_template_generator": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "template_generation_status": "GENERATED",
        "summary_level": "NORMAL",
        "headline": "Scheduler configuration templates were generated for manual review.",
        "generated_template_count": 5,
        "output_templates": output_templates,
        "output_artifacts": {
            "metadata_json": {
                "path": f"data/derived/operator_briefs/scheduler_templates/{json_path.name}"
            },
            "summary_markdown": {
                "path": f"data/derived/operator_briefs/scheduler_templates/{markdown_path.name}"
            },
        },
        "summary_markdown_path": (
            f"data/derived/operator_briefs/scheduler_templates/{markdown_path.name}"
        ),
        "safety_validation": {
            "status": "PASS",
            "templates_only": True,
            "no_scheduler_created": True,
            "no_scheduler_installed": True,
            "no_scheduler_enabled": True,
            "no_operator_brief_execution": True,
            "no_pipeline_execution": True,
            "no_data_download": True,
            "no_apply_or_rollback": True,
            "no_broker_replay_trading": True,
            "blocking_reasons": [],
        },
        "manual_review_required": {
            "required": True,
            "instructions": [
                "Review generated templates before copying them.",
                "Run TRADING-026 scheduler dry run before enabling any scheduler.",
            ],
        },
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text("# Daily Operator Brief Scheduler Templates\n", encoding="utf-8")
    return payload


def _write_operator_brief_scheduler_template_validation(
    tmp_path: Path,
    as_of: date,
) -> dict[str, Any]:
    suffix = as_of.isoformat()
    validation_root = (
        tmp_path / "data" / "derived" / "operator_briefs" / "scheduler_template_validation"
    )
    json_path = (
        validation_root / f"daily_operator_brief_scheduler_template_validation_{suffix}.json"
    )
    markdown_path = json_path.with_suffix(".md")
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "daily_operator_brief_scheduler_template_validation",
        "task_id": "TRADING-029",
        "date": suffix,
        "mode": "daily_operator_brief_scheduler_template_validation_only",
        "production_effect": "none",
        "manual_review_only": True,
        "scheduler_template_validation_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "scheduler_created": False,
        "scheduler_installed": False,
        "scheduler_enabled": False,
        "templates_executed_by_validator": False,
        "operator_brief_executed_by_validator": False,
        "pipelines_executed_by_validator": False,
        "data_downloaded_by_validator": False,
        "apply_executed_by_validator": False,
        "rollback_executed_by_validator": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "validation_status": "PASS_WITH_WARNINGS",
        "summary_level": "WATCH",
        "headline": "Scheduler templates passed static validation with warnings.",
        "coverage": {
            "templates_declared": 5,
            "templates_found": 5,
            "templates_missing": 0,
            "templates_passed": 4,
            "templates_with_warnings": 1,
            "templates_failed": 0,
        },
        "alerts": {
            "critical": [],
            "warnings": ["Placeholder repo path detected. Manual review required."],
            "notes": ["Validation is static and does not install or run any scheduler."],
        },
        "output_artifacts": {
            "validation_json": {
                "path": (
                    "data/derived/operator_briefs/scheduler_template_validation/"
                    f"{json_path.name}"
                )
            },
            "validation_markdown": {
                "path": (
                    "data/derived/operator_briefs/scheduler_template_validation/"
                    f"{markdown_path.name}"
                )
            },
        },
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text("# Scheduler Template Validation Report\n", encoding="utf-8")
    return payload


def _write_operator_brief_notification_draft(tmp_path: Path, as_of: date) -> dict[str, Any]:
    suffix = as_of.isoformat()
    notification_root = tmp_path / "data" / "derived" / "operator_briefs" / "notifications"
    json_path = notification_root / f"operator_brief_notification_draft_{suffix}.json"
    markdown_path = json_path.with_suffix(".md")
    email_path = notification_root / "email" / f"operator_brief_email_draft_{suffix}.md"
    chat_path = notification_root / "chat" / f"operator_brief_chat_draft_{suffix}.md"
    mobile_path = notification_root / "mobile" / f"operator_brief_mobile_summary_{suffix}.md"
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "operator_brief_notification_draft",
        "task_id": "TRADING-030",
        "date": suffix,
        "mode": "operator_brief_notification_draft_only",
        "production_effect": "none",
        "manual_review_only": True,
        "notification_draft_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "email_sent": False,
        "gmail_draft_created": False,
        "slack_sent": False,
        "discord_sent": False,
        "mobile_push_sent": False,
        "operator_brief_executed_by_notification_draft": False,
        "pipelines_executed_by_notification_draft": False,
        "data_downloaded_by_notification_draft": False,
        "apply_executed_by_notification_draft": False,
        "rollback_executed_by_notification_draft": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "draft_status": "GENERATED",
        "notification_severity": "NORMAL",
        "headline": "Trading system status is stable. No immediate manual action is required.",
        "source_snapshot": {
            "brief_status": "OK",
            "summary_level": "NORMAL",
            "headline": "Trading system status is stable. No immediate manual action is required.",
            "can_trust_outputs_today": True,
            "manual_action_required": False,
            "parameter_governance_status": "OK",
            "pipeline_health_status": "OK",
            "data_freshness_status": "OK",
            "critical_alert_count": 0,
            "warning_count": 0,
        },
        "draft_outputs": {
            "email_draft": {
                "path": f"data/derived/operator_briefs/notifications/email/{email_path.name}",
                "subject": "[Trading System] Daily Operator Brief - OK - 2026-05-24",
            },
            "chat_draft": {
                "path": f"data/derived/operator_briefs/notifications/chat/{chat_path.name}"
            },
            "mobile_summary": {
                "path": f"data/derived/operator_briefs/notifications/mobile/{mobile_path.name}"
            },
            "summary_markdown": {
                "path": f"data/derived/operator_briefs/notifications/{markdown_path.name}"
            },
        },
        "safety_validation": {
            "status": "PASS",
            "operator_brief_task_id_valid": True,
            "operator_brief_production_effect_none": True,
            "operator_brief_read_only": True,
            "operator_brief_no_execution_flags": True,
            "no_notification_sent": True,
            "no_external_webhook_called": True,
            "no_pipeline_execution": True,
            "no_data_download": True,
            "no_apply_or_rollback": True,
            "no_broker_replay_trading": True,
            "blocking_reasons": [],
        },
        "manual_review_required": {
            "required": True,
            "instructions": ["Review notification drafts before sending."],
        },
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    email_path.parent.mkdir(parents=True, exist_ok=True)
    chat_path.parent.mkdir(parents=True, exist_ok=True)
    mobile_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text("# Operator Brief Notification Draft\n", encoding="utf-8")
    email_path.write_text("# Email Draft\n", encoding="utf-8")
    chat_path.write_text("# Chat Draft\n", encoding="utf-8")
    mobile_path.write_text("Trading System OK - no manual action required.\n", encoding="utf-8")
    return payload


def _write_operator_brief_notification_delivery_preflight(
    tmp_path: Path,
    as_of: date,
) -> dict[str, Any]:
    suffix = as_of.isoformat()
    preflight_root = (
        tmp_path / "data" / "derived" / "operator_briefs" / "notifications" / "delivery_preflight"
    )
    json_path = preflight_root / f"operator_brief_notification_delivery_preflight_{suffix}.json"
    markdown_path = json_path.with_suffix(".md")
    run_log_json_path = (
        preflight_root
        / "logs"
        / f"operator_brief_notification_delivery_preflight_run_{suffix}.json"
    )
    run_log_markdown_path = run_log_json_path.with_suffix(".md")
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "operator_brief_notification_delivery_preflight",
        "task_id": "TRADING-031",
        "date": suffix,
        "mode": "operator_brief_notification_delivery_preflight_only",
        "production_effect": "none",
        "manual_review_only": True,
        "notification_delivery_preflight_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "slack_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "operator_brief_executed_by_delivery_preflight": False,
        "notification_draft_executed_by_delivery_preflight": False,
        "pipelines_executed_by_delivery_preflight": False,
        "data_downloaded_by_delivery_preflight": False,
        "apply_executed_by_delivery_preflight": False,
        "rollback_executed_by_delivery_preflight": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "preflight_status": "PASS",
        "delivery_readiness": "READY_FOR_MANUAL_REVIEW",
        "notification_severity": "NORMAL",
        "headline": (
            "Notification drafts are available and ready for manual review. "
            "No notification was sent."
        ),
        "approval_validation": {
            "status": "PASS",
            "approval_required": False,
            "approval_policy_available": False,
            "approval_reason": "NORMAL severity does not require delivery approval.",
            "blocking_reasons": [],
            "warnings": [],
        },
        "channel_readiness": {
            "email": {
                "status": "READY_FOR_MANUAL_REVIEW",
                "draft_available": True,
                "recipient_config_available": False,
                "can_send_automatically": False,
                "manual_send_only": True,
                "blocking_reasons": [],
                "warnings": [
                    "Recipient/channel configs were not found. Drafts are manual-review only."
                ],
            },
            "chat": {
                "status": "READY_FOR_MANUAL_REVIEW",
                "draft_available": True,
                "channel_config_available": False,
                "can_send_automatically": False,
                "manual_send_only": True,
                "blocking_reasons": [],
                "warnings": [],
            },
            "mobile": {
                "status": "READY_FOR_MANUAL_REVIEW",
                "draft_available": True,
                "channel_config_available": False,
                "can_send_automatically": False,
                "manual_send_only": True,
                "blocking_reasons": [],
                "warnings": [],
            },
        },
        "safety_validation": {
            "status": "PASS",
            "notification_metadata_task_id_valid": True,
            "notification_metadata_safe": True,
            "no_email_sent": True,
            "no_gmail_draft_created": True,
            "no_webhook_called": True,
            "no_mobile_push_sent": True,
            "no_pipeline_execution": True,
            "no_data_download": True,
            "no_apply_or_rollback": True,
            "no_broker_replay_trading": True,
            "blocking_reasons": [],
        },
        "alerts": {
            "critical": [],
            "warnings": [
                "Recipient/channel configs were not found. Drafts are manual-review only."
            ],
            "notes": ["Delivery preflight is read-only and did not send any notification."],
        },
        "output_artifacts": {
            "preflight_json": {
                "path": (
                    "data/derived/operator_briefs/notifications/delivery_preflight/"
                    f"{json_path.name}"
                )
            },
            "preflight_markdown": {
                "path": (
                    "data/derived/operator_briefs/notifications/delivery_preflight/"
                    f"{markdown_path.name}"
                )
            },
            "run_log_json": {
                "path": (
                    "data/derived/operator_briefs/notifications/delivery_preflight/logs/"
                    f"{run_log_json_path.name}"
                )
            },
            "run_log_markdown": {
                "path": (
                    "data/derived/operator_briefs/notifications/delivery_preflight/logs/"
                    f"{run_log_markdown_path.name}"
                )
            },
        },
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    run_log_json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text("# Operator Brief Notification Delivery Preflight\n", encoding="utf-8")
    run_log_json_path.write_text(
        json.dumps({"report_type": "operator_brief_notification_delivery_preflight_run"}),
        encoding="utf-8",
    )
    run_log_markdown_path.write_text(
        "# Operator Brief Notification Delivery Preflight Run\n",
        encoding="utf-8",
    )
    return payload


def _write_operator_brief_notification_dispatch_preview(
    tmp_path: Path,
    as_of: date,
) -> dict[str, Any]:
    suffix = as_of.isoformat()
    preview_root = (
        tmp_path / "data" / "derived" / "operator_briefs" / "notifications" / "dispatch_preview"
    )
    json_path = preview_root / f"operator_brief_notification_dispatch_preview_{suffix}.json"
    markdown_path = json_path.with_suffix(".md")
    latest_json_path = preview_root / "latest.json"
    latest_markdown_path = preview_root / "latest.md"
    run_log_path = preview_root / "run.log"
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "operator_brief_notification_dispatch_preview",
        "task_id": "TRADING-032",
        "date": suffix,
        "mode": "dry_run",
        "production_effect": "none",
        "manual_review_only": True,
        "dispatch_preview_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "external_side_effects": False,
        "network_access_required": False,
        "secrets_required": False,
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "slack_sent": False,
        "telegram_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "operator_brief_executed_by_dispatch_preview": False,
        "notification_draft_executed_by_dispatch_preview": False,
        "delivery_preflight_executed_by_dispatch_preview": False,
        "pipelines_executed_by_dispatch_preview": False,
        "data_downloaded_by_dispatch_preview": False,
        "apply_executed_by_dispatch_preview": False,
        "rollback_executed_by_dispatch_preview": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "metadata": {
            "task_id": "TRADING-032",
            "task_name": "Operator Brief Notification Dry-run Dispatch Preview",
            "run_date": suffix,
            "generated_at": "2026-05-24T00:00:00Z",
            "preview_generated_at": "2026-05-24T00:00:00Z",
            "mode": "dry_run",
            "production_effect": "none",
            "manual_review_only": True,
        },
        "input_refs": {
            "preflight_artifact": {
                "path": (
                    "data/derived/operator_briefs/notifications/delivery_preflight/"
                    f"operator_brief_notification_delivery_preflight_{suffix}.json"
                ),
                "status": "FOUND",
            },
            "operator_brief_json": {
                "path": (
                    "data/derived/operator_briefs/"
                    f"daily_trading_system_operator_brief_{suffix}.json"
                ),
                "status": "FOUND",
            },
            "operator_brief_markdown": {
                "path": (
                    "data/derived/operator_briefs/"
                    f"daily_trading_system_operator_brief_{suffix}.md"
                ),
                "status": "FOUND",
            },
            "notification_draft_metadata": {
                "path": (
                    "data/derived/operator_briefs/notifications/"
                    f"operator_brief_notification_draft_{suffix}.json"
                ),
                "status": "FOUND",
            },
            "template_refs": [],
        },
        "preflight_summary": {
            "status": "PASS",
            "allowed_to_dispatch": True,
            "reasons": [],
            "warnings": [],
        },
        "dispatch_preview": {
            "dispatch_status": "WOULD_SEND",
            "channels": [
                {
                    "channel_id": "email",
                    "channel_type": "email",
                    "target_ref": "o***@example.com",
                    "enabled": True,
                    "would_send": True,
                    "reason": "email channel is enabled for dry-run preview only.",
                },
                {
                    "channel_id": "chat",
                    "channel_type": "file",
                    "target_ref": "operator-chat-channel",
                    "enabled": True,
                    "would_send": False,
                    "reason": "chat requires approval before real dispatch.",
                },
            ],
            "message": {
                "subject_preview": "[Trading System] Daily Operator Brief - OK - 2026-05-24",
                "title_preview": "Daily Trading System Operator Brief - 2026-05-24",
                "body_excerpt": "Ready for manual review.",
                "body_length": 24,
                "contains_markdown": True,
            },
        },
        "safety": {
            "external_side_effects": False,
            "network_access_required": False,
            "secrets_required": False,
            "recipient_masking_applied": True,
            "sensitive_content_flags": [],
        },
        "decision": {
            "final_status": "WOULD_SEND",
            "human_action_required": True,
            "next_recommended_action": (
                "Manual reviewer may approve a future real dispatch task; this run sent nothing."
            ),
        },
        "output_artifacts": {
            "dispatch_preview_json": {
                "path": (
                    "data/derived/operator_briefs/notifications/dispatch_preview/"
                    f"{json_path.name}"
                )
            },
            "dispatch_preview_markdown": {
                "path": (
                    "data/derived/operator_briefs/notifications/dispatch_preview/"
                    f"{markdown_path.name}"
                )
            },
            "latest_json": {
                "path": "data/derived/operator_briefs/notifications/dispatch_preview/latest.json"
            },
            "latest_markdown": {
                "path": "data/derived/operator_briefs/notifications/dispatch_preview/latest.md"
            },
            "run_log": {
                "path": "data/derived/operator_briefs/notifications/dispatch_preview/run.log"
            },
        },
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text("# Operator Brief Notification Dispatch Preview\n", encoding="utf-8")
    latest_json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    latest_markdown_path.write_text(
        "# Operator Brief Notification Dispatch Preview\n",
        encoding="utf-8",
    )
    run_log_path.write_text("final_status=WOULD_SEND\n", encoding="utf-8")
    return payload


def _write_operator_brief_notification_approval_gate(
    tmp_path: Path,
    as_of: date,
) -> dict[str, Any]:
    suffix = as_of.isoformat()
    gate_root = (
        tmp_path / "data" / "derived" / "operator_briefs" / "notifications" / "approval_gate"
    )
    json_path = gate_root / f"operator_brief_notification_approval_gate_{suffix}.json"
    markdown_path = json_path.with_suffix(".md")
    latest_json_path = gate_root / "latest.json"
    latest_markdown_path = gate_root / "latest.md"
    run_log_path = gate_root / "run.log"
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "operator_brief_notification_approval_gate",
        "task_id": "TRADING-033",
        "date": suffix,
        "mode": "approval_gate",
        "production_effect": "none",
        "manual_review_only": True,
        "approval_gate_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "external_side_effects": False,
        "network_access_required": False,
        "secrets_required": False,
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "slack_sent": False,
        "telegram_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "operator_brief_executed_by_approval_gate": False,
        "notification_draft_executed_by_approval_gate": False,
        "delivery_preflight_executed_by_approval_gate": False,
        "dispatch_preview_executed_by_approval_gate": False,
        "pipelines_executed_by_approval_gate": False,
        "data_downloaded_by_approval_gate": False,
        "apply_executed_by_approval_gate": False,
        "rollback_executed_by_approval_gate": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "metadata": {
            "task_id": "TRADING-033",
            "task_name": "Operator Brief Notification Approval Gate",
            "run_date": suffix,
            "generated_at": "2026-05-24T00:00:00Z",
            "mode": "approval_gate",
            "production_effect": "none",
            "manual_review_only": True,
        },
        "input_refs": {
            "dispatch_preview_artifact": {
                "path": "data/derived/operator_briefs/notifications/dispatch_preview/latest.json",
                "status": "FOUND",
                "sha256": "abc",
            },
            "approval_marker": {
                "path": (
                    "data/derived/operator_briefs/notifications/approval_gate/"
                    "approval_marker.json"
                ),
                "status": "FOUND",
                "sha256": "def",
            },
        },
        "dispatch_preview_summary": {
            "final_status": "WOULD_SEND",
            "human_action_required": True,
            "channel_count": 2,
            "would_send_channel_count": 1,
        },
        "approval_marker_summary": {
            "exists": True,
            "approved": True,
            "approved_by": "operator",
            "approved_at": "2026-05-24T00:00:00Z",
            "expires_at": "2026-05-25T00:00:00Z",
            "preview_hash": "sha256:abc",
            "hash_matches": True,
            "expired": False,
        },
        "hashes": {
            "dispatch_preview_hash": "sha256:abc",
            "hash_algorithm": "sha256",
            "hash_scope": "canonical_dispatch_preview_json",
        },
        "decision": {
            "approval_gate_status": "APPROVED",
            "allowed_to_enter_dispatch": True,
            "human_action_required": False,
            "next_recommended_action": (
                "Future real dispatch may read this approval gate artifact; "
                "TRADING-033 sent nothing."
            ),
        },
        "safety": {
            "external_side_effects": False,
            "network_access_required": False,
            "secrets_required": False,
            "approval_required_for_would_send": True,
        },
        "reasons": ["Approval marker is valid."],
        "warnings": [],
        "output_artifacts": {
            "approval_gate_json": {
                "path": (
                    "data/derived/operator_briefs/notifications/approval_gate/" f"{json_path.name}"
                )
            },
            "approval_gate_markdown": {
                "path": (
                    "data/derived/operator_briefs/notifications/approval_gate/"
                    f"{markdown_path.name}"
                )
            },
            "latest_json": {
                "path": "data/derived/operator_briefs/notifications/approval_gate/latest.json"
            },
            "latest_markdown": {
                "path": "data/derived/operator_briefs/notifications/approval_gate/latest.md"
            },
            "run_log": {"path": "data/derived/operator_briefs/notifications/approval_gate/run.log"},
        },
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text("# Operator Brief Notification Approval Gate\n", encoding="utf-8")
    latest_json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    latest_markdown_path.write_text(
        "# Operator Brief Notification Approval Gate\n",
        encoding="utf-8",
    )
    run_log_path.write_text("approval_gate_status=APPROVED\n", encoding="utf-8")
    return payload


def _write_operator_brief_notification_draft_dispatch(
    tmp_path: Path,
    as_of: date,
) -> dict[str, Any]:
    suffix = as_of.isoformat()
    draft_root = (
        tmp_path / "data" / "derived" / "operator_briefs" / "notifications" / "draft_dispatch"
    )
    json_path = draft_root / f"operator_brief_notification_draft_dispatch_{suffix}.json"
    markdown_path = json_path.with_suffix(".md")
    latest_json_path = draft_root / "latest.json"
    latest_markdown_path = draft_root / "latest.md"
    run_log_path = draft_root / "run.log"
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "operator_brief_notification_draft_dispatch",
        "task_id": "TRADING-034",
        "date": suffix,
        "mode": "draft_dispatch",
        "production_effect": "none",
        "manual_review_only": True,
        "draft_dispatch_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "external_side_effects": False,
        "network_access_required": False,
        "secrets_required": False,
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "smtp_called": False,
        "slack_sent": False,
        "telegram_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "operator_brief_executed_by_draft_dispatch": False,
        "notification_draft_executed_by_draft_dispatch": False,
        "delivery_preflight_executed_by_draft_dispatch": False,
        "dispatch_preview_executed_by_draft_dispatch": False,
        "approval_gate_executed_by_draft_dispatch": False,
        "pipelines_executed_by_draft_dispatch": False,
        "data_downloaded_by_draft_dispatch": False,
        "apply_executed_by_draft_dispatch": False,
        "rollback_executed_by_draft_dispatch": False,
        "operator_brief_executed_by_dispatch": False,
        "pipelines_executed_by_dispatch": False,
        "data_downloaded_by_dispatch": False,
        "apply_executed_by_dispatch": False,
        "rollback_executed_by_dispatch": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "metadata": {
            "task_id": "TRADING-034",
            "task_name": "Operator Brief Notification Draft Dispatch",
            "run_date": suffix,
            "generated_at": "2026-05-24T00:00:00Z",
            "mode": "draft_dispatch",
            "production_effect": "none",
            "manual_review_only": True,
        },
        "input_refs": {
            "approval_gate_artifact": {
                "path": "data/derived/operator_briefs/notifications/approval_gate/latest.json",
                "status": "FOUND",
                "sha256": "abc",
            },
            "dispatch_preview_artifact": {
                "path": "data/derived/operator_briefs/notifications/dispatch_preview/latest.json",
                "status": "FOUND",
                "sha256": "def",
            },
        },
        "approval_gate_summary": {
            "approval_gate_status": "APPROVED",
            "allowed_to_enter_dispatch": True,
            "dispatch_preview_hash": "sha256:preview",
            "current_dispatch_preview_hash": "sha256:preview",
        },
        "draft": {
            "draft_status": "DRAFT_READY",
            "draft_id": "local-draft-abc123",
            "channel_count": 2,
            "draft_ready_channel_count": 1,
            "channels": [
                {
                    "channel_type": "email",
                    "target_ref": "o***@example.com",
                    "enabled": True,
                    "draft_ready": True,
                    "reason": "email channel ready.",
                },
                {
                    "channel_type": "file",
                    "target_ref": "operator-chat-channel",
                    "enabled": True,
                    "draft_ready": False,
                    "reason": "chat is not selected for actual dispatch.",
                },
            ],
            "message": {
                "subject": "[Trading System] Daily Operator Brief - OK - 2026-05-24",
                "title": "Daily Trading System Operator Brief - 2026-05-24",
                "body_markdown": "# Operator Brief\nReady.\n",
                "body_length": 24,
                "contains_markdown": True,
            },
        },
        "hashes": {
            "dispatch_preview_hash": "sha256:preview",
            "approval_gate_dispatch_preview_hash": "sha256:preview",
            "draft_hash": "sha256:abc123",
            "hash_algorithm": "sha256",
            "hash_scope": "canonical_draft_dispatch_json",
        },
        "decision": {
            "final_status": "DRAFT_READY",
            "ready_for_actual_dispatch": True,
            "human_action_required": True,
            "next_recommended_action": (
                "Review this local draft dispatch artifact before any future actual dispatch task; "
                "TRADING-034 sent nothing."
            ),
        },
        "safety": {
            "external_side_effects": False,
            "network_access_required": False,
            "secrets_required": False,
            "recipient_masking_applied": True,
            "approval_gate_required": True,
            "approval_gate_passed": True,
            "sensitive_content_flags": [],
        },
        "reasons": ["Approved TRADING-033 gate and TRADING-032 preview are aligned."],
        "warnings": [],
        "output_artifacts": {
            "draft_dispatch_json": {
                "path": (
                    "data/derived/operator_briefs/notifications/draft_dispatch/" f"{json_path.name}"
                )
            },
            "draft_dispatch_markdown": {
                "path": (
                    "data/derived/operator_briefs/notifications/draft_dispatch/"
                    f"{markdown_path.name}"
                )
            },
            "latest_json": {
                "path": "data/derived/operator_briefs/notifications/draft_dispatch/latest.json"
            },
            "latest_markdown": {
                "path": "data/derived/operator_briefs/notifications/draft_dispatch/latest.md"
            },
            "run_log": {
                "path": "data/derived/operator_briefs/notifications/draft_dispatch/run.log"
            },
        },
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text("# Operator Brief Notification Draft Dispatch\n", encoding="utf-8")
    latest_json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    latest_markdown_path.write_text(
        "# Operator Brief Notification Draft Dispatch\n",
        encoding="utf-8",
    )
    run_log_path.write_text("final_status=DRAFT_READY\n", encoding="utf-8")
    return payload


def _write_retry_execution_dry_run(tmp_path: Path, as_of: date) -> dict[str, Any]:
    root = tmp_path / "outputs" / "retry_execution_dry_run"
    json_path = root / f"retry_execution_dry_run_{as_of.isoformat()}.json"
    markdown_path = json_path.with_suffix(".md")
    log_path = json_path.with_suffix(".log")
    payload = {
        "schema_version": "1.0",
        "report_type": "retry_execution_dry_run",
        "task_id": "TRADING-038",
        "date": as_of.isoformat(),
        "mode": "dry_run_only",
        "production_effect": "none",
        "manual_review_only": True,
        "retry_execution_dry_run_only": True,
        "dry_run_only": True,
        "read_only": True,
        "approval_record_modified": False,
        "approval_state_modified": False,
        "email_sent": False,
        "gmail_draft_created": False,
        "gmail_draft_modified": False,
        "slack_sent": False,
        "discord_sent": False,
        "webhook_called": False,
        "mobile_push_sent": False,
        "retry_executed": False,
        "actual_retry_executed": False,
        "external_delivery_executed": False,
        "delivery_state_mutated": False,
        "state_mutation_executed": False,
        "production_parameters_modified": False,
        "retry_candidate_queue_executed_by_dry_run": False,
        "notification_delivery_failure_classification_executed_by_dry_run": False,
        "notification_delivery_audit_executed_by_dry_run": False,
        "notification_draft_executed_by_dry_run": False,
        "delivery_preflight_executed_by_dry_run": False,
        "draft_dispatch_executed_by_dry_run": False,
        "operator_brief_executed_by_dry_run": False,
        "pipelines_executed_by_dry_run": False,
        "data_downloaded_by_dry_run": False,
        "apply_executed_by_dry_run": False,
        "rollback_executed_by_dry_run": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "metadata": {
            "task_id": "TRADING-038",
            "task_name": "Manual Approval Record / Retry Execution Dry Run",
            "mode": "dry_run_only",
            "production_effect": "none",
            "manual_review_only": True,
            "generated_at": "2026-05-26T00:00:00Z",
            "schema_version": "1.0",
        },
        "source_queue": {
            "task_id": "TRADING-037",
            "queue_report_path": (
                "outputs/retry_candidate_queue/retry_candidate_queue_2026-05-26.json"
            ),
            "queue_status": "PENDING_APPROVAL",
            "source_available": True,
            "source_parse_status": "OK",
            "source_error": "",
        },
        "approval_record": {
            "approval_record_path": (
                "inputs/manual_retry_approvals/manual_retry_approval_2026-05-26.json"
            ),
            "approval_record_available": True,
            "approval_parse_status": "OK",
            "approval_error": "",
            "approved_candidate_count": 1,
            "rejected_candidate_count": 0,
            "unapproved_candidate_count": 0,
            "approval_mismatch_count": 0,
        },
        "dry_run_summary": {
            "dry_run_status": "READY_FOR_DRY_RUN",
            "total_candidates": 1,
            "approved_for_dry_run": 1,
            "blocked_from_dry_run": 0,
            "simulated_retry_actions": 1,
            "real_retry_allowed": False,
            "external_delivery_allowed": False,
            "production_state_mutation_allowed": False,
        },
        "simulated_retry_actions": [
            {
                "candidate_id": "retry_candidate_2026-05-26_001",
                "dry_run_action_id": "dry_run_retry_2026-05-26_001",
                "source_category": "TRANSIENT_DELIVERY_FAILURE",
                "simulated_channel": "notification_channel_placeholder",
                "simulated_target": "redacted_or_placeholder",
                "payload_available": False,
                "would_retry": True,
                "actual_retry_executed": False,
                "external_delivery_executed": False,
                "state_mutation_executed": False,
                "safety_result": "PASS",
            }
        ],
        "blocked_items": [],
        "recommended_actions": ["Review the simulated retry actions."],
        "safety_invariants": {
            "dry_run_only": True,
            "no_external_delivery": True,
            "no_retry_execution": True,
            "no_state_mutation": True,
            "no_production_parameter_change": True,
            "approval_record_is_input_only": True,
            "dashboard_read_only": True,
        },
        "output_artifacts": {
            "retry_execution_dry_run_json": {"path": str(json_path)},
            "retry_execution_dry_run_markdown": {"path": str(markdown_path)},
            "run_log": {"path": str(log_path)},
        },
    }
    root.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(
        "# Manual Approval Record / Retry Execution Dry Run\n",
        encoding="utf-8",
    )
    log_path.write_text("dry_run_status=READY_FOR_DRY_RUN\n", encoding="utf-8")
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
