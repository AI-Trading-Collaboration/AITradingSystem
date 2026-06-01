from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.calculation_explainers import (
    build_calculation_explainers_payload,
    write_calculation_explainers_json,
)
from ai_trading_system.reports.reader_brief import (
    build_reader_brief_payload,
    build_reader_brief_quality_payload,
    render_reader_brief_html,
)


def test_reader_brief_payload_summarizes_daily_decision_inputs(tmp_path: Path) -> None:
    inputs = _write_reader_brief_inputs(tmp_path)

    payload = build_reader_brief_payload(
        as_of=date(2026, 5, 4),
        reports_dir=tmp_path,
        decision_snapshot_path=inputs["snapshot"],
        calculation_explainers_path=inputs["calculation_explainers"],
        daily_decision_summary_path=inputs["daily_decision_summary"],
        evidence_dashboard_json_path=inputs["evidence_dashboard"],
        daily_task_dashboard_json_path=inputs["daily_task_dashboard"],
        daily_report_path=inputs["daily_report"],
        trace_bundle_path=inputs["trace_bundle"],
        score_change_attribution_path=inputs["score_change_attribution"],
        market_panel_path=inputs["market_panel"],
        research_governance_summary_path=inputs["research_governance_summary"],
        report_index_path=inputs["report_index"],
        documentation_contract_path=inputs["documentation_contract"],
    )

    assert payload["status"] == "LIMITED_READER_CONTEXT"
    assert payload["production_effect"] == "none"
    assert payload["reader_entry_role"] == "daily_reading_home"
    assert payload["status_panel"]["build_status"] == "LIMITED_READER_CONTEXT"
    assert payload["status_panel"]["decision_usability"] == "LIMITED_CONTEXT"
    assert payload["status_panel"]["research_promotion_status"] == "BLOCKED_BY_MISSING_ARTIFACTS"
    assert payload["action_checklist"][0]["impact_type"] == "today_decision"
    assert payload["narrative_executive_summary"]["today_conclusion"]
    assert payload["narrative_executive_summary"]["production_effect_statement"]
    assert payload["executive_summary"]["manual_review_count"] >= 1
    assert payload["run_context"]["market_regime"] == "ai_after_chatgpt"
    assert payload["executive_decision"]["action"] == "观察"
    assert payload["executive_decision"]["final_risk_asset_ai_position"] == "40%-60%"
    assert payload["executive_decision"]["data_gate"] == "PASS"
    assert payload["executive_decision"]["binding_gate_id"] == "valuation"
    assert payload["executive_decision"]["not_trade_instruction"] is True
    assert payload["score_to_position_funnel"]["steps"][1]["metric_id"] == "overall_score"
    assert payload["score_change_attribution_summary"]["overall_score_delta"] == 3.0
    assert payload["score_change_attribution_summary"]["drivers"][0]["driver"] == "trend"
    assert payload["market_situation_snapshot"]["market_price_panel_status"] == "AVAILABLE"
    assert "SPY 1D=+1.00%" in payload["executive_summary"]["market_movement"]
    assert not any(
        item["artifact_id"] == "market_panel"
        for item in payload["missing_limited_artifact_impact"]["items"]
    )
    assert payload["report_index_summary"]["missing_count"] == 1
    assert payload["report_index_summary"]["problem_reports"][0]["report_id"] == "data_quality"
    assert payload["task_cadence_calendar"]["groups"][0]["cadence"] == "daily"
    impact = payload["missing_limited_artifact_impact"]
    assert any(item["impact_level"] == "BLOCKING" for item in impact["items"])
    impact_summary = impact["impact_summary"]
    assert {item["chain"] for item in impact_summary} == {
        "今日评分链路",
        "阅读上下文",
        "研究/权重晋升链路",
    }
    assert (
        next(item for item in impact_summary if item["chain"] == "研究/权重晋升链路")["status"]
        == "BLOCKED_BY_MISSING_ARTIFACTS"
    )
    assert "trend" in payload["contribution_summary"]["top_positive_contributors"]
    assert "今日 score +3.00" in payload["score_change_narrative"]["summary"]
    assert payload["documentation_contract_summary"]["status"] == "PASS"
    trend = payload["component_score_explainability"]["components"][0]
    assert trend["component"] == "trend"
    assert trend["effective_weight"] == 0.25
    assert trend["contribution_to_overall_score"] == 18.0
    valuation_gate = next(
        row for row in payload["binding_gate_ladder"]["gates"] if row["gate_id"] == "valuation"
    )
    assert valuation_gate["binding"] is True
    assert payload["data_quality_pit_safety"]["data_gate_status"] == "PASS"
    assert payload["data_quality_pit_safety"]["as_of_date"] == "2026-05-04"
    assert payload["data_quality_pit_safety"]["future_data_check"] == "PASS"
    assert payload["backtest_shadow_governance"]["source"] == "research_governance_summary"
    assert payload["backtest_shadow_governance"]["promotion_status"] == "NOT_PROMOTABLE"
    assert (
        "promotion_status = NOT_PROMOTABLE" in payload["executive_summary"]["research_governance"]
    )
    assert (
        "research governance status = PASS_WITH_LIMITATIONS"
        in payload["narrative_executive_summary"]["research_governance_summary"]
    )
    assert payload["backtest_shadow_governance"]["candidate_research_count"] == 3
    assert payload["manual_review_queue"]["status"] == "ACTION_REQUIRED"
    assert payload["manual_review_queue"]["groups"][0]["label"] == "Critical / Must Review Today"
    assert payload["manual_review_queue"]["top_items"][0]["impact_type"] in {
        "today_decision",
        "research_promotion",
    }
    assert payload["manual_review_queue"]["impact_groups"][0]["label"] == "影响今日结论"
    assert all(
        "recommended_next_action" in item for item in payload["manual_review_queue"]["items"]
    )
    assert any(
        item["category"] == "report_freshness" for item in payload["manual_review_queue"]["items"]
    )
    assert any(item["artifact_id"] == "daily_report" for item in payload["appendix_links"])
    assert any(item["artifact_id"] == "trace_bundle" for item in payload["appendix_links"])
    assert any(item["artifact_id"] == "artifact_catalog" for item in payload["report_navigation"])
    assert any(
        item["artifact_id"] == "etf_portfolio_brief"
        and item["freshness_status"] == "FRESH"
        and item["navigation_source"] == "report_index_runtime"
        for item in payload["report_navigation"]
    )
    calibration = payload["etf_calibration_experiments"]
    assert calibration["availability"] == "AVAILABLE"
    assert calibration["latest_experiment_pack"] == "etf_calibration_v1"
    assert calibration["top_candidate"] == "etf-exp-20260504T000000Z:base_ai_growth"
    assert calibration["rejected_count"] == 2
    assert calibration["active_shadow_candidates"] == 1
    assert calibration["weekly_review_action"] == "promote_to_longer_observation"
    assert calibration["safety_status"] == (
        "observe_only=true; production_effect=none; broker_action=none"
    )
    forward = payload["etf_forward_simulation"]
    assert forward["availability"] == "AVAILABLE"
    assert forward["active_shadow_candidates"] == 1
    assert forward["watch_count"] == 1
    assert forward["watchlist_attention_count"] == 1
    assert forward["best_candidate"].startswith("etf-exp-20260504T000000Z:base_ai_growth")
    assert forward["safety_status"] == (
        "observe_only=true; production_effect=none; broker_action=none; "
        "manual_review_required=true"
    )
    assert forward["decision_input_usage"] == "none; forward metrics are evaluation-only"
    core_items = payload["report_navigation_groups"]["groups"][0]["items"]
    daily_summary_rows = [
        item for item in core_items if item["artifact_id"] == "daily_decision_summary"
    ]
    assert len(daily_summary_rows) == 1
    assert daily_summary_rows[0]["status"] == "limited"
    assert len(daily_summary_rows[0]["navigation_sources"]) == 2
    assert payload["report_navigation_groups"]["groups"][0]["purpose"] == "Core decision artifacts"


def test_reader_brief_missing_optional_artifacts_degrades_to_warnings(tmp_path: Path) -> None:
    snapshot_path = _write_decision_snapshot(tmp_path)

    payload = build_reader_brief_payload(
        as_of=date(2026, 5, 4),
        reports_dir=tmp_path,
        decision_snapshot_path=snapshot_path,
        calculation_explainers_path=tmp_path / "missing_calculation_explainers.json",
        daily_decision_summary_path=tmp_path / "missing_daily_decision_summary.json",
        evidence_dashboard_json_path=tmp_path / "missing_evidence_dashboard.json",
        daily_task_dashboard_json_path=tmp_path / "missing_daily_task_dashboard.json",
        daily_report_path=tmp_path / "missing_daily_score.md",
        trace_bundle_path=tmp_path / "missing_trace.json",
        score_change_attribution_path=tmp_path / "missing_score_change.json",
        market_panel_path=tmp_path / "missing_market_panel.json",
        research_governance_summary_path=tmp_path / "missing_research_governance.json",
        report_index_path=tmp_path / "missing_report_index.json",
        documentation_contract_path=tmp_path / "missing_documentation_contract.json",
    )

    assert payload["status"] == "LIMITED_READER_CONTEXT"
    assert payload["executive_decision"]["binding_gate_id"] == "valuation"
    assert payload["component_score_explainability"]["status"] == "AVAILABLE"
    assert payload["backtest_shadow_governance"]["availability"] == "LIMITED"
    assert payload["etf_calibration_experiments"]["availability"] == "MISSING"
    assert payload["etf_calibration_experiments"]["safety_status"] == "MISSING"
    assert payload["report_index_summary"]["availability"] == "MISSING"
    assert payload["documentation_contract_summary"]["availability"] == "MISSING"
    assert payload["task_cadence_calendar"]["availability"] == "REGISTRY_FALLBACK"
    assert payload["task_cadence_calendar"]["source"] == "registry_fallback"
    assert payload["source_inputs"]["daily_report"]["availability"] == "MISSING"
    assert "daily_report_missing" in ";".join(payload["warnings"])
    assert any(
        item["artifact_id"] == "task_cadence_calendar" and item["impact_level"] == "IMPORTANT"
        for item in payload["missing_limited_artifact_impact"]["items"]
    )
    assert any(
        item["artifact_id"] == "market_panel" and item["impact_level"] == "IMPORTANT"
        for item in payload["missing_limited_artifact_impact"]["items"]
    )
    html = render_reader_brief_html(payload)
    assert "ETF Calibration Experiments" in html
    assert "ETF Forward Simulation" in html
    assert 'safety_status</th><td><span class="status-badge status-missing">MISSING</span>' in html
    assert "impact-group impact-important" in html
    assert "status-badge status-important" in html


def test_reports_reader_brief_cli_writes_html_and_json(tmp_path: Path) -> None:
    inputs = _write_reader_brief_inputs(tmp_path)
    html_path = tmp_path / "reader_brief_2026-05-04.html"
    json_path = tmp_path / "reader_brief_2026-05-04.json"

    result = CliRunner().invoke(
        app,
        [
            "reports",
            "reader-brief",
            "--date",
            "2026-05-04",
            "--reports-dir",
            str(tmp_path),
            "--decision-snapshot-path",
            str(inputs["snapshot"]),
            "--calculation-explainers-path",
            str(inputs["calculation_explainers"]),
            "--daily-decision-summary-path",
            str(inputs["daily_decision_summary"]),
            "--evidence-dashboard-json-path",
            str(inputs["evidence_dashboard"]),
            "--daily-task-dashboard-json-path",
            str(inputs["daily_task_dashboard"]),
            "--daily-report-path",
            str(inputs["daily_report"]),
            "--trace-bundle-path",
            str(inputs["trace_bundle"]),
            "--score-change-attribution-path",
            str(inputs["score_change_attribution"]),
            "--market-panel-path",
            str(inputs["market_panel"]),
            "--research-governance-summary-path",
            str(inputs["research_governance_summary"]),
            "--report-index-path",
            str(inputs["report_index"]),
            "--documentation-contract-path",
            str(inputs["documentation_contract"]),
            "--output-path",
            str(html_path),
            "--json-output-path",
            str(json_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "Reader Brief：LIMITED_READER_CONTEXT" in result.output
    assert "不生成交易指令" in result.output
    assert html_path.exists()
    assert json_path.exists()
    html = html_path.read_text(encoding="utf-8")
    assert "Reader Brief" in html
    assert "Executive Summary" in html
    assert "Reader Brief Build Status" in html
    assert "Decision Usability" in html
    assert "Research Promotion Status" in html
    assert "今日建议动作" in html
    assert "今日结论" in html
    assert "Missing / Limited Artifact Impact" in html
    assert "研究/权重晋升链路" in html
    assert "Core Decision" in html
    assert "Score &amp; Decision Funnel" in html
    assert "综合评分" in html
    assert "Score Change Attribution" in html
    assert "Report Index Freshness" in html
    assert "Task Cadence Calendar" in html
    assert "Report Navigation" in html
    assert "Top 3 Review Items Today" in html
    assert "影响今日结论" in html
    assert "影响研究晋升" in html
    assert "Critical / Must Review Today" in html
    assert "Contribution Summary" in html
    assert "<details" in html
    assert "不是实盘交易指令" in html
    assert 'class="summary-card-grid"' in html
    assert "Final Action" in html
    assert "Final AI Position" in html
    assert "Binding Gate" in html
    assert "Market Movement" in html
    assert "Manual Review" in html
    assert "production_effect=none" in html
    assert "observe_only=true; production_effect=none; broker_action=none" in html
    assert "status-badge status-limited-reader-context" in html
    assert "status-badge status-not-promotable" in html
    assert 'class="market-card-grid"' in html
    assert "SPY" in html
    assert "QQQ" in html
    assert "SMH" in html
    assert "SOXX" in html
    assert "VIX" in html
    assert "DGS10" in html
    assert "1D" in html
    assert "5D" in html
    assert "20D" in html
    assert 'class="funnel-flow"' in html
    assert 'class="funnel-node binding"' in html
    assert 'class="binding-row"' in html
    assert "recommended-action" in html
    assert "impact-group impact-blocking" in html
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["production_effect"] == "none"
    assert payload["executive_decision"]["not_trade_instruction"] is True
    assert render_reader_brief_html(payload) == render_reader_brief_html(payload)


def test_reader_brief_quality_payload_summarizes_reader_ux_checks(tmp_path: Path) -> None:
    inputs = _write_reader_brief_inputs(tmp_path)
    payload = build_reader_brief_payload(
        as_of=date(2026, 5, 4),
        reports_dir=tmp_path,
        decision_snapshot_path=inputs["snapshot"],
        calculation_explainers_path=inputs["calculation_explainers"],
        daily_decision_summary_path=inputs["daily_decision_summary"],
        evidence_dashboard_json_path=inputs["evidence_dashboard"],
        daily_task_dashboard_json_path=inputs["daily_task_dashboard"],
        daily_report_path=inputs["daily_report"],
        trace_bundle_path=inputs["trace_bundle"],
        score_change_attribution_path=inputs["score_change_attribution"],
        market_panel_path=inputs["market_panel"],
        research_governance_summary_path=inputs["research_governance_summary"],
        report_index_path=inputs["report_index"],
        documentation_contract_path=inputs["documentation_contract"],
    )

    quality = build_reader_brief_quality_payload(
        reader_brief_payload=payload,
        reader_brief_json_path=tmp_path / "reader_brief_2026-05-04.json",
        reader_brief_html_path=tmp_path / "reader_brief_2026-05-04.html",
    )

    assert quality["report_type"] == "reader_brief_quality"
    assert quality["status"] == "LIMITED_READER_CONTEXT"
    assert quality["production_effect"] == "none"
    assert quality["summary"]["check_count"] >= 7
    assert quality["summary"]["failed_check_count"] == 0
    assert any(check["check_id"] == "grouped_report_navigation" for check in quality["checks"])


def test_reports_validate_reader_brief_cli_writes_quality_outputs(tmp_path: Path) -> None:
    inputs = _write_reader_brief_inputs(tmp_path)
    reader_json = tmp_path / "reader_brief_2026-05-04.json"
    reader_html = tmp_path / "reader_brief_2026-05-04.html"
    result = CliRunner().invoke(
        app,
        [
            "reports",
            "reader-brief",
            "--date",
            "2026-05-04",
            "--reports-dir",
            str(tmp_path),
            "--decision-snapshot-path",
            str(inputs["snapshot"]),
            "--calculation-explainers-path",
            str(inputs["calculation_explainers"]),
            "--daily-decision-summary-path",
            str(inputs["daily_decision_summary"]),
            "--evidence-dashboard-json-path",
            str(inputs["evidence_dashboard"]),
            "--daily-task-dashboard-json-path",
            str(inputs["daily_task_dashboard"]),
            "--daily-report-path",
            str(inputs["daily_report"]),
            "--trace-bundle-path",
            str(inputs["trace_bundle"]),
            "--score-change-attribution-path",
            str(inputs["score_change_attribution"]),
            "--market-panel-path",
            str(inputs["market_panel"]),
            "--research-governance-summary-path",
            str(inputs["research_governance_summary"]),
            "--report-index-path",
            str(inputs["report_index"]),
            "--documentation-contract-path",
            str(inputs["documentation_contract"]),
            "--output-path",
            str(reader_html),
            "--json-output-path",
            str(reader_json),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    assert result.exit_code == 0, result.output

    quality_json = tmp_path / "reader_brief_quality_2026-05-04.json"
    quality_md = tmp_path / "reader_brief_quality_2026-05-04.md"
    quality_result = CliRunner().invoke(
        app,
        [
            "reports",
            "validate-reader-brief",
            "--date",
            "2026-05-04",
            "--reports-dir",
            str(tmp_path),
            "--reader-brief-json-path",
            str(reader_json),
            "--reader-brief-html-path",
            str(reader_html),
            "--json-output-path",
            str(quality_json),
            "--markdown-output-path",
            str(quality_md),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert quality_result.exit_code == 0, quality_result.output
    assert "Reader Brief quality：LIMITED_READER_CONTEXT" in quality_result.output
    assert quality_json.exists()
    assert quality_md.exists()
    quality_payload = json.loads(quality_json.read_text(encoding="utf-8"))
    assert quality_payload["report_type"] == "reader_brief_quality"


def _write_reader_brief_inputs(tmp_path: Path) -> dict[str, Path]:
    snapshot_path = _write_decision_snapshot(tmp_path)
    calculation_explainers_path = tmp_path / "calculation_explainers_2026-05-04.json"
    calculation_payload = build_calculation_explainers_payload(
        as_of=date(2026, 5, 4),
        decision_snapshot_path=snapshot_path,
        scores_daily_path=tmp_path / "scores_daily.csv",
    )
    write_calculation_explainers_json(calculation_payload, calculation_explainers_path)

    daily_decision_summary_path = tmp_path / "daily_decision_summary_2026-05-04.json"
    daily_decision_summary_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "daily_decision_summary",
                "as_of": "2026-05-04",
                "run_id": "daily_run:2026-05-04:test",
                "production_effect": "none",
                "data_gate": {
                    "availability": "available",
                    "status": "PASS",
                    "blocking_reasons": [],
                },
                "investment_conclusion": {
                    "availability": "available",
                    "action_bias": "观察",
                    "confidence": "0.71",
                    "position_band": "40%-60%",
                    "major_risks": ["估值拥挤"],
                    "production_effect": "none",
                },
                "parameter_governance": {
                    "availability": "available",
                    "status": "PASS_WITH_LIMITATIONS",
                    "promotion_status": "NOT_PROMOTABLE",
                    "blocking_reasons": ["available=8，contract floor=30"],
                },
                "feedback_review": {
                    "availability": "available",
                    "status": "PASS_WITH_LIMITATIONS",
                    "summary": "shadow return 9.02%，production 4.74%",
                },
                "system_health": {"status": "PASS", "warnings": []},
                "source_artifacts": [],
                "hrefs": {},
                "checksums": {},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    evidence_dashboard_path = tmp_path / "evidence_dashboard_2026-05-04.json"
    evidence_dashboard_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "evidence_dashboard",
                "production_effect": "none",
                "decision": {
                    "action": "观察，不形成交易结论",
                    "final_risk_asset_ai_position": "40%-60%",
                    "total_risk_asset_budget": "40%-60%",
                    "confidence": "0.71",
                    "data_gate": "PASS",
                    "largest_constraint": "估值拥挤 上限 40%",
                    "market_regime": "ai_after_chatgpt",
                },
                "quality": {"market_data_status": "PASS"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    daily_task_dashboard_path = tmp_path / "daily_task_dashboard_2026-05-04.json"
    daily_task_dashboard_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "daily_task_dashboard",
                "run_id": "daily_run:2026-05-04:test",
                "production_effect": "none",
                "summary": {"task_count": 4, "risk_count": 1},
                "key_conclusions": [
                    {
                        "area": "Shadow Iteration",
                        "status": "PASS_WITH_LIMITATIONS",
                        "primary": "shadow candidate 仍处 observe-only",
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    daily_report_path = tmp_path / "daily_score_2026-05-04.md"
    daily_report_path.write_text(
        "# AI 产业链日报\n\n- 生产影响：advisory only\n",
        encoding="utf-8",
    )
    etf_brief_path = tmp_path / "reports" / "etf_portfolio" / "2026-05-04_portfolio_brief.md"
    etf_brief_path.parent.mkdir(parents=True)
    etf_brief_path.write_text(
        "# AITradingSystem Daily Portfolio Brief - 2026-05-04\n\n"
        "- Data Quality: PASS\n- production_effect: none\n",
        encoding="utf-8",
    )
    experiment_run_dir = (
        tmp_path
        / "reports"
        / "etf_portfolio"
        / "experiments"
        / "etf-exp-20260504T000000Z"
    )
    experiment_run_dir.mkdir(parents=True)
    experiment_manifest_path = experiment_run_dir / "run_manifest.json"
    experiment_manifest_path.write_text(
        json.dumps(
            {
                "run_id": "etf-exp-20260504T000000Z",
                "pack_id": "etf_calibration_v1",
                "start_date": "2022-12-01",
                "end_date": "2026-05-04",
                "observe_only": True,
                "production_effect": "none",
                "broker_action": "none",
                "manual_review_required": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    experiment_comparison_path = experiment_run_dir / "comparison_report.json"
    experiment_comparison_path.write_text(
        json.dumps(
            {
                "report_type": "etf_experiment_comparison",
                "run_metadata": {
                    "run_id": "etf-exp-20260504T000000Z",
                    "pack_id": "etf_calibration_v1",
                },
                "ranking_policy_status": "APPLIED:risk_adjusted_v1",
                "observe_only": True,
                "production_effect": "none",
                "broker_action": "none",
                "manual_review_required": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    candidate_selection_path = experiment_run_dir / "candidate_selection_report.json"
    candidate_selection_path.write_text(
        json.dumps(
            {
                "report_type": "etf_experiment_candidate_selection",
                "run_metadata": {
                    "run_id": "etf-exp-20260504T000000Z",
                    "pack_id": "etf_calibration_v1",
                },
                "selection_summary": {
                    "status": "PASS",
                    "rejected_count": 1,
                    "blocked_count": 1,
                },
                "candidates": [
                    {
                        "candidate_id": "etf-exp-20260504T000000Z:base_ai_growth",
                        "experiment_id": "base_ai_growth",
                        "selection_status": "eligible_for_shadow",
                    }
                ],
                "observe_only": True,
                "production_effect": "none",
                "broker_action": "none",
                "manual_review_required": True,
                "production_promotion_allowed": False,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    shadow_registry_path = tmp_path / "data" / "simulation" / "etf_shadow_candidates.json"
    shadow_registry_path.parent.mkdir(parents=True)
    shadow_registry_path.write_text(
        json.dumps(
            {
                "registry_type": "etf_shadow_candidates",
                "candidate_count": 1,
                "candidates": [
                    {
                        "candidate_id": "etf-exp-20260504T000000Z:base_ai_growth",
                        "experiment_id": "base_ai_growth",
                        "status": "active_shadow_observation",
                    }
                ],
                "observe_only": True,
                "production_effect": "none",
                "broker_action": "none",
                "manual_review_required": True,
                "production_promotion_allowed": False,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    weekly_review_dir = experiment_run_dir.parent / "weekly_reviews"
    weekly_review_dir.mkdir(parents=True)
    weekly_review_path = weekly_review_dir / "weekly_review_2026-05-04.json"
    weekly_review_path.write_text(
        json.dumps(
            {
                "report_type": "etf_experiment_weekly_review",
                "review_period": {"as_of": "2026-05-04"},
                "summary": {"status": "READY_FOR_LONGER_OBSERVATION_REVIEW"},
                "candidate_reviews": [
                    {
                        "candidate_id": "etf-exp-20260504T000000Z:base_ai_growth",
                        "recommended_action": "promote_to_longer_observation",
                    }
                ],
                "observe_only": True,
                "production_effect": "none",
                "broker_action": "none",
                "manual_review_required": True,
                "production_promotion_allowed": False,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    forward_dashboard_dir = tmp_path / "reports" / "etf_portfolio" / "forward" / "dashboard"
    forward_dashboard_dir.mkdir(parents=True)
    forward_dashboard_path = forward_dashboard_dir / "forward_dashboard_2026-05-04.json"
    forward_dashboard_path.write_text(
        json.dumps(
            {
                "report_type": "etf_forward_dashboard",
                "status": "WATCH",
                "as_of": "2026-05-04",
                "candidate_summary_table": [
                    {
                        "candidate_id": "etf-exp-20260504T000000Z:base_ai_growth",
                        "status": "watch",
                        "days_since_enrollment": 44,
                        "return_since_enrollment": 0.012,
                        "excess_return_vs_baseline": 0.018,
                        "excess_return_vs_QQQ": 0.006,
                        "excess_return_vs_SPY": 0.022,
                        "excess_return_vs_SMH": -0.004,
                        "max_drawdown_since_enrollment": -0.035,
                        "turnover_since_enrollment": 0.28,
                        "constraint_hits_since_enrollment": 0,
                        "last_evaluated_date": "2026-05-04",
                        "recommended_action": "watch",
                    }
                ],
                "status_summary": {
                    "active_candidate_count": 1,
                    "needs_more_data_count": 0,
                    "watch_count": 1,
                    "reject_pending_review_count": 0,
                },
                "observe_only": True,
                "production_effect": "none",
                "broker_action": "none",
                "manual_review_required": True,
                "production_promotion_allowed": False,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    forward_watchlist_dir = tmp_path / "reports" / "etf_portfolio" / "forward" / "watchlist"
    forward_watchlist_dir.mkdir(parents=True)
    forward_watchlist_path = forward_watchlist_dir / "forward_watchlist_2026-05-04.json"
    forward_watchlist_path.write_text(
        json.dumps(
            {
                "report_type": "etf_forward_watchlist",
                "status": "ATTENTION_REQUIRED",
                "as_of": "2026-05-04",
                "summary": {"item_count": 1},
                "attention_required": [
                    {
                        "candidate_id": "etf-exp-20260504T000000Z:base_ai_growth",
                        "issue": "candidate moved to watch",
                        "severity": "warning",
                        "recommended_action": "needs_manual_review",
                    }
                ],
                "observe_only": True,
                "production_effect": "none",
                "broker_action": "none",
                "manual_review_required": True,
                "production_promotion_allowed": False,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    trace_bundle_path = tmp_path / "daily_score_2026-05-04_trace.json"
    trace_bundle_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "claims": [{"claim_id": "overall_claim", "text": "score 支持观察"}],
                "dataset_refs": [{"dataset_id": "scores_daily"}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    score_change_path = tmp_path / "score_change_attribution_2026-05-04.json"
    score_change_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "score_change_attribution",
                "as_of": "2026-05-04",
                "status": "PASS",
                "production_effect": "none",
                "comparison_window": {
                    "previous_signal_date": "2026-05-03",
                    "current_signal_date": "2026-05-04",
                },
                "overall_score_delta": {"delta": 3.0},
                "position_attribution": {"final_max_delta": -0.1},
                "top_changes": {
                    "positive_contribution_drivers": [
                        {"component": "trend", "contribution_delta": 5.35}
                    ],
                    "negative_contribution_drivers": [
                        {"component": "valuation", "contribution_delta": -3.6}
                    ],
                    "weight_changes": [{"component": "trend", "effective_weight_delta": 0.05}],
                    "coverage_changes": [],
                    "gate_changes": [
                        {"gate_id": "valuation", "cap_delta": -0.1, "change_flags": ["CAP_CHANGED"]}
                    ],
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    market_panel_path = tmp_path / "market_panel_2026-05-04.json"
    market_panel_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "market_panel",
                "as_of": "2026-05-04",
                "status": "PASS",
                "production_effect": "none",
                "data_quality": {
                    "status": "PASS",
                    "report_path": str(tmp_path / "data_quality.md"),
                },
                "source_artifacts": {
                    "prices_daily": {
                        "path": str(tmp_path / "prices_daily.csv"),
                        "exists": True,
                    },
                    "rates_daily": {
                        "path": str(tmp_path / "rates_daily.csv"),
                        "exists": True,
                    },
                },
                "summary": {
                    "proxy_count": 4,
                    "available_proxy_count": 4,
                    "missing_proxy_count": 0,
                    "missing_roles": [],
                    "market_movement_sentence": (
                        "SPY 1D=+1.00%；SMH 1D=+2.00%；" "^VIX 1D=-3.00%；DGS10 1D=+0.0100pp。"
                    ),
                },
                "proxies": [
                    {
                        "symbol": "SPY",
                        "role": "benchmark_proxy",
                        "last_price": 505.0,
                        "return_1d": 0.01,
                        "return_5d": 0.02,
                        "return_20d": 0.04,
                        "trend_label": "UP_20D",
                        "risk_interpretation": "benchmark proxy 上行。",
                        "data_status": "AVAILABLE",
                        "source_artifact": str(tmp_path / "prices_daily.csv"),
                        "change_mode": "ratio",
                        "production_effect": "none",
                    },
                    {
                        "symbol": "SMH",
                        "role": "ai_sector_proxy",
                        "last_price": 250.0,
                        "return_1d": 0.02,
                        "return_5d": 0.03,
                        "return_20d": 0.08,
                        "trend_label": "UP_20D",
                        "risk_interpretation": "AI sector proxy 上行。",
                        "data_status": "AVAILABLE",
                        "source_artifact": str(tmp_path / "prices_daily.csv"),
                        "change_mode": "ratio",
                        "production_effect": "none",
                    },
                    {
                        "symbol": "^VIX",
                        "role": "risk_proxy",
                        "last_price": 14.0,
                        "return_1d": -0.03,
                        "return_5d": -0.05,
                        "return_20d": -0.10,
                        "trend_label": "DOWN_20D",
                        "risk_interpretation": "VIX 下行，风险压力缓和。",
                        "data_status": "AVAILABLE",
                        "source_artifact": str(tmp_path / "prices_daily.csv"),
                        "change_mode": "ratio",
                        "production_effect": "none",
                    },
                    {
                        "symbol": "DGS10",
                        "role": "liquidity_proxy",
                        "last_price": 4.2,
                        "return_1d": 0.01,
                        "return_5d": 0.02,
                        "return_20d": 0.03,
                        "trend_label": "UP_20D",
                        "risk_interpretation": "10Y yield 上行，流动性压力上升。",
                        "data_status": "AVAILABLE",
                        "source_artifact": str(tmp_path / "rates_daily.csv"),
                        "change_mode": "difference",
                        "production_effect": "none",
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    research_governance_path = tmp_path / "research_governance_summary_2026-05-04.json"
    research_governance_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "research_governance_summary",
                "as_of": "2026-05-04",
                "status": "PASS_WITH_WARNINGS",
                "governance_status": "PASS_WITH_LIMITATIONS",
                "research_readiness": "READY_FOR_REVIEW",
                "promotion_status": "NOT_PROMOTABLE",
                "manual_review_required": True,
                "production_effect": "none",
                "summary_text": (
                    "当前研究治理状态为 PASS_WITH_LIMITATIONS；promotion_status=NOT_PROMOTABLE。"
                ),
                "backtest": {
                    "backtest_status": "AVAILABLE",
                    "robustness_status": "PASS_WITH_LIMITATIONS",
                },
                "weight_iteration": {
                    "promotion_status": "NOT_PROMOTABLE",
                    "weight_candidate_evaluation_status": "MISSING",
                    "weight_promotion_gate_status": "MISSING",
                },
                "shadow_observe": {
                    "shadow_monitor_status": "OK_MONITORING",
                    "rollback_recommended": False,
                },
                "sec_pit": {
                    "sec_pit_shadow_observe_status": "OK",
                    "pit_grade_policy": "B_RECONSTRUCTED_SEC_FILING_PIT",
                    "production_effect": "none",
                },
                "documentation": {
                    "documentation_contract_status": "PASS",
                    "report_index_status": "PASS_WITH_WARNINGS",
                },
                "manual_review_queue": [
                    {
                        "item_id": "missing_weight_promotion_gate",
                        "severity": "critical",
                        "category": "weight_iteration",
                        "reason": "缺少 weight_promotion_gate，promotion 默认阻断。",
                        "recommended_next_action": "run_weight_promotion_gate",
                        "decision_impact": "promotion_status=BLOCKED_BY_MISSING_ARTIFACTS",
                        "source_artifact": "weight_promotion_gate",
                        "production_effect": "none",
                    }
                ],
                "summary": {
                    "card_count": 8,
                    "missing_count": 1,
                    "warning_count": 2,
                    "manual_review_required_count": 4,
                    "groups": {
                        "Shadow observe-only": 2,
                        "Candidate / research-only": 3,
                        "Blocked / insufficient data": 1,
                    },
                },
                "cards": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    report_index_path = tmp_path / "report_index_2026-05-04.json"
    report_index_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "report_index",
                "as_of": "2026-05-04",
                "status": "PASS_WITH_WARNINGS",
                "production_effect": "none",
                "summary": {
                    "report_count": 4,
                    "available_count": 3,
                    "missing_count": 1,
                    "stale_count": 0,
                    "required_missing_count": 1,
                },
                "reports": [
                    {
                        "report_id": "daily_score",
                        "title": "Daily Score",
                        "cadence": "daily",
                        "owner": "system",
                        "freshness_status": "FRESH",
                        "artifact_date": "2026-05-04",
                        "latest_artifact_path": str(daily_report_path),
                        "exists": True,
                        "owner_action": "none",
                        "production_effect": "none",
                    },
                    {
                        "report_id": "data_quality",
                        "title": "Data Quality",
                        "cadence": "daily",
                        "owner": "system",
                        "freshness_status": "MISSING",
                        "artifact_date": "",
                        "latest_artifact_path": "",
                        "exists": False,
                        "owner_action": "run_validate_data",
                        "production_effect": "none",
                        "required_for_daily_reading": True,
                    },
                    {
                        "report_id": "daily_decision_summary",
                        "title": "Daily Decision Summary",
                        "cadence": "daily",
                        "owner": "system",
                        "freshness_status": "FRESH",
                        "artifact_status": "limited",
                        "artifact_date": "2026-05-04",
                        "latest_artifact_path": str(daily_decision_summary_path),
                        "exists": True,
                        "owner_action": "regenerate_daily_tasks_if_missing",
                        "production_effect": "none",
                    },
                    {
                        "report_id": "etf_portfolio_brief",
                        "title": "ETF Portfolio Brief",
                        "cadence": "daily",
                        "owner": "system",
                        "freshness_status": "FRESH",
                        "artifact_status": "AVAILABLE",
                        "artifact_date": "2026-05-04",
                        "latest_artifact_path": str(etf_brief_path),
                        "exists": True,
                        "owner_action": "run_aits_etf_run_daily_after_etf_data_quality_passes",
                        "production_effect": "none",
                    },
                    {
                        "report_id": "etf_experiment_run_manifest",
                        "title": "ETF Experiment Run Manifest",
                        "cadence": "ad_hoc",
                        "owner": "system",
                        "freshness_status": "FRESH",
                        "artifact_status": "AVAILABLE",
                        "artifact_date": "2026-05-04",
                        "latest_artifact_path": str(experiment_manifest_path),
                        "exists": True,
                        "owner_action": "review_experiment_manifest",
                        "production_effect": "none",
                    },
                    {
                        "report_id": "etf_experiment_comparison",
                        "title": "ETF Experiment Comparison",
                        "cadence": "ad_hoc",
                        "owner": "system",
                        "freshness_status": "FRESH",
                        "artifact_status": "AVAILABLE",
                        "artifact_date": "2026-05-04",
                        "latest_artifact_path": str(experiment_comparison_path),
                        "exists": True,
                        "owner_action": "review_experiment_comparison",
                        "production_effect": "none",
                    },
                    {
                        "report_id": "etf_experiment_candidate_selection",
                        "title": "ETF Experiment Candidate Selection",
                        "cadence": "ad_hoc",
                        "owner": "system",
                        "freshness_status": "FRESH",
                        "artifact_status": "PASS",
                        "artifact_date": "2026-05-04",
                        "latest_artifact_path": str(candidate_selection_path),
                        "exists": True,
                        "owner_action": "review_candidate_selection",
                        "production_effect": "none",
                    },
                    {
                        "report_id": "etf_shadow_candidates",
                        "title": "ETF Shadow Candidate Registry",
                        "cadence": "ad_hoc",
                        "owner": "system",
                        "freshness_status": "AVAILABLE_DATE_UNKNOWN",
                        "artifact_status": "AVAILABLE",
                        "artifact_date": "",
                        "latest_artifact_path": str(shadow_registry_path),
                        "exists": True,
                        "owner_action": "review_shadow_candidates",
                        "production_effect": "none",
                    },
                    {
                        "report_id": "etf_experiment_weekly_review",
                        "title": "ETF Experiment Weekly Review",
                        "cadence": "weekly",
                        "owner": "system",
                        "freshness_status": "FRESH",
                        "artifact_status": "READY_FOR_LONGER_OBSERVATION_REVIEW",
                        "artifact_date": "2026-05-04",
                        "latest_artifact_path": str(weekly_review_path),
                        "exists": True,
                        "owner_action": "review_weekly_experiment_report",
                        "production_effect": "none",
                    },
                    {
                        "report_id": "etf_forward_dashboard",
                        "title": "ETF Forward Simulation Dashboard",
                        "cadence": "daily",
                        "owner": "system",
                        "freshness_status": "FRESH",
                        "artifact_status": "WATCH",
                        "artifact_date": "2026-05-04",
                        "latest_artifact_path": str(forward_dashboard_path),
                        "exists": True,
                        "owner_action": "review_forward_dashboard",
                        "production_effect": "none",
                    },
                    {
                        "report_id": "etf_forward_watchlist",
                        "title": "ETF Forward Simulation Watchlist",
                        "cadence": "daily",
                        "owner": "system",
                        "freshness_status": "FRESH",
                        "artifact_status": "ATTENTION_REQUIRED",
                        "artifact_date": "2026-05-04",
                        "latest_artifact_path": str(forward_watchlist_path),
                        "exists": True,
                        "owner_action": "review_forward_watchlist",
                        "production_effect": "none",
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    documentation_contract_path = tmp_path / "documentation_contract_2026-05-04.json"
    documentation_contract_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "documentation_contract",
                "as_of": "2026-05-04",
                "status": "PASS",
                "production_effect": "none",
                "summary": {
                    "report_count": 3,
                    "error_count": 0,
                    "warning_count": 0,
                },
                "issues": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (tmp_path / "scores_daily.csv").write_text(
        "as_of,overall_score\n2026-05-04,73\n",
        encoding="utf-8",
    )
    return {
        "snapshot": snapshot_path,
        "calculation_explainers": calculation_explainers_path,
        "daily_decision_summary": daily_decision_summary_path,
        "evidence_dashboard": evidence_dashboard_path,
        "daily_task_dashboard": daily_task_dashboard_path,
        "daily_report": daily_report_path,
        "trace_bundle": trace_bundle_path,
        "score_change_attribution": score_change_path,
        "market_panel": market_panel_path,
        "research_governance_summary": research_governance_path,
        "report_index": report_index_path,
        "documentation_contract": documentation_contract_path,
    }


def _write_decision_snapshot(tmp_path: Path) -> Path:
    path = tmp_path / "decision_snapshot_2026-05-04.json"
    path.write_text(
        json.dumps(
            {
                "snapshot_id": "decision_snapshot:2026-05-04",
                "signal_date": "2026-05-04",
                "market_regime": {
                    "regime_id": "ai_after_chatgpt",
                    "start_date": "2022-12-01",
                },
                "scores": {
                    "overall_score": 73.0,
                    "confidence_score": 66.0,
                    "confidence_level": "medium",
                    "confidence_reasons": ["data coverage medium"],
                    "components": [
                        {
                            "component": "trend",
                            "score": 72.0,
                            "weight": 25.0,
                            "source_type": "hard_data",
                            "coverage": 1.0,
                            "confidence": 0.9,
                            "reason": "趋势支持。",
                        },
                        {
                            "component": "fundamentals",
                            "score": 68.0,
                            "weight": 25.0,
                            "source_type": "hard_data",
                            "coverage": 0.8,
                            "confidence": 0.75,
                            "reason": "基本面支持。",
                        },
                        {
                            "component": "macro_liquidity",
                            "score": 70.0,
                            "weight": 25.0,
                            "source_type": "hard_data",
                            "coverage": 1.0,
                            "confidence": 0.8,
                            "reason": "宏观中性偏正。",
                        },
                        {
                            "component": "valuation",
                            "score": 82.0,
                            "weight": 25.0,
                            "source_type": "manual_input",
                            "coverage": 0.7,
                            "confidence": 0.6,
                            "reason": "估值偏贵。",
                        },
                    ],
                },
                "positions": {
                    "model_risk_asset_ai_band": {
                        "min_position": 0.4,
                        "max_position": 0.6,
                        "label": "中高配",
                    },
                    "confidence_adjusted_risk_asset_ai_band": {
                        "min_position": 0.4,
                        "max_position": 0.5,
                        "label": "置信度受限",
                    },
                    "final_risk_asset_ai_band": {
                        "min_position": 0.4,
                        "max_position": 0.4,
                        "label": "受限中配",
                    },
                    "final_total_risk_asset_band": {
                        "min_position": 0.4,
                        "max_position": 0.6,
                        "label": "总风险资产预算",
                    },
                    "macro_risk_asset_budget": {
                        "level": "neutral",
                        "triggered": False,
                        "source": "portfolio_policy",
                        "reasons": ["宏观预算中性。"],
                    },
                    "position_gates": [
                        {
                            "gate_id": "score_model",
                            "label": "评分模型仓位",
                            "source": "weighted_score_model",
                            "max_position": 0.6,
                            "triggered": True,
                            "reason": "score band cap",
                        },
                        {
                            "gate_id": "valuation",
                            "label": "估值拥挤",
                            "source": "valuation_review",
                            "max_position": 0.4,
                            "triggered": True,
                            "reason": "估值分位过高。",
                        },
                    ],
                },
                "quality": {
                    "market_data_status": "PASS",
                    "market_data_error_count": 0,
                    "market_data_warning_count": 0,
                    "feature_status": "PASS",
                    "sec_feature_status": "PASS",
                },
                "manual_review": [
                    {
                        "name": "owner_review",
                        "status": "WARNING",
                        "summary": "人工复核摘要存在警告项",
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path
