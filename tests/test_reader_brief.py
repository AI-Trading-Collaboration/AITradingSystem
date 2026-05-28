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
        research_governance_summary_path=inputs["research_governance_summary"],
        report_index_path=inputs["report_index"],
        documentation_contract_path=inputs["documentation_contract"],
    )

    assert payload["status"] == "LIMITED_READER_CONTEXT"
    assert payload["production_effect"] == "none"
    assert payload["reader_entry_role"] == "daily_reading_home"
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
    assert payload["report_index_summary"]["missing_count"] == 1
    assert payload["report_index_summary"]["problem_reports"][0]["report_id"] == "data_quality"
    assert payload["task_cadence_calendar"]["groups"][0]["cadence"] == "daily"
    impact = payload["missing_limited_artifact_impact"]
    assert any(item["impact_level"] == "BLOCKING" for item in impact["items"])
    assert "trend" in payload["contribution_summary"]["top_positive_contributors"]
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
    assert payload["backtest_shadow_governance"]["source"] == "research_governance_summary"
    assert payload["backtest_shadow_governance"]["candidate_research_count"] == 3
    assert payload["manual_review_queue"]["status"] == "ACTION_REQUIRED"
    assert payload["manual_review_queue"]["groups"][0]["label"] == "Critical / Must Review Today"
    assert all(
        "recommended_next_action" in item for item in payload["manual_review_queue"]["items"]
    )
    assert any(
        item["category"] == "report_freshness" for item in payload["manual_review_queue"]["items"]
    )
    assert any(item["artifact_id"] == "daily_report" for item in payload["appendix_links"])
    assert any(item["artifact_id"] == "trace_bundle" for item in payload["appendix_links"])
    assert any(item["artifact_id"] == "artifact_catalog" for item in payload["report_navigation"])
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
        research_governance_summary_path=tmp_path / "missing_research_governance.json",
        report_index_path=tmp_path / "missing_report_index.json",
        documentation_contract_path=tmp_path / "missing_documentation_contract.json",
    )

    assert payload["status"] == "LIMITED_READER_CONTEXT"
    assert payload["executive_decision"]["binding_gate_id"] == "valuation"
    assert payload["component_score_explainability"]["status"] == "AVAILABLE"
    assert payload["backtest_shadow_governance"]["availability"] == "LIMITED"
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
    assert "今日结论" in html
    assert "Missing / Limited Artifact Impact" in html
    assert "Core Decision" in html
    assert "Score &amp; Decision Funnel" in html
    assert "Score Change Attribution" in html
    assert "Report Index Freshness" in html
    assert "Task Cadence Calendar" in html
    assert "Report Navigation" in html
    assert "Critical / Must Review Today" in html
    assert "Contribution Summary" in html
    assert "<details" in html
    assert "不是实盘交易指令" in html
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["production_effect"] == "none"
    assert payload["executive_decision"]["not_trade_instruction"] is True


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
    research_governance_path = tmp_path / "research_governance_summary_2026-05-04.json"
    research_governance_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "research_governance_summary",
                "as_of": "2026-05-04",
                "status": "PASS_WITH_WARNINGS",
                "production_effect": "none",
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
                    "report_count": 3,
                    "available_count": 2,
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
