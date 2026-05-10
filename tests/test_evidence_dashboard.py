from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.evidence_dashboard import (
    build_evidence_dashboard_report,
    render_evidence_dashboard,
)


def test_evidence_dashboard_links_conclusion_to_inputs(tmp_path: Path) -> None:
    inputs = _write_dashboard_inputs(tmp_path)

    report = build_evidence_dashboard_report(
        as_of=date(2026, 5, 4),
        daily_report_path=inputs["daily_report"],
        trace_bundle_path=inputs["trace"],
        decision_snapshot_path=inputs["snapshot"],
        alerts_report_path=inputs["alerts"],
        scores_daily_path=inputs["scores"],
    )
    html = render_evidence_dashboard(report)

    assert report.status == "PASS"
    assert "今日决策视图" in html
    assert "告警聚合" in html
    assert "最近可用评分趋势" in html
    assert "总分走势" in html
    assert "Score sparkline" not in html
    assert "<svg" in html
    assert "快速读者" in html
    assert "投资复核者" in html
    assert "系统审计者" in html
    assert "最终 AI 仓位为 40%-60%" in html
    assert "估值拥挤 上限 40%" in html
    assert "估值拥挤 触发仓位上限" in html
    assert "2026-05-03" in html
    assert "价格日线缓存" in html
    assert "abc123" in html
    assert "市场数据质量门禁: PASS" in html
    assert "aits trace lookup --bundle-path" in html
    assert "production_effect" in html
    assert "none" in html


def test_reports_dashboard_cli_writes_html(tmp_path: Path) -> None:
    inputs = _write_dashboard_inputs(tmp_path)
    output_path = tmp_path / "dashboard.html"

    result = CliRunner().invoke(
        app,
        [
            "reports",
            "dashboard",
            "--as-of",
            "2026-05-04",
            "--daily-report-path",
            str(inputs["daily_report"]),
            "--trace-bundle-path",
            str(inputs["trace"]),
            "--decision-snapshot-path",
            str(inputs["snapshot"]),
            "--output-path",
            str(output_path),
            "--alerts-report-path",
            str(inputs["alerts"]),
            "--scores-daily-path",
            str(inputs["scores"]),
            "--json-output-path",
            str(tmp_path / "dashboard.json"),
        ],
    )

    assert result.exit_code == 0
    assert output_path.exists()
    assert (tmp_path / "dashboard.json").exists()
    assert "Evidence dashboard：PASS" in result.output
    assert "dashboard.json" in result.output
    assert "Claim 到输入映射" in output_path.read_text(encoding="utf-8")
    payload = json.loads((tmp_path / "dashboard.json").read_text(encoding="utf-8"))
    assert payload["production_effect"] == "none"
    assert payload["decision"]["action"] == "观察，不形成交易结论（`observe_only`）"
    assert payload["alerts"]["active_count"] == 1


def _write_dashboard_inputs(tmp_path: Path) -> dict[str, Path]:
    daily_report_path = tmp_path / "daily_score_2026-05-04.md"
    daily_report_path.write_text(
        "\n".join(
            [
                "# AI 产业链每日评分",
                "",
                "## 今日结论卡",
                "",
                "| 项目 | 结论 |",
                "|---|---|",
                "| 状态标签 | 中高配但受限 |",
                "| 执行动作 | 观察，不形成交易结论（`observe_only`） |",
                "| 判断置信度 | 中，66.0/100 |",
                "| Data Gate | PASS |",
                "| 总风险资产预算 | 50%-70%（normal） |",
                "",
                "### 最大限制",
                "",
                "- 估值拥挤限制最终仓位。",
                "",
                "### Main Invalidator",
                "",
                "- 估值拥挤是当前主要 invalidator。",
                "",
                "### Next Checks",
                "",
                "- 复核 NVDA / TSM / AMD 趋势。",
                "",
                "## 变化原因树",
                "",
                "- 本期仓位变化：2026-05-03 35%-55% -> 2026-05-04 40%-60%；总分变化 +2.0分。",
                "",
                "### 什么情况会改变判断",
                "",
                "- 估值分位回落且基本面不恶化。",
                "- risk event gate 解除。",
                "",
                "## 数据门禁",
                "",
            ]
        ),
        encoding="utf-8",
    )

    alerts_path = tmp_path / "alerts_2026-05-04.md"
    alerts_path.write_text(
        "\n".join(
            [
                "# 投资与数据告警报告",
                "",
                "- 状态：ACTIVE_WARNINGS",
                "- 评估日期：2026-05-04",
                "- production_effect=none；告警只做复核提示，不改变评分、仓位、回测或执行建议。",
                "- 活跃告警数：1",
                "- data/system：0",
                "- investment/risk：1",
                "",
                "## 严重度摘要",
                "",
                "| 等级 | 数量 |",
                "|---|---:|",
                "| critical | 0 |",
                "| high | 1 |",
                "| warning | 0 |",
                "",
                "## 告警明细",
                "",
                "| 等级 | 类别 | 来源 | 标题 | 触发条件 | 解除条件 | 引用 | 去重键 | 说明 |",
                "|---|---|---|---|---|---|---|---|---|",
                (
                    "| high | investment_risk | position_gate | 估值拥挤 触发仓位上限 | "
                    "gate_id=valuation | gate 解除 | daily_score:2026-05-04:overall_position | "
                    "k1 | 估值限制仓位。 |"
                ),
                "",
                "## 治理边界",
                "",
                "- 告警不是交易指令。",
            ]
        ),
        encoding="utf-8",
    )

    scores_path = tmp_path / "scores_daily.csv"
    score_columns = [
        "as_of",
        "component",
        "score",
        "weight",
        "source_type",
        "coverage",
        "reason",
        "confidence",
        "confidence_level",
        "confidence_reasons",
        "model_risk_asset_ai_min",
        "model_risk_asset_ai_max",
        "final_risk_asset_ai_min",
        "final_risk_asset_ai_max",
        "confidence_adjusted_risk_asset_ai_min",
        "confidence_adjusted_risk_asset_ai_max",
        "total_asset_ai_min",
        "total_asset_ai_max",
        "triggered_position_gates",
        "static_total_risk_asset_min",
        "static_total_risk_asset_max",
        "final_total_risk_asset_min",
        "final_total_risk_asset_max",
        "macro_risk_asset_budget_level",
        "macro_risk_asset_budget_triggered",
        "macro_risk_asset_budget_reasons",
    ]
    score_rows = [
        [
            "2026-05-03",
            "overall",
            "71.0",
            "100.0",
            "derived",
            "",
            "上一日",
            "64.0",
            "medium",
            "warning note",
            "0.6",
            "0.8",
            "0.35",
            "0.55",
            "0.5",
            "0.6",
            "0.21",
            "0.33",
            "估值拥挤",
            "0.6",
            "0.8",
            "0.5",
            "0.7",
            "normal",
            "False",
            "无",
        ],
        [
            "2026-05-04",
            "overall",
            "73.0",
            "100.0",
            "derived",
            "",
            "当日",
            "66.0",
            "medium",
            "warning note",
            "0.6",
            "0.8",
            "0.4",
            "0.6",
            "0.5",
            "0.6",
            "0.24",
            "0.36",
            "估值拥挤",
            "0.6",
            "0.8",
            "0.5",
            "0.7",
            "normal",
            "False",
            "无",
        ],
    ]
    scores_path.write_text(
        "\n".join([",".join(score_columns), *[",".join(row) for row in score_rows]]) + "\n",
        encoding="utf-8",
    )

    quality_path = tmp_path / "data_quality.md"
    quality_path.write_text("PASS\n", encoding="utf-8")
    trace_path = tmp_path / "evidence" / "daily_score_2026-05-04_trace.json"
    trace_path.parent.mkdir()
    trace_path.write_text(
        json.dumps(
            {
                "report_id": "daily_score:2026-05-04",
                "report_type": "daily_score",
                "report_path": str(daily_report_path),
                "run_manifest": {
                    "run_id": "run:daily_score:2026-05-04",
                    "command": "aits score-daily",
                    "market_regime": {
                        "regime_id": "ai_after_chatgpt",
                        "start_date": "2022-12-01",
                    },
                },
                "quality_refs": [
                    {
                        "quality_id": "quality:market_data:2026-05-04",
                        "label": "市场数据质量门禁",
                        "status": "PASS",
                        "report_path": str(quality_path),
                        "error_count": 0,
                        "warning_count": 0,
                    }
                ],
                "dataset_refs": [
                    {
                        "dataset_id": "dataset:prices_daily",
                        "label": "价格日线缓存",
                        "dataset_type": "raw_market_data",
                        "path": str(tmp_path / "prices_daily.csv"),
                        "row_count": 260,
                        "checksum_sha256": "abc123",
                        "provider": "Tiingo",
                    }
                ],
                "evidence_cards": [
                    {
                        "evidence_id": "evidence:daily_score:2026-05-04:position",
                        "summary": "评分、置信度和仓位闸门支持最终仓位结论。",
                        "dataset_ids": ["dataset:prices_daily"],
                        "quality_ids": ["quality:market_data:2026-05-04"],
                        "artifact_paths": [str(daily_report_path)],
                    }
                ],
                "claims": [
                    {
                        "claim_id": "daily_score:2026-05-04:overall_position",
                        "statement": "最终 AI 仓位为 40%-60%。",
                        "report_section": "今日结论卡 / 仓位闸门",
                        "evidence_ids": [
                            "evidence:daily_score:2026-05-04:position"
                        ],
                        "dataset_ids": ["dataset:prices_daily"],
                        "quality_ids": ["quality:market_data:2026-05-04"],
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    belief_path = tmp_path / "belief_state_2026-05-04.json"
    belief_path.write_text(
        json.dumps(
            {
                "thesis_state": {"summary": "Thesis warning 1"},
                "risk_state": {"summary": "Risk stable"},
                "valuation_state": {"summary": "估值偏贵"},
                "position_boundary": {"summary": "估值 gate 是当前主要上限。"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    snapshot_path = tmp_path / "decision_snapshot_2026-05-04.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "signal_date": "2026-05-04",
                "scores": {
                    "overall_score": 73.0,
                    "confidence_score": 66.0,
                    "confidence_level": "medium",
                    "components": [
                        {
                            "component": "trend",
                            "score": 72.0,
                            "weight": 25.0,
                            "source_type": "hard_data",
                            "coverage": 1.0,
                            "confidence": 0.9,
                            "reason": "趋势支持。",
                        }
                    ],
                },
                "positions": {
                    "final_risk_asset_ai_band": {
                        "min_position": 0.4,
                        "max_position": 0.6,
                        "label": "中高配",
                    },
                    "final_total_risk_asset_band": {
                        "min_position": 0.5,
                        "max_position": 0.7,
                        "label": "风险资产预算",
                    },
                    "position_gates": [
                        {
                            "gate_id": "valuation",
                            "label": "估值拥挤",
                            "source": "valuation_review",
                            "max_position": 0.4,
                            "triggered": True,
                            "reason": "估值分位过高。",
                        }
                    ],
                },
                "quality": {"market_data_status": "PASS"},
                "manual_review": [
                    {
                        "name": "交易 thesis",
                        "status": "PASS_WITH_WARNINGS",
                        "summary": "Thesis warning 1",
                    }
                ],
                "valuation_state": {"status": "PASS_WITH_WARNINGS"},
                "risk_event_state": {"status": "PASS"},
                "belief_state_ref": {"path": str(belief_path), "read_only": True},
                "trace": {
                    "trace_bundle_path": str(trace_path),
                    "overall_claim_id": "daily_score:2026-05-04:overall_position",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    return {
        "daily_report": daily_report_path,
        "trace": trace_path,
        "snapshot": snapshot_path,
        "belief": belief_path,
        "alerts": alerts_path,
        "scores": scores_path,
    }
