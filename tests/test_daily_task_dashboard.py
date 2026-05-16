from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.reports.daily_task_dashboard_view_model import (
    DailyTaskDashboardReport,
)


def test_daily_task_dashboard_summarizes_task_conclusions_and_risks(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 4)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    _write_shadow_parameter_search(tmp_path)
    _write_shadow_iteration_report(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        run_report_path=tmp_path / "daily_ops_run_2026-05-04.md",
        reports_dir=tmp_path,
    )
    html = render_daily_task_dashboard(report)
    payload = build_daily_task_dashboard_payload(report)

    assert isinstance(report, DailyTaskDashboardReport)
    assert report.status == "PASS"
    assert report.risk_count == 2
    assert "关键结论总览" in html
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
    assert '<strong>实际 40.00%</strong>' in html
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
    investment = next(
        item for item in payload["key_conclusions"] if item["area"] == "投资结论"
    )
    assert investment["primary"] == (
        "执行动作：观察；最终 AI 仓位：40%-60%；置信度：0.71；Data Gate：PASS"
    )
    parameter = next(
        item for item in payload["key_conclusions"] if item["area"] == "参数治理"
    )
    assert parameter["status"] == "PASS_WITH_LIMITATIONS"
    feedback = next(
        item for item in payload["key_conclusions"] if item["area"] == "反馈复盘"
    )
    assert "source_current__grid_gate_0217" in feedback["primary"]
    assert "shadow return 9.02%" in feedback["primary"]
    assert "production 4.74%" in feedback["primary"]
    assert "excess +4.29%" in feedback["primary"]
    assert "promotion=NOT_PROMOTABLE" in feedback["primary"]
    assert "available：8，total：11" in "；".join(feedback["supporting"])
    assert "available=8，contract floor=30" in feedback["important_risk"]
    assert (
        "单日 return = 仓位中点 × 标的 outcome return"
        in feedback["result_methodology"]
    )
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
        row
        for row in comparison
        if row["group"] == "gate_cap" and row["parameter"] == "valuation"
    )
    assert valuation_cap["production"] == "主线实际 gate：40.00%（无静态 override）"
    assert valuation_cap["production_observed_min"] == 0.4
    assert valuation_cap["production_observed_max"] == 0.4
    assert valuation_cap["candidate"] == "70.00%"
    assert valuation_cap["delta"] == "新增 override"
    assert "primary cap" in valuation_cap["note"]
    trend_weight = next(
        row
        for row in comparison
        if row["group"] == "weight" and row["parameter"] == "trend"
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
    assert "dashboard 只读取 shadow_iteration JSON" in "；".join(
        shadow_iteration["supporting"]
    )
    assert "gate_only 只能进入 gate policy review" in shadow_iteration["important_risk"]
    score_task = next(task for task in payload["tasks"] if task["step_id"] == "score_daily")
    assert score_task["conclusion"] == (
        "执行动作：观察；最终 AI 仓位：40%-60%；置信度：0.71；Data Gate：PASS"
    )
    assert score_task["detail_reports"][1]["href"] == "daily_score_2026-05-04.md"


def test_reports_daily_tasks_cli_writes_html_and_json(tmp_path: Path) -> None:
    as_of = date(2026, 5, 4)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_detail_reports(tmp_path, as_of)
    _write_shadow_parameter_search(tmp_path)
    _write_shadow_iteration_report(tmp_path, as_of)
    output_path = tmp_path / "daily_task_dashboard_2026-05-04.html"
    json_output_path = tmp_path / "daily_task_dashboard_2026-05-04.json"

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
        ],
    )

    assert result.exit_code == 0, result.output
    assert "每日任务展示：PASS" in result.output
    assert output_path.exists()
    assert json_output_path.exists()
    assert "关键结论总览" in output_path.read_text(encoding="utf-8")
    payload = json.loads(json_output_path.read_text(encoding="utf-8"))
    assert payload["summary"]["risk_count"] == 2
    assert payload["key_conclusions"][0]["area"] == "投资结论"
    assert any(
        "source_current__grid_gate_0217" in item["primary"]
        for item in payload["key_conclusions"]
    )
    assert any(
        item["area"] == "Shadow Iteration"
        for item in payload["key_conclusions"]
    )
    assert payload["tasks"][0]["step_id"] == "download_data"


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
                    'source_current__production_observed_gates,11,8,1,2,'
                    '0.0473519476,0.0473519476,0.0,-0.0108505772,'
                    '-0.0108505772,1.2,1.2,0.0,False,'
                    'available_samples_below_objective_floor,'
                    '"{""trend"": 0.25, ""fundamentals"": 0.25}",'
                    '{}'
                ),
                (
                    'source_current__grid_gate_0217,11,8,1,2,0.0473519476,'
                    '0.0902253121,0.0428733645,-0.0108505772,'
                    '-0.0169856501,1.2,0.65,0.75,False,'
                    'available_samples_below_objective_floor,'
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
