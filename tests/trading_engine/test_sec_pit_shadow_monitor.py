from __future__ import annotations

import builtins
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli_commands import sec_pit as sec_pit_cli
from ai_trading_system.fundamentals.sec_pit_shadow_monitor import (
    ACTIVE_SHADOW_CONFIG_PATHS,
    PRODUCTION_CONFIG_PATHS,
    ROLLING_METRICS_COLUMNS,
    WARNING_EVENTS_COLUMNS,
    run_sec_pit_shadow_monitor,
)


def test_sec_pit_shadow_monitor_writes_expected_artifacts(tmp_path: Path) -> None:
    paths = _write_monitor_inputs(tmp_path, days=4, minimum_days=2, min_sample=4)

    artifacts = run_sec_pit_shadow_monitor(
        shadow_observe_dir=paths["shadow_observe_dir"],
        baseline_coverage_dir=paths["baseline_coverage_dir"],
        baseline_score_path=paths["baseline_score_path"],
        window_days=(20, 60),
        output_dir=paths["monitor_dir"],
    )

    summary = _read_json(artifacts.summary_json_path)
    metrics = pd.read_csv(artifacts.rolling_metrics_path)
    warnings = pd.read_csv(artifacts.warning_events_path)
    markdown = artifacts.summary_markdown_path.read_text(encoding="utf-8")

    assert artifacts.status == "OK_MONITORING"
    assert summary["schema_version"] == "1.1"
    assert summary["report_type"] == "sec_pit_shadow_monitor"
    assert summary["task_id"] == "TRADING-046"
    assert summary["state_policy_task_id"] == "TRADING-046A"
    assert summary["monitor_status"] == "OK_MONITORING"
    assert summary["monitor_maturity"] == "MINIMUM_EVIDENCE_ACHIEVED"
    assert summary["minimum_evidence_achieved"] is True
    assert summary["rolling_metrics_available"] is True
    assert "无 warning 或 rollback" in summary["state_transition_reason"]
    assert summary["candidate_feature"] == "capex_intensity"
    assert summary["observe_weight"] == -0.025
    assert summary["production_effect"] == "none"
    assert summary["manual_review_required"] is True
    assert summary["coverage_gate_passed"] is True
    assert summary["monitoring_ready"] is True
    assert summary["rollback_recommended"] is False
    assert summary["monitoring_days_remaining"] == 0
    assert tuple(metrics.columns) == ROLLING_METRICS_COLUMNS
    assert tuple(warnings.columns) == WARNING_EVENTS_COLUMNS
    assert warnings.empty
    assert summary["warning_count"] == 0
    assert "# SEC PIT Shadow Observe Rolling Monitor" in markdown
    assert "production_effect: none" in markdown
    assert "monitor_maturity: MINIMUM_EVIDENCE_ACHIEVED" in markdown


def test_sec_pit_shadow_monitor_transitions_insufficient_sample_to_monitoring_active(
    tmp_path: Path,
) -> None:
    paths = _write_monitor_inputs(tmp_path, days=3, minimum_days=5, min_sample=4)

    artifacts = run_sec_pit_shadow_monitor(
        shadow_observe_dir=paths["shadow_observe_dir"],
        baseline_coverage_dir=paths["baseline_coverage_dir"],
        baseline_score_path=paths["baseline_score_path"],
        window_days=(20, 60),
        output_dir=paths["monitor_dir"],
    )

    summary = _read_json(artifacts.summary_json_path)
    assert artifacts.status == "MONITORING_ACTIVE"
    assert summary["monitor_status"] == "MONITORING_ACTIVE"
    assert summary["monitor_maturity"] == "ACCUMULATING_EVIDENCE"
    assert summary["minimum_evidence_achieved"] is False
    assert summary["monitoring_ready"] is False
    assert summary["monitoring_days_elapsed"] == 3
    assert summary["monitoring_days_remaining"] == 2
    assert summary["rollback_recommended"] is False
    assert "仍在积累" in summary["state_transition_reason"]


def test_sec_pit_shadow_monitor_transitions_active_to_ok_when_minimum_evidence_is_met(
    tmp_path: Path,
) -> None:
    paths = _write_monitor_inputs(tmp_path, days=4, minimum_days=2, min_sample=4)

    artifacts = run_sec_pit_shadow_monitor(
        shadow_observe_dir=paths["shadow_observe_dir"],
        baseline_coverage_dir=paths["baseline_coverage_dir"],
        baseline_score_path=paths["baseline_score_path"],
        window_days=(20, 10),
        output_dir=paths["monitor_dir"],
    )

    summary = _read_json(artifacts.summary_json_path)
    assert artifacts.status == "OK_MONITORING"
    assert summary["monitor_status"] == "OK_MONITORING"
    assert summary["monitor_maturity"] == "MINIMUM_EVIDENCE_ACHIEVED"
    assert summary["minimum_evidence_achieved"] is True
    assert summary["monitoring_ready"] is True
    assert summary["rolling_metrics_available"] is False
    assert summary["monitoring_sample_count"] >= summary["min_monitoring_sample_count"]
    assert summary["monitoring_days_remaining"] == 0
    assert summary["warning_count"] == 0
    assert summary["rollback_recommended"] is False
    assert "不再视为 monitoring sample 不足" in summary["state_transition_reason"]


def test_sec_pit_shadow_monitor_recommends_rollback_only_after_coverage_gates_pass(
    tmp_path: Path,
) -> None:
    paths = _write_monitor_inputs(
        tmp_path,
        days=4,
        minimum_days=2,
        min_sample=4,
        wrong_direction=True,
    )

    artifacts = run_sec_pit_shadow_monitor(
        shadow_observe_dir=paths["shadow_observe_dir"],
        baseline_coverage_dir=paths["baseline_coverage_dir"],
        baseline_score_path=paths["baseline_score_path"],
        window_days=(20, 60),
        output_dir=paths["monitor_dir"],
    )
    summary = _read_json(artifacts.summary_json_path)
    warnings = pd.read_csv(artifacts.warning_events_path)

    assert artifacts.status == "ROLLBACK_RECOMMENDED"
    assert summary["factor_underperformance_confirmed"] is True
    assert summary["rollback_recommended"] is True
    assert set(warnings["severity"]).issuperset({"ROLLBACK_CONDITION"})

    _write_baseline_coverage(
        paths["baseline_coverage_dir"],
        date(2023, 1, 5),
        status="LIMITED_COVERAGE",
    )
    second = run_sec_pit_shadow_monitor(
        shadow_observe_dir=paths["shadow_observe_dir"],
        baseline_coverage_dir=paths["baseline_coverage_dir"],
        baseline_score_path=paths["baseline_score_path"],
        window_days=(20, 60),
        output_dir=paths["monitor_dir"],
    )
    second_summary = _read_json(second.summary_json_path)

    assert second.status == "FAILED_VALIDATION"
    assert second_summary["coverage_gate_passed"] is False
    assert second_summary["factor_underperformance_confirmed"] is True
    assert second_summary["rollback_recommended"] is False


def test_sec_pit_shadow_monitor_blocks_rollback_without_rolling_metrics(
    tmp_path: Path,
) -> None:
    paths = _write_monitor_inputs(
        tmp_path,
        days=4,
        minimum_days=2,
        min_sample=4,
        wrong_direction=True,
    )

    artifacts = run_sec_pit_shadow_monitor(
        shadow_observe_dir=paths["shadow_observe_dir"],
        baseline_coverage_dir=paths["baseline_coverage_dir"],
        baseline_score_path=paths["baseline_score_path"],
        window_days=(20, 10),
        output_dir=paths["monitor_dir"],
    )
    summary = _read_json(artifacts.summary_json_path)

    assert artifacts.status == "WARNING"
    assert summary["coverage_gate_passed"] is True
    assert summary["minimum_evidence_achieved"] is True
    assert summary["rolling_metrics_available"] is False
    assert summary["factor_underperformance_confirmed"] is True
    assert summary["rollback_recommended"] is False


def test_sec_pit_shadow_monitor_cli_latest_mode(tmp_path: Path) -> None:
    paths = _write_monitor_inputs(tmp_path, days=4, minimum_days=2, min_sample=4)

    result = CliRunner().invoke(
        sec_pit_cli.sec_pit_app,
        [
            "shadow-monitor",
            "--latest",
            "--shadow-observe-dir",
            str(paths["shadow_observe_dir"]),
            "--baseline-coverage-dir",
            str(paths["baseline_coverage_dir"]),
            "--baseline-score-path",
            str(paths["baseline_score_path"]),
            "--window-days",
            "20",
            "60",
            "--output-dir",
            str(paths["monitor_dir"]),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "SEC PIT shadow monitor status: OK_MONITORING" in result.output
    assert (paths["monitor_dir"] / "sec_pit_shadow_monitor_summary_2023-01-05.json").exists()


def test_sec_pit_shadow_monitor_repeated_run_is_deterministic(tmp_path: Path) -> None:
    paths = _write_monitor_inputs(tmp_path, days=4, minimum_days=2, min_sample=4)

    first = run_sec_pit_shadow_monitor(
        shadow_observe_dir=paths["shadow_observe_dir"],
        baseline_coverage_dir=paths["baseline_coverage_dir"],
        baseline_score_path=paths["baseline_score_path"],
        window_days=(20, 60),
        output_dir=paths["monitor_dir"],
    )
    first_summary = first.summary_json_path.read_text(encoding="utf-8")
    first_metrics = first.rolling_metrics_path.read_text(encoding="utf-8")
    second = run_sec_pit_shadow_monitor(
        shadow_observe_dir=paths["shadow_observe_dir"],
        baseline_coverage_dir=paths["baseline_coverage_dir"],
        baseline_score_path=paths["baseline_score_path"],
        window_days=(20, 60),
        output_dir=paths["monitor_dir"],
    )

    assert second.summary_json_path.read_text(encoding="utf-8") == first_summary
    assert second.rolling_metrics_path.read_text(encoding="utf-8") == first_metrics


def test_sec_pit_shadow_monitor_does_not_write_production_or_shadow_configs(
    tmp_path: Path,
) -> None:
    paths = _write_monitor_inputs(tmp_path, days=4, minimum_days=2, min_sample=4)
    before = _hash_paths((*PRODUCTION_CONFIG_PATHS, *ACTIVE_SHADOW_CONFIG_PATHS))

    run_sec_pit_shadow_monitor(
        shadow_observe_dir=paths["shadow_observe_dir"],
        baseline_coverage_dir=paths["baseline_coverage_dir"],
        baseline_score_path=paths["baseline_score_path"],
        window_days=(20, 60),
        output_dir=paths["monitor_dir"],
    )

    after = _hash_paths((*PRODUCTION_CONFIG_PATHS, *ACTIVE_SHADOW_CONFIG_PATHS))
    assert after == before


def test_dashboard_reads_sec_pit_shadow_monitor_artifact_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    from ai_trading_system.daily_task_dashboard import (
        build_daily_task_dashboard_payload,
        build_daily_task_dashboard_report,
        render_daily_task_dashboard,
    )

    as_of = date(2023, 1, 5)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_dashboard_shadow_monitor_summary(tmp_path, as_of)
    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked = (
            "ai_trading_system.fundamentals.sec_pit_shadow_monitor",
            "ai_trading_system.fundamentals.sec_pit_shadow_observe",
            "ai_trading_system.fundamentals.sec_pit_baseline_coverage",
            "ai_trading_system.data.download",
            "ai_trading_system.backtest",
            "ai_trading_system.scoring",
        )
        if any(token in name for token in blocked):
            raise AssertionError(f"dashboard must not import shadow monitor pipeline: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        reports_dir=tmp_path,
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    summary = payload["sec_pit_shadow_monitor"]
    assert summary["exists"] is True
    assert summary["status"] == "OK_MONITORING"
    assert summary["latest_monitor_date"] == "2023-01-05"
    assert summary["candidate_feature"] == "capex_intensity"
    assert summary["observe_weight"] == -0.025
    assert summary["monitor_maturity"] == "MINIMUM_EVIDENCE_ACHIEVED"
    assert summary["rolling_metrics_available"] is True
    assert (
        summary["state_transition_reason"]
        == "minimum evidence 与 rolling metrics 均可用，且无 warning 或 rollback。"
    )
    assert summary["rolling_rank_ic_20d"] == 0.42
    assert summary["rolling_rank_ic_60d"] == 0.31
    assert summary["monitoring_sample_count"] == 16
    assert summary["monitoring_days_elapsed"] == 4
    assert summary["monitoring_days_remaining"] == 0
    assert summary["warning_count"] == 0
    assert summary["rollback_recommended"] is False
    assert summary["production_effect"] == "none"
    assert "SEC PIT Shadow Monitor" in html
    assert "capex_intensity" in html
    assert "MINIMUM_EVIDENCE_ACHIEVED" in html


def _write_monitor_inputs(
    tmp_path: Path,
    *,
    days: int,
    minimum_days: int,
    min_sample: int,
    wrong_direction: bool = False,
) -> dict[str, Path]:
    shadow_observe_dir = tmp_path / "outputs" / "sec_pit_shadow_observe"
    baseline_coverage_dir = tmp_path / "outputs" / "sec_pit_baseline_coverage"
    monitor_dir = tmp_path / "outputs" / "sec_pit_shadow_monitor"
    baseline_score_path = tmp_path / "data" / "processed" / "research" / "scores_daily.csv"
    for path in (
        shadow_observe_dir,
        baseline_coverage_dir,
        baseline_score_path.parent,
        monitor_dir,
    ):
        path.mkdir(parents=True, exist_ok=True)
    as_of = date(2023, 1, 5)
    scores_path = shadow_observe_dir / f"sec_pit_shadow_scores_{as_of.isoformat()}.csv"
    bucket_path = shadow_observe_dir / f"sec_pit_shadow_bucket_comparison_{as_of.isoformat()}.csv"
    plan_path = shadow_observe_dir / f"sec_pit_shadow_monitoring_plan_{as_of.isoformat()}.csv"
    markdown_path = shadow_observe_dir / f"sec_pit_shadow_observe_summary_{as_of.isoformat()}.md"
    summary_path = shadow_observe_dir / f"sec_pit_shadow_observe_summary_{as_of.isoformat()}.json"

    scores = _score_rows(days=days, wrong_direction=wrong_direction)
    pd.DataFrame(scores).to_csv(scores_path, index=False)
    pd.DataFrame(
        [
            {"bucket": "all", "sample_count": len(scores), "rank_ic_20d": 0.4},
            {"bucket": "semiconductor", "sample_count": days * 2, "rank_ic_20d": 0.4},
            {"bucket": "platform", "sample_count": days * 2, "rank_ic_20d": 0.2},
        ]
    ).to_csv(bucket_path, index=False)
    _write_monitoring_plan(plan_path, minimum_days=minimum_days)
    baseline_rows = [
        {
            "decision_date": row["decision_date"],
            "ticker": row["ticker"],
            "baseline_score": row["baseline_score"],
        }
        for row in scores
    ]
    pd.DataFrame(baseline_rows).to_csv(baseline_score_path, index=False)
    summary_path.write_text(
        json.dumps(
            {
                "report_type": "sec_pit_shadow_observe",
                "end_date": as_of.isoformat(),
                "shadow_status": "OK",
                "baseline_coverage_status": "OK",
                "monitoring_status": "OK",
                "candidate_feature": "capex_intensity",
                "observe_weight": -0.025,
                "production_effect": "none",
                "manual_review_required": True,
                "min_monitoring_sample_count": min_sample,
                "output_artifacts": {
                    "summary_markdown": str(markdown_path),
                    "shadow_scores_csv": str(scores_path),
                    "bucket_comparison_csv": str(bucket_path),
                    "monitoring_plan_csv": str(plan_path),
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    markdown_path.write_text("# SEC PIT Observe-Only Shadow Lane Summary\n", encoding="utf-8")
    _write_baseline_coverage(baseline_coverage_dir, as_of, status="OK")
    return {
        "shadow_observe_dir": shadow_observe_dir,
        "baseline_coverage_dir": baseline_coverage_dir,
        "baseline_score_path": baseline_score_path,
        "monitor_dir": monitor_dir,
    }


def _score_rows(*, days: int, wrong_direction: bool) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    start = date(2023, 1, 2)
    for offset in range(days):
        current = start + timedelta(days=offset)
        if wrong_direction:
            rows.extend(_wrong_direction_rows(current))
            continue
        rows.extend(_supportive_rows(current))
    return rows


def _supportive_rows(current: date) -> list[dict[str, object]]:
    base = [
        ("NVDA", "semiconductor", 100.0, 1, 1, 0.040, 0.040, 0.050, -0.010),
        ("AMD", "semiconductor", 90.0, 2, 2, 0.030, 0.030, 0.040, -0.020),
        ("MSFT", "platform", 80.0, 3, 3, 0.010, 0.010, 0.020, -0.030),
        ("META", "platform", 70.0, 4, 4, 0.000, 0.000, 0.010, -0.040),
    ]
    return [_score_row(current, *item) for item in base]


def _wrong_direction_rows(current: date) -> list[dict[str, object]]:
    base = [
        ("NVDA", "semiconductor", 80.0, 3, 1, 50.0, -0.100, -0.120, -0.200),
        ("AMD", "semiconductor", 70.0, 4, 2, 40.0, -0.090, -0.110, -0.180),
        ("MSFT", "platform", 100.0, 1, 3, 0.0, 0.050, 0.070, -0.010),
        ("META", "platform", 90.0, 2, 4, 0.0, 0.040, 0.060, -0.020),
    ]
    return [_score_row(current, *item) for item in base]


def _score_row(
    current: date,
    ticker: str,
    bucket: str,
    baseline_score: float,
    baseline_rank: int,
    observe_rank: int,
    component: float,
    forward_20d: float,
    forward_60d: float,
    drawdown_20d: float,
) -> dict[str, object]:
    return {
        "decision_date": current.isoformat(),
        "ticker": ticker,
        "bucket": bucket,
        "baseline_score": baseline_score,
        "feature_id": "capex_intensity",
        "feature_value": abs(component),
        "normalized_feature_value": component,
        "observe_weight": -0.025,
        "sec_pit_shadow_component": component,
        "sec_pit_observe_score": baseline_score + component,
        "baseline_rank": baseline_rank,
        "sec_pit_observe_rank": observe_rank,
        "rank_delta": baseline_rank - observe_rank,
        "forward_return_20d": forward_20d,
        "forward_return_60d": forward_60d,
        "relative_return_vs_QQQ_20d": forward_20d,
        "max_drawdown_forward_20d": drawdown_20d,
        "pit_grade": "B_RECONSTRUCTED_SEC_FILING_PIT",
        "available_time": "2023-01-01T00:00:00+00:00",
        "accession_number": f"{ticker}-23-000001",
        "source_lineage": "{}",
        "manual_review_required": True,
        "production_effect": "none",
    }


def _write_monitoring_plan(path: Path, *, minimum_days: int) -> None:
    rows = []
    for metric, warning, rollback in (
        ("rolling_rank_ic_20d", ">= 0.0200", "< -0.0200"),
        ("rolling_rank_ic_60d", ">= 0.0200", "< -0.0200"),
        ("relative_return_vs_baseline_20d", ">= 0.0000", "-0.0500"),
        ("drawdown_improvement_20d", ">= 0.0000", "-0.0300"),
    ):
        rows.append(
            {
                "lane_id": "sec_pit_capex_intensity_observe_only",
                "feature_id": "capex_intensity",
                "observe_weight": -0.025,
                "start_date": "2023-01-02",
                "minimum_monitoring_days": minimum_days,
                "preferred_monitoring_days": minimum_days + 30,
                "monitoring_metric": metric,
                "target_direction": "positive",
                "warning_threshold": warning,
                "rollback_threshold": rollback,
                "current_value": "",
                "status": "OK",
                "manual_review_required": True,
                "production_effect": "none",
            }
        )
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_baseline_coverage(root: Path, as_of: date, *, status: str) -> None:
    summary_path = root / f"sec_pit_baseline_coverage_summary_{as_of.isoformat()}.json"
    markdown_path = root / f"sec_pit_baseline_coverage_summary_{as_of.isoformat()}.md"
    ratio = 1.0 if status == "OK" else 0.75
    summary_path.write_text(
        json.dumps(
            {
                "report_type": "sec_pit_baseline_coverage",
                "end_date": as_of.isoformat(),
                "coverage_status": status,
                "coverage_ratio": ratio,
                "expected_rows": 16,
                "actual_rows": int(16 * ratio),
                "missing_rows": 16 - int(16 * ratio),
                "output_artifacts": {"summary_markdown": str(markdown_path)},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    markdown_path.write_text("# SEC PIT Baseline Coverage Summary\n", encoding="utf-8")


def _write_daily_ops_metadata(tmp_path: Path, as_of: date) -> Path:
    path = tmp_path / f"daily_ops_metadata_{as_of.isoformat()}.json"
    path.write_text(
        json.dumps(
            {
                "run_id": "unit-test",
                "status": "PASS",
                "project_root": str(tmp_path),
                "started_at": "2023-01-05T00:00:00Z",
                "finished_at": "2023-01-05T00:01:00Z",
                "commands": [],
                "step_results": [],
                "git": {"commit": "abc123", "dirty": False},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def _write_dashboard_shadow_monitor_summary(tmp_path: Path, as_of: date) -> None:
    root = tmp_path / "outputs" / "sec_pit_shadow_monitor"
    root.mkdir(parents=True)
    summary_path = root / f"sec_pit_shadow_monitor_summary_{as_of.isoformat()}.json"
    markdown_path = root / f"sec_pit_shadow_monitor_summary_{as_of.isoformat()}.md"
    summary_path.write_text(
        json.dumps(
            {
                "report_type": "sec_pit_shadow_monitor",
                "monitor_status": "OK_MONITORING",
                "monitor_maturity": "MINIMUM_EVIDENCE_ACHIEVED",
                "rolling_metrics_available": True,
                "state_transition_reason": (
                    "minimum evidence 与 rolling metrics 均可用，且无 warning 或 rollback。"
                ),
                "monitor_date": as_of.isoformat(),
                "candidate_feature": "capex_intensity",
                "observe_weight": -0.025,
                "rolling_rank_ic_20d": 0.42,
                "rolling_rank_ic_60d": 0.31,
                "monitoring_sample_count": 16,
                "monitoring_days_elapsed": 4,
                "monitoring_days_remaining": 0,
                "warning_count": 0,
                "rollback_recommended": False,
                "production_effect": "none",
                "manual_review_required": True,
                "output_artifacts": {"summary_markdown": str(markdown_path)},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    markdown_path.write_text("# SEC PIT Shadow Observe Rolling Monitor\n", encoding="utf-8")


def _hash_paths(paths: tuple[Path, ...]) -> dict[str, str]:
    result: dict[str, str] = {}
    for path in paths:
        if not path.exists():
            result[str(path)] = ""
            continue
        result[str(path)] = path.read_bytes().hex()
    return result


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload
