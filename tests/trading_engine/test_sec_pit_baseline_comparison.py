from __future__ import annotations

import builtins
import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli_commands import sec_pit as sec_pit_cli
from ai_trading_system.fundamentals.sec_pit_baseline_comparison import (
    DECISION_IMPACT_COLUMNS,
    INCREMENTAL_ALPHA_COLUMNS,
    RANK_SHIFT_COLUMNS,
    run_sec_pit_baseline_comparison,
)


def test_sec_pit_baseline_comparison_writes_expected_artifacts(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    artifacts = _run(paths, tmp_path / "outputs")

    summary = _read_json(artifacts.summary_json_path)
    impact = pd.read_csv(artifacts.decision_impact_path)
    rank_shift = pd.read_csv(artifacts.rank_shift_path)
    incremental_alpha = pd.read_csv(artifacts.incremental_alpha_path)
    markdown = artifacts.summary_markdown_path.read_text(encoding="utf-8")

    assert summary["comparison_status"] == "OK"
    assert summary["decision_count"] == 4
    assert summary["action_changed_count"] == 2
    assert summary["material_rank_shift_count"] >= 1
    assert summary["incremental_alpha_20d"] > 0
    assert summary["safety"]["manual_review_required"] is True
    assert summary["safety"]["production_effect"] == "none"
    assert tuple(impact.columns) == DECISION_IMPACT_COLUMNS
    assert tuple(rank_shift.columns) == RANK_SHIFT_COLUMNS
    assert tuple(incremental_alpha.columns) == INCREMENTAL_ALPHA_COLUMNS
    assert impact["manual_review_required"].all()
    assert set(impact["production_effect"]) == {"none"}
    assert json.loads(impact.loc[impact["ticker"] == "NVDA", "source_lineage"].iloc[0]) == [
        {
            "accession_number": "NVDA-23-000001",
            "available_time": "2023-01-02T00:00:00+00:00",
            "metric_id": "gross_margin",
            "raw_sha256": "hash-NVDA",
        }
    ]
    assert set(incremental_alpha["bucket"]) == {
        "top_baseline",
        "top_sec_pit",
        "promoted_by_sec_pit",
        "downgraded_by_sec_pit",
        "unchanged",
    }
    assert "# SEC PIT Baseline Comparison Summary" in markdown
    assert "## Executive Summary" in markdown
    assert "## PIT Safety" in markdown
    assert "## Manual Review Checklist" in markdown


def test_sec_pit_baseline_comparison_missing_baseline_degrades(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    artifacts = run_sec_pit_baseline_comparison(
        start=date(2023, 1, 2),
        end=date(2023, 1, 2),
        sec_pit_evaluation_dir=paths["evaluation_dir"],
        baseline_score_dir=tmp_path / "missing_baseline",
        output_dir=tmp_path / "outputs",
    )

    summary = _read_json(artifacts.summary_json_path)
    impact = pd.read_csv(artifacts.decision_impact_path)
    assert artifacts.status == "LIMITED_BASELINE_MISSING"
    assert summary["comparison_status"] == "LIMITED_BASELINE_MISSING"
    assert tuple(impact.columns) == DECISION_IMPACT_COLUMNS
    assert impact.empty


def test_sec_pit_baseline_comparison_missing_evaluation_degrades(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    artifacts = run_sec_pit_baseline_comparison(
        start=date(2023, 1, 2),
        end=date(2023, 1, 2),
        sec_pit_evaluation_dir=tmp_path / "missing_evaluation",
        baseline_score_dir=paths["baseline_dir"],
        output_dir=tmp_path / "outputs",
    )

    summary = _read_json(artifacts.summary_json_path)
    assert artifacts.status == "LIMITED_SEC_PIT_EVALUATION_MISSING"
    assert summary["comparison_status"] == "LIMITED_SEC_PIT_EVALUATION_MISSING"
    assert pd.read_csv(artifacts.rank_shift_path).empty


def test_sec_pit_baseline_comparison_insufficient_overlap(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path, baseline_date=date(2023, 1, 3))

    artifacts = _run(paths, tmp_path / "outputs")

    summary = _read_json(artifacts.summary_json_path)
    assert artifacts.status == "INSUFFICIENT_OVERLAP"
    assert summary["comparison_status"] == "INSUFFICIENT_OVERLAP"
    assert pd.read_csv(artifacts.incremental_alpha_path).empty


def test_sec_pit_compare_baseline_cli_writes_requested_artifacts(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)

    result = CliRunner().invoke(
        sec_pit_cli.sec_pit_app,
        [
            "compare-baseline",
            "--start",
            "2023-01-02",
            "--end",
            "2023-01-02",
            "--sec-pit-evaluation-dir",
            str(paths["evaluation_dir"]),
            "--baseline-score-dir",
            str(paths["baseline_dir"]),
            "--benchmark",
            "QQQ",
            "--output-dir",
            str(tmp_path / "cli_outputs"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "SEC PIT baseline comparison status: OK" in result.output
    assert (
        tmp_path / "cli_outputs" / "sec_pit_baseline_comparison_summary_2023-01-02.json"
    ).exists()
    assert (tmp_path / "cli_outputs" / "sec_pit_decision_impact_2023-01-02.csv").exists()


def test_sec_pit_baseline_comparison_repeated_run_is_deterministic(
    tmp_path: Path,
) -> None:
    paths = _write_inputs(tmp_path)
    output_dir = tmp_path / "outputs"

    first = _run(paths, output_dir)
    first_summary = first.summary_json_path.read_text(encoding="utf-8")
    first_impact = first.decision_impact_path.read_text(encoding="utf-8")
    second = _run(paths, output_dir)

    assert first_summary == second.summary_json_path.read_text(encoding="utf-8")
    assert first_impact == second.decision_impact_path.read_text(encoding="utf-8")


def test_daily_task_dashboard_sec_pit_baseline_comparison_card_is_read_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    from ai_trading_system.daily_task_dashboard import (
        build_daily_task_dashboard_payload,
        build_daily_task_dashboard_report,
        render_daily_task_dashboard,
    )

    as_of = date(2023, 1, 2)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_dashboard_comparison_summary(tmp_path, as_of)
    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked = (
            "ai_trading_system.fundamentals.sec_pit_baseline_comparison",
            "ai_trading_system.fundamentals.sec_pit_evaluation",
            "ai_trading_system.data.download",
            "ai_trading_system.backtest",
            "ai_trading_system.scoring",
        )
        if any(token in name for token in blocked):
            raise AssertionError(f"dashboard must not import comparison pipeline: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        reports_dir=tmp_path,
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    summary = payload["sec_pit_baseline_comparison"]
    assert summary["exists"] is True
    assert summary["status"] == "OK"
    assert summary["latest_comparison_date"] == "2023-01-02"
    assert summary["decision_count"] == 4
    assert summary["action_changed_count"] == 2
    assert summary["material_rank_shift_count"] == 1
    assert summary["production_effect"] == "none"
    assert "SEC PIT Baseline Comparison" in html
    assert "NVDA" in html


def _run(paths: dict[str, Path], output_dir: Path) -> Any:
    return run_sec_pit_baseline_comparison(
        start=date(2023, 1, 2),
        end=date(2023, 1, 2),
        sec_pit_evaluation_dir=paths["evaluation_dir"],
        baseline_score_dir=paths["baseline_dir"],
        benchmark="QQQ",
        output_dir=output_dir,
    )


def _write_inputs(
    tmp_path: Path,
    *,
    baseline_date: date = date(2023, 1, 2),
) -> dict[str, Path]:
    evaluation_dir = tmp_path / "sec_pit_evaluation"
    baseline_dir = tmp_path / "baseline"
    evaluation_dir.mkdir(parents=True)
    baseline_dir.mkdir(parents=True)
    _write_sec_pit_evaluation(evaluation_dir, date(2023, 1, 2))
    _write_baseline_scores(baseline_dir, baseline_date)
    return {"evaluation_dir": evaluation_dir, "baseline_dir": baseline_dir}


def _write_sec_pit_evaluation(root: Path, as_of: date) -> None:
    suffix = as_of.isoformat()
    summary_path = root / f"sec_pit_evaluation_summary_{suffix}.json"
    feature_path = root / f"sec_pit_feature_effectiveness_{suffix}.csv"
    attribution_path = root / f"sec_pit_signal_attribution_{suffix}.csv"
    weights_path = root / f"sec_pit_shadow_candidate_weights_{suffix}.csv"
    summary_path.write_text(
        json.dumps(
            {
                "report_type": "sec_pit_cognitive_evaluation",
                "status": "PASS",
                "end_date": suffix,
                "production_effect": "none",
                "output_artifacts": {
                    "feature_effectiveness_csv": str(feature_path),
                    "signal_attribution_csv": str(attribution_path),
                    "shadow_candidate_weights_csv": str(weights_path),
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "feature_id": "gross_margin",
                "metric_id": "gross_profit,revenue",
                "sample_count": 40,
                "coverage_ratio": 1.0,
                "valid_ticker_count": 4,
                "start_date": suffix,
                "end_date": suffix,
                "pit_grade": "B_RECONSTRUCTED_SEC_FILING_PIT",
                "ic_1d": 0.1,
                "ic_5d": 0.1,
                "ic_20d": 0.2,
                "ic_60d": 0.1,
                "rank_ic_20d": 0.4,
                "hit_rate_20d": 0.75,
                "avg_forward_return_top_quantile_20d": 0.08,
                "avg_forward_return_bottom_quantile_20d": -0.05,
                "spread_top_minus_bottom_20d": 0.13,
                "max_drawdown_top_quantile_20d": -0.02,
                "stability_score": 0.9,
                "data_quality_score": 1.0,
                "recommendation": "PROMOTE_TO_SHADOW",
            },
            {
                "feature_id": "debt_ratio",
                "metric_id": "debt,assets",
                "sample_count": 40,
                "coverage_ratio": 1.0,
                "valid_ticker_count": 4,
                "start_date": suffix,
                "end_date": suffix,
                "pit_grade": "B_RECONSTRUCTED_SEC_FILING_PIT",
                "ic_1d": -0.1,
                "ic_5d": -0.1,
                "ic_20d": -0.2,
                "ic_60d": -0.1,
                "rank_ic_20d": -0.3,
                "hit_rate_20d": 0.75,
                "avg_forward_return_top_quantile_20d": -0.05,
                "avg_forward_return_bottom_quantile_20d": 0.08,
                "spread_top_minus_bottom_20d": -0.13,
                "max_drawdown_top_quantile_20d": -0.08,
                "stability_score": 0.4,
                "data_quality_score": 1.0,
                "recommendation": "KEEP_RESEARCH_ONLY",
            },
        ]
    ).to_csv(feature_path, index=False)
    rows = [
        ("NVDA", "gross_margin", 0.060, 0.08, 0.06, -0.02),
        ("MSFT", "gross_margin", 0.010, 0.02, 0.00, -0.03),
        ("AMD", "debt_ratio", -0.010, -0.01, -0.03, -0.06),
        ("AVGO", "debt_ratio", -0.100, -0.05, -0.07, -0.10),
    ]
    pd.DataFrame(
        [
            {
                "decision_date": suffix,
                "ticker": ticker,
                "feature_id": feature,
                "metric_id": feature,
                "feature_value": contribution,
                "normalized_value": contribution,
                "signal_direction": "POSITIVE" if contribution >= 0 else "NEGATIVE",
                "weight": 1.0,
                "contribution": contribution,
                "available_time": f"{suffix}T00:00:00+00:00",
                "period": "2022Q4",
                "form": "10-Q",
                "accession_number": f"{ticker}-23-000001",
                "accepted_datetime": f"{suffix}T00:00:00+00:00",
                "filed_date": suffix,
                "raw_sha256": f"hash-{ticker}",
                "source_lineage": json.dumps(
                    [
                        {
                            "metric_id": feature,
                            "accession_number": f"{ticker}-23-000001",
                            "available_time": f"{suffix}T00:00:00+00:00",
                            "raw_sha256": f"hash-{ticker}",
                        }
                    ],
                    sort_keys=True,
                ),
                "pit_grade": "B_RECONSTRUCTED_SEC_FILING_PIT",
                "forward_return_20d": forward_return,
                "relative_return_vs_QQQ_20d": relative_return,
                "max_drawdown_forward_20d": drawdown,
            }
            for ticker, feature, contribution, forward_return, relative_return, drawdown in rows
        ]
    ).to_csv(attribution_path, index=False)
    pd.DataFrame(
        [
            {
                "feature_id": "gross_margin",
                "metric_id": "gross_profit,revenue",
                "current_weight": 0.0,
                "suggested_shadow_weight": 0.05,
                "weight_delta": 0.05,
                "evidence_score": 1.0,
                "stability_score": 0.9,
                "coverage_ratio": 1.0,
                "pit_quality_score": 1.0,
                "risk_note": "unit test",
                "manual_review_required": True,
                "production_effect": "none",
            }
        ]
    ).to_csv(weights_path, index=False)


def _write_baseline_scores(root: Path, as_of: date) -> None:
    suffix = as_of.isoformat()
    pd.DataFrame(
        [
            {
                "decision_date": suffix,
                "ticker": "NVDA",
                "baseline_score": 68.0,
                "baseline_action": "WATCH",
            },
            {
                "decision_date": suffix,
                "ticker": "MSFT",
                "baseline_score": 65.0,
                "baseline_action": "WATCH",
            },
            {
                "decision_date": suffix,
                "ticker": "AMD",
                "baseline_score": 64.0,
                "baseline_action": "WATCH",
            },
            {
                "decision_date": suffix,
                "ticker": "AVGO",
                "baseline_score": 72.0,
                "baseline_action": "REVIEW_POSITIVE",
            },
        ]
    ).to_csv(root / f"baseline_scores_{suffix}.csv", index=False)


def _write_daily_ops_metadata(tmp_path: Path, as_of: date) -> Path:
    path = tmp_path / f"daily_ops_metadata_{as_of.isoformat()}.json"
    payload = {
        "run_id": "unit-test",
        "status": "PASS",
        "project_root": str(tmp_path),
        "started_at": "2023-01-02T00:00:00Z",
        "finished_at": "2023-01-02T00:01:00Z",
        "commands": [],
        "step_results": [],
        "git": {"commit": "abc123", "dirty": False},
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_dashboard_comparison_summary(tmp_path: Path, as_of: date) -> None:
    root = tmp_path / "outputs" / "sec_pit_baseline_comparison"
    root.mkdir(parents=True, exist_ok=True)
    summary_path = root / f"sec_pit_baseline_comparison_summary_{as_of.isoformat()}.json"
    markdown_path = root / f"sec_pit_baseline_comparison_summary_{as_of.isoformat()}.md"
    summary_path.write_text(
        json.dumps(
            {
                "report_type": "sec_pit_baseline_comparison",
                "comparison_status": "OK",
                "generated_at": "2023-01-02T00:00:00+00:00",
                "end_date": as_of.isoformat(),
                "decision_count": 4,
                "action_changed_count": 2,
                "material_rank_shift_count": 1,
                "incremental_alpha_20d": 0.13,
                "drawdown_improvement_20d": 0.08,
                "top_promoted_tickers": [{"ticker": "NVDA", "rank_delta": 1, "score_delta": 6.0}],
                "top_downgraded_tickers": [
                    {"ticker": "AVGO", "rank_delta": -3, "score_delta": -10.0}
                ],
                "safety": {
                    "manual_review_required": True,
                    "production_effect": "none",
                    "production_weights_modified": False,
                    "production_actions_modified": False,
                },
                "output_artifacts": {"summary_markdown": str(markdown_path)},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    markdown_path.write_text("# SEC PIT Baseline Comparison Summary\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
