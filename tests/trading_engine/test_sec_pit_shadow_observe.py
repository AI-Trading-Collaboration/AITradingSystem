from __future__ import annotations

import builtins
import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli_commands import sec_pit as sec_pit_cli
from ai_trading_system.fundamentals.sec_pit_shadow_observe import (
    ACTIVE_SHADOW_CONFIG_PATHS,
    BUCKET_COMPARISON_COLUMNS,
    MONITORING_PLAN_COLUMNS,
    PRODUCTION_CONFIG_PATHS,
    RANK_SHIFT_COLUMNS,
    SAFETY_AUDIT_COLUMNS,
    SHADOW_SCORE_COLUMNS,
    run_sec_pit_shadow_observe,
)


def test_sec_pit_shadow_observe_writes_expected_artifacts(tmp_path: Path) -> None:
    paths = _write_shadow_inputs(tmp_path)

    artifacts = _run_shadow_observe(paths, tmp_path)

    summary = _read_json(artifacts.summary_json_path)
    scores = pd.read_csv(artifacts.shadow_scores_path)
    rank_shift = pd.read_csv(artifacts.rank_shift_path)
    buckets = pd.read_csv(artifacts.bucket_comparison_path)
    monitoring = pd.read_csv(artifacts.monitoring_plan_path)
    safety = pd.read_csv(artifacts.safety_audit_path)
    markdown = artifacts.summary_markdown_path.read_text(encoding="utf-8")

    assert artifacts.status == "OK"
    assert summary["shadow_status"] == "OK"
    assert summary["lane_id"] == "sec_pit_capex_intensity_observe_only"
    assert summary["candidate_feature"] == "capex_intensity"
    assert summary["observe_weight"] == -0.025
    assert summary["max_allowed_initial_weight"] == 0.05
    assert summary["production_effect"] == "none"
    assert summary["manual_review_required"] is True
    assert summary["candidate_review_status"] == "READY_FOR_MANUAL_REVIEW"
    assert summary["diagnostics_status"] == "OK"
    assert summary["provenance_complete"] is True
    assert summary["baseline_overlap_risk"] == "LOW"
    assert summary["monitoring_status"] == "INSUFFICIENT_MONITORING_SAMPLE"
    assert summary["factor_rollback_triggered"] is False
    assert summary["data_limitation_triggered"] is True
    assert tuple(scores.columns) == SHADOW_SCORE_COLUMNS
    assert tuple(rank_shift.columns) == RANK_SHIFT_COLUMNS
    assert tuple(buckets.columns) == BUCKET_COMPARISON_COLUMNS
    assert tuple(monitoring.columns) == MONITORING_PLAN_COLUMNS
    assert tuple(safety.columns) == SAFETY_AUDIT_COLUMNS
    assert scores["manual_review_required"].astype(str).str.lower().eq("true").all()
    assert set(scores["production_effect"]) == {"none"}
    assert rank_shift["manual_review_required"].astype(str).str.lower().eq("true").all()
    assert set(rank_shift["production_effect"]) == {"none"}
    assert set(monitoring["production_effect"]) == {"none"}
    assert set(buckets["bucket"]).issuperset({"all", "semiconductor", "platform"})
    assert set(scores["bucket"]).issuperset({"semiconductor", "platform"})
    assert scores["source_lineage"].astype(str).str.contains("accession_number").all()
    assert (safety["status"] == "FAIL").sum() == 0
    assert "# SEC PIT Observe-Only Shadow Lane Summary" in markdown
    assert "## Manual Review Checklist" in markdown


def test_sec_pit_shadow_observe_cli_latest_mode(tmp_path: Path) -> None:
    paths = _write_shadow_inputs(tmp_path)

    result = CliRunner().invoke(
        sec_pit_cli.sec_pit_app,
        [
            "shadow-observe",
            "--latest",
            "--candidate-review-dir",
            str(paths["candidate_review_dir"]),
            "--evaluation-dir",
            str(paths["evaluation_dir"]),
            "--comparison-dir",
            str(paths["comparison_dir"]),
            "--diagnostics-dir",
            str(paths["diagnostics_dir"]),
            "--feature-panel",
            str(paths["feature_panel_path"]),
            "--baseline-score-path",
            str(paths["baseline_score_path"]),
            "--candidate-feature",
            "capex_intensity",
            "--observe-weight",
            "-0.025",
            "--max-allowed-weight",
            "0.050",
            "--output-dir",
            str(tmp_path / "outputs" / "sec_pit_shadow_observe"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "SEC PIT shadow observe status: OK" in result.output
    assert (
        tmp_path
        / "outputs"
        / "sec_pit_shadow_observe"
        / "sec_pit_shadow_observe_summary_2023-01-05.json"
    ).exists()


def test_candidate_review_status_must_be_ready_for_manual_review(tmp_path: Path) -> None:
    paths = _write_shadow_inputs(tmp_path, candidate_status="KEEP_RESEARCH_ONLY")

    artifacts = _run_shadow_observe(paths, tmp_path)

    summary = _read_json(artifacts.summary_json_path)
    safety = pd.read_csv(artifacts.safety_audit_path)
    assert artifacts.status == "FAILED_SAFETY_CHECK"
    assert summary["shadow_status"] == "FAILED_SAFETY_CHECK"
    row = safety.loc[safety["check_name"] == "candidate_review_status"].iloc[0]
    assert row["status"] == "FAIL"
    assert row["severity"] == "critical"
    assert not artifacts.shadow_scores_path.exists()


def test_safety_audit_fails_if_production_effect_is_not_none(tmp_path: Path) -> None:
    paths = _write_shadow_inputs(tmp_path, production_effect="production")

    artifacts = _run_shadow_observe(paths, tmp_path)

    safety = pd.read_csv(artifacts.safety_audit_path)
    assert artifacts.status == "FAILED_SAFETY_CHECK"
    row = safety.loc[safety["check_name"] == "production_effect_none"].iloc[0]
    assert row["status"] == "FAIL"
    assert row["severity"] == "critical"


def test_observe_weight_cannot_exceed_max_allowed_initial_weight(tmp_path: Path) -> None:
    paths = _write_shadow_inputs(tmp_path)

    artifacts = _run_shadow_observe(paths, tmp_path, observe_weight=-0.10)

    safety = pd.read_csv(artifacts.safety_audit_path)
    assert artifacts.status == "FAILED_SAFETY_CHECK"
    row = safety.loc[safety["check_name"] == "observe_weight_within_limit"].iloc[0]
    assert row["status"] == "FAIL"


def test_missing_baseline_degrades_but_preserves_shadow_score_schema(tmp_path: Path) -> None:
    paths = _write_shadow_inputs(tmp_path)
    missing_baseline = tmp_path / "missing" / "scores_daily.csv"
    paths["baseline_score_path"] = missing_baseline

    artifacts = _run_shadow_observe(paths, tmp_path)

    summary = _read_json(artifacts.summary_json_path)
    scores = pd.read_csv(artifacts.shadow_scores_path)
    rank_shift = pd.read_csv(artifacts.rank_shift_path)
    assert artifacts.status == "LIMITED_BASELINE_MISSING"
    assert summary["shadow_status"] == "LIMITED_BASELINE_MISSING"
    assert summary["monitoring_status"] == "LIMITED_BASELINE_MISSING"
    assert summary["factor_rollback_triggered"] is False
    assert summary["data_limitation_triggered"] is True
    assert tuple(scores.columns) == SHADOW_SCORE_COLUMNS
    assert tuple(rank_shift.columns) == RANK_SHIFT_COLUMNS
    assert len(scores) > 0
    assert rank_shift.empty


def test_factor_rollback_requires_monitoring_quality_gates(tmp_path: Path) -> None:
    paths = _write_shadow_inputs(tmp_path)
    _make_signal_attribution_wrong_direction(paths["evaluation_dir"])
    config_path = _write_shadow_config(tmp_path, min_monitoring_sample_count=1)

    artifacts = _run_shadow_observe(paths, tmp_path, config_path=config_path)

    summary = _read_json(artifacts.summary_json_path)
    assert summary["shadow_status"] == "OK"
    assert summary["monitoring_status"] == "ROLLBACK_TRIGGERED_BY_FACTOR"
    assert summary["factor_rollback_triggered"] is True
    assert summary["data_limitation_triggered"] is False


def test_missing_candidate_review_degrades_without_emitting_score_rows(tmp_path: Path) -> None:
    paths = _write_shadow_inputs(tmp_path)
    paths["candidate_review_dir"] = tmp_path / "missing_review"

    artifacts = _run_shadow_observe(paths, tmp_path)

    summary = _read_json(artifacts.summary_json_path)
    safety = pd.read_csv(artifacts.safety_audit_path)
    assert artifacts.status == "LIMITED_CANDIDATE_REVIEW_MISSING"
    assert summary["shadow_status"] == "LIMITED_CANDIDATE_REVIEW_MISSING"
    assert summary["score_rows"] == 0
    assert (safety["severity"] == "critical").loc[safety["status"] == "FAIL"].sum() == 0


def test_sec_pit_shadow_observe_repeated_run_is_deterministic(tmp_path: Path) -> None:
    paths = _write_shadow_inputs(tmp_path)
    output_dir = tmp_path / "outputs" / "sec_pit_shadow_observe"

    first = _run_shadow_observe(paths, tmp_path, output_dir=output_dir)
    first_summary = first.summary_json_path.read_text(encoding="utf-8")
    first_scores = first.shadow_scores_path.read_text(encoding="utf-8")
    second = _run_shadow_observe(paths, tmp_path, output_dir=output_dir)

    assert second.summary_json_path.read_text(encoding="utf-8") == first_summary
    assert second.shadow_scores_path.read_text(encoding="utf-8") == first_scores


def test_sec_pit_shadow_observe_does_not_write_production_or_shadow_configs(
    tmp_path: Path,
) -> None:
    paths = _write_shadow_inputs(tmp_path)
    before = _hash_paths((*PRODUCTION_CONFIG_PATHS, *ACTIVE_SHADOW_CONFIG_PATHS))

    _run_shadow_observe(paths, tmp_path)

    after = _hash_paths((*PRODUCTION_CONFIG_PATHS, *ACTIVE_SHADOW_CONFIG_PATHS))
    assert after == before


def test_dashboard_reads_sec_pit_shadow_observe_artifact_only(
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
    _write_dashboard_shadow_summary(tmp_path, as_of)
    _write_dashboard_baseline_coverage_summary(tmp_path, as_of)
    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked = (
            "ai_trading_system.fundamentals.sec_pit_shadow_observe",
            "ai_trading_system.fundamentals.sec_pit_candidate_review",
            "ai_trading_system.fundamentals.sec_pit_real_run_diagnostics",
            "ai_trading_system.fundamentals.sec_pit_evaluation",
            "ai_trading_system.fundamentals.sec_pit_baseline_comparison",
            "ai_trading_system.data.download",
            "ai_trading_system.backtest",
            "ai_trading_system.scoring",
        )
        if any(token in name for token in blocked):
            raise AssertionError(f"dashboard must not import shadow observe pipeline: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        reports_dir=tmp_path,
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    summary = payload["sec_pit_shadow_observe"]
    assert summary["exists"] is True
    assert summary["status"] == "OK"
    assert summary["latest_shadow_observe_date"] == "2023-01-05"
    assert summary["lane_id"] == "sec_pit_capex_intensity_observe_only"
    assert summary["candidate_feature"] == "capex_intensity"
    assert summary["observe_weight"] == -0.025
    assert summary["production_effect"] == "none"
    assert summary["score_rows"] == 8
    assert summary["safety_check_status"] == "PASS"
    assert summary["monitoring_status"] == "OK"
    assert summary["baseline_coverage_ratio"] == 1.0
    coverage = payload["sec_pit_baseline_coverage"]
    assert coverage["exists"] is True
    assert coverage["coverage_status"] == "OK"
    assert coverage["coverage_ratio"] == 1.0
    assert "SEC PIT Observe-Only Shadow Lane" in html
    assert "SEC PIT Baseline Coverage" in html
    assert "capex_intensity" in html


def _run_shadow_observe(
    paths: dict[str, Path],
    tmp_path: Path,
    *,
    observe_weight: float = -0.025,
    output_dir: Path | None = None,
    config_path: Path | None = None,
) -> Any:
    kwargs: dict[str, Any] = {
        "start": date(2023, 1, 1),
        "end": date(2023, 1, 5),
        "candidate_review_dir": paths["candidate_review_dir"],
        "evaluation_dir": paths["evaluation_dir"],
        "comparison_dir": paths["comparison_dir"],
        "diagnostics_dir": paths["diagnostics_dir"],
        "feature_panel_path": paths["feature_panel_path"],
        "baseline_score_path": paths["baseline_score_path"],
        "candidate_feature": "capex_intensity",
        "observe_weight": observe_weight,
        "max_allowed_weight": 0.05,
        "output_dir": output_dir or tmp_path / "outputs" / "sec_pit_shadow_observe",
    }
    if config_path is not None:
        kwargs["config_path"] = config_path
    return run_sec_pit_shadow_observe(**kwargs)


def _write_shadow_inputs(
    tmp_path: Path,
    *,
    candidate_status: str = "READY_FOR_MANUAL_REVIEW",
    production_effect: str = "none",
) -> dict[str, Path]:
    root = tmp_path / "outputs"
    candidate_review_dir = root / "sec_pit_candidate_review"
    evaluation_dir = root / "sec_pit_evaluation"
    comparison_dir = root / "sec_pit_baseline_comparison"
    diagnostics_dir = root / "sec_pit_diagnostics"
    feature_panel_path = tmp_path / "data" / "processed" / "sec_edgar" / "sec_pit_feature_panel.csv"
    baseline_score_path = tmp_path / "data" / "processed" / "scores_daily.csv"
    for path in (
        candidate_review_dir,
        evaluation_dir,
        comparison_dir,
        diagnostics_dir,
        feature_panel_path.parent,
        baseline_score_path.parent,
    ):
        path.mkdir(parents=True, exist_ok=True)
    _write_candidate_review_artifacts(candidate_review_dir, candidate_status, production_effect)
    _write_evaluation_artifacts(evaluation_dir, production_effect)
    _write_comparison_artifacts(comparison_dir)
    _write_diagnostics_artifacts(diagnostics_dir)
    _write_feature_panel(feature_panel_path)
    _write_baseline_scores(baseline_score_path)
    return {
        "candidate_review_dir": candidate_review_dir,
        "evaluation_dir": evaluation_dir,
        "comparison_dir": comparison_dir,
        "diagnostics_dir": diagnostics_dir,
        "feature_panel_path": feature_panel_path,
        "baseline_score_path": baseline_score_path,
    }


def _write_candidate_review_artifacts(
    root: Path,
    candidate_status: str,
    production_effect: str,
) -> None:
    suffix = "2023-01-05"
    summary = root / f"sec_pit_candidate_review_summary_{suffix}.json"
    evidence = root / f"sec_pit_candidate_evidence_{suffix}.csv"
    proposal = root / f"sec_pit_candidate_shadow_proposal_{suffix}.csv"
    overlap = root / f"sec_pit_candidate_overlap_with_baseline_{suffix}.csv"
    markdown = root / f"sec_pit_candidate_review_summary_{suffix}.md"
    summary.write_text(
        json.dumps(
            {
                "report_type": "sec_pit_candidate_review",
                "review_status": "OK",
                "start_date": "2023-01-01",
                "end_date": suffix,
                "production_effect": production_effect,
                "manual_review_required": True,
                "top_candidates": [
                    {
                        "feature_id": "capex_intensity",
                        "proposal_status": candidate_status,
                    }
                ],
                "output_artifacts": {
                    "summary_markdown": str(markdown),
                    "candidate_evidence_csv": str(evidence),
                    "candidate_shadow_proposal_csv": str(proposal),
                    "candidate_overlap_with_baseline_csv": str(overlap),
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    markdown.write_text("# SEC PIT Shadow Candidate Review\n", encoding="utf-8")
    pd.DataFrame(
        [
            {
                "feature_id": "capex_intensity",
                "metric_id": "capex,revenue",
                "rank_ic_20d": -0.12,
                "ic_20d": -0.10,
                "hit_rate_20d": 0.625,
                "coverage_ratio": 1.0,
                "data_quality_score": 0.95,
                "stability_score": 0.75,
                "drawdown_improvement_20d": 0.01,
                "incremental_alpha_20d": 0.02,
                "sample_count": 8,
                "valid_ticker_count": 4,
                "pit_grade": "B_RECONSTRUCTED_SEC_FILING_PIT",
                "manual_review_required": True,
                "production_effect": production_effect,
                "recommendation": candidate_status,
                "blocking_reasons": "",
                "supporting_reasons": "unit test",
            }
        ]
    ).to_csv(evidence, index=False)
    pd.DataFrame(
        [
            {
                "feature_id": "capex_intensity",
                "metric_id": "capex,revenue",
                "proposal_status": candidate_status,
                "suggested_observe_only_weight": -0.05,
                "max_allowed_initial_weight": 0.05,
                "review_required": True,
                "production_effect": production_effect,
                "rationale": "unit test",
                "risk_notes": "",
                "minimum_monitoring_days": 60,
                "rollback_condition": "unit test",
            }
        ]
    ).to_csv(proposal, index=False)
    pd.DataFrame(
        [
            {
                "feature_id": "capex_intensity",
                "baseline_signal": "baseline_score",
                "correlation": 0.1,
                "rank_correlation": 0.1,
                "overlap_sample_count": 8,
                "overlap_interpretation": "Candidate appears distinct.",
                "redundancy_risk": "LOW",
            }
        ]
    ).to_csv(overlap, index=False)


def _write_evaluation_artifacts(root: Path, production_effect: str) -> None:
    suffix = "2023-01-05"
    summary = root / f"sec_pit_evaluation_summary_{suffix}.json"
    feature = root / f"sec_pit_feature_effectiveness_{suffix}.csv"
    attribution = root / f"sec_pit_signal_attribution_{suffix}.csv"
    summary.write_text(
        json.dumps(
            {
                "report_type": "sec_pit_cognitive_evaluation",
                "status": "PASS",
                "start_date": "2023-01-01",
                "end_date": suffix,
                "production_effect": production_effect,
                "output_artifacts": {
                    "feature_effectiveness_csv": str(feature),
                    "signal_attribution_csv": str(attribution),
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
                "feature_id": "capex_intensity",
                "metric_id": "capex,revenue",
                "sample_count": 8,
                "coverage_ratio": 1.0,
                "valid_ticker_count": 4,
                "pit_grade": "B_RECONSTRUCTED_SEC_FILING_PIT",
                "rank_ic_20d": -0.12,
                "hit_rate_20d": 0.625,
                "data_quality_score": 0.95,
                "recommendation": "PROMOTE_TO_SHADOW",
            }
        ]
    ).to_csv(feature, index=False)
    pd.DataFrame(_attribution_rows()).to_csv(attribution, index=False)


def _attribution_rows() -> list[dict[str, object]]:
    base = [
        ("2023-01-02", "NVDA", -2.0, 0.08, 0.12, 0.05, -0.02),
        ("2023-01-02", "MSFT", 0.2, 0.01, 0.02, -0.01, -0.04),
        ("2023-01-02", "AMD", 0.8, -0.02, -0.03, -0.03, -0.07),
        ("2023-01-02", "AVGO", -1.0, 0.04, 0.06, 0.02, -0.03),
        ("2023-01-03", "NVDA", -1.5, 0.07, 0.10, 0.04, -0.02),
        ("2023-01-03", "MSFT", 0.5, 0.00, 0.01, -0.02, -0.05),
        ("2023-01-03", "AMD", 1.0, -0.03, -0.04, -0.03, -0.08),
        ("2023-01-03", "AVGO", -0.6, 0.03, 0.05, 0.01, -0.03),
    ]
    rows: list[dict[str, object]] = []
    for decision_date, ticker, normalized, forward20, forward60, relative, drawdown in base:
        rows.append(
            {
                "decision_date": decision_date,
                "ticker": ticker,
                "feature_id": "capex_intensity",
                "metric_id": "capex,revenue",
                "feature_value": abs(normalized) / 10,
                "normalized_value": normalized,
                "signal_direction": "NEGATIVE",
                "weight": -0.05,
                "contribution": normalized * -0.05,
                "available_time": "2023-01-01T00:00:00+00:00",
                "period": "2022Q4",
                "form": "10-Q",
                "accession_number": f"{ticker}-23-000001",
                "accepted_datetime": "2023-01-01T00:00:00+00:00",
                "filed_date": "2023-01-01",
                "source_concept": "PaymentsToAcquirePropertyPlantAndEquipment,Revenue",
                "source_taxonomy": "us-gaap",
                "raw_sha256": f"hash-{ticker}",
                "source_url_or_raw_path": f"raw/{ticker}.json",
                "pit_grade": "B_RECONSTRUCTED_SEC_FILING_PIT",
                "source_lineage": json.dumps(
                    [
                        {
                            "metric_id": "capex",
                            "accession_number": f"{ticker}-23-000001",
                            "available_time": "2023-01-01T00:00:00+00:00",
                            "raw_sha256": f"hash-{ticker}",
                        }
                    ],
                    sort_keys=True,
                ),
                "forward_return_20d": forward20,
                "forward_return_60d": forward60,
                "relative_return_vs_QQQ_20d": relative,
                "max_drawdown_forward_20d": drawdown,
            }
        )
    return rows


def _write_comparison_artifacts(root: Path) -> None:
    suffix = "2023-01-05"
    summary = root / f"sec_pit_baseline_comparison_summary_{suffix}.json"
    impact = root / f"sec_pit_decision_impact_{suffix}.csv"
    summary.write_text(
        json.dumps(
            {
                "report_type": "sec_pit_baseline_comparison",
                "comparison_status": "OK",
                "start_date": "2023-01-01",
                "end_date": suffix,
                "output_artifacts": {"decision_impact_csv": str(impact)},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "decision_date": "2023-01-02",
                "ticker": "NVDA",
                "baseline_score": 70.00,
                "manual_review_required": True,
                "production_effect": "none",
            }
        ]
    ).to_csv(impact, index=False)


def _write_diagnostics_artifacts(root: Path) -> None:
    suffix = "2023-01-05"
    summary = root / f"sec_pit_real_run_diagnostics_{suffix}.json"
    labels = root / f"sec_pit_label_coverage_audit_{suffix}.csv"
    summary.write_text(
        json.dumps(
            {
                "report_type": "sec_pit_real_run_diagnostics",
                "diagnostics_status": "OK",
                "start_date": "2023-01-01",
                "end_date": suffix,
                "production_effect": "none",
                "provenance": {"missing_rows": 0, "complete_ratio": 1.0},
                "labels": {"max_drawdown_forward_20d_coverage": 0.95},
                "output_artifacts": {"label_coverage_audit_csv": str(labels)},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "label_name": "max_drawdown_forward_20d",
                "required": True,
                "available_count": 8,
                "missing_count": 0,
                "coverage_ratio": 0.95,
                "source_artifact": "unit",
                "recommended_fix": "",
            }
        ]
    ).to_csv(labels, index=False)


def _write_feature_panel(path: Path) -> None:
    pd.DataFrame(_attribution_rows()).to_csv(path, index=False)


def _write_baseline_scores(path: Path) -> None:
    rows: list[dict[str, object]] = []
    for decision_date in ("2023-01-02", "2023-01-03"):
        for ticker, score in (
            ("NVDA", 70.00),
            ("MSFT", 70.04),
            ("AMD", 70.02),
            ("AVGO", 69.99),
        ):
            rows.append(
                {
                    "decision_date": decision_date,
                    "ticker": ticker,
                    "baseline_score": score,
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)


def _make_signal_attribution_wrong_direction(evaluation_dir: Path) -> None:
    path = evaluation_dir / "sec_pit_signal_attribution_2023-01-05.csv"
    frame = pd.read_csv(path)
    frame["forward_return_20d"] = pd.to_numeric(frame["normalized_value"], errors="coerce")
    frame["forward_return_60d"] = pd.to_numeric(frame["normalized_value"], errors="coerce")
    frame.to_csv(path, index=False)


def _write_shadow_config(tmp_path: Path, *, min_monitoring_sample_count: int) -> Path:
    path = tmp_path / "sec_pit_shadow_observe.yaml"
    path.write_text(
        "\n".join(
            [
                "lane_id: sec_pit_capex_intensity_observe_only",
                "lane_status: observe_only",
                "production_effect: none",
                "manual_review_required: true",
                "candidates:",
                "  - feature_id: capex_intensity",
                "    metric_id: capex,revenue",
                "    approval_status: APPROVE_OBSERVE_ONLY_SHADOW",
                "    observe_weight: -0.025",
                "    max_allowed_initial_weight: 0.05",
                "    weight_direction: negative",
                "    pit_grade_policy: B_RECONSTRUCTED_SEC_FILING_PIT",
                "    minimum_monitoring_days: 60",
                "    preferred_monitoring_days: 90",
                "    enabled: true",
                "safety:",
                "  allow_production_write: false",
                "  allow_active_shadow_write: false",
                "  require_candidate_review_status: READY_FOR_MANUAL_REVIEW",
                "  require_diagnostics_status: OK",
                "  require_provenance_complete: true",
                "  min_data_quality_score: 0.90",
                "  min_drawdown_label_coverage: 0.90",
                "  max_abs_initial_weight: 0.05",
                "monitoring:",
                "  forward_windows: [20, 60]",
                "  compare_against: [baseline_score, score_daily]",
                "  buckets: [semiconductor, platform, all]",
                "  semiconductor_tickers: [NVDA, AMD, AVGO]",
                "  platform_tickers: [MSFT, GOOG, META, AMZN]",
                "monitoring_quality_gate:",
                "  min_baseline_coverage_ratio: 0.90",
                "  min_label_coverage_ratio: 0.90",
                f"  min_monitoring_sample_count: {min_monitoring_sample_count}",
                "  data_limited_status_prevents_factor_rollback: true",
                "rollback:",
                "  rolling_rank_ic_20d_wrong_direction_threshold: 0.02",
                "  max_negative_relative_return_20d: -0.05",
                "  max_drawdown_deterioration_20d: -0.03",
                f"  min_monitoring_sample_count: {min_monitoring_sample_count}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def _write_daily_ops_metadata(tmp_path: Path, as_of: date) -> Path:
    path = tmp_path / f"daily_ops_metadata_{as_of.isoformat()}.json"
    payload = {
        "run_id": "unit-test",
        "status": "PASS",
        "project_root": str(tmp_path),
        "started_at": "2023-01-05T00:00:00Z",
        "finished_at": "2023-01-05T00:01:00Z",
        "commands": [],
        "step_results": [],
        "git": {"commit": "abc123", "dirty": False},
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_dashboard_shadow_summary(tmp_path: Path, as_of: date) -> None:
    root = tmp_path / "outputs" / "sec_pit_shadow_observe"
    root.mkdir(parents=True)
    summary_path = root / f"sec_pit_shadow_observe_summary_{as_of.isoformat()}.json"
    markdown_path = root / f"sec_pit_shadow_observe_summary_{as_of.isoformat()}.md"
    summary_path.write_text(
        json.dumps(
            {
                "report_type": "sec_pit_shadow_observe",
                "shadow_status": "OK",
                "end_date": as_of.isoformat(),
                "lane_id": "sec_pit_capex_intensity_observe_only",
                "candidate_feature": "capex_intensity",
                "observe_weight": -0.025,
                "production_effect": "none",
                "manual_review_required": True,
                "score_rows": 8,
                "top_positive_rank_shifts": [
                    {
                        "decision_date": as_of.isoformat(),
                        "ticker": "AVGO",
                        "rank_delta": 1,
                        "score_delta": 0.025,
                    }
                ],
                "top_negative_rank_shifts": [
                    {
                        "decision_date": as_of.isoformat(),
                        "ticker": "MSFT",
                        "rank_delta": -1,
                        "score_delta": -0.005,
                    }
                ],
                "safety_checks": {"passed": 14, "warning": 0, "failed": 0},
                "monitoring_status": "OK",
                "monitoring_status_reason": "Monitoring quality gates passed.",
                "baseline_coverage_ratio": 1.0,
                "baseline_coverage_status": "OK",
                "factor_rollback_triggered": False,
                "data_limitation_triggered": False,
                "safety": {
                    "manual_review_required": True,
                    "production_effect": "none",
                    "production_weights_modified": False,
                    "active_shadow_weights_modified": False,
                },
                "output_artifacts": {"summary_markdown": str(markdown_path)},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    markdown_path.write_text("# SEC PIT Observe-Only Shadow Lane Summary\n", encoding="utf-8")


def _write_dashboard_baseline_coverage_summary(tmp_path: Path, as_of: date) -> None:
    root = tmp_path / "outputs" / "sec_pit_baseline_coverage"
    root.mkdir(parents=True)
    summary_path = root / f"sec_pit_baseline_coverage_summary_{as_of.isoformat()}.json"
    markdown_path = root / f"sec_pit_baseline_coverage_summary_{as_of.isoformat()}.md"
    summary_path.write_text(
        json.dumps(
            {
                "report_type": "sec_pit_baseline_coverage",
                "coverage_status": "OK",
                "end_date": as_of.isoformat(),
                "expected_rows": 8,
                "actual_rows": 8,
                "missing_rows": 0,
                "coverage_ratio": 1.0,
                "score_completeness_avg": 1.0,
                "output_artifacts": {"summary_markdown": str(markdown_path)},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    markdown_path.write_text("# SEC PIT Baseline Coverage Summary\n", encoding="utf-8")


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
