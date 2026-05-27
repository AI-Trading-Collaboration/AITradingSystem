from __future__ import annotations

import builtins
import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli_commands import sec_pit as sec_pit_cli
from ai_trading_system.config import FundamentalFeaturesConfig, FundamentalRatioFeatureConfig
from ai_trading_system.fundamentals import sec_pit_real_run_diagnostics as diagnostics
from ai_trading_system.fundamentals.sec_pit_panel import (
    build_fundamental_pit_intervals,
    build_sec_pit_feature_panel,
)
from ai_trading_system.fundamentals.sec_pit_real_run_diagnostics import (
    ALIAS_AUDIT_COLUMNS,
    CANDIDATE_SENSITIVITY_COLUMNS,
    COVERAGE_AUDIT_COLUMNS,
    LABEL_AUDIT_COLUMNS,
    PROVENANCE_GAP_COLUMNS,
    build_candidate_sensitivity,
    build_coverage_audit,
    build_label_coverage_audit,
    resolve_baseline_artifact,
    run_sec_pit_real_run_diagnostics,
)


def test_sec_pit_real_run_diagnostics_writes_expected_artifacts(tmp_path: Path) -> None:
    paths = _write_diagnostics_inputs(tmp_path)

    artifacts = _run_diagnostics(paths, tmp_path / "diagnostics")

    summary = _read_json(artifacts.summary_json_path)
    provenance = pd.read_csv(artifacts.provenance_gap_path)
    coverage = pd.read_csv(artifacts.coverage_audit_path)
    alias = pd.read_csv(artifacts.alias_resolution_audit_path)
    labels = pd.read_csv(artifacts.label_coverage_audit_path)
    sensitivity = pd.read_csv(artifacts.candidate_sensitivity_path)
    markdown = artifacts.summary_markdown_path.read_text(encoding="utf-8")

    assert summary["diagnostics_status"] == "OK"
    assert summary["provenance"]["first_loss_stage"] == "feature_panel"
    assert summary["alias_resolution"]["remapped_count"] == 1
    assert summary["baseline"]["status"] == "OK"
    assert summary["coverage"]["features_with_ratio_above_1_before_fix"] == 1
    assert summary["candidate_sensitivity"]["near_promotion_count"] == 1
    assert tuple(provenance.columns) == PROVENANCE_GAP_COLUMNS
    assert tuple(coverage.columns) == COVERAGE_AUDIT_COLUMNS
    assert tuple(alias.columns) == ALIAS_AUDIT_COLUMNS
    assert tuple(labels.columns) == LABEL_AUDIT_COLUMNS
    assert tuple(sensitivity.columns) == CANDIDATE_SENSITIVITY_COLUMNS
    assert "# SEC PIT Real Run Diagnostics" in markdown
    assert "## Manual Review Checklist" in markdown


def test_sec_pit_diagnose_run_cli_and_latest_mode(tmp_path: Path) -> None:
    paths = _write_diagnostics_inputs(tmp_path)

    result = CliRunner().invoke(
        sec_pit_cli.sec_pit_app,
        [
            "diagnose-run",
            "--latest",
            "--tickers",
            "GOOGL",
            "--feature-panel",
            str(paths["feature_panel"]),
            "--evaluation-dir",
            str(paths["evaluation_dir"]),
            "--comparison-dir",
            str(paths["comparison_dir"]),
            "--baseline-score-path",
            str(paths["baseline"]),
            "--output-dir",
            str(tmp_path / "cli_diagnostics"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "SEC PIT diagnostics status: OK" in result.output
    assert (tmp_path / "cli_diagnostics" / "sec_pit_real_run_diagnostics_2023-01-03.json").exists()


def test_provenance_propagation_preserves_derived_feature_lineage() -> None:
    mapped = pd.DataFrame(
        [
            _mapped_metric_row("research_and_development", "RDA", "hash-rd", 1.0),
            _mapped_metric_row("revenue", "REVA", "hash-revenue", 10.0),
        ]
    )
    intervals = build_fundamental_pit_intervals(mapped)
    features = FundamentalFeaturesConfig(
        features=[
            FundamentalRatioFeatureConfig(
                feature_id="research_and_development_intensity",
                name="R&D Intensity",
                description="unit test",
                numerator_metric_id="research_and_development",
                denominator_metric_id="revenue",
                preferred_periods=["quarterly"],
            )
        ]
    )
    sec_companies = _sec_companies_model()

    panel = build_sec_pit_feature_panel(
        intervals=intervals,
        features=features,
        sec_companies=sec_companies,
        start=date(2023, 1, 3),
        end=date(2023, 1, 3),
    )

    assert not panel.empty
    row = panel.iloc[0]
    lineage = json.loads(row["source_lineage"])
    assert row["accession_number"] == "RDA,REVA"
    assert row["accepted_datetime"] == ("2023-01-02T20:00:00+00:00,2023-01-02T20:00:00+00:00")
    assert {item["metric_id"] for item in lineage} == {
        "research_and_development",
        "revenue",
    }
    assert {item["raw_sha256"] for item in lineage} == {"hash-rd", "hash-revenue"}


def test_alias_resolution_reports_unknown_ticker(tmp_path: Path) -> None:
    paths = _write_diagnostics_inputs(tmp_path)

    artifacts = run_sec_pit_real_run_diagnostics(
        start=date(2023, 1, 3),
        end=date(2023, 1, 3),
        tickers=["XYZ"],
        feature_panel_path=paths["feature_panel"],
        evaluation_dir=paths["evaluation_dir"],
        comparison_dir=paths["comparison_dir"],
        baseline_score_path=paths["baseline"],
        output_dir=tmp_path / "diagnostics",
    )

    alias = pd.read_csv(artifacts.alias_resolution_audit_path)
    assert alias.iloc[0]["input_ticker"] == "XYZ"
    assert alias.iloc[0]["resolved"] is False or str(alias.iloc[0]["resolved"]) == "False"
    assert "did not resolve" in alias.iloc[0]["warning"]


def test_baseline_resolver_priority_and_degraded_status(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    explicit = tmp_path / "explicit_scores.csv"
    fallback = tmp_path / "data" / "processed" / "scores_daily.csv"
    _write_baseline(explicit, ticker="GOOG")
    _write_baseline(fallback, ticker="MSFT")

    resolved = resolve_baseline_artifact(
        explicit_path=explicit,
        baseline_score_dir=tmp_path / "missing_daily_score",
        end=date(2023, 1, 3),
    )
    assert resolved.path == explicit
    assert resolved.status == "OK"

    monkeypatch.setattr(diagnostics, "DEFAULT_PROCESSED_BASELINE_SCORE_PATH", fallback)
    fallback_resolved = resolve_baseline_artifact(
        explicit_path=None,
        baseline_score_dir=diagnostics.DEFAULT_BASELINE_SCORE_DIR,
        end=date(2023, 1, 3),
    )
    assert fallback_resolved.path == fallback
    assert fallback_resolved.status == "FALLBACK_USED"

    missing = resolve_baseline_artifact(
        explicit_path=tmp_path / "missing.csv",
        baseline_score_dir=tmp_path / "missing_daily_score",
        end=date(2023, 1, 3),
    )
    assert missing.status == "LIMITED_BASELINE_MISSING"


def test_label_coverage_detects_missing_drawdown_labels(tmp_path: Path) -> None:
    paths = _write_diagnostics_inputs(tmp_path, include_drawdown=False)

    labels = build_label_coverage_audit(
        evaluation_dir=paths["evaluation_dir"],
        comparison_dir=paths["comparison_dir"],
        end=date(2023, 1, 3),
    )

    drawdown = labels.loc[labels["label_name"] == "max_drawdown_forward_20d"].iloc[0]
    assert drawdown["coverage_ratio"] == 0.0
    assert "label column missing" in drawdown["recommended_fix"]


def test_coverage_ratio_is_corrected_and_duplicates_reported(tmp_path: Path) -> None:
    paths = _write_diagnostics_inputs(tmp_path)

    coverage = build_coverage_audit(
        feature_panel_path=paths["feature_panel"],
        start=date(2023, 1, 3),
        end=date(2023, 1, 3),
        canonical_tickers=["GOOG"],
    )

    row = coverage.iloc[0]
    assert row["coverage_ratio_before"] > 1.0
    assert row["coverage_ratio_after"] <= 1.0
    assert row["duplicate_observations"] == 1


def test_candidate_sensitivity_never_promotes_automatically(tmp_path: Path) -> None:
    paths = _write_diagnostics_inputs(tmp_path)

    sensitivity = build_candidate_sensitivity(
        evaluation_dir=paths["evaluation_dir"],
        end=date(2023, 1, 3),
    )

    row = sensitivity.iloc[0]
    assert row["current_recommendation"] == "KEEP_RESEARCH_ONLY"
    assert row["hypothetical_recommendation_if_provenance_fixed"] == "PROMOTE_TO_SHADOW"
    assert row["manual_review_required"] is True or str(row["manual_review_required"]) == "True"
    assert row["production_effect"] == "none"


def test_sec_pit_real_run_diagnostics_output_is_deterministic(tmp_path: Path) -> None:
    paths = _write_diagnostics_inputs(tmp_path)
    output_dir = tmp_path / "diagnostics"

    first = _run_diagnostics(paths, output_dir)
    first_summary = first.summary_json_path.read_text(encoding="utf-8")
    first_gap = first.provenance_gap_path.read_text(encoding="utf-8")
    second = _run_diagnostics(paths, output_dir)

    assert second.summary_json_path.read_text(encoding="utf-8") == first_summary
    assert second.provenance_gap_path.read_text(encoding="utf-8") == first_gap


def test_dashboard_reads_sec_pit_diagnostics_artifact_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    from ai_trading_system.daily_task_dashboard import (
        build_daily_task_dashboard_payload,
        build_daily_task_dashboard_report,
        render_daily_task_dashboard,
    )

    as_of = date(2023, 1, 3)
    metadata_path = _write_daily_ops_metadata(tmp_path, as_of)
    _write_dashboard_diagnostics_summary(tmp_path, as_of)
    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked = (
            "ai_trading_system.fundamentals.sec_pit_real_run_diagnostics",
            "ai_trading_system.fundamentals.sec_pit_evaluation",
            "ai_trading_system.fundamentals.sec_pit_baseline_comparison",
            "ai_trading_system.data.download",
            "ai_trading_system.backtest",
            "ai_trading_system.scoring",
        )
        if any(token in name for token in blocked):
            raise AssertionError(f"dashboard must not import diagnostics pipeline: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        reports_dir=tmp_path,
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    summary = payload["sec_pit_real_run_diagnostics"]
    assert summary["exists"] is True
    assert summary["status"] == "OK"
    assert summary["latest_diagnostics_date"] == "2023-01-03"
    assert summary["missing_provenance_rows"] == 2
    assert summary["first_provenance_loss_stage"] == "feature_panel"
    assert summary["alias_unresolved_count"] == 0
    assert summary["baseline_artifact_status"] == "FALLBACK_USED"
    assert summary["features_with_coverage_ratio_above_1"] == 1
    assert summary["near_promotion_feature_count"] == 1
    assert summary["production_effect"] == "none"
    assert "SEC PIT Real Run Diagnostics" in html
    assert "feature_panel" in html


def _run_diagnostics(paths: dict[str, Path], output_dir: Path) -> Any:
    return run_sec_pit_real_run_diagnostics(
        start=date(2023, 1, 3),
        end=date(2023, 1, 3),
        tickers=["GOOGL"],
        feature_panel_path=paths["feature_panel"],
        evaluation_dir=paths["evaluation_dir"],
        comparison_dir=paths["comparison_dir"],
        baseline_score_path=paths["baseline"],
        output_dir=output_dir,
    )


def _write_diagnostics_inputs(
    tmp_path: Path,
    *,
    include_drawdown: bool = True,
) -> dict[str, Path]:
    processed = tmp_path / "data" / "processed" / "sec_edgar"
    evaluation_dir = tmp_path / "outputs" / "sec_pit_evaluation"
    comparison_dir = tmp_path / "outputs" / "sec_pit_baseline_comparison"
    processed.mkdir(parents=True)
    evaluation_dir.mkdir(parents=True)
    comparison_dir.mkdir(parents=True)
    feature_panel = processed / "sec_pit_feature_panel.csv"
    baseline = tmp_path / "baseline_scores.csv"
    _write_upstream_provenance_artifacts(processed)
    _write_feature_panel_with_provenance_gap(feature_panel)
    _write_evaluation_artifacts(evaluation_dir)
    _write_comparison_artifacts(comparison_dir, include_drawdown=include_drawdown)
    _write_baseline(baseline, ticker="GOOG")
    return {
        "feature_panel": feature_panel,
        "evaluation_dir": evaluation_dir,
        "comparison_dir": comparison_dir,
        "baseline": baseline,
    }


def _write_upstream_provenance_artifacts(processed: Path) -> None:
    base = {
        "ticker": "GOOG",
        "metric_id": "research_and_development,revenue",
        "period": "2022Q4",
        "decision_date": "2023-01-03",
        "accession_number": "GOOG-23-000001",
        "accepted_datetime": "2023-01-02T20:00:00+00:00",
        "filed_date": "2023-01-02",
        "raw_sha256": "hash-good",
        "source_concept": "ResearchAndDevelopmentExpense,RevenueFromContract",
        "pit_grade": "B_RECONSTRUCTED_SEC_FILING_PIT",
    }
    for name in (
        "filing_timeline",
        "xbrl_facts_long",
        "mapped_metrics_long",
        "fundamental_pit_intervals",
    ):
        pd.DataFrame([base]).to_csv(processed / f"{name}.csv", index=False)
    raw_manifest = processed.parents[1] / "raw" / "sec_edgar" / "manifest"
    raw_manifest.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                **base,
                "output_path": str(processed.parents[1] / "raw" / "sec_edgar" / "GOOG.json"),
                "checksum_sha256": "hash-good",
            }
        ]
    ).to_csv(raw_manifest / "sec_edgar_raw_manifest.csv", index=False)


def _write_feature_panel_with_provenance_gap(path: Path) -> None:
    rows = [
        {
            "decision_date": "2023-01-03",
            "ticker": "GOOG",
            "feature_id": "research_and_development_intensity",
            "feature_value": 0.1,
            "feature_unit": "ratio",
            "input_metric_ids": "research_and_development,revenue",
            "input_accession_numbers": "GOOG-23-000001,GOOG-23-000001",
            "input_available_times_utc": ("2023-01-02T20:00:00+00:00,2023-01-02T20:00:00+00:00"),
            "max_input_available_time_utc": "2023-01-02T20:00:00+00:00",
            "pit_data_grade": "B_RECONSTRUCTED_SEC_FILING_PIT",
            "confidence_level": "high",
            "confidence_reason": "unit test",
            "period_type": "quarterly",
            "period_end": "2022-12-31",
            "input_metric_units": "USD,USD",
            "accepted_datetime": "",
            "filed_date": "",
            "raw_sha256": "",
            "source_concept": "research_and_development,revenue",
            "source_lineage": "",
        },
        {
            "decision_date": "2023-01-03",
            "ticker": "GOOG",
            "feature_id": "research_and_development_intensity",
            "feature_value": 0.12,
            "feature_unit": "ratio",
            "input_metric_ids": "research_and_development,revenue",
            "input_accession_numbers": "GOOG-23-000002,GOOG-23-000002",
            "input_available_times_utc": ("2023-01-02T21:00:00+00:00,2023-01-02T21:00:00+00:00"),
            "max_input_available_time_utc": "2023-01-02T21:00:00+00:00",
            "pit_data_grade": "B_RECONSTRUCTED_SEC_FILING_PIT",
            "confidence_level": "high",
            "confidence_reason": "unit test duplicate",
            "period_type": "annual",
            "period_end": "2022-12-31",
            "input_metric_units": "USD,USD",
            "accepted_datetime": "",
            "filed_date": "",
            "raw_sha256": "",
            "source_concept": "research_and_development,revenue",
            "source_lineage": "",
        },
    ]
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_evaluation_artifacts(root: Path) -> None:
    suffix = "2023-01-03"
    summary = root / f"sec_pit_evaluation_summary_{suffix}.json"
    feature = root / f"sec_pit_feature_effectiveness_{suffix}.csv"
    attribution = root / f"sec_pit_signal_attribution_{suffix}.csv"
    weights = root / f"sec_pit_shadow_candidate_weights_{suffix}.csv"
    summary.write_text(
        json.dumps(
            {
                "report_type": "sec_pit_cognitive_evaluation",
                "start_date": suffix,
                "end_date": suffix,
                "metadata": {"tickers": ["GOOG"]},
                "output_artifacts": {
                    "feature_effectiveness_csv": str(feature),
                    "signal_attribution_csv": str(attribution),
                    "shadow_candidate_weights_csv": str(weights),
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
                "feature_id": "research_and_development_intensity",
                "metric_id": "research_and_development,revenue",
                "sample_count": 40,
                "coverage_ratio": 1.0,
                "valid_ticker_count": 5,
                "start_date": suffix,
                "end_date": suffix,
                "pit_grade": "B_RECONSTRUCTED_SEC_FILING_PIT",
                "ic_1d": 0.1,
                "ic_5d": 0.1,
                "ic_20d": 0.1,
                "ic_60d": 0.1,
                "rank_ic_20d": 0.06,
                "hit_rate_20d": 0.6,
                "avg_forward_return_top_quantile_20d": 0.1,
                "avg_forward_return_bottom_quantile_20d": 0.0,
                "spread_top_minus_bottom_20d": 0.1,
                "max_drawdown_top_quantile_20d": -0.02,
                "stability_score": 0.8,
                "data_quality_score": 0.45,
                "recommendation": "KEEP_RESEARCH_ONLY",
            }
        ]
    ).to_csv(feature, index=False)
    pd.DataFrame(
        [
            {
                "decision_date": suffix,
                "ticker": "GOOG",
                "feature_id": "research_and_development_intensity",
                "metric_id": "research_and_development,revenue",
                "feature_value": 0.1,
                "normalized_value": 1.0,
                "signal_direction": "UNKNOWN",
                "weight": 0.0,
                "contribution": 0.0,
                "available_time": "2023-01-02T20:00:00+00:00",
                "period": "2022-12-31",
                "form": "",
                "accession_number": "GOOG-23-000001",
                "accepted_datetime": "",
                "filed_date": "",
                "source_concept": "research_and_development,revenue",
                "source_taxonomy": "",
                "raw_sha256": "",
                "source_url_or_raw_path": "",
                "pit_grade": "B_RECONSTRUCTED_SEC_FILING_PIT",
                "source_lineage": "",
                "forward_return_20d": 0.05,
                "relative_return_vs_QQQ_20d": 0.02,
                "max_drawdown_forward_20d": -0.03,
            }
        ]
    ).to_csv(attribution, index=False)
    pd.DataFrame(
        [
            {
                "feature_id": "research_and_development_intensity",
                "metric_id": "research_and_development,revenue",
                "current_weight": 0.0,
                "suggested_shadow_weight": 0.0,
                "weight_delta": 0.0,
                "evidence_score": 0.8,
                "stability_score": 0.8,
                "coverage_ratio": 1.0,
                "pit_quality_score": 0.45,
                "risk_note": "unit test",
                "manual_review_required": True,
                "production_effect": "none",
            }
        ]
    ).to_csv(weights, index=False)


def _write_comparison_artifacts(root: Path, *, include_drawdown: bool) -> None:
    suffix = "2023-01-03"
    summary = root / f"sec_pit_baseline_comparison_summary_{suffix}.json"
    impact = root / f"sec_pit_decision_impact_{suffix}.csv"
    summary.write_text(
        json.dumps(
            {
                "report_type": "sec_pit_baseline_comparison",
                "start_date": suffix,
                "end_date": suffix,
                "comparison_status": "OK",
                "output_artifacts": {"decision_impact_csv": str(impact)},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    row = {
        "decision_date": suffix,
        "ticker": "GOOG",
        "baseline_score": 60.0,
        "sec_pit_enhanced_score": 60.0,
        "score_delta": 0.0,
        "baseline_rank": 1,
        "sec_pit_rank": 1,
        "rank_delta": 0,
        "baseline_action": "WATCH",
        "sec_pit_suggested_action": "WATCH",
        "action_changed": False,
        "top_positive_sec_pit_features": "",
        "top_negative_sec_pit_features": "",
        "forward_return_20d": 0.05,
        "relative_return_vs_QQQ_20d": 0.02,
        "pit_grade": "B_RECONSTRUCTED_SEC_FILING_PIT",
        "manual_review_required": True,
        "production_effect": "none",
    }
    if include_drawdown:
        row["max_drawdown_forward_20d"] = -0.03
    pd.DataFrame([row]).to_csv(impact, index=False)


def _write_baseline(path: Path, *, ticker: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "decision_date": "2023-01-03",
                "ticker": ticker,
                "baseline_score": 60.0,
                "baseline_action": "WATCH",
            }
        ]
    ).to_csv(path, index=False)


def _mapped_metric_row(
    metric_id: str,
    accession: str,
    raw_sha: str,
    value: float,
) -> dict[str, Any]:
    return {
        "ticker": "GOOG",
        "metric_id": metric_id,
        "period_type": "quarterly",
        "period_end": "2022-12-31",
        "value": value,
        "unit": "USD",
        "source_accession_number": accession,
        "accession_number": accession,
        "available_for_signal_date": "2023-01-03",
        "available_time_utc": "2023-01-02T20:00:00+00:00",
        "accepted_datetime": "2023-01-02T20:00:00+00:00",
        "filed_date": "2023-01-02",
        "form": "10-Q",
        "source_concept": metric_id,
        "source_taxonomy": "us-gaap",
        "raw_sha256": raw_sha,
        "source_url_or_raw_path": f"raw/{accession}.json",
        "pit_data_grade": "B_RECONSTRUCTED_SEC_FILING_PIT",
        "confidence_level": "high",
        "source_lineage": json.dumps(
            [
                {
                    "metric_id": metric_id,
                    "accession_number": accession,
                    "available_time": "2023-01-02T20:00:00+00:00",
                    "raw_sha256": raw_sha,
                }
            ],
            sort_keys=True,
            separators=(",", ":"),
        ),
    }


def _sec_companies_model() -> Any:
    from ai_trading_system.config import SecCompaniesConfig, SecCompanyConfig

    return SecCompaniesConfig(
        companies=[
            SecCompanyConfig(
                ticker="GOOG",
                cik="0001652044",
                company_name="Alphabet Inc.",
                expected_taxonomies=["us-gaap", "dei"],
            )
        ]
    )


def _write_daily_ops_metadata(tmp_path: Path, as_of: date) -> Path:
    path = tmp_path / "daily_ops_metadata.json"
    path.write_text(
        json.dumps(
            {
                "run_id": f"unit:{as_of.isoformat()}",
                "status": "PASS",
                "project_root": str(tmp_path),
                "commands": [],
                "step_results": [],
                "git": {"dirty": False, "commit": "unit"},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def _write_dashboard_diagnostics_summary(tmp_path: Path, as_of: date) -> None:
    root = tmp_path / "outputs" / "sec_pit_diagnostics"
    root.mkdir(parents=True)
    json_path = root / f"sec_pit_real_run_diagnostics_{as_of.isoformat()}.json"
    md_path = root / f"sec_pit_real_run_diagnostics_{as_of.isoformat()}.md"
    payload = {
        "report_type": "sec_pit_real_run_diagnostics",
        "diagnostics_status": "OK",
        "end_date": as_of.isoformat(),
        "production_effect": "none",
        "provenance": {"missing_rows": 2, "first_loss_stage": "feature_panel"},
        "alias_resolution": {"unresolved_count": 0},
        "baseline": {"status": "FALLBACK_USED"},
        "labels": {"max_drawdown_forward_20d_coverage": 1.0},
        "coverage": {"features_with_ratio_above_1_before_fix": 1},
        "candidate_sensitivity": {"near_promotion_count": 1},
        "output_artifacts": {"summary_markdown": str(md_path)},
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text("# SEC PIT Real Run Diagnostics\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload
