from __future__ import annotations

import builtins
import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli_commands import sec_pit as sec_pit_cli
from ai_trading_system.fundamentals.sec_pit_candidate_review import (
    BASELINE_OVERLAP_COLUMNS,
    BY_PERIOD_COLUMNS,
    BY_TICKER_COLUMNS,
    CANDIDATE_EVIDENCE_COLUMNS,
    SHADOW_PROPOSAL_COLUMNS,
    run_sec_pit_candidate_review,
)


def test_sec_pit_candidate_review_writes_expected_artifacts(tmp_path: Path) -> None:
    paths = _write_review_inputs(tmp_path)

    artifacts = _run_review(paths, tmp_path / "review")

    summary = _read_json(artifacts.summary_json_path)
    evidence = pd.read_csv(artifacts.candidate_evidence_path)
    by_ticker = pd.read_csv(artifacts.by_ticker_path)
    by_period = pd.read_csv(artifacts.by_period_path)
    overlap = pd.read_csv(artifacts.baseline_overlap_path)
    proposal = pd.read_csv(artifacts.shadow_proposal_path)
    markdown = artifacts.summary_markdown_path.read_text(encoding="utf-8")

    assert artifacts.status == "OK"
    assert summary["review_status"] == "OK"
    assert summary["candidate_count"] == 1
    assert summary["ready_for_manual_review_count"] == 1
    assert summary["primary_candidate"] == "capex_intensity"
    assert summary["diagnostics_status"] == "OK"
    assert summary["provenance_complete"] is True
    assert tuple(evidence.columns) == CANDIDATE_EVIDENCE_COLUMNS
    assert tuple(by_ticker.columns) == BY_TICKER_COLUMNS
    assert tuple(by_period.columns) == BY_PERIOD_COLUMNS
    assert tuple(overlap.columns) == BASELINE_OVERLAP_COLUMNS
    assert tuple(proposal.columns) == SHADOW_PROPOSAL_COLUMNS
    assert set(evidence["production_effect"]) == {"none"}
    assert evidence["manual_review_required"].astype(str).str.lower().eq("true").all()
    assert set(proposal["production_effect"]) == {"none"}
    assert proposal["review_required"].astype(str).str.lower().eq("true").all()
    assert proposal.iloc[0]["proposal_status"] == "READY_FOR_MANUAL_REVIEW"
    assert abs(float(proposal.iloc[0]["suggested_observe_only_weight"])) <= float(
        proposal.iloc[0]["max_allowed_initial_weight"]
    )
    assert "# SEC PIT Shadow Candidate Review" in markdown
    assert "## Manual Review Checklist" in markdown


def test_sec_pit_review_candidates_cli_latest_mode(tmp_path: Path) -> None:
    paths = _write_review_inputs(tmp_path)

    result = CliRunner().invoke(
        sec_pit_cli.sec_pit_app,
        [
            "review-candidates",
            "--latest",
            "--evaluation-dir",
            str(paths["evaluation_dir"]),
            "--comparison-dir",
            str(paths["comparison_dir"]),
            "--diagnostics-dir",
            str(paths["diagnostics_dir"]),
            "--candidate-feature",
            "capex_intensity",
            "--output-dir",
            str(tmp_path / "cli_review"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "SEC PIT candidate review status: OK" in result.output
    assert (tmp_path / "cli_review" / "sec_pit_candidate_review_summary_2023-01-05.json").exists()


def test_baseline_overlap_degrades_when_baseline_fields_are_missing(tmp_path: Path) -> None:
    paths = _write_review_inputs(tmp_path, baseline_fields=False)

    artifacts = _run_review(paths, tmp_path / "review")

    overlap = pd.read_csv(artifacts.baseline_overlap_path)
    assert overlap.iloc[0]["redundancy_risk"] == "UNKNOWN"
    assert "LIMITED_BASELINE_FIELDS_MISSING" in overlap.iloc[0]["overlap_interpretation"]


def test_sec_pit_candidate_review_does_not_write_production_configs(tmp_path: Path) -> None:
    paths = _write_review_inputs(tmp_path)
    production_config = tmp_path / "config" / "weights" / "weight_profile_current.yaml"
    shadow_config = tmp_path / "config" / "weights" / "shadow_weight_profiles.yaml"
    production_config.parent.mkdir(parents=True)
    production_config.write_text("weights:\n  trend: 0.25\n", encoding="utf-8")
    shadow_config.write_text("profiles: []\n", encoding="utf-8")
    before = {
        production_config: production_config.read_text(encoding="utf-8"),
        shadow_config: shadow_config.read_text(encoding="utf-8"),
    }

    _run_review(paths, tmp_path / "review")

    assert production_config.read_text(encoding="utf-8") == before[production_config]
    assert shadow_config.read_text(encoding="utf-8") == before[shadow_config]


def test_sec_pit_candidate_review_repeated_run_is_deterministic(tmp_path: Path) -> None:
    paths = _write_review_inputs(tmp_path)
    output_dir = tmp_path / "review"

    first = _run_review(paths, output_dir)
    first_summary = first.summary_json_path.read_text(encoding="utf-8")
    first_evidence = first.candidate_evidence_path.read_text(encoding="utf-8")
    second = _run_review(paths, output_dir)

    assert second.summary_json_path.read_text(encoding="utf-8") == first_summary
    assert second.candidate_evidence_path.read_text(encoding="utf-8") == first_evidence


def test_missing_artifacts_degrade_with_schema_outputs(tmp_path: Path) -> None:
    artifacts = run_sec_pit_candidate_review(
        start=date(2023, 1, 1),
        end=date(2023, 1, 5),
        evaluation_dir=tmp_path / "missing_eval",
        comparison_dir=tmp_path / "missing_cmp",
        diagnostics_dir=tmp_path / "missing_diag",
        candidate_features=["capex_intensity"],
        output_dir=tmp_path / "review",
    )

    summary = _read_json(artifacts.summary_json_path)
    evidence = pd.read_csv(artifacts.candidate_evidence_path)
    assert artifacts.status == "LIMITED_MISSING_ARTIFACTS"
    assert summary["review_status"] == "LIMITED_MISSING_ARTIFACTS"
    assert tuple(evidence.columns) == CANDIDATE_EVIDENCE_COLUMNS
    assert evidence.iloc[0]["recommendation"] == "INSUFFICIENT_EVIDENCE"


def test_candidate_concentration_risk_is_reported(tmp_path: Path) -> None:
    paths = _write_review_inputs(tmp_path, concentrated=True)

    artifacts = _run_review(paths, tmp_path / "review")

    evidence = pd.read_csv(artifacts.candidate_evidence_path)
    proposal = pd.read_csv(artifacts.shadow_proposal_path)
    summary = _read_json(artifacts.summary_json_path)
    assert "candidate_concentration_risk" in evidence.iloc[0]["blocking_reasons"]
    assert "candidate_concentration_risk" in proposal.iloc[0]["risk_notes"]
    assert any("concentrated" in item for item in summary["limitations"])


def test_insufficient_evidence_status(tmp_path: Path) -> None:
    paths = _write_review_inputs(tmp_path, insufficient=True)

    artifacts = _run_review(paths, tmp_path / "review")

    summary = _read_json(artifacts.summary_json_path)
    proposal = pd.read_csv(artifacts.shadow_proposal_path)
    assert artifacts.status == "INSUFFICIENT_EVIDENCE"
    assert summary["review_status"] == "INSUFFICIENT_EVIDENCE"
    assert proposal.iloc[0]["proposal_status"] == "INSUFFICIENT_EVIDENCE"


def test_summary_json_contains_required_schema_fields(tmp_path: Path) -> None:
    paths = _write_review_inputs(tmp_path)

    artifacts = _run_review(paths, tmp_path / "review")

    summary = _read_json(artifacts.summary_json_path)
    assert {
        "generated_at",
        "start_date",
        "end_date",
        "review_status",
        "candidate_count",
        "ready_for_manual_review_count",
        "keep_research_only_count",
        "insufficient_evidence_count",
        "top_candidates",
        "primary_candidate",
        "diagnostics_status",
        "provenance_complete",
        "drawdown_label_coverage",
        "limitations",
    }.issubset(summary)


def test_dashboard_reads_sec_pit_candidate_review_artifact_only(
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
    _write_dashboard_candidate_review_summary(tmp_path, as_of)
    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked = (
            "ai_trading_system.fundamentals.sec_pit_candidate_review",
            "ai_trading_system.fundamentals.sec_pit_real_run_diagnostics",
            "ai_trading_system.fundamentals.sec_pit_evaluation",
            "ai_trading_system.fundamentals.sec_pit_baseline_comparison",
            "ai_trading_system.data.download",
            "ai_trading_system.backtest",
            "ai_trading_system.scoring",
        )
        if any(token in name for token in blocked):
            raise AssertionError(f"dashboard must not import candidate review pipeline: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        reports_dir=tmp_path,
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    summary = payload["sec_pit_candidate_review"]
    assert summary["exists"] is True
    assert summary["status"] == "OK"
    assert summary["latest_review_date"] == "2023-01-05"
    assert summary["candidate_count"] == 1
    assert summary["ready_for_manual_review_count"] == 1
    assert summary["primary_candidate"] == "capex_intensity"
    assert summary["diagnostics_status"] == "OK"
    assert summary["drawdown_label_coverage"] == 0.9444
    assert summary["top_candidate_feature"] == "capex_intensity"
    assert summary["proposal_status"] == "READY_FOR_MANUAL_REVIEW"
    assert summary["production_effect"] == "none"
    assert "SEC PIT Candidate Review" in html
    assert "capex_intensity" in html


def _run_review(paths: dict[str, Path], output_dir: Path) -> Any:
    return run_sec_pit_candidate_review(
        start=date(2023, 1, 1),
        end=date(2023, 1, 5),
        evaluation_dir=paths["evaluation_dir"],
        comparison_dir=paths["comparison_dir"],
        diagnostics_dir=paths["diagnostics_dir"],
        candidate_features=["capex_intensity"],
        output_dir=output_dir,
    )


def _write_review_inputs(
    tmp_path: Path,
    *,
    baseline_fields: bool = True,
    concentrated: bool = False,
    insufficient: bool = False,
) -> dict[str, Path]:
    evaluation_dir = tmp_path / "outputs" / "sec_pit_evaluation"
    comparison_dir = tmp_path / "outputs" / "sec_pit_baseline_comparison"
    diagnostics_dir = tmp_path / "outputs" / "sec_pit_diagnostics"
    evaluation_dir.mkdir(parents=True)
    comparison_dir.mkdir(parents=True)
    diagnostics_dir.mkdir(parents=True)
    _write_evaluation_artifacts(
        evaluation_dir,
        concentrated=concentrated,
        insufficient=insufficient,
    )
    _write_comparison_artifacts(comparison_dir, baseline_fields=baseline_fields)
    _write_diagnostics_artifacts(diagnostics_dir)
    return {
        "evaluation_dir": evaluation_dir,
        "comparison_dir": comparison_dir,
        "diagnostics_dir": diagnostics_dir,
    }


def _write_evaluation_artifacts(
    root: Path,
    *,
    concentrated: bool,
    insufficient: bool,
) -> None:
    suffix = "2023-01-05"
    summary = root / f"sec_pit_evaluation_summary_{suffix}.json"
    feature = root / f"sec_pit_feature_effectiveness_{suffix}.csv"
    attribution = root / f"sec_pit_signal_attribution_{suffix}.csv"
    weights = root / f"sec_pit_shadow_candidate_weights_{suffix}.csv"
    summary.write_text(
        json.dumps(
            {
                "report_type": "sec_pit_cognitive_evaluation",
                "status": "PASS",
                "start_date": "2023-01-01",
                "end_date": suffix,
                "production_effect": "none",
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
                "feature_id": "capex_intensity",
                "metric_id": "capex,revenue",
                "sample_count": 8 if not insufficient else 2,
                "coverage_ratio": 1.0 if not insufficient else 0.2,
                "valid_ticker_count": 4 if not insufficient else 1,
                "start_date": "2023-01-01",
                "end_date": suffix,
                "pit_grade": "B_RECONSTRUCTED_SEC_FILING_PIT",
                "ic_1d": -0.05,
                "ic_5d": -0.08,
                "ic_20d": -0.12,
                "ic_60d": -0.10,
                "rank_ic_20d": -0.12,
                "hit_rate_20d": 0.625,
                "avg_forward_return_top_quantile_20d": 0.01,
                "avg_forward_return_bottom_quantile_20d": 0.08,
                "spread_top_minus_bottom_20d": -0.07,
                "max_drawdown_top_quantile_20d": -0.03,
                "stability_score": 0.75 if not insufficient else 0.2,
                "data_quality_score": 0.95,
                "recommendation": (
                    "PROMOTE_TO_SHADOW" if not insufficient else "EXCLUDE_INSUFFICIENT_DATA"
                ),
            }
        ]
    ).to_csv(feature, index=False)
    rows = _attribution_rows(concentrated=concentrated)
    pd.DataFrame(rows).to_csv(attribution, index=False)
    pd.DataFrame(
        [
            {
                "feature_id": "capex_intensity",
                "metric_id": "capex,revenue",
                "current_weight": 0.0,
                "suggested_shadow_weight": -0.05 if not insufficient else 0.0,
                "weight_delta": -0.05 if not insufficient else 0.0,
                "evidence_score": 1.0,
                "stability_score": 0.75,
                "coverage_ratio": 1.0,
                "pit_quality_score": 0.95,
                "risk_note": "unit test",
                "manual_review_required": True,
                "production_effect": "none",
            }
        ]
    ).to_csv(weights, index=False)


def _attribution_rows(*, concentrated: bool) -> list[dict[str, object]]:
    base = [
        ("2023-01-02", "NVDA", -2.0 if concentrated else -1.0, 0.10, 0.08, -0.02),
        ("2023-01-03", "NVDA", -2.0 if concentrated else -0.8, 0.09, 0.07, -0.03),
        ("2023-01-02", "MSFT", -0.1, 0.02, 0.01, -0.01),
        ("2023-01-03", "MSFT", 0.2, -0.01, -0.02, -0.04),
        ("2023-01-02", "AMD", 0.8, -0.04, -0.05, -0.08),
        ("2023-01-03", "AMD", 0.9, -0.03, -0.04, -0.07),
        ("2023-01-02", "AVGO", -0.3, 0.03, 0.02, -0.02),
        ("2023-01-03", "AVGO", 0.4, -0.02, -0.03, -0.05),
    ]
    rows: list[dict[str, object]] = []
    for decision_date, ticker, normalized, forward, relative, drawdown in base:
        contribution = normalized * -0.05
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
                "contribution": contribution,
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
                "forward_return_20d": forward,
                "relative_return_vs_QQQ_20d": relative,
                "max_drawdown_forward_20d": drawdown,
            }
        )
    return rows


def _write_comparison_artifacts(root: Path, *, baseline_fields: bool) -> None:
    suffix = "2023-01-05"
    summary = root / f"sec_pit_baseline_comparison_summary_{suffix}.json"
    impact = root / f"sec_pit_decision_impact_{suffix}.csv"
    alpha = root / f"sec_pit_incremental_alpha_{suffix}.csv"
    summary.write_text(
        json.dumps(
            {
                "report_type": "sec_pit_baseline_comparison",
                "comparison_status": "OK",
                "start_date": "2023-01-01",
                "end_date": suffix,
                "output_artifacts": {
                    "decision_impact_csv": str(impact),
                    "incremental_alpha_csv": str(alpha),
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    records: list[dict[str, object]] = []
    for ticker, score in (("NVDA", 70), ("MSFT", 65), ("AMD", 60), ("AVGO", 68)):
        row: dict[str, object] = {
            "decision_date": "2023-01-02",
            "ticker": ticker,
            "sec_pit_enhanced_score": score + 1,
            "score_delta": 1,
            "forward_return_20d": 0.05,
            "relative_return_vs_QQQ_20d": 0.02,
            "max_drawdown_forward_20d": -0.03,
            "manual_review_required": True,
            "production_effect": "none",
        }
        if baseline_fields:
            row["baseline_score"] = score
        records.append(row)
    pd.DataFrame(records).to_csv(impact, index=False)
    pd.DataFrame(
        [
            {
                "bucket": "top_sec_pit",
                "sample_count": 4,
                "avg_forward_return_20d": 0.05,
                "avg_relative_return_vs_QQQ_20d": 0.02,
                "avg_max_drawdown_forward_20d": -0.03,
                "hit_rate_20d": 0.75,
                "baseline_avg_forward_return_20d": 0.04,
                "sec_pit_avg_forward_return_20d": 0.05,
                "incremental_return_20d": 0.01,
                "drawdown_improvement_20d": 0.02,
                "interpretation": "unit test",
            }
        ]
    ).to_csv(alpha, index=False)


def _write_diagnostics_artifacts(root: Path) -> None:
    suffix = "2023-01-05"
    summary = root / f"sec_pit_real_run_diagnostics_{suffix}.json"
    sensitivity = root / f"sec_pit_candidate_sensitivity_{suffix}.csv"
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
                "labels": {"max_drawdown_forward_20d_coverage": 0.9444},
                "output_artifacts": {
                    "candidate_sensitivity_csv": str(sensitivity),
                    "label_coverage_audit_csv": str(labels),
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
                "rank_ic_20d": -0.12,
                "data_quality_score": 0.95,
                "coverage_ratio": 1.0,
                "stability_score": 0.75,
                "current_recommendation": "PROMOTE_TO_SHADOW",
                "hypothetical_recommendation_if_provenance_fixed": "PROMOTE_TO_SHADOW",
                "blocking_reason": "",
                "minimum_required_fix": "manual review required",
                "manual_review_required": True,
                "production_effect": "none",
            }
        ]
    ).to_csv(sensitivity, index=False)
    pd.DataFrame(
        [
            {
                "label_name": "max_drawdown_forward_20d",
                "required": True,
                "available_count": 8,
                "missing_count": 0,
                "coverage_ratio": 1.0,
                "source_artifact": "unit",
                "recommended_fix": "",
            }
        ]
    ).to_csv(labels, index=False)


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


def _write_dashboard_candidate_review_summary(tmp_path: Path, as_of: date) -> None:
    root = tmp_path / "outputs" / "sec_pit_candidate_review"
    root.mkdir(parents=True)
    summary_path = root / f"sec_pit_candidate_review_summary_{as_of.isoformat()}.json"
    markdown_path = root / f"sec_pit_candidate_review_summary_{as_of.isoformat()}.md"
    summary_path.write_text(
        json.dumps(
            {
                "report_type": "sec_pit_candidate_review",
                "review_status": "OK",
                "end_date": as_of.isoformat(),
                "candidate_count": 1,
                "ready_for_manual_review_count": 1,
                "primary_candidate": "capex_intensity",
                "diagnostics_status": "OK",
                "drawdown_label_coverage": 0.9444,
                "manual_review_required": True,
                "production_effect": "none",
                "top_candidates": [
                    {
                        "feature_id": "capex_intensity",
                        "proposal_status": "READY_FOR_MANUAL_REVIEW",
                    }
                ],
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
    markdown_path.write_text("# SEC PIT Shadow Candidate Review\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload
