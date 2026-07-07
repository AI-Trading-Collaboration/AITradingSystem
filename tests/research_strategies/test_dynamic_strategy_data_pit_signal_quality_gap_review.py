from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_data_pit_signal_quality_gap_review as review
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_dynamic_strategy_data_pit_signal_quality_gap_review_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "gap_review"
    docs_root = tmp_path / "docs" / "research"

    payload = review.run_dynamic_strategy_data_pit_signal_quality_gap_review(
        **_source_kwargs(source_paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 7),
        validate_data_as_of=date(2026, 7, 5),
    )

    assert payload["status"] == review.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is True
    assert payload["data_quality_gate_command"] == "aits validate-data --as-of 2026-07-05"
    assert payload["validate_data_status"] == "PASS_WITH_WARNINGS"
    assert payload["validate_data_error_count"] == 0
    assert payload["validate_data_warning_count"] == 2
    assert payload["backtest_run"] is False
    assert payload["new_strategy_backtest_run"] is False
    assert payload["new_signal_generated"] is False
    assert payload["scoring_run"] is False
    assert payload["fresh_market_data_read_by_2402"] is False
    assert payload["recombination_line_paused"] is True
    assert payload["resume_candidate_search_recommended"] is False
    assert payload["pit_coverage_matrix_recommended"] is True
    assert payload["signal_construction_review_recommended"] is True
    assert payload["regime_expectation_scoring_review_recommended"] is True
    assert payload["threshold_meta_dataset_recommended"] is True
    assert payload["recommended_next_research_task"] == review.NEXT_ROUTE

    assert payload["data_quality_gap_review_ready"] is True
    assert payload["pit_coverage_gap_review_ready"] is True
    assert payload["signal_quality_gap_review_ready"] is True
    assert payload["regime_labeling_gap_review_ready"] is True
    assert payload["threshold_meta_dataset_gap_review_ready"] is True
    assert payload["prioritized_gap_matrix_ready"] is True
    assert {
        row["gap_id"] for row in payload["prioritized_gap_matrix"]
    } == {
        "2402-DATA-01",
        "2402-DATA-02",
        "2402-PIT-01",
        "2402-SIGNAL-01",
        "2402-VALIDUNTIL-01",
        "2402-REGIME-01",
        "2402-THRESHOLD-01",
        "2402-REPORTING-01",
    }
    assert len(payload["data_quality_gap_review"]["warnings_relevant_to_dynamic_strategy"]) == 2

    for field in review.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    for key in (
        "json_path",
        "data_quality_gap_matrix_json",
        "pit_coverage_gap_review_json",
        "signal_quality_gap_review_json",
        "regime_labeling_gap_review_json",
        "threshold_meta_dataset_gap_review_json",
        "markdown_path",
        "data_quality_gap_matrix_markdown",
        "pit_coverage_gap_review_markdown",
        "signal_quality_gap_review_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_data_pit_signal_quality_gap_review_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "gap_review_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-data-pit-signal-quality-gap-review",
            *_source_args(source_paths),
            "--as-of",
            "2026-07-07",
            "--validate-data-as-of",
            "2026-07-05",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert review.READY_STATUS in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert "production_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "gap_review_result.json").exists()
    assert (output_root / "data_quality_gap_matrix.json").exists()
    assert (output_root / "pit_coverage_gap_review.json").exists()
    assert (output_root / "signal_quality_gap_review.json").exists()
    assert (output_root / "regime_labeling_gap_review.json").exists()
    assert (output_root / "threshold_meta_dataset_gap_review.json").exists()


def test_dynamic_strategy_data_pit_signal_quality_gap_review_registry_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_data_pit_signal_quality_gap_review"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-data-pit-signal-quality-gap-review"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("gap_review_result.json" in item for item in entry["artifact_globs"])
    assert any("threshold_meta_dataset_gap_review.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2403_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "dynamic_strategy_data_pit_signal_quality_gap_review" in catalog
    assert "dynamic-strategy-data-pit-signal-quality-gap-review" in system_flow
    assert review.TASK_REGISTER_ID in task_register


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    paths = {
        "plateau_decision_2401": root / "plateau_decision_2401.json",
        "route_2401": root / "route_2401.json",
        "owner_review_2400": root / "owner_review_2400.json",
        "targeted_retest_2399": root / "targeted_retest_2399.json",
        "gate_evidence_matrix_2399": root / "gate_evidence_matrix_2399.json",
        "decision_update_2399": root / "decision_update_2399.json",
        "expanded_retest_2386": root / "expanded_retest_2386.json",
        "signal_family_screening_2386": root / "signal_family_screening_2386.json",
        "cadence_bias_audit_2364": root / "cadence_bias_audit_2364.json",
        "validate_data_audit": root / "validate_data_audit.json",
        "validate_data_report": root / "data_quality_2026-07-05.md",
    }
    _write_json(
        paths["plateau_decision_2401"],
        {
            **_safe_doc(review.m2401.READY_STATUS),
            "owner_decision": review.m2401.OWNER_DECISION,
            "recombination_line_plateau_detected": True,
            "continue_local_targeted_improvement_recommended": False,
            "recommended_next_research_task": review.m2401.NEXT_ROUTE,
            "data_signal_quality_review_scope": {
                "PIT_coverage": [
                    "feature point-in-time lineage",
                    "advisory valid-from / valid-until correctness",
                ],
            },
        },
    )
    _write_json(
        paths["route_2401"],
        {
            **_safe_doc(review.m2401.READY_STATUS),
            "data_signal_quality_review_route": {
                "recommended_next_research_task": review.m2401.NEXT_ROUTE,
            },
            "recommended_next_research_task": review.m2401.NEXT_ROUTE,
        },
    )
    _write_json(
        paths["owner_review_2400"],
        {
            **_safe_doc(review.m2400.READY_STATUS),
            "owner_decision": review.m2400.OWNER_DECISION,
        },
    )
    _write_json(
        paths["targeted_retest_2399"],
        {
            **_safe_doc(review.m2399.READY_STATUS),
            "best_targeted_variant": review.m2401.BEST_TARGETED_VARIANT,
            "best_targeted_variant_decision": "CONTINUE_TARGETED_IMPROVEMENT",
            "observation_preview_candidates_count": 0,
        },
    )
    _write_json(
        paths["gate_evidence_matrix_2399"],
        {
            **_safe_doc(review.m2399.READY_STATUS),
            "gate_evidence_matrix": [
                {
                    "candidate_id": review.m2401.BEST_TARGETED_VARIANT,
                    "valid_until_stale_signal_evidence": {
                        "stale_signal_execution_count": 2,
                        "signal_to_execution_lag_days": 1.5,
                    },
                    "execution_metrics": {
                        "stale_signal_execution_count": 2,
                        "signal_to_execution_lag_days": 1.5,
                    },
                    "regime_expectation_evidence": {
                        "regime_expectation_score": 0.42,
                        "regime_expectation_not_weak": False,
                    },
                },
            ],
        },
    )
    _write_json(
        paths["decision_update_2399"],
        {
            **_safe_doc(review.m2399.READY_STATUS),
            "decision_update": {
                "research_only_observation_preview_exists": False,
                "observation_preview_candidates_count": 0,
                "candidate_decisions": [
                    {
                        "candidate_id": review.m2401.BEST_TARGETED_VARIANT,
                        "decision": "CONTINUE_TARGETED_IMPROVEMENT",
                    }
                ],
            },
        },
    )
    _write_json(
        paths["expanded_retest_2386"],
        {
            **_safe_doc(review.m2386.READY_STATUS),
            "candidate_count": 6,
        },
    )
    _write_json(
        paths["signal_family_screening_2386"],
        {
            **_safe_doc(review.m2386.READY_STATUS),
            "signal_family_screening": [
                {
                    "signal_family": "growth_tilt",
                    "screening_decision": "REVIEW_SIGNAL_QUALITY",
                }
            ],
        },
    )
    _write_json(
        paths["cadence_bias_audit_2364"],
        {
            **_safe_doc(review.m2364.READY_STATUS),
            "primary_execution_cadence": "valid_until_window",
        },
    )
    _write_json(paths["validate_data_audit"], _validate_data_audit(paths["validate_data_report"]))
    paths["validate_data_report"].write_text(_validate_data_report_markdown(), encoding="utf-8")
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_plateau_decision_2401_path": paths["plateau_decision_2401"],
        "source_route_2401_path": paths["route_2401"],
        "source_owner_review_2400_path": paths["owner_review_2400"],
        "source_targeted_retest_2399_path": paths["targeted_retest_2399"],
        "source_gate_evidence_matrix_2399_path": paths["gate_evidence_matrix_2399"],
        "source_decision_update_2399_path": paths["decision_update_2399"],
        "source_expanded_retest_2386_path": paths["expanded_retest_2386"],
        "source_signal_family_screening_2386_path": paths[
            "signal_family_screening_2386"
        ],
        "source_cadence_bias_audit_2364_path": paths["cadence_bias_audit_2364"],
        "source_validate_data_audit_path": paths["validate_data_audit"],
        "source_validate_data_report_path": paths["validate_data_report"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "plateau_decision_2401": "--source-plateau-decision-2401",
        "route_2401": "--source-route-2401",
        "owner_review_2400": "--source-owner-review-2400",
        "targeted_retest_2399": "--source-targeted-retest-2399",
        "gate_evidence_matrix_2399": "--source-gate-evidence-matrix-2399",
        "decision_update_2399": "--source-decision-update-2399",
        "expanded_retest_2386": "--source-expanded-retest-2386",
        "signal_family_screening_2386": "--source-signal-family-screening-2386",
        "cadence_bias_audit_2364": "--source-cadence-bias-audit-2364",
        "validate_data_audit": "--source-validate-data-audit",
        "validate_data_report": "--source-validate-data-report",
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _safe_doc(status: str) -> dict[str, object]:
    return {
        "status": status,
        **{field: False for field in review.SAFETY_FALSE_FIELDS},
        "production_effect": "none",
        "broker_action": "none",
    }


def _validate_data_audit(report_path: Path) -> dict[str, object]:
    return {
        "raw_status": "PASS_WITH_WARNINGS",
        "status": "SUCCESS_WITH_WARNINGS",
        "error_count": 0,
        "warning_count": 2,
        "info_count": 12,
        "report_path": str(report_path),
        "quality_gate": {
            "data_quality_status": "PASS_WITH_WARNINGS",
            "error_count": 0,
            "warning_count": 2,
            "info_count": 12,
            "passed": True,
        },
        "file_summaries": {
            "price_data": {
                "rows": 56288,
                "min_date": "2018-01-02",
                "max_date": "2026-07-02",
            },
            "secondary_price_data": {
                "rows": 51769,
                "min_date": "2018-01-02",
                "max_date": "2026-07-02",
            },
            "macro_rate_data": {
                "rows": 6365,
                "min_date": "2018-01-02",
                "max_date": "2026-07-01",
            },
        },
    }


def _validate_data_report_markdown() -> str:
    return "\n".join(
        [
            "# Data quality 2026-07-05",
            "",
            "| 级别 | 来源 | Code | 行数 | 说明 | 样本 |",
            "|---|---|---|---|---|---|",
            (
                "| 警告 | 下载审计清单 | prices_download_manifest_checksum_missing |  | "
                "价格数据当前文件 sha256 未出现在下载审计清单中；请确认缓存是否由 "
                "download-data 生成。 | data/raw/prices_daily.csv |"
            ),
            (
                "| 警告 | 价格主源 | prices_adjustment_ratio_jump | 1 | "
                "价格数据的复权比例出现明显跳变 | {'date': '2025-11-20', "
                "'ticker': 'TQQQ'} |"
            ),
            "",
        ]
    )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
