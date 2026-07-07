from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_pit_coverage_signal_construction_review as review
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_dynamic_strategy_pit_coverage_signal_construction_review_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "pit_signal"
    docs_root = tmp_path / "docs" / "research"

    payload = review.run_dynamic_strategy_pit_coverage_signal_construction_review(
        **_source_kwargs(source_paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 7),
        validate_data_as_of=date(2026, 7, 5),
    )

    assert payload["status"] == review.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_validation_errors"] == []
    assert payload["validate_data_status"] == "PASS_WITH_WARNINGS"
    assert payload["validate_data_error_count"] == 0
    assert payload["validate_data_warning_count"] == 2
    assert payload["pit_coverage_matrix_ready"] is True
    assert payload["signal_construction_review_ready"] is True
    assert payload["valid_until_stale_signal_review_ready"] is True
    assert payload["regime_labeling_review_ready"] is True
    assert payload["threshold_meta_dataset_gap_ready"] is True
    assert payload["prioritized_remediation_matrix_ready"] is True
    assert payload["candidate_search_resumed"] is False
    assert payload["candidate_retest_resume_recommended"] is False
    assert payload["recommended_next_research_task"] == review.NEXT_ROUTE
    assert len(payload["pit_coverage_matrix"]) == 14
    assert {
        row["remediation_id"] for row in payload["prioritized_remediation_matrix"]
    } == {
        "2403-PIT-01",
        "2403-SIGNAL-01",
        "2403-VALIDUNTIL-01",
        "2403-REGIME-01",
        "2403-THRESHOLD-01",
        "2403-DATA-01",
        "2403-DATA-02",
        "2403-REPORTING-01",
    }
    pit_by_id = {row["input_id"]: row for row in payload["pit_coverage_matrix"]}
    assert pit_by_id["growth_tilt_engine"]["severity"] == "BLOCKING"
    assert pit_by_id["valid_until_window"]["severity"] == "BLOCKING"

    for field in review.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    for key in (
        "json_path",
        "pit_coverage_matrix_json",
        "signal_construction_review_json",
        "regime_labeling_review_json",
        "remediation_matrix_json",
        "threshold_meta_dataset_gap_json",
        "markdown_path",
        "pit_coverage_matrix_markdown",
        "signal_construction_review_markdown",
        "regime_labeling_review_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_pit_coverage_signal_construction_review_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "pit_signal_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-pit-coverage-signal-construction-review",
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
    assert (output_root / "pit_signal_review_result.json").exists()
    assert (output_root / "pit_coverage_matrix.json").exists()
    assert (output_root / "signal_construction_review.json").exists()
    assert (output_root / "regime_labeling_review.json").exists()
    assert (output_root / "remediation_matrix.json").exists()
    assert (output_root / "threshold_meta_dataset_gap.json").exists()


def test_dynamic_strategy_pit_coverage_signal_construction_registry_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_pit_coverage_signal_construction_review"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-pit-coverage-signal-construction-review"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("pit_signal_review_result.json" in item for item in entry["artifact_globs"])
    assert any("pit_coverage_matrix.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2404_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "dynamic_strategy_pit_coverage_signal_construction_review" in catalog
    assert "dynamic-strategy-pit-coverage-signal-construction-review" in system_flow
    assert review.TASK_REGISTER_ID in task_register


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    paths = {
        "gap_review_2402": root / "gap_review_2402.json",
        "pit_gap_review_2402": root / "pit_gap_review_2402.json",
        "signal_gap_review_2402": root / "signal_gap_review_2402.json",
        "regime_gap_review_2402": root / "regime_gap_review_2402.json",
        "threshold_gap_review_2402": root / "threshold_gap_review_2402.json",
        "plateau_decision_2401": root / "plateau_decision_2401.json",
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
        paths["gap_review_2402"],
        {
            **_safe_doc(review.m2402.READY_STATUS),
            "recommended_next_research_task": review.m2402.NEXT_ROUTE,
            "resume_candidate_search_recommended": False,
            "pit_coverage_matrix_recommended": True,
            "signal_construction_review_recommended": True,
            "regime_expectation_scoring_review_recommended": True,
            "threshold_meta_dataset_recommended": True,
            "data_quality_gap_review": {
                "warnings_relevant_to_dynamic_strategy": [
                    {"code": "prices_download_manifest_checksum_missing"},
                    {"code": "prices_adjustment_ratio_jump"},
                ]
            },
        },
    )
    for key, payload_key in (
        ("pit_gap_review_2402", "pit_coverage_gap_review"),
        ("signal_gap_review_2402", "signal_quality_gap_review"),
        ("regime_gap_review_2402", "regime_labeling_gap_review"),
        ("threshold_gap_review_2402", "threshold_meta_dataset_gap_review"),
    ):
        _write_json(
            paths[key],
            {
                **_safe_doc(review.m2402.READY_STATUS),
                payload_key: {"record_ready": True},
            },
        )
    _write_json(
        paths["plateau_decision_2401"],
        {
            **_safe_doc(review.m2401.READY_STATUS),
            "owner_decision": review.m2401.OWNER_DECISION,
            "recombination_line_plateau_detected": True,
        },
    )
    _write_json(
        paths["targeted_retest_2399"],
        {
            **_safe_doc(review.m2399.READY_STATUS),
            "best_targeted_variant": review.m2401.BEST_TARGETED_VARIANT,
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
        {**_safe_doc(review.m2386.READY_STATUS), "candidate_count": 6},
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
        "source_gap_review_2402_path": paths["gap_review_2402"],
        "source_pit_gap_review_2402_path": paths["pit_gap_review_2402"],
        "source_signal_gap_review_2402_path": paths["signal_gap_review_2402"],
        "source_regime_gap_review_2402_path": paths["regime_gap_review_2402"],
        "source_threshold_gap_review_2402_path": paths["threshold_gap_review_2402"],
        "source_plateau_decision_2401_path": paths["plateau_decision_2401"],
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
        "gap_review_2402": "--source-gap-review-2402",
        "pit_gap_review_2402": "--source-pit-gap-review-2402",
        "signal_gap_review_2402": "--source-signal-gap-review-2402",
        "regime_gap_review_2402": "--source-regime-gap-review-2402",
        "threshold_gap_review_2402": "--source-threshold-gap-review-2402",
        "plateau_decision_2401": "--source-plateau-decision-2401",
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
