from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_filtered_candidate_readiness_helpers import assert_research_safe
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio import (
    dynamic_v3_benchmark_baseline_control as baseline,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_candidate_regression_replay as replay,
)
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.reports import reader_brief


def test_candidate_regression_replay_passes_expected_behavior(tmp_path: Path) -> None:
    current_path = _write_current_behavior(tmp_path)

    result = replay.run_candidate_regression_replay(
        as_of=date(2026, 6, 16),
        current_behavior_path=current_path,
        output_dir=tmp_path / "candidate_regression_replay",
        generated_at=datetime(2026, 6, 16, 3, tzinfo=UTC),
    )
    report = result["candidate_regression_replay_report"]
    summary = report["comparison_summary"]

    assert report["candidate_regression_replay_status"] == "REGRESSION_REPLAY_PASS"
    assert summary["breaking_change_count"] == 0
    assert summary["acceptable_change_count"] == 0
    assert summary["comparison_count"] > 0
    assert report["changed_outputs"] == []
    assert result["candidate_regression_replay_validation"]["status"] == "PASS"
    assert "candidate_regression_replay_status" in result["reader_brief_section"]
    assert_research_safe(report)
    assert report["regression_guard_only"] is True
    assert report["strategy_behavior_changed"] is False
    assert report["data_downloaded_by_replay"] is False
    assert report["broker_action_allowed"] is False


def test_candidate_regression_replay_breaking_change_fails_closed(
    tmp_path: Path,
) -> None:
    current_path = _write_current_behavior(
        tmp_path,
        overrides={
            "benchmark_baseline_status": "CANDIDATE_OUTPERFORMS_BASELINES",
            "comparison_summary": {
                "baseline_count": 5,
                "outperformed_baseline_count": 5,
                "underperformed_baseline_count": 0,
                "insufficient_metric_baseline_count": 0,
                "worst_baseline_delta": 0.01,
                "best_baseline_delta": 0.02,
            },
        },
    )

    result = replay.run_candidate_regression_replay(
        as_of=date(2026, 6, 16),
        current_behavior_path=current_path,
        output_dir=tmp_path / "candidate_regression_replay",
        generated_at=datetime(2026, 6, 16, 3, tzinfo=UTC),
    )
    report = result["candidate_regression_replay_report"]

    assert report["candidate_regression_replay_status"] == "BREAKING_CHANGE_DETECTED"
    assert report["comparison_summary"]["breaking_change_count"] >= 1
    assert "breaking_changes:" in ",".join(report["blocking_reasons"])
    assert any(
        row["field_path"] == "benchmark_baseline_status"
        for row in report["changed_outputs"]
    )
    assert result["candidate_regression_replay_validation"]["status"] == "PASS"


def test_candidate_regression_replay_cli_run_report_and_validate(
    tmp_path: Path,
) -> None:
    current_path = _write_current_behavior(tmp_path)
    output_dir = tmp_path / "candidate_regression_replay_cli"

    run = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "candidate-regression-replay",
            "run",
            "--as-of",
            "2026-06-16",
            "--current-behavior-path",
            str(current_path),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert run.exit_code == 0
    assert "candidate_regression_replay_status=REGRESSION_REPLAY_PASS" in run.output
    assert "breaking_change_count=0" in run.output
    assert "validation_status=PASS" in run.output
    replay_id = next(
        line.split("=", 1)[1]
        for line in run.output.splitlines()
        if line.startswith("replay_id=")
    )

    report = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "candidate-regression-replay",
            "report",
            "--replay-id",
            replay_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert report.exit_code == 0
    assert "candidate_regression_replay_status=REGRESSION_REPLAY_PASS" in report.output

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "validate-candidate-regression-replay",
            "--replay-id",
            replay_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output


def test_reader_brief_summarizes_candidate_regression_replay(tmp_path: Path) -> None:
    current_path = _write_current_behavior(tmp_path)
    result = replay.run_candidate_regression_replay(
        as_of=date(2026, 6, 16),
        current_behavior_path=current_path,
        output_dir=tmp_path / "candidate_regression_replay",
        generated_at=datetime(2026, 6, 16, 3, tzinfo=UTC),
    )
    leaderboard_path = _write_minimal_leaderboard(tmp_path)
    manifest_path = result["replay_dir"] / "candidate_regression_replay_manifest.json"
    report_index = {
        "reports": [
            {
                "report_id": "etf_dynamic_v3_parameter_sweep_leaderboard",
                "latest_artifact_path": str(leaderboard_path),
            },
            {
                "report_id": "etf_dynamic_v3_candidate_regression_replay",
                "latest_artifact_path": str(manifest_path),
            },
        ]
    }

    summary = reader_brief._etf_dynamic_v3_parameter_research_summary(report_index)

    assert summary["candidate_regression_replay_id"] == result["replay_id"]
    assert summary["candidate_regression_replay_status"] == "REGRESSION_REPLAY_PASS"
    assert summary["candidate_regression_breaking_change_count"] == 0
    assert summary["candidate_regression_validation_status"] == "PASS"
    assert summary["candidate_regression_replay"] == str(manifest_path)


def _write_current_behavior(
    tmp_path: Path,
    overrides: Mapping[str, Any] | None = None,
) -> Path:
    path = tmp_path / "benchmark_baseline_control" / "benchmark_baseline_control_pack.json"
    payload = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_benchmark_baseline_control_pack",
        "control_id": "benchmark-baseline-control-fixture",
        "candidate": "median_plus_regime_mismatch_filter",
        "as_of": "2026-06-16",
        "benchmark_baseline_status": "INSUFFICIENT_BASELINE_METRICS",
        "baseline_count": 5,
        "comparison_summary": {
            "baseline_count": 5,
            "outperformed_baseline_count": 0,
            "underperformed_baseline_count": 0,
            "insufficient_metric_baseline_count": 5,
            "worst_baseline_delta": None,
            "best_baseline_delta": None,
        },
        "source_artifacts": {},
        "next_required_action": (
            "provide_candidate_and_baseline_metrics_before_baseline_control_review"
        ),
        **baseline.BENCHMARK_BASELINE_SAFETY,
    }
    payload.update(dict(overrides or {}))
    _write_json(path, payload)
    (path.parent / "reader_brief_section.md").write_text(
        "\n".join(
            [
                "- benchmark_baseline_status: INSUFFICIENT_BASELINE_METRICS",
                "- baseline_count: 5",
                "- outperformed_baseline_count: 0",
                "- underperformed_baseline_count: 0",
                "- worst_baseline_delta: None",
                "- best_baseline_delta: None",
                "- next_required_action: "
                "provide_candidate_and_baseline_metrics_before_baseline_control_review",
                "- safety_boundary: research-only benchmark controls",
                "",
            ]
        ),
        encoding="utf-8",
    )
    _write_json(path.parent / "benchmark_baseline_validation.json", {"status": "PASS"})
    return path


def _write_minimal_leaderboard(tmp_path: Path) -> Path:
    path = tmp_path / "leaderboard" / "leaderboard.json"
    safety = {
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_state_mutated": False,
        "baseline_config_mutated": False,
        "official_target_weights_mutated": False,
        "automatic_candidate_promotion": False,
        "auto_enrollment_without_owner_approval": False,
        "shadow_enrollment_allowed": False,
        "automatic_enrollment_allowed": False,
        "owner_approval_executed": False,
        "production_candidate_generated": False,
    }
    _write_json(
        path,
        {
            "schema_version": st.SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_parameter_sweep_leaderboard",
            "status": "PASS",
            "evaluator_mode": "tiny_fixture_proxy",
            "metrics_source": "fixture",
            "not_for_investment_decision": True,
            "summary_sentence": "fixture leaderboard",
            "candidate_count": 1,
            "top_eligible_candidates": [
                {
                    "candidate": "median_plus_regime_mismatch_filter",
                    "gate": "observe_only",
                    "score": 0.1,
                }
            ],
            "most_common_reject_reasons": [],
            "safety": safety,
            "production_candidate_generated": False,
        },
    )
    return path


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
