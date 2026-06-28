from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.candidate_signal_prediction_artifact_audit import (
    run_candidate_signal_prediction_artifact_audit_pack,
)
from ai_trading_system.cli_commands.research_trends import trends_app


def test_candidate_artifact_audit_marks_inconclusive_rows_permanent(
    tmp_path: Path,
) -> None:
    payload = run_candidate_signal_prediction_artifact_audit_pack(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
    )

    assert payload["status"] == "CANDIDATE_SIGNAL_PREDICTION_ARTIFACT_AUDIT_READY_PROMOTION_BLOCKED"
    assert payload["summary"]["inconclusive_candidate_count"] == 4
    assert payload["summary"]["artifact_row_count"] == 28
    assert payload["summary"]["backfill_possible_candidate_count"] == 0
    assert payload["summary"]["backfilled_artifact_count"] == 0
    assert payload["summary"]["permanently_inconclusive_count"] == 4
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"

    rows = {row["candidate_id"]: row for row in payload["candidate_rows"]}
    assert set(rows) == {
        "baseline",
        "baseline_plus_trend_structure",
        "risk_appetite",
        "volatility_regime",
    }
    for row in rows.values():
        assert row["backfill_possible"] is False
        assert row["permanently_inconclusive"] is True
        assert row["promotion_allowed"] is False
        assert row["paper_shadow_allowed"] is False


def test_candidate_artifact_audit_baseline_source_is_schema_incompatible(
    tmp_path: Path,
) -> None:
    payload = run_candidate_signal_prediction_artifact_audit_pack(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
    )

    baseline_rows = [row for row in payload["artifact_rows"] if row["candidate_id"] == "baseline"]
    by_type = {row["artifact_type"]: row for row in baseline_rows}

    assert by_type["experiment_definition"]["artifact_status"] == "present_registered"
    assert by_type["candidate_signal_spec"]["gap_category"] == "never_generated"
    assert by_type["candidate_signal_series"]["gap_category"] == "schema_incompatible"
    assert by_type["candidate_prediction_artifact"]["gap_category"] == "schema_incompatible"
    assert by_type["registry_reference"]["gap_category"] == "registry_missing_reference"
    assert (
        "first_layer_composer_v2_predictions.csv"
        in (by_type["candidate_prediction_artifact"]["evidence_paths"][0])
    )
    assert by_type["registry_reference"]["evidence_paths"][0].endswith(
        "config\\report_registry.yaml"
    )


def test_candidate_artifact_audit_outputs_and_cli_registration(tmp_path: Path) -> None:
    payload = run_candidate_signal_prediction_artifact_audit_pack(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
    )

    for key in (
        "candidate_signal_prediction_artifact_gap_report",
        "candidate_artifact_provenance_matrix_md",
        "inconclusive_candidate_recovery_plan_md",
        "candidate_artifact_provenance_matrix_json",
        "candidate_artifact_gap_matrix_json",
        "inconclusive_candidate_recovery_plan_json",
    ):
        assert Path(payload["artifact_paths"][key]).exists()

    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "candidate-signal-prediction-artifact-audit" in result.output
