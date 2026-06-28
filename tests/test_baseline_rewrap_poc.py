from __future__ import annotations

import csv
import json
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.baseline_frozen_composer_rewrap import (
    run_candidate_signal_binding_schema_poc,
)
from ai_trading_system.cli_commands.research_trends import trends_app


def _write_source_predictions(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "date": "2023-02-22",
            "model_id": "first_layer_composer_v2",
            "trend_state": "neutral",
            "confidence": "0.586266",
            "expected_horizon_days": "20",
            "validity_days": "10",
            "feature_snapshot_hash": "214844892182",
            "model_version": "first_layer_composer_v2",
            "known_at": "2023-02-22",
            "available_at": "2023-02-22",
            "decision_at": "2023-02-23",
            "do_not_de_risk_pred": "True",
            "stay_constructive_pred": "False",
            "add_risk_pred": "False",
            "high_confidence_risk_on_pred": "False",
        },
        {
            "date": "2023-02-23",
            "model_id": "first_layer_composer_v2",
            "trend_state": "risk_on",
            "confidence": "0.701",
            "expected_horizon_days": "20",
            "validity_days": "7",
            "feature_snapshot_hash": "81270680688",
            "model_version": "first_layer_composer_v2",
            "known_at": "2023-02-23",
            "available_at": "2023-02-23",
            "decision_at": "2023-02-24",
            "do_not_de_risk_pred": "False",
            "stay_constructive_pred": "True",
            "add_risk_pred": "True",
            "high_confidence_risk_on_pred": "True",
        },
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_baseline_rewrap_poc_generates_candidate_bound_artifacts(tmp_path: Path) -> None:
    source = tmp_path / "first_layer_composer_v2_predictions.csv"
    _write_source_predictions(source)

    payload = run_candidate_signal_binding_schema_poc(
        source_predictions=source,
        output_dir=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
    )

    assert payload["status"] == "CANDIDATE_SIGNAL_BINDING_SCHEMA_POC_READY_PROMOTION_BLOCKED"
    assert payload["summary"]["source_row_count"] == 2
    assert payload["summary"]["rewrapped_signal_record_count"] == 2
    assert payload["summary"]["validation_status"] == "PASS"
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"
    assert payload["permanently_inconclusive_override_allowed"] is False
    assert payload["trading_2281_permanently_inconclusive_unchanged"] is True

    for path in payload["artifact_paths"].values():
        assert Path(path).exists()


def test_baseline_rewrap_poc_signal_series_contains_binding_and_provenance(
    tmp_path: Path,
) -> None:
    source = tmp_path / "first_layer_composer_v2_predictions.csv"
    _write_source_predictions(source)
    payload = run_candidate_signal_binding_schema_poc(
        source_predictions=source,
        output_dir=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
    )

    csv_path = Path(payload["artifact_paths"]["baseline_rewrapped_candidate_signal_series_csv"])
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert rows[0]["candidate_id"] == "baseline"
    assert rows[0]["source_artifact_hash"] == payload["source_artifact_hash"]
    assert rows[0]["as_of_timestamp"] == "2023-02-22T00:00:00+00:00"
    assert rows[0]["decision_timestamp"] == "2023-02-23T00:00:00+00:00"
    assert rows[0]["horizon"] == "20d"
    assert rows[0]["signal_spec_version"] == "first_layer_candidate_signal_spec.v1"
    assert rows[0]["prediction_schema_version"] == "candidate_bound_prediction_artifact.v1"
    provenance = json.loads(rows[0]["provenance"])
    assert provenance["regeneration_mode"] == "schema_migration_poc"
    assert provenance["pit_policy"] == "non_pit_source_evidence_only"
    assert provenance["promotion_eligible"] is False


def test_baseline_rewrap_prediction_artifact_stays_schema_poc_only(tmp_path: Path) -> None:
    source = tmp_path / "first_layer_composer_v2_predictions.csv"
    _write_source_predictions(source)
    payload = run_candidate_signal_binding_schema_poc(
        source_predictions=source,
        output_dir=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
    )

    artifact_path = Path(
        payload["artifact_paths"]["baseline_rewrapped_candidate_prediction_artifact_json"]
    )
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert artifact["candidate_id"] == "baseline"
    assert artifact["artifact_role"] == "schema_migration_poc"
    assert artifact["historical_executable_artifact"] is False
    assert artifact["actual_path_validation_ready"] is False
    assert artifact["promotion_eligible"] is False
    assert artifact["promotion_allowed"] is False
    assert artifact["paper_shadow_allowed"] is False
    assert artifact["production_allowed"] is False
    assert artifact["broker_action"] == "none"
    assert artifact["prediction_records"][0]["candidate_id"] == "baseline"


def test_candidate_signal_binding_schema_poc_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "candidate-signal-binding-schema-poc" in result.output
