from __future__ import annotations

from pathlib import Path

from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_report_registry_marks_candidate_signal_binding_poc_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    reports = registry["reports"]
    entry = next(
        report
        for report in reports
        if report["report_id"] == "candidate_signal_binding_schema_poc"
    )

    assert entry["command"] == "aits research trends candidate-signal-binding-schema-poc"
    assert entry["artifact_role"] == "schema_migration_poc"
    assert entry["promotion_eligible"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "baseline_rewrapped_candidate_prediction_artifact.json" in glob
        for glob in entry["artifact_globs"]
    )
