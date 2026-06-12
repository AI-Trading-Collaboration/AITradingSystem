from __future__ import annotations

import json
from datetime import UTC, datetime

from dynamic_v3_system_target_helpers import run_selection_review_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_data_warning_impact_keeps_unknown_warning_effect_review_required(tmp_path) -> None:
    fixture = run_selection_review_fixture(tmp_path)
    backfill_id = fixture["backfill"]["backfill_id"]
    selection_id = fixture["selection"]["selection_review_id"]
    backfill_root = tmp_path / "paper_shadow_backfill" / backfill_id
    quality_path = backfill_root / "backfill_data_quality.json"
    manifest_path = backfill_root / "paper_shadow_backfill_manifest.json"

    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    quality["data_quality"] = "PASS_WITH_WARNINGS"
    quality["price_source_status"] = "PASS_WITH_WARNINGS"
    quality.pop("warnings", None)
    quality_path.write_text(json.dumps(quality, sort_keys=True) + "\n", encoding="utf-8")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["data_quality_status"] = "PASS_WITH_WARNINGS"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8")

    impact = system_target.run_data_warning_impact_review(
        backfill_id=backfill_id,
        selection_review_id=selection_id,
        backfill_dir=tmp_path / "paper_shadow_backfill",
        selection_review_dir=tmp_path / "system_target_selection_review",
        output_dir=tmp_path / "data_warning_impact",
        generated_at=datetime(2024, 3, 1, 8, tzinfo=UTC),
    )

    inventory = impact["data_warning_inventory"]
    sensitivity = impact["recommendation_sensitivity_to_warnings"]
    affected = impact["affected_metrics"]["metrics"]

    assert inventory["data_quality"] == "PASS_WITH_WARNINGS"
    assert inventory["warnings"][0]["warning_id"] == "pass_with_warnings_detail_unavailable"
    assert inventory["warnings"][0]["potential_metric_impact"] == "UNKNOWN"
    assert {row["impact_level"] for row in affected} == {"UNKNOWN"}
    assert sensitivity["recommendation_stability"] == "REVIEW_REQUIRED"
    assert sensitivity["data_quality_decision"] == "REVIEW_REQUIRED"
    assert sensitivity["would_change_if_warnings_excluded"] is None
    assert sensitivity["broker_action_allowed"] is False

    validation = system_target.validate_data_warning_impact_artifact(
        impact_id=impact["impact_id"],
        output_dir=tmp_path / "data_warning_impact",
    )
    assert validation["status"] == "PASS"
