from __future__ import annotations

import json
from datetime import UTC, datetime

from dynamic_v3_system_target_helpers import TARGET_AS_OF

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_daily_emission_preserves_unregistered_binding_as_zero_events(tmp_path) -> None:
    emission = system_target.run_smoothed_daily_emission(
        as_of=TARGET_AS_OF,
        output_dir=tmp_path / "smoothed_daily_emission",
        generated_at=datetime(2026, 1, 5, 3, tzinfo=UTC),
    )

    manifest = emission["manifest"]
    weights = emission["smoothed_event_weights"]
    validation = weights["weight_validation"]

    assert manifest["emitted_event_count"] == 0
    assert manifest["event_status"] == "NOT_REGISTERED"
    assert manifest["candidate_method"] is None
    assert manifest["binding_status"] == "NOT_REGISTERED"
    assert emission["smoothed_forward_events"] == []
    assert weights["event_weights"] == []
    assert emission["smoothed_emission_data_quality"]["future_data_used"] is False
    assert validation["all_weights_sum_to_one"] is True
    assert validation["no_negative_weights"] is True
    assert weights["broker_action_allowed"] is False
    assert manifest["production_effect"] == "none"
    assert "Dynamic Rescue Smoothed Daily Emission" in emission["reader_brief_section"]

    check = system_target.validate_smoothed_daily_emission_artifact(
        emission_id=emission["emission_id"],
        output_dir=tmp_path / "smoothed_daily_emission",
    )
    assert check["status"] == "PASS"

    manifest_path = emission["emission_dir"] / "smoothed_daily_emission_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["emitted_event_count"] = 1
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    tampered = system_target.validate_smoothed_daily_emission_artifact(
        emission_id=emission["emission_id"],
        output_dir=tmp_path / "smoothed_daily_emission",
    )
    assert tampered["status"] == "FAIL"
