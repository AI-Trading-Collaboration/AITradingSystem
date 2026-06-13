from __future__ import annotations

from datetime import UTC, datetime

from dynamic_v3_system_target_helpers import (
    EVALUATION_AS_OF,
    TARGET_AS_OF,
    build_model_target_fixture,
    write_market_cache,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def _update_fixture(tmp_path):
    target = build_model_target_fixture(tmp_path)
    prices_path, _ = write_market_cache(tmp_path / "market_cache")
    system_target.run_smoothed_daily_emission(
        as_of=TARGET_AS_OF,
        target_id=target["target_id"],
        model_target_dir=tmp_path / "model_target",
        output_dir=tmp_path / "smoothed_daily_emission",
        price_cache_path=prices_path,
        generated_at=datetime(2026, 1, 5, 3, tzinfo=UTC),
    )
    due = system_target.scan_smoothed_outcome_due(
        as_of=EVALUATION_AS_OF,
        emission_dir=tmp_path / "smoothed_daily_emission",
        output_dir=tmp_path / "smoothed_outcome_due",
        price_cache_path=prices_path,
        generated_at=datetime(2026, 1, 8, tzinfo=UTC),
    )
    update = system_target.run_smoothed_outcome_update(
        due_id=due["due_id"],
        due_dir=tmp_path / "smoothed_outcome_due",
        emission_dir=tmp_path / "smoothed_daily_emission",
        output_dir=tmp_path / "smoothed_outcome_update",
        price_cache_path=prices_path,
        generated_at=datetime(2026, 1, 8, 1, tzinfo=UTC),
    )
    return update


def test_smoothed_forward_classify_marks_sideways_proxy(tmp_path) -> None:
    update = _update_fixture(tmp_path)

    classification = system_target.run_smoothed_forward_classification(
        update_id=update["update_id"],
        update_dir=tmp_path / "smoothed_outcome_update",
        emission_dir=tmp_path / "smoothed_daily_emission",
        output_dir=tmp_path / "smoothed_forward_classification",
        generated_at=datetime(2026, 1, 8, 2, tzinfo=UTC),
    )

    summary = classification["classification_summary"]
    rows = classification["classified_forward_events"]

    assert summary["events_classified"] == 1
    assert summary["sideways_events_available"] == 1
    assert summary["recovery_events_available"] == 0
    assert summary["lag_warning_count"] == 0
    assert rows[0]["regime_classification"] == ["sideways_choppy"]
    assert rows[0]["classification_confidence"] in {"LOW", "HIGH"}
    assert rows[0]["sideways_relevant"] is True
    assert rows[0]["event_status"] == "AVAILABLE"
    assert classification["manifest"]["broker_action_allowed"] is False

    check = system_target.validate_smoothed_forward_classification_artifact(
        classification_id=classification["classification_id"],
        output_dir=tmp_path / "smoothed_forward_classification",
    )
    assert check["status"] == "PASS"
