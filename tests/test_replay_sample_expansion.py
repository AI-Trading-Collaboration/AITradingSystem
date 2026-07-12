from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_paper_tracking_helpers import (
    read_json,
    write_market_cache,
    write_validated_owner_review,
)

from ai_trading_system.etf_portfolio import dynamic_v3_outcome_accumulation as accumulation


def test_replay_sample_expansion_freezes_validated_sources_and_recomputes_views(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(accumulation, "DEFAULT_LATEST_POINTER_DIR", tmp_path / "latest")
    source = write_validated_owner_review(
        tmp_path,
        owner_decision="monitor",
        as_of=date(2026, 6, 8),
        generated_at=datetime(2026, 6, 8, 10, tzinfo=UTC),
    )
    prices_path, rates_path = write_market_cache(
        tmp_path / "market_cache",
        start="2026-06-05",
        end="2026-07-10",
    )
    output_dir = tmp_path / "replay_sample_expansion"
    result = accumulation.run_replay_sample_expansion(
        start=date(2026, 6, 1),
        end=date(2026, 7, 10),
        output_dir=output_dir,
        daily_advisory_dir=source["daily_advisory_dir"],
        owner_review_dir=source["owner_review_dir"],
        replay_inventory_dir=tmp_path / "replay_inventory",
        prices_path=prices_path,
        rates_path=rates_path,
        generated_at=datetime(2026, 7, 10, 23, tzinfo=UTC),
    )

    summary = result["pit_classification_summary"]
    assert summary["total_expanded_events"] == 1
    assert summary["pit_unsafe_count"] == 1
    assert summary["ineligible_count"] == 1
    event = result["expanded_replay_events"][0]
    assert event["sample_key"] == f"{source['daily_advisory_id']}|2026-06-08"
    assert event["source_types"] == ["daily_advisory"]
    assert event["owner_decision_available"] is True
    assert event["limitations"] == ["MISSING_CURRENT_WEIGHTS"]
    assert event["replay_eligibility"] == "INELIGIBLE"
    assert result["manifest"]["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    validation = accumulation.validate_replay_sample_expansion_artifact(
        expansion_id=result["expansion_id"],
        output_dir=output_dir,
    )
    assert validation["status"] == "PASS"

    expansion_dir = Path(result["expansion_dir"])
    events_path = expansion_dir / "expanded_replay_events.jsonl"
    original_events = events_path.read_text(encoding="utf-8")
    tampered_event = dict(event)
    tampered_event["replay_eligibility"] = "PARTIAL"
    events_path.write_text(json.dumps(tampered_event, sort_keys=True) + "\n", encoding="utf-8")
    assert (
        accumulation.validate_replay_sample_expansion_artifact(
            expansion_id=result["expansion_id"], output_dir=output_dir
        )["status"]
        == "FAIL"
    )
    events_path.write_text(original_events, encoding="utf-8")

    daily_manifest_path = (
        Path(source["daily_advisory_dir"])
        / source["daily_advisory_id"]
        / "daily_advisory_manifest.json"
    )
    original_manifest = daily_manifest_path.read_text(encoding="utf-8")
    tampered_manifest = read_json(daily_manifest_path)
    tampered_manifest["recommended_action"] = "tampered"
    daily_manifest_path.write_text(
        json.dumps(tampered_manifest, sort_keys=True), encoding="utf-8"
    )
    assert (
        accumulation.validate_replay_sample_expansion_artifact(
            expansion_id=result["expansion_id"], output_dir=output_dir
        )["status"]
        == "FAIL"
    )
    daily_manifest_path.write_text(original_manifest, encoding="utf-8")
    assert (
        accumulation.validate_replay_sample_expansion_artifact(
            expansion_id=result["expansion_id"], output_dir=output_dir
        )["status"]
        == "PASS"
    )


def test_replay_sample_expansion_fails_before_output_on_invalid_range_or_data(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(accumulation, "DEFAULT_LATEST_POINTER_DIR", tmp_path / "latest")
    output_dir = tmp_path / "replay_sample_expansion"
    with pytest.raises(accumulation.DynamicV3OutcomeAccumulationError):
        accumulation.run_replay_sample_expansion(
            start=date(2026, 7, 11),
            end=date(2026, 7, 10),
            output_dir=output_dir,
            generated_at=datetime(2026, 7, 10, 23, tzinfo=UTC),
        )
    assert not output_dir.exists()

    prices_path = tmp_path / "bad_prices.csv"
    rates_path = tmp_path / "bad_rates.csv"
    prices_path.write_text("date,ticker,close\n", encoding="utf-8")
    rates_path.write_text("date,series,value\n", encoding="utf-8")
    with pytest.raises(accumulation.DynamicV3OutcomeAccumulationError):
        accumulation.run_replay_sample_expansion(
            start=date(2026, 6, 1),
            end=date(2026, 7, 10),
            output_dir=output_dir,
            daily_advisory_dir=tmp_path / "daily",
            owner_review_dir=tmp_path / "owner",
            replay_inventory_dir=tmp_path / "replay",
            prices_path=prices_path,
            rates_path=rates_path,
            generated_at=datetime(2026, 7, 10, 23, tzinfo=UTC),
        )
    assert not output_dir.exists()
