from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_historical_replay_helpers import (
    build_replay_inventory,
    prepare_replay_test_environment,
    write_owner_reviews,
    write_replay_daily_advisory,
)

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    REPLAY_VARIANTS,
    run_historical_paper_sim,
    run_historical_replay,
    validate_historical_paper_sim_artifact,
    validate_historical_replay_artifact,
    validate_replay_inventory_artifact,
)


def test_dynamic_v3_historical_replay_skips_unsafe_and_requires_warning_opt_in(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    target = {"QQQ": 0.45, "SMH": 0.30, "SOXX": 0.10, "CASH": 0.15}
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="safe",
        as_of="2026-06-03",
        target_weights=target,
    )
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="warning",
        as_of="2026-06-04",
        target_weights=target,
    )
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="unsafe",
        as_of="2026-06-05",
        target_weights=None,
    )
    write_owner_reviews(paths["owner_review_dir"], ["safe", "unsafe"])
    inventory = build_replay_inventory(paths, start=date(2026, 6, 1), end=date(2026, 6, 10))

    default_replay = run_historical_replay(
        inventory_id=inventory["inventory_id"],
        include_pit_warning=False,
        inventory_dir=paths["inventory_dir"],
        output_dir=paths["historical_replay_dir"],
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )
    assert default_replay["manifest"]["replay_event_count"] == 1
    assert default_replay["manifest"]["skipped_count"] == 2
    event = default_replay["events"][0]
    assert event["daily_advisory_id"] == "safe"
    assert {row["variant"] for row in event["variants"]} == set(REPLAY_VARIANTS)
    assert all(row["broker_action_taken"] is False for row in event["variants"])
    assert event["outcome_mode"] == "HISTORICAL_REPLAY"
    assert event["broker_action_taken"] is False
    limited = next(row for row in event["variants"] if row["variant"] == "limited_adjustment")
    assert limited["turnover"] > 0

    validation = validate_historical_replay_artifact(
        replay_id=default_replay["replay_id"],
        output_dir=paths["historical_replay_dir"],
    )
    assert validation["status"] == "PASS"

    warning_replay = run_historical_replay(
        inventory_id=inventory["inventory_id"],
        include_pit_warning=True,
        inventory_dir=paths["inventory_dir"],
        output_dir=paths["historical_replay_dir"],
        generated_at=datetime(2026, 6, 11, tzinfo=UTC),
    )
    assert warning_replay["manifest"]["replay_event_count"] == 2
    assert {row["daily_advisory_id"] for row in warning_replay["events"]} == {"safe", "warning"}
    assert all(row["pit_safety_status"] != "PIT_UNSAFE" for row in warning_replay["events"])


def test_dynamic_v3_historical_replay_marks_hard_pit_limitations_unsafe(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    target = {"QQQ": 0.45, "SMH": 0.30, "SOXX": 0.10, "CASH": 0.15}
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="safe",
        as_of="2026-06-03",
        target_weights=target,
    )
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="generated-after-as-of",
        as_of="2026-06-04",
        generated_at="2026-06-05T12:00:00+00:00",
        target_weights=target,
    )
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="missing-future-price",
        as_of="2026-08-01",
        target_weights=target,
    )
    write_owner_reviews(
        paths["owner_review_dir"],
        ["safe", "generated-after-as-of", "missing-future-price"],
    )
    inventory = build_replay_inventory(
        paths,
        start=date(2026, 6, 1),
        end=date(2026, 8, 10),
    )
    rows = {row["daily_advisory_id"]: row for row in inventory["rows"]}

    assert rows["safe"]["pit_safety_status"] == "PIT_SAFE"
    assert rows["generated-after-as-of"]["pit_safety_status"] == "PIT_UNSAFE"
    assert rows["generated-after-as-of"]["replay_eligibility"] == "INELIGIBLE"
    assert (
        "ADVISORY_GENERATED_AFTER_AS_OF_DATE" in rows["generated-after-as-of"]["replay_limitations"]
    )
    assert rows["missing-future-price"]["pit_safety_status"] == "PIT_UNSAFE"
    assert rows["missing-future-price"]["replay_eligibility"] == "INELIGIBLE"
    assert "MISSING_PRICE_DATA" in rows["missing-future-price"]["replay_limitations"]
    assert inventory["pit_safety_audit"]["hard_pit_limitation_count"] == 2
    assert (
        validate_replay_inventory_artifact(
            inventory_id=inventory["inventory_id"],
            output_dir=paths["inventory_dir"],
        )["status"]
        == "PASS"
    )

    replay = run_historical_replay(
        inventory_id=inventory["inventory_id"],
        include_pit_warning=True,
        inventory_dir=paths["inventory_dir"],
        output_dir=paths["historical_replay_dir"],
        generated_at=datetime(2026, 8, 10, tzinfo=UTC),
    )
    assert replay["manifest"]["replay_event_count"] == 1
    assert replay["manifest"]["skipped_count"] == 2
    assert [event["daily_advisory_id"] for event in replay["events"]] == ["safe"]
    assert {row["skip_reason"] for row in replay["action_summary"]["skipped_events"]} == {
        "PIT_UNSAFE_EXCLUDED"
    }

    unsafe_only_inventory = build_replay_inventory(
        paths,
        start=date(2026, 6, 4),
        end=date(2026, 8, 10),
    )
    unsafe_only_replay = run_historical_replay(
        inventory_id=unsafe_only_inventory["inventory_id"],
        include_pit_warning=False,
        inventory_dir=paths["inventory_dir"],
        output_dir=paths["historical_replay_dir"],
        generated_at=datetime(2026, 8, 11, tzinfo=UTC),
    )
    sim = run_historical_paper_sim(
        replay_id=unsafe_only_replay["replay_id"],
        replay_dir=paths["historical_replay_dir"],
        output_dir=paths["paper_sim_dir"],
        prices_path=paths["prices_path"],
        generated_at=datetime(2026, 8, 11, tzinfo=UTC),
    )
    assert sim["manifest"]["status"] == "INSUFFICIENT_DATA"
    assert sim["state_history"] == []
    assert (
        validate_historical_paper_sim_artifact(
            sim_id=sim["sim_id"],
            output_dir=paths["paper_sim_dir"],
        )["status"]
        == "PASS"
    )

    corrupted_rows = []
    for row in inventory["rows"]:
        if row["daily_advisory_id"] == "generated-after-as-of":
            row = {**row, "pit_safety_status": "PIT_WARNING", "replay_eligibility": "PARTIAL"}
        corrupted_rows.append(row)
    (inventory["inventory_dir"] / "replay_artifact_inventory.jsonl").write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in corrupted_rows),
        encoding="utf-8",
    )
    failed_validation = validate_replay_inventory_artifact(
        inventory_id=inventory["inventory_id"],
        output_dir=paths["inventory_dir"],
    )
    assert failed_validation["status"] == "FAIL"
    assert "hard_limitations_are_pit_unsafe" in {
        row["check_id"] for row in failed_validation["checks"] if not row["passed"]
    }
