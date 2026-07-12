from __future__ import annotations

import json
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_historical_replay_helpers import (
    build_replay_inventory,
    prepare_replay_test_environment,
    write_owner_reviews,
    write_paper_action,
    write_replay_daily_advisory,
)

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DynamicV3HistoricalReplayError,
    validate_replay_inventory_artifact,
)


def test_replay_inventory_classifies_pit_safety_and_blocks_unsafe(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    safe_target = {"QQQ": 0.45, "SMH": 0.30, "SOXX": 0.10, "CASH": 0.15}
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="safe",
        as_of="2026-06-03",
        target_weights=safe_target,
    )
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="warning",
        as_of="2026-06-04",
        target_weights=safe_target,
    )
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="unsafe",
        as_of="2026-06-05",
        target_weights=None,
    )
    write_owner_reviews(paths["owner_review_dir"], ["safe", "unsafe"])
    write_paper_action(
        paths["paper_portfolio_dir"],
        daily_advisory_id="safe",
        after_weights=safe_target,
    )

    result = build_replay_inventory(paths, start=date(2026, 6, 1), end=date(2026, 6, 10))
    rows = {row["daily_advisory_id"]: row for row in result["rows"]}

    assert rows["safe"]["pit_safety_status"] == "PIT_SAFE"
    assert rows["warning"]["pit_safety_status"] == "PIT_WARNING"
    assert rows["unsafe"]["pit_safety_status"] == "PIT_UNSAFE"
    assert rows["unsafe"]["replay_eligibility"] == "INELIGIBLE"
    assert result["manifest"]["pit_safe_count"] == 1
    assert result["manifest"]["pit_warning_count"] == 1
    assert result["manifest"]["pit_unsafe_count"] == 1
    assert result["pit_safety_audit"]["pit_unsafe_allowed_in_default_replay"] is False

    validation = validate_replay_inventory_artifact(
        inventory_id=result["inventory_id"],
        output_dir=paths["inventory_dir"],
    )
    assert validation["status"] == "PASS"


def test_replay_inventory_rejects_invalid_range_before_output(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)

    with pytest.raises(DynamicV3HistoricalReplayError, match="start must be on or before end"):
        build_replay_inventory(paths, start=date(2026, 6, 10), end=date(2026, 6, 1))

    assert not paths["inventory_dir"].exists()


def test_replay_inventory_excludes_sources_generated_after_cutoff(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    target = {"QQQ": 0.45, "SMH": 0.30, "SOXX": 0.10, "CASH": 0.15}
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="visible",
        as_of="2026-06-03",
        generated_at="2026-06-03T12:00:00+00:00",
        target_weights=target,
    )
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="future",
        as_of="2026-06-04",
        generated_at="2026-06-11T12:00:00+00:00",
        target_weights=target,
    )

    result = build_replay_inventory(paths, start=date(2026, 6, 1), end=date(2026, 6, 10))

    assert [row["daily_advisory_id"] for row in result["rows"]] == ["visible"]
    assert result["manifest"]["future_generated_source_excluded_count"] == 1
    assert result["manifest"]["evidence_cutoff"] == datetime(
        2026, 6, 10, tzinfo=UTC
    ).isoformat()
    assert result["manifest"]["price_data_role"] == (
        "outcome_availability_only_not_decision_input"
    )


@pytest.mark.parametrize("duplicate_field", ["daily_id", "as_of"])
def test_replay_inventory_rejects_ambiguous_daily_sources_before_output(
    tmp_path: Path,
    monkeypatch: Any,
    duplicate_field: str,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    target = {"QQQ": 0.45, "SMH": 0.30, "SOXX": 0.10, "CASH": 0.15}
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="first",
        as_of="2026-06-03",
        target_weights=target,
    )
    second_dir = write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="second",
        as_of="2026-06-04" if duplicate_field == "daily_id" else "2026-06-03",
        target_weights=target,
    )
    if duplicate_field == "daily_id":
        manifest_path = second_dir / "daily_advisory_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["daily_advisory_id"] = "first"
        manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")

    expected = "daily advisory ids" if duplicate_field == "daily_id" else "per as_of"
    with pytest.raises(DynamicV3HistoricalReplayError, match=expected):
        build_replay_inventory(paths, start=date(2026, 6, 1), end=date(2026, 6, 10))

    assert not paths["inventory_dir"].exists()


def test_replay_inventory_uses_cutoff_visible_owner_and_paper_inputs(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    target = {"QQQ": 0.45, "SMH": 0.30, "SOXX": 0.10, "CASH": 0.15}
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="visible",
        as_of="2026-06-03",
        target_weights=target,
    )
    write_owner_reviews(paths["owner_review_dir"], ["visible"])
    owner_path = paths["owner_review_dir"] / "owner_review_journal.jsonl"
    owner_row = json.loads(owner_path.read_text(encoding="utf-8"))
    owner_row["created_at"] = "2026-06-11T00:00:00+00:00"
    owner_row["updated_at"] = "2026-06-11T00:00:00+00:00"
    owner_path.write_text(json.dumps(owner_row, sort_keys=True) + "\n", encoding="utf-8")
    write_paper_action(
        paths["paper_portfolio_dir"],
        daily_advisory_id="visible",
        after_weights=target,
    )
    ledger_path = paths["paper_portfolio_dir"] / "paper-1" / "paper_action_ledger.jsonl"
    paper_row = json.loads(ledger_path.read_text(encoding="utf-8"))
    paper_row["created_at"] = "2026-06-11T00:00:00+00:00"
    ledger_path.write_text(json.dumps(paper_row, sort_keys=True) + "\n", encoding="utf-8")

    result = build_replay_inventory(
        paths,
        start=date(2026, 6, 1),
        end=date(2026, 6, 10),
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )

    row = result["rows"][0]
    assert row["source_artifacts"]["owner_review_id"] == ""
    assert row["decision_inputs"]["paper_action_weights"] == {}
    assert "OWNER_DECISION_MISSING" in row["replay_limitations"]
    assert result["source_snapshot"]["cutoff_visible_bindings"][0][
        "daily_advisory_id"
    ] == "visible"


@pytest.mark.parametrize("tamper_target", ["snapshot", "report", "source"])
def test_replay_inventory_validation_detects_tamper_and_source_drift(
    tmp_path: Path,
    monkeypatch: Any,
    tamper_target: str,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    target = {"QQQ": 0.45, "SMH": 0.30, "SOXX": 0.10, "CASH": 0.15}
    advisory_dir = write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="tamper",
        as_of="2026-06-03",
        target_weights=target,
    )
    result = build_replay_inventory(paths, start=date(2026, 6, 1), end=date(2026, 6, 10))
    inventory_dir = Path(result["inventory_dir"])

    if tamper_target == "snapshot":
        snapshot_path = inventory_dir / "replay_inventory_source_snapshot.json"
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        snapshot["canonical_rows"][0]["recommended_action"] = "tampered"
        snapshot_path.write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        manifest_path = inventory_dir / "replay_inventory_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["source_snapshot_checksum"] = sha256(snapshot_path.read_bytes()).hexdigest()
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    elif tamper_target == "report":
        report_path = inventory_dir / "replay_inventory_report.md"
        report_path.write_text(
            report_path.read_text(encoding="utf-8") + "\ntampered\n",
            encoding="utf-8",
        )
    else:
        actions_path = advisory_dir / "daily_advisory_actions.json"
        actions_path.write_text(
            actions_path.read_text(encoding="utf-8") + " ",
            encoding="utf-8",
        )

    validation = validate_replay_inventory_artifact(
        inventory_id=result["inventory_id"],
        output_dir=paths["inventory_dir"],
    )
    failed = {
        check["check_id"] for check in validation["checks"] if check["passed"] is False
    }
    assert validation["status"] == "FAIL"
    expected = {
        "snapshot": "snapshot_rows_recomputed",
        "report": "report_recomputed",
        "source": "source_files_unchanged",
    }
    assert expected[tamper_target] in failed


def test_replay_inventory_missing_artifact_is_not_accepted_as_legacy(
    tmp_path: Path,
) -> None:
    validation = validate_replay_inventory_artifact(
        inventory_id="missing",
        output_dir=tmp_path,
    )

    assert validation["status"] == "FAIL"
    assert validation["source_snapshot_status"] == "LEGACY_UNSNAPSHOTTED"
