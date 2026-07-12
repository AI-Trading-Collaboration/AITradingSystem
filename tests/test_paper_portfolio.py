from __future__ import annotations

import json
from datetime import date
from hashlib import sha256
from pathlib import Path

import pytest
import yaml
from dynamic_v3_paper_tracking_helpers import (
    paper_config_path,
    read_json,
    read_jsonl,
    write_validated_owner_review,
)

from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DynamicV3PaperTrackingError,
    apply_owner_review_to_paper_portfolio,
    init_paper_portfolio,
    validate_paper_portfolio_artifact,
)


def test_paper_portfolio_init_from_manual_snapshot(tmp_path: Path) -> None:
    result = init_paper_portfolio(
        config_path=paper_config_path(tmp_path),
        output_dir=tmp_path / "paper_portfolio",
    )

    state = result["state"]
    assert state["state_status"] == "ACTIVE"
    assert state["broker_action_taken"] is False
    assert state["positions"] == {"CASH": 0.2, "QQQ": 0.5, "SMH": 0.2, "TLT": 0.1}
    assert (
        validate_paper_portfolio_artifact(
            paper_portfolio_id=result["paper_portfolio_id"],
            output_dir=tmp_path / "paper_portfolio",
        )["status"]
        == "PASS"
    )


@pytest.mark.parametrize("owner_decision", ["monitor", "no_trade"])
def test_monitor_and_no_trade_do_not_change_paper_state(
    tmp_path: Path, owner_decision: str
) -> None:
    config_path = paper_config_path(tmp_path)
    portfolio = init_paper_portfolio(
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
    )
    review = write_validated_owner_review(tmp_path, owner_decision=owner_decision)

    result = apply_owner_review_to_paper_portfolio(
        review_id=review["review_id"],
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
        owner_review_dir=tmp_path / "owner_review_journal",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
    )

    assert result["state"]["positions"] == portfolio["state"]["positions"]
    assert result["event"]["owner_decision"] == owner_decision
    assert result["event"]["applied_paper_deltas"] == {}
    assert result["event"]["broker_action_taken"] is False


def test_paper_adjustment_changes_paper_state_only_and_ledger_rebuilds(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(tmp_path)
    portfolio = init_paper_portfolio(
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
    )
    review = write_validated_owner_review(tmp_path, owner_decision="paper_adjustment")

    result = apply_owner_review_to_paper_portfolio(
        review_id=review["review_id"],
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
        owner_review_dir=tmp_path / "owner_review_journal",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
    )

    state_path = (
        tmp_path
        / "paper_portfolio"
        / portfolio["paper_portfolio_id"]
        / "paper_portfolio_state.json"
    )
    ledger_path = (
        tmp_path / "paper_portfolio" / portfolio["paper_portfolio_id"] / "paper_action_ledger.jsonl"
    )
    assert result["state"]["positions"] != portfolio["state"]["positions"]
    assert result["state"]["broker_action_taken"] is False
    assert max(abs(value) for value in result["event"]["applied_paper_deltas"].values()) <= 0.05
    assert read_json(state_path)["last_review_id"] == review["review_id"]
    assert len(read_jsonl(ledger_path)) == 1
    assert (
        validate_paper_portfolio_artifact(
            paper_portfolio_id=portfolio["paper_portfolio_id"],
            output_dir=tmp_path / "paper_portfolio",
        )["status"]
        == "PASS"
    )


def test_paper_portfolio_ledger_is_append_only_and_rejects_duplicate_review(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(tmp_path)
    portfolio = init_paper_portfolio(
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
    )
    first_review = write_validated_owner_review(
        tmp_path / "source_one",
        owner_decision="monitor",
        as_of=date(2026, 6, 7),
    )
    first = apply_owner_review_to_paper_portfolio(
        review_id=str(first_review["review_id"]),
        paper_portfolio_id=portfolio["paper_portfolio_id"],
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
        owner_review_dir=Path(first_review["owner_review_dir"]),
        daily_advisory_dir=Path(first_review["daily_advisory_dir"]),
    )
    ledger_path = Path(portfolio["paper_portfolio_dir"]) / "paper_action_ledger.jsonl"
    first_event = read_jsonl(ledger_path)[0]

    with pytest.raises(DynamicV3PaperTrackingError, match="already applied"):
        apply_owner_review_to_paper_portfolio(
            review_id=str(first_review["review_id"]),
            paper_portfolio_id=portfolio["paper_portfolio_id"],
            config_path=config_path,
            output_dir=tmp_path / "paper_portfolio",
            owner_review_dir=Path(first_review["owner_review_dir"]),
            daily_advisory_dir=Path(first_review["daily_advisory_dir"]),
        )

    second_review = write_validated_owner_review(
        tmp_path / "source_two",
        owner_decision="no_trade",
        as_of=date(2026, 6, 8),
    )
    second = apply_owner_review_to_paper_portfolio(
        review_id=str(second_review["review_id"]),
        paper_portfolio_id=portfolio["paper_portfolio_id"],
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
        owner_review_dir=Path(second_review["owner_review_dir"]),
        daily_advisory_dir=Path(second_review["daily_advisory_dir"]),
    )
    events = read_jsonl(ledger_path)
    assert events[0] == first_event
    assert events[0]["event_sequence"] == 1
    assert events[0]["previous_event_checksum"] == "GENESIS"
    assert events[1]["event_sequence"] == 2
    assert events[1]["previous_event_checksum"] == events[0]["event_checksum"]
    assert first["state"]["positions"] == second["state"]["positions"]


def test_paper_portfolio_rejects_source_drift_and_wrong_daily_root_before_write(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(tmp_path)
    portfolio = init_paper_portfolio(
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
    )
    review = write_validated_owner_review(tmp_path / "source", owner_decision="monitor")
    ledger_path = Path(portfolio["paper_portfolio_dir"]) / "paper_action_ledger.jsonl"
    with pytest.raises(DynamicV3PaperTrackingError, match="frozen source"):
        apply_owner_review_to_paper_portfolio(
            review_id=str(review["review_id"]),
            paper_portfolio_id=portfolio["paper_portfolio_id"],
            config_path=config_path,
            output_dir=tmp_path / "paper_portfolio",
            owner_review_dir=Path(review["owner_review_dir"]),
            daily_advisory_dir=tmp_path / "wrong_daily_root",
        )
    assert read_jsonl(ledger_path) == []

    source_report = (
        Path(review["daily_advisory_dir"])
        / str(review["daily_advisory_id"])
        / "daily_position_advisory_report.md"
    )
    source_report.write_text(
        source_report.read_text(encoding="utf-8") + "\ntampered\n",
        encoding="utf-8",
    )
    with pytest.raises(DynamicV3PaperTrackingError, match="owner review validation must PASS"):
        apply_owner_review_to_paper_portfolio(
            review_id=str(review["review_id"]),
            paper_portfolio_id=portfolio["paper_portfolio_id"],
            config_path=config_path,
            output_dir=tmp_path / "paper_portfolio",
            owner_review_dir=Path(review["owner_review_dir"]),
            daily_advisory_dir=Path(review["daily_advisory_dir"]),
        )
    assert read_jsonl(ledger_path) == []


def test_paper_portfolio_manual_adjustment_requires_finite_zero_sum_deltas(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(tmp_path)
    portfolio = init_paper_portfolio(
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
    )
    review = write_validated_owner_review(
        tmp_path / "source", owner_decision="manual_adjustment"
    )
    common = {
        "review_id": str(review["review_id"]),
        "paper_portfolio_id": portfolio["paper_portfolio_id"],
        "config_path": config_path,
        "output_dir": tmp_path / "paper_portfolio",
        "owner_review_dir": Path(review["owner_review_dir"]),
        "daily_advisory_dir": Path(review["daily_advisory_dir"]),
    }
    with pytest.raises(DynamicV3PaperTrackingError, match="non-empty"):
        apply_owner_review_to_paper_portfolio(**common)
    with pytest.raises(DynamicV3PaperTrackingError, match="finite"):
        apply_owner_review_to_paper_portfolio(
            **common,
            manual_deltas={"QQQ": float("nan"), "CASH": 0.0},
        )
    with pytest.raises(DynamicV3PaperTrackingError, match="sum to zero"):
        apply_owner_review_to_paper_portfolio(
            **common,
            manual_deltas={"QQQ": -0.02, "CASH": 0.01},
        )

    result = apply_owner_review_to_paper_portfolio(
        **common,
        manual_deltas={"QQQ": -0.02, "CASH": 0.02},
    )
    assert result["event"]["manual_override"] is True
    assert result["event"]["applied_paper_deltas"] == {"CASH": 0.02, "QQQ": -0.02}


def test_paper_portfolio_validator_detects_rehashed_event_and_materialized_tamper(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(tmp_path)
    portfolio = init_paper_portfolio(
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
    )
    review = write_validated_owner_review(
        tmp_path / "source", owner_decision="paper_adjustment"
    )
    apply_owner_review_to_paper_portfolio(
        review_id=str(review["review_id"]),
        paper_portfolio_id=portfolio["paper_portfolio_id"],
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
        owner_review_dir=Path(review["owner_review_dir"]),
        daily_advisory_dir=Path(review["daily_advisory_dir"]),
    )
    portfolio_dir = Path(portfolio["paper_portfolio_dir"])
    ledger_path = portfolio_dir / "paper_action_ledger.jsonl"
    event = read_jsonl(ledger_path)[0]
    original_event = json.loads(json.dumps(event))
    event["proposed_deltas"] = {"CASH": 0.01, "QQQ": -0.01}
    event["event_checksum"] = _event_checksum(event)
    ledger_path.write_text(json.dumps(event, sort_keys=True) + "\n", encoding="utf-8")
    validation = validate_paper_portfolio_artifact(
        paper_portfolio_id=portfolio["paper_portfolio_id"],
        output_dir=tmp_path / "paper_portfolio",
    )
    assert validation["status"] == "FAIL"
    assert "event_chain_and_content_valid" in _failed_check_ids(validation)

    ledger_path.write_text(
        json.dumps(original_event, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    state_path = portfolio_dir / "paper_portfolio_state.json"
    state = read_json(state_path)
    state["last_action_id"] = "tampered"
    state_path.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")
    history_path = portfolio_dir / "paper_position_history.jsonl"
    history = read_jsonl(history_path)
    history[-1]["positions"] = {"CASH": 1.0}
    history_path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in history) + "\n",
        encoding="utf-8",
    )
    report_path = portfolio_dir / "paper_portfolio_report.md"
    report_path.write_text(
        report_path.read_text(encoding="utf-8") + "\ntampered\n",
        encoding="utf-8",
    )
    validation = validate_paper_portfolio_artifact(
        paper_portfolio_id=portfolio["paper_portfolio_id"],
        output_dir=tmp_path / "paper_portfolio",
    )
    assert validation["status"] == "FAIL"
    failed = _failed_check_ids(validation)
    assert "state_matches_event_replay" in failed
    assert "history_matches_event_replay" in failed
    assert "report_matches_event_replay" in failed


def test_paper_portfolio_legacy_is_read_only_and_invalid_init_sources_fail_before_write(
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "invalid_snapshot.yaml"
    snapshot_path.write_text(
        yaml.safe_dump(
            {
                "as_of": "2026-06-07",
                "cash": {"symbol": "CASH", "weight": 0.5},
                "positions": [{"symbol": "QQQ", "weight": 0.5}],
                "metadata": {"owner_reviewed": False, "broker_imported": False},
            }
        ),
        encoding="utf-8",
    )
    invalid_root = tmp_path / "invalid"
    invalid_root.mkdir()
    invalid_config = paper_config_path(invalid_root, snapshot_path=snapshot_path)
    with pytest.raises(DynamicV3PaperTrackingError, match="owner reviewed"):
        init_paper_portfolio(
            config_path=invalid_config,
            output_dir=tmp_path / "invalid_output",
        )
    assert not (tmp_path / "invalid_output").exists()

    invalid_policy_root = tmp_path / "invalid_policy"
    invalid_policy_root.mkdir()
    invalid_policy = paper_config_path(invalid_policy_root)
    invalid_policy_payload = yaml.safe_load(invalid_policy.read_text(encoding="utf-8"))
    invalid_policy_payload["simulation"]["max_single_symbol_adjustment"] = float("nan")
    invalid_policy.write_text(
        yaml.safe_dump(invalid_policy_payload, sort_keys=False),
        encoding="utf-8",
    )
    with pytest.raises(DynamicV3PaperTrackingError, match="finite number"):
        init_paper_portfolio(
            config_path=invalid_policy,
            output_dir=tmp_path / "invalid_policy_output",
        )
    assert not (tmp_path / "invalid_policy_output").exists()

    valid_root = tmp_path / "valid"
    valid_root.mkdir()
    config_path = paper_config_path(valid_root)
    portfolio = init_paper_portfolio(
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
    )
    manifest_path = Path(portfolio["paper_portfolio_dir"]) / "paper_portfolio_manifest.json"
    manifest = read_json(manifest_path)
    manifest.pop("ledger_schema_version")
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    validation = validate_paper_portfolio_artifact(
        paper_portfolio_id=portfolio["paper_portfolio_id"],
        output_dir=tmp_path / "paper_portfolio",
    )
    assert validation["status"] == "PASS_WITH_WARNINGS"
    assert validation["event_chain_status"] == "LEGACY_UNCHAINED"
    with pytest.raises(DynamicV3PaperTrackingError, match="validation must PASS"):
        apply_owner_review_to_paper_portfolio(
            review_id="missing",
            paper_portfolio_id=portfolio["paper_portfolio_id"],
            config_path=config_path,
            output_dir=tmp_path / "paper_portfolio",
            owner_review_dir=tmp_path / "owner_review",
            daily_advisory_dir=tmp_path / "daily",
        )


def test_paper_portfolio_config_checksum_drift_fails_validation_and_blocks_mutation(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(tmp_path)
    portfolio = init_paper_portfolio(
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
    )
    config_path.write_text(
        config_path.read_text(encoding="utf-8") + "\n# checksum drift\n",
        encoding="utf-8",
    )
    validation = validate_paper_portfolio_artifact(
        paper_portfolio_id=portfolio["paper_portfolio_id"],
        output_dir=tmp_path / "paper_portfolio",
    )
    assert validation["status"] == "FAIL"
    assert "source_recomputation_succeeds" in _failed_check_ids(validation)
    with pytest.raises(DynamicV3PaperTrackingError, match="validation must PASS"):
        apply_owner_review_to_paper_portfolio(
            review_id="missing",
            paper_portfolio_id=portfolio["paper_portfolio_id"],
            config_path=config_path,
            output_dir=tmp_path / "paper_portfolio",
            owner_review_dir=tmp_path / "owner_review",
            daily_advisory_dir=tmp_path / "daily",
        )


def _event_checksum(event: dict[str, object]) -> str:
    payload = dict(event)
    payload.pop("event_checksum", None)
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256(canonical.encode("utf-8")).hexdigest()


def _failed_check_ids(payload: dict[str, object]) -> set[str]:
    return {
        str(check["check_id"])
        for check in payload["checks"]  # type: ignore[index]
        if check["passed"] is False  # type: ignore[index]
    }
