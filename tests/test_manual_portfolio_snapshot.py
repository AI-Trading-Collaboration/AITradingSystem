from __future__ import annotations

from pathlib import Path

from manual_portfolio_guardrail_helpers import manual_snapshot_payload, write_manual_snapshot

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    validate_manual_portfolio_snapshot_file,
    write_manual_portfolio_snapshot_artifact,
)


def test_manual_portfolio_snapshot_valid_payload_passes(tmp_path: Path) -> None:
    snapshot_path = write_manual_snapshot(tmp_path)

    artifact = write_manual_portfolio_snapshot_artifact(
        snapshot_path=snapshot_path,
        output_dir=tmp_path / "manual_portfolio_snapshot",
    )

    assert artifact["manifest"]["status"] == "PASS"
    assert artifact["normalized_portfolio"]["source"] == "manual_owner_input"
    assert artifact["normalized_portfolio"]["weights"]["QQQ"] == 0.50
    assert artifact["manifest"]["broker_action_allowed"] is False


def test_manual_portfolio_snapshot_invalid_weight_sum_fails(tmp_path: Path) -> None:
    snapshot_path = write_manual_snapshot(
        tmp_path,
        manual_snapshot_payload(qqq_weight=0.55),
    )

    validation = validate_manual_portfolio_snapshot_file(
        snapshot_path=snapshot_path,
        output_dir=tmp_path / "manual_portfolio_snapshot",
    )

    assert validation["status"] == "FAIL"
    assert _failed(validation, "weight_sum_within_tolerance")


def test_manual_portfolio_snapshot_duplicate_symbol_fails(tmp_path: Path) -> None:
    payload = manual_snapshot_payload()
    payload["positions"].append(dict(payload["positions"][0]))
    snapshot_path = write_manual_snapshot(tmp_path, payload)

    validation = validate_manual_portfolio_snapshot_file(
        snapshot_path=snapshot_path,
        output_dir=tmp_path / "manual_portfolio_snapshot",
    )

    assert validation["status"] == "FAIL"
    assert _failed(validation, "symbol_not_duplicated")


def test_manual_portfolio_snapshot_negative_weight_fails(tmp_path: Path) -> None:
    snapshot_path = write_manual_snapshot(
        tmp_path,
        manual_snapshot_payload(smh_weight=-0.05),
    )

    validation = validate_manual_portfolio_snapshot_file(
        snapshot_path=snapshot_path,
        output_dir=tmp_path / "manual_portfolio_snapshot",
    )

    assert validation["status"] == "FAIL"
    assert _failed(validation, "non_negative_weights")


def _failed(validation: dict[str, object], check_id: str) -> bool:
    return any(
        check["check_id"] == check_id and check["passed"] is False
        for check in validation["checks"]
    )
