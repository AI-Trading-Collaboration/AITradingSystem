from __future__ import annotations

from pathlib import Path

import yaml

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    validate_portfolio_snapshot_file,
    write_portfolio_snapshot_artifact,
)


def _snapshot_payload(weight_sum: float = 1.0) -> dict[str, object]:
    return {
        "schema_version": 1,
        "as_of": "2026-06-07",
        "base_currency": "USD",
        "account_type": "manual_snapshot",
        "source": "manual",
        "total_equity": 100000.0,
        "cash": {"symbol": "CASH", "weight": 0.20, "value": 20000.0},
        "positions": [
            {"symbol": "QQQ", "weight": 0.50, "value": 50000.0, "currency": "USD"},
            {
                "symbol": "SMH",
                "weight": round(weight_sum - 0.70, 6),
                "value": 30000.0,
                "currency": "USD",
            },
        ],
        "metadata": {
            "owner_reviewed": False,
            "broker_imported": False,
            "notes": "Manual snapshot for advisory only",
        },
    }


def test_portfolio_snapshot_validate_pass_and_normalize(tmp_path: Path) -> None:
    snapshot = tmp_path / "snapshot.yaml"
    snapshot.write_text(yaml.safe_dump(_snapshot_payload(), sort_keys=False), encoding="utf-8")

    validation = validate_portfolio_snapshot_file(
        snapshot_path=snapshot,
        output_dir=tmp_path / "portfolio_snapshot",
    )
    artifact = write_portfolio_snapshot_artifact(
        snapshot_path=snapshot,
        output_dir=tmp_path / "portfolio_snapshot",
    )

    assert validation["status"] == "PASS"
    assert artifact["normalized_positions"]["weights"]["CASH"] == 0.20
    assert artifact["manifest"]["broker_imported"] is False


def test_portfolio_snapshot_invalid_weight_sum_fails(tmp_path: Path) -> None:
    snapshot = tmp_path / "invalid_snapshot.yaml"
    snapshot.write_text(
        yaml.safe_dump(_snapshot_payload(weight_sum=1.05), sort_keys=False),
        encoding="utf-8",
    )

    validation = validate_portfolio_snapshot_file(
        snapshot_path=snapshot,
        output_dir=tmp_path / "portfolio_snapshot",
    )

    assert validation["status"] == "FAIL"
    assert any(
        check["check_id"] == "weight_sum_within_tolerance" and check["passed"] is False
        for check in validation["checks"]
    )


def test_portfolio_snapshot_broker_imported_fails(tmp_path: Path) -> None:
    payload = _snapshot_payload()
    payload["metadata"]["broker_imported"] = True  # type: ignore[index]
    snapshot = tmp_path / "broker_snapshot.yaml"
    snapshot.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    validation = validate_portfolio_snapshot_file(
        snapshot_path=snapshot,
        output_dir=tmp_path / "portfolio_snapshot",
    )

    assert validation["status"] == "FAIL"
    assert any(
        check["check_id"] == "broker_imported_false" and check["passed"] is False
        for check in validation["checks"]
    )
