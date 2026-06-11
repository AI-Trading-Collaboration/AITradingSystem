from __future__ import annotations

from pathlib import Path

from manual_portfolio_guardrail_helpers import (
    manual_snapshot_payload,
    write_manual_snapshot_artifact,
)

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    run_portfolio_exposure_validation,
    validate_portfolio_exposure_artifact,
)


def test_portfolio_exposure_flags_semiconductor_limit(tmp_path: Path) -> None:
    snapshot = write_manual_snapshot_artifact(
        tmp_path,
        manual_snapshot_payload(
            cash_weight=0.20,
            qqq_weight=0.30,
            smh_weight=0.45,
            tlt_weight=0.05,
        ),
    )

    exposure = run_portfolio_exposure_validation(
        snapshot_id=snapshot["snapshot_id"],
        snapshot_dir=tmp_path / "manual_portfolio_snapshot",
        output_dir=tmp_path / "portfolio_exposure",
    )
    validation = validate_portfolio_exposure_artifact(
        exposure_id=exposure["exposure_id"],
        output_dir=tmp_path / "portfolio_exposure",
    )

    assert exposure["exposure_summary"]["status"] == "FAIL"
    assert "semiconductor_watch" in exposure["exposure_summary"]["warnings"]
    assert validation["status"] == "PASS"


def test_portfolio_exposure_validates_non_base_currency(tmp_path: Path) -> None:
    snapshot = write_manual_snapshot_artifact(
        tmp_path,
        manual_snapshot_payload(
            cash_weight=0.20,
            qqq_weight=0.30,
            smh_weight=0.20,
            tlt_weight=0.30,
            qqq_currency="EUR",
        ),
    )

    exposure = run_portfolio_exposure_validation(
        snapshot_id=snapshot["snapshot_id"],
        snapshot_dir=tmp_path / "manual_portfolio_snapshot",
        output_dir=tmp_path / "portfolio_exposure",
    )

    assert exposure["currency_exposure"]["status"] == "FAIL"
    assert exposure["currency_exposure"]["non_base_currency_weight"] == 0.30
    assert "max_non_base_currency_weight" in exposure["exposure_summary"]["warnings"]
    assert exposure["manifest"]["broker_action_allowed"] is False
