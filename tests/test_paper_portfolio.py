from __future__ import annotations

from pathlib import Path

import pytest
from dynamic_v3_paper_tracking_helpers import (
    paper_config_path,
    read_json,
    read_jsonl,
    write_daily_advisory,
    write_owner_review,
)

from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
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
    write_daily_advisory(tmp_path)
    review = write_owner_review(tmp_path, owner_decision=owner_decision)

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
    write_daily_advisory(tmp_path)
    review = write_owner_review(tmp_path, owner_decision="paper_adjustment")

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
        tmp_path
        / "paper_portfolio"
        / portfolio["paper_portfolio_id"]
        / "paper_action_ledger.jsonl"
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
