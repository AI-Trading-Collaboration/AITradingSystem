from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from dynamic_v3_outcome_loop_helpers import (
    build_ready_outcome_update_fixture,
    run_safe_update_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_outcome_accumulation as accumulation
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    validate_advisory_outcome_artifact,
)


def test_outcome_update_updates_ready_window_and_audits_skips(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = run_safe_update_fixture(tmp_path, monkeypatch)
    result = fixture["update"]
    delta = result["outcome_status_delta"]

    assert result["manifest"]["updated_count"] == 1
    assert result["updated_windows"][0]["old_status"] == "PENDING"
    assert result["updated_windows"][0]["new_status"] == "AVAILABLE"
    assert result["updated_windows"][0]["future_data_used_in_decision"] is False
    assert {row["skip_reason"] for row in result["skipped_windows"]} >= {"NOT_DUE"}
    assert delta["before"]["forward_available"] == 0
    assert delta["after"]["forward_available"] == 1
    assert delta["before"]["forward_pending"] == 4
    assert delta["after"]["forward_pending"] == 3
    outcome_id = fixture["outcome"]["outcome_id"]
    event_rows = [
        json.loads(line)
        for line in (
            tmp_path
            / "advisory_outcome"
            / outcome_id
            / "outcome_update_events.jsonl"
        )
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert event_rows[-1]["allowed_window_days"] == [1]
    assert (
        validate_advisory_outcome_artifact(
            outcome_id=outcome_id,
            output_dir=tmp_path / "advisory_outcome",
        )["status"]
        == "PASS"
    )
    assert (
        accumulation.validate_outcome_update_artifact(
            update_id=result["outcome_update_id"],
            output_dir=tmp_path / "outcome_update",
        )["status"]
        == "PASS"
    )


def test_outcome_update_treats_cash_as_zero_return_without_price_row(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = build_ready_outcome_update_fixture(tmp_path, monkeypatch)
    prices_path = fixture["update_prices_path"]
    prices = pd.read_csv(prices_path)
    prices = prices[prices["ticker"] != "CASH"]
    prices.to_csv(prices_path, index=False)

    result = accumulation.run_outcome_update(
        update_review_id=fixture["update_review"]["update_review_id"],
        output_dir=tmp_path / "outcome_update",
        review_dir=tmp_path / "outcome_update_review",
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        prices_path=prices_path,
        rates_path=fixture["update_rates_path"],
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )

    assert result["manifest"]["updated_count"] == 1
    assert result["updated_windows"][0]["new_status"] == "AVAILABLE"


def test_outcome_update_requires_valid_review_before_mutation(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = build_ready_outcome_update_fixture(tmp_path, monkeypatch)
    review_id = fixture["update_review"]["update_review_id"]
    report_path = (
        tmp_path
        / "outcome_update_review"
        / review_id
        / "outcome_update_review_report.md"
    )
    report_path.write_text("tampered\n", encoding="utf-8")
    outcome_id = fixture["outcome"]["outcome_id"]
    events_path = (
        tmp_path / "advisory_outcome" / outcome_id / "outcome_update_events.jsonl"
    )
    before = events_path.read_bytes()

    with pytest.raises(accumulation.DynamicV3OutcomeAccumulationError):
        accumulation.run_outcome_update(
            update_review_id=review_id,
            output_dir=tmp_path / "outcome_update",
            review_dir=tmp_path / "outcome_update_review",
            advisory_outcome_dir=tmp_path / "advisory_outcome",
            paper_portfolio_dir=tmp_path / "paper_portfolio",
            prices_path=fixture["update_prices_path"],
            rates_path=fixture["update_rates_path"],
            generated_at=datetime(2026, 6, 10, tzinfo=UTC),
        )

    assert events_path.read_bytes() == before
    assert not (tmp_path / "outcome_update").exists()


def test_outcome_update_is_single_use_and_full_validator_detects_tamper(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = run_safe_update_fixture(tmp_path, monkeypatch)
    update = fixture["update"]
    review_id = fixture["update_review"]["update_review_id"]

    with pytest.raises(
        accumulation.DynamicV3OutcomeAccumulationError,
        match="already has a COMMITTED update",
    ):
        accumulation.run_outcome_update(
            update_review_id=review_id,
            output_dir=tmp_path / "outcome_update",
            review_dir=tmp_path / "outcome_update_review",
            advisory_outcome_dir=tmp_path / "advisory_outcome",
            paper_portfolio_dir=tmp_path / "paper_portfolio",
            prices_path=fixture["update_prices_path"],
            rates_path=fixture["update_rates_path"],
            generated_at=datetime(2026, 6, 10, 1, tzinfo=UTC),
        )

    update_dir = Path(update["outcome_update_dir"])
    transaction_path = update_dir / "outcome_update_transaction.json"
    transaction = json.loads(transaction_path.read_text(encoding="utf-8"))
    transaction["status"] = "PREPARED"
    transaction_path.write_text(
        json.dumps(transaction, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    validation = accumulation.validate_outcome_update_artifact(
        update_id=update["outcome_update_id"],
        output_dir=tmp_path / "outcome_update",
    )
    assert validation["status"] == "FAIL"
    assert any(
        check["check_id"] == "transaction_committed" and not check["passed"]
        for check in validation["checks"]
    )


def test_outcome_update_rolls_back_source_after_post_mutation_failure(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = build_ready_outcome_update_fixture(tmp_path, monkeypatch)
    outcome_id = fixture["outcome"]["outcome_id"]
    outcome_dir = tmp_path / "advisory_outcome" / outcome_id
    before = {
        path.relative_to(outcome_dir).as_posix(): path.read_bytes()
        for path in outcome_dir.rglob("*")
        if path.is_file()
    }
    original = accumulation.update_advisory_outcome

    def fail_after_actual_mutation(**kwargs: Any) -> dict[str, Any]:
        result = original(**kwargs)
        if Path(kwargs["output_dir"]) == tmp_path / "advisory_outcome":
            raise RuntimeError("forced post-mutation failure")
        return result

    monkeypatch.setattr(accumulation, "update_advisory_outcome", fail_after_actual_mutation)
    with pytest.raises(
        accumulation.DynamicV3OutcomeAccumulationError,
        match="transaction rolled back",
    ):
        accumulation.run_outcome_update(
            update_review_id=fixture["update_review"]["update_review_id"],
            output_dir=tmp_path / "outcome_update",
            review_dir=tmp_path / "outcome_update_review",
            advisory_outcome_dir=tmp_path / "advisory_outcome",
            paper_portfolio_dir=tmp_path / "paper_portfolio",
            prices_path=fixture["update_prices_path"],
            rates_path=fixture["update_rates_path"],
            generated_at=datetime(2026, 6, 10, tzinfo=UTC),
        )

    after = {
        path.relative_to(outcome_dir).as_posix(): path.read_bytes()
        for path in outcome_dir.rglob("*")
        if path.is_file()
    }
    assert after == before
    transactions = list((tmp_path / "outcome_update").glob("*/outcome_update_transaction.json"))
    assert len(transactions) == 1
    transaction = json.loads(transactions[0].read_text(encoding="utf-8"))
    assert transaction["status"] == "ROLLED_BACK"
    assert transaction["rollback_validation"] == {outcome_id: "PASS"}


@pytest.mark.parametrize("target", ["snapshot", "report", "live_post"])
def test_outcome_update_full_validator_detects_replay_tamper(
    tmp_path: Path,
    monkeypatch: Any,
    target: str,
) -> None:
    fixture = run_safe_update_fixture(tmp_path, monkeypatch)
    update = fixture["update"]
    update_dir = Path(update["outcome_update_dir"])
    if target == "snapshot":
        path = update_dir / "outcome_update_source_snapshot.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["selected_outcome_ids"] = []
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    elif target == "report":
        (update_dir / "outcome_update_report.md").write_text(
            "tampered\n", encoding="utf-8"
        )
    else:
        outcome_id = fixture["outcome"]["outcome_id"]
        path = (
            tmp_path
            / "advisory_outcome"
            / outcome_id
            / "advisory_outcome_report.md"
        )
        path.write_text(path.read_text(encoding="utf-8") + "tampered\n", encoding="utf-8")

    validation = accumulation.validate_outcome_update_artifact(
        update_id=update["outcome_update_id"],
        output_dir=tmp_path / "outcome_update",
    )
    assert validation["status"] == "FAIL"
