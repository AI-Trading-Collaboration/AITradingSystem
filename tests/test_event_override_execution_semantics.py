from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml
from test_execution_semantics import _write_execution_caches
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.execution_semantics import (
    EventOverrideDecision,
    _actual_position_path,
    _empty_event_override_runtime,
    evaluate_event_override_decision,
    run_execution_semantics_rebacktest,
    supersede_pending_plan,
)


def test_executed_position_cannot_be_superseded() -> None:
    decision = _decision(pending_plan_status="EXECUTED")
    original, new_plan, log = supersede_pending_plan(
        pending_plan=_pending_plan(status="EXECUTED"),
        decision=decision,
        new_target_weights={"QQQ": 0.4, "TQQQ": 0.0, "SGOV": 0.6},
        supersede_timestamp="2026-01-02",
        policy_hash="policy-hash",
    )

    assert decision.allowed_by_policy is False
    assert "pending_plan_status_not_supersedable:EXECUTED" in decision.blocked_reasons
    assert original["status"] == "EXECUTED"
    assert new_plan is None
    assert log is None


def test_pending_rebalance_can_be_superseded_before_execution() -> None:
    decision = _decision(pending_plan_status="PENDING_REBALANCE")
    original, new_plan, log = supersede_pending_plan(
        pending_plan=_pending_plan(status="PENDING_REBALANCE"),
        decision=decision,
        new_target_weights={"QQQ": 0.4, "TQQQ": 0.0, "SGOV": 0.6},
        supersede_timestamp="2026-01-02",
        policy_hash="policy-hash",
    )

    assert decision.allowed_by_policy is True
    assert original["status"] == "SUPERSEDED"
    assert new_plan is not None
    assert new_plan["status"] == "PENDING_REBALANCE"
    assert new_plan["supersedes_plan_id"] == "plan-1"
    assert log is not None
    assert log["original_pending_plan_id"] == "plan-1"


def test_advisory_generated_can_be_superseded_before_execution() -> None:
    decision = _decision(pending_plan_status="ADVISORY_GENERATED")
    original, new_plan, _log = supersede_pending_plan(
        pending_plan=_pending_plan(status="ADVISORY_GENERATED"),
        decision=decision,
        new_target_weights={"QQQ": 0.4, "TQQQ": 0.0, "SGOV": 0.6},
        supersede_timestamp="2026-01-02",
        policy_hash="policy-hash",
    )

    assert original["status"] == "SUPERSEDED"
    assert new_plan is not None
    assert new_plan["source_event_ids"] == ["event-1"]


def test_superseded_plan_remains_in_ledger(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    event_policy_path = _write_explicit_event_policy(tmp_path, prices_path)
    output_root = tmp_path / "execution_semantics"

    run_execution_semantics_rebacktest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        strategy_ids=["limited_adjustment"],
        as_of_date=as_of,
        enable_event_override=True,
        event_override_policy_path=event_policy_path,
        emit_pending_plan_ledger=True,
        emit_supersede_log=True,
        emit_event_override_trace=True,
        event_override_survival_matrix_path=tmp_path / "matrix.yaml",
        event_override_review_path=tmp_path / "review.md",
    )

    ledger = pd.read_csv(
        output_root / "limited_adjustment_event_override_v1" / "pending_plan_ledger.csv"
    )
    assert "SUPERSEDED" in set(ledger["status"])
    assert "EXECUTED" in set(ledger["status"])


def test_t_day_known_event_can_trigger_t_plus_1_override() -> None:
    dates = pd.bdate_range("2026-01-02", periods=5)
    runtime = _runtime()
    actual, rows = _actual_position_path(
        strategy_id="toy_event_override",
        execution_policy_id="no_rebalance",
        target_weights=_constant_target(dates),
        policy=_no_rebalance_policy(),
        enable_event_override=True,
        event_override_policy=_policy_with_event(dates[1].date().isoformat()),
        event_override_runtime=runtime,
    )

    assert rows[1]["event_override_executed"] is False
    assert rows[2]["event_override_executed"] is True
    assert rows[2]["trigger_reason"] == "event_override_t_plus_1"
    assert rows[2]["actual_execution_date"] == dates[2].date().isoformat()
    assert actual.iloc[2]["QQQ"] < actual.iloc[1]["QQQ"]


def test_event_known_after_review_cutoff_cannot_trigger_same_review_override() -> None:
    decision = evaluate_event_override_decision(
        event_id="future-event",
        event_known_at="2026-01-03",
        review_at="2026-01-02",
        decision_at="2026-01-02",
        event_risk_score=90,
        override_direction="RISK_REDUCTION",
        pending_plan_status="PENDING_REBALANCE",
        effective_at="2026-01-05",
        policy=_policy(),
        original_target_weights={"QQQ": 0.8, "TQQQ": 0.0, "SGOV": 0.2},
        new_target_weights={"QQQ": 0.6, "TQQQ": 0.0, "SGOV": 0.4},
    )

    assert decision.allowed_by_policy is False
    assert "event_known_after_review_cutoff" in decision.blocked_reasons


def test_event_override_effective_date_must_be_after_decision_date() -> None:
    decision = evaluate_event_override_decision(
        event_id="same-day",
        event_known_at="2026-01-02",
        review_at="2026-01-02",
        decision_at="2026-01-02",
        event_risk_score=90,
        override_direction="RISK_REDUCTION",
        pending_plan_status="PENDING_REBALANCE",
        effective_at="2026-01-02",
        policy=_policy(),
        original_target_weights={"QQQ": 0.8, "TQQQ": 0.0, "SGOV": 0.2},
        new_target_weights={"QQQ": 0.6, "TQQQ": 0.0, "SGOV": 0.4},
    )

    assert decision.allowed_by_policy is False
    assert "effective_date_not_after_decision_date" in decision.blocked_reasons


def test_event_override_does_not_rewrite_prior_actual_path() -> None:
    dates = pd.bdate_range("2026-01-02", periods=5)
    _actual, rows = _actual_position_path(
        strategy_id="toy_event_override",
        execution_policy_id="no_rebalance",
        target_weights=_constant_target(dates),
        policy=_no_rebalance_policy(),
        enable_event_override=True,
        event_override_policy=_policy_with_event(dates[2].date().isoformat()),
        event_override_runtime=_runtime(),
    )

    assert [row["event_override_executed"] for row in rows[:3]] == [False, False, False]
    assert rows[3]["event_override_executed"] is True
    assert rows[1]["actual_weight_qqq"] == rows[2]["actual_weight_qqq"]


def test_risk_off_override_can_reduce_equity_exposure() -> None:
    decision = _decision(
        original={"QQQ": 0.8, "TQQQ": 0.0, "SGOV": 0.2},
        new={"QQQ": 0.6, "TQQQ": 0.0, "SGOV": 0.4},
    )

    assert decision.allowed_by_policy is True
    assert decision.override_direction == "RISK_REDUCTION"


def test_risk_off_override_can_increase_cash_or_sgov() -> None:
    decision = _decision(
        original={"QQQ": 0.5, "TQQQ": 0.2, "SGOV": 0.3},
        new={"QQQ": 0.4, "TQQQ": 0.0, "SGOV": 0.6},
    )

    assert decision.allowed_by_policy is True


def test_risk_on_override_is_blocked_without_confirmation() -> None:
    decision = evaluate_event_override_decision(
        event_id="risk-on",
        event_known_at="2026-01-02",
        review_at="2026-01-02",
        decision_at="2026-01-02",
        event_risk_score=90,
        override_direction="RISK_INCREASE",
        pending_plan_status="PENDING_REBALANCE",
        effective_at="2026-01-05",
        policy=_policy(),
        original_target_weights={"QQQ": 0.4, "TQQQ": 0.0, "SGOV": 0.6},
        new_target_weights={"QQQ": 0.7, "TQQQ": 0.0, "SGOV": 0.3},
    )

    assert decision.allowed_by_policy is False
    assert "risk_on_fast_override_disabled" in decision.blocked_reasons


def test_event_override_cannot_increase_leverage() -> None:
    decision = _decision(
        original={"QQQ": 1.0, "TQQQ": 0.0, "SGOV": 0.0},
        new={"QQQ": 1.1, "TQQQ": 0.0, "SGOV": 0.1},
    )

    assert decision.allowed_by_policy is False
    assert "leverage_increased" in decision.blocked_reasons


def test_event_override_cannot_add_tqqq_by_default() -> None:
    decision = _decision(
        original={"QQQ": 0.7, "TQQQ": 0.0, "SGOV": 0.3},
        new={"QQQ": 0.5, "TQQQ": 0.1, "SGOV": 0.4},
    )

    assert decision.allowed_by_policy is False
    assert "risk_asset_weight_increased:TQQQ" in decision.blocked_reasons


def test_event_override_requires_no_lookahead_evidence() -> None:
    decision = _decision()

    assert decision.no_lookahead_evidence["passed"] is True
    assert decision.no_lookahead_evidence["checks"]["event_known_before_review"] is True
    assert decision.no_lookahead_evidence["checks"]["decision_before_effective"] is True


def test_override_decision_rejects_future_event_known_at() -> None:
    decision = evaluate_event_override_decision(
        event_id="future-known",
        event_known_at="2026-01-06",
        review_at="2026-01-05",
        decision_at="2026-01-05",
        event_risk_score=90,
        override_direction="RISK_REDUCTION",
        pending_plan_status="PENDING_REBALANCE",
        effective_at="2026-01-06",
        policy=_policy(),
        original_target_weights={"QQQ": 0.8, "TQQQ": 0.0, "SGOV": 0.2},
        new_target_weights={"QQQ": 0.6, "TQQQ": 0.0, "SGOV": 0.4},
    )

    assert decision.no_lookahead_evidence["passed"] is False
    assert "no_lookahead_evidence_failed" in decision.blocked_reasons


def test_override_trace_records_known_review_decision_effective_dates() -> None:
    dates = pd.bdate_range("2026-01-02", periods=5)
    runtime = _runtime()
    _actual_position_path(
        strategy_id="toy_event_override",
        execution_policy_id="no_rebalance",
        target_weights=_constant_target(dates),
        policy=_no_rebalance_policy(),
        enable_event_override=True,
        event_override_policy=_policy_with_event(dates[1].date().isoformat()),
        event_override_runtime=runtime,
    )

    trace = runtime["event_override_trace"][0]
    assert trace["event_known_at"] == dates[1].date().isoformat()
    assert trace["review_at"] == dates[1].date().isoformat()
    assert trace["decision_at"] == dates[1].date().isoformat()
    assert trace["effective_at"] == dates[2].date().isoformat()


def test_execution_semantics_rebacktest_cli_accepts_event_override_flags(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    event_policy_path = _write_explicit_event_policy(tmp_path, prices_path)
    output_root = tmp_path / "cli_rebacktest"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "execution-semantics-rebacktest",
            "--strategy",
            "limited_adjustment",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            as_of.isoformat(),
            "--output",
            str(output_root),
            "--enable-event-override",
            "--event-override-policy",
            str(event_policy_path),
            "--event-override-mode",
            "event_override_t_plus_1",
            "--emit-pending-plan-ledger",
            "--emit-supersede-log",
            "--emit-event-override-trace",
            "--event-override-survival-matrix-path",
            str(tmp_path / "event_override_survival_matrix.yaml"),
            "--event-override-review-path",
            str(tmp_path / "event_override_review.md"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    strategy_dir = output_root / "limited_adjustment_event_override_v1"
    assert (strategy_dir / "event_override_trace.json").exists()
    assert (strategy_dir / "pending_plan_ledger.csv").exists()
    assert (strategy_dir / "supersede_log.csv").exists()
    assert (strategy_dir / "event_override_summary.json").exists()
    assert (strategy_dir / "no_lookahead_evidence.json").exists()
    assert (output_root / "event_override_gate.json").exists()
    assert (tmp_path / "event_override_survival_matrix.yaml").exists()
    assert (tmp_path / "event_override_review.md").exists()


def _decision(
    *,
    pending_plan_status: str = "PENDING_REBALANCE",
    original: dict[str, float] | None = None,
    new: dict[str, float] | None = None,
) -> EventOverrideDecision:
    return evaluate_event_override_decision(
        event_id="event-1",
        event_known_at="2026-01-02",
        review_at="2026-01-02",
        decision_at="2026-01-02",
        event_risk_score=90,
        override_direction="RISK_REDUCTION",
        pending_plan_status=pending_plan_status,
        effective_at="2026-01-05",
        policy=_policy(),
        original_target_weights=original or {"QQQ": 0.6, "TQQQ": 0.0, "SGOV": 0.4},
        new_target_weights=new or {"QQQ": 0.4, "TQQQ": 0.0, "SGOV": 0.6},
        superseded_plan_id="plan-1",
        new_plan_id="plan-2",
    )


def _pending_plan(status: str) -> dict[str, object]:
    return {
        "plan_id": "plan-1",
        "strategy_id": "limited_adjustment",
        "created_at": "2026-01-02",
        "known_at": "2026-01-02",
        "decision_at": "2026-01-02",
        "intended_effective_at": "2026-01-05",
        "first_executable_date": "2026-01-05",
        "actual_execution_date": "2026-01-05" if status == "EXECUTED" else None,
        "status": status,
        "status_reason": "test",
        "target_weights": {"QQQ": 0.6, "TQQQ": 0.0, "SGOV": 0.4},
        "source_signal_ids": ["signal-1"],
        "source_event_ids": [],
        "policy_hash": "policy-hash",
        "superseded_by_plan_id": None,
        "supersedes_plan_id": None,
    }


def _constant_target(dates: pd.DatetimeIndex) -> pd.DataFrame:
    return pd.DataFrame(
        {"QQQ": [1.0] * len(dates), "TQQQ": [0.0] * len(dates), "SGOV": [0.0] * len(dates)},
        index=dates,
    )


def _no_rebalance_policy() -> dict[str, object]:
    return {
        "execution_policy_id": "no_rebalance",
        "execution_frequency": "no_rebalance",
        "signal_to_execution_lag": 0,
        "minimum_holding_period": 20,
        "drift_threshold": None,
        "validity_period_days": 20,
        "max_turnover_per_period": 1.0,
        "cost_model": {"explicit_cost_bps": 0.0},
    }


def _policy() -> dict[str, object]:
    return {
        "enabled": True,
        "risk_off_override": {
            "enabled": True,
            "min_event_risk_score": 80,
            "max_single_override_weight_delta": 0.20,
            "max_total_override_weight_delta": 0.35,
        },
        "risk_on_override": {"enabled": False},
    }


def _policy_with_event(review_at: str) -> dict[str, object]:
    return {
        **_policy(),
        "research_event_schedule": [
            {
                "event_id": f"test_event_{review_at}",
                "event_known_at": review_at,
                "review_at": review_at,
                "decision_at": review_at,
                "event_risk_score": 90,
                "override_direction": "RISK_REDUCTION",
            }
        ],
    }


def _runtime() -> dict[str, object]:
    return _empty_event_override_runtime(
        strategy_id="toy_event_override",
        mode="event_override_t_plus_1",
        policy_hash="policy-hash",
    )


def _write_explicit_event_policy(tmp_path: Path, prices_path: Path) -> Path:
    dates = list(pd.read_csv(prices_path)["date"].drop_duplicates())
    review_at = str(dates[5])
    path = tmp_path / "event_override_policy.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "event_override_policy": _policy_with_event(review_at),
                "schema_version": "event_override_policy_v1",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path
