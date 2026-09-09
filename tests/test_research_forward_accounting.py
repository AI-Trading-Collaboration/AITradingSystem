"""Synthetic money/calendar fixtures only; no cache, DQ or observed outcome reads."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime
from decimal import Decimal, Inexact, Rounded, localcontext
from fractions import Fraction
from typing import Any

import pytest

from ai_trading_system.research_forward_accounting import (
    AssetPnl,
    ForwardAccountingError,
    SessionLedger,
    summarize_forward_ledger,
)

START = date(2000, 1, 3)
SESSIONS = (date(2000, 1, 4), date(2000, 1, 5))
ASSETS = ("QQQ", "SGOV")


def row(
    session: date,
    beginning: str,
    qqq: str,
    sgov: str,
    ending: str,
    cost: str = "0",
) -> SessionLedger:
    return SessionLedger(
        session,
        Decimal(beginning),
        (AssetPnl("QQQ", Decimal(qqq)), AssetPnl("SGOV", Decimal(sgov))),
        Decimal(cost),
        Decimal(ending),
        Decimal("0"),
    )


def summarize(rows: tuple[SessionLedger, ...], **changes: Any) -> Any:
    request: dict[str, Any] = dict(
        initial_valuation_date=START,
        expected_sessions=SESSIONS[: len(rows)],
        expected_assets=ASSETS,
        ledger=rows,
    )
    request.update(changes)
    return summarize_forward_ledger(**request)


def test_compound_money_contributions_reconcile_and_do_not_sum_asset_returns() -> None:
    result = summarize(
        (
            row(SESSIONS[0], "100", "9", "2", "110", "1"),
            row(SESSIONS[1], "110", "10", "3", "121", "2"),
        )
    )
    contributions = {item.asset_id: item.initial_capital_contribution for item in result.assets}
    assert contributions == {"QQQ": Fraction(19, 100), "SGOV": Fraction(5, 100)}
    assert result.cost_contribution == Fraction(-3, 100)
    assert result.net_return == Fraction(21, 100)
    assert result.net_return != Fraction(1, 10) + Fraction(1, 10)
    assert sum(contributions.values()) + result.cost_contribution == result.net_return
    assert result.net_pnl == 21
    assert result.final_nav == 121
    assert result.max_drawdown == 0


def test_drawdown_contains_opening_nav_and_reports_positive_loss_ratio() -> None:
    result = summarize(
        (row(SESSIONS[0], "100", "-20", "0", "80"), row(SESSIONS[1], "80", "10", "0", "90"))
    )
    assert result.max_drawdown == Fraction(1, 5)
    assert result.net_return == Fraction(-1, 10)


def test_drawdown_uses_new_high_water_mark_and_net_costs() -> None:
    result = summarize(
        (
            row(SESSIONS[0], "100", "25", "0", "125"),
            row(SESSIONS[1], "125", "-20", "0", "100", "5"),
        )
    )
    assert result.max_drawdown == Fraction(1, 5)
    assert result.net_return == 0


@pytest.mark.parametrize("precision", [1, 2, 28, 60])
def test_exact_recurring_ratios_ignore_decimal_precision_and_traps(precision: int) -> None:
    with localcontext() as context:
        context.prec = precision
        context.traps[Inexact] = True
        context.traps[Rounded] = True
        result = summarize((row(SESSIONS[0], "3", "1", "1", "4", "1"),))
    assert result.net_return == Fraction(1, 3)
    assert result.cost_contribution == Fraction(-1, 3)
    assert sum(item.initial_capital_contribution for item in result.assets) == Fraction(2, 3)


def test_large_capital_and_tiny_pnl_are_not_rounded_away() -> None:
    with localcontext() as context:
        context.prec = 2
        result = summarize(
            (
                row(
                    SESSIONS[0],
                    "1000000000000000000000000000000",
                    "0.000001",
                    "0",
                    "1000000000000000000000000000000.000001",
                ),
            )
        )
    assert result.net_pnl == Fraction(1, 1_000_000)
    assert result.net_return == Fraction(1, 10**36)


def test_tiny_unaccounted_pnl_is_rejected_even_with_extreme_decimal_context() -> None:
    with localcontext() as context:
        context.prec = 1
        context.Emax = 1
        context.Emin = -1
        context.traps[Inexact] = True
        context.traps[Rounded] = True
        with pytest.raises(ForwardAccountingError, match="SESSION_ACCOUNTING_IDENTITY_MISMATCH"):
            summarize(
                (
                    row(
                        SESSIONS[0],
                        "1000000000000000000000000000000",
                        "0.000001",
                        "0",
                        "1000000000000000000000000000000",
                    ),
                )
            )


def test_later_peak_can_produce_largest_drawdown_despite_final_recovery() -> None:
    sessions = (*SESSIONS, date(2000, 1, 6), date(2000, 1, 7))
    result = summarize(
        (
            row(sessions[0], "100", "-20", "0", "80"),
            row(sessions[1], "80", "80", "0", "160"),
            row(sessions[2], "160", "-80", "0", "80"),
            row(sessions[3], "80", "120", "0", "200"),
        ),
        expected_sessions=sessions,
    )
    assert result.max_drawdown == Fraction(1, 2)
    assert result.net_return == 1


def test_asset_order_has_no_effect_and_zero_pnl_assets_are_explicit() -> None:
    original = row(SESSIONS[0], "100", "-1", "0", "99")
    assert summarize((original,)) == summarize(
        (replace(original, asset_pnl=original.asset_pnl[::-1]),), expected_assets=ASSETS[::-1]
    )
    assert summarize((original,)).assets[1].monetary_pnl == 0


def test_cash_asset_pnl_is_not_inferred_from_weight_or_a_rate() -> None:
    result = summarize((row(SESSIONS[0], "100", "0", "2", "102"),))
    assert result.assets[1].asset_id == "SGOV"
    assert result.assets[1].initial_capital_contribution == Fraction(1, 50)
    assert not hasattr(result, "cash_drag")


def test_result_is_immutable_and_has_no_calendar_dq_or_access_authority() -> None:
    result = summarize((row(SESSIONS[0], "100", "0", "0", "100"),))
    assert result.requested_sessions == result.evaluated_sessions == SESSIONS[:1]
    assert result.initial_valuation_date == START
    assert result.evidence_scope == "ARITHMETIC_IDENTITY_ONLY"
    assert result.calendar_authority_verified is False
    assert result.data_quality_verified is False
    assert result.real_outcome_access_authorized is False
    with pytest.raises(FrozenInstanceError):
        result.net_return = Fraction(1)


@pytest.mark.parametrize(
    ("field", "bad_value", "code"),
    [
        ("beginning_nav", Decimal("NaN"), "BEGINNING_NAV_INVALID"),
        ("ending_nav", Decimal("Infinity"), "ENDING_NAV_INVALID"),
        ("trading_cost", Decimal("-Infinity"), "TRADING_COST_INVALID"),
        ("external_flow", Decimal("sNaN"), "EXTERNAL_FLOW_INVALID"),
        ("beginning_nav", 100, "BEGINNING_NAV_INVALID"),
        ("ending_nav", 100.0, "ENDING_NAV_INVALID"),
        ("trading_cost", "0", "TRADING_COST_INVALID"),
        ("external_flow", False, "EXTERNAL_FLOW_INVALID"),
        ("beginning_nav", Decimal("0"), "NONPOSITIVE_NAV_UNSUPPORTED"),
        ("ending_nav", Decimal("-1"), "NONPOSITIVE_NAV_UNSUPPORTED"),
        ("trading_cost", Decimal("-1"), "NEGATIVE_TRADING_COST"),
        ("external_flow", Decimal("1"), "EXTERNAL_FLOW_UNSUPPORTED"),
        ("external_flow", Decimal("-1"), "EXTERNAL_FLOW_UNSUPPORTED"),
        ("ending_nav", Decimal("101"), "SESSION_ACCOUNTING_IDENTITY_MISMATCH"),
        ("session", datetime(2000, 1, 4), "LEDGER_SESSION_MISMATCH"),
        ("session", SESSIONS[1], "LEDGER_SESSION_MISMATCH"),
        ("asset_pnl", [], "ASSET_PNL_INVALID"),
        ("asset_pnl", ("QQQ",), "ASSET_PNL_INVALID"),
        ("asset_pnl", (AssetPnl("QQQ", Decimal("0")),), "ASSET_UNIVERSE_MISMATCH"),
        (
            "asset_pnl",
            (AssetPnl("QQQ", Decimal("0")), AssetPnl("QQQ", Decimal("0"))),
            "ASSET_PNL_DUPLICATE",
        ),
        (
            "asset_pnl",
            (AssetPnl("QQQ", Decimal("0")), AssetPnl("SHY", Decimal("0"))),
            "ASSET_UNIVERSE_MISMATCH",
        ),
        (
            "asset_pnl",
            (AssetPnl("QQQ", Decimal("NaN")), AssetPnl("SGOV", Decimal("0"))),
            "ASSET_PNL_AMOUNT_INVALID",
        ),
        (
            "asset_pnl",
            (AssetPnl("QQQ", 0.0), AssetPnl("SGOV", Decimal("0"))),
            "ASSET_PNL_AMOUNT_INVALID",
        ),
    ],
)
def test_invalid_session_ledger_is_rejected(field: str, bad_value: Any, code: str) -> None:
    valid = row(SESSIONS[0], "100", "0", "0", "100")
    with pytest.raises(ForwardAccountingError, match=code) as caught:
        summarize((replace(valid, **{field: bad_value}),))
    assert caught.value.code == code


@pytest.mark.parametrize(
    ("changes", "code"),
    [
        ({"initial_valuation_date": datetime(2000, 1, 3)}, "INITIAL_DATE_INVALID"),
        ({"initial_valuation_date": SESSIONS[0]}, "EXPECTED_SESSION_ORDER_INVALID"),
        ({"expected_sessions": ()}, "EXPECTED_SESSIONS_INVALID"),
        ({"expected_sessions": list(SESSIONS)}, "EXPECTED_SESSIONS_INVALID"),
        ({"expected_sessions": (datetime(2000, 1, 4),)}, "EXPECTED_SESSIONS_INVALID"),
        ({"expected_sessions": (SESSIONS[0], SESSIONS[0])}, "EXPECTED_SESSION_ORDER_INVALID"),
        ({"expected_sessions": SESSIONS[::-1]}, "EXPECTED_SESSION_ORDER_INVALID"),
        ({"expected_sessions": SESSIONS}, "LEDGER_LENGTH_INVALID"),
        ({"expected_assets": ()}, "EXPECTED_ASSETS_INVALID"),
        ({"expected_assets": list(ASSETS)}, "EXPECTED_ASSETS_INVALID"),
        ({"expected_assets": ("QQQ", "QQQ")}, "EXPECTED_ASSETS_DUPLICATE"),
        ({"expected_assets": ("", "SGOV")}, "ASSET_ID_INVALID"),
        ({"expected_assets": ("QQQ ", "SGOV")}, "ASSET_ID_INVALID"),
        ({"expected_assets": ("Q\x00QQ", "SGOV")}, "ASSET_ID_INVALID"),
        ({"expected_assets": (None, "SGOV")}, "ASSET_ID_INVALID"),
        ({"ledger": []}, "LEDGER_LENGTH_INVALID"),
        ({"ledger": (None,)}, "LEDGER_ROW_INVALID"),
    ],
)
def test_declared_scope_is_strict(changes: dict[str, Any], code: str) -> None:
    with pytest.raises(ForwardAccountingError, match=code):
        summarize((row(SESSIONS[0], "100", "0", "0", "100"),), **changes)


def test_balanced_sessions_with_discontinuous_capital_are_rejected() -> None:
    with pytest.raises(ForwardAccountingError, match="NAV_CONTINUITY_MISMATCH"):
        summarize(
            (row(SESSIONS[0], "100", "0", "0", "100"), row(SESSIONS[1], "101", "0", "0", "101"))
        )


def test_already_net_pnl_and_separately_charged_cost_fail_identity() -> None:
    with pytest.raises(ForwardAccountingError, match="SESSION_ACCOUNTING_IDENTITY_MISMATCH"):
        summarize((row(SESSIONS[0], "100", "9", "0", "109", "1"),))


def test_reordered_or_duplicate_actual_sessions_are_not_silently_sorted() -> None:
    valid = (row(SESSIONS[0], "100", "0", "0", "100"), row(SESSIONS[1], "100", "0", "0", "100"))
    for ledger in (valid[::-1], (valid[0], valid[0])):
        with pytest.raises(ForwardAccountingError, match="LEDGER_SESSION_MISMATCH"):
            summarize(ledger)
