"""Exact arithmetic for a declared, closed, self-financing forward ledger.

This module performs no I/O and grants no source, calendar, DQ, PIT or outcome
access authority. See TRADING-2564_S4_Forward_Accounting_V1.md. In particular,
asset PnL is a caller-supplied monetary gain before the separately listed cost;
it is not a weight, asset return, or independently attested market fact.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Literal


class ForwardAccountingError(ValueError):
    """The declared ledger cannot support the requested arithmetic summary."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class AssetPnl:
    """Monetary PnL before the session's separately listed trading cost."""

    asset_id: str
    amount: Decimal


@dataclass(frozen=True, slots=True)
class SessionLedger:
    """One valuation interval ending on a caller-declared expected session."""

    session: date
    beginning_nav: Decimal
    asset_pnl: tuple[AssetPnl, ...]
    trading_cost: Decimal
    ending_nav: Decimal
    external_flow: Decimal


@dataclass(frozen=True, slots=True)
class AssetContribution:
    asset_id: str
    monetary_pnl: Fraction
    initial_capital_contribution: Fraction


@dataclass(frozen=True, slots=True)
class ForwardAccountingSummary:
    initial_valuation_date: date
    requested_sessions: tuple[date, ...]
    evaluated_sessions: tuple[date, ...]
    initial_nav: Fraction
    final_nav: Fraction
    assets: tuple[AssetContribution, ...]
    total_trading_cost: Fraction
    cost_contribution: Fraction
    net_pnl: Fraction
    net_return: Fraction
    max_drawdown: Fraction
    schema_version: Literal["forward_accounting_summary.v1"] = "forward_accounting_summary.v1"
    evidence_scope: Literal["ARITHMETIC_IDENTITY_ONLY"] = "ARITHMETIC_IDENTITY_ONLY"
    calendar_authority_verified: Literal[False] = False
    data_quality_verified: Literal[False] = False
    real_outcome_access_authorized: Literal[False] = False


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise ForwardAccountingError(code)


def _amount(value: Decimal, code: str) -> Fraction:
    _require(type(value) is Decimal and value.is_finite(), code)
    # Fraction(Decimal) preserves the exact finite input. Decimal arithmetic or
    # division here would depend on ambient precision and can silently round.
    return Fraction(value)


def _asset_id(value: str) -> str:
    _require(
        type(value) is str
        and bool(value)
        and value == value.strip()
        and not any(character.isspace() or ord(character) < 32 for character in value),
        "ASSET_ID_INVALID",
    )
    return value


def summarize_forward_ledger(
    *,
    initial_valuation_date: date,
    expected_sessions: tuple[date, ...],
    expected_assets: tuple[str, ...],
    ledger: tuple[SessionLedger, ...],
) -> ForwardAccountingSummary:
    """Verify an exact monetary ledger and normalize PnL to its initial capital.

    The caller supplies the full expected calendar and asset universe; this
    function never infers either from available rows. All assets, including
    explicit zero PnL assets, must occur once per row. V1 excludes external
    funding, withdrawals and nonpositive NAV. No missing row is filled.

    Contributions sum exactly to the portfolio's compounded net return because
    they use monetary PnL divided by initial NAV, not a sum of asset returns.
    ``max_drawdown`` is a nonnegative loss ratio and includes opening NAV.
    Fractions are exact results; a downstream presentation may round them, but
    rounded display values must not replace the accounting identity.
    """
    _require(type(initial_valuation_date) is date, "INITIAL_DATE_INVALID")
    _require(
        type(expected_sessions) is tuple
        and bool(expected_sessions)
        and all(type(session) is date for session in expected_sessions),
        "EXPECTED_SESSIONS_INVALID",
    )
    _require(
        initial_valuation_date < expected_sessions[0]
        and all(
            left < right
            for left, right in zip(expected_sessions, expected_sessions[1:], strict=False)
        ),
        "EXPECTED_SESSION_ORDER_INVALID",
    )
    _require(type(expected_assets) is tuple and bool(expected_assets), "EXPECTED_ASSETS_INVALID")
    asset_ids = tuple(_asset_id(asset_id) for asset_id in expected_assets)
    _require(len(set(asset_ids)) == len(asset_ids), "EXPECTED_ASSETS_DUPLICATE")
    _require(
        type(ledger) is tuple and len(ledger) == len(expected_sessions), "LEDGER_LENGTH_INVALID"
    )

    amounts = {asset_id: Fraction(0) for asset_id in asset_ids}
    initial_nav = Fraction(0)
    previous_nav = Fraction(0)
    peak_nav = Fraction(0)
    maximum_drawdown = Fraction(0)
    total_cost = Fraction(0)
    for index, (session, row) in enumerate(zip(expected_sessions, ledger, strict=True)):
        _require(type(row) is SessionLedger, "LEDGER_ROW_INVALID")
        _require(type(row.session) is date and row.session == session, "LEDGER_SESSION_MISMATCH")
        beginning = _amount(row.beginning_nav, "BEGINNING_NAV_INVALID")
        ending = _amount(row.ending_nav, "ENDING_NAV_INVALID")
        cost = _amount(row.trading_cost, "TRADING_COST_INVALID")
        external_flow = _amount(row.external_flow, "EXTERNAL_FLOW_INVALID")
        _require(beginning > 0 and ending > 0, "NONPOSITIVE_NAV_UNSUPPORTED")
        _require(cost >= 0, "NEGATIVE_TRADING_COST")
        _require(external_flow == 0, "EXTERNAL_FLOW_UNSUPPORTED")
        _require(type(row.asset_pnl) is tuple, "ASSET_PNL_INVALID")
        session_amounts: dict[str, Fraction] = {}
        for pnl in row.asset_pnl:
            _require(type(pnl) is AssetPnl, "ASSET_PNL_INVALID")
            asset_id = _asset_id(pnl.asset_id)
            _require(asset_id not in session_amounts, "ASSET_PNL_DUPLICATE")
            session_amounts[asset_id] = _amount(pnl.amount, "ASSET_PNL_AMOUNT_INVALID")
        _require(set(session_amounts) == set(asset_ids), "ASSET_UNIVERSE_MISMATCH")
        _require(
            ending == beginning + sum(session_amounts.values(), Fraction(0)) - cost,
            "SESSION_ACCOUNTING_IDENTITY_MISMATCH",
        )
        if index == 0:
            initial_nav = beginning
            # Opening capital is part of the path: a first-session loss cannot
            # establish a new lower initial high-water mark.
            peak_nav = beginning
        else:
            _require(beginning == previous_nav, "NAV_CONTINUITY_MISMATCH")
        previous_nav = ending
        peak_nav = max(peak_nav, ending)
        maximum_drawdown = max(maximum_drawdown, (peak_nav - ending) / peak_nav)
        for asset_id, amount in session_amounts.items():
            amounts[asset_id] += amount
        total_cost += cost

    net_pnl = previous_nav - initial_nav
    _require(
        sum(amounts.values(), Fraction(0)) - total_cost == net_pnl,
        "TOTAL_ACCOUNTING_IDENTITY_MISMATCH",
    )
    assets = tuple(
        AssetContribution(asset_id, amounts[asset_id], amounts[asset_id] / initial_nav)
        for asset_id in sorted(asset_ids)
    )
    net_return = net_pnl / initial_nav
    cost_contribution = -total_cost / initial_nav
    _require(
        sum((asset.initial_capital_contribution for asset in assets), Fraction(0))
        + cost_contribution
        == net_return,
        "CONTRIBUTION_IDENTITY_MISMATCH",
    )
    return ForwardAccountingSummary(
        initial_valuation_date=initial_valuation_date,
        requested_sessions=expected_sessions,
        evaluated_sessions=tuple(row.session for row in ledger),
        initial_nav=initial_nav,
        final_nav=previous_nav,
        assets=assets,
        total_trading_cost=total_cost,
        cost_contribution=cost_contribution,
        net_pnl=net_pnl,
        net_return=net_return,
        max_drawdown=maximum_drawdown,
    )
