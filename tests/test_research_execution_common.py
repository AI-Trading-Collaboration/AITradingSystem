from __future__ import annotations

from collections.abc import Callable
from datetime import date

import pytest
import typer

from ai_trading_system.cli_commands.research_execution_common import (
    as_of_kwargs,
    date_range_kwargs,
)
from ai_trading_system.execution_semantics import DEFAULT_AI_REGIME_BACKTEST_START


def test_date_range_kwargs_preserves_execution_builder_contract() -> None:
    assert date_range_kwargs("2026-07-11", "2021-02-22", "2026-06-30") == {
        "as_of_date": date(2026, 7, 11),
        "start_date": date(2021, 2, 22),
        "end_date": date(2026, 6, 30),
    }


def test_date_range_kwargs_uses_ai_regime_default_only_for_missing_start() -> None:
    assert date_range_kwargs(None, None, None) == {
        "as_of_date": None,
        "start_date": DEFAULT_AI_REGIME_BACKTEST_START,
        "end_date": None,
    }


def test_as_of_kwargs_preserves_execution_builder_contract() -> None:
    assert as_of_kwargs("2026-07-11") == {"as_of_date": date(2026, 7, 11)}
    assert as_of_kwargs(None) == {"as_of_date": None}


@pytest.mark.parametrize(
    "adapter",
    [as_of_kwargs, lambda value: date_range_kwargs(value, None, None)],
)
def test_date_adapters_reject_non_iso_dates(
    adapter: Callable[[str | None], dict[str, date | None]],
) -> None:
    with pytest.raises(typer.BadParameter, match="YYYY-MM-DD"):
        adapter("2026/07/11")
