from __future__ import annotations

import pytest

from ai_trading_system.features.technical import (
    is_above_moving_average,
    moving_average,
    percentage_change,
    relative_strength,
)


def test_moving_average_uses_trailing_window() -> None:
    assert moving_average([1, 2, 3, 4], window=3) == [None, None, 2.0, 3.0]


def test_is_above_moving_average() -> None:
    assert is_above_moving_average([10, 11, 12, 13, 16], window=3) is True


def test_relative_strength() -> None:
    assert relative_strength([10, 12], [5, 4]) == [2.0, 3.0]


def test_percentage_change_rejects_zero_base() -> None:
    with pytest.raises(ValueError, match="previous must not be zero"):
        percentage_change(current=1, previous=0)
