from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from ai_trading_system.trading_engine.parameters.parameter_schema import WalkForwardConfig


@dataclass(frozen=True)
class WalkForwardWindow:
    window_id: str
    train_start: date
    train_end: date
    validation_start: date
    validation_end: date

    def to_dict(self) -> dict[str, str]:
        return {
            "window_id": self.window_id,
            "train_start": self.train_start.isoformat(),
            "train_end": self.train_end.isoformat(),
            "validation_start": self.validation_start.isoformat(),
            "validation_end": self.validation_end.isoformat(),
        }


def generate_walk_forward_windows(
    trading_dates: list[date] | tuple[date, ...],
    config: WalkForwardConfig,
) -> tuple[WalkForwardWindow, ...]:
    dates = tuple(sorted(dict.fromkeys(trading_dates)))
    if len(dates) < config.min_history_days:
        return ()
    windows: list[WalkForwardWindow] = []
    start_index = 0
    window_number = 1
    while True:
        train_start_index = start_index
        train_end_index = train_start_index + config.train_window_days - 1
        validation_start_index = train_end_index + 1
        validation_end_index = validation_start_index + config.validation_window_days - 1
        if validation_end_index >= len(dates):
            break
        windows.append(
            WalkForwardWindow(
                window_id=f"wf-{window_number:03d}",
                train_start=dates[train_start_index],
                train_end=dates[train_end_index],
                validation_start=dates[validation_start_index],
                validation_end=dates[validation_end_index],
            )
        )
        window_number += 1
        start_index += config.step_days
    return tuple(windows)
