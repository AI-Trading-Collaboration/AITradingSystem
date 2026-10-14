"""Whether heavy pi work may start now: the daily-run windows (DEVX-031).

``agent_config.json`` lists the daily-run windows in ``daily_windows_jst``. Inside a window no pi
run starts and no gate or publish runs, so the scheduled daily run has the machine. A window with a
``label`` (the scheduler's window name, PRIMARY or RESCUE) opens early once that day's run of that
window wrote its summary under ``daily_scheduler_dir``: ``<YYYY-MM-DD>_<label>[_n].summary.json``,
written by scripts/ops/daily_scheduler_summary.py at the end of the run, no earlier than the
window's start (owner_decision:DEVX-031:2026-10-14:parallel_pi_lanes). Without a summary, or
without the directory, the window stays closed until its end.

Standard library only, so run-brief.ps1 and lane.py can call it with ``python -I``. Tokyo has no
daylight saving time, so JST is a fixed +09:00 offset (Windows Python has no tz database).

usage:
  python -I scripts/agent/daily_window.py --config scripts/agent/agent_config.json [--now ISO]
  exit 0: open; exit 3: inside a closed window; exit 2: bad config or --now. Prints one JSON line.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any

JST = dt.timezone(dt.timedelta(hours=9), "JST")
BLOCKED_EXIT = 3


def _at(day: dt.datetime, hhmm: str) -> dt.datetime:
    hours, minutes = (int(part) for part in hhmm.split(":"))
    return day.replace(hour=hours, minute=minutes, second=0, microsecond=0)


def _summary(directory: Path, day: str, label: str, start: dt.datetime) -> Path | None:
    candidates = [
        *directory.glob(f"{day}_{label}.summary.json"),
        *directory.glob(f"{day}_{label}_*.summary.json"),
    ]
    for path in sorted(candidates):
        written = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.UTC)
        if written >= start:
            return path
    return None


def window_state(config: dict[str, Any], now: dt.datetime) -> dict[str, Any]:
    """{"blocked": bool, "now": ..., and for a window: "window", "released_by" or "reason"}."""
    if now.tzinfo is None:
        raise ValueError("now must carry a UTC offset")
    jst = now.astimezone(JST)
    state: dict[str, Any] = {"blocked": False, "now": jst.strftime("%Y-%m-%d %H:%M %z")}
    for window in config.get("daily_windows_jst") or []:
        start = _at(jst, str(window["start"]))
        end = _at(jst, str(window["end"]))
        if not start <= jst <= end:
            continue
        state["window"] = f"{window['start']}-{window['end']} JST"
        label = window.get("label")
        directory = config.get("daily_scheduler_dir")
        if label and directory and Path(directory).is_dir():
            found = _summary(Path(directory), jst.strftime("%Y-%m-%d"), str(label), start)
            if found is not None:
                state["released_by"] = str(found)
                return state
            state["reason"] = f"no {label} summary of today written since {window['start']} JST"
        else:
            state["reason"] = "the window has no label or summary directory to release it early"
        state["blocked"] = True
        return state
    return state


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--now", help="ISO time with offset (tests); default: the current time")
    args = parser.parse_args()
    try:
        config = json.loads(args.config.read_text(encoding="utf-8-sig"))
        now = dt.datetime.fromisoformat(args.now) if args.now else dt.datetime.now(dt.UTC)
        state = window_state(config, now)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(state, ensure_ascii=False))
    return BLOCKED_EXIT if state["blocked"] else 0


if __name__ == "__main__":
    sys.exit(main())
