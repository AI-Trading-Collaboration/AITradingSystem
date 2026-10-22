"""只通过 forward_ledger.read_ledger 核链并读取前瞻账本，画累计 QQQ 和三条敞口。

不读市场数据，不判、不追加账本，不接入周五流程；同样的行生成同样的 SVG。
账本图只描述已登记的行，不授权生产或券商动作。
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Annotated, Any

import pandas as pd
import typer

from ai_trading_system.research_probes import forward_ledger, plot_run

FIGURE = "ledger.svg"
DEFAULT_OUT = forward_ledger.DEFAULT_LEDGER_DIR / "figures"
LINES = (
    ("exp_daily_held", "日报持有敞口", "#d62728"),
    ("exp_fixed", "固定 0.8", "#8c8c8c"),
    ("exp_overlay", "覆盖 F0001 fast_slow", "#1f77b4"),
)
app = typer.Typer(help="前瞻账本图：只读、核链、不判、不追加。")


def cumulative(rows: Sequence[Mapping[str, Any]]) -> pd.Series:
    level = 1.0
    values = []
    for row in rows:
        value = row["qqq_tr"]
        if value is None:
            values.append(math.nan)
        else:
            level *= 1 + float(value)
            values.append(level)
    return pd.Series(values, index=pd.DatetimeIndex([r["date"] for r in rows]), dtype=float)


def _dates(svg: plot_run.Svg, panel: plot_run.Panel, index: pd.DatetimeIndex) -> None:
    # At most twelve date labels is a display constraint, not an interpretation threshold.
    positions = sorted(
        {
            round(i * (len(index) - 1) / max(1, min(12, len(index)) - 1))
            for i in range(min(12, len(index)))
        }
    )
    for i in positions:
        svg.text(
            panel.px(index[i]),
            panel.y + panel.height + 18,
            index[i].strftime("%m-%d"),
            size=10,
            anchor="middle",
        )


def render(rows: Sequence[Mapping[str, Any]]) -> bytes:
    if not rows:
        raise ValueError("the ledger is empty")
    level = cumulative(rows)
    index = pd.DatetimeIndex(level.index)
    first, last = index[0] - pd.Timedelta(days=1), index[-1] + pd.Timedelta(days=1)
    width = max(900, 18 * len(rows) + 160)
    finite = [1.0, *[float(v) for v in level if math.isfinite(v)]]
    low, high = min(finite), max(finite)
    padding = max(high - low, 0.01) * 0.12
    top = plot_run.Panel(
        80, 100, width - 140, 250, first, last, low - padding, high + padding, False
    )
    bottom = plot_run.Panel(80, 425, width - 140, 140, first, last, -0.05, 1.08, False)
    svg = plot_run.Svg(width, 640)
    svg.text(80, 30, "前瞻账本：QQQ 累计总回报和三条敞口", size=16, weight="bold")
    plot_run.legend(
        svg, 80, 75, [("QQQ 累计总回报（起点前一天 = 1，缺的日子断开）", "#222", 1.5, "line")]
    )
    plot_run.y_ticks(svg, top)
    with_start = pd.concat([pd.Series([1.0], index=pd.DatetimeIndex([first])), level])
    plot_run.line(svg, top, with_start, "#222", 1.5, "price")
    _dates(svg, top, index)
    plot_run.plot_area(svg, top)
    plot_run.legend(svg, 80, 405, [(text, color, 1.3, "line") for _, text, color in LINES])
    plot_run.y_ticks(svg, bottom)
    for key, _, color in LINES:
        series = pd.Series([row[key] for row in rows], index=index, dtype=float)
        plot_run.step_line(svg, bottom, series, color, "exposure")
    _dates(svg, bottom, index)
    plot_run.plot_area(svg, bottom)
    prev = str(rows[-1].get("prev_sha256") or "")[:12]
    svg.text(
        80,
        615,
        f"{len(rows)} 行（{rows[0]['date']}..{rows[-1]['date']}，"
        f"回填到 {forward_ledger.FORWARD_START} 之前）；"
        f"最后一行 prev_sha256 {prev}；只读账本，不判。",
        size=10,
    )
    return svg.render()


def draw(ledger_dir: Path, out_dir: Path) -> tuple[Path, bool]:
    data = render(forward_ledger.read_ledger(ledger_dir))
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / FIGURE
    if path.exists() and path.read_bytes() == data:
        return path, False
    path.write_bytes(data)
    return path, True


@app.callback()
def _callback() -> None:
    """前瞻账本只读图。"""


@app.command("draw")
def draw_command(
    out: Annotated[Path, typer.Option()] = DEFAULT_OUT,
    ledger_dir: Annotated[Path, typer.Option()] = forward_ledger.DEFAULT_LEDGER_DIR,
) -> None:
    try:
        path, changed = draw(ledger_dir, out)
    except (forward_ledger.ForwardLedgerError, ValueError, OSError) as exc:
        typer.echo(f"forward_figure: {str(exc).splitlines()[0]}")
        raise typer.Exit(code=1) from None
    typer.echo(f"forward_figure: {'已更新' if changed else '没有变化'} {path}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
