"""Metadata-only command interface for private judgment checks and imports."""

from __future__ import annotations

import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.config import PROJECT_ROOT, load_universe, load_watchlist
from ai_trading_system.paths import state_dir
from ai_trading_system.research_judgments.importer import import_export
from ai_trading_system.research_judgments.methods import load_methods
from ai_trading_system.research_judgments.settle import load_settle_params
from ai_trading_system.research_judgments.settlements import settle

app = typer.Typer(add_completion=False)


def _run(file: Path, root: Path, *, dry_run: bool) -> None:
    universe = load_universe()
    tickers = {item.ticker for item in load_watchlist().items}
    tickers.update(universe.market.benchmarks)
    for group in universe.ai_chain.values():
        tickers.update(group)
    result = import_export(
        file,
        data_dir=state_dir(root, "data", "external", "judgments"),
        known_tickers=tickers,
        method_ids=load_methods().ids(),
        now=datetime.now(UTC),
        dry_run=dry_run,
    )
    for issue in result.issues:
        typer.echo(f"issue {issue.code} id={issue.id or '-'} field={issue.field or '-'}")
    if result.status == "rejected":
        typer.echo("rejected: nothing was written")
        raise typer.Exit(1)
    if result.status == "unchanged" and not dry_run:
        typer.echo(f"unchanged seq={result.seq}")
        return
    counts = result.counts
    prefix = "ok" if dry_run else f"imported seq={result.seq}"
    typer.echo(
        f"{prefix} rows={counts['rows']} research={counts['research']}"
        f" learning={counts['learning']}"
    )


@app.command("check")
def check_command(file: Path, root: Annotated[Path, typer.Option("--root")] = PROJECT_ROOT) -> None:
    _run(file, root, dry_run=True)


@app.command("import")
def import_command(
    file: Path, root: Annotated[Path, typer.Option("--root")] = PROJECT_ROOT
) -> None:
    _run(file, root, dry_run=False)


@app.command("settle")
def settle_command(
    root: Annotated[Path, typer.Option("--root")] = PROJECT_ROOT,
    asof: Annotated[str | None, typer.Option("--asof")] = None,
    prices: Annotated[Path | None, typer.Option("--prices")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
) -> None:
    now = datetime.now(UTC)
    selected_asof = now.date()
    if asof is not None:
        try:
            if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", asof):
                raise ValueError("invalid date format")
            selected_asof = date.fromisoformat(asof)
        except ValueError as exc:
            raise typer.BadParameter("expected YYYY-MM-DD", param_hint="--asof") from exc
    result = settle(
        data_dir=state_dir(root, "data", "external", "judgments"),
        prices_path=prices
        if prices is not None
        else state_dir(PROJECT_ROOT, "data", "raw", "prices_daily.csv"),
        params=load_settle_params(),
        asof=selected_asof,
        now=now,
        dry_run=dry_run,
    )
    for issue in result.issues:
        typer.echo(f"issue {issue.code} id={issue.id or '-'} field={issue.field or '-'}")
    if result.status == "rejected":
        typer.echo("rejected: nothing was written")
        raise typer.Exit(1)
    if result.status == "unchanged":
        typer.echo(f"unchanged seq={result.seq}")
        return
    prefix = "ok" if dry_run else f"settled seq={result.seq}"
    counts = result.counts
    typer.echo(
        f"{prefix} asof={selected_asof.isoformat()} settled={counts['settled']}"
        f" pending={counts['pending']} void={counts['void']} data_gap={counts['data_gap']}"
        f" conflicts={counts['conflicts']}"
    )


if __name__ == "__main__":
    app()
