"""Metadata-only command interface for private judgment checks and imports."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.config import PROJECT_ROOT, load_universe, load_watchlist
from ai_trading_system.paths import state_dir
from ai_trading_system.research_judgments.importer import import_export
from ai_trading_system.research_judgments.methods import load_methods

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


if __name__ == "__main__":
    app()
