"""GOV-008 P1: classify every ``src/ai_trading_system`` module by static reachability.

Classes (first match wins):
  DEV_MACHINERY       platform/architecture: fence, leases, DEVX-015 runtime, hash authorities.
  ETF_RETIRE          etf_portfolio package and its CLI: owner retired the ETF dynamic v3 steps
  ATLAS_RETIRE        atlas package: owner retired Atlas (2026-10-10).
                      (GOV-008 P1 review, 2026-10-10). Deleted only after the two decouplings.
  LIVE_FIVE_LINES     import closure of the five research lines the owner keeps.
  LIVE_DAILY          import closure of the commands in the daily_trading_day cadence.
  LIVE_OTHER_SCHEDULE closure of the other scheduled cadences.
  UNASSIGNED          not reachable from the above: candidate FROZEN / DEAD, needs line owner.

Static reachability over-approximates (one import counts) and misses dynamic ``import_module``
use and commands the owner runs by hand, so UNASSIGNED is a candidate list, not a delete list.

usage:  PYTHONPATH=src python tools/gov008/code_usage.py --repo . \
        --out docs/requirements/GOV-008_lists
"""

from __future__ import annotations

import argparse
import collections
import csv
import json
import re
import shlex
import subprocess
from pathlib import Path

import click
import typer
import yaml
from modgraph import (
    PKG,
    build_src_graph,
    closure,
    count_lines,
    is_machinery_module,
    raw_imports,
    src_modules,
)

ETF_PREFIXES = (f"{PKG}.etf_portfolio", f"{PKG}.interfaces.cli.etf_portfolio")
# Owner decision 2026-10-10 (GOV-008 P1 review): these five research lines are kept. Seeds are an
# approximation by module name; the owner confirms line ownership of UNASSIGNED modules.
FIVE_LINE_PATTERN = re.compile(
    r"(composer|equal_risk|simple_baseline|layer1|layer2|qqq_options|prospective|named_quality|"
    r"named_data_quality|research_outcome|first_layer|growth_tilt)"
)


def is_etf(name: str) -> bool:
    return any(name == p or name.startswith(p + ".") for p in ETF_PREFIXES)


def is_atlas(name: str) -> bool:
    """Atlas is retired (owner decision 2026-10-10, GOV-008 P1 review)."""
    return name == f"{PKG}.atlas" or name.startswith(f"{PKG}.atlas.")


def scheduled_seeds(repo: Path, modules: dict[str, Path]) -> tuple[dict[str, set[str]], list[str]]:
    """Cadence -> implementing src modules, resolved from config/scheduled_tasks.yaml."""
    from ai_trading_system.cli import app  # imported lazily: needs PYTHONPATH=src

    root = typer.main.get_command(app)
    cfg = yaml.safe_load((repo / "config/scheduled_tasks.yaml").read_text(encoding="utf-8"))
    seeds: dict[str, set[str]] = collections.defaultdict(set)
    unresolved: list[str] = []
    for cadence, body in cfg["cadences"].items():
        for task in body.get("tasks", []):
            command = task.get("command")
            if not command:
                continue
            tokens = shlex.split(command.replace("{", "X").replace("}", "X"))
            if tokens and tokens[0] == "python":
                script = next((t for t in tokens[1:] if t.endswith(".py")), None)
                path = repo / script if script else None
                if path and path.exists():
                    for dotted in raw_imports(path):
                        if dotted in modules:
                            seeds[cadence].add(dotted)
                else:
                    unresolved.append(command)
                continue
            if not tokens or tokens[0] != "aits":
                unresolved.append(command)
                continue
            node: click.Command = root
            for token in tokens[1:]:
                if not isinstance(node, click.Group) or token.startswith("-"):
                    break
                child = node.commands.get(token)
                if child is None:
                    break
                node = child
            callback = getattr(node, "callback", None)
            module = getattr(getattr(callback, "__wrapped__", callback), "__module__", None)
            if module in modules:
                seeds[cadence].add(module)
            else:
                unresolved.append(command)
    return seeds, unresolved


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    repo: Path = args.repo.resolve()
    out: Path = (repo / args.out) if not args.out.is_absolute() else args.out
    out.mkdir(parents=True, exist_ok=True)

    modules = src_modules(repo)
    loc = {name: count_lines(path) for name, path in modules.items()}
    graph = build_src_graph(modules)
    seeds, unresolved = scheduled_seeds(repo, modules)

    daily = closure(graph, seeds.get("daily_trading_day", set()))
    other_seeds = set().union(*[v for k, v in seeds.items() if k != "daily_trading_day"])
    other = closure(graph, other_seeds)
    five_seeds = [
        name
        for name in modules
        if not is_atlas(name)
        and (FIVE_LINE_PATTERN.search(name.split(".")[-1]) or ".qqq_options_research" in name)
    ]
    five = closure(graph, five_seeds)

    rows = []
    for name in sorted(modules):
        if is_machinery_module(name):
            cls = "DEV_MACHINERY"
        elif is_etf(name):
            cls = "CONFLICT_ETF_NEEDED_BY_FIVE_LINES" if name in five else "ETF_RETIRE"
        elif is_atlas(name):
            cls = "CONFLICT_ATLAS_NEEDED_BY_FIVE_LINES" if name in five else "ATLAS_RETIRE"
        elif name in five:
            cls = "LIVE_FIVE_LINES"
        elif name in daily:
            cls = "LIVE_DAILY"
        elif name in other:
            cls = "LIVE_OTHER_SCHEDULE"
        else:
            cls = "UNASSIGNED"
        rows.append(
            {
                "module": name,
                "path": modules[name].relative_to(repo).as_posix(),
                "loc": loc[name],
                "class": cls,
                "in_daily": int(name in daily),
                "in_other_schedule": int(name in other),
                "in_five_lines": int(name in five),
            }
        )
    with (out / "code_usage.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    # Decoupling worklists: who still binds to the machinery / to the ETF package.
    machinery_work = []
    etf_work = []
    for name in sorted(modules):
        if is_machinery_module(name):
            continue
        direct = sorted(graph[name])
        mach = [d for d in direct if is_machinery_module(d)]
        etf = [d for d in direct if is_etf(d)]
        if mach:
            machinery_work.append(
                {
                    "module": name,
                    "loc": loc[name],
                    "in_five_lines": int(name in five),
                    "in_daily": int(name in daily),
                    "direct_machinery_imports": ";".join(m.removeprefix(PKG + ".") for m in mach),
                }
            )
        if etf and not is_etf(name):
            etf_work.append(
                {
                    "module": name,
                    "loc": loc[name],
                    "in_daily": int(name in daily),
                    "direct_etf_imports": len(etf),
                    "examples": ";".join(e.removeprefix(PKG + ".") for e in etf[:3]),
                }
            )
    for fname, data in (
        ("decoupling_machinery_worklist.csv", machinery_work),
        ("decoupling_etf_worklist.csv", etf_work),
    ):
        with (out / fname).open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle, fieldnames=list(data[0]) if data else ["module"], lineterminator="\n"
            )
            writer.writeheader()
            writer.writerows(data)

    total = sum(loc.values())
    by_class: dict[str, dict[str, int]] = collections.defaultdict(lambda: {"modules": 0, "loc": 0})
    for row in rows:
        by_class[row["class"]]["modules"] += 1
        by_class[row["class"]]["loc"] += int(row["loc"])
    head = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    summary = {
        "source_commit": head,
        "total_modules": len(modules),
        "total_loc": total,
        "by_class": dict(sorted(by_class.items())),
        "scheduled_seed_modules": {k: len(v) for k, v in sorted(seeds.items())},
        "unresolved_scheduled_commands": unresolved,
        "five_line_seed_modules": len(five_seeds),
        "machinery_worklist_files": len(machinery_work),
        "machinery_worklist_loc": sum(int(r["loc"]) for r in machinery_work),
        "etf_worklist_files": len(etf_work),
        "caveats": [
            "static import closure over-approximates reachability",
            "dynamic import_module use and hand-run aits research commands are not counted",
            "five-line seeds are a module-name approximation until the owner confirms ownership",
        ],
    }
    (out / "code_usage_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=1) + "\n", encoding="utf-8", newline="\n"
    )
    print(json.dumps(summary["by_class"], ensure_ascii=False, indent=1))
    print(
        "machinery worklist:",
        len(machinery_work),
        "files;",
        "ETF worklist:",
        len(etf_work),
        "files",
    )


if __name__ == "__main__":
    main()
