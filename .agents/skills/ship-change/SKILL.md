---
name: ship-change
description: Make and publish any change to the AITradingSystem repository (code, config, tests, docs, task records). Use for every implementation, bug fix, refactor, data-pipeline, scoring, backtest, report, or documentation change that should reach main.
---

# Ship a change

The repository rules are in `AGENTS.md`; this skill is the step order. Use the project venv interpreter
(`.venv/Scripts/python.exe` on Windows) for every Python command.

## 1. Before editing

1. `git status` must be clean apart from the paths in `tree_clean_exclusions` of `config/gov008_ship.yaml`.
   Never open, stage, or modify those paths; exclude them from repository-wide `git status`/`git diff` with an
   exact literal pathspec, for example `git status -- . ':(exclude,literal)<path>'`.
2. Start from the current `main` on a task branch: `git checkout -b <agent>/<task-id>-<topic> main`.
3. A change to behavior, scoring, data pipelines, backtests, or reports needs a task record first
   (see the `task-records` skill). Trivial housekeeping does not.

## 2. While editing

- Run focused tests for what you touched:
  `python -m pytest <a few test files or one directory> -n 16 --dist loadfile -q`.
  Never pass hundreds of test file paths; pass `tests` or a directory and select with markers.
- Changing data flow, cache schemas, report outputs, quality gates, scoring, backtests, or regime
  interpretation: update `docs/system_flow.md` sections 1-6 in the same change. Never edit its section 7
  appendix or the older sections of `docs/artifact_catalog.md`: research modules check text in them.
- Never weaken a test, an invariant under `tests/invariants/`, or `config/gov008_ship.yaml` to make a
  gate pass.

## 3. Commit

- Stage only files that belong to the change. Commit message: what changed and why; add an
  `Owner-Decision: owner_decision:<task>:<date>:<slug>` trailer when the change touches zone C paths
  (data quality, PIT, research window, scoring, thresholds, position caps, production/broker
  boundaries, `tests/invariants`, `.github`, `AGENTS.md`, the ship policy). Only cite decisions the
  owner actually made; if none exists, stop and ask the owner.

## 4. Ship

```powershell
python tools/gov008/ship.py --repo . --dry-run            # zones, gate commands, problems
python tools/gov008/ship.py --repo . --execute --push     # gate (~10 min for zone B/C), then main and origin
```

- `ship` runs the gate, writes a `Gate:` trailer, fast-forwards `main` to the tested tree, pushes
  normally, and checks `origin/main` before and after. Do nothing else in the repository while it runs.
- If the gate fails: read the failures, fix them on the branch, commit, and run `ship` again. Never
  bypass `ship` with a manual `git push`, and never force-push, rebase, or rewrite history.
- If `main` moved meanwhile, `ship` refuses. The second shipper brings the new `main` into its branch
  and re-runs the gate (AGENTS.md); if that would need a merge commit or a rebase, stop and ask the owner.
- Afterwards: `git rev-parse main origin/main` must be equal; delete the merged task branch.
