---
name: task-records
description: Create, update, close, or look up AITradingSystem task records and requirement documents (tasks/<ID>.yaml, docs/task_register.md, owner decisions). Use before any non-trivial change and whenever work moves forward, gets blocked, or finishes.
---

# Task records

Task records are `tasks/<ID>.yaml` (schema `task.v1`, one file per task). `docs/task_register.md`
(open work) and `docs/task_register_completed.md` (`DONE`/`DROPPED`) are rendered views: never edit
them by hand; the PR suite fails when they are stale. History is git history.

## Commands (project venv Python)

```powershell
python tools/tasks.py list [--status IN_PROGRESS] [--priority P0]
python tools/tasks.py show <ID>
python tools/tasks.py new <ID> --area "..." --priority P1 --next-owner "..." --next-step "..." --acceptance "..." [--status PROPOSED] [--notes "..."]
python tools/tasks.py set <ID> status=IN_PROGRESS "next_step=..."      # any field=value
python tools/tasks.py render     # rewrite both views (new and set already do this)
python tools/tasks.py check      # validate every task file and that the views are current
```

Fields: `id`, `area`, `priority` (P0-P3), `status`, `next_owner`, `next_step`, `acceptance`, `notes`.
Every value is one line; `|` is allowed only in `notes`. Statuses: `PROPOSED`, `READY`,
`IN_PROGRESS`, `BLOCKED_OWNER_INPUT`, `BLOCKED_EXTERNAL`, `BASELINE_DONE`, `VALIDATING`, `DEFERRED`,
`DONE`, `DROPPED` (`STATUSES` in `tools/tasks.py`).

## Rules

- Before a non-trivial change to behavior, scoring, data pipelines, backtests, or reports: create or
  update the record with priority, status, next owner, blocker, and acceptance criteria.
- Multi-step work gets a requirement document `docs/requirements/<ID>_<Title>.md` with the step plan,
  acceptance per step, open questions, and progress notes; the record links to it. Update both when
  the work moves.
- Priority follows long-term risk (correctness, data quality, auditability, investment interpretation,
  backtest validity before convenience). A stop-gap is `BASELINE_DONE` with what remains named.
- Owner decisions are written as `owner_decision:<task>:<YYYY-MM-DD>:<slug>` in the requirement
  document, quoted from what the owner actually said, and cited in commit `Owner-Decision:` trailers.
  Never invent one; ask the owner instead.
- Keep `production_effect=none` and `broker_action=none` in notes unless the owner authorizes otherwise.
- Record changes ship with the change that caused them (see the `ship-change` skill).
