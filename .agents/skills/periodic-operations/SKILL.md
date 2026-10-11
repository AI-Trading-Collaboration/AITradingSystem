---
name: periodic-operations
description: Run, check, or troubleshoot AITradingSystem daily, weekly, biweekly, monthly, or ad hoc operations (aits ops daily-run, daily-plan, periodic-dispatch, validate-data, report index, Reader Brief). Use before any scheduler, cadence, data refresh, or report catalog task.
---

# Periodic operations

Read `docs/operations/operations_runbook.md` first; it is the source of cadence, trigger path, gates, and
expected artifacts. `docs/system_flow.md` sections 1-6 show the data flow.

## Fixed rules

- One external entry point: `aits ops daily-run`, started by the single Windows scheduled task
  `\AITradingSystem Daily Run` in the runtime checkout `D:\Work\AITradingSystem_ops_runtime` (runbook
  section 9). Never register a second scheduler or a weekly/monthly/ad hoc command as its own task.
  `aits ops daily-plan --fail-on-missing-env` previews without executing.
- Each scheduled run writes `outputs\run_control\scheduler\<date>_<window>.log` and a code-generated
  `.summary.md`/`.summary.json` in the runtime checkout; read those first. The development checkout
  does not run the daily report, so its DQ receipts say nothing about production daily status.
- Move the runtime checkout only to a shipped `main` commit with the runbook 9.1 steps (clean tree,
  no active run, `fetch` + `checkout --detach`); never pull, reset, clean, or stash it.
- `aits validate-data` must pass (strict `PASS`) before anything consumes cached market or macro data.
  Report the data quality status, or link the quality report, in every output that depends on it.
- Never delete or hand-edit `outputs/run_control/**` state, ledgers, or locks to force a rerun, and never
  rewrite a terminal run. A changed step list starts a new run key by itself.
- Terminal recovery is only `aits ops daily-run --recovery-parent-run-id ... --recovery-from-step ...
  --recovery-reason-code ...` with all three values, run in the runtime checkout on owner instruction.
  The current release is the runtime checkout HEAD: the tree must be clean, HEAD reachable from
  `origin/main`, and different from the parent run's `git_commit` (OPS-082). No deployment receipt.
- Non-daily tasks run manually through `aits ops periodic-dispatch` with real evidence ids
  (`--data-quality-evidence-id`, `--source-artifact-id`, `--owner-decision-id`, `--confirm-manual-dispatch`,
  plus `--explicit-trigger` for ad hoc). Automatic dispatch stays disabled.
- `production_effect=none`, no weight writes, no broker or order actions. Anything paper/live, broker,
  order, position, or production needs separate, exact owner authorization.
- Provider quotas and paid requests: follow `config/data_source_request_budget_policy.yaml`; a quota
  shortfall fails closed. Never retry provider calls in a loop to get past a failure.

## When something fails

1. Read the step's report and the run state under `outputs/run_control/daily/`; name the typed blocker.
2. Classify it: data or provider (wait for the next provider-ready day or fix the source), code (task
   record plus a fix through the `ship-change` skill), or owner decision needed (explain the background
   and options in Chinese, then ask one question).
3. Write up what was run, the as-of date, the requested and evaluated date ranges, the DQ status, and
   what is still blocked, in Chinese.
