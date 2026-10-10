# Project Engineering Rules

This is an investment decision-support system. Data quality, auditability, and correctness are
product requirements, not polish. "The agent" means whichever coding agent works in this repository;
nothing here depends on a specific harness. Background and the decisions behind these rules:
`docs/requirements/GOV-008_Research_First_Governance_Refactor.md`. Step-by-step procedures are Agent
Skills in `.agents/skills/` (ship-change, task-records, periodic-operations, research-run);
`.pi/extensions/aits-guard` blocks the forbidden git actions for pi. Neither replaces these rules.

## Primary Research Window

Active strategy research, primary backtests, and investment-facing conclusions use the exact
QQQ/SGOV/TQQQ validated window beginning on 2021-02-22 (`config/research/primary_research_window_policy.yaml`).

- default research and backtest start: 2021-02-22; this is the single project default.
- Results before 2021-02-22 are allowed only for governed sensitivity, proxy, or stress testing with
  the relevant data-quality caveats.
- 2022-12-01 is not an active default, a primary conclusion boundary, a required comparator, or a
  minimum start. It may remain only in immutable historical artifacts and descriptions of prior runs.
- Backtest and strategy reports state the selected research window and the actual requested and
  evaluated date ranges. Retained historical evidence must not silently supply the default of a new run.

## Data Source Discipline

Market, macro, fundamental, valuation, and news data are critical inputs. For each integration record
provider, endpoint, request parameters, download timestamp, row count, and checksum where practical;
distinguish primary, paid-vendor, public-convenience, and manual sources; validate schema,
completeness, freshness, duplicate keys, and suspicious values before scoring; treat provider
inconsistencies as investigation items, never smooth them over silently.

## Required Data Quality Gate

`aits validate-data` is the required gate for cached market and macro data. Any command that produces
features, scores, backtests, or daily reports from cached data runs it first (or calls the same code
path) and stops on failure. Passing validation is visible in downstream outputs: reports state the data
quality status or link the quality report. Local data-dependent commands enforce the gate themselves;
CI cannot see the untracked cache. `tests/invariants/` proves the gate stops downstream work.

## Heuristic and Threshold Governance

Any threshold, score band, confidence cutoff, sample floor, position cap, readiness rule, promotion
gate, risk multiplier, conclusion boundary, or backtest acceptance rule that can affect investment
interpretation must be (1) in a reviewed configuration with owner, version/status, rationale, intended
effect, validation evidence, and a review condition; or (2) a named constant whose adjacent comment
explains why it is an invariant; or (3) a documented temporary pilot baseline in the task record with an
exit condition. Allowed low-risk constants: scale bounds (0/1/100), indices, formatting precision, unit
conversions, protocol constants, timeouts, retry counts, UI sizing, test fixtures. The audit is
`config/heuristic_governance.yaml` plus `tests/test_heuristic_governance.py`. Reports that depend on a
subjective policy expose its version or link the policy report.

## How a change ships

One flow for every change, run by `python tools/gov008/ship.py --repo . --execute --push` (use
`--dry-run` first to see zones and gate): task branch -> local gate -> `main` fast-forwards to the
tested tree -> ordinary push, with the remote checked before and after.

- **Zones** (`config/gov008_ship.yaml`): **A** records (docs, task and experiment records) need no
  tests. **B** product code runs the PR suite:
  `python -m pytest tests -n 16 --dist loadfile -m "not slow"` (about 10 minutes; `slow` marks
  single tests of 30 s or more, which run nightly). **C** semantic-critical
  paths (data quality, PIT, research window, scoring, thresholds, position caps, production and broker
  boundaries, `tests/invariants`, `.github`, this file, the ship policy) run the PR suite and need an
  `Owner-Decision:` trailer in at least one commit of the range; `ship` refuses without it.
- `ship` writes a `Gate:` trailer recording the tested tree. The gate is run by the agent itself and
  is not an independent verifier; for zone C the owner decision is the real control. Do not weaken a test,
  invariant, or this policy to make a gate pass.
- Never pass hundreds of test file paths to pytest (collection becomes quadratic on Windows); pass
  `tests` and select with markers. Run serial pytest only to reproduce a parallelism-related failure
  and say so. Use the project venv interpreter, not a system Python.

## Git

- Work on a task branch. `main` only moves by fast-forward to a gated tree. No merge commits, rebase,
  force-push, history rewrite, or remote-divergence repair by the agent.
- Parallel work (several agents or sessions) uses separate worktrees and branches. Git's fast-forward
  check is the only coordination: whoever ships second brings the new `main` into its branch and
  re-runs the gate.
- Pushing `main` (ordinary, non-force) after a green gate is a standing authorization; no per-change
  question. Pull requests and force-pushes always need explicit authorization.
- Stop and report instead of pushing when the owner said not to, the tree holds unrelated user changes,
  the remote has diverged, or the update would not be a fast-forward.
- Paths listed under `tree_clean_exclusions` in `config/gov008_ship.yaml` are unrelated dirty files:
  never open, stage, or modify them, and exclude them (exact literal pathspec) from any repository-wide
  `git status` or `git diff`.

## Tasks and decisions

- Before a non-trivial change to behavior, scoring, data pipelines, backtests, or reports, create or
  update the task record with priority, status, next owner, blocker, and acceptance criteria. Trivial
  housekeeping needs none. Multi-step work gets a requirement document under `docs/requirements/` with
  the step plan, acceptance per step, open questions, and progress; the task record links to it.
- Record owner decisions as `owner_decision:<task>:<date>:<slug>` in the requirement document and cite
  them in the `Owner-Decision:` trailer.
- Task records are `tasks/<ID>.yaml` (schema `task.v1`): edit the file or use
  `python tools/tasks.py new|set`, then `python tools/tasks.py render`. `docs/task_register.md` and
  `docs/task_register_completed.md` are rendered views: never edit them; the PR suite fails when they
  are stale. A task moves to the completed view by setting `DONE` or `DROPPED`.
- Priority follows long-term risk: correctness, data quality, auditability, investment interpretation
  and backtest validity rank above convenience. Mark a stop-gap `BASELINE_DONE` and name what remains.
- No silent workarounds: state the best solution, why it is blocked, and whether to fix the blocker; a
  temporary workaround needs the owner's agreement and a record of reason, impact, risk, validation,
  and exit condition.

## Runs and evidence

- Every research or daily run records the commit, whether result-affecting code was modified, content
  hashes of its configuration, and the requested and evaluated date ranges (`core/provenance.py`).
- State a hypothesis, window, cost model, and kill criteria and commit them before reading any outcome
  of a new experiment; record every trial. A validation PASS never means a candidate is valid,
  paper-shadow eligible, or approved for production or a broker.
- Prospective captures run under a capture hold (`scripts/capture_hold.py`; exact commit, unmodified
  code, exclusive output paths, bounded validity). A hold is a local self-attestation, not an
  independent authority. Steps are in the operations runbook.
- Changing data flow, cache schemas, report outputs, data quality gates, scoring, backtest behavior, or
  market-regime interpretation updates `docs/system_flow.md` in the same change (it is being rewritten
  as a real flow diagram in P5). A change that does not affect data flow needs no update.
- Project-facing conclusions, reports, and CLI summaries are written in Chinese; keep tickers, feature
  IDs, file names, schema columns, status codes, and established market terms in English.

## Operations

Before any daily, weekly, monthly, scheduler, or catalog task, read `docs/operations/operations_runbook.md`
for cadence, trigger path, quality gates, and expected artifacts. The daily scheduler trigger is the one
external entry point; longer-cadence tasks run through a documented date- and condition-gated path or
manually with the runbook's checks, never as separate unaudited scheduler entries.

## External actions and workspaces

- **R0** local read-only and offline work needs no extra authorization. **R1** bounded work in an existing
  research sandbox needs a task naming target, action maximum, zero-order boundary, and exit condition.
  **R2** original-project writes outside normal change flow, meaningful paid-resource use, cloud deletion,
  public sharing, or external messages need a concise explicit owner instruction. **R3** paper/live,
  broker, order, fill, position, or production promotion needs separate exact-scope authorization.
  `production_effect=none` and `broker_action=none` are defaults; never change them silently.
- Temporary worktrees, clones, caches, and run directories get a task-identifiable name, an owner, and an
  exit condition; remove them at closeout only after confirming nothing unique remains. Dirty or
  uncertain directories are preserved until audited and recorded if they cannot be removed.
