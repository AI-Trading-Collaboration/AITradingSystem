# Project Engineering Rules

This project is an investment decision-support system. Data quality, auditability,
and correctness are product requirements, not optional polish.

## Primary Research Window

The project's active strategy research, primary backtests, and investment-facing
conclusions use the exact QQQ/SGOV/TQQQ validated window beginning on 2021-02-22.
This is the single project default unless a reviewed policy explicitly defines a
different sensitivity or stress role.

- default research and backtest start: 2021-02-22;
- results before 2021-02-22 may be used only for governed sensitivity, proxy, or
  stress testing with the relevant data-quality caveats;
- 2022-12-01 is not an active default, primary conclusion boundary, required
  comparator, or minimum allowed start. It may remain only in immutable historical
  artifacts, legacy compatibility evidence, and descriptions of prior runs.

Backtest and strategy reports must state the selected research window and the
actual requested and evaluated date ranges. Historical retained evidence must not
silently supply the default for a new run.

## System Flow Diagram Maintenance

`docs/system_flow.md` is the source-of-truth diagram for the path from data
inputs to intermediate evaluation and final conclusions. Any change that affects
CLI commands, critical configuration files, cache schemas, report outputs, data
quality gates, scoring modules, backtest behavior, market-regime interpretation,
or major new modules must update that diagram in the same change.

If a change intentionally does not affect the documented data flow, no diagram
update is required.

## No Silent Workarounds

When development hits a blocker, do not bypass it with a temporary workaround by
default.

Required process:

1. Identify the intended best solution.
2. Explain why that solution is currently blocked: technical cause, dependency
   limitation, data-source limitation, cost, latency, API permission, or missing
   requirement.
3. Evaluate whether the blocker should be fixed directly before continuing.
4. If a temporary workaround is truly necessary, discuss it with the project
   owner before implementing it.
5. Document every accepted workaround with:
   - reason it exists;
   - behavioral impact;
   - risk;
   - validation coverage;
   - exit condition for removing it.

Temporary code must not be hidden behind vague names such as `quick_fix`,
`fallback`, or `hack` without a tracked explanation.

## Data Source Discipline

Market, macro, fundamental, valuation, and news data are critical inputs. Data
source choices must be explicit and reviewable.

For each data source integration:

- record provider name, endpoint, request parameters, download timestamp, row
  count, and checksum where practical;
- distinguish primary sources, paid vendor sources, public convenience sources,
  and manual inputs;
- validate schema, completeness, freshness, duplicate keys, and suspicious
  values before downstream scoring;
- treat provider inconsistencies as investigation items, not as values to smooth
  over silently.

## Required Data Quality Gate

`aits validate-data` is the required quality gate for cached market and macro
data.

Any command, workflow, or module that produces technical features, scoring
outputs, backtest results, or daily reports from cached data must either:

1. run `aits validate-data` first and stop on failure; or
2. call the same validation code path directly and stop on failure.

Passing validation must be visible in downstream outputs. Reports that depend on
cached data must state the data quality status or link to the generated quality
report.

CI cannot validate local untracked market data because the cache is intentionally
not committed. This does not weaken the runtime requirement: local data-dependent
commands must enforce the gate themselves.

## Periodic Operations Runbook

`docs/operations/operations_runbook.md` is the entry-point runbook for periodic
operations. Before executing any daily, weekly, biweekly, monthly, governance,
scheduler validation, or artifact catalog consistency task, Codex must read that
runbook and use it to confirm the cadence, trigger path, required quality gates,
expected artifacts, and production-effect boundary.

The daily scheduler trigger is the unified external entry point for periodic
operations. Longer-cadence tasks may only be reached through a documented,
date- and condition-gated orchestration path, or run manually with the runbook's
checks applied. Do not scatter weekly, monthly, governance, or catalog checks
into separate unaudited scheduler entries.

## Change Discipline

- Prefer durable, well-tested fixes over local patches that only satisfy the
  immediate command.
- Keep scope narrow, but do not trade correctness for speed in the scoring,
  data, or backtest path.
- If a design decision affects investment interpretation, record it in docs or
  configuration rather than leaving it implicit in code.

## Pytest Validation Discipline

When a task needs pytest validation, use parallel pytest by default through
`python scripts/run_validation_tier.py <suite> --write-runtime-artifact`, or use
equivalent explicit pytest-xdist arguments for focused one-off tests:
`python -m pytest -n 16 --dist loadfile ...`.

Run serial pytest only for explicit reproduction or debugging of a
parallelism-related failure, and make that exception visible in the validation
notes. Do not silently replace a failed parallel run with a serial PASS.

## Heuristic and Threshold Governance

Investment-facing heuristics are model policy, not incidental code.

Any threshold, score band, confidence cutoff, sample floor, position cap,
readiness rule, promotion gate, risk multiplier, report conclusion boundary, or
backtest acceptance rule that can affect investment interpretation must satisfy
one of the following before it is introduced or changed:

1. Be defined in a reviewed configuration or policy manifest with owner,
   version/status, rationale, intended effect, validation evidence or planned
   validation, and review/expiry condition where applicable.
2. Be a named code constant with an adjacent comment or linked requirement that
   explains why it is an invariant rather than a tunable heuristic.
3. Be explicitly documented as a temporary pilot baseline in the task register
   and supporting requirement document, including the exit condition for
   replacing it with evidence-backed calibration.

Avoid unexplained numeric literals in scoring, position gates, confidence
assessment, feedback calibration, learning queues, backtests, promotion reports,
and investment reports. Existing hardcoded heuristics in these paths should be
treated as audit findings until migrated to configuration or documented with a
clear rationale.

Allowed low-risk constants include pure scale bounds such as 0/1/100, array
indices, formatting precision, unit conversions, protocol/schema constants,
HTTP timeouts, retry counts, UI sizing, and test fixture values, provided they do
not change investment interpretation.

When a heuristic is intentionally configurable but still subjective, reports
that depend on it should expose the policy/config version or link to the
generated policy report so the conclusion remains auditable.

## Task Register Discipline

`registry/development_tasks/` plus
`inputs/architecture/arch_005_task_registry_index.yaml` is the canonical source
of truth for unfinished work, deferred enhancements, owner-dependent data tasks,
and baseline implementations that are not yet complete enough for long-term
system quality. `docs/task_register.md` and `docs/task_register_completed.md` are
generated, do-not-edit compatibility views; use
`scripts/architecture_arch005_task_source.py register|update` for task mutations.

Any non-trivial TODO, planned enhancement, accepted workaround, or follow-up
from a review must be recorded in the canonical registry instead of being left
only in code comments, chat history, or an ad hoc checklist. Each item should
include:

- stable task id;
- priority;
- status;
- owner or next responsible party;
- blocker or dependency, if any;
- acceptance criteria;
- last update date or reason for status change.

Before implementing any non-trivial requirement, bug fix, scoring change, data
pipeline change, or report behavior change discussed with the project owner,
first create or update the relevant canonical task record with priority, status,
next owner, blocker/dependency, and acceptance criteria. Do not move directly
from discussion to implementation unless the change is trivial housekeeping that
does not affect system behavior, investment interpretation, data flow, data
quality, scoring, backtests, or reports.

When a requirement has too much context for a concise task-register row, create
a supporting Markdown document, preferably under `docs/requirements/` or another
clearly named `docs/` subdirectory. The task-register row should then contain a
short summary and link to that document. The supporting document should preserve
the longer context, design decisions, open questions, acceptance criteria,
progress notes, and status transitions. Implementation progress must update both
the task-register summary/status and the linked document when the detailed
context changes.

When a task needs to be split into multiple development steps, create or update
a supporting Markdown document before implementation. The document must record
the step breakdown, dependencies, sequencing, acceptance criteria for each
stage, open questions, and status changes. The task-register row must link to
that document instead of trying to carry the full plan in one table cell.

Priority should reflect long-term system risk, not implementation convenience:
correctness, data quality, auditability, investment interpretation, and backtest
validity rank above UI polish or developer ergonomics. When a basic version is
implemented only to keep other work moving, mark it as `BASELINE_DONE` and record
the remaining data-source, validation, or design dependency. When progress
depends on the project owner providing a more credible data source, access,
policy decision, or manual review, mark the blocker explicitly instead of
treating the task as complete.

Whenever a task moves forward, becomes blocked, is superseded, or is completed,
update the register in the same change as the code or documentation change that
caused the status transition.

Canonical task events are append-only and bind actor, change id, timestamp, base
commit, previous state event, and resulting projection. Moving a row manually
between active and completed Markdown files is forbidden; terminal status updates
regenerate both compatibility views and their index bindings atomically.

## Governed Development Workflow Skill Discipline

Before the first implementation write for any non-trivial tracked repository
mutation, Codex must use the `run-governed-development` skill or execute its
documented equivalent preflight. This applies to implementation, bug fixes,
refactors, data pipelines, scoring, backtests, report behavior, architecture,
governance, and developer-workflow changes.

The only mutation allowed before a missing task passes preflight is the
coordinator-owned creation or update of the task-register row and its supporting
requirement. Rerun preflight after that record exists and before implementation.

Choose exactly one governed mode:

- `READ_ONLY`: explanation, review, status, and diagnosis that do not modify
  tracked content. Do not create a branch, worktree, or write lease merely to
  answer a read-only request.
- `SINGLE_LANE`: one task-owned implementation scope. Start from the exact local
  `main` commit, use a task branch, run lane-focused validation, and close through
  the local-main integration rules below.
- `DUAL_LANE`: independent engineering and strategy-evidence scopes. Freeze one
  exact local-main base, use isolated branches/worktrees, declare disjoint path,
  module, contract, resource, and evidence-lineage claims, and integrate through
  one coordinator candidate.

If shared schema, public contract, global policy, DQ/PIT semantics, cache
identity, or another consumer-visible boundary must change, complete the smallest
reviewed serial contract wave first. Recompute both lanes from its new exact base;
do not let a consumer continue from a stale contract merely because its files do
not overlap.

When a clean task lane still descends from its frozen base but local `main`
advances, do not create replacement v2/v3 worktrees merely to make the base SHA
current. Continue lane-focused work on the frozen base. At the natural integration
boundary, generate and validate `integration_revalidation_plan.v1` from the real
frozen-base/lane-head/latest-main Git deltas. Unrelated drift may proceed through
one latest-main coordinator candidate; reviewed generated/shared overlap must be
rebuilt once on that final tree; domain overlap requires reviewed coordinator
reconciliation; contract-sensitive or consumer-semantic drift requires the
smallest serial contract wave. Undeclared paths, wrong ancestry or identity,
dirty target state, missing evidence, and plan tamper fail closed.

For domain-only `RECONCILIATION_REQUIRED`, the integration preflight may continue
without rebuilding the lane only when the coordinator supplies the exact
validated plan id through the reviewed-reconciliation argument. Record that id
in the preflight result and reconcile only the reported overlap rows in the one
latest-main candidate.

The drift plan is read-only authority. It must not automatically rebase, merge,
cherry-pick, commit, push, delete, clean, or mutate task status. Heavyweight
formal validation remains bound to the final integration candidate, not to each
intermediate frozen lane or main advancement.

If the skill is unavailable or damaged, follow the equivalent rules in this file
and the ARCH-005 operating model manually, report the skill issue, and remain
fail closed. Skill availability never weakens task registration, checkout audit,
lease, validation, integration, or cleanup requirements.

## Known-Unrelated Worktree Audit Discipline

When `config/architecture/arch_005_s4d_checkout_guard.yaml` registers one or
more `known_unrelated_exclusions`, repository-wide closeout inspection must use:

`python scripts/architecture_arch005_checkout_guard.py worktree-audit`

Do not run a bare repository-wide `git status`, `git diff`, or
`git diff --check` in that checkout. The governed audit command applies every
registered exclusion as an exact literal pathspec to dirty inventory, unstaged
diff checking, and staged diff checking. It must not open, hash, copy, stage, or
modify excluded file contents.

Direct Git inspection is allowed only when every invocation includes the same
complete exact exclusion set, or when it targets an explicit allowlist of
task-owned paths that cannot include an excluded path. If an exclusion is
accidentally omitted, record the inspection as an audit incident even when no
file content was printed and no modification occurred.

## Temporary Workspace Lifecycle Discipline

Temporary Git worktrees, local clones, validation snapshots, external cache
directories, and supervised-run workspaces must have an explicit lifecycle.

Before creating one:

- record the owning task, purpose, absolute path, and exit condition in the
  supporting requirement or task progress notes when it may survive the current
  command or Codex turn;
- use a task-identifiable name and the narrowest safe parent directory;
- do not create another candidate directory when an existing audited workspace
  can satisfy the same isolation and reproducibility requirement.

Before task closeout, after a successful commit/push, or after abandoning a
development attempt:

1. inspect tracked, untracked, and ignored content and confirm whether any
   evidence or implementation remains unique;
2. move required evidence to its canonical governed location and verify the
   destination before cleanup;
3. confirm no active process or current validation depends on the path;
4. remove clean Git worktrees with `git worktree remove`, remove disposable
   clones/caches with an explicit absolute-path allowlist, and run
   `git worktree prune` when applicable;
5. report what was removed, what was retained, and whether recovery is possible.

Merge status is necessary but not sufficient for cleanup. A temporary workspace
whose HEAD or patch has entered the reviewed mainline should be removed when all
of the following are true:

- the merge is confirmed by commit ancestry or reviewed patch/PR equivalence;
- every required artifact or validation result exists in its canonical governed
  location and immutable files match the recorded hashes;
- tracked, untracked, and ignored content has been audited and contains no
  unique unsuperseded implementation or evidence;
- no active process, scheduler entry, current validation, or operational
  acceptance still depends on the path;
- the exact deletion allowlist, released size, retained evidence, and
  recoverability boundary will be recorded in the owning task documentation.

A dirty intermediate clone is not retained merely because its bytes differ from
the latest mainline. If later reviewed commits or candidates supersede those
bytes and canonical evidence has been preserved, classify the clone as
superseded and clean it. Conversely, branch merge alone never authorizes
deleting unaudited dirty or ignored content.

Abandoning an attempt does not authorize discarding unreviewed changes. Dirty or
uncertain directories must be preserved until audited. If a temporary directory
cannot be removed at closeout, record its path, reason, behavioral or evidence
risk, next owner, and concrete exit condition in the canonical task registry or
the linked supporting requirement. Leaving temporary directories behind without
that record is not an acceptable closeout state.

## Risk-Tiered External Actions and Evidence Admission

External-action permission and technical evidence validity are separate axes.
Authorization limits account, resource, third-party, reversibility, production,
and broker risk. Evidence admission depends on exact code/data identity, runtime
provenance, requested/evaluated scope, output integrity, reproducibility, DQ/PIT,
and independent validation. A missing preformatted token must not, by itself,
make an observed result technically false or permanently inadmissible.

Apply these risk tiers:

- `R0_LOCAL_READ_ONLY`: local reads, static analysis, offline validation, and
  retained-result review require no per-action authorization beyond the user's
  task request.
- `R1_BOUNDED_RESEARCH_SANDBOX`: bounded work in an existing research sandbox or
  clone may proceed under standing owner scope when the owner has asked Codex to
  continue that research task and a reviewed task/manifest fixes the target,
  code identity, action maxima, zero-order/zero-fill boundary, and exit condition.
  Do not require the owner to paste machine-generated hashes back into chat.
  Codex must automatically replay the manifest before dispatch and record actual
  counters and terminal evidence afterward.
- `R2_MATERIAL_EXTERNAL_CHANGE`: original-project writes, meaningful paid-resource
  consumption, cloud deletion, public sharing, external messages, or similarly
  material/recovery-cost actions require a concise explicit owner instruction
  bound to the target and intended effect.
- `R3_PRODUCTION_OR_BROKER`: paper/live, broker, order, fill, capital/position,
  production promotion, or comparably high-consequence actions require separate,
  exact-scope authorization and all existing fail-closed safety gates.

For every external run, record `authorization_state` separately from
`technical_validation_state`. Allowed authorization states are
`EXACT_PREAUTHORIZED`, `STANDING_OWNER_SCOPE`, `RETROSPECTIVELY_REVIEWED`, and
`UNAUTHORIZED_ACTION_INCIDENT`. An action outside owner intent is quarantined and
reviewed; its bytes are preserved as incident evidence, but it cannot drive DQ/PIT,
selection, engine, production, or investment conclusions until scope and safety
review complete. Historical exact-token artifacts remain immutable. PR,
force-push, history rewrite, remote-divergence repair, destructive cleanup, and
R2/R3 actions retain their separate authorization rules.

## Integration Publication Fence Discipline

Before a coordinator mutates canonical task state, shared/generated authority,
the final candidate, formal Full evidence, local `main`, or remote publication,
it must acquire one `integration_publication_fence.v1` transaction through
`scripts/architecture_arch005_publication_fence.py`. The transaction must reuse
the S4D `CheckoutLeaseGuard` / `FileExecutionLeaseStore` authority and declare
all task, coordinator, generated, and validation-resource paths; do not create a
second lock, scheduler, or publication queue.

Advance the transaction in the reviewed order: `TASK_SOURCE_PRE_WRITE`,
`GENERATED_REBUILD_PRE/POST`, `CANDIDATE_COMMIT_PRE`,
`FORMAL_VALIDATION_PRE`, `FULL_DISPATCHED`, `FORMAL_VALIDATION_RESULT`,
`LOCAL_MAIN_FF_PRE`, `REMOTE_PUSH_PRE`,
`CLEANUP_PRE`, then `RELEASED`. Revalidate expected main, active lease, plan
bytes, dirty attribution, generator order, candidate SHA, parent Full artifact,
remote ancestry, and final SHA equality at the applicable phase. A stale,
expired, terminal, tampered, wrong-phase, dirty, or undeclared transaction fails
closed before mutation.

Mutating `architecture_arch005_task_source.py` commands and executed
`run_validation_tier.py full` require the exact `--publication-transaction`.
`INTEGRATION` and `CLOSEOUT` governed preflights require the same transaction;
closeout accepts it at `REMOTE_PUSH_PRE`. A failed publication is immutable
terminal evidence. Any retry uses a new transaction and, for
`failure_fix_rerun`, binds the prior failed Full artifact. Release only through
the publication command so ordinary push, SHA verification, cleanup evidence,
and lease release remain replayable in one closeout receipt.

## Local Branch, Commit, and Main Integration Discipline

When completing work that was explicitly selected from the canonical task registry
or another project TODO list, the finished change may be committed directly to
the current task branch after the relevant validation has passed. The commit
must include the task-register/status update, supporting documentation updates,
and the implementation or test changes that caused the task to move forward.

The default closeout boundary includes local `main` and a normal push of that
validated local-main commit to its configured remote main branch. After the
local fast-forward, fetch the remote main ref, require it to be an ancestor of
the validated candidate, perform an ordinary non-force push, and verify the
local-main and remote-main SHAs are identical.

Do not push when the user explicitly asks not to push, the candidate contains
unrelated user changes or commits, no configured remote/upstream is available,
the remote has diverged, the update is non-fast-forward, or publication would
require a merge, rebase, history rewrite, force-push, PR, or broader external
action. Stop and report those conditions rather than repairing remote history
automatically. Pull requests and force-pushes always require separate explicit
authorization.

For `SINGLE_LANE`, require local `main` to be an ancestor of the validated task
commit and use `git merge --ff-only <task-branch>`. Do not create an empty or
redundant post-merge commit.

If `main` advanced after the task branch froze, first use the governed
base-drift plan above. A compatible lane must be reconciled into one candidate
created from latest local `main`; do not require the frozen task commit itself
to be a direct fast-forward target. Refresh shared/generated state and run final
validation on that candidate before applying the normal local-main fast-forward
and remote-push gates.

For `DUAL_LANE`, do not fast-forward sibling branches into `main` one after the
other: after the first fast-forward the second sibling necessarily diverges.
Create one coordinator integration branch from the frozen base, absorb validated
lane changes in the fixed reviewed order, refresh shared/generated state, and
validate the combined final tree. Fast-forward local `main` exactly once to that
integration candidate. Do not automatically rebase, create merge commits,
force-push, or delete user changes to make the topology fit.

At the end of a qualifying task, Codex must:

1. confirm the worktree only contains changes attributable to the completed
   task;
2. run and record the relevant validation;
3. stage only the attributable files;
4. create a local commit on the current branch after validation passes;
5. build and validate the coordinator integration candidate when multiple lanes
   are involved;
6. fast-forward local `main` only after ancestry, final-tree, attribution,
   generated-freshness, and active-lease checks pass;
7. fetch the remote main ref, perform the default ordinary push when its safety
   conditions hold, and verify `local main = remote main = candidate`;
8. audit and clean merged branches/worktrees under the temporary workspace
   lifecycle rules, then report the task commit, local-main SHA, validation,
   push/remote SHA, cleanup, and any reason the default push was skipped.

## Parallel Development Discipline

When multiple missing modules or feature slices can be developed independently,
prefer parallel development. Split work by clear ownership boundaries such as
data source adapter, schema validation, scoring integration, backtest history
support, reports, or tests.

Parallel work must remain reviewable:

- assign each worker a concrete module responsibility and a mostly disjoint file
  scope before implementation starts;
- avoid parallel edits to shared integration files such as CLI wiring, central
  scoring rules, global config, and `docs/system_flow.md` unless coordination is
  explicit;
- keep shared documentation, configuration, and final integration under one
  coordinating change so that data flow, audit requirements, and tests stay
  consistent;
- do not duplicate logic across parallel branches just to move faster; extract
  shared helpers during integration when the duplication affects correctness or
  auditability.

All parallel workers must start from the same exact local-main commit. The
integration coordinator owns task-register updates, root/shared wiring,
catalogs/registries, generated manifests/views, formal validation, the final
candidate commit, and the single local-main fast-forward. Lane workers run
focused/impact validation; heavyweight Full validation runs at the natural
integration boundary and must not compete with another Full run.

## Output Language

Project-facing conclusions, Markdown reports, and CLI result summaries should be
written in Chinese by default. Keep standard identifiers such as ticker symbols,
feature IDs, file names, schema columns, status codes, and established market
terms in English when translating them would reduce precision or break data
compatibility.
