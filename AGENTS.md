# Project Engineering Rules

This project is an investment decision-support system. Data quality, auditability,
and correctness are product requirements, not optional polish.

## AI Regime Priority

The project primarily studies U.S. AI-related trading after the public launch of
ChatGPT. Strategy design, information collection, backtests, and report
interpretation should prioritize the configured `ai_after_chatgpt` market regime:

- anchor event: ChatGPT public launch on 2022-11-30;
- default backtest start: 2022-12-01, the first full U.S. trading day after that
  event;
- pre-2022 history may be used for warm-up, stress testing, and regime
  comparison, but it must not be treated as the default AI-cycle conclusion
  window.

Backtest and strategy reports should state the selected market regime and the
actual requested date range. If a primary conclusion relies on data before the AI
regime start, document why that older regime is relevant.

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

## Change Discipline

- Prefer durable, well-tested fixes over local patches that only satisfy the
  immediate command.
- Keep scope narrow, but do not trade correctness for speed in the scoring,
  data, or backtest path.
- If a design decision affects investment interpretation, record it in docs or
  configuration rather than leaving it implicit in code.

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

`docs/task_register.md` is the source of truth for unfinished work, deferred
enhancements, owner-dependent data tasks, and baseline implementations that are
not yet complete enough for long-term system quality.

Any non-trivial TODO, planned enhancement, accepted workaround, or follow-up
from a review must be recorded there instead of being left only in code comments,
chat history, or an ad hoc checklist. Each item should include:

- stable task id;
- priority;
- status;
- owner or next responsible party;
- blocker or dependency, if any;
- acceptance criteria;
- last update date or reason for status change.

Before implementing any non-trivial requirement, bug fix, scoring change, data
pipeline change, or report behavior change discussed with the project owner,
first create or update the relevant task-register item with priority, status,
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

## Local Commit Discipline

When completing work that was explicitly selected from `docs/task_register.md`
or another project TODO list, the finished change may be committed directly to
the current local branch after the relevant validation has passed. The commit
must include the task-register/status update, supporting documentation updates,
and the implementation or test changes that caused the task to move forward.

This permission applies to local commits only. Pushing a branch, opening or
updating a pull request, rewriting history, or including unrelated user changes
still requires an explicit project-owner request.

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

## Output Language

Project-facing conclusions, Markdown reports, and CLI result summaries should be
written in Chinese by default. Keep standard identifiers such as ticker symbols,
feature IDs, file names, schema columns, status codes, and established market
terms in English when translating them would reduce precision or break data
compatibility.
