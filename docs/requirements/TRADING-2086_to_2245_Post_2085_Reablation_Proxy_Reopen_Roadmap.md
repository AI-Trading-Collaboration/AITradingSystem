# TRADING-2086 to 2245 Post-2085 Re-Ablation, Proxy Validation, Reopen Gate

最后更新：2026-06-28

## Status

- Task id: `TRADING-2086_to_2245_POST_2085_REABLATION_PROXY_REOPEN_ROADMAP`
- Status: `IN_PROGRESS`
- Last updated: 2026-06-28
- Owner: system

## Scope

Implement the post-2085 research-only path after free PIT data source ingestion.
The objective is to test whether newly ingested free feature families and
low-cost participation proxies add enough auditable information to justify a
first-layer channel reopen review.

The primary market regime is `ai_after_chatgpt`, anchored on 2022-11-30, with
the default backtest start of 2022-12-01. Historical data before that point may
only support warm-up, stress slices, or comparison windows.

## Stages

1. `TRADING-2086_to_2115_FREE_FEATURE_FAMILY_REABLATION`
   - Create re-ablation scope and free feature registry v2.
   - Build the family-level ablation dataset from cached free feature parquet
     and QQQ forward outcomes after the cached-data quality gate.
   - Produce risk-on veto, stay-constructive, add-risk diagnostic,
     participation-proxy incremental value, multi-window stability, dependency
     diagnostics, recommendation, closeout, and final matrix artifacts.
2. `TRADING-2116_to_2145_PARTICIPATION_PROXY_VALIDATION`
   - Keep participation proxy explicitly separate from true PIT breadth.
   - Build ETF ratio features from available cached ETF prices.
   - Record Alpha Vantage listing-status and FMP holdings trial gates as
     diagnostic / permission-dependent evidence, not model-ready breadth.
   - Produce PIT contract, channel ablation, Norgate value-of-information
     estimate, closeout, and final matrix.
3. `TRADING-2146_to_2175_FIRST_LAYER_REOPEN_DECISION_GATE`
   - Aggregate re-ablation, proxy validation, channel closeout, PIT audit,
     coverage, and dependency evidence.
   - Apply a pre-registered policy engine.
   - Deny reopen unless all required gates and owner approval pass.
4. `TRADING-2176_to_2215_CHANNEL_SPECIFIC_FIRST_LAYER_V4`
   - Conditional only. Generate fail-closed scope/contract/closeout when the
     reopen gate does not allow narrow v4 or owner approval is absent.
   - Do not train v4 or build a candidate without explicit gate pass and owner
     approval.
5. `TRADING-2216_to_2245_MINIMAL_FORWARD_DIAGNOSTIC_LOG`
   - Add disabled policy, log schema, CLI, outcome-backfill guardrail, and
     closeout artifacts.
   - Default status remains disabled until owner approval and eligible
     observable signals exist.

## Non-Goals

- Do not restore dynamic promotion.
- Do not enter paper-shadow, production, or broker workflows.
- Do not output trade advice, target allocation, production weights, or TQQQ
  allocation.
- Do not modify frozen second-layer probes or second-layer weights.
- Do not train a universal first-layer model.
- Do not treat `participation_proxy_free_v1` or `participation_proxy_free_v2`
  as true PIT breadth or promotion evidence.
- Do not execute channel-specific v4 unless `REOPEN_ALLOWED_FOR_NARROW_CHANNEL_V4`
  and owner manual approval are both present.

## Acceptance Criteria

- New task artifacts are registered in `docs/task_register.md` before code
  implementation.
- Re-ablation and proxy validation commands generate the required YAML,
  Markdown, Parquet, and JSON artifacts with data-quality status and safety
  fields.
- Reopen gate policy is configurable, pre-registered, and tested for target-path
  only, 2023+ only, beta/TQQQ dependency, PIT warning, owner approval, and
  promotion blocking.
- Conditional v4 remains fail-closed unless the reopen gate and owner approval
  both pass.
- Minimal forward diagnostic remains disabled and blocks allocation, trade,
  paper-shadow, production, and broker fields.
- `docs/system_flow.md`, `docs/artifact_catalog.md`, and
  `config/report_registry.yaml` are updated for the new commands and artifacts.
- Focused parallel pytest, Ruff, compileall, CLI smoke checks, and diff checks
  pass or any blocker is documented.

## Open Questions And Blockers

- Local cached ETF price coverage may not include all ratio tickers
  (`QQQE`, `RSP`, `SPY`, `SMH`, `SOXX`, `XLK`). Missing ratios must be exposed
  as coverage blockers rather than imputed silently.
- Alpha Vantage `LISTING_STATUS` is not Nasdaq-100 membership and cannot prove
  historical index breadth. It can only support survivorship diagnostic review.
- FMP ETF holdings historical PIT semantics remain blocked until `holding_date`,
  `reported_date`, and `known_at` are confirmed.
- Owner approval is required before any channel-specific first-layer v4 or
  forward diagnostic log activation.

## Progress Log

- 2026-06-28: Implementation generated real artifacts with `--as-of
  2026-06-26`. Free re-ablation completed with
  `final_status=DIAGNOSTIC_ONLY_EVIDENCE` and recommendation
  `FREE_FEATURES_BLOCKED_BY_PIT_OR_DEPENDENCY`. Participation proxy validation
  generated ETF ratio and `participation_proxy_free_v2` features with
  `final_status=NORGATE_DUE_DILIGENCE_RECOMMENDED`, while preserving
  `model_ready_breadth_allowed=false`. First-layer reopen gate denied reopen
  with `FIRST_LAYER_REOPEN_DENIED`; channel-specific v4 remained
  `CHANNEL_V4_REOPEN_EVIDENCE_INSUFFICIENT`; minimal forward diagnostic stayed
  `MINIMAL_FORWARD_DIAGNOSTIC_DISABLED_READY`. Promotion, paper-shadow,
  production, and broker all remained disabled.
- 2026-06-28: Validation passed Ruff, compileall, focused parallel pytest
  (`12 passed`), focused governance/report/task/documentation pytest
  (`39 passed`), `git diff --check`, and
  `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
  (`193 passed`). Runtime artifact:
  `outputs/validation_runtime/contract-validation_20260628T065717Z/test_runtime_summary.json`.
- 2026-06-28: Task created from owner roadmap. Implementation starts with
  free re-ablation, participation proxy validation, reopen gate, conditional v4
  fail-closed artifacts, and disabled minimal forward diagnostic scaffolding.
