# TRADING-2046 to 2085 Free PIT Data Source Ingestion

最后更新：2026-06-28

## Status

- Task id: `TRADING-2046_to_2085_FREE_PIT_DATA_SOURCE_INGESTION`
- Status: `VALIDATING`
- Last updated: 2026-06-28
- Owner: system

## Scope

Build a research-only ingestion and feature-readiness layer for free, auditable
data sources that can support future first-layer channel reopen work. The
primary regime remains `ai_after_chatgpt`, anchored on 2022-11-30 with default
backtest start 2022-12-01.

The batch covers:

- free source scope and source-contract registry;
- FRED and ALFRED connector contracts, with vintage-aware constraints for
  revision-sensitive macro series;
- VIX free-source ingestion and cross-check status;
- official macro calendar connector contracts and calendar PIT fields;
- `rates_liquidity_free_v1`, `volatility_compression_free_v1`,
  `macro_event_calendar_free_v1`, `event_risk_free_v1`, and
  `participation_proxy_free_v1` readiness;
- PIT audit, coverage matrix, reopen readiness, owner brief, closeout, CLI, and
  guardrail tests.

## Non-Goals

- Do not buy paid data or integrate Norgate, CRSP, Bloomberg, Refinitiv,
  FactSet, OptionMetrics, ORATS, or similar paid sources.
- Do not build true PIT breadth or historical index constituents.
- Do not use current constituents to backfill historical breadth.
- Do not restart first-layer channel research, retrain first-layer models, or
  modify frozen second-layer probes.
- Do not enable owner review, promotion, paper-shadow, production, broker, or
  trading advice.

## Implementation Notes

The owner plan requested connector files under
`src/ai_trading_system/data_sources/`. This repository already has
`src/ai_trading_system/data_sources.py` as an importable module, so adding a
same-named package would create an ambiguous module/package boundary. The
durable implementation uses explicit adjacent modules:

- `src/ai_trading_system/free_data_connectors.py`
- `src/ai_trading_system/free_pit_data_sources.py`
- `src/ai_trading_system/features/*_free.py`

This is a permanent repository-layout adaptation, not a temporary workaround.

## Stages

1. Source scope and registry:
   create `docs/research/free_pit_data_source_scope.md`,
   `inputs/research_reviews/free_pit_data_source_scope.yaml`,
   `config/data/free_data_source_registry.yaml`, and
   `docs/research/free_data_source_registry_review.md`.
2. Connector and ingestion baseline:
   add connector classes and `aits data free-sources ingest|validate`; write
   processed free-source parquet artifacts when local inputs are available.
3. Feature families:
   generate free research feature families and expose policy metadata for
   heuristic thresholds through `config/research/free_feature_policy.yaml`.
4. Audit and readiness:
   write PIT audit, coverage matrix, feature-family reopen readiness, owner
   brief, closeout matrix, and diagnostic-only/bocked-family status.
5. Governance:
   update report registry, artifact catalog, system flow, task register, and
   guardrail tests.
6. Validation:
   run Ruff, compileall, focused parallel pytest, documentation/task/report
   contract tests, and diff checks.

## Acceptance Criteria

- Free source registry exists and validates source contracts.
- FRED/ALFRED/VIX/calendar connector contracts are represented in code and docs.
- Processed FRED market series, VIX history, and macro calendar artifacts are
  generated or explicitly marked as missing/diagnostic with warnings.
- Free feature families are generated where source coverage supports them.
- Revision-sensitive macro series without vintage support cannot be marked
  model-ready.
- Participation proxies are explicitly `DIAGNOSTIC_ONLY` and
  `NOT_TRUE_PIT_BREADTH`.
- All outputs keep `promotion_allowed=false`, `paper_shadow_allowed=false`,
  `production_allowed=false`, and `broker_action=none`.
- `docs/system_flow.md`, `docs/artifact_catalog.md`,
  `config/report_registry.yaml`, and `docs/task_register.md` are updated.
- Guardrail tests cover source contracts, PIT calendar warning behavior,
  diagnostic-only proxies, and no-promotion safety.

## Open Questions And Blockers

- Local cached rates currently include `DGS2`, `DGS10`, and `DTWEXBGS`; `DGS3MO`,
  `FEDFUNDS`, `SOFR`, and FRED `VIXCLS` may be absent until a future refresh.
- Official macro calendar historical `source_published_at` metadata may be
  unavailable from public pages; those rows must remain `PIT_WARNING` and
  diagnostic-only unless a verifiable timestamp is captured.
- True PIT breadth and survivorship-free universe construction remain blocked
  without a credible paid or owner-provided data source.

## Progress Log

- 2026-06-28: Implementation completed and moved to `VALIDATING`. Real run
  used `--as-of 2026-06-26`; data quality status was `PASS_WITH_WARNINGS`.
  Generated free source parquet artifacts, free feature parquet artifacts,
  source scope, registry review, FRED/VIX/calendar reviews, calendar PIT
  contract, PIT audit, coverage matrix, reopen readiness, participation proxy
  review, owner brief, and closeout/final matrix. Current blockers remain:
  `DGS3MO`, `FEDFUNDS`, `SOFR`, and `VIXCLS` missing from local FRED cache;
  official macro calendar event rows are not yet captured; true PIT breadth
  remains unavailable. Validation passed Ruff, compileall, focused free-source
  tests, first-layer/research governance tests, report/documentation/task
  contract tests, `aits data free-sources validate`, and diff checks.
- 2026-06-28: Task created from owner attachment; implementation started with
  research-only safety boundary and explicit free-source scope.
