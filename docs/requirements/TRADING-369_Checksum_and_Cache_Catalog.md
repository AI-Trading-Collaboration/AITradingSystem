# TRADING-369 Checksum and Cache Catalog

最后更新：2026-06-16

## Status

`DONE`

## Goal

Create a read-only cache catalog that records checksums and freshness metadata for price, market panel, and macro data caches before paper-shadow or research reports interpret cached inputs.

## Scope

- Define a governed cache catalog schema and policy manifest.
- Track cache path, artifact id, data type, as-of date, checksum, row count, column count, created timestamp, validated timestamp, source name, and refresh audit id.
- Detect checksum mismatches against the most recent catalog or explicit expected checksums.
- Detect missing catalog entries for configured required caches.
- Integrate cache catalog summary with data refresh audit trail, evidence staleness monitor, PIT source manifest, shadow continuation readiness report, and Reader Brief.
- Add `aits data cache-catalog run/report/validate`.
- Register the artifact family and update README, system flow, operations runbook, requirements, task register, and artifact catalog.
- Add focused tests.

## Out Of Scope

- No cache refresh, cache repair, data download, provider switching, or cache fabrication.
- No weakening of `aits validate-data`; cached-data-dependent workflows still need the existing quality gate.
- No scoring, backtest, paper-shadow state mutation, production mutation, official target weights, broker integration, order ticket, or automatic position control.

## Safety Boundary

- `production_effect=none`
- `read_only=true`
- `data_refresh_allowed=false`
- `cache_mutation_allowed=false`
- `cache_repair_allowed=false`
- `score_or_backtest_allowed=false`
- `broker_action_allowed=false`
- `order_ticket_allowed=false`
- `production_state_mutation_allowed=false`
- manual review only

## Design Decisions

- The first catalog is a governance artifact, not an authoritative storage index. It reads configured paths and existing artifacts, then reports observed state.
- Missing required cache entries are blocking because downstream data interpretation should fail closed when required cache evidence is absent.
- Checksum mismatch is blocking when a previous catalog or explicit expected checksum says the same cache artifact changed without a refreshed catalog lineage.
- `created_at` uses filesystem creation time where available; `validated_at` is inherited from latest validate-data audit sidecar or latest data refresh audit validation evidence.
- Source lineage comes from `config/data_sources.yaml`; a cache path may map to multiple active sources, so records retain `source_ids` and `source_names` instead of forcing a single provider.

## Implementation Steps

1. Add `config/cache_catalog.yaml` with required cache entries and policy metadata.
2. Add `src/ai_trading_system/cache_catalog.py` with payload builder, artifact writer, loader, resolver, validator, markdown renderer, and latest summary helper.
3. Add CLI under `aits data cache-catalog`.
4. Integrate latest cache catalog summary into data refresh audit, PIT source manifest, evidence staleness monitor, shadow continuation readiness, and Reader Brief.
5. Register the artifact family in `config/report_registry.yaml` and document it in `docs/artifact_catalog.md`, `README.md`, `docs/system_flow.md`, and `docs/operations/operations_runbook.md`.
6. Add focused tests for normal catalog generation, checksum mismatch detection, missing entry detection, CLI behavior, and downstream summary propagation.

## Acceptance Criteria

- `aits data cache-catalog run` produces JSON, Markdown, validation JSON/Markdown, Reader Brief section, and latest pointer.
- `aits data cache-catalog validate --latest` fails closed for missing required entries or checksum mismatch.
- Data refresh audit, PIT source manifest, evidence staleness monitor, shadow continuation readiness report, and Reader Brief expose cache catalog status.
- Report registry and documentation contract discover the new artifact family.
- Focused tests, ruff, compileall, git diff check, documentation contract, report index, Reader Brief, and Reader Brief quality pass.

## Progress Notes

- 2026-06-16: Task registered and implementation started after TRADING-368. Initial boundary is governance-only and read-only; no data refresh or cache mutation is allowed.
- 2026-06-16: Implementation completed. Real catalog `cache-catalog_2026-06-16_11d2420c5196cef6` passed with `cache_integrity_status=OK`, four entries, zero missing required entries, and zero checksum mismatches. Data refresh audit `data_refresh_audit_2026-06-16_4fa6aa0ac17fca43` and PIT manifest `pit_source_manifest_2026-06-16_1f5fc6458ef1d88f` expose the catalog summary. Focused pytest, Ruff, compileall, documentation contract, report index, Reader Brief latest/quality, explicit 2026-06-16 Reader Brief catalog verification, and git diff check passed or produced only documented local-context limitations.
