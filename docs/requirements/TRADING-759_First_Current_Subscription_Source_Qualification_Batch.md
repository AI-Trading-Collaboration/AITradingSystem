# TRADING-759: First Current-Subscription Source Qualification Batch

## 背景

TRADING-739～758 已合入 validation-only baseline，可以把 current subscription access 进一步组织成 source qualification 和 controlled strategy research artifacts。本任务执行第一轮真实 current-subscription batch，读取当前本地 subscription coverage / qualification artifacts，重跑受控 qualification commands，并生成一份 batch rollup。

本轮不得把 endpoint accessible、contract-ready、teacher/oracle、benchmark/control、reverse diagnostic 或 regret casebook 输出解释为 promotion、paper-shadow 或 production evidence。

## 市场 regime

- regime：`ai_after_chatgpt`
- anchor event：ChatGPT public launch on 2022-11-30
- default start：2022-12-01

## 范围

1. 运行 data source usage guardrails，输出 current-view-only、research-label-only、blocked-until-qualified violation counts。
2. 运行 FMP price / corporate action qualification，覆盖 historical EOD full/light/non-split-adjusted、dividends、splits 和 delisted companies。
3. 运行 Marketstack second-source reconciliation qualification，代表性 universe 为 SPY、QQQ、SMH、MSFT、GOOGL、NVDA、AMD、TSM。
4. 运行 forward evidence archive reclassification，确认 TRADING-737-P0-08 是否可从 `requires_new_paid_source` 改为 `internal_capture_requirement`。
5. 运行 data foundation acceptance v2，输出 readiness level 和 source status counts。
6. 如果 acceptance v2 至少达到 `DIAGNOSTIC_ONLY_READY`，运行小型 controlled strategy pilot：benchmark/control batch、one reverse diagnostic、one regret casebook pilot。
7. 输出 batch review，每个候选只能标记 `CONTINUE`、`PAUSE`、`WATCHLIST`、`KILL` 或 `DATA_REQUIRED`。

## Acceptance Criteria

- 所有 artifacts 固定 `production_effect=none`、`broker_action=none`、`promotion_gate_allowed=false`、`paper_shadow_change_allowed=false`、`production_weight_change_allowed=false`、`lookahead_violation_count=0`。
- 不记录任何 API key 原文；batch rollup 只能引用 provider / endpoint / sanitized status。
- Acceptance v2 summary 必须显式输出 `minimum_research_readiness_level`、`promotion_candidate_after_qualification_count`、`diagnostic_only_count`、`blocked_until_qualified_count`、`current_view_only_count`、`research_label_only_count` 和 `lookahead_violation_count`。
- Marketstack 只能保持 second-source reconciliation；如果缺少 refreshed FMP / Marketstack row snapshots，price / split / dividend discrepancy 必须输出 `DATA_REQUIRED`，不能写成 PASS。
- Controlled strategy pilot 只允许 diagnostic-only / controlled-research-only / blocked 结论，不允许 promotion、paper-shadow 或 production review。

## Progress Notes

- 2026-06-21：新增 TRADING-759 requirement；实现目标是 batch runner + rollup，不升级 source status，不放宽 PIT / data-quality / lineage gate。
- 2026-06-21：实现 `aits data source-qualification first-batch` 并执行第一轮真实 current-subscription batch；输出 `outputs/data_quality/current_subscription_source_qualification_batch/current_subscription_source_qualification_batch_review.json/md`，状态为 `PASS_WITH_DATA_REQUIRED`。
- 2026-06-21：本轮结果：usage guardrail violation counts 均为 0；FMP 6 个目标 endpoints 均生成 manifest/policy contract，remaining PIT gaps 为 provider timestamp unavailable、available-time conservative assumption、as-of/lineage owner review required、delisted membership validation pending；Marketstack 保持 second-source-only，row-snapshot coverage ratio=0.125，price/split/dividend discrepancy 均为 `DATA_REQUIRED`；TRADING-737-P0-08 reclassified to `internal_capture_requirement`，不需要新增 paid source；acceptance v2 为 `CONTROLLED_RESEARCH_READY`，但不允许 promotion / paper-shadow / production review。
- 2026-06-21：小型 controlled pilot 已运行 benchmark/control batch、one reverse diagnostic summary 和 one regret casebook pilot；candidate decisions 为 FMP `WATCHLIST`、Marketstack `DATA_REQUIRED`、forward evidence `CONTINUE`、benchmark controls `CONTINUE`、reverse diagnostic `WATCHLIST`、regret casebook `WATCHLIST`；所有 outputs 继续 `production_effect=none`、`broker_action=none`、`promotion_gate_allowed=false`、`paper_shadow_change_allowed=false`、`production_weight_change_allowed=false`、`lookahead_violation_count=0`。
- 2026-06-21：验证通过 Ruff、scoped Black、compileall、focused/docs parallel pytest 33 passed、`fast-unit` 103 passed、`contract-validation` 102 passed、`report-validation` 55 passed；runtime artifacts 写入 `outputs/validation_runtime/fast-unit_20260621T044313Z/`、`outputs/validation_runtime/contract-validation_20260621T044403Z/`、`outputs/validation_runtime/report-validation_20260621T044453Z/`。
