# TRADING-702 PIT Data Source Readiness Audit

## 状态

- 状态：VALIDATING
- 日期：2026-06-21
- 范围：validation-only PIT data source readiness audit
- Owner：系统实现 + 项目 owner 后续复核

## 背景

TRADING-701 确认 dynamic/trend full-advisory equivalent trace 样本仍停留在 22，
另有 18 个 requested dates 被归为 `expected_pit_limitation`。当前分类过粗，无法
区分真正不可在 decision time 前获得的数据、timestamp/manifest/config 可修复问题、
vendor current-view-only 风险、reconstruction gap gap、calendar/asset mapping
或仍需人工复核的数据源缺口。

## 目标

1. 梳理所有会阻断 full-advisory replay 的 PIT-sensitive features，至少覆盖：
   - SEC / EDGAR reconstructed PIT features
   - fundamental / valuation features
   - macro / calendar event features
   - price / volume / volatility features
   - trend / risk-on / risk-off features
   - data_quality_gate dependencies
2. 为每个 feature 建立 PIT availability contract：
   - `feature_id`
   - `source`
   - `raw_event_time`
   - `release_time`
   - `accepted_time`
   - `available_time`
   - `ingestion_time`
   - `decision_time`
   - `revision_policy`
   - `as_of_snapshot_available`
   - `current_view_only_risk`
   - `lookahead_risk`
   - `fail_closed_rule`
3. 对历史 blocked dates 重新分类：
   - `true_not_available_before_decision_time`
   - `availability_timestamp_missing`
   - `timestamp_model_too_conservative`
   - `source_snapshot_missing`
   - `reconstruction_gap`
   - `lineage_manifest_missing`
   - `replay_config_issue`
   - `vendor_current_view_only`
4. 输出 `outputs/research_indicators/pit_source_readiness_audit.json/md`。

## 实现设计

- Contract registry：`config/research/pit_feature_availability_contracts.yaml`
- CLI：`aits research indicators pit-source-readiness-audit`
- Direct artifact root：`outputs/research_indicators/`
- Validation pack artifact root：`outputs/research_indicators/control_plane_v1_validation/`
- Validation pack summary key：`pit_source_readiness_summary`

CLI 支持：

- `--start-date/--end-date`
- `--event-window-start/--event-window-end`
- `--asset-universe`
- `--trace-path`
- `--gate-audit-root`
- `--feature-availability-config`

## 输出要求

Report summary 必须包含：

- `total_blocked_dates`
- `blocked_by_feature`
- `blocked_by_reason_class`
- `repairable_without_relaxing_gate_count`
- `not_repairable_true_pit_limitation_count`
- `candidate_data_source_needed`
- `expected_full_advisory_case_gain_if_repaired`

同时输出：

- `pit_availability_contracts`
- `blocked_date_reclassification`
- `pit_sensitive_feature_inventory`
- `next_recommendations`

## 安全边界

- validation-only。
- 不修改 production / paper-shadow / official weights。
- 不修改任何 threshold current value。
- 不放宽 `aits validate-data`、feature availability、lineage manifest、PIT snapshot
  或 production-equivalent proof gate。
- 不允许用未来数据补 signal。
- 所有可修复建议必须满足：
  - `available_time <= decision_time`
  - as-of snapshot 可复现
  - trace manifest 可证明
  - production gate 不放宽
- `production_effect=none`
- `promotion_gate_allowed=false`
- `paper_shadow_change_allowed=false`
- `production_weight_change_allowed=false`

## 下一步建议枚举

- `fix_timestamp_or_manifest`
- `adopt_more_reliable_pit_source`
- `maintain_fail_closed`
- `component_level_diagnostic_only`

## 验收标准

- `pit_source_readiness_audit.json/md` 可由 CLI 生成，并纳入 validation pack summary。
- 历史 blocked dates 不再只汇总为 `expected_pit_limitation`，而是输出细分 reason
  class 和 feature/source 维度。
- 对真正 available_time 晚于 decision_time 的日期保持 fail-closed，不给 full-advisory
  case gain。
- 对 timestamp/manifest/config/source snapshot 类型缺口给出 repairability 和候选数据源。
- focused tests、CLI、Ruff、py_compile、`git diff --check` 和 validation tier 通过或明确记录阻塞。

## 进度记录

- 2026-06-21：按 owner 指令新增；进入 IN_PROGRESS；实现前固定 validation-only
  safety boundary、PIT availability contract 字段、blocked-date reclassification
  枚举和 no-gate-relaxation 验收标准。
- 2026-06-21：实现 baseline audit。新增 PIT feature availability contract registry、
  `pit-source-readiness-audit` builder/CLI、blocked-date reason reclassification、
  validation-pack `pit_source_readiness_summary` 和 focused tests；所有输出保持
  `production_effect=none`、`promotion_gate_allowed=false`、
  `paper_shadow_change_allowed=false`、`production_weight_change_allowed=false`，
  不放宽 data quality / feature availability / lineage gates。
- 2026-06-21：真实 CLI 输出
  `outputs/research_indicators/pit_source_readiness_audit.json/md`，summary 为
  `pit_contract_count=26`、`missing_availability_contract_count=0`、
  `total_blocked_dates=18`、
  `blocked_by_feature={sec_fundamentals_filing:18}`、
  `blocked_by_reason_class={true_not_available_before_decision_time:18}`、
  `repairable_without_relaxing_gate_count=0`、
  `not_repairable_true_pit_limitation_count=18`、
  `candidate_data_source_needed=[]`、
  `expected_full_advisory_case_gain_if_repaired=0`、
  `next_recommendation_counts={maintain_fail_closed:18}`；validation pack 已同步
  `pit_source_readiness_summary`，仍为 validation-only / no production effect。
- 2026-06-21：验证完成并保持 VALIDATING。最终 taxonomy / CLI option surface
  复核通过 focused PIT pytest `5 passed`、focused CLI/docs/config xdist
  `30 passed`、Black check、Ruff、py_compile、PIT CLI smoke、validation-pack smoke、
  stale taxonomy sweep 和 `git diff --check`。较早的并行 full tier
  `--workers 8` 通过 `2990 passed / 642 warnings / 265.1s`，runtime artifact 为
  `outputs/validation_runtime/full_20260620T160055Z/test_runtime_summary.json`；之后针对
  最终 CLI option 增量重跑 full tier 时出现 xdist worker termination / workspace
  taxonomy reversion instability，未静默改用 serial PASS，最终增量以 focused 并行测试和
  真实 CLI smoke 覆盖。
