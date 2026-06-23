# TRADING-701 Dynamic Trend Full-Advisory Expansion
最后更新：2026-06-23

## 状态

- 状态：VALIDATING
- 日期：2026-06-20
- 范围：validation-only full-advisory expansion audit
- Owner：系统实现 + 项目 owner 后续复核

## 背景

TRADING-700 的 dynamic/trend bridge consistency audit 基线为：

- `full_advisory_case_count=22`
- `backtest_bridge_case_count=1728`
- `recommendation=sensitivity_tested_only`
- `evidence_strength=low`
- 3 个 threshold 都是 `insufficient_full_advisory_to_assess`

当前缺口是 full-advisory equivalent trace 样本太少，导致 bridge evidence 不能用于判断
source-layer consistency。

## 目标

1. 审计为什么 `full_advisory_case_count` 只有 22：
   - PIT feature availability gate
   - lineage manifest
   - `score-daily` trace 缺失
   - `decision_time` 不匹配
   - artifact 不可证明 full advisory equivalent
   - historical replay config 限制
2. 在不放宽 production gate 的前提下，尽可能纳入更多 historical
   `score-daily` full-advisory traces。
3. 输出 `dynamic_trend_full_advisory_expansion_report.json/md`。
4. 重新运行 `dynamic_trend_bridge_consistency_audit`。

## 输出要求

Expansion report summary 必须包含：

- `requested_date_count`
- `eligible_date_count`
- `blocked_date_count`
- `full_advisory_case_count_before`
- `full_advisory_case_count_after`
- `blocked_by_reason`
- `repairable_without_relaxing_gate`
- `expected_pit_limitation_count`
- `lineage_missing_count`
- `replay_config_issue_count`

同时输出：

- `date_expansion_audit`
- `candidate_trace_paths_before`
- `candidate_trace_paths_after`
- `generated_expanded_trace_path`
- `consistency_audit_after_expansion_summary`

## 安全边界

- 不修改任何 threshold current value。
- 不修改 production / paper-shadow / official weights。
- 不放宽 `aits validate-data`、PIT feature availability、lineage manifest 或
  production-equivalent proof gate。
- `production_effect=none`
- `promotion_gate_allowed=false`
- `paper_shadow_change_allowed=false`
- `production_weight_change_allowed=false`
- `backtest_trace_bridge` 不能单独作为 promotion gate evidence。
- 本轮不得升级为 `VALIDATED_BOUNDARY`。

## Reliability 升级条件

只有当 full-advisory source layer 样本达到 consistency assessment floor，且
direction agreement 不冲突时，才允许将 bridge reliability 从
`insufficient_full_advisory_to_assess` 升级到：

- `bridge_directionally_consistent_but_magnitude_uncertain`
- `bridge_consistent_with_full_advisory`

即使 bridge 一致，也只能作为 validation recommendation，不能修改 threshold value。

## 实现计划

1. 读取 TRADING-699/700 sensitivity 与 bridge consistency baseline。
2. 读取 historical trace gate availability audit root 和 expanded trace source paths。
3. 扫描 date-level `daily_indicator_weight_trace.json`，只把通过 data-quality、
   feature-availability 和 lineage proof 的 trace 纳入 candidate set。
4. 如果发现新增 full-advisory trace，写出 validation-only rebuilt expanded trace。
5. 用 rebuilt trace 重新运行 dynamic/trend sensitivity review 和 bridge consistency audit。
6. 写出 expansion report，并纳入 validation pack summary。

## 验收标准

- Expansion report 明确说明 22 个 full-advisory cases 的阻塞来源。
- 如果没有可修复日期，after count 必须保持 22，并说明主要 blocker。
- 不允许把 PIT gate fail 的日期纳入 full-advisory equivalence。
- consistency audit rerun 继续输出 no-change safety flags。
- focused tests、CLI、Ruff、py_compile、`git diff --check` 和 validation tier 通过。

## 进度记录

- 2026-06-20：按 owner 指令新增；进入 IN_PROGRESS；实现前固定 validation-only
  safety boundary、full-advisory expansion 字段契约和 bridge reliability 升级条件。
- 2026-06-20：实现 `dynamic-trend-full-advisory-expansion` report，并纳入
  validation pack summary。真实 CLI 使用 expanded historical trace、2026-06-17
  prices、historical gate audit root 和 coverage-extension root 后输出
  `requested_date_count=40`、`eligible_date_count=22`、`blocked_date_count=18`、
  `full_advisory_case_count_before=22`、`full_advisory_case_count_after=22`、
  `blocked_by_reason={expected_pit_limitation:18}`、
  `repairable_without_relaxing_gate=false`、`lineage_missing_count=0`、
  `replay_config_issue_count=0`。重新运行 dynamic/trend sensitivity review 与
  bridge consistency audit 后，3 个 threshold 仍为
  `insufficient_full_advisory_to_assess`，`evidence_strength=low`，
  `recommendation=sensitivity_tested_only`，`validated_boundary_count=0`，
  `thresholds_changed_count=0`，`production_effect=none`。
- 2026-06-20：验证通过 py_compile、Ruff、Black check、focused indicator pytest
  44 passed、docs/task/config 并行 pytest 21 passed、`git diff --check` 和 full
  并行 validation tier 2985 passed / 643 warnings / 166.26s，runtime artifact
  `outputs/validation_runtime/full_20260620T145657Z/test_runtime_summary.json`。
