# TRADING-1221 to 1235 Execution Semantics Actual-Path Rebacktest Evidence Snapshot

## 背景

TRADING-1187～1200 已经生成 execution-semantics-aware actual-path rebacktest
runtime artifacts，但 `outputs/` 在本地 Git exclude 中不会提交。为了让 owner
后续复核和排序讨论有稳定证据，本批把 runtime artifact 的摘要、哈希和 warning
诊断固化为 tracked evidence snapshot。

本批只做 evidence snapshot 和 warning diagnosis，不重新打开 dynamic promotion。

## 范围

1. 读取 `outputs/research_strategies/execution_semantics/` 下的 rebacktest gate、
   aggregate rebacktest 和 5 个策略的 per-strategy artifacts。
2. 新增 tracked review report：
   `docs/research/execution_semantics_actual_path_rebacktest_review.md`。
3. 新增 compact machine-readable snapshot：
   `docs/research/artifact_snapshots/execution_semantics_actual_path_rebacktest_snapshot.yaml`。
4. 展开 `PASS_WITH_WARNINGS` 的具体原因，并判断是否影响 actual-path strategy ranking。
5. 同步 `docs/system_flow.md`、`docs/artifact_catalog.md` 和 task register。

## 验收标准

- Review report 包含 run timestamp、command used、policy registry path、strategies
  included、per-strategy artifact completeness、gate status、blocked reasons、data
  quality status、`PASS_WITH_WARNINGS` 明细、actual path vs target path 主要差异、
  lag cost summary、signal staleness summary 和 promotion readiness summary。
- Snapshot 不提交完整 runtime artifacts，只记录 artifact relative path、sha256、
  strategy id、metrics actual/target presence、promotion readiness status、data quality
  status 和 blocked reasons。
- 如果 `PASS_WITH_WARNINGS` 影响排序或指标可信度，warning diagnosis status 必须为
  `BLOCKED` 并列出 required action。
- 如果 `PASS_WITH_WARNINGS` 不影响排序，warning diagnosis status 必须为
  `NON_BLOCKING_WARNING`，并说明证据。
- Dynamic promotion 保持 blocked；所有新增文档不得暗示 paper-shadow、production 或
  broker 放行。

## 进展记录

- 2026-06-27：新增并进入 `IN_PROGRESS`。本批读取本地 actual-path rebacktest
  runtime artifacts，生成 tracked review report 和 compact snapshot，并把数据质量 warning
  与 dynamic promotion blocker 分开解释。
- 2026-06-27：实现完成并转入 `VALIDATING`。新增
  `docs/research/execution_semantics_actual_path_rebacktest_review.md` 与
  `docs/research/artifact_snapshots/execution_semantics_actual_path_rebacktest_snapshot.yaml`；
  `PASS_WITH_WARNINGS` 的唯一 WARNING 为主价格源 `prices_adjustment_ratio_jump` 1 行，
  诊断为 `NON_BLOCKING_WARNING`。Actual-path ranking 使用完整 artifacts 和 passed data gate，
  未发现该 warning 影响排序的证据；dynamic promotion 继续 `BLOCKED`。
