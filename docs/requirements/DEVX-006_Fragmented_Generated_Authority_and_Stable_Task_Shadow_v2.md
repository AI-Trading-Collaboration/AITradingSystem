# DEVX-006：Fragmented Generated Authority 与 Stable Task Shadow v2

## 状态

- priority：P1
- status：PROPOSED
- owner：architecture coordinator + developer workflow owner
- source finding：OPS-070 2026-07-27 checkout-dirty stability audit
- production effect：none

## 问题

开发 checkout 中一次任务登记或少量代码变化会放大为大量 generated dirty paths：

1. task shadow fragment 包含 Markdown `line_number` 和 partition；中间插入/删除行会让后续
   fragments 级联变化，active→completed 还会发生 delete/add；
2. module/test manifests 是合理的全仓 aggregate，但 lane 中间刷新会重复制造 stale/
   refresh churn；
3. `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md` 仍是
   1.8–2.06 MB monolith；现有 18 个 fragments 全部 inactive，不能 lossless 重建；
4. 1.87 MB append-only compatibility baseline 与 851 KB 中央测试是跨任务冲突热点。

这些 dirty paths 是真实开发证据，不应由 daily operation 声明为 owned/shared，也不应通过
auto-stash/clean/reset 消失。OPS-070 的独立 runtime clone 负责隔离运营；本任务只降低开发
侧冲突放大。

## 分阶段方案

### A：立即执行的协调规则

- worker 只修改 leaf/domain 文件；
- task register、report registry、artifact catalog、system flow、compatibility authority、
  module/test manifests 与 aggregate indexes 全部 coordinator-only；
- lane 中允许 deterministic generated freshness 暂时 stale；
- 只在最终 integration candidate 上刷新一次 shared/generated state，并在该 final tree
  运行 formal freshness/Architecture/Contract/Full。

### B：Task shadow v2

- Markdown task register 暂时仍是 source of truth，不越权提前执行 ARCH-005 S5；
- per-task fragment 继续使用 stable task-id path，保留 raw row hash、全部 cells 和事件证据；
- current partition/line number 移到 baseline/index，不再进入每个 task fragment identity；
- v1/v2 side-by-side 生成，证明 task mapping、active/completed partition、raw cells、排序、
  duplicate/tamper/freshness fail-closed parity 后，才一次性迁移。

### C：Compatibility authority fragmentation

- 冻结现有 v1 monolith 历史 bytes；
- 新 section 写独立 content-addressed fragment；
- 小型 hash-chain index 记录 section id/path/hash/previous hash/order；
- loader 同时读取 legacy prefix + fragments，证明 mapping/order/hash parity 后才停止向
  monolith 追加。

### D：Report/catalog/flow lossless fragments

- 不激活现有 shallow inactive fragments；
- 定义 full-entry v2 fragments，逐项覆盖完整 registry/catalog/flow 语义；
- shadow render 必须对当前 monolith byte-identical、100% coverage、0 silent drop；
- 所有 direct consumers 完成迁移并经过 owner review 后，才允许 source-of-truth cutover。

## 验收

- task register 中间插入一行只改变目标/new fragment、baseline 与 index，不改变后续任务
  fragments；
- active→completed 保留 stable task identity 和 event lineage；
- compatibility loader 对 legacy + fragments 的 section order/hash 完全可重放；
- report/catalog/flow v2 shadow render 与 monolith byte-identical，missing/duplicate/tamper
  fail closed；
- 131 个 task-register literal consumers 及 report/catalog/flow direct consumers 有完整
  inventory、迁移状态和 rollback；
- focused、Architecture、Contract、Integration、Reproducibility、Full parallel validation
  通过；
- 不修改 daily scheduler、data/PIT/scoring/backtest 语义，不写 weights，不触发 broker/
  trading，`production_effect=none`。

## 风险与 cutover 边界

- 当前仅登记 `PROPOSED`，未授权 S5 或任何 canonical source cutover；
- line number 从 fragment 移到 index 不能丢失 legacy byte/row traceability；
- 现有 55 条多于 8 cells 的 legacy rows 必须原样保留；
- 任何不能证明 lossless parity 的 fragment 只能保持 inactive shadow；
- OPS-070 不等待本任务完成，因 permanent independent runtime 已消除 daily 对开发 dirty
  inventory 的依赖。
