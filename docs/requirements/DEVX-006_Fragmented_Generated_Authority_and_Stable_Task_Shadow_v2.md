# DEVX-006：Fragmented Generated Authority 与 Stable Task Shadow v2

## 状态

- priority：P1
- status：IN_PROGRESS（Task Shadow v2 B 波完成；C 波已独立授权并推进，D/S5 仍串行锁定）
- owner：architecture coordinator + developer workflow owner
- source finding：OPS-070 2026-07-27 checkout-dirty stability audit
- production effect：none

## 2026-08-09 C → D → S5 串行授权

Owner 决定：
`owner_decision:DEVX-006C:2026-08-09:authorize_c_then_d_then_s5_serial_v1`

TRADING-2504 已由其单一 coordinator 完成 ordinary push、cleanup 与 resource release；released
exact main=`cb437a4d4be178180f60cb3ee2d2994c1be45f94`。Owner 要求按 C → D → S5 顺序
真正解决 shared path 冲突。本 parent task 因此重新进入 `IN_PROGRESS`，但当前只开放独立任务
`DEVX-006C_COMPATIBILITY_AUTHORITY_FRAGMENTATION`：

- C 波 requirement：`docs/requirements/DEVX-006C_Compatibility_Authority_Fragmentation.md`；
- D 波必须等待 C ordinary push/cleanup/resource release 后从新 exact main 独立登记；
- ARCH-005 S5 必须再等待 D 波完成并独立登记；
- 不允许复用前序 frozen tree formal evidence，也不允许三个 cutover 并行或合并为一次 mutation。

2026-08-09 C 波进展：legacy compatibility monolith 已按 exact base seal 为 immutable prefix；
dynamic inventory 识别 8 个 consumer，growth-assuming direct read 从 135 降为 0、runtime append
writer 为 0。Canonical JSON fragment、hash-chain index、merged loader、explicit legacy reader 与
rollback/tamper contract 已实现；当前正在 final-tree validation，D/S5 尚未登记或启动。

## 2026-07-30 当前授权波与顺序

Owner 要求在恢复策略线与 Atlas 两条线并行前，先串行解除 shared/generated 冲突放大。
本轮执行顺序为：

1. 完成既有 `TRADING-2465` 并释放 checkout；
2. 恢复 Git canonical 与 installed `run-governed-development` bundle 的 byte parity；
3. 以单一 coordinator lane 实现 Task Shadow v2 side-by-side、动态 consumer inventory 和
   单行变更稳定性证明；
4. 仅在 governed final-tree validation 确认可并行后，从同一新 `main` 恢复两条独立线。

第 1～3 步已经完成。B 波实现与 final-candidate 正式验证均通过；长期 priority 仍为 P1。
第 4 步只剩 task commit、local-main fast-forward、ordinary push、clean audit 与向暂停线程
发送 exact 新 `main`，任何一项失败都保持并行暂停。

原 B 波明确不授权：

- Markdown task register 或 v1 shadow 的 source-of-truth cutover；
- ARCH-005 S5、dual write、task status 自动写回或 scheduler dispatch；
- compatibility authority fragmentation（C 波；已由 2026-08-09 上述后续 Owner 决定取代）；
- report registry、artifact catalog、system flow source fragmentation（D 波）；
- production、broker、trading、data/PIT、scoring 或 backtest 语义变化。

## 当前 B 波实施分解

### B0：任务与合同冻结

- task register 保持 P1，但状态改为 `IN_PROGRESS`；
- legacy Markdown 与 `arch_005_task_shadow_fragment.v1` 继续是既有 shadow authority；
- v2 只新增 inactive side-by-side contract，所有 cutover 另需 Owner review。

### B1：Stable fragment 与定位 index

- v2 fragment path 只由 stable task id digest 决定，不包含 active/completed partition；
- v2 fragment 不保存 current partition、source path 或 Markdown line number；
- fragment 保留 task id、全部 raw cells、raw row、row hash、first-eight projection、docs links、
  terminal projection和legacy import event；
- v2 index/baseline 负责保存 current partition、source path、line number、排序与document
  checksum，因此定位信息不丢失但不再污染每个 task fragment identity。

### B2：动态 consumer inventory

- inventory 从当前仓库重新计算，不把历史 `131` 写死；
- 2026-07-30 启动快照为 `134`：9 runtime、122 test、3 script；
- 每个 consumer 记录path、role、targets、reference count、migration status与rollback；
- 本波仅盘点，所有 direct consumer 保持读取 legacy Markdown。

### B3：side-by-side 生成与稳定性证明

- v1/v2 在同一 generator 命令中生成和验证；
- v2 task mapping、partition classification、raw cells、排序、duplicate/tamper/freshness 与
  byte-identical compatibility render 必须和 legacy/v1 parity；
- 测试必须证明在同一 register 中间插入一个新 row 时，已有 task 的 v2 path 与 fragment
  bytes 均不变化，仅新增 task fragment和baseline/index发生变化；
- 测试必须证明 active→completed 后 task path 保持不变，index locator与event/raw row按
  新source更新。

### B4：验证、集成与恢复并行门禁

- focused 测试使用 pytest-xdist；
- 刷新 v1/v2 shadow、module/test manifests与必要append-only compatibility authority；
- Architecture、Contract、Integration、Reproducibility、Full 在final candidate执行；
- local-main fast-forward、ordinary push与SHA复核通过后，受治理audit必须无task-owned
  dirty paths、无active lease；
- 只有满足以上条件，才向暂停线程发送“parallel gate PASS”，并要求策略与Atlas从同一
  exact新main建立隔离worktree与互斥claims。

### 2026-07-30 B 波结果

- v1/v2 对 928 个 tasks 均可 byte-identical replay；v2 一次性建立 928 个 stable task-id
  fragments；
- 中间插入 task row 时，既有 v2 path/payload 变化数为 0；active→completed 只移动 index
  locator，不移动 fragment path；
- v1 仅因 DEVX-006 自身 task row 状态变化而更新 1 个 fragment，没有级联改写；
- dynamic consumer inventory 为 134：9 runtime、122 test、3 script；全部保持
  `LEGACY_DIRECT_OR_LITERAL_CONSUMER` 和 `READ_LEGACY_MARKDOWN_DIRECT` rollback；
- focused=`162 passed`；Architecture=`805 passed`；Contract=`276 passed`；
  Integration=`995 passed`；Reproducibility=`24 passed`；Full=`7749 passed / 3 skipped`；
- 所有正式 tier 使用 16 workers / `loadfile`，没有串行替代失败；现有 NumPy/asyncio
  warnings 不影响门禁；
- source-of-truth 仍为 `LEGACY_MARKDOWN_ONLY`，`source_cutover_allowed=false`，
  `production_effect=none`、`broker_action=none`。

## 当前工作区生命周期

- frozen base：`1029c8568a08363f4431e73a709e231c5a466e42`；
- task branch：`codex/devx-006-task-shadow-v2`；
- 本波不创建额外临时worktree或clone；
- 既有 `D:\Work\AITradingSystem_ops_runtime_20260725` 与
  `D:\Work\AITradingSystem_t2463_target_redesign` 不属于本任务，不读取、不修改、不清理；
- known-unrelated `docs/research/growth_tilt_owner_diagnosis_pack.md` 继续由exact exclusion
  保护，本任务不读取、不修改。

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
- 134 个 task-register literal consumers 及 report/catalog/flow direct consumers 有完整
  inventory、迁移状态和 rollback；
- focused、Architecture、Contract、Integration、Reproducibility、Full parallel validation
  通过；
- 不修改 daily scheduler、data/PIT/scoring/backtest 语义，不写 weights，不触发 broker/
  trading，`production_effect=none`。

## 风险与 cutover 边界

- 当前只授权 DEVX-006C compatibility source cutover；D 波与 ARCH-005 S5 仍未登记、未授权实施；
- line number 从 fragment 移到 index 不能丢失 legacy byte/row traceability；
- 现有 55 条多于 8 cells 的 legacy rows 必须原样保留；
- 任何不能证明 lossless parity 的 fragment 只能保持 inactive shadow；
- OPS-070 不等待本任务完成，因 permanent independent runtime 已消除 daily 对开发 dirty
  inventory 的依赖。
