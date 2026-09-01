# TRADING-2553：QQQ Options Paired Comparison Wave A Non-Executable Implementation V1

最后更新：2026-09-01

- stable task id：`TRADING-2553_QQQ_OPTIONS_PAIRED_COMPARISON_WAVE_A_IMPLEMENTATION_V1`
- priority：`P0`
- status：`BASELINE_DONE`
- task class：`NON_EXECUTABLE_DATA_RESEARCH_IMPLEMENTATION`
- production effect：`none`
- broker action：`none`

## 1. Owner 决定与精确边界

Project Owner 在 TRADING-2552 Owner review 完成后回复“批准”。本任务把该回复精确解释为：
批准 Wave A 的 fixture-only、non-executable 实现，包括 fully-funded QQQ/cash comparator、
aggregate-only exporter contract、local admission/reducer/independent replay 与 synthetic/golden/negative tests。

Owner decision：
`owner_decision:TRADING-2553:2026-09-01:authorize_non_executable_paired_comparison_wave_a_v1`

本决定不授权：

- 市场数据或信号 package 读取/下载、cache mutation 或 DQ；
- exact run manifest 生成；
- QuantConnect save/build/backtest/retry 或 provider/Object Store/public share；
- raw option rows、chain、SID、contract quote history 或本地 option repricing；
- paper/live/production/broker 或任何 orders/fills/positions。

## 2. 精确继承的冻结权威

- paired contract file SHA-256：
  `8c748634f6869eb4d4e9dfb14493acd072d146074ce7e86462eec0adae15714a`；
- paired contract canonical SHA-256：
  `6f77cf17af6e435799a2e86e1fb6a81936368e053b2367efb3a8e2be13412267`；
- freeze-admission file SHA-256：
  `fbedb47e5f2a748dc75669faabee9641ba7e0596de4ad8c340ed7ebcbd4c5c76`；
- signal：`first_layer_composer_v2:trend_state`，五状态 mapping、37-slot option policy、
  `2021-02-22..2025-12-02` / XNYS `1202` sessions / `83` transitions 全部 immutable；
- primary comparator：`SAME_SIGNAL_FULLY_FUNDED_QQQ_CASH_ACCOUNT`；
- common capital：optionized/underlying 均为 `USD 100,000`；
- terminal precedence：`INVALID > FAIL > INSUFFICIENT > PASS`。

TRADING-2553 不修改上述 contract、freeze admission、signal、mapping、37-slot policy、
historical aggregate 或任何 investment threshold。

## 3. 实现设计

### 3.1 Fully-funded virtual QQQ/cash ledger

实现纯本地、deterministic `Decimal` ledger，只消费 fixture quote/event：

- `LONG_CALL` effective event 在当前 QQQ ask 以整数 shares 部署不超过现金的 QQQ；
- `FLAT` effective event 在当前 QQQ bid 平掉 virtual shares；
- 无合格 option contract 不影响 underlying exposure，option lifecycle 不触发 comparator roll；
- 禁止 negative cash、margin、leverage、short QQQ 与非单调 event clock；
- 维护 cash/shares/equity/P&L/return/peak/drawdown/time-in-market/capital-at-risk-time/
  quote-availability/event-alignment 聚合证据。

underlying fee 未在冻结 paired contract 中给出费率。Wave A 不默认一个会改变 estimand
的费率：每次 fixture event 必须显式传入非负 fee，未提供时 fail closed。Wave B 在生成
exact manifest 前必须精确冻结平台 comparator fee semantics；Wave A tests 可使用明示的
zero-fee fixture，但不把它升级为真实运行默认。

### 3.2 QC adapter fragment

生成 LF-normalized、deterministic 的 standalone QC helper fragment，包含 virtual ledger 与
aggregate field allowlist，但不生成 runnable `main.py`、project target、manifest 或 dispatch package。
Wave B 只能在本 Wave final-tree validation PASS 后把该 fragment 嵌入 exact project。

### 3.3 Export-safe aggregate admission

- exact-load paired contract/freeze admission 并从 contract 导出六组大写 field inventory；
- aggregate 顶层 keys 必须与冻结 inventory 精确相等，不允许 extra/missing/unknown；
- 拒绝 raw rows、complete chain、option contract identifier/SID/quote history/local repricing input；
- 只接纳 JSON-safe scalar、有界 counts 与冻结 named diagnostic aggregate；
- canonical JSON 与 SHA-256 使 independent replay 字节级可重放。

### 3.4 Local 16-axis reducer/replay

QC aggregate 不新增冻结外字段。无法仅由 aggregate 证明的 manifest identity、
37-slot replay 和五个 calendar partitions 通过独立 `ReplayContext` fixture 传入；该 context 是本地
admission input，不是 QC export surface。Reducer 必须：

1. 覆盖且只覆盖冻结 16 axis IDs；
2. 对每轴输出 `PASS|FAIL|INSUFFICIENT|INVALID` 与 typed reason；
3. 按 `INVALID > FAIL > INSUFFICIENT > PASS` 唯一归并；
4. 缺失/unknown/not-evaluated 不得默认 PASS；
5. 对相同 aggregate/context 产生字节级相同的 replay receipt。

## 4. 实施步骤与验收

### Stage A1：contract-bound models

- exact-load frozen contract/admission；
- 实现 ledger、aggregate inventory 与 strict validators；
- synthetic tests 覆盖 entry/exit、cash remainder、bid/ask、fee、drawdown、no-option-contract
  independence 与 fail-closed chronology。

### Stage A2：adapter/export/reducer

- deterministic QC helper fragment；
- aggregate canonicalization/admission；
- 16-axis context-bound evaluation/reducer/replay receipt；
- golden bytes、missing/extra/raw-field、identity drift、account reconciliation、event mismatch、
  positive/nonpositive estimand 和 precedence negative tests。

### Stage A3：governed closeout

- 更新 `docs/system_flow.md`；
- task 进入 `BASELINE_DONE`，next owner 仅为 Project Owner 审批 Wave B exact manifest；
- focused + applicable formal validation PASS，生成唯一 candidate；
- fast-forward local `main`、普通 push `origin/main`、复核 SHA 并清理 worktree。

总验收条件：不修改冻结决策或收益结论；不读取真实数据；不产生 manifest；
QuantConnect/provider/DQ/backtest/paper/live/production/broker/orders/fills/positions 全为
`0/false/none`。

## 5. Path ownership 与临时工作区生命周期

- worktree：`D:\Work\AITradingSystem_trading2553_wave_a`；
- branch：`codex/trading-2553-paired-comparison-wave-a`；
- frozen base：`43a3d7169afbd0c4db2f6d952577fc00bef912cf`；
- owner：`TRADING-2553_QQQ_OPTIONS_PAIRED_COMPARISON_WAVE_A_IMPLEMENTATION_V1`；
- purpose：隔离共享 checkout 中尚未提交的 TRADING-2550 用户改动，完成 Wave A 实现、
  validation 与唯一发布 candidate；
- exit condition：candidate 已进入 local/origin `main`，formal evidence 已复制并验证到
  主 checkout，无进程依赖，tracked/untracked/ignored 无唯一未保全内容；
- deletion allowlist：仅限精确路径 `D:\Work\AITradingSystem_trading2553_wave_a`，满足 exit
  condition 后使用 `git worktree remove`。tracked bytes 可由 Git/main 恢复；清理后未保全的
  ignored bytes 不可恢复。

known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 不读取、不 hash、
不 diff、不 stage、不修改。

## 6. 当前状态

- implementation：`FIXTURE_BASELINE_IMPLEMENTED_FOCUSED_VALIDATION_PASS`；
- current paired outcome：`INSUFFICIENT_PLATFORM_EVIDENCE`；
- manifest authorized：`false`；
- external/data/trading counters：`0`；
- next owner：Project Owner 独立审阅并授权 Wave B fee semantics、exact package/manifest 与
  bounded QuantConnect run；在此之前 Codex 只完成本任务 formal publication。

## 7. 进度记录

- 2026-09-01：Owner 批准 Wave A fixture-only non-executable implementation。READ_ONLY governed
  preflight=`PASS`，local/origin main=`43a3d7169afbd0c4db2f6d952577fc00bef912cf`，active
  lease=`0`。共享 checkout 仍保留 TRADING-2550 用户改动；本任务已从 exact main 创建上述
  isolated worktree，未复制、覆盖或读取该改动。
- 2026-09-01：完成 `paired_comparison_wave_a.py`：纯 `Decimal` fully-funded ledger、explicit
  fee/no-negative-cash/chronological gates、冻结 101-field inventory、authority/SHA/count/raw-surface
  admission、两侧账户 arithmetic reconciliation、五分区 `ReplayContext`、exact 16-axis reducer 与
  canonical replay receipt。deterministic QC helper fragment 为 `5385` bytes，LF SHA-256=
  `7e67d422296db8773e9b9ddb4ec4dd5278976929add1612ff2e0d69b7f042b17`，包含冻结 field
  inventory，但不含 `QCAlgorithm`、`MarketOrder(`、`main.py`、project 或 dispatch surface。
- 2026-09-01：implementation synthetic/golden/negative suite=`11 passed`；Atlas current-state 与
  source-coverage focused suite 合计=`58 passed / 1 skipped`，均使用 `pytest-xdist`。canonical
  all-PASS fixture replay SHA-256=
  `f5d30239fb2591888a9d06b08f0485b5705d7ae12b700c8e0beb765c81fd2e63`。专项 mypy
  首次检查报告仓库既有跨模块 33 项告警与本模块 1 项 `_to_int` narrowing；本模块问题已直接修复，
  不用 serial pytest 替代并行证据。task 已经 canonical writer 转为 `VALIDATING`。
- 2026-09-01：首个 publication transaction 已完成 task/source 写入与 dirty attribution，但 Atlas
  exact-source writer 要求所有 source paths 已存在于 `HEAD`，report-flow seal 也必须绑定新
  `docs/system_flow.md` Git blob；因此该 transaction 在 candidate/formal/main/push 前 fail-closed 结束。
  后继先用窄化 source-stage fence 生成可审计 intermediate source commit，再从该 exact HEAD 取得最终
  transaction，统一重建 Atlas/report-flow/architecture/compatibility authority。该分段不改变候选内容或
  validation 门槛；external/data/trading counters 仍为零。
- 2026-09-01：source-stage fence 通过 canonical task-source replay 与 dirty attribution 后生成
  task-branch-only intermediate commit `5ed9ab93c30abbabcb3e70c6ac8d0a8ba909383c`；未进入
  local/origin `main`。最终 transaction `trading-2553-wave-a-final-20260901-v3` 从该 exact HEAD
  获取 authority，canonical task 已转为 `BASELINE_DONE`。这表示 fixture baseline 完成，不表示真实 paired
  evidence、DQ、QuantConnect run 或策略结论通过。
- 2026-09-01：最新 main 上的 canonical task 登记、task-count ratchet、architecture manifest 与
  compatibility current-hash authority 已完成串行修复。第一次正式 Full 在候选
  `aa977ee0ef34599bff88786cb96b3becdb793093` 得到 `10074 passed / 27 failed / 6 skipped`；
  13 项为 Atlas 尚未按最终 exact commit 重建，另外 14 项为隔离 worktree 缺少两个既有聚合验证
  fixture，并非 Wave A comparator/reducer 逻辑失败。
- 为完成 clean-worktree Full，仅允许从共享本地 checkout 逐字节复制以下两个 ignored、aggregate-only
  fixture 到本任务 worktree 的相同路径：
  `outputs/validation_runtime/trading_2464_o1_dq_20260729T183000Z/o1_dq_gate.json` 与
  `outputs/research_trends/operational_forecast/trading_2542i_real_v3/real_materialization_receipt.json`。
  目的仅为复现既有测试依赖；不读取或复制 raw market/options payload，不写 cache，不用于 TRADING-2553
  结论。退出条件为 final Full 完成且 summary 已落入 canonical validation runtime；随后删除这两个临时副本。
