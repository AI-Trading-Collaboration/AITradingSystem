# TRADING-2554：QQQ 期权 paired comparison Wave B Owner Review V1

## 1. 当前状态

本任务只提交一个不可执行的 owner-review 草案，解决 TRADING-2553 Wave A 留下的唯一
estimand 语义缺口：fully-funded virtual QQQ/cash comparator 的交易手续费如何计算。

- 状态：`BLOCKED_OWNER_INPUT`；
- 范围：`NON_EXECUTABLE_DATA_RESEARCH`；
- 本轮只允许静态来源核对、决策草案、任务登记与本地验证；
- runnable exporter、run manifest、市场数据、DQ、QuantConnect save/build/backtest/retry、
  provider/Object Store、raw option export、public share、paper/live/production/broker、
  orders/fills/positions 均为 `0`。

精确审批对象：
`config/research/qc_qqq_options_paired_comparison_wave_b_owner_review_v1.yaml`。

## 2. 为什么不能继续沿用 zero-fee fixture

Wave A 的 ledger 要求每个 event 显式提供非负 `fee_usd`，zero-fee 只是一项 synthetic
fixture，不是真实运行默认。手续费会同时改变可购买的整数 QQQ shares、entry/exit cash、
净收益和最大回撤，因此属于 primary estimand 的组成部分，不能在代码里静默补值。

当前被冻结的 QC 实现显式使用：

```python
self.set_brokerage_model(DefaultBrokerageModel(AccountType.CASH))
```

并只对 option contract 覆盖 `$0.65/contract` fee model；QQQ equity 保留 brokerage 提供的
security fee model。QuantConnect 官方文档说明默认 brokerage model 会向 security 提供 fee
model；LEAN 当前 `DefaultBrokerageModel` 对 Equity 返回 `InteractiveBrokersFeeModel`。当前
LEAN 实现中的美国 Equity 分支包含 per-share、minimum 和 maximum-per-order 规则，因此
把 underlying fee 直接当成 0 与当前平台行为不一致。

官方来源：

- <https://www.quantconnect.com/docs/v2/writing-algorithms/reality-modeling/transaction-fees/key-concepts>
- <https://github.com/QuantConnect/Lean/blob/master/Common/Brokerages/DefaultBrokerageModel.cs>
- <https://github.com/QuantConnect/Lean/blob/master/Common/Orders/Fees/InteractiveBrokersFeeModel.cs>

## 3. 推荐冻结方案

推荐 `SAME_RUN_QQQ_SECURITY_FEE_MODEL_NON_SUBMITTED_PROBE`：

1. Comparator 仍是 virtual ledger，不提交任何 QQQ order。
2. 每个 legal entry/exit event 构造仅存在于算法内存中的 hypothetical `MarketOrder` 对象，
   不交给 `Transactions`，不创建 ticket、fill 或 position。
3. 使用同一 run、同一 QQQ `Security` 的 `FeeModel.GetOrderFee` 计算 fee；future manifest
   同时精确绑定 `DefaultBrokerageModel(AccountType.CASH)`、预期 fee-model type 与 LEAN build。
4. Entry shares 取满足
   `q * ask + buy_fee(q) <= cash` 的最大非负整数；从 `floor(cash / ask)` 单调递减，直到
   affordable。Exit 在 bid 卖出全部 virtual shares，并扣同模型 sell fee。
5. ask-entry / bid-exit 已承担 spread；underlying 不再叠加另一项 slippage。
6. aggregate 只累计现有 `UNDERLYING_FEES_USD` 与
   `UNDERLYING_SPREAD_SLIPPAGE_COST_USD`，不扩充冻结的 101-field export surface。

这个设计比硬编码当前 IB 公式更稳健：未来 bounded run 的 fee 语义与其精确 LEAN build
一致，不会由本地复制公式产生 drift；同时 fee probe 不是交易行为，保持 frozen comparator
的 virtual-ledger 边界。

## 4. 兼容性边界

本草案不改变：

- `first_layer_composer_v2:trend_state` 及 1,202-session signal package；
- `risk_on/constructive -> LONG_CALL`、其余状态 `-> FLAT`；
- option policy 37 slots、option `$0.65/contract` fee 与 `$0.01` adverse limit fill；
- `$100,000` common capital、2021-02-22 至 2025-12-02 window、XNYS calendar；
- primary comparator、primary estimand、falsification/reducer precedence；
- aggregate-only、101-field allowlist、raw option bytes 禁止导出；
- 既有 capability backtest 的 evidence role 与结果。

明确拒绝：zero underlying fee、复制一份本地 IB fee 公式、提交真实 QQQ comparator
orders、看到结果后再改 fee。

## 5. Owner 批准后的 Wave B

Owner 精确冻结本草案后，后续一个独立任务才可：

1. 把已验证的 Wave A helper 嵌入 exact QC source package；
2. 生成 exact LF-normalized `main.py` 与 bounded run manifest；
3. 静态 replay signal、contract、owner decision、project/code/LEAN identity、action maxima
   与 aggregate-only boundary；
4. 运行本地 fixture/negative/independent replay 与正式验证；
5. 停在 `READY_FOR_SEPARATE_RUN_AUTHORIZATION`。

这一批准不授权 QC save/build/backtest。唯一一次 bounded QuantConnect DATA_RESEARCH
backtest 仍需单独、精确授权。

## 6. 建议审批文本

> 批准 `qc_qqq_options_paired_comparison_wave_b_owner_review_v1@1.0.0-draft.1`
> （file SHA `5badeb0ded36ce3712a38413484a1422202eaa25aef275ecd01a7d73fea2fc38` /
> canonical SHA `12bfd9800f62ad52ada0bd261b098a2eaf9bc3e247370490b6090fe5d5ec75b9`）
> 全部所列 same-run QQQ Security
> fee-model、non-submitted hypothetical order、fully-funded integer quantity、ask/bid、
> zero additional underlying slippage、compatibility、falsification 与 safety 规则按草案
> 精确冻结；授权生成 non-executable Wave B exact source package 与 bounded run manifest；
> 仅限 DATA_RESEARCH；不授权市场数据读取/下载、DQ、QuantConnect save/build/backtest/retry、
> provider/Object Store/raw option export/public share、paper/live/production/broker 或任何
> orders/fills/positions。

## 7. 验收条件

- 草案能被 YAML strict-load，且 `executable=false`；
- authority binding 与 frozen paired contract、Wave A helper、既有 QC source 的 bytes 相符；
- 推荐方案、拒绝方案、compatibility fence 与 authorization boundary 均为显式字段；
- task register 指向本文件与精确审批对象；
- formal validation PASS；
- 本轮 external/data/trading counters 全为 0。

## 8. 进度记录

- 2026-09-02：READ_ONLY 审计确认 local main/origin main 为
  `17189fc1680b4bc989b3f102b62e44216ed7e84c`；TRADING-2553 Wave A 已完成，Wave B
  尚无 task/package/manifest。
- 2026-09-02：核对当前 QC source、QuantConnect transaction-fee 文档与 LEAN 官方源码；
  提出 same-run non-submitted fee probe，未读取数据、未触发 QC 或交易动作。
- 2026-09-02：owner-review YAML strict-load、4 项 authority binding 与 authorization
  boundary 静态复核 PASS；file SHA `5badeb0d…`，canonical SHA `12bfd980…`。
- 2026-09-02：用户指示继续完成本 owner-review 草案；该指示仅授权发布
  `BLOCKED_OWNER_INPUT` 的 non-executable review artifact，不构成第 6 节精确审批文本，
  不授权 Wave B package/manifest、数据读取、QuantConnect 或交易动作。
- 2026-09-02：旧 lane `a0bedc4a13e1ee8f290f2603d71c687df212b414` 相对最新
  local/origin main `12f1e6458e5af4b7ced444e0f4fdc25365afe965` 执行 governed
  base-drift 审计；计划 `integration-revalidation-3bddf402e7cd254b36a6` 仅报告
  `docs/task_register.md` 的 domain overlap 与 task index 的 coordinator refresh。
  协调策略是在最新 main 保留两份 task-owned 草案原始字节，并通过 canonical task writer
  重建共享登记视图，不复用旧 lane 的生成文件。
- 2026-09-02：临时 workspace 生命周期归属本任务。旧审阅 lane 位于
  `D:\Work\AITradingSystem.worktrees\trading-2554-owner-review`，最新-main 协调候选位于
  `D:\Work\AITradingSystem.worktrees\trading-2554-integration`；用途分别为保留冻结-base
  原始草案与构建最终候选。退出条件为候选验证、local-main 集成与安全发布完成，所需治理
  evidence 已迁入 canonical workspace 且无活动进程依赖；届时先审计 tracked/untracked/
  ignored 内容，再删除无唯一证据的 worktree 并记录释放范围。
- 2026-09-02：首次 focused suite 暴露 canonical registry task-count 自检仍为 `1051`；
  85 项中 84 PASS、仅该新增任务计数失败。旧 publication transaction `v3` 已以 FAIL
  终止并释放 lease；`v4` 显式纳入计数测试路径，将期望值同步为 `1052`，不改变任何
  投资、数据、回测或手续费语义。
- 2026-09-02：修正后 focused suite 以 `pytest-xdist -n 16 --dist loadfile` 完成
  85/85 PASS；owner-review file SHA 与 canonical SHA 分别复核为
  `5badeb0ded36ce3712a38413484a1422202eaa25aef275ecd01a7d73fea2fc38`、
  `12bfd9800f62ad52ada0bd261b098a2eaf9bc3e247370490b6090fe5d5ec75b9`，4 项
  authority binding 全部匹配，未授权动作边界全部为 0/false/none。
- 2026-09-02：最终 publication lane 从 exact latest main `12f1e6458e5af4b7ced444e0f4fdc25365afe965`
  建立；publication transaction `v6` 在 clean base 绑定并验证 drift plan
  `integration-revalidation-48786aab6434a3c90e08`，canonical task registration 后
  `SINGLE_LANE/LANE` preflight PASS。先前 `v5` 因 INTEGRATION preflight 时序错误以 FAIL
  终止，未导致任何数据、外部平台或交易动作。
- 2026-09-02：正式 architecture-fitness 首轮 883 项中 882 PASS；唯一失败为本任务把
  canonical task-count 测试从 `1051` 更新到 `1052` 后，`arch_004e_test_manifest` 未同步
  新文件哈希。`v6` 已绑定失败摘要后终止；`v7` 显式声明 `architecture-manifests` 后再按
  顺序运行 `compatibility-authority`，修复范围仅为生成 authority freshness。
- 2026-09-02：完整重跑进一步定位 `architecture_devex.py generate` 的 deprecation
  scanner 使用 process-global 原始 checkout，生成清单记录 1,343 个测试文件而 active
  worktree 实际为 1,345。任务 acceptance 已追加 active-worktree root 可重放要求；`v8`
  直接修复 root/policy/fitness path 传递并增加 regression test，不采用手工覆盖清单。
- 2026-09-02：`docs/system_flow.md` 增加同一 active-worktree root 约束后，report-flow
  source seal 按设计拒绝旧绑定；`v8` 未越界修改未声明 policy 并以 FAIL 终止。`v9`
  精确更新 system-flow seal 为 2,334,273 bytes、SHA `a3465cca…`、Git blob
  `cf6c2467…`、1,185 entries，再按官方生成顺序重建全部派生 authority。
- 2026-09-02：report-flow 定向测试随后仅报告 3 个旧冻结常量（总 entry、system-flow
  SHA、system-flow entry count）；`v9` 因未声明该测试路径而终止。`v10` 精确同步为
  3,118 total entries、SHA `a3465cca…` 与 1,185 system-flow entries，并重新生成依赖
  该测试哈希的 architecture/compatibility authority。
