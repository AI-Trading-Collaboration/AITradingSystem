# TRADING-2518 — QQQ Options 主窗口 Collector Filter Failure-Fix 与重新授权 V1

- status: `BASELINE_DONE`
- priority: `P0`
- governed mode: `SINGLE_LANE`
- registration base: `de4c8f5f608e77c94ca50944681cea5e43190c75`
- implementation base: `2ba0e799bc5e9549eb0220f33a5c2d907de86596`
- production effect: `none`
- broker action: `none`

## 1. 背景与不可改写的真实事实

Project Owner 在当前 Codex 对话提供了
`owner_decision:TRADING-2516:2026-08-13:authorize_single_zero_order_primary_window_derived_aggregate_collection_v2`。
2517 canonical validator 已严格 admission；token file/content SHA-256 分别为
`ee20b55c525a2241f88d215241f516ca0d9e016bc6611c966058ce8468fc82b2` /
`1304491e50feaf22764260e3d227656381d2b1b6984e095bb9a1c93bbbdf1d72`，refresh candidate、collector
authorization、refresh admission receipt content SHA-256 分别为
`fecd74ae2c3477c3b1afb87dff7589111d4ec280b342485efc414dc003350fa0`、
`3db9a31a84af7abe3392e18b4838597110f639cb8ed194497d7025681c26b1d3`、
`21ee0862314a22c7100bf99384d8dc226fbb91c307131491fc922aca1a987661`。

2026-08-13 的唯一 bounded lifecycle 事实如下：

1. QuantConnect 登录与 Free tier observation 完成，target project id=`34808569`；
2. 线上旧代码 LF SHA-256=`1da0d834d5509aabd7fb3baeeff9b8b3f56eed3d9ba095679f84fda926843139`；
3. 使用授权中的唯一一次 project mutation，把 `main.py` 更新为 reviewed 2513 bytes，LF SHA-256=
   `d7f96fbb14e03a1f248b0a14b3ebdaa1bbeeada2d15f87fb3277b98b9c6641a6`，随后重新复制线上全文并 exact
   hash PASS；Cloud build 完成；
4. 唯一一次 Cloud backtest 于 `2026-08-13T13:54:35.282Z` 提交，名称=
   `Muscular Fluorescent Yellow Buffalo`，backtest id=
   `9518360aeb329219cd83e78442a1d229`；
5. QuantConnect history 外层显示 `Completed`，但结果页在 `2021-02-22 00:00:00` 明确产生 Runtime Error，
   因此 governed semantic status 必须是 `FAILED`，不得把平台外层标签解释为能力 PASS；
6. exact failure：`option.set_filter(lambda universe: universe.contracts(lambda symbols: symbols))` 在
   `main.py:25` 把 `CastingEnumerable<BaseData, OptionUniverse>` 错误转换为
   `IEnumerable<Symbol>`；typed reason=`QC_RUNTIME_OPTION_FILTER_CASTING_ERROR`；
7. orders/fills=`0/0`，start/end equity=`100000/100000`，fees=`0`，portfolio invested=`false`；
8. 当前 UI 未提供冻结合同要求的 Owner manual `Download Results` JSON 入口；没有替换为 Report/Logs/API/CLI/
   HTTP/Object Store，也没有 raw option rows；Results bytes、evidence admission 与 DQ/PIT 均未完成；
9. 2517 external-action ledger content SHA-256=
   `e3979529e8bdca48e6b44a74376bbea635f02c218b3737afb265b33481e827f2`，状态=`FAILED/FAIL`；
10. first-run consumption receipt content SHA-256=
    `235bf53686052fabfd21089d6b0fb4dcafeb1b039375d142dfc9478ae595d498`；authorization 已消费并永久禁止
    第二次 Cloud run，evidence collection completed=`false`。

上述事实不修改 2513/2514/2516/2517 的历史 authority。2516 token 不得复用；没有新的 reviewed
proposal/token 时 external action 必须为 `none`。

## 2. Root cause 与正式修复方向

LEAN Python API 的 `OptionFilterUniverse.contracts` selector 输入是 OptionUniverse rows，返回值必须是明确的
`list[Symbol]` 或兼容的 contract rows。直接返回运行时 enumerable 会触发错误的 `IEnumerable<Symbol>` 绑定。

修复只做显式 Symbol 投影，不引入任何 DTE、strike/moneyness、delta、spread、OI、volume、freshness、fee、
slippage、latency 或 partial-fill 阈值：

```python
option.set_filter(
    lambda universe: universe.contracts(
        lambda contracts: [contract.symbol for contract in contracts]
    )
)
```

该写法的目的仅是保持“选择当前 universe 提供的全部 contract symbols”的既有 transport 语义；不得把它解释为
投资选择政策，也不得激活 2502/2504/2507 policy slots、selection 或 engine。

参考的 primary authority：

- QuantConnect Equity Options universe documentation：
  <https://www.quantconnect.com/docs/v2/writing-algorithms/securities/asset-classes/equity-options/requesting-data/universes>
- LEAN Python `OptionFilterUniverse.contracts` API reference：
  <https://www.lean.io/docs/v2/lean-engine/class-reference/py/QuantConnect/Securities/OptionFilterUniverse/>

## 3. 实现计划

### S0 — registration boundary

- 登记 2518 canonical task row 与本 supporting requirement；
- 同步 2517 projection，记录真实 token consumption/run failure/evidence missing；
- 重建 task shadow/DevEx/current authority，focused validate；
- ordinary non-force push registration boundary；从其 exact latest main 重新 START/LANE。

### S1 — versioned failure-fix package

- 保留 2513 historical package exact bytes；新增 2518 versioned successor package；
- 只替换 filter selector 为显式 `contract.symbol` list；
- 冻结 predecessor code SHA、failure backtest id、ledger/consumption hashes、corrected code LF SHA 与 no-threshold
  invariant；
- 新 proposal/token template 必须绑定 corrected code；不得自动生成 Owner 授权事实。

### S2 — tests 与 negative coverage

- unit/property/golden：selector 输入排列不影响 symbol set/identity、返回明确 list、空输入、重复/异常 row
  fail closed；
- source guard：旧 `lambda symbols: symbols` 不得出现在 successor；历史 2513 bytes/hash 必须不变；
- project/range/calendar/1202 sessions/orders/fills/raw/log/Object Store/engine cash-preservation 继续 exact；
- second-run、old token、local token、missing token、code/hash/backtest drift 全部拒绝。

### S3 — closeout

- 更新 `docs/system_flow.md`、architecture fragments、task shadow、DevEx 与 compatibility current authority；
- focused/adjacent/compatibility 后，在 final tree 串行运行 Architecture → Contract → Integration →
  Reproducibility → exclusive Full；
- ordinary non-force push、SHA verify 与 branch/worktree cleanup；
- closeout 只表示离线 failure-fix 与新授权前置已冻结，不表示新 run/evidence/DQ/PIT PASS。

## 4. 验收标准

1. 2517 failed run、ledger、consumption、0 order/fill 与 missing Results facts 被 exact 继承，不得重写为 PASS。
2. 2513 historical package与 consumed token bytes/hash 不变。
3. successor filter 返回显式 `list[Symbol]`，不再返回 `OptionUniverse` enumerable。
4. 不引入任何投资阈值或 hidden default，不激活 selection/engine/backtest/investment interpretation。
5. 新 Cloud run 只允许在新 proposal/code hashes 和 Project Owner exact single-use token 到位后执行；默认
   `POLICY_BLOCKED_CASH_PRESERVATION`。
6. offline focused、generated/compatibility 与 final five-tier gates PASS；ordinary push/cleanup完成。

## 5. Path claims

Task-owned：

- `docs/requirements/TRADING-2518_QC_QQQ_Options_Primary_Window_Collector_Filter_Failure_Fix_and_Reauthorization_V1.md`；
- 2518 versioned policy/module/tests/package（START/LANE 后精确冻结）；
- 不包含真实 Owner token bytes、raw option rows 或伪造 Results/DQ evidence。

Coordinator-owned shared：

- canonical task registry/index 与 generated task shadow；
- `docs/system_flow.md`；
- architecture fragments、DevEx seal、compatibility/current-authority sources。

## 6. 当前 blocker / next owner

- Codex capability coordinator：严格离线 failure-fix、versioned package 与验证已完成；
- Project Owner：仅在 ordinary-pushed corrected proposal/package/code hashes 发布后，决定是否签署新的
  single-use v3 token；
- 当前 blocker：`OWNER_REAUTHORIZATION_NOT_PROVIDED_FOR_CORRECTED_TRADING_2518_PROPOSAL`；external action、
  second run、evidence admission、DQ/PIT、engine activation 与投资解释均禁止。

## 7. 实现结果

- policy：`config/research/qc_qqq_options_primary_window_collector_filter_failure_fix_v1.yaml`；
- module：`src/ai_trading_system/qqq_options_research/primary_window_collector_filter_failure_fix.py`；
- package：`inputs/research/qqq_options/trading_2518_primary_window_collector_filter_failure_fix_v1/`；
- corrected `main.py` LF SHA-256=
  `064a3bba10d1599a886eb52340ba843ff19ef9caf6a0da89ac5b5119c929d49d`；
- failure receipt content SHA-256=
  `eebb37bbbabe584bd38c013ef41c48fd1e8196bf7207d24c265301310c93fb07`；
- package manifest content/canonical SHA-256=
  `08c1b32901aa6dc67923c2432017438ac3bee7d90810d910ba2149dd2fd85931` /
  `a9d335f5c80a425301d25a39967b8d251d90960ef1301b55e55ac5b3380a21f7`；
- 2513 historical `main.py` LF SHA-256 仍为
  `d7f96fbb14e03a1f248b0a14b3ebdaa1bbeeada2d15f87fb3277b98b9c6641a6`；
- no-threshold、0 orders/fills、missing Results、DQ/PIT=`NOT_EVALUATED`、cash-preservation 均由 typed
  policy/receipt/manifest 与 negative tests fail closed；
- Atlas page-effectiveness successor coverage 同步到 2518，明确披露 v2 token 已消费、run FAILED、修复已完成但
  v3 reauthorization/evidence/DQ-PIT 尚未发生。
