# TRADING-2519 — QQQ Options 主窗口日级回调结果失败准入 V1

- status: `IN_PROGRESS`
- priority: `P0`
- governed mode: `SINGLE_LANE`
- registration base: `6e36874e38119ede21a7085e2e6df3bc2cbb5b33`
- production effect: `none`
- broker action: `none`

## 1. 不可改写的 v3 单次授权与运行事实

Project Owner 于 2026-08-14 在当前 Codex 对话提供 exact token
`owner_decision:TRADING-2518:2026-08-14:authorize_single_zero_order_primary_window_derived_aggregate_collection_v3`。
token 绑定 ordinary-pushed main `6e36874e38119ede21a7085e2e6df3bc2cbb5b33`、corrected project code LF
SHA-256 `064a3bba10d1599a886eb52340ba843ff19ef9caf6a0da89ac5b5119c929d49d`、target project
`34808569`、PRIMARY range `2021-02-22..2025-12-02`、1202 sessions、最多一次 project mutation、一次
Cloud backtest、0 orders、0 fills，并在第一次 run attempt 后失效。

本次 bounded lifecycle 的真实事实为：

1. QuantConnect 登录后只使用既有 project `34808569`；Free tier / Community B-MICRO；
2. 线上旧代码 LF SHA-256 为
   `d7f96fbb14e03a1f248b0a14b3ebdaa1bbeeada2d15f87fb3277b98b9c6641a6`；
3. 使用唯一一次 project mutation 写入 2518 corrected bytes，复制线上全文后 LF SHA-256 exact 等于
   `064a3bba10d1599a886eb52340ba843ff19ef9caf6a0da89ac5b5119c929d49d`；
4. Cloud build PASS，build id=`c87c22-be5a81`；
5. 唯一一次 backtest 于 `2026-08-14T01:08:48.887Z` 提交，名称=`Muscular Fluorescent Orange Bat`，
   backtest id=`b6d711f67a47199667c8a62f86208b28`；v3 token 自该时刻永久失效；
6. run 自然 `Completed`，start/end=`2021-02-22..2025-12-02`，耗时 `900.27s`，处理
   `38,397,482` data points，43k data points/s；无 runtime error；
7. QuantConnect Results JSON 由 UI 的 `Download Results` 手工取得，原文件 SHA-256=
   `30f95852fe509e5229a86bed77978f62f9756016f17c3159c5afb63b6eaa205b`，byte count=`813023`；
8. result state=`Completed`、orders/fills=`0/0`、fees=`$0.00`、start/end equity=`100000/100000`、
   portfolio invested=`false`，Orders/Trades 表均无 row；
9. runtime identity exact 匹配 2513/2518 frozen scope/collector/transport authority；但 terminal exact 为
   `status=INVALID_INCOMPLETE|observed_sessions=0|invalid_sessions=1202|orders=0|fills=0|portfolio_invested=false|raw_rows=false|log_data=false|object_store=false`；
10. chart `TRADING2512_EXPORT_SAFE_DERIVED_AGGREGATES_V1` 存在但 series 为空；Results JSON 不含 raw option rows；
    2514 strict admission 必须拒绝该结果，DQ/PIT 不得产生 PASS。

本任务不重写 2516/2517/2518 历史。v3 authorization 已消费；未经新的 reviewed proposal 与 Project Owner
exact token，不得再次修改 QuantConnect project、build/run、下载或采集其他外部证据。

## 2. Root cause

2518 corrected collector 修复了 `OptionFilterUniverse.contracts` 的 Python casting 问题，但仍将
`Resolution.DAILY` option chain 的采集安排在 `after_market_open + 1 minute`。真实运行表明引擎处理了完整区间的
38M+ data points，但 9:31 scheduled callback 在日级 slice/option chain 可用前执行；`option_chain(self._option)`
对全部 1202 个 expected sessions 为空，因此 `_seen_sessions` 始终为 0，所有 session 被 fail closed。

正式修复方向是从 canonical `Slice.option_chains` 的日级 `OnData`/`on_data` delivery 事实触发一次每-session
采集；不得通过改成 minute resolution、延迟猜测时间、放宽 1202 exact session、把空链当 PASS 或伪造 chart
points 绕过。

## 3. 实现计划

### S0 — registration / evidence freeze

- 登记 2519 task row 与本 supporting requirement；
- 更新 2518 projection，追加真实 v3 consumption/result failure facts；
- 封存 export-safe Results JSON exact bytes 与 typed failure receipt；
- registration boundary ordinary push 后，从 exact latest main 重新 START/LANE。

### S1 — strict failure admission

- 复用 2512 strict result envelope，不复制 shared evidence/DQ/PIT records；
- 明确要求 2514 admission 对 `INVALID_INCOMPLETE`、empty series、0/1202 observed fail closed；
- failure receipt 绑定 token、project/code/build/backtest/result/runtime/range/order/fill/no-raw identity；
- DQ/PIT=`NOT_EVALUATED`，engine/selection=`POLICY_BLOCKED_CASH_PRESERVATION`。

### S2 — daily delivery failure-fix

- versioned successor collector 仅将采集 trigger 改为 `Slice.option_chains` 日级 delivery；
- 同一 session 重放、非 expected date、缺链、重复链、empty candidates、异常数据继续 fail closed；
- 输入 contract 排列不改变 deterministic aggregates/identity；
- 不引入 DTE/moneyness/delta/spread/OI/volume/freshness/fee/slippage/latency/partial-fill 等阈值。

### S3 — closeout

- 更新 system flow、architecture/generated/compatibility 与 Atlas reviewed-successor disclosure；
- focused/unit/property/golden/adjacent/compat 后，从 final tree 串行运行 Architecture → Contract →
  Integration → Reproducibility → exclusive Full；
- ordinary non-force push、SHA verify 与 task branch/worktree cleanup；
- closeout 只证明失败证据与离线 fix 已冻结，不表示 Cloud capability、evidence admission、DQ/PIT、engine、
  策略或投资结论 PASS。

## 4. 验收标准

1. v3 token consumption、唯一 mutation/run、build/backtest/result hash、0 orders/fills 与
   `INVALID_INCOMPLETE 0/1202` exact facts不可改写。
2. Results JSON 无 raw option rows；不得输出或提交 raw option rows。
3. 2514 strict parser 对本 result exact fail closed，typed reason 指向 incomplete runtime/empty derived series。
4. successor 只从 canonical daily Slice delivery 派生 session；scheduled 9:31 callback 不再是采集 authority。
5. 1202 expected sessions、PRIMARY range、shared schema/DQ/PIT/transport/selector/engine cash-preservation保持不变。
6. 未获得新 Owner token时 external action=`none`；不得自动重跑、扩大区间、下单或投资解释。
7. final-tree focused 与正式五级门禁 PASS，普通 push/verify/cleanup 完成。

## 5. Path claims

Task-owned：

- 本 supporting requirement；
- `config/research/qc_qqq_options_primary_window_daily_slice_failure_fix_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/primary_window_daily_slice_failure_fix.py`；
- `tests/test_qqq_options_primary_window_daily_slice_failure_fix.py`；
- `inputs/research/qqq_options/trading_2519_primary_window_daily_slice_failure_fix_v1/` 下的 export-safe result、
  failure receipt、versioned `main.py` 与 package manifest。

Coordinator-owned shared：canonical task registry/index、task shadow、`docs/system_flow.md`、architecture
fragments、DevEx、compatibility/current-authority 与 Atlas page-effectiveness/generated package。

明确排除：2481 shared records/envelope、2482 DQ/PIT semantics、2512 collector parser shared contract、
2514 evidence admission shared schema、raw option rows、真实策略/engine/selection、任何新的外部动作。

## 6. 当前状态 / next owner

- Codex capability coordinator：封存真实 v3 failure evidence，完成离线 daily Slice failure-fix 与验证；
- Project Owner：仅在 2519 ordinary-pushed package/code hashes 发布后决定是否另行授权；
- 当前 blocker：`V3_AUTHORIZATION_CONSUMED_DAILY_SCHEDULE_RESULT_INVALID`；external action=`none`；
  evidence admission=`FAIL`；DQ/PIT=`NOT_EVALUATED`。
