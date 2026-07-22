# TRADING-2455：Simulation Defensive 胜率策略门禁修正

最后更新：2026-07-22

状态：`COMPLETE`

稳定任务 ID：`TRADING-2455_SIMULATION_DEFENSIVE_WIN_RATE_POLICY_GATE_CORRECTION`

## 背景与风险

Wave 8 独立集成审查发现，受控 policy
`config/etf_portfolio/dynamic_v3_rescue/sim_defensive_validation_v1.yaml` 已声明
`minimum_win_rate_vs_no_trade`，但 loader 未校验该字段，regime status 也未把它纳入
`PROVEN_DEFENSIVE` 判定。因此，低胜率样本只要平均相对收益和平均回撤改善达到门槛，仍可能被误报为
已证明防御有效。这是投资解释正确性问题，不是性能任务的附带清理。

## 输入、输出与计算逻辑

- 输入：同一 `regime + sim_event_id + window_days` 下
  `defensive_limited_adjustment` 与 `no_trade` 的 AVAILABLE、finite 配对样本，以及 reviewed policy。
- 中间量：`win_rate_vs_no_trade = count(defensive_return > no_trade_return) / paired_window_count`。
- 输出：regime matrix 的 `win_rate_vs_no_trade` 与 `status`，再由三个 pressure-regime status 汇总总状态。
- `minimum_win_rate_vs_no_trade` 必须是 finite 且位于 `[0, 1]`；缺失、NaN、无穷或越界均在输出前 fail closed。
- `PROVEN_DEFENSIVE` 必须同时满足 distinct-event sample floor、平均相对收益门槛、平均回撤改善门槛与胜率门槛；只满足部分指标返回 `PARTIALLY_DEFENSIVE`，全部失败返回
  `FAILS_DEFENSIVE_EXPECTATION`。

## 实施步骤与验收

1. loader 增加胜率门槛的 finite/range contract；
2. regime matrix 只从同一 paired cohort 计算胜率并传入 status gate；
3. 补充低于门槛、恰好等于门槛、invalid policy 与现有不足样本精确语义测试；
4. 更新 system flow、task register、generated manifests/source hashes；
5. focused、architecture、contract、reproducibility 与 Wave 8 唯一自然 Full 全部通过后完成。

安全边界保持 `outcome_mode=BACKTEST_SIMULATION`、`SIMULATION_NOT_PIT`、
`auto_policy_apply=false`、`production_effect=none`、`broker_action=none`。本修正不校准门槛数值，
只执行已审核 policy 的现有值。

## 完成证据

2026-07-22：loader 已对胜率门槛执行 finite 与 `[0,1]` 校验，regime matrix 从同一 paired cohort
一次计算胜率并传入统一 status gate；`PROVEN_DEFENSIVE` 现在必须同时满足 return、drawdown 和 win-rate，
边界 `0.50` 通过、低于边界不通过，invalid/NaN/越界 policy 均 fail closed。现有不足样本 fixture 精确保持
overall=`INSUFFICIENT_DATA`，三个 pressure regime 只允许 `INSUFFICIENT_SAMPLE|INSUFFICIENT_DATA`。
expanded focused=`164 passed/1 skipped`；architecture/contract/reproducibility=`446/265/23 passed`；唯一自然
Full=`6,575 passed/2 skipped`，profile/telemetry/performance/provenance 全部 PASS。policy 数值、研究窗口、
生产状态和 broker 状态均未改变。
