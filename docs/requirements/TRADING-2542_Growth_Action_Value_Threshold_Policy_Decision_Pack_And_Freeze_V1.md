# TRADING-2542：Growth Action Value Threshold Policy Decision Pack And Freeze V1

最后更新：2026-08-23

稳定任务 ID：`TRADING-2542_GROWTH_ACTION_VALUE_THRESHOLD_POLICY_DECISION_PACK_AND_FREEZE_V1`

优先级：`P0`

状态：`IN_PROGRESS`

mode：`SINGLE_LANE`

`production_effect=none`；`broker_action=none`；`external_action=none`。

## 1. 背景与目标

TRADING-2540 已保留唯一 QQQ Options evidence lane，并冻结一个只在 QQQ/SGOV 内进行 bounded、
non-leveraged reallocation 的 growth action-value hypothesis。TRADING-2541 已把 transport completeness
修复为 `1202/1202`，但明确保持 `dq_pit_promoted=false`。

2540 的 threshold authority inventory 证明，现有 `action_value_score_policy_v2`、defensive、
first-layer、promotion policy 与 `threshold_registry` 的 scope/calibration state 均不能直接组成覆盖八个
mandatory axes 的 reviewed bundle；`transaction_cost_model` 也只能提供 cost input。不得在新 hypothesis
结果可见后拼接历史阈值。

本任务先建立 evidence-to-threshold gap matrix 和 Owner decision pack，再在 Owner 对精确 policy values、
来源、适用范围与 review condition 作出决定后冻结 versioned threshold policy。它不执行 QQQ Options
data lane、cache mutation、DQ/PIT run、strategy backtest、holdout 或任何外部动作。

## 2. 必须冻结的八个 axis

1. `NON_BETA_ACTION_VALUE`；
2. `NET_OF_COST_RETURN`；
3. `ACTUAL_PATH_DRAWDOWN_REGRESSION`；
4. `FALSE_RISK_OFF_COST`；
5. `CANONICAL_DQ_PIT`；
6. `SAMPLE_AND_WINDOW_DEPENDENCE`；
7. `ACTUAL_PATH_TURNOVER`；
8. `LEVERAGE_BETA_ATTRIBUTION`。

每个 axis 必须记录 threshold id、unit、direction、source authority、calibration evidence、rationale、
intended effect、PASS/FAIL/INSUFFICIENT/INVALID 规则、review/expiry condition。不能由测试 fixture、
未来 DQ result 或 strategy result 反推出 policy value。

## 3. 分步计划

### S0：registration boundary

- canonical task row、supporting requirement 与 generated task views；
- 只登记任务，不创建 threshold value 或 empirical evidence。

### S1：authority inventory 与 decision pack

- 逐 axis 评估现有 policy 是否 `ADMISSIBLE`、`PARTIAL_INPUT_ONLY`、`WRONG_SCOPE`、
  `RETIRED_FAMILY` 或 `UNCALIBRATED_INVENTORY`；
- 对缺口给出可审阅的 calibration-source 选项与风险，不选择数值；
- 明确 transaction-cost input 与 investment-facing acceptance threshold 的区别；
- 输出 Owner decision 所需的最小问题集。

### S2：Owner-reviewed freeze

- 只有收到 exact Owner decision 后才写入 numeric/directional policy values；
- policy 必须 canonical serialize、seal、from-json/replay，并绑定 2540 hypothesis、baseline、comparator、
  primary window 和 selected QQQ Options lane；
- threshold-after-result、wrong family、hidden leverage、double lane、wrong window 与 authority drift
  均 fail closed。

### S3：收口与后继门禁

- focused、Architecture、Contract、Integration、Reproducibility 与 Full 在 final tree PASS；
- 只有 threshold bundle frozen 后，才可另立 DQ/PIT admission successor；
- threshold freeze、DQ/PIT PASS 都不自动授权 empirical growth evaluation、investment conclusion、
  paper/live、production 或 broker。

## 4. 当前 blocker / next step

- Codex 可立即完成 S1 inventory 与 decision pack；
- S2 numeric/directional policy freeze 需要 Project Owner 对 decision pack 作出 exact choice；
- 在该选择之前，2540 的 mechanical terminal 保持 `BLOCKED_POLICY_INPUT`。

## 5. 验收标准

- 八个 axis 全部进入 gap matrix，0 silent omission；
- 每个复用或拒绝的现有 policy 都有 exact scope/rationale；
- decision pack 不读取或生成新 hypothesis empirical result；
- Owner 决定发生在任何 DQ/strategy result 可见前；
- frozen policy 无 unexplained numeric literal，且可 canonical seal/replay；
- `production_effect=none`、`broker_action=none`，所有 empirical/external flags 为 false。

## 6. 生命周期

S0 只随 TRADING-2540 final integration candidate 完成 canonical registration。S1 起必须从 2540
ordinary-pushed exact main 建立独立 task branch/workspace；其路径、purpose 与 exit condition 在首次创建前
补充到本节。不得复用 2540 的 formal evidence。
