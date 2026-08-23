# TRADING-2542：Growth Action Value Threshold Policy Decision Pack And Freeze V1

最后更新：2026-08-23

稳定任务 ID：`TRADING-2542_GROWTH_ACTION_VALUE_THRESHOLD_POLICY_DECISION_PACK_AND_FREEZE_V1`

优先级：`P0`

状态：`BLOCKED_OWNER_INPUT`

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

- S1 inventory、typed decision pack 与 exact authority replay 已完成；
- S2 numeric/directional policy freeze 需要 Project Owner 对 decision pack 作出 exact choice；
- 在该选择之前，2540 的 mechanical terminal 保持 `BLOCKED_POLICY_INPUT`。

### 4.1 S1 authority inventory 结论

| authority | disposition | 可复用边界 |
| --- | --- | --- |
| `action_value_score_policy_v2` | `WRONG_SCOPE` | 不可提供 threshold value 或 PASS basis |
| `defensive_lane_action_value_policy` | `RETIRED_FAMILY` | 仅术语参考 |
| `first_layer_threshold_policy_v2` | `RETIRED_FAMILY` | 仅 calibration method 示例 |
| `promotion_gate_thresholds` | `WRONG_SCOPE` | 仅治理结构示例 |
| `threshold_registry` | `UNCALIBRATED_INVENTORY` | 仅 inventory/lineage，不可提供数值 |
| `transaction_cost_model` | `PARTIAL_INPUT_ONLY` | 只提供成本输入，不是 acceptance threshold |
| `qqq_options_dq_pit_identity_v1` | `PARTIAL_INPUT_ONLY` | 只提供 identity/completeness 输入 |
| `qqq_options_staged_dq_pit_readiness_v1` | `PARTIAL_INPUT_ONLY` | 只提供 staged readiness 输入 |

没有 `ADMISSIBLE` 的完整八轴 bundle。decision pack 已覆盖全部八个 axis，所有
`owner_value_state=NOT_PROVIDED`，并保持 `threshold_value_selected=false`。

### 4.2 Owner 最小决策面

1. `SOURCE_ASSIGNMENT`：是否采用 decision pack 的 per-axis 推荐 calibration source；
2. `EXACT_VALUE_SHEET`：一次性提供八轴列出的全部 numeric、categorical、set 与 policy-reference 值；
3. `REVIEW_CONDITION`：是否将 V1 锁定到一次 primary-window evaluation，任何修改都新建版本。

不完整或仅提供部分 axis 的回复不会触发 S2 freeze；Codex 不会用后见结果补齐缺项。

### 4.3 S1 实现与验证

- decision pack：`config/research/strategy_growth_action_value_threshold_decision_pack_v1.yaml`；
- typed loader：`src/ai_trading_system/strategy_growth_action_value_threshold_decision_pack.py`；
- pack file SHA-256：`b19269c23382dddb70882cd610c2ea506a643bdd2be5cc164a6496219ea930e8`；
- canonical SHA-256：`e4604ccb6bf313ce93a8ae269208490431e40388474ee5d3ac7ab459efd519e3`；
- authority-set SHA-256：`c4c5d5edb2faeb6b1745516022033bb3edaae4620d52582e2529aa4bb578f197`；
- preregistration + decision-pack focused tests：`58 passed`；Atlas/authority/deprecation 联合
  focused：`94 passed`；Ruff 与 strict mypy：`PASS`；
- final-tree 前置正式门：Architecture=`865 passed`、Contract=`276 passed`、
  Integration=`995 passed / 642 warnings`、Reproducibility=`24 passed`；
- 首次 Full：`9354 passed / 1 failed / 3 skipped / 644 warnings`，parent artifact=
  `outputs/validation_runtime/full_20260823T020352Z/test_runtime_summary.json`。唯一失败是本地 ignored
  Atlas canonical page/sidecar 仍绑定旧 repository commit 与旧 2542 coverage；decision-pack、策略、
  task source、report-flow、compatibility、DQ/PIT 与交易路径均未失败。修复仅允许用当前 final commit
  重建既有 read-only page artifacts，并以该 parent 执行完整 `failure_fix_rerun`。

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

- exact base：`ce91cb768010a9e30104fd7a6bfa219bf3459af8`，且创建前已验证
  `local main = origin/main`；
- task branch：`codex/trading-2542-threshold-decision-pack`；
- workspace：复用 `D:\Work\AITradingSystem` 的 existing clean checkout，并在首次 tracked mutation 后、
  branch 创建前仅保留本生命周期记录；不创建新的 worktree 或 clone；
- purpose：只完成 S1 authority inventory、gap matrix、typed decision pack 与 focused validation；不写入
  numeric threshold、不运行 DQ/cache/backtest/Cloud/empirical/external action；
- S1 exit condition：decision pack 与 tests 通过审阅和 formal validation，task source 更新为
  `BLOCKED_OWNER_INPUT` 或等价非终态，S1 commit 普通推送到 main，checkout 切回 clean main，并删除已合并
  task branch；
- recovery：合入前由 task branch/commit 恢复，合入后由 local/remote main 恢复；S1 不复用
  TRADING-2540 的 runtime evidence。
