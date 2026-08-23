# TRADING-2542：Growth Action Value Threshold Policy Decision Pack And Freeze V1

最后更新：2026-08-23

稳定任务 ID：`TRADING-2542_GROWTH_ACTION_VALUE_THRESHOLD_POLICY_DECISION_PACK_AND_FREEZE_V1`

优先级：`P0`

状态：`BLOCKED_OWNER_INPUT`

mode：`SINGLE_LANE`

Owner 决定：
`owner_decision:TRADING-2542:2026-08-23:adopt_recommended_sources_and_draft_complete_exact_value_sheet_before_freeze_v1`

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

### S2A：Owner-review exact value sheet draft

- Owner 已选择 decision pack 的 per-axis 推荐来源；
- Codex 起草一次完整 numeric、categorical、set、policy-reference value sheet；
- 每个值必须记录 unit、measurement basis、经济理由、风险和逐项 review state；
- value sheet 状态固定为 `DRAFT_FOR_OWNER_REVIEW`，任何值均不得被解释为已批准、已冻结或可供
  empirical evaluation 使用；
- V1 绑定一次 primary-window evaluation，任何修改必须新建版本。

### S2B：Owner-reviewed freeze

- 只有收到 Owner 对完整 value sheet 的逐项 exact approval 后，才能另行生成 frozen policy；
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
- Owner 已批准 `APPROVE_RECOMMENDED_PER_AXIS` 与
  `LOCK_V1_FOR_ONE_PRIMARY_WINDOW_EVALUATION_NEW_VERSION_FOR_CHANGE`；
- S2A 完整 exact value sheet 已起草并通过 focused schema/identity/safety validation；
- 当前 next owner 为 Project Owner，需逐项审阅八轴；
- S2B numeric/directional policy freeze 在逐项 approval 前仍禁止；
- 在完整逐项 approval 之前，2540 的 mechanical terminal 保持 `BLOCKED_POLICY_INPUT`。

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

### 4.4 S2A Owner 指令与非冻结边界

2026-08-23，Project Owner 明确采用每轴推荐来源，要求 Codex 先起草完整 exact value sheet 供逐项
审阅，未经逐项确认不得冻结；V1 基于 primary window 进行一次评估，后续修改必须新建版本。

该指令解除 `SOURCE_ASSIGNMENT` 与 `REVIEW_CONDITION` 阻塞，但不是 `EXACT_VALUE_SHEET` 的最终
逐项 approval。S2A 可以提出完整建议值和测量合同；`threshold_bundle_frozen=false`、
`dq_successor_authorized=false`、`empirical_successor_authorized=false` 必须保持不变。

### 4.5 S2A exact value sheet 草案

canonical draft：
`config/research/strategy_growth_action_value_threshold_exact_value_sheet_v1.yaml`。

| axis | 建议 exact value | measurement / gate 摘要 |
| --- | --- | --- |
| `NON_BETA_ACTION_VALUE` | annualized delta `>= 0.0100` | 20-session moving-block bootstrap、10,000 resamples、one-sided 90% lower bound `> 0` |
| `NET_OF_COST_RETURN` | annualized net delta `>= 0.0075` | `transaction_cost_model_v1`，独立成本重算 tolerance `0.0001` |
| `ACTUAL_PATH_DRAWDOWN_REGRESSION` | absolute regression `<= 0.0200` | full primary window 与 5 个预声明 calendar/stress slices 分别通过 |
| `FALSE_RISK_OFF_COST` | mean event-cost regression `<= 0.0025` | 20-session event；QQQ-SGOV forward excess `>= 0.0300` 且 QQQ forward drawdown `>= -0.0500` |
| `CANONICAL_DQ_PIT` | exact `PASS` | draft DQ：quote age `<=120s`、relative spread `<=0.20`、OI `>=10`、volume `>=1`、exact source date、UNKNOWN fail closed |
| `SAMPLE_AND_WINDOW_DEPENDENCE` | count `>=30`、per-slice `>=3`、regime share `<=0.50` | independent episode gap `20` exchange sessions；5 个 primary-window calendar slices |
| `ACTUAL_PATH_TURNOVER` | annualized one-way turnover `<=1.00`、cost-drag share `<=0.25` | actual fills-equivalent path，不接受 target-weight delta 替代 |
| `LEVERAGE_BETA_ATTRIBUTION` | realized beta increment `<=0.0200`、exposure mismatch `<=0.0100` | QLD/TQQQ/options/borrowed leverage 均禁止 |

这些建议值来自 Owner economic materiality、governed cost model、precommitted stability rule 与
canonical strict DQ/PIT 的组合，不读取当前 hypothesis、holdout 或新 DQ result。DQ numeric 子政策也只是
draft；即使逐项批准，仍需独立 serial DQ contract wave 才能替换现有
`UNKNOWN_REQUIRES_POLICY_REVIEW`。

当前每轴 `owner_review_state=PENDING_OWNER_APPROVAL`。只允许逐轴
`APPROVE_EXACTLY_AS_DRAFTED` 或 `REJECT_AND_REQUEST_NEW_VERSION`；partial review 可记录，但不能冻结。

### 4.7 Web Pro 复核采纳与 V1 disposition

2026-08-23，Project Owner 采纳对 exact commit
`b70fe3963988241b187bc0d30bbc422eed2b2160` 的 ChatGPT Web Pro 审阅结论
`REQUEST_NEW_VERSION_BEFORE_ANY_FREEZE`。会话：
`https://chatgpt.com/c/6a8a90ac-2e40-83e8-9ce6-6fc1cfb4dfdd`；UI 与回答自报
`GPT-5.6 Pro`，backend route 未得到 attestation。

该决定否决 V1 作为 freeze authority，但不改写 V1 bytes：七轴
`REJECT_AND_REQUEST_NEW_VERSION`，`CANONICAL_DQ_PIT` 为
`INSUFFICIENT_EVIDENCE_TO_APPROVE`。V1 继续保持 `DRAFT_FOR_OWNER_REVIEW`、
`threshold_bundle_frozen=false`。新任务
`TRADING-2542A_GROWTH_ACTION_VALUE_EXACT_MEASUREMENT_AND_JOINT_DECISION_CONTRACT_V1`
负责 serial V2 measurement/comparator contract wave；V2 完成后仍须 Owner 逐项审阅，DQ 轴还须独立
serial DQ contract。当前任务继续 `BLOCKED_OWNER_INPUT`，不得据 V1 运行 DQ 或 empirical evaluation。

建议逐项审阅回复格式：

```yaml
NON_BETA_ACTION_VALUE: APPROVE_EXACTLY_AS_DRAFTED
NET_OF_COST_RETURN: APPROVE_EXACTLY_AS_DRAFTED
ACTUAL_PATH_DRAWDOWN_REGRESSION: APPROVE_EXACTLY_AS_DRAFTED
FALSE_RISK_OFF_COST: APPROVE_EXACTLY_AS_DRAFTED
CANONICAL_DQ_PIT: APPROVE_EXACTLY_AS_DRAFTED
SAMPLE_AND_WINDOW_DEPENDENCE: APPROVE_EXACTLY_AS_DRAFTED
ACTUAL_PATH_TURNOVER: APPROVE_EXACTLY_AS_DRAFTED
LEVERAGE_BETA_ATTRIBUTION: APPROVE_EXACTLY_AS_DRAFTED
```

任一 axis 如需修改，使用 `REJECT_AND_REQUEST_NEW_VERSION` 并在同一行说明修改方向；Codex 不会在
当前 `1.0.0-draft.1` 上原地替换值后冒充已审版本。

### 4.6 S2A 实现身份与初步验证

- exact value sheet file SHA-256：
  `82f75b55bb4a9576775d4e60a9a31bc01b24d3b5b8cf270c6aabbed9e9d17e7f`；
- exact value sheet canonical SHA-256：
  `14286008f464230921400c1def4173f34a6e9231e77c434504a5abab78451dfb`；
- typed loader：
  `src/ai_trading_system/strategy_growth_action_value_threshold_exact_value_sheet.py`；
- exact tests：`tests/test_strategy_growth_action_value_threshold_exact_value_sheet.py`；
- S1 + S2A schema/identity/safety focused：`30 passed`；
- Atlas/deprecation/report-flow focused：`36 passed`；
- combined S1/S2A/Atlas/deprecation/report-flow focused：`66 passed`；
- Ruff 与 strict mypy：`PASS`；
- final-tree 前置正式门：Architecture=`865 passed`、Contract=`276 passed`、
  Integration=`995 passed / 643 warnings`、Reproducibility=`24 passed`；
- 首次 Full：`9369 passed / 2 failed / 3 skipped / 644 warnings`，parent artifact=
  `outputs/validation_runtime/full_20260823T040241Z/test_runtime_summary.json`。第一项失败定位为 S2A Atlas
  摘要替换时遗漏仍然有效的“任何新 empirical result、cache、DQ 或 backtest 可见前”安全边界；恢复该边界后
  cited-query/page-effectiveness focused=`13 passed`。第二项失败仅是 ignored Atlas canonical page/sidecar
  仍绑定旧 repository commit；final candidate commit 后只重建该 read-only artifact，并以 parent artifact
  执行完整 `failure_fix_rerun`。通过前不改变草案或任务安全状态。

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

S2A 生命周期：

- exact base：`b2ba8fb680c151a06cf0419b2701491dae553e42`；
- task branch：`codex/trading-2542-exact-value-sheet-draft`；
- workspace：复用 `D:\Work\AITradingSystem` existing checkout，不创建 worktree 或 clone；
- purpose：只起草、校验并发布 `DRAFT_FOR_OWNER_REVIEW` exact value sheet；不冻结 threshold、不运行
  DQ/cache/backtest/Cloud/empirical/external action；
- exit condition：完整八轴草案和 negative tests 通过，task 回到 `BLOCKED_OWNER_INPUT` 等待逐项
  approval，S2A commit 普通推送到 main，checkout 回到 clean main，并删除已合并 task branch；
- recovery：合入前由 task branch/commit 恢复，合入后由 local/remote main 恢复。

S2B 仍未开始。后继 TRADING-2542B 已完成独立 CANONICAL_DQ_PIT serial contract draft，补齐 quote
clock、spread、exact-date/PIT、contract/session/window aggregation 与 terminal precedence，但不构成 numeric
approval 或 independent review。只有 2542A V2 全轴 Owner review 和 2542B 独立审阅均完成后，才可新建
version 进入 freeze；当前 `threshold_bundle_frozen=false`，所有 DQ/empirical/external/trading action 关闭。
