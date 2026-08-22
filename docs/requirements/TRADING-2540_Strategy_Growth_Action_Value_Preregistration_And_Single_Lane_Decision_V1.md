# TRADING-2540：Strategy Growth Action Value Preregistration And Single-Lane Decision V1

最后更新：2026-08-22

稳定任务 ID：`TRADING-2540_STRATEGY_GROWTH_ACTION_VALUE_PREREGISTRATION_AND_SINGLE_LANE_DECISION_V1`

优先级：`P0`

状态：`IN_PROGRESS`

mode：`SINGLE_LANE`

topology：`SERIAL_CONTRACT_WAVE_FIRST`

registration base：`9717949319e619952c192e39c4ed2db1ee9f9eab`

Owner 决定：
`owner_decision:TRADING-2540:2026-08-22:proceed_serial_growth_preregistration_then_single_qld_dq_sequence_v1`

`production_effect=none`；`broker_action=none`；`external_action=none`。

## 1. Owner 方向与当前授权

Project Owner 要求参考 Web Pro exact-commit 审阅形成的顺序继续推进后续开发：

1. 先完成增长假设 preregistration serial contract wave；
2. 后继只保留一条 `QLD_CANONICAL_FULL_CACHE_DQ` 数据资格车道；
3. 只有 canonical strict DQ/PIT PASS 且取得独立 Owner reopen 授权后，才允许一次 locked、
   non-leveraged `QQQ/SGOV` growth action-value evaluation；
4. 只有非杠杆 growth action value 与独立 exposure-scaling hypothesis 均通过后，才允许另立任务
   评估 QLD/TQQQ 的 role-limited implementation value。

本任务只获准执行第 1 步，并冻结第 2 步的单车道选择。Owner 当前方向不授权 cache mutation、
provider/API/CLI/cloud/QuantConnect 调用、经验研究、candidate/parameter search、backtest、holdout、
paper/live/broker/production 或投资结论。

## 2. 目标

在现有 `KEEP_CLOSED + PREREGISTRATION_ONLY` 边界内，建立可重放、可审计、fail-closed 的首个
增长假设合同。合同只定义研究问题、输入 authority、唯一 action、baseline/comparator、
evidence lineage、typed terminal taxonomy 和后继门禁，不读取或生成任何 market-result evidence。

首个假设语义固定为：

> 在独立 defensive hard gate 全部 clear 的前提下，一个由唯一预注册增长状态触发、只在
> QQQ/SGOV 内进行 bounded、non-leveraged reallocation 的 growth overlay，相对
> `equal_risk_qqq_sgov`，需要在扣除成本并控制 QQQ beta/exposure 后证明增量 action value，
> 同时不得违反 reviewed drawdown、false-risk-off、turnover、sample 与 evidence-integrity gates。

本任务不得用未审阅数值补齐任何 threshold、sample floor、position cap、cost、turnover、
drawdown、no-regression 或 exit policy。缺少 reviewed policy reference 时必须输出
`BLOCKED_POLICY_INPUT`，而不是临时基线或弱 PASS。

## 3. 必须继承的 authority

- `AGENTS.md`：Primary Research Window 从 `2021-02-22` 开始，DQ gate、heuristic governance、
  task registry、system flow 与 governed development 纪律；
- `config/research/strategy_research_reopen_readiness_decision_v1.yaml`：
  `KEEP_CLOSED + PREREGISTRATION_ONLY`、单一 data lane 与所有 empirical/external safety flags；
- `docs/requirements/TRADING-2515_Strategy_Research_Reopen_Readiness_Decision_V1.md`：真实 reopen、
  data-lane execution 和经验研究必须另立任务并取得独立 Owner exact decision；
- `config/research/simple_baseline_strategy_registry.yaml`：baseline identity
  `equal_risk_qqq_sgov`；
- `config/research/two_layer_strategy_boundary_contract.yaml`：risk veto 最高优先级、growth 与
  defensive channel 分离、当前 allocation candidate count 为 0；
- first-layer、defensive 与 two-lane closeout：现有 trend/add-risk family 不得被重包装为新候选；
- `config/research/strategy_style_discovery_universe_v1.yaml`：QLD 仅为 role-limited 2x
  implementation instrument，不是 trend signal、独立 style 或自由 candidate dimension；
- `docs/research/trading2458_candidate_family_retirement.md`：旧 300-candidate family 不得主动复用。

Primary Research Window 固定从 `2021-02-22` 开始。`2022-12-01` 不得成为新研究默认。

## 4. 合同边界

### 4.1 唯一 hypothesis 与 action

- hypothesis id：`BASELINE_BOUNDED_QQQ_GROWTH_OVERLAY_NON_BETA_ACTION_VALUE_V1`；
- baseline id：`equal_risk_qqq_sgov`；
- action universe：`QQQ`、`SGOV`；
- action count：1；candidate count：1；
- `uses_leverage_etf=false`；`uses_options=false`；
- parameter/candidate search 均为 false；
- defensive/risk-veto policy 不得被本任务修改或共同调参；
- growth signal 不得直接输出 official target weight、order 或 broker action。

### 4.2 单一数据车道

- selected lane：`QLD_CANONICAL_FULL_CACHE_DQ`；
- 当前任务只记录 Owner selection，不授权执行该 lane；
- QQQ Options lane 必须保持未选择、未授权；
- 同时出现两个 data lane 必须 fail closed；
- QLD DQ 只证明 shared canonical data qualification，不证明 QLD 或 growth action 有投资价值。

### 4.3 typed terminal taxonomy

后继 empirical evidence 只能聚合为：

- `PASS`：所有 mandatory axis 均 PASS；
- `FAIL`：protocol 有效但任一 mandatory axis 未满足 reviewed gate，退役当前 hypothesis/version；
- `INSUFFICIENT`：只有预注册证据缺失，不得扩大 signal universe；
- `INVALID`：任一 DQ/PIT、lineage、holdout、threshold-after-result、hidden-leverage 或 retired-family
  违规，全部经验结论作废。

聚合优先级固定为 `INVALID > FAIL > INSUFFICIENT > PASS`。任何优秀维度不得抵消 DQ、drawdown、
leverage attribution 或 protocol failure。

## 5. Mandatory evaluation axes

本任务只冻结 axis 与 policy-reference requirement，不计算结果：

1. non-beta action value；
2. net-of-cost return reconciliation；
3. actual-path drawdown/stress no-regression；
4. false-risk-off cost no-regression；
5. canonical DQ/PIT；
6. primary-window、mandatory slice 与 sample sufficiency；
7. actual-path turnover/execution trace；
8. leverage/beta/exposure attribution。

每个 axis 必须提供 `PASS/FAIL/INSUFFICIENT/INVALID` 的机械判定和 stop action；数值门槛只能来自
reviewed policy/version，不能在本合同、测试 fixture 或后继结果出现后临时发明。

## 6. 实施步骤

### S0：registration boundary

- canonical task row 与本 supporting requirement；
- generated task registry/current authority 重建；
- focused registration validation；
- registration-only commit、local-main fast-forward、ordinary push 与 exact base release。

### S1：serial contract wave

- reviewed preregistration policy；
- typed immutable source、authority inventory、semantic-fact validation；
- hypothesis/action/comparator/evaluation-axis/terminal taxonomy；
- canonical serialization、SHA-256 seal、from-json/replay；
- fail-closed builder，最高输出不超过 `PREREGISTRATION_FROZEN_AWAITING_DQ` 或 `BLOCKED_*`。

### S2：consumer-safe wiring

- architecture fragment 与 `docs/system_flow.md`；
- 只披露 preregistration、selected data lane 与 downstream gates；
- 不接入 empirical runner、backtest CLI、periodic operations、external platform 或投资报告结论。

### S3：验证与收口

- unit/property/golden 与 negative tests；
- authority hash drift、wrong window、double lane、hidden leverage、threshold-after-result、
  retired-family reuse、forged PASS、unauthorized empirical/cache/external action 均 fail closed；
- focused、adjacent、architecture/contract 与适用 formal validation；
- task status、generated authority、commit、local-main integration、ordinary push 与 workspace cleanup。

## 7. Task-owned 与 coordinator paths

预期 task-owned：

- `config/research/strategy_growth_action_value_preregistration_v1.yaml`；
- `src/ai_trading_system/strategy_growth_action_value_preregistration.py`；
- `tests/test_strategy_growth_action_value_preregistration.py`；
- `docs/requirements/TRADING-2540_Strategy_Growth_Action_Value_Preregistration_And_Single_Lane_Decision_V1.md`；
- 对应 architecture fragment。

Coordinator 负责：

- canonical task registry 与 generated views；
- `docs/system_flow.md`；
- architecture/current-authority/compatibility state；
- formal validation、local-main integration、ordinary push 与 cleanup。

如果实际实现需要修改 shared public schema、readiness state machine、DQ/PIT/cache identity 或其他
consumer-visible contract，必须先收窄为本 serial contract wave 并重新执行 governed preflight；
不得在 domain implementation 中静默扩张。

## 8. 验收标准

- 任务已通过 canonical registry 登记，包含 P0、owner、blocker、acceptance 与 requirement link；
- hypothesis、action、baseline、comparator、evaluation unit 和 selected data lane 唯一且无歧义；
- Primary Research Window 固定 `2021-02-22`，报告合同要求披露 requested/evaluated range；
- action 仅限 QQQ/SGOV bounded non-leveraged reallocation，不含 QLD/TQQQ/options/借入杠杆；
- defense 是独立 hard gate，risk veto 最高优先级且不被本任务共同调参；
- mandatory axes 与四态 taxonomy 可 canonical serialize、seal、from-json/replay；
- 缺少 reviewed threshold policy 时 fail closed 为 `BLOCKED_POLICY_INPUT`；
- selected data lane 最多一个，QLD lane 当前仅 selected-not-executable；
- retired family、historical leaderboard 与 prospective/locked holdout 不进入 active lineage；
- 所有 empirical/search/backtest/holdout/cache/external/paper/live/broker/production flags 保持 false；
- negative tests 覆盖 hash drift、wrong window、double lane、hidden leverage、threshold-after-result、
  retired-family reuse、forged PASS 与 unauthorized actions；
- 任务最高状态只允许 `PREREGISTRATION_FROZEN_AWAITING_DQ` 或 typed `BLOCKED_*`，不得输出
  strategy PASS、reopen authorization、official weights 或投资建议。

## 9. Blocker、后继任务与退出条件

当前 blocker：

- reviewed threshold policy references 需要在实现中完成 authority 盘点；缺失项不得自行补数值；
- QLD canonical DQ execution 尚未获得独立 task/token/source/cache mutation authorization；
- empirical growth evaluation、holdout、paper/live/broker/production 均未授权。

本任务 exit condition：serial preregistration contract、tests、system flow、canonical registry 与 formal
gates 完成并普通 push。完成后只允许另立 `QLD_CANONICAL_FULL_CACHE_DQ` 单车道任务；该任务 PASS
最多到 `READY_FOR_OWNER_REOPEN_REVIEW`，不自动授权经验研究。

## 10. 临时工作区生命周期

- registration/workspace：`D:\Work\AITradingSystem_trading2540_growth_registration`；
- owner task：`TRADING-2540_STRATEGY_GROWTH_ACTION_VALUE_PREREGISTRATION_AND_SINGLE_LANE_DECISION_V1`；
- purpose：从 exact local main 完成 registration boundary，并在登记后作为 task-owned
  `SINGLE_LANE` workspace；
- exit condition：task commit 已进入 validated local/remote main，canonical evidence 已保存，
  tracked/untracked/ignored 内容审计无 unique residue，且无 active process/lease 依赖后，通过
  `git worktree remove` 清理并 `git worktree prune`；
- recovery：清理前由 task branch 与 Git commit 恢复；清理后由 merged main 与 remote main 恢复。

## 11. 进度记录

- 2026-08-22：Owner 要求参考 Web Pro exact-commit 审阅顺序继续推进；本任务只启动 Wave 1
  preregistration serial contract，不运行任何 empirical、cache 或 external action。
- 2026-08-22：从 exact local/main `9717949319e619952c192e39c4ed2db1ee9f9eab` 创建 task-owned
  registration/workspace；下一步是 canonical task registration 与 registration boundary validation。
