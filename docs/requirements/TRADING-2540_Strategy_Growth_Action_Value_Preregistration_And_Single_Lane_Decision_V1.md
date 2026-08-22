# TRADING-2540：Strategy Growth Action Value Preregistration And Single-Lane Decision V1

最后更新：2026-08-22

稳定任务 ID：`TRADING-2540_STRATEGY_GROWTH_ACTION_VALUE_PREREGISTRATION_AND_SINGLE_LANE_DECISION_V1`

优先级：`P0`

状态：`BLOCKED_OWNER_INPUT`

mode：`SINGLE_LANE`

topology：`SERIAL_CONTRACT_WAVE_FIRST`

registration base：`9717949319e619952c192e39c4ed2db1ee9f9eab`

初始解释记录（已因后继 authority 冲突而暂停，不构成 active lane supersession）：
`owner_direction_interpretation:TRADING-2540:2026-08-22:proceed_serial_growth_preregistration_then_single_qld_dq_sequence_v1`

`production_effect=none`；`broker_action=none`；`external_action=none`。

## 1. Owner 方向与当前授权

Project Owner 要求参考 Web Pro exact-commit 审阅形成的顺序继续推进后续开发。该审阅只读取了
13 个选定文件，遗漏同一 exact commit 中已经生效的 TRADING-2516 至 TRADING-2539 后继
authority；因此以下顺序只能视为待冲突裁决的初始解释，不能直接覆盖现有单车道决定：

1. 先完成增长假设 preregistration serial contract wave；
2. 后继只保留一条 `QLD_CANONICAL_FULL_CACHE_DQ` 数据资格车道；
3. 只有 canonical strict DQ/PIT PASS 且取得独立 Owner reopen 授权后，才允许一次 locked、
   non-leveraged `QQQ/SGOV` growth action-value evaluation；
4. 只有非杠杆 growth action value 与独立 exposure-scaling hypothesis 均通过后，才允许另立任务
   评估 QLD/TQQQ 的 role-limited implementation value。

在 Project Owner 对现有 QQQ Options lane 与拟议 QLD lane 作出 exact successor decision 之前，
本任务不再获准冻结第 2 步或继续提交 S1/S2 实现。当前方向仍不授权 cache mutation、
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
- `docs/requirements/TRADING-2516_QC_QQQ_Options_Primary_Window_Evidence_Lane_Authorization_Refresh_V1.md`：
  已把 2515 的唯一数据证据车道落实为
  `QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE`，不得由 2540 静默覆盖；
- `docs/requirements/TRADING-2539_QC_Cloud_File_API_Exact_Content_Mutation_And_Retry_Proposal_V1.md`：
  已在该 QQQ Options lane 上完成 bounded zero-order execution，并把唯一缺失 session 定位到
  `2022-08-26`；当前实质 blocker 是可审计 exact-date options source，而不是“尚未选择车道”；
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

### 4.2 单一数据车道（等待 Owner successor decision）

- current active predecessor lane：`QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE`；
- proposed successor lane：`QLD_CANONICAL_FULL_CACHE_DQ`；
- 当前不得把 proposed lane 写成 selected/active，也不得把 predecessor lane 写成未选择；
- 同时激活两个 data lane 必须 fail closed；
- 若 Owner 选择切换，必须通过 versioned successor contract 正式暂停、关闭或 supersede 2516-2539
  active lane，同时保留其 immutable evidence；
- 若 Owner 选择保留 QQQ Options lane，2540 必须重新设计为与该 lane 兼容的 growth
  preregistration，不能包含 QLD selected-lane 语义；
- 任一路径都不自动授权 data execution、cache mutation、empirical evaluation 或投资解释。

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
- hypothesis、action、baseline、comparator 与 evaluation unit 唯一且无歧义；active data lane 必须由
  Owner successor decision 唯一确定；
- Primary Research Window 固定 `2021-02-22`，报告合同要求披露 requested/evaluated range；
- action 仅限 QQQ/SGOV bounded non-leveraged reallocation，不含 QLD/TQQQ/options/借入杠杆；
- defense 是独立 hard gate，risk veto 最高优先级且不被本任务共同调参；
- mandatory axes 与四态 taxonomy 可 canonical serialize、seal、from-json/replay；
- 缺少 reviewed threshold policy 时 fail closed 为 `BLOCKED_POLICY_INPUT`；
- selected data lane 最多一个；当前保持 predecessor QQQ Options lane，不得把拟议 QLD lane
  升级为 selected；
- retired family、historical leaderboard 与 prospective/locked holdout 不进入 active lineage；
- 所有 empirical/search/backtest/holdout/cache/external/paper/live/broker/production flags 保持 false；
- negative tests 覆盖 hash drift、wrong window、double lane、hidden leverage、threshold-after-result、
  retired-family reuse、forged PASS 与 unauthorized actions；
- 任务最高状态只允许 `PREREGISTRATION_FROZEN_AWAITING_DQ` 或 typed `BLOCKED_*`，不得输出
  strategy PASS、reopen authorization、official weights 或投资建议。

## 9. Blocker、后继任务与退出条件

当前 blocker：

- TRADING-2516 已选择 QQQ Options evidence lane，TRADING-2539 已在该 lane 上产生受控证据；
  Web Pro 文件清单遗漏这些后继 authority，因而其“尚未选择车道”前提不成立；
- Project Owner 必须明确选择二者之一：
  1. 通过 versioned successor contract 暂停/关闭/supersede QQQ Options active lane、保留
     2516-2539 immutable evidence，并唯一选择 QLD；或
  2. 保留 QQQ Options active lane，并移除 2540 草案中的 QLD selected-lane 语义；
- reviewed threshold policy references 需要在实现中完成 authority 盘点；缺失项不得自行补数值；
- QLD canonical DQ execution 尚未获得独立 task/token/source/cache mutation authorization；
- empirical growth evaluation、holdout、paper/live/broker/production 均未授权。

当前 unblock condition：收到上述二选一的 exact Owner direction，并将其记录为 versioned successor
authority。之后才能重做 preflight、修订合同并恢复 S1/S2。最终 exit condition 仍为 serial
preregistration contract、tests、system flow、canonical registry 与 formal gates 完成并普通 push；
任何后继数据任务 PASS 最多到 `READY_FOR_OWNER_REOPEN_REVIEW`，不自动授权经验研究。

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
- 2026-08-22：registration boundary 已提交为 `a55ebb43778cf6579e1086d62743481b40ecc019`，并完成
  local-main fast-forward、ordinary push，local main 与 remote main 一致。
- 2026-08-22：S1 草案生成后，本地 authority 复核发现 TRADING-2516 已选择
  `QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE`，TRADING-2539 已把缺失 session
  定位到 `2022-08-26` 并维持 exact-date source blocker。Web Pro 的 13 文件清单遗漏 2516-2539，
  因此初始 QLD 解释与项目 active authority 冲突。任务改为 `BLOCKED_OWNER_INPUT`；四个未提交草案
  文件保留在 task-owned worktree，仅用于可恢复审阅，不构成 authority，不运行回测、cache 或任何
  external action。恢复前必须取得 exact Owner successor decision，并重新执行 governed preflight。
