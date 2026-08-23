# TRADING-2540：Strategy Growth Action Value Preregistration And Single-Lane Decision V1

最后更新：2026-08-23

稳定任务 ID：`TRADING-2540_STRATEGY_GROWTH_ACTION_VALUE_PREREGISTRATION_AND_SINGLE_LANE_DECISION_V1`

优先级：`P0`

状态：`BASELINE_DONE`

mode：`SINGLE_LANE`

topology：`SERIAL_CONTRACT_WAVE_FIRST`

registration base：`9717949319e619952c192e39c4ed2db1ee9f9eab`

初始解释记录（已因后继 authority 冲突而暂停，不构成 active lane supersession）：
`owner_direction_interpretation:TRADING-2540:2026-08-22:proceed_serial_growth_preregistration_then_single_qld_dq_sequence_v1`

Owner 后继决定：
`owner_decision:TRADING-2540:2026-08-23:retain_qqq_options_lane_and_remove_qld_selected_lane_semantics_v1`

`production_effect=none`；`broker_action=none`；`external_action=none`。

## 1. Owner 方向与当前授权

Project Owner 要求参考 Web Pro exact-commit 审阅形成的顺序继续推进后续开发。该审阅只读取了
13 个选定文件，遗漏同一 exact commit 中已经生效的 TRADING-2516 至 TRADING-2539 后继
authority；因此其 QLD selected-lane 解释不能覆盖现有单车道决定。2026-08-23 Owner 已选择保留
QQQ Options active evidence lane，并要求继续推进本任务及其后继：

1. 先完成增长假设 preregistration serial contract wave；
2. 唯一 selected lane 保持
   `QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE`，移除 QLD selected-lane 语义；
3. 只有 canonical strict DQ/PIT PASS 且取得独立 Owner reopen 授权后，才允许一次 locked、
   non-leveraged `QQQ/SGOV` growth action-value evaluation；
4. 只有非杠杆 growth action value 与独立 exposure-scaling hypothesis 均通过后，才允许另立任务
   评估 QLD/TQQQ 的 role-limited implementation value。

TRADING-2541 已对该车道的唯一缺失 session `2022-08-26` 完成 exact-date recovery，并得到
`1202/1202`、unresolved=`0` 的 zero-order Cloud terminal。该结果修复 transport completeness，
但不自动提升为 DQ/PIT、策略、生产或 broker readiness。当前方向授权继续完成 S1/S2
preregistration contract；仍不授权 cache mutation、
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
- `docs/requirements/TRADING-2541_QC_QQQ_Options_Exact_Date_Subscription_Missing_Remediation_V1.md`：
  已在该 QQQ Options lane 上把唯一缺失 session `2022-08-26` 以 same-source-date、正确
  availability date 的 provider history 补齐；transport completeness=`1202/1202`、unresolved=`0`，
  但 `dq_pit_promoted=false`；
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

### 4.2 单一数据车道（Owner 已选择保留 QQQ Options）

- selected active lane：`QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE`；
- QLD 仍只保留 role-limited implementation instrument 语义，不是 selected data lane；
- 不得把 QLD 写成 selected/active，也不得把 QQQ Options predecessor lane 写成未选择；
- 同时激活两个 data lane 必须 fail closed；
- 2541 的 exact-date recovery 只修复该 lane 的 transport completeness；
- selected lane 本身不自动授权 data execution、cache mutation、empirical evaluation 或投资解释。

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

当前 blocker / next step：

- Owner 的单车道 successor decision 已收到，不再阻塞；
- TRADING-2541 已解除 `2022-08-26` transport completeness blocker，但没有执行 DQ/PIT promotion；
- reviewed threshold policy references 需要在实现中完成 authority 盘点；缺失项不得自行补数值；
- 当前 S1/S2 只允许冻结 QQQ Options-compatible preregistration contract；
- empirical growth evaluation、holdout、paper/live/broker/production 均未授权。

2026-08-23 authority inventory 已检查以下现有 policy：

- `action_value_score_policy_v2.yaml` 面向旧 second-layer label score，并非本 hypothesis 的
  exposure-matched action-value acceptance；
- `defensive_lane_action_value_policy.yaml` 只治理 defensive preservation，不能被 growth channel
  复用为共同调参门槛；
- `transaction_cost_model.yaml` 可作为未来 cost input 候选，但单独不能覆盖其余 mandatory axes；
- `first_layer_threshold_policy_v2.yaml` 属于已关闭 first-layer family；
- `promotion_gate_thresholds.yaml` 面向不同 promotion source gates；
- `threshold_registry.yaml` 状态为 `validation_inventory`，明确不是已校准统计边界。

因此目前不存在可直接覆盖八个 mandatory axes、且与本 hypothesis/comparator/evaluation unit 精确
匹配的 reviewed threshold bundle。preregistration 必须机械输出 `BLOCKED_POLICY_INPUT`；后继不能在
DQ、回测或结果可见后拼接这些历史阈值。

当前 next step：完成 authority inventory、修订合同并恢复 S1/S2。最终 exit condition 仍为 serial
preregistration contract、tests、system flow、canonical registry 与 formal gates 完成并普通 push；
任何后继数据任务 PASS 最多到 `READY_FOR_OWNER_REOPEN_REVIEW`，不自动授权经验研究。

本任务的实现 exit condition 已满足后，策略线由
[TRADING-2542 threshold policy decision pack and freeze](TRADING-2542_Growth_Action_Value_Threshold_Policy_Decision_Pack_And_Freeze_V1.md)
承接。2542 必须先于任何新 DQ/strategy result 冻结 exact threshold authority；在此之前，2540 保持
`BASELINE_DONE / BLOCKED_POLICY_INPUT`，不得以 2541 的 transport PASS 代替 threshold 或 DQ/PIT PASS。

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
- 2026-08-23：Owner 确认按既定后续顺序继续，接受保留 QQQ Options active lane、移除 QLD
  selected-lane 语义的建议。TRADING-2541 已实证补齐 `2022-08-26`，transport completeness=
  `1202/1202`；本任务恢复 `IN_PROGRESS`，只推进 S1/S2 preregistration，不执行 empirical、cache、
  Cloud、production 或 broker action。
- 2026-08-23：S1 threshold authority inventory 完成。六个相邻历史 policy 均因 scope、family、
  source-gate 或 calibration-state 不匹配而不能组成 TRADING-2540 reviewed threshold bundle；这一
  结论不是临时绕行。当前合同保持 `BLOCKED_POLICY_INPUT`，等待后继在任何 empirical result 可见前
  独立冻结精确 threshold policy。
- 2026-08-23：最新 main 协调合成使用 reviewed plan
  `integration-revalidation-fcdb8ef89400992d10b6`；唯一 domain overlap 为 canonical task view，已从
  task source 重放，Arch004E/report-flow/compatibility generated authority 均在最终树重建。Policy
  file/canonical/authority-set SHA-256 分别为
  `f7e769c4a9f82d0a462c327669df0cc2bdfeb3f151c292cdb784ad3327a52f46`、
  `0be7e6b775523e55d68574563bd9f77bd22682c4d5241a90d8404ae1b541956f`、
  `3198e9e52a1165f25ee494511810e1389e38acfd69645fc96687aa60cb2be417`；sealed decision
  canonical SHA-256=`0f52f4d6fd993e8120e87e044ca2ada9a87e312d8cc8847769314bf998f9c4db`。
  Focused contract=`43 passed`，Arch004E combined=`59 passed`，Ruff 与 strict mypy PASS；任务转为
  `BASELINE_DONE`，后继 TRADING-2542 已 registration-only 建立。
- 2026-08-23：首轮 final-tree Architecture=`864 passed / 1 failed`；唯一失败是新增一个 module 与
  一个 test 后 `arch_004g_deprecation_inventory` 的 frozen repository counts/id 尚未刷新，runtime artifact=
  `outputs/validation_runtime/architecture-fitness_20260822T233819Z/test_runtime_summary.json`。该失败保留，
  不以 serial pytest 替代。已按实际 scan 更新 module/test counts=`1141/1303` 与 inventory id
  `arch_004g_deprecation_inventory_bd9781d7b2e6f543c1bd`，随后重建 Arch004E/compatibility authority；
  correction rerun 必须使用明确 task/boundary provenance。
- 2026-08-23：Architecture correction rerun=`865 passed`、Contract=`276 passed`、Integration=
  `995 passed`、Reproducibility=`24 passed`。首轮 Full=`9308 passed / 30 failed / 6 skipped`，runtime
  artifact=`outputs/validation_runtime/full_20260822T235737Z/test_runtime_summary.json`。30 个失败归并为两个
  共享工程前置条件：9 个 O1 ledger test 所引用的 Git-ignored DQ gate 未随 temporary worktree 携带；
  其 retained main-workspace source 的 SHA-256 与 policy seal 同为
  `ca02b4310f99d664bb8d987debd4900f4367935b3938663c7a633400d988a1ca`。其余 21 个 Atlas failure 均由
  TRADING-2541 canonical `requirement_refs=[]` 与 page-effectiveness contract 不一致连锁触发。修正为：只把
  exact-hash DQ fixture 复制到 task worktree 的相同 ignored path；通过 canonical task-source event 补齐
  2541 requirement binding；刷新 Atlas 中 2540/2541 的实际终态说明。不得修改 O1 evidence bytes、不得
  把 transport PASS 提升为 DQ/PIT PASS。聚焦回归进一步确认新登记的 2542 必须进入 Atlas successor
  分类；渲染器的显式展示上限由 2541 精确推进到 2542，task coverage frozen count 由 62 更新为 63，
  未放宽 unknown-successor fail-closed 规则。最终 Full 必须绑定上述失败产物以 `failure_fix_rerun` 重跑。
