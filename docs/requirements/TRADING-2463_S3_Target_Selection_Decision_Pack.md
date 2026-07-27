# TRADING-2463：S3 Target Selection Decision Pack

最后更新：2026-07-28

状态：`S3_PACK_COMPLETE_OWNER_SELECTION_REQUIRED_S4_NOT_STARTED`

所属任务：`TRADING-2463_DECISION_TARGET_REDESIGN_PREREGISTRATION`

进入 S3 的 Owner 决策：
`owner_decision:TRADING-2463:2026-07-28:enter_s3_target_selection_pack_v1`

本文件完成 S3 的 Owner redesign pack 与 target 选择位。它只使用 S1/S2 已冻结的设计事实，
不读取任何新 target result、coverage count、模型输出或 prospective evidence。

本文件中的 `RECOMMENDED_FOR_OWNER_SELECTION` 只是基于经济语义、政策负担、可审计性和
可证伪性的设计推荐，不是 capability claim，也不自动形成 active target。只有 Owner 后续
给出本文件第 10 节中的一个显式决定，S3 才能退出。

## 1. 当前不可变边界

- 已关闭的 `QQQ_FUTURE_WORST_1D_RETURN` capability 不得复活或更名继承；
- S1/S2 的四个 option 仍是 design options，不含新结果；
- QQQ/SGOV/TQQQ 保持 primary action assets；
- SPY 只作为 reference / regime-control；
- QLD 保持 role-limited implementation instrument，automatic selection 与 production
  governance 继续等待 canonical DQ strict PASS；
- `selected_target=NONE`，S4、capability audit、Decision Value Audit、risk overlay、
  candidate、backtest、weights、paper-shadow、production 与 broker action 均未启动；
- primary research window 仍从 `2021-02-22` 开始；S3 不创建新研究窗口。

## 2. S3 选择原则

S3 不按“哪个 label 更容易在历史上通过”排序，而按以下先验原则选择：

1. target 必须直接支持一个清晰的上游经济决策；
2. target 与 instrument、weights、risk overlay 和 execution policy 必须隔离；
3. label interval、available-at 与 maturity 必须可机械重建；
4. target 的主观 policy 自由度必须尽量少，并在结果读取前冻结；
5. 单 target 能回答问题时，不引入双 target multiplicity；
6. 不依赖尚未审阅的 utility、risk-aversion、event budget 或 scoped DQ exception；
7. 任一推荐都必须允许独立 capability audit 将其证伪并关闭。

这些原则只进行设计层筛选，不使用 predictive performance、event count、fold result、
base rate、candidate PnL 或历史收益排名。

## 3. 四个 Option 的 S3 Triage

|option|S3结论|核心理由|后续资格|
|---|---|---|---|
|O1 `RELATIVE_OPPORTUNITY_SPREAD`|`RECOMMENDED_FOR_OWNER_SELECTION`|经济语义直接；现有QQQ/SGOV label与availability schema可复用；单target政策负担最低|Owner可选择进入S4 freeze|
|O2 `PATH_LOSS_BUDGET_EVENT`|`DEFER_NOT_PRIMARY`|只能提供risk veto，不能单独证明risk-on opportunity；budget、event form与base-rate policy尚未冻结|可在未来独立risk-target任务重新评估|
|O3 `ACTION_REGRET_OR_NET_UTILITY`|`NOT_S4_ELIGIBLE_POLICY_GAPS`|utility、risk penalty、action template、execution timing与cost policy自由度过高|先完成独立utility/action policy后再立项|
|O4 `SEPARATE_OPPORTUNITY_AND_PATH_RISK`|`DEFER_MULTIPLICITY`|同时继承O1/O2两套能力与组合gate负担；当前没有理由在单target前引入双重选择|仅在两个独立target各自通过后再审|

### 3.1 为什么推荐 O1

O1 直接预测 QQQ 相对 SGOV 的未来 gross opportunity spread。它回答的是“是否有承担
Nasdaq-100 风险的相对机会”，而不是预测具体证券、权重或杠杆倍数。与其他 option 相比：

- 不需要先定义 path-loss budget；
- 不需要先定义 utility 或 risk-aversion；
- 不需要先定义双 target 组合 gate；
- 可沿用 TRADING-2460 已建立的 interval、available-at、maturity 与 receipt schema；
- 可以用连续值保留信息，不必在 S4 前先引入分类 threshold；
- 若未来 capability audit 不通过，可直接输出 `NO_MEASURABLE_SKILL` 并关闭，不需要用
  O2/O3/O4 事后补救。

该推荐不声称 O1 有预测能力，也不声称 O1 能产生正 strategy value。

### 3.2 为什么 O2 不作为当前 primary target

O2 的合理角色是对风险承担进行 veto 或 cap，而不是证明风险承担本身有正机会。若直接选择
O2，系统可能只学会识别部分路径风险，却无法回答何时应承担 QQQ 风险。当前还缺少：

- reviewed cumulative-loss / drawdown budget；
- continuous severity 与 binary event 的选择；
- event base-rate 与 minimum event coverage policy；
- false-negative cost 与 calibration gate；
- rapid shock、slow drawdown 等 event ledger。

因此 O2 保留设计价值，但不进入本轮 S4 primary target freeze。

### 3.3 为什么 O3 当前不具备 S4 资格

O3 虽然与最终 action 最接近，但 target 本身会被 action template、turnover、cost、
risk penalty 与 utility function共同定义。当前选择 O3 会把 target selection、decision-value
policy 和 implementation policy混在同一层，增加结果驱动设计空间。O3 必须先有独立 reviewed
utility/action policy，不能在本任务用临时 composite score 替代。

### 3.4 为什么暂不选择 O4

O4 需要 O1 与 O2 分别具备独立 capability，并另行验证 opportunity gate 与 risk veto 的
组合 mapping。当前直接选择 O4 会：

- 扩大 multiple-testing family；
- 增加两个 target、多个 horizon 与组合 threshold 的选择自由度；
- 允许一个 target 的正面结果掩盖另一个 target 的无能力；
- 在 O2 policy 尚未冻结时提前引入 risk overlay 语义。

因此先选择一个最小可证伪 target 更符合当前阶段。

## 4. 推荐的单 Target Contract

以下 contract 只有在 Owner 选择
`SELECT_O1_SINGLE_TARGET_FOR_S4` 后才成为 S4 的 freeze 输入；当前状态仍为 proposal。

|字段|S3 proposal|
|---|---|
|target id|`RELATIVE_OPPORTUNITY_SPREAD`|
|target form|continuous regression / ranking target；S4不得先行二值化|
|economic meaning|QQQ相对SGOV在受治理未来区间的gross total-return opportunity|
|direction|较大正值表示相对机会更有利；较大负值表示防御资产相对更有利|
|unit|decimal total-return spread，禁止混用百分数和basis points|
|label|`QQQ_FORWARD_TOTAL_RETURN - SGOV_FORWARD_TOTAL_RETURN`|
|decision cutoff|decision session受治理close publication完成后|
|label interval|下一共同session至第`H`个共同session；`H`在S4结果读取前由Owner冻结|
|available-at|第`H`个共同session的两条price path均成熟且receipt-bound DQ通过之后|
|maturity|QQQ与SGOV共同calendar、adjusted-close lineage、corporate-action解释和exact-scope DQ均完整|
|primary role|判断`DEFENSIVE_ELIGIBLE`与`RISK_BEARING_ELIGIBLE`之间的相对机会|
|prohibited role|不得直接选择QQQ/TQQQ/QLD、不得生成weights、不得决定杠杆水平|

`RISK_BEARING_ELIGIBLE` 只表示风险承担值得进入未来独立 Decision Value Audit，不等于
`UNLEVERED_QQQ_RISK` 或 `HIGHER_QQQ_EQUIVALENT_RISK` 已获选择。两者之间的 action mapping、
净成本、风险预算和权重必须留给后续独立 policy。

## 5. 单 Target 与双 Target 结构选择

S3 推荐：

- `target_structure=SINGLE_TARGET`
- `primary_target=RELATIVE_OPPORTUNITY_SPREAD`
- `secondary_target=NONE`
- `risk_veto_target=NONE`

理由是先证明一个最小、直接、可证伪的 opportunity target 是否有能力。O2 或 O4 只有在未来
存在独立 Owner 授权、独立 preregistration 与独立 multiplicity budget 时才能重新进入。

以下结构在本轮明确禁止：

- 用 O1 与 O2 任一通过即声称联合 target 通过；
- 在看到 O1 结果后临时增加 O2 作为补救 target；
- 把 O2 output 当作已经获批的 risk overlay；
- 用 QLD/TQQQ realized path 定义或校准 target；
- 用 SPY leg 替换 QQQ/SGOV primary spread。

## 6. S4 必须冻结的 Policy Slots

S3 不冻结 numeric policy。若 Owner 选择 O1，S4 必须在任何 label count、模型或结果读取前
逐项冻结并版本化：

### 6.1 Horizon

- 只允许一个 primary `H`；
- 经济 rationale 必须来自预期 decision cadence 与持有语义，而不是历史 metric；
- sensitivity horizon只能标记为非promotion evidence；
- 不得自动继承 TRADING-2461 的旧 horizon family；
- primary与sensitivity共同进入multiple-testing family。

### 6.2 Execution 与 availability

- decision cutoff 的market session与close publication边界；
- 下一共同session作为label interval起点；
- holiday、missing session与partial path的fail-closed处理；
- feature publication lag、price revision与receipt capture边界；
- target maturity晚于train cutoff时的purge规则。

### 6.3 Coverage 与 sample floors

以下 floor 必须由预期误差、置信区间、overlap-adjusted effective sample和可重复性需求解释，
不得根据当前数据“调到能通过”：

- minimum train/test effective sample per fold；
- minimum regime-cell effective sample与fold coverage；
- minimum independent event-family coverage；
- final partial fold policy；
- label-overlap折减方法；
- missing cell到`INSUFFICIENT_COVERAGE_OR_DQ`的机械映射。

S4 冻结前不得读取对应 cell counts。

### 6.4 Baseline 与 metric

- simple baseline必须是train-only unconditional spread或预先定义的简单预测器；
- primary metric必须与连续target form一致；
- classification threshold若未来需要，只能属于下游Decision Value Audit，不能回写target；
- 所有metric方向、aggregation与failure mapping必须在结果读取前冻结。

## 7. Mandatory 与 Diagnostic Falsification Axes

若 Owner 选择 O1，S4 推荐以下轴：

### 7.1 Mandatory

- exact input/label/split reconstruction；
- feature timing lag；
- purge/embargo stress；
- fold jackknife与fold influence；
- trend/volatility/current-drawdown regime concentration；
- autocorrelation-preserving placebo；
- target-boundary perturbation；
- primary与sensitivity horizon consistency；
- simple-baseline increment；
- multiple-testing control；
- exact-scope DQ与lineage closure。

任一 mandatory 轴不可评估、低于 reviewed coverage floor或触发 leakage，结论必须是
`INSUFFICIENT_COVERAGE_OR_DQ`、`INSUFFICIENT_ROBUSTNESS_EVIDENCE` 或
`NO_MEASURABLE_SKILL`，不得以其他轴通过抵消。

### 7.2 Diagnostic

- SPY-based decomposition；
- source-role ablation；
- rapid selloff、slow drawdown、recovery/whipsaw与sustained risk-on event slices；
- calibration-like bucket diagnostics；
- transaction-cost overlay的只读解释。

Diagnostic 不能单独使 target 通过，也不能改变 mandatory failure。

## 8. Multiple-testing Family 与停止规则

未来独立 capability audit 的同一 family 至少包括：

- 所有尝试过的 feature set；
- 所有 model class与hyperparameter attempt；
- primary及sensitivity horizon；
- 所有 target transformation、winsorization或normalization；
- 所有 metric与aggregation选择；
- 所有 threshold或bucket cut；
- 所有 fold/regime/event定义变化；
- 任何失败后提出的替代 target。

必须维护 append-only attempt ledger。以下任一情况立即停止：

- canonical或exact-scope DQ不能支持完整transitive dependency；
- label/available-at/maturity不能exactly重建；
- 任何feature或policy使用future information；
- mandatory coverage不足；
- 只有pooled aggregate或单fold通过；
- 只有事后最佳horizon、model或transformation通过；
- placebo、timing lag、purge/embargo或jackknife证伪能力；
- 必须增加O2/O4、QLD或旧tail-risk evidence才能维持结论；
- 结果被用于反向修改target、horizon、sample floor或event ledger。

停止后不得在同一任务内切换 target；新的 target 需要新的 Owner 决策和 preregistration。

## 9. S3 九个 Owner 问题的当前答案

|问题|S3答案|
|---|---|
|保留哪些option进入最终选择包|O1进入推荐选择位；O2/O4作为明确defer；O3当前不具备S4资格|
|单target还是双target|推荐单target；本轮不选择O4|
|target form/direction/unit/action mapping|O1 continuous QQQ-minus-SGOV decimal spread；只支持defensive/risk-bearing eligibility|
|horizon family|S4只冻结一个primary `H`；sensitivity不得承担promotion结论；当前不冻结数值|
|execution timing/available-at|decision close后开始下一共同session；第`H`个共同session两条path成熟且DQ通过后available|
|sample/fold/regime/event floors|S4按误差、overlap-adjusted effective sample与可重复性冻结；当前不查看counts|
|mandatory/diagnostic axes|第7节已分层；mandatory不可评估或失败即fail closed|
|multiple-testing与停止规则|第8节定义统一family、append-only ledger与停止条件|
|选择target或关闭|等待Owner在第10节明确选择|

## 10. Owner 显式选择位

S3 当前推荐但不自动执行：

### A. 选择 O1 单 Target 进入 S4（推荐）

决定 token：

`owner_decision:TRADING-2463:YYYY-MM-DD:select_o1_relative_opportunity_spread_single_target_for_s4_v1`

含义：S4 只冻结 O1 的 reviewed preregistration policy；仍不运行模型、capability audit、
Decision Value Audit、risk overlay、candidate/backtest/weights。

### B. 选择 O2 Risk-veto Target 进入 S4

决定 token：

`owner_decision:TRADING-2463:YYYY-MM-DD:select_o2_path_loss_budget_target_for_s4_v1`

前置条件：Owner必须同时选择continuous/binary form并授权建立budget与event policy。当前不推荐。

### C. 选择 O4 双 Target 进入 S4

决定 token：

`owner_decision:TRADING-2463:YYYY-MM-DD:select_o4_separate_opportunity_and_path_risk_for_s4_v1`

前置条件：Owner接受两套独立capability与组合mapping的multiplicity负担。当前不推荐。

### D. 关闭本轮 Redesign

决定 token：

`owner_decision:TRADING-2463:YYYY-MM-DD:close_decision_target_redesign_without_s4_v1`

含义：四个option均不进入active policy，未来重启必须另立任务。

O3 因 policy gaps 不提供直接 S4 选择位。若 Owner希望推进 O3，应先另立 reviewed
utility/action policy任务，而不是在本任务内绕过。

## 11. 当前停止点

- `S1=COMPLETE`
- `S2=COMPLETE`
- `S3=PACK_COMPLETE_OWNER_SELECTION_REQUIRED`
- `S4=NOT_STARTED`
- `recommended_target=RELATIVE_OPPORTUNITY_SPREAD`
- `selected_target=NONE`
- `target_structure=UNSELECTED`
- `numeric_policy_frozen=false`
- `new_results_read=false`
- `prospective_accessed=false`
- `model_training_executed=false`
- `capability_audit_started=false`
- `decision_value_audit_started=false`
- `risk_overlay_created=false`
- `candidate_backtest_weights_created=false`
- `qld_automatic_selection_enabled=false`
- `production_effect=none`
- `broker_action=none`

## 12. S3 Phase-exit 验证与证据

2026-07-28 的 S3 pack 验证均绑定
`TRADING-2463-S3-20260728` / `natural_integration_boundary`：

- focused：`136 passed`；
- Architecture：`759 passed`；
- Contract：`275 passed`；
- Reproducibility：`23 passed`；
- Integration：`995 passed / 642 warnings`；
- Full：`7584 passed / 5 skipped / 643 warnings`。

上述五类 runtime evidence、checkout intent 与当前 lease events 已按 7 项显式路径白名单
迁移到 canonical `D:\Work\AITradingSystem`，共 29 个文件逐文件 SHA-256 一致。
post-Full Architecture/Contract 仍须在本阶段最终 tracked state 上通过，完成后才允许提交；
该技术 closeout 不会自动选择 target，也不会启动 S4。
