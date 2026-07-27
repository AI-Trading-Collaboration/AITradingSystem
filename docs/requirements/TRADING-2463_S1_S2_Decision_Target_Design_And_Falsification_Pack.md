# TRADING-2463：S1/S2 Decision Target Design 与可证伪性设计包

最后更新：2026-07-27

状态：`S1_S2_COMPLETE_S3_NOT_STARTED`

所属任务：`TRADING-2463_DECISION_TARGET_REDESIGN_PREREGISTRATION`

Owner 决策：
`owner_decision:TRADING-2463:2026-07-27:proceed_s1_s2_design_and_falsification_pack_v1`

本文件只完成：

- S1：decision problem 与 target design option contract；
- S2：coverage、PIT/DQ feasibility 与 falsification design。

本文件不选择 target，不冻结 numeric threshold、sample floor 或 horizon，不读取新结果，
不运行模型或评估，也不授权 Decision Value Audit、risk overlay、candidate、backtest、
weights、paper-shadow、production 或 broker action。

## 1. 权威输入与不可继承结论

本设计只使用以下已归档事实：

- TRADING-2460 已建立 QQQ/SPY/SGOV adjusted-close label、interval、available-at 与
  capability receipt contract；
- TRADING-2461 已证明旧 target family 的 return target 没有通过 horizon，机械分类曾为
  `TAIL_RISK_ONLY_SKILL`；
- TRADING-2462 对 `QQQ_FUTURE_WORST_1D_RETURN` 的稳健性审计结论为
  `INSUFFICIENT_ROBUSTNESS_EVIDENCE`，Owner 已关闭该 capability path；
- TRADING-2459 已把 SPY 固定为 reference/regime-control，把 QLD 固定为 role-limited
  implementation instrument，并把 QLD automatic selection / production governance 延后到
  canonical DQ strict PASS 之后。

以下内容不得继承：

- 旧 worst-1d capability 的任何正面能力声明；
- TRADING-2461 的 model、feature prefix、horizon 或 threshold 作为新 target 的默认选择；
- TRADING-2462 的 4/5 mandatory variant、placebo 或 fold influence 通过作为新 target 的
  promotion evidence；
- QLD 历史收益、beta、turnover 或 Pareto 结果作为 signal、style 或 target label；
- scoped DQ PASS 作为 canonical full-cache strict PASS；
- TRADING-2452/2458 已退役 candidate identity、score、threshold 或 selection evidence。

## 2. S1：Decision Problem

### 2.1 决策问题

系统未来要回答的问题定义为：

> 在 decision cutoff 时只使用当时可得、PIT-safe 且通过适用 DQ gate 的信息，是否存在足够
> 稳健的证据支持组合在下一受治理决策区间维持防御、承担未杠杆 Nasdaq-100 风险，或承担更高
> QQQ-equivalent exposure？

该问题的输出首先是**经济决策语义**，不是证券代码、目标权重或交易指令。Target 只负责为
该决策提供可审计的预测对象；具体如何由 QQQ/SGOV/TQQQ 实现，属于未来独立的 decision-value
与 implementation policy。QLD 只能在 canonical DQ strict PASS 后、且上游已经独立形成
QQQ-equivalent exposure decision 时参与 implementation comparison。

### 2.2 非激活的语义动作集合

S1 只定义以下 action semantics，全部保持 `DESIGN_ONLY_NOT_ACTIVE`：

|action semantic|经济含义|当前实现边界|
|---|---|---|
|`DEFENSIVE_HOLD`|不主动承担 Nasdaq-100 风险，保留防御资产角色|不得自动映射为 SGOV 权重|
|`UNLEVERED_QQQ_RISK`|承担未杠杆 Nasdaq-100 风险|不得自动映射为 QQQ 权重|
|`HIGHER_QQQ_EQUIVALENT_RISK`|承担高于未杠杆的 QQQ-equivalent exposure|不得自动选择 TQQQ、QLD 或组合|

本集合不是 action universe 变更。它不定义仓位比例、切换阈值、rebalance cadence、持有期、
交易成本容差、risk budget 或退出规则。

### 2.3 决策与实现隔离

- QQQ/SGOV/TQQQ 保持 primary action assets；
- SPY 只作为 broad-equity reference / regime-control；
- QLD 保持 role-limited implementation instrument，不进入 target、feature、signal 或 style；
- target design 必须先于 instrument selection；
- target 通过 capability audit 也不等于某个 action 具有正 decision value；
- action 有正 decision value 也不等于 risk overlay、candidate、weights 或 production 获批。

## 3. S1：Target Design Options

所有 option 均为 `DESIGN_OPTION_ONLY`。`H` 表示未来由 Owner 审阅并在读取新结果前冻结的
session horizon；本文件不选择 `H`，也不把旧 `1d/5d/10d/20d` horizon 自动继承为新 policy。

### O1：`RELATIVE_OPPORTUNITY_SPREAD`

|字段|设计|
|---|---|
|支持的决策|防御与承担 QQQ 风险之间是否存在未来机会差|
|label|QQQ forward total return minus SGOV forward total return|
|方向与单位|连续值；正值表示 QQQ 相对 SGOV 的 gross opportunity 为正|
|label interval|decision session 后第一个共同 session 至第 `H` 个共同 session|
|available-at|第 `H` 个共同 session 的受治理 close publication 后|
|maturity|QQQ/SGOV 两条 total-return path 均完整且 applicable DQ PASS|
|action mapping|只能支持 defensive/risk-bearing 的方向性判断；不得直接生成权重或杠杆水平|
|主要优点|经济含义直接；既有 label/availability schema 可复用|
|主要风险|equity risk premium 基准率、正类不平衡、horizon overlap、gross opportunity 不等于可交易净价值|
|需要的新证据|独立新 capability audit；不得复用旧 return-target 失败或 tail-risk 通过项|

O1 若被选择，SPY_MINUS_SGOV 与 QQQ_MINUS_SPY 只可作为解释性 decomposition，不得被事后
挑选为更容易通过的替代 primary target。

### O2：`PATH_LOSS_BUDGET_EVENT`

|字段|设计|
|---|---|
|支持的决策|未来路径风险是否足以否决或限制承担 QQQ 风险|
|label|未来路径是否触及 Owner 预先冻结的 cumulative loss / drawdown budget，或对应连续严重度|
|方向与单位|event probability 或连续 path-loss severity；具体形式待 Owner 选择|
|label interval|decision session 后第一个共同 session 至第 `H` 个共同 session的完整路径|
|available-at|第 `H` 个共同 session close 后，且完整路径已成熟|
|maturity|路径无缺口、corporate action可解释、DQ与calendar contract通过|
|action mapping|只允许作为 risk veto/capability input；不得单独证明 risk-on opportunity|
|主要优点|比单点 worst-1d 更贴近持有区间中的可承受路径损失|
|主要风险|budget定义、base-rate稀疏、事件重叠、regime集中、阈值选择污染|
|需要的新证据|全新 label/policy/capability audit；旧 worst-1d capability 明确不可继承|

O2 不得把 `QQQ_FUTURE_WORST_1D_RETURN` 改名后重新启用。若 Owner 选择 event 形式，
budget、方向、event maturity 与连续/二分类选择必须在任何 event count 或结果读取前冻结。

### O3：`ACTION_REGRET_OR_NET_UTILITY`

|字段|设计|
|---|---|
|支持的决策|预先定义的 semantic action 中，哪个 action 的事后净效用或 regret 更低|
|label|每个 frozen action template 的 realized net utility，或相对 best frozen action 的 regret|
|方向与单位|utility / regret；单位由 return、risk penalty 与 cost contract共同决定|
|label interval|action 生效后的第一个可执行 session至第 `H` 个 session|
|available-at|所有 action path、cost与risk inputs成熟后|
|maturity|action template、execution timing、cost model、risk penalty和路径数据全部完整|
|action mapping|可以直接评估 action ranking，但不得跳过未来独立 Decision Value Audit|
|主要优点|预测对象与实际决策最直接|
|主要风险|utility/risk-aversion主观性、成本模型漂移、动作模板自由度、多重比较与结果驱动设计|
|需要的新证据|先建立 reviewed utility/action policy，再做独立 capability audit|

O3 当前不可执行，因为 risk penalty、action template、execution timing 与 net-utility policy
尚未获 Owner 审阅。不得用一个未经治理的 composite score 代替这些缺失决定。

### O4：`SEPARATE_OPPORTUNITY_AND_PATH_RISK`

|字段|设计|
|---|---|
|支持的决策|先判断机会差，再以独立路径风险判断是否否决或限制该风险承担|
|label|O1 opportunity target 与 O2 path-risk target 的双输出，不在 label 层合成为单分数|
|方向与单位|两个独立输出，各自保留原单位和 calibration|
|label interval|两者均使用同一 frozen `H` 与明确路径边界，或由 Owner显式批准不同 horizon|
|available-at|两项 label 均成熟后的较晚时点|
|maturity|O1与O2各自DQ/coverage/maturity均通过|
|action mapping|未来 policy 才可定义 opportunity gate 与 risk veto 的组合逻辑|
|主要优点|避免把收益和风险压成不可解释 composite|
|主要风险|双重选择、多重检验、组合 gate threshold不稳定、其中一项掩盖另一项无能力|
|需要的新证据|两项 capability 分别通过，再独立验证组合 mapping 的 decision value|

O4 不得把“任一 target 通过”解释为联合 target 通过，也不得根据结果选择串联或并联 gate。

## 4. S1 Option 对照与淘汰条件

|option|直接决策相关性|新增政策负担|主要可证伪难点|S1状态|
|---|---|---|---|---|
|O1 Relative Opportunity|中高|horizon、direction/classification policy|equity premium与fold/horizon稳定性|`ELIGIBLE_FOR_S3_REVIEW`|
|O2 Path Loss Budget|中高，偏risk veto|budget、event form、base-rate policy|稀疏事件与regime集中|`ELIGIBLE_FOR_S3_REVIEW`|
|O3 Action Regret/Utility|最高|action、cost、risk penalty、utility policy|policy sensitivity与多重比较|`ELIGIBLE_WITH_POLICY_GAPS`|
|O4 Separate Opportunity/Risk|高|两套target与组合gate policy|双重能力和mapping稳定性|`ELIGIBLE_WITH_MULTIPLICITY_GAPS`|

S1 不推荐 winner。以下任一情况应在 S3 直接移除对应 option：

- 无法给出唯一、可执行的 action mapping；
- label 在 decision cutoff 已部分包含未来信息；
- available-at 或 maturity 不能机械重建；
- 数据源或 DQ scope 必须依赖未经审阅的 scoped exception；
- 需要先查看新结果才能定义 horizon、threshold、sample floor 或 utility；
- 只能通过引入 QLD、旧 candidate output 或已关闭 tail-risk capability 才成立。

## 5. S2：PIT、DQ 与 Source Feasibility

|输入/合同|已知状态|S2结论|
|---|---|---|
|QQQ/SPY/SGOV adjusted-close panel|TRADING-2460 已有 receipt-bound schema；full canonical DQ 仍非 strict PASS|可用于设计与未来 exact-scope feasibility，不得声称 global DQ PASS|
|label interval / available-at|TRADING-2460 已有可重建合同|可复用 schema，不复用 target结论|
|QQQ-equivalent implementation|QQQ/SGOV/TQQQ action role已存在|只能在 target/action decision 后使用|
|QLD|role-limited implementation；automatic selection延期|不得进入本轮 target、feature或action label|
|SPY|reference / regime-control|可用于诊断 decomposition；不得成为自动 action asset|
|rate data|DATA-GOV Phase C source-owner/runtime attribution尚未完成|若 option依赖rate，必须等待独立DQ合同，不得静默排除|
|transaction cost|存在受治理cost model，但O3 utility policy未定义|只可登记依赖，不得在S2形成net-utility label|
|event calendar|尚无本任务冻结的event set|必须在结果读取前另行形成immutable event ledger|

所有未来 source package 必须记录 provider、endpoint、parameters、capture timestamp、row count、
requested/evaluated range、checksum、DQ report/receipt与consumer identity。Unknown scope、
missing lineage、非共同session、duplicate、non-finite、non-positive、future-dated 或不可解释的
corporate action必须 fail closed。

## 6. S2：Coverage 设计

### 6.1 必须覆盖的维度

未来 capability audit 必须在读取预测结果前冻结：

1. chronological purged walk-forward folds；
2. horizon 与 label-overlap-adjusted effective sample；
3. decision-time trend state；
4. decision-time realized-volatility state；
5. decision-time current-drawdown state；
6. rapid selloff、slow drawdown、recovery/whipsaw 与 sustained risk-on event families；
7. 每个 fold/regime/event cell 的 matured、eligible、purged、embargoed 和 missing counts；
8. source、ticker、field、date与row-level DQ attribution；
9. worst fold、fold influence 与单一时期贡献集中度；
10. option-specific base rate或action class balance。

Regime 只能从 decision time 已知的 trailing inputs形成。Event dates必须在结果读取前进入
immutable ledger，并记录来源与as-of；不得用 test label 或 prediction residual定义 regime/event。

### 6.2 Sample-floor governance

本文件不设置 numeric floor。S3/S4 若选择 target，必须为以下 floor 建立 reviewed policy：

- 每 fold minimum train/test effective sample；
- 每 regime cell minimum effective sample及minimum fold coverage；
- 每 event family minimum independent event count；
- event calibration 的minimum positive/negative count；
- horizon overlap后的effective-sample折减方法；
- final partial fold是否允许及其minimum coverage；
- 缺失cell触发 `INSUFFICIENT_COVERAGE` 的机械映射。

Floor rationale必须来自预期误差、置信区间、base rate、horizon overlap和可重复性需求，不得来自
“让当前数据通过”。在 floor 未冻结前，不得查看新 target 的 cell counts。

### 6.3 Coverage fail-closed

以下任一情况必须输出 `INSUFFICIENT_COVERAGE_OR_DQ`，不得合并cell、缩短horizon或删除fold：

- mandatory fold/regime/event cell低于reviewed floor；
-只有 pooled aggregate通过而多数fold方向不一致；
- 结果由单一fold、单一event或单一market phase主导；
- label maturity或source coverage随option/horizon系统性缺失；
- scoped DQ不能证明与target transitive dependency完全闭合；
- event定义、sample floor或regime cut在结果读取后改变。

## 7. S2：共同 Falsification Axes

未来独立 capability audit 至少预注册以下轴，并区分 mandatory 与 diagnostic：

|轴|目的|机械失败含义|
|---|---|---|
|Exact reconstruction|证明input、label、split与metric可重建|任何不一致均终止|
|Feature timing lag|识别同日可见性或publication timing依赖|lag后能力消失则不能称稳健|
|Purge/embargo stress|识别overlap leakage|合理更严格边界下崩溃则判脆弱|
|Fold jackknife|识别单fold主导|移除单fold后结论大幅反转则判脆弱|
|Regime concentration|识别只在单一已知状态成立|mandatory状态不可评估或方向反转则不通过|
|Event calibration|检查risk/event target的排序与基准率|event不足或calibration不稳定则不通过|
|Autocorrelation-preserving placebo|证明能力优于保留时序结构的null|不优于placebo则证伪|
|Target perturbation|检查定义边界的小幅、预注册变化|只在一个精确标签定义成立则判脆弱|
|Horizon family consistency|识别结果驱动挑选horizon|只有事后最佳horizon通过则不通过|
|Source-role ablation|识别单一资产/来源依赖|删除非必要source后完全崩溃则需解释或证伪|
|Simple-baseline increment|证明复杂模型增量|不优于预注册简单baseline则无可测能力|
|Multiple-testing control|阻止option/model/horizon事后筛选|未控制的winner不得进入Owner选择|

Placebo必须保留horizon overlap与时序相关性，不得使用简单iid row shuffle冒充严格null。

## 8. S2：Option-specific Falsification

### O1 Relative Opportunity

- 必须相对 train-only unconditional/base-rate baseline有增量；
- direction、rank与spread不能只在同一单fold或单horizon成立；
- QQQ_MINUS_SGOV decomposition用于解释，不允许事后改选SPY leg；
- gross opportunity skill不能被表述为net strategy value；
- 若所有预注册horizon均无稳定增量，则结论为`NO_MEASURABLE_SKILL`。

### O2 Path Loss Budget

- event threshold必须完全train-only或预先固定；
- 必须披露base rate、precision/recall、calibration、top-risk lift与false-negative cost；
- rapid shock与slow drawdown不能互相替代coverage；
- 若mandatory event family不足，必须`INSUFFICIENT_COVERAGE`，不能重演TRADING-2462的
  不可评估轴被部分通过项掩盖；
- 旧worst-1d结果不计入任何新gate。

### O3 Action Regret/Utility

- simple frozen action baseline必须先定义；
- risk penalty、cost、cadence或action set的预注册sensitivity必须全部报告；
- 若winner随合理policy variation频繁反转，则不得进入Decision Value Audit；
- 不允许从同一historical-seen结果同时选择action set、utility与预测模型；
- 必须把capability skill与action net value分成两道独立gate。

### O4 Separate Opportunity and Path Risk

- O1与O2必须分别满足各自mandatory gate；
- 组合mapping必须在联合结果读取前冻结；
- 不得按结果选择AND/OR、veto顺序或threshold；
- multiplicity必须覆盖两target、全部horizon与全部model attempts；
- 若只有单target有能力，结论只能退回单target owner review，不能声称联合能力。

## 9. Leakage 与 Selection-contamination 清单

- feature timestamp晚于decision cutoff；
- label interval与train/test边界相交；
- label maturity晚于train cutoff；
- standardization、imputation、threshold或regime cut使用test data；
- 通过新结果选择option、horizon、event、metric、model或sample floor；
- 从TRADING-2461/2462挑选旧通过项而忽略旧失败轴；
- 用QLD/TQQQ realized implementation outcome定义上游target；
- 用未来action PnL反向选择target；
- pooled metric掩盖fold/regime/event不稳定；
- 多option共享同一历史窗口却不计multiple testing；
- 把historical-seen称为prospective/OOS。

任一项成立均必须停止，不允许用报告文字降级为warning。

## 10. S3 前置问题与停止点

S1/S2 完成后，S3 在任何新 capability computation 前必须向 Owner 提交：

1. 保留哪些 option进入最终选择包；
2. 是否允许一个target或O4双target结构；
3. target form、direction、unit与action mapping；
4. horizon family及其经济rationale；
5. execution timing与available-at边界；
6. sample/fold/regime/event floors及其rationale；
7. mandatory/diagnostic falsification轴；
8. multiple-testing family与停止规则；
9. 选择一个target进入S4，或关闭本轮redesign。

当前停止点：

- `S1=COMPLETE`
- `S2=COMPLETE`
- `S3=NOT_STARTED`
- `S4=NOT_STARTED`
- `selected_target=NONE`
- `new_results_read=false`
- `prospective_accessed=false`
- `model_training_executed=false`
- `decision_value_audit_started=false`
- `risk_overlay_created=false`
- `candidate_backtest_weights_created=false`
- `qld_automatic_selection_enabled=false`
- `production_effect=none`
- `broker_action=none`

## 11. S1/S2 阶段退出证据

2026-07-27 在冻结的 S1/S2 final tree 上完成：

- focused：`133 passed`；
- Architecture：`756 passed`；
- Contract：`275 passed`；
- Reproducibility：`23 passed`；
- Integration：`995 passed / 642 warnings`；
- Full：`7581 passed / 5 skipped / 642 warnings`，provenance 绑定
  `TRADING-2463-S1-S2-20260727`，trigger reason 为
  `natural_integration_boundary`。

上述验证未运行任何新 target computation、模型训练、Decision Value Audit、risk overlay、
candidate/backtest/weights、paper-shadow 或生产动作。五类 validation runtime、checkout intent
与当前 lease events 已按 7 项显式路径白名单迁移至 canonical
`D:\Work\AITradingSystem`，共 27 个文件逐文件 SHA-256 一致；临时 worktree 继续按已登记的
known-unrelated exclusion 保留，不由本任务清理。
