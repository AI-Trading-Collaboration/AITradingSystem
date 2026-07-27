# TRADING-2463：S4 O1 Relative Opportunity Spread Preregistration Freeze

最后更新：2026-07-28

状态：`S4_IN_PROGRESS_NUMERIC_POLICY_OWNER_REVIEW_REQUIRED_CAPABILITY_AUDIT_NOT_STARTED`

所属任务：`TRADING-2463_DECISION_TARGET_REDESIGN_PREREGISTRATION`

进入 S4 的 Owner 决策：

`owner_decision:TRADING-2463:2026-07-28:select_o1_relative_opportunity_spread_single_target_for_s4_v1`

## 1. 本阶段边界

Owner 已选择 O1 `RELATIVE_OPPORTUNITY_SPREAD` 作为本轮唯一 target，并授权进入 S4。
本文件只把该选择转化为结果读取前的 preregistration freeze proposal。Owner 尚未批准本文件
第 5 节的 numeric pilot policy bundle，因此当前：

- `selected_target=RELATIVE_OPPORTUNITY_SPREAD`
- `target_structure=SINGLE_TARGET`
- `primary_target=RELATIVE_OPPORTUNITY_SPREAD`
- `secondary_target=NONE`
- `risk_veto_target=NONE`
- `numeric_policy_frozen=false`
- `capability_audit_started=false`

S4 不读取新 label count、fold/regime/event coverage、模型输出、capability result 或 prospective
evidence，不运行模型、Decision Value Audit、risk overlay、candidate、backtest、weights、
paper-shadow、production 或 broker action。未来 capability audit 必须另立任务，不能由本文件
自动启动。

## 2. Owner 选择已经冻结的 Target Contract

以下非数值 target contract 自 Owner 选择起生效，不再是 S3 proposal：

|字段|冻结值|
|---|---|
|target id|`RELATIVE_OPPORTUNITY_SPREAD`|
|target form|continuous；不得在 target 层先行二值化|
|label|`QQQ_FORWARD_TOTAL_RETURN - SGOV_FORWARD_TOTAL_RETURN`|
|economic meaning|QQQ 相对可交易防御资产 SGOV 的未来 gross total-return opportunity|
|direction|正值越大表示相对风险承担机会更有利；负值越大表示 SGOV 相对更有利|
|unit|decimal total-return spread|
|structure|`SINGLE_TARGET`|
|action role|只判断 `DEFENSIVE_ELIGIBLE` 与 `RISK_BEARING_ELIGIBLE` 的相对机会|
|prohibited role|不得直接选择 QQQ/TQQQ/QLD、生成 weights、设定杠杆或形成 risk overlay|

`RISK_BEARING_ELIGIBLE` 只允许未来进入独立 Decision Value Audit，不表示任何风险资产、
杠杆工具或仓位已经获批。O2/O4 不得在看到 O1 结果后作为补救 target 加入；O3 继续受
utility/action policy gap 阻断。

## 3. 数据、时间与可见性合同

未来独立 capability audit 必须遵守：

- primary research window 从 `2021-02-22` 开始；
- label source 只允许受治理 QQQ/SGOV adjusted-close total-return panel；
- decision cutoff 是共同交易 session 的 close publication 完成后；
- label interval 从下一共同 session 开始，到第 `H` 个共同 session 结束；
- `label_available_on_session` 等于第 `H` 个共同 session，早于该时点不得训练或评估；
- 任一 train row 的 label interval 与 test interval 相交即 purge；
- target maturity 晚于 train cutoff 的 row 不得进入 train；
- holiday、missing common session、non-positive/non-finite price、duplicate key、revision 或
  receipt drift 均 fail closed；
- 在任何 data-dependent command 前执行项目统一 `aits validate-data` 路径；canonical DQ
  不是 strict `PASS` 时不得启动本轮新 audit，不复用 QLD scoped exception；
- source provider、endpoint、request parameters、capture timestamp、requested/evaluated
  range、row count、size 与 SHA-256 必须进入 immutable input commitment。

SPY 仅可作为 reference/regime-control diagnostic；QLD 保持 role-limited implementation
instrument，automatic selection 与 production governance 继续等待 canonical DQ strict PASS
后的独立 Owner review。

## 4. 结果读取前必须完成的冻结原则

### 4.1 单一主 Horizon

- 只允许一个 primary `H`；
- 本轮不设置 sensitivity horizon，避免在已知旧结果背景下扩大选择自由度；
- `H` 必须由 decision cadence、持有语义与 overlap-adjusted precision 共同解释；
- Owner 批准后，任何新增或替换 horizon 都构成新版本和新的 multiple-testing attempt。

### 4.2 Split、purge 与 effective sample

- outer evaluation 使用 session-indexed expanding purged walk-forward；
- fold schedule 必须在读取当前 coverage count 前冻结；
- embargo 至少覆盖 primary `H` 的相邻边界；
- effective sample 同时披露：
  - `non_overlap_equivalent = floor(eligible_rows / H)`；
  - 基于 label autocorrelation 的 ESS；
  - 两者中的较小值作为 coverage gate 输入；
- final partial fold 不能静默并入 pooled aggregate；
- coverage 不足时输出 `INSUFFICIENT_COVERAGE_OR_DQ`，不得缩短 horizon 或合并 fold。

### 4.3 Baseline、metric 与 capability class

- simple baseline 固定为 fold 内 train-only unconditional target mean；
- primary metric 必须是连续 target 相对该 baseline 的 out-of-fold loss improvement；
- rank correlation、directional accuracy、bucket spread 与 tail association只能作为预注册的
  supporting/diagnostic metrics，不能替代 primary failure；
- capability class 只允许：
  - `MEASURABLE_RELATIVE_OPPORTUNITY_SKILL`
  - `NO_MEASURABLE_SKILL`
  - `INSUFFICIENT_COVERAGE_OR_DQ`
  - `INSUFFICIENT_ROBUSTNESS_EVIDENCE`
- capability 通过不等于 strategy value、action selection、risk overlay 或 promotion 通过。

## 5. S4 Pilot Policy Bundle v1 Proposal

Policy ID：`TRADING_2463_O1_S4_PILOT_V1_PROPOSAL`

Policy owner：project owner

Policy status：`OWNER_REVIEW_REQUIRED_NOT_ACTIVE`

Rationale：本 bundle 以每周相对机会判断为经济单位，使用半年度 outer fold 取得可复核的
时间稳定性，并按 `H` 折减重叠 label。所有数字均在不读取本轮新 count、fold result 或模型输出
的前提下提出；它们是需 Owner 审阅的 pilot baseline，不是已证明最优的参数。

### 5.1 推荐数值

|policy slot|proposal|结果无关 rationale|
|---|---:|---|
|primary horizon `H`|5 common sessions|对应约一周相对机会；比 20-session target 提供更多近似独立 observation，且不改变最终 action cadence|
|sensitivity horizons|none|最小化 multiplicity；后续新增必须新版本|
|initial train raw rows|504|约两年共同 sessions，避免以单一短市场状态初始化|
|outer test raw rows|126|约半年，允许在时间上形成多个连续 fold|
|embargo|5 common sessions|与 primary label interval 等长|
|final partial raw-row floor|63|低于约一季度即不形成独立 final fold|
|minimum completed outer folds|5|要求跨多个连续时段而非 pooled aggregate|
|minimum train effective sample per fold|100|对应 `floor(504 / 5)` 的 non-overlap equivalent 下界|
|minimum test effective sample per full fold|24|接近 `floor(126 / 5)`，仅允许少量 DQ/purge 损耗|
|minimum test effective sample for final partial fold|12|对应 `floor(63 / 5)` 的下界|
|minimum total OOF effective sample|120|至少 5 个 full-fold floor 的总量|
|mandatory regime-cell effective sample|15 total and present in 3 folds|防止结论只来自一个 fold 或少量局部状态|
|mandatory event-family coverage|3 independent episodes across 2 folds|防止单一事件决定结论|

若 autocorrelation ESS 小于表内 non-overlap floor，以较小值为准并 fail closed；不得用 raw row
count 取代 effective sample。

### 5.2 推荐 Primary Gate

Primary score：

```text
OOF_MSE_SKILL = 1 - OOF_MSE_MODEL / OOF_MSE_TRAIN_MEAN_BASELINE
```

推荐 capability gate：

1. `OOF_MSE_SKILL` point estimate 至少为 `0.02`；
2. 以 5-session moving-block bootstrap 计算的 one-sided 95% lower confidence bound 大于 `0`；
3. 至少 4 个 completed outer folds 的 fold-level skill 为正；
4. worst completed fold 的 skill 不低于 `-0.10`；
5. Spearman rank correlation 的同类 lower confidence bound 大于 `0`；
6. 第 6 节所有 mandatory falsification axes 均可评估且通过；
7. exact reconstruction、DQ、coverage、multiple-testing ledger 任一失败均覆盖上述数值结果。

`0.02` 是拒绝统计上可见但经济上过小 improvement 的 pilot minimum practical effect；
`-0.10` 是限制单个时期显著反向失效的 pilot instability bound。两者必须由 Owner 显式批准，
未来 audit 不得根据结果下调。

### 5.3 Review 与失效条件

本 proposal 在下列任一情况发生时自动失效并需新版本：

- Owner 未显式批准而开始读取 coverage count 或结果；
- target、horizon、action semantics、canonical DQ contract 或 source lineage 改变；
- future audit 增加 sensitivity horizon、model family、target transformation 或 metric；
- 实际可用窗口无法满足本 bundle 的 coverage floor；
- policy review 发现 decision cadence 不再对应 5-session horizon。

## 6. Mandatory Falsification 与停止规则

未来独立 capability audit 必须全部执行：

- exact input/label/split reconstruction；
- feature timing lag；
- purge/embargo stress；
- fold jackknife与fold influence；
- trend/volatility/current-drawdown regime concentration；
- autocorrelation-preserving placebo；
- target-boundary perturbation；
- simple-baseline increment；
- multiple-testing control；
- exact-scope与canonical DQ/lineage closure。

以下任一情况立即停止并输出负面或不足结论：

- label、available-at 或 maturity 不能 exact 重建；
- future information、source drift 或 leakage；
- mandatory coverage floor 不满足；
- 只有 pooled aggregate、单 fold 或事后最佳 model/transformation 通过；
- placebo、timing lag、purge/embargo、jackknife 或 regime concentration 证伪能力；
- 必须增加 O2/O4、QLD、旧 tail-risk evidence 或下调 numeric gate 才能维持结论；
- 结果被用于修改 target、horizon、sample floor、metric、fold 或 event ledger。

停止后不得在同一 audit 内改 target 或降低 gate。

## 7. Multiple-testing Family

从 Owner 批准本 policy 起，append-only attempt ledger 至少包含：

- 所有 feature set 与 family prefix；
- 所有 model class、hyperparameter 与 preprocessing；
- 所有 target transformation、winsorization 与 normalization；
- 所有 metric、aggregation、threshold 与 bucket cut；
- 所有 fold、regime、event、embargo 与 sample-floor变化；
- 任何 sensitivity horizon 或失败后提出的替代 target。

未运行的 proposal 不计为 empirical attempt，但已知并讨论过的设计选项必须留在 ledger 的
design history 中。

## 8. Owner Review 选择位

### A. 批准完整 Pilot Bundle v1（推荐）

`owner_decision:TRADING-2463:YYYY-MM-DD:approve_s4_o1_pilot_policy_bundle_v1`

批准后本任务只完成 S4 freeze 与治理验证；capability audit 仍须另立新任务和新授权。

### B. 只修改 Horizon

Owner 指定一个新的 single primary `H` 及 decision-cadence rationale；其余数字必须按新 `H`
重新推导并形成 v2 proposal，不能直接沿用本表。

### C. 要求逐项重审

保持 `numeric_policy_frozen=false`，由 Owner 指定需要修改的 split、coverage、metric 或 gate。

### D. 关闭 O1 S4

不批准 numeric policy，关闭本轮 redesign；未来重启需新任务。

## 9. 当前停止点

- `S1=COMPLETE`
- `S2=COMPLETE`
- `S3=COMPLETE_O1_SELECTED`
- `S4=IN_PROGRESS_OWNER_POLICY_REVIEW_REQUIRED`
- `selected_target=RELATIVE_OPPORTUNITY_SPREAD`
- `target_structure=SINGLE_TARGET`
- `primary_horizon=UNAPPROVED_PROPOSAL_5_COMMON_SESSIONS`
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

## 10. S4 Entry Phase-exit 验证与证据

2026-07-28 的 S4 entry proposal 验证均绑定
`TRADING-2463-S4-ENTRY-20260728` / `natural_integration_boundary`：

- focused：`139 passed`；
- Architecture：`762 passed`；
- Contract：`275 passed`；
- Reproducibility：`23 passed`；
- Integration：`995 passed / 642 warnings`；
- Full：`7587 passed / 5 skipped / 642 warnings`。

上述五类 runtime evidence、checkout intent 与当前 lease events 已按 7 项显式路径白名单
迁移到 canonical `D:\Work\AITradingSystem`，共 19 个文件逐文件 SHA-256 一致。
post-Full Architecture/Contract 仍须在最终 tracked state 上通过。以上验证只证明 S4 entry
governance tree 一致，不批准 proposal 数字、不构成 capability evidence，也不启动未来 audit。
