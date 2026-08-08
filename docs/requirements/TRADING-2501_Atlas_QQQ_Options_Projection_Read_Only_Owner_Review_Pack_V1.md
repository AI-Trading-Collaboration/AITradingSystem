# TRADING-2501 Atlas QQQ Options Projection Read-Only Owner Review Pack V1

最后更新：2026-08-08

稳定任务 ID：`TRADING-2501_ATLAS_QQQ_OPTIONS_PROJECTION_READ_ONLY_OWNER_REVIEW_PACK_V1`

优先级：`P1`

状态：`BASELINE_DONE`

governed mode：`SINGLE_LANE`

exact registration base：`c233c82e2f6c4dfcd4b1302c99b98789bc6f3def`

Owner decision：`owner_decision:TRADING-2501:2026-08-08:build_read_only_owner_review_pack_before_canonical_projection_v1`

Owner acceptance：
`owner_decision:TRADING-2501:2026-08-08:accept_read_only_owner_review_pack_recommendations_v1`

production effect：`none`

broker action：`none`

## 1. 目标与当前决策

TRADING-2494、TRADING-2495 与 TRADING-2496 的 canonical Atlas authority 都明确排除
`TRADING-2481..2493`。用户接受 Web Pro 的建议：不直接把这 13 项投影进当前结果页，先形成一份
面向 Owner 的只读审阅包，逐项决定哪些事实可以进入下一轮 canonical projection contract。

本任务不改变现有页面、renderer、sidecar、canonical projection、研究状态、DQ/PIT、策略结论、
研究窗口、QQQ Options contracts 或外部平台状态。Owner 已按 exact token 接受逐项 decision，
因此本 review-pack blocker 已解除；在后继 serial projection contract/renderer consumer 完成前，
2481–2493 仍继续保持排除，当前页面不变。

## 2. Authority 与可追溯性

Web Pro advisory 使用 exact reviewed commit：
`ab22067ab9f57cc11144ae4eef899cb21f639181`，对 13 份 requirement 以及
`AGENTS.md`、TRADING-2494、TRADING-2495、TRADING-2496 做了文件级审阅。当前登记基线为
`c233c82e2f6c4dfcd4b1302c99b98789bc6f3def`；正式 Owner Review Pack 必须从该基线重新校验
每个 source path、task id、status、decision 与关键限制，不能把 Web Pro 建议当作 canonical fact。

13 个 exact source：

1. `TRADING-2481_QQQ_Options_Shared_Schema_Policy_Freeze_V1.md`；
2. `TRADING-2482_QQQ_Options_DQ_PIT_Cache_Evidence_Identity_V1.md`；
3. `TRADING-2483_QQQ_Options_Signal_Run_Manifest_Export_V1.md`；
4. `TRADING-2484_QC_QQQ_Options_Project_Adapter_Contract_V1.md`；
5. `TRADING-2485_QQQ_Option_Universe_Deterministic_Selection_V1.md`；
6. `TRADING-2486_QQQ_Options_Minute_Execution_Reality_Model_V1.md`；
7. `TRADING-2487_QQQ_Options_Cash_Premium_Settlement_Accounting_V1.md`；
8. `TRADING-2488_QQQ_Options_Lifecycle_Expiry_Corporate_Action_Safety_V1.md`；
9. `TRADING-2489_QC_QQQ_Options_Platform_Evidence_Manual_Bundle_V1.md`；
10. `TRADING-2490_QC_QQQ_Options_Local_Ingest_Validator_Reconciliation_V1.md`；
11. `TRADING-2491_QQQ_Options_Cross_Layer_Validation_Harness_V1.md`；
12. `TRADING-2492_QC_QQQ_Options_Bounded_Free_Cloud_Pilot_V1.md`；
13. `TRADING-2493_QC_QQQ_Options_Owner_Stage_Gate_Signoff_V1.md`。

### 2.1 Exact source replay manifest

以下清单从 registration commit `2064a2e1855229f7260c725f8287174dc09b63f3` 的 Git tree
直接重放。`blob` 是 Git blob identity，`bytes` 是该 checkout 中的文件字节数。按表中顺序拼接
`path|blob|bytes\n` 后的 source-set SHA-256 为
`29c97b0524c0ccf2ce1b215da9122bbfa875f45b08d682145a7409d6c1abd11f`。

|任务|source path|blob|bytes|
|---|---|---|---:|
|2481|`docs/requirements/TRADING-2481_QQQ_Options_Shared_Schema_Policy_Freeze_V1.md`|`a6a17188e81da79d1d70555725c6e66bbeedd426`|12,767|
|2482|`docs/requirements/TRADING-2482_QQQ_Options_DQ_PIT_Cache_Evidence_Identity_V1.md`|`8c820d8ff60cb19a954a5a5e54ffb54913790bfb`|10,175|
|2483|`docs/requirements/TRADING-2483_QQQ_Options_Signal_Run_Manifest_Export_V1.md`|`d911ee2d3107e8bada019c8cfc1e9f49f51b412d`|14,863|
|2484|`docs/requirements/TRADING-2484_QC_QQQ_Options_Project_Adapter_Contract_V1.md`|`59cb3f06a6ea26c9970077308f96a7361c75b053`|13,057|
|2485|`docs/requirements/TRADING-2485_QQQ_Option_Universe_Deterministic_Selection_V1.md`|`0ba4124cfffea203366c59deb4180b7679549f2e`|12,012|
|2486|`docs/requirements/TRADING-2486_QQQ_Options_Minute_Execution_Reality_Model_V1.md`|`61317fe900d0100cbebf0683c711848ffda21f5b`|15,349|
|2487|`docs/requirements/TRADING-2487_QQQ_Options_Cash_Premium_Settlement_Accounting_V1.md`|`12fc97779072a2784755f15a7af803bd7787047f`|18,000|
|2488|`docs/requirements/TRADING-2488_QQQ_Options_Lifecycle_Expiry_Corporate_Action_Safety_V1.md`|`bac081ec5335a7672c4520acacefbdf118383b42`|19,025|
|2489|`docs/requirements/TRADING-2489_QC_QQQ_Options_Platform_Evidence_Manual_Bundle_V1.md`|`2c13c30430dbdb889a84e45c8e4af7e939cb31d4`|20,461|
|2490|`docs/requirements/TRADING-2490_QC_QQQ_Options_Local_Ingest_Validator_Reconciliation_V1.md`|`aebf21519ad13f7b796145bbbe392a943f3fc059`|19,295|
|2491|`docs/requirements/TRADING-2491_QQQ_Options_Cross_Layer_Validation_Harness_V1.md`|`cbc706f0a0a342db8e9535602f529cd82968e92f`|11,492|
|2492|`docs/requirements/TRADING-2492_QC_QQQ_Options_Bounded_Free_Cloud_Pilot_V1.md`|`c8a47316c0e83f9469fa26d6956837aabd524bdf`|26,705|
|2493|`docs/requirements/TRADING-2493_QC_QQQ_Options_Owner_Stage_Gate_Signoff_V1.md`|`b4dbde69804a4aeccffe2692ebe9e5f42442f789`|12,387|

source-set 任一 path/blob/byte-count 漂移都必须重新审阅，不能继续沿用本 pack 的逐项建议。

## 3. 冻结的读者分组

Owner Review Pack 按读者理解顺序分成四组，而不是按文件编号平铺：

1. `2481–2484 Foundation contracts`：共享 schema、DQ/PIT identity、signal package、adapter contract；
2. `2485–2488 Mechanics implemented / policy blocked`：选券、执行、记账、生命周期机制；
3. `2489–2491 Evidence scaffolding / synthetic validation`：人工证据、local ingest/reconciliation、cross-layer harness；
4. `2492–2493 Historical external evidence / governance verdict`：bounded pilot 与 Owner aggregate gate。

每组必须先回答“这组让系统具备了什么”，再回答“它仍不能证明什么”，最后给出“Owner 需要决定什么”。

## 4. 五层状态合同

每个任务必须分别展示五个互不替代的状态层：

1. `engineering_baseline`：工程合同或实现是否存在并通过其记录的验证；
2. `evidence_quality`：证据是 synthetic、manual、bounded external 还是更强 authority；
3. `policy_readiness`：投资解释相关阈值与执行政策是否经 Owner review；
4. `external_authority`：外部平台、数据、run、export 权限是否存在；
5. `strategy_conclusion`：是否足以形成策略有效性、收益、稳健性或可部署结论。

`engineering_baseline=READY` 不得被渲染为 `strategy_conclusion=PASS`。只要 policy、external authority
或 strategy evidence 仍 blocked/unknown，视觉上不得使用会被普通读者理解为“策略已验证”的绿色主状态。

## 5. 初步逐项建议（待 Owner 决定）

### 5.1 A/B/C/D 表示什么

这些字母不是优先级、成绩或完成度，也不使用“绿色=成功”的隐含语义：

- `A — MAINLINE_GOVERNANCE_FACT`：Owner 接受后，可作为主阅读层的基础事实或支配性治理结论；
- `B — SECONDARY_EVIDENCE_FACT`：Owner 接受后，只进入次级证据层或组级摘要，不能成为主状态；
- `C — BLOCKED_MECHANICS_FACT`：Owner 接受后，只能说明“机械能力已实现但 policy 未授权”，必须与
  cash-preservation/blocker 同屏；
- `D — KEEP_EXCLUDED`：source drift、authority 不足、含义冲突或读者风险未消除，继续排除。

`A` 不表示策略 PASS。2492/2493 被建议为 A，恰恰因为 `NO-GO` 是必须让读者优先看到的支配性事实。

### 5.2 五层状态矩阵

`engineering` 使用 current task register（source of truth）的状态；其他四层从 exact requirement facts
保守归纳。`NOT_ESTABLISHED` 表示没有形成策略有效性结论，不等于负收益结论。

|任务|engineering|evidence quality|policy readiness|external authority|strategy conclusion|建议|
|---|---|---|---|---|---|---|
|2481|`BASELINE_DONE`|`OFFLINE_CONTRACT_REPLAY`|`FROZEN_WITHOUT_INVESTMENT_THRESHOLDS`|`NOT_GRANTED`|`NOT_ESTABLISHED`|A|
|2482|`BASELINE_DONE`|`OFFLINE_DQ_PIT_REPLAY`|`NUMERIC_FRESHNESS_THRESHOLDS_UNRESOLVED`|`NOT_GRANTED`|`NOT_ESTABLISHED`|A|
|2483|`BASELINE_DONE`|`OFFLINE_SIGNAL_PACKAGE_REPLAY`|`ETF_MAPPING_AND_NON_PRIMARY_UNRESOLVED`|`NOT_GRANTED`|`NOT_ESTABLISHED`|A|
|2484|`BASELINE_DONE`|`OFFLINE_ADAPTER_CONTRACT_REPLAY`|`CONTRACT_READY_NO_CLOUD_RUN`|`NOT_GRANTED`|`NOT_ESTABLISHED`|B|
|2485|`BASELINE_DONE`|`SYNTHETIC_SELECTOR_MECHANICS`|`OWNER_REVIEW_REQUIRED_SELECTION_FALSE`|`NOT_GRANTED`|`NOT_ESTABLISHED`|C|
|2486|`BASELINE_DONE`|`SYNTHETIC_EXECUTION_MECHANICS`|`OWNER_REVIEW_REQUIRED_EXECUTION_FALSE`|`NOT_GRANTED`|`NOT_ESTABLISHED`|C|
|2487|`BASELINE_DONE`|`SYNTHETIC_ACCOUNTING_MECHANICS`|`OWNER_REVIEW_REQUIRED_ACCOUNTING_FALSE`|`NOT_GRANTED`|`NOT_ESTABLISHED`|C|
|2488|`BASELINE_DONE`|`SYNTHETIC_LIFECYCLE_MECHANICS`|`OWNER_REVIEW_REQUIRED_LIFECYCLE_FALSE`|`NOT_GRANTED`|`NOT_ESTABLISHED`|C|
|2489|`BASELINE_DONE`|`OFFLINE_MANUAL_BUNDLE_CONTRACT_ONLY`|`COLLECTION_AUTHORIZED_FALSE`|`NOT_GRANTED`|`NOT_ESTABLISHED`|B|
|2490|`BASELINE_DONE`|`SYNTHETIC_RECONCILIATION_ONLY`|`RECONCILIATION_AUTHORIZED_FALSE`|`NOT_GRANTED`|`NOT_ESTABLISHED`|B|
|2491|`BASELINE_DONE`|`SYNTHETIC_CROSS_LAYER_ONLY`|`PILOT_AUTHORIZED_FALSE`|`NOT_GRANTED`|`NOT_ESTABLISHED`|B|
|2492|`BASELINE_DONE`|`BOUNDED_EXTERNAL_EVIDENCE_SCOPE_VIOLATION`|`SINGLE_USE_TOKEN_CONSUMED`|`NO_FURTHER_ACTION_AUTHORIZED`|`PILOT_NO_GO_LICENSE_OR_EVIDENCE`|A|
|2493|`BASELINE_DONE`|`OWNER_SIGNED_GOVERNANCE_RECORD`|`SIGNED_NO_GO`|`NO_EXTERNAL_ACTION_AUTHORIZED`|`NO_GO_KEEP_BLOCKED`|A|

审计发现：2489 supporting requirement 顶部仍写 `IN_PROGRESS`，current task register 已是
`BASELINE_DONE`。本 pack 依项目规则以 task register 为 lifecycle source of truth，但把该不一致保留为
`SOURCE_STATUS_MISMATCH_REVIEW_REQUIRED`；在进入 canonical projection contract 前，应由 2489 authority
owner 另行协调，而不是由 2501 静默改写历史任务文件。

### 5.3 四组 reader-first 审阅卡

#### 组 1：2481–2484 Foundation contracts

一句话结论：**底座已经能被严格重放，但真实期权研究还没有因此被证明。**

|任务|已完成什么|还不能证明什么|当前 blocker|Owner 初步建议|
|---|---|---|---|---|
|2481|冻结 12 类 shared records、canonical seal/replay、安全与 export 边界。|不证明任何选券、成交、收益或平台能力。|投资阈值与 external token 均未授予。|接受 A，表述为“合同底座已冻结”。|
|2482|冻结 local-cache 与 option-event 分轴的 DQ/PIT、15 项检查和 evidence identity。|不证明 quote/OI/Greeks 在真实研究窗完整、及时或可交易。|freshness/spread 等数值 policy 未审。|接受 A，表述为“质量检查规则已就位”。|
|2483|建立 daily signal/run package、primary window 与 canonical DQ receipt replay。|不证明 ETF direction mapping、选券、执行或收益。|ETF mapping 与 non-primary authority 未审。|接受 A，表述为“研究输入包可审计”。|
|2484|建立离线 QC adapter descriptor、subscription/engine/result mapping contract。|不证明 entitlement、真实 engine identity、历史 coverage 或 cloud run 可用。|external token/input admission 未授予。|接受 B，只在证据层展示。|

#### 组 2：2485–2488 Mechanics implemented / policy blocked

一句话结论：**选券、执行、记账和生命周期“怎么运转”已实现，但决定真实行为的 policy 钥匙尚未插入。**

|任务|已完成什么|还不能证明什么|当前 blocker|Owner 初步建议|
|---|---|---|---|---|
|2485|确定性 candidate filtering/ranking、SID tie-break 与 no-contract cash path。|不证明哪组 DTE/delta/spread/OI/volume/rank 适合投资。|`selection_authorized=false`；所有数值为 `UNKNOWN_REQUIRES_POLICY_REVIEW`。|接受 C，与 policy blocker 同屏。|
|2486|next-independent-minute、bid/ask side、limit、partial/reject/cancel 机械与 DQ replay。|不证明真实 latency、slippage、fee、quote-age 或成交概率。|`execution_authorized=false`；现实模型数值未审。|接受 C，不使用“执行验证通过”。|
|2487|Decimal cash/reservation/premium/fee/settlement/lot/snapshot mechanics。|不证明真实初始资金、预算、费用、结算或策略 PnL。|`accounting_authorized=false`；cash/fee/settlement 方法未审。|接受 C，表述为“会计机械已实现”。|
|2488|open/partial/exit/expiry/scope-violation lifecycle 与 fail-closed safety。|不证明真实 exercise/assignment/corporate-action 处置可用。|`lifecycle_authorized=false`；pre-expiry/settlement policy 未审。|接受 C，突出 scope violation 与 invalid-run。|

#### 组 3：2489–2491 Evidence scaffolding / synthetic validation

一句话结论：**证据收集、对账和跨层验收工具已就位，但它们目前主要证明“工具会检查”，不是“真实策略已通过”。**

|任务|已完成什么|还不能证明什么|当前 blocker|Owner 初步建议|
|---|---|---|---|---|
|2489|定义 Results/Orders/Trades/Logs/Report/Project Files/screenshot/attestation strict bundle。|不证明真实 bundle 已完整收集或 license/export 已满足。|`collection_authorized=false`，且存在 supporting-status mismatch。|接受 B，但先标注 source mismatch。|
|2490|定义 normalized ingest、七类 difference taxonomy 与 strict reconciliation。|不证明真实 external/local facts 已在 reviewed tolerance 下对齐。|`reconciliation_authorized=false`；mapping/tolerance 未审。|接受 B，只展示“对账工具就位”。|
|2491|建立十个 synthetic scenarios、golden identity、cross-layer report 与 blocked checklist。|synthetic PASS 不证明 platform evidence、市场有效性或可扩窗。|pilot checklist 默认 `BLOCKED_OWNER_AUTHORIZATION`。|接受 B，必须带 synthetic-only badge。|

#### 组 4：2492–2493 Historical external evidence / governance verdict

一句话结论：**唯一一次 bounded external pilot 留下了可审计事实，但违反预注册资源上限；Owner 已签署继续 NO-GO。**

|任务|先给读者看的结论|解释原因|随后才可补充的事实|Owner 初步建议|
|---|---|---|---|---|
|2492|`PILOT_NO_GO_LICENSE_OR_EVIDENCE`。|16 项 scope audit 中 `PROCESSED_DATA_POINTS` 唯一 FAIL：`734127 > 250000`；single-use token 已消费并失效，2489/2490 仍 blocked。|仅在上述结论后说明：单日 run completed、1 order/1 fill、09:31→09:32→09:33 chronology、无 raw option rows。|接受 A，把 NO-GO 与 cap violation 置于主层。|
|2493|aggregate=`NO_GO_KEEP_BLOCKED`，terminal=`SIGNED_NO_GO`。|license、DQ/PIT、resource、shared reconciliation、range、paid-tier 轴均 `NO_GO`，并保留五项 UNKNOWN/exit condition。|platform capability 与 technical correctness 只有 subordinate `CONDITIONAL_GO`，不得覆盖 aggregate。|接受 A，作为当前 QQQ options 治理总判定。|

2492 的 `$100000.00 → $100088.35`、terminal holdings 或 runtime unrealized 只属于一次 bounded smoke 的
运行事实，不能进入读者主层，也不能被命名为收益证明。

### 5.4 Owner 逐项 decision contract

Owner 对每项只能选择以下之一：

- `ACCEPT_AS_RECOMMENDED`：接受本 pack 的 A/B/C 层级和 reader wording；
- `DOWNGRADE_TO_SECONDARY_OR_BLOCKED`：A→B/C 或 B→C，说明原因；
- `KEEP_EXCLUDED`：等同 D，给出 blocker 与可观察 exit condition；
- `RETURN_FOR_SOURCE_RECONCILIATION`：发现 source/status/identity 冲突，先回原 authority owner 修复。

Owner 已接受集合：`A={2481,2482,2483,2492,2493}`、`B={2484,2489,2490,2491}`、
`C={2485,2486,2487,2488}`、`D={}`。acceptance exact token 为：

```text
owner_decision:TRADING-2501:2026-08-08:accept_read_only_owner_review_pack_recommendations_v1
```

该 token 同时接受本节的 reader wording、2489 source-status mismatch 披露、2492 阅读顺序、2493 dominance、
primary-window 与 no-green/no-strategy-PASS hard stops。它只授权后继任务建立 serial projection contract/renderer
consumer；在该后继任务完成前，2494–2496 的 13 项排除规则和当前页面保持不变。

## 6. 冲突支配与 hard stops

- 2492 的读者顺序固定为：`NO-GO` → 原因 → cap violation `734127 > 250000` → 最后才说明
  `1 order / 1 fill`；不得用订单/成交先制造“试点成功”的印象；
- 2493 的 aggregate `NO_GO_KEEP_BLOCKED` 支配 subordinate `CONDITIONAL_GO`；
- 2494–2496 的 excluded-task 规则在 Owner decision 前保持原样；
- primary requested/evaluated start 保持 `2021-02-22`；`2022-12-01` 仅可作为历史语境；
- 不新增 DTE、moneyness、delta、spread、OI、volume、fee、slippage、latency、partial-fill、expiry
  或任何投资解释阈值；
- 不登录或调用 QuantConnect，不执行 API/CLI/HTTP/download/export/cloud backtest；
- 不写 `outputs/atlas/**`，不改变 current canonical page；
- 不产生投资建议、promotion、paper/live/broker/production action。

## 7. 实施阶段

1. S0：登记 task row 与本 requirement，完成短 registration boundary ordinary push；
2. S1：从 exact latest main 逐文件重放 13 项 authority，记录 path/blob/task/status/decision；
3. S2：形成四组、五层状态、A/B/C/D decision matrix 与 reader-first 摘要；
4. S3：加入 2492/2493 dominance checks、窗口与 excluded-task hard stops；
5. S4：静态/确定性/authority focused validation，刷新 task shadow 与必要 append-only current authority；
6. S5：Owner review handoff；保持 `IN_PROGRESS`/`OWNER_REVIEW_REQUIRED`，不自动修改 projection；
7. S6：只有 Owner 逐项接受后，另立 serial projection contract/renderer consumer 任务。

## 8. 路径所有权

task-owned：

```text
docs/requirements/TRADING-2501_Atlas_QQQ_Options_Projection_Read_Only_Owner_Review_Pack_V1.md
```

coordinator-owned（仅在确定性工具要求时）：

```text
docs/task_register.md
registry/development_tasks_shadow/**
registry/development_tasks_shadow_v2/**
inputs/architecture/** current authority
tests/test_arch_004_refactor_policy.py
tests/test_arch_004g_deprecation.py
```

明确不拥有：

```text
docs/system_flow.md
src/ai_trading_system/atlas/**
tests/atlas/**
outputs/atlas/**
src/ai_trading_system/qqq_options_research/**
config/research/qqq_options_*.yaml
```

本任务不改变数据流，因此不更新 `docs/system_flow.md`。不读取、hash、stage 或修改
registered known-unrelated exclusion。

## 9. 验收标准

1. 13 个 task/source exact 一一对应，缺失、重复、漂移或 task id mismatch fail closed；
2. 四组与五层状态完整，engineering/evidence/policy/external/strategy 不互相偷换；
3. 每项具备“一句话定位、已完成、未证明、仍阻塞、Owner decision”五类信息；
4. A/B/C/D 建议可追溯至 exact source，不以颜色或完成状态推断策略 PASS；
5. 2492、2493 dominance 和 primary-window 约束通过 deterministic checks；
6. 2494–2496 excluded-task authority、current page bytes 与 Atlas renderer bytes保持不变；
7. task shadow、DevEx/necessary compatibility authority 与 focused validation PASS；
8. Owner 未签署前不进入 canonical projection；
9. `investment_conclusion_generated=false`、`production_effect=none`、`broker_action=none`。

## 10. 生命周期记录

- 2026-08-08：Web Pro 建议 `PROCEED_TO_READ_ONLY_OWNER_REVIEW_PACK`；用户接受并要求继续；
- 2026-08-08：与 QQQ engineering 线程冻结顺序：2500 terminal push → 2501 registration push → 2499 START；
- 2026-08-08：2500 exact main `c233c82e2f6c4dfcd4b1302c99b98789bc6f3def` RELEASE，开始短登记边界；
- 2026-08-08：registration boundary ordinary push 完成，exact main=
  `2064a2e1855229f7260c725f8287174dc09b63f3`；task shadow=`967/462/505`、v1/v2
  byte-identical，DevEx=`1095 modules/1259 tests/856 writers/0 violations`，focused governance=`23 PASS`；
- 2026-08-08：从该 exact tree 重放 13 个 source，source-set SHA-256=
  `29c97b0524c0ccf2ce1b215da9122bbfa875f45b08d682145a7409d6c1abd11f`；完成四组、五层状态、
  A/B/C/D 建议、2492/2493 dominance 与 Owner decision contract；
- 2026-08-08：Owner 回复“按建议接受”，封存 acceptance token=
  `owner_decision:TRADING-2501:2026-08-08:accept_read_only_owner_review_pack_recommendations_v1`；
- 2026-08-08：基于 frozen base `2064a2e1855229f7260c725f8287174dc09b63f3`、lane head
  `7f227c6159426dc5b1aad0630efa72e5193666aa` 与 latest main
  `2cff6c2641d168f0b51120cf6150dbc46d0b9fec` 生成并验证 drift plan=
  `integration-revalidation-9dff5eabaeb0c0a0e8c7`；decision=`READY_FOR_SINGLE_INTEGRATION_CANDIDATE`、
  overlap=[]、contract conflict=[]。
- 当前：`BASELINE_DONE`。A/B/C/D 分层与 reader wording 已获接受并完成 latest-main integration；
  canonical projection、renderer/page mutation、外部动作与投资结论仍未启动，须由后继 serial
  projection contract/renderer consumer 任务另行登记与实现。

## 11. Owner 初步验收入口

建议按以下顺序阅读，避免先被实现细节淹没：

1. 先审 2493：当前总判定是否应明确保持 `NO_GO_KEEP_BLOCKED`；
2. 再审 2492：是否同意把 `734127 > 250000` 放在 1 order/1 fill 之前；
3. 再审 2481–2484：哪些基础事实值得进入主层，哪些只放证据层；
4. 再审 2485–2488：是否同意统一使用“机械已实现 / policy 未授权”；
5. 最后审 2489–2491：是否同意统一标记 manual/synthetic evidence 限制，以及 2489 source-status mismatch。

|审阅范围|建议|Owner decision|需要补充的理由|
|---|---|---|---|
|2481–2483 foundation mainline facts|A|`ACCEPT_AS_RECOMMENDED`|主层仍须明确“合同底座，不是策略结论”。|
|2484 adapter evidence|B|`ACCEPT_AS_RECOMMENDED`|只进入次级证据层。|
|2485–2488 policy-blocked mechanics|C|`ACCEPT_AS_RECOMMENDED`|必须与 unauthorized/cash-preservation 同屏。|
|2489–2491 evidence scaffolding|B|`ACCEPT_AS_RECOMMENDED`|保留 manual/synthetic caveat 与 2489 mismatch。|
|2492 bounded pilot NO-GO|A|`ACCEPT_AS_RECOMMENDED`|NO-GO 与 cap violation 必须先于 order/fill。|
|2493 signed aggregate NO-GO|A|`ACCEPT_AS_RECOMMENDED`|aggregate NO-GO 支配 subordinate conditional axes。|

这些 decision 已由上述 exact Owner token 接受。S6 可在 2501 latest-main integration 后另立 serial
projection contract/renderer consumer 任务；2501 本身仍不修改当前页面。
