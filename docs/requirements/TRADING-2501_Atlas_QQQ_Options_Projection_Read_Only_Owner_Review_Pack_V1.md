# TRADING-2501 Atlas QQQ Options Projection Read-Only Owner Review Pack V1

最后更新：2026-08-08

稳定任务 ID：`TRADING-2501_ATLAS_QQQ_OPTIONS_PROJECTION_READ_ONLY_OWNER_REVIEW_PACK_V1`

优先级：`P1`

状态：`IN_PROGRESS`

governed mode：`SINGLE_LANE`

exact registration base：`c233c82e2f6c4dfcd4b1302c99b98789bc6f3def`

Owner decision：`owner_decision:TRADING-2501:2026-08-08:build_read_only_owner_review_pack_before_canonical_projection_v1`

production effect：`none`

broker action：`none`

## 1. 目标与当前决策

TRADING-2494、TRADING-2495 与 TRADING-2496 的 canonical Atlas authority 都明确排除
`TRADING-2481..2493`。用户接受 Web Pro 的建议：不直接把这 13 项投影进当前结果页，先形成一份
面向 Owner 的只读审阅包，逐项决定哪些事实可以进入下一轮 canonical projection contract。

本任务不改变现有页面、renderer、sidecar、canonical projection、研究状态、DQ/PIT、策略结论、
研究窗口、QQQ Options contracts 或外部平台状态。当前 blocker 固定为
`OWNER_PROJECTION_REVIEW_NOT_ACCEPTED`；在 Owner 签署逐项 decision 前，2481–2493 继续保持排除。

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

|任务|Web Pro 初步分类|面向读者的一句话定位|
|---|---|---|
|2481|A|共享记录与安全边界已冻结；这只是数据合同，不是策略结论。|
|2482|A|DQ/PIT 与证据身份可被严格重放；它证明可审计性，不证明收益。|
|2483|A|信号包与 run manifest 可确定性导出；输入质量仍不等于选券或执行有效。|
|2484|B|平台 adapter contract 已建立；真实平台能力和权限仍需独立证据。|
|2485|C|确定性选券机制存在，但关键阈值未获 Owner review，默认不授权选券。|
|2486|C|执行现实模型结构已建立，但 latency、slippage、fee 等政策未获批准。|
|2487|C|现金、权利金与结算记账机制已建立；真实成交与策略盈亏仍未证明。|
|2488|C|到期与公司行动安全路径已建立；exercise/assignment 等仍按 fail-closed 处理。|
|2489|B|人工平台证据包可被规范收集；manual evidence 不等于完整历史验证。|
|2490|B|本地 ingest/validator/reconciliation 边界已建立；外部 raw data authority 仍受限。|
|2491|B|跨层 synthetic harness 可检查合同连贯性；synthetic PASS 不等于市场有效。|
|2492|A|bounded pilot 的 aggregate verdict 为 NO-GO；必须先解释 cap violation，再提 1 order/1 fill。|
|2493|A|aggregate `NO_GO_KEEP_BLOCKED` 支配局部 `CONDITIONAL_GO`，不能被局部通过覆盖。|

分类语义将在实现阶段从 exact authority 固化；本表是 Owner Review Pack 的初步审阅建议，不是
canonical projection decision。`D` 当前为空；若 exact source drift 或 authority 不足，可把对应项降为 D。

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
- 当前：registration boundary，尚未启动 canonical projection 或外部动作。
