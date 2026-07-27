# TRADING-2463：Decision Target Redesign 预注册

最后更新：2026-07-27

状态：`IN_PROGRESS_S1_S2_COMPLETE_S3_NOT_STARTED`

稳定任务 ID：`TRADING-2463_DECISION_TARGET_REDESIGN_PREREGISTRATION`

Owner 决策：

- `owner_decision:TRADING-2462:2026-07-27:close_current_tail_risk_capability_path_v1`
- `owner_decision:TRADING-2463:2026-07-27:authorize_decision_target_redesign_preregistration_v1`
- `owner_decision:TRADING-2463:2026-07-27:proceed_s1_s2_design_and_falsification_pack_v1`
- `owner_decision:TRADING-2459:2026-07-27:defer_qld_automatic_selection_and_production_governance_until_canonical_dq_strict_pass_v1`

## 1. 决策与目标

Owner 已关闭当前 `QQQ_FUTURE_WORST_1D_RETURN` tail-risk capability path。TRADING-2462 的
`INSUFFICIENT_ROBUSTNESS_EVIDENCE`、`CLOSE_TAIL_RISK_PATH_OR_REDESIGN_DECISION_TARGET`
和全部历史 artifact 保持不可变；不得把 exact reconstruction、fold influence、placebo 或
4/5 mandatory variant 通过改写为 robust capability。

Owner 同时授权另立本任务，只建立新的 decision-target redesign 预注册。目标是在任何新模型、
能力评估或策略实现前，先回答：

1. 后续系统究竟要支持哪一类可执行投资决策，而不是先寻找能通过历史指标的 label；
2. target 的经济含义、action mapping、label interval、available-at、horizon 与样本成熟度是否
   可由 PIT-safe 数据证明；
3. target 是否有足够的跨 fold、regime 与 event coverage，能够在结果读取前冻结能力和证伪门槛；
4. target 与 QQQ/SGOV/TQQQ primary action space、SPY reference role、QLD role-limited
   implementation role之间如何隔离。

本任务不自动选择最终 target，不运行 Decision Value Audit，不创建 risk overlay、candidate
family、backtest、weights、paper-shadow、promotion、production 或 broker action。

## 2. 权威输入与不可继承项

允许引用的权威证据仅限：

- TRADING-2460 label foundation 的 schema、receipt、DQ 与 availability contract；
- TRADING-2461 的 purged walk-forward/model-ladder contract及能力矩阵；
- TRADING-2462 的 robustness/falsification结论、样本不足位置与安全边界；
- TRADING-2458 已退役 candidate family 的 immutable closure evidence；
- TRADING-2459 的 SPY reference 与 QLD role-limited implementation contract。

以下内容不得继承为新 target 的正面依据：

- 已关闭的 `QQQ_FUTURE_WORST_1D_RETURN` capability claim；
- 已退役 TRADING-2452/2458 candidate family 的 active/selectable identity；
- 结果可见后的 horizon、feature、sample floor、threshold 或 model choice；
- QLD 历史收益排名、QLD signal/style/candidate role；
- canonical full-cache DQ 未取得 strict PASS 时的 QLD scoped exception。

Primary research window 默认从 `2021-02-22` 开始。任何不同窗口只能作为明确标注的
sensitivity/stress role，不能静默替代项目默认。

## 3. 预注册产物

本任务后续应形成一份 owner-readable redesign pack，至少包含：

- decision problem 与允许动作的明确映射；
- 每个 target design option 的经济语义、方向、单位、label interval、available-at 与
  maturity contract；
- PIT/DQ/source feasibility、provider与lineage要求；
- 预期 fold/regime/event coverage 与样本不足的 fail-closed 处置；
- leakage、overlap、multiple-testing、selection contamination 与 target decomposition 风险；
- 允许的 capability classification、falsification axes 与停止条件；
- 与 SPY reference、QQQ/SGOV/TQQQ action space 和 QLD implementation-only role 的隔离；
- Owner 对“选择一个 target 进入独立 capability audit”或“关闭本轮 redesign”的显式选择位。

所有可能影响投资解释的 threshold、sample floor、horizon、classification gate 或 review boundary
必须在结果读取前进入 reviewed policy，记录 owner、版本、rationale、planned evidence 和 review
condition。本任务不得以未解释 numeric literal 冻结这些政策。

S1/S2 当前权威设计包：
`docs/requirements/TRADING-2463_S1_S2_Decision_Target_Design_And_Falsification_Pack.md`。
该文件只完成 option、PIT/DQ/coverage/falsification 设计，不选择 target，不冻结 numeric
policy，不启动 S3/S4 或任何 capability computation。

## 4. 阶段与依赖

|阶段|内容|退出条件|
|---|---|---|
|S0|Owner 决策、任务登记、checkout/lease与证据边界|`SINGLE_LANE` preflight PASS；旧tail-risk path保持closed|
|S0a|task-shadow重建后的最小append-only compatibility authority serial wave|不改写历史section；只追加本任务current hash authority并使Architecture tier PASS|
|S1|decision problem与target design option contract|每个option的action/label/availability/PIT/DQ语义完整；不读取新结果|
|S2|coverage与可证伪性设计|fold/regime/event feasibility、sample-floor rationale、leakage与停止条件可审计|
|S3|Owner redesign pack与target选择位|Owner选择一个target进入未来独立capability audit，或关闭本轮redesign|
|S4|reviewed preregistration freeze与正式验证|仅对Owner已选择target冻结policy；仍不运行模型、回测或权重|

依赖关系：

- TRADING-2462 已归档且当前 tail-risk capability path 关闭；
- QLD automatic selection与production governance必须等待 canonical DQ strict PASS 后另行复核；
- 未来 capability audit 必须是独立新任务，不由本任务自动启动。

## 5. 验收标准

- Owner closure、redesign authorization与QLD deferral均在权威任务/需求文档中可追溯；
- 当前 tail-risk capability 不再具有进入 Decision Value Audit 或 risk overlay 的资格；
- redesign pack完整回答decision/action/label/availability/PIT/DQ/coverage/falsification边界；
- target选择发生在任何新模型、evaluation、candidate或backtest之前；
- 未获Owner选择的target不形成active policy；
- 不访问prospective，不修改既有策略阈值、candidate universe、selection、weights或execution；
- QLD继续仅为role-limited implementation instrument，automatic selection、paper-shadow、
  production和broker保持关闭；
- task consistency、task shadow、documentation/architecture适用门禁通过；
- `production_effect=none`、`broker_action=none`。

## 6. 工作区生命周期

- mode：`SINGLE_LANE`；
- exact base：`8d1effbd77b34e8e7dc6a95a751562b46746c3d7`；
- branch：`codex/trading-2463-decision-target-redesign-prereg`；
- workspace：`D:\Work\AITradingSystem_t2463_target_redesign`；
- purpose：隔离共享根 checkout 的 DATA-GOV-002C2P 在途变更，登记Owner决策并初始化本任务；
- owned scope：本 requirement、相关任务登记/历史决定、QLD延期说明与必要治理投影；
- known-unrelated exclusion：
  `docs/research/growth_tilt_owner_diagnosis_pack.md`，不得读取、hash、复制、stage、修改或删除；
- exit condition：本次Owner决定和任务初始化通过适用验证、提交、ff-only进入local `main`并完成
  ordinary push；确认无unique tracked/untracked/ignored evidence或活动进程后删除worktree并prune；
- recoverability：tracked变更由最终commit恢复；可重建的测试cache与临时验证输出不承诺恢复。

## 7. 当前进度

- 2026-07-27：Owner关闭当前tail-risk capability path，授权另立decision-target redesign
  预注册任务；Decision Value Audit、risk overlay、candidate/backtest/weights保持关闭。
- 2026-07-27：Owner要求QLD automatic selection与production governance等待canonical DQ
  strict PASS后再审；当前role-limited implementation身份不变。
- 2026-07-27：S0任务登记启动；本次只初始化任务、需求和治理投影，不实施S1～S4研究产物。
- 2026-07-27：`SINGLE_LANE` preflight=`PASS`，exact base/local main/origin main均为
  `8d1effbd77b34e8e7dc6a95a751562b46746c3d7`，active lease初始为0。首次lease acquire以
  非allowlisted actor=`codex`被`CHECKOUT_ACTOR_NOT_ALLOWLISTED`正确阻断，未创建lease；
  按reviewed policy改用`integration-coordinator`后取得
  `lease-ae2b1f4575738d81aa0f`，owned/shared path与本任务范围一致。
- 2026-07-27：task-shadow首次validate因隔离worktree缺少bootstrap handoff绑定的四份ignored
  historical validation summary而`HANDOFF_FILE_MISSING`，未跳过门禁。四份source均从canonical
  根工作区按tracked `arch_005_bootstrap_validation_bundle.v1`的exact path/SHA复制并逐文件复核；
  destination SHA与bundle四项全部一致。随后task registry generate/validate=`PASS`，
  `918 total / 421 active / 497 completed / byte_identical=true`。四份临时只读summary仅服务
  本worktree治理验证，随worktree清理；tracked bundle与final commit是恢复边界。
- 2026-07-27：本任务专属Architecture tier真实执行为`713 passed / 36 failed`；provenance绑定
  `TRADING-2463-S0-FINAL-TREE-20260727`。失败集中于append-only compatibility/current-hash
  authority：task-register重建改变了受治理task-shadow哈希，而candidate尚无新的EOF authority
  section。该失败不通过绕过、重跑或放宽断言处理；S0a将以最小serial contract wave追加
  TRADING-2463 authority并更新对应架构测试，历史section字节保持不可变。
- 2026-07-27：S0a策略测试达到`106 passed`后，第二次本任务Architecture tier为
  `748 passed / 3 failed`。其中两项由运行期间local/origin `main`推进至
  `b99b8b95ada8feb2c784fbc00625d4275084e755`触发冻结lane的`CARRIER_PUSH_DRIFT`，按
  base-drift流程留待latest-main integration candidate验证；第三项为新增架构测试后的
  `arch_004e_test_manifest` deterministic rebuild差异。后续先在冻结lane刷新必要DevEx生成物，
  再提交lane并生成/验证`integration_revalidation_plan.v1`，不在旧base上伪造formal PASS。
- 2026-07-27：冻结lane提交=`3f8dad2fed0e96a45d9761892bdf1290a9fd66d0`。由真实
  frozen-base/lane-head/latest-main delta生成并验证
  `integration-revalidation-3012426f9b2096e54293`；结论=`RECONCILIATION_REQUIRED`。
  Coordinator仅人工融合`docs/task_register.md`与compatibility测试的domain overlap，
  其余task-only owner决定保持不变；共享task-shadow/DevEx投影在latest-main final tree统一
  重建。DATA-GOV-002C2P的实现、证据与append-only authority完整保留。
- 2026-07-27：latest-main integration candidate=`53d1b69b5314f96e6d0f0f3a4c2e4a5d0b6743a8`。
  Focused/Architecture/Contract/Integration/Reproducibility分别为
  `145/753/275/995/23 passed`；唯一required Full为
  `7578 passed / 5 skipped / 643 warnings`，post-Full Architecture/Contract为
  `753/275 passed`。未启动S1～S4、未访问prospective、未生成策略候选/回测/权重；
  `production_effect=none`、`broker_action=none`。
- 2026-07-27：最终验证证据已按22项显式白名单迁移到canonical
  `D:\Work\AITradingSystem`，共52个文件逐文件SHA-256一致。临时worktree
  `D:\Work\AITradingSystem_t2463_target_redesign`暂予保留，因为其中存在已登记的
  `known_unrelated_exclusion`，治理规则禁止本任务读取、复制、修改或删除该文件。
  保留风险仅为本地磁盘占用及旧branch/worktree残留，不改变代码、研究结论、生产或
  券商状态；其余validation/cache为可再生或已迁移证据。下一责任人为项目owner/Codex
  cleanup coordinator；退出条件是owner另行授权释放或迁移该排除项，随后确认无活动
  进程、无未迁移唯一证据并通过checkout audit，再执行`git worktree remove`与
  `git worktree prune`。在此之前不得强制清理。
- 2026-07-27：Owner要求继续推进S1+S2。已形成
  `TRADING-2463_S1_S2_Decision_Target_Design_And_Falsification_Pack.md`，定义一个
  decision problem、三类非激活action semantics与四个target design options，并冻结
  PIT/DQ/source feasibility、coverage dimensions、sample-floor governance、共同及
  option-specific falsification axes、leakage/selection-contamination停止条件。所有numeric
  horizon/threshold/sample floor保持`OWNER_REVIEW_REQUIRED`；`selected_target=NONE`，
  S3/S4未启动，未读取新结果、未访问prospective、未运行模型/Decision Value Audit/
  risk overlay/candidate/backtest/weights。
- 2026-07-27：S1/S2 phase-exit验证通过：focused=`133 passed`、Architecture=`756 passed`、
  Contract=`275 passed`、Reproducibility=`23 passed`、Integration=`995 passed /
  642 warnings`、Full=`7581 passed / 5 skipped / 642 warnings`。Full provenance绑定
  `TRADING-2463-S1-S2-20260727`与`natural_integration_boundary`。五类runtime evidence、
  checkout intent与当前lease events按7项显式路径白名单迁移到canonical
  `D:\Work\AITradingSystem`，共27个文件逐文件SHA-256一致。阶段仍停在S2，
  `production_effect=none`、`broker_action=none`。
