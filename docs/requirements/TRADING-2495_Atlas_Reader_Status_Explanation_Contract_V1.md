# TRADING-2495：Atlas Reader Status Explanation Contract V1

最后更新：2026-08-03

稳定任务 ID：
`TRADING-2495_ATLAS_READER_STATUS_EXPLANATION_CONTRACT_V1`

优先级：`P1`

状态：`BASELINE_DONE`

Owner 决定：

```text
owner_decision:TRADING-2495:2026-08-03:approve_contract_first_reader_explanation_v1
```

production effect：`none`

broker action：`none`

## 1. 决策背景

TRADING-2473 已把八阶段 Atlas 流程图、原生 `details/summary`、状态颜色、来源类型、
exact refs 与可确认/不可推出边界加入 cited-query 静态页面。TRADING-2494 又把五份经审阅的
historical records 投影进 canonical snapshot/page，同时继续排除 `TRADING-2481..2493`。

Owner 视觉复核指出：当前“状态为什么是这样”主要复述 `raw_status=RUNNING` 或
`display_status=LIMITED`，只能证明页面和底层字段一致，不能回答普通读者真正关心的四个问题：

1. 现实中现在发生了什么；
2. 为什么还不能把它当成已验证结果；
3. 具体缺少哪类证据或决定；
4. 什么可观察事件会让状态发生变化。

Web Pro 基于 exact commit `13292726540dc78039a85f17a39f64ddbee956d1` 的规划审阅建议先做
最小 serial contract wave，再做 renderer-only consumer wave。Owner 已接受该顺序。本任务只冻结
解释 authority、缺失值语义、projection 与 validator，不把页面可读性任务伪装成新的研究结论。

## 2. Owner decisions 与权威边界

本任务采用独立 sidecar，不修改现有 snapshot/diff/query/request/response/citation public schema。
sidecar schema 固定为 `strategy_research_status_explanation.v1`，它是新的 consumer-visible contract，
因此由本任务串行冻结。

Owner 接受以下决策：

- `plain_summary` 只允许 deterministic derived value，不是独立事实来源；
- `current_work`、`completed_milestones`、`unmet_conditions`、`evidence_gaps`、
  `responsible_role` 与 `transition_conditions` 必须来自 typed authority，不能从状态值、文件名、
  commit author 或自由摘要关键词推断；
- `transition_conditions` 只允许引用 canonical acceptance、governed task acceptance 或明确
  Owner decision；条件满足也不自动修改 canonical status；
- `responsible_role` 允许 `NOT_RECORDED`，不得从 Git author 推断个人负责人；
- result limitation 只有在存在 typed status-cause binding 时才可称为 LIMITED 的正式原因；
  否则只能展示为已知限制，并明确“完整原因尚未记录”；
- `next_reader_action` 只包含展开、阅读、比较与查看出处，不执行研究、回测、DQ、production 或
  broker action。

允许输入 authority：

- 同一 fingerprint 的 current canonical snapshot；
- current node/result/attribution/source refs；
- 已审阅的 governed task authority 和明确 Owner decision；
- TRADING-2494 允许的五份 historical records；
- 稳定 reader semantics，例如“validator PASS 不等于策略 PASS”。

禁止输入 authority：

- moving main、未审阅 roadmap、commit author 或文件名推断；
- 从 `RUNNING`、`LIMITED`、`BLOCKED`、`PASS` 或 prose 关键词反推具体原因；
- `TRADING-2481..2493`；
- 2022-12-01 作为 active research default；
- 新研究、回测、DQ、QuantConnect/cloud、paper/live、production 或 broker 结果。

## 3. Contract 设计

顶层 `StrategyResearchStatusExplanationBundle` 至少包含：

```text
schema_id
schema_version
snapshot_id
snapshot_fingerprint
primary_research_start
excluded_task_ids
explanation_records
validation_summary
content_sha256
```

每条 `StatusExplanationRecord` 至少包含：

```text
explanation_id
stage_id
target_kind
target_id
status_code
status_object_scope
plain_summary
current_work
completed_milestones
unmet_conditions
evidence_gaps
reader_impact
transition_conditions
responsible_role
next_reader_action
technical_refs
checked_authority_scope
authority_bindings
template_version
```

每个解释字段使用 typed value state：

```text
PRESENT
NOT_RECORDED
NOT_APPLICABLE
NOT_YET_DUE
SOURCE_UNAVAILABLE
OWNER_DECISION_PENDING
```

`AUTHORITY_CONFLICT` 与 `LINEAGE_INVALID` 不作为可渲染普通值；命中即 bundle `INVALID` 并
fail closed。

每个 `PRESENT` factual item 必须提供 stable fact ID、authority kind/ID 与至少一个 exact source
ref。`NOT_RECORDED` 必须同时记录 `checked_authority_scope` 和已检查 authority IDs，避免把 builder
漏读伪装成“仓库未记录”。

`TransitionCondition` 必须包含 stable ID、当前状态、可观察事件、deciding authority、候选
target status 与 source refs。target status 只表示满足条件后可能进入的候选状态，必须由 authoritative
producer 另行更新 canonical state。

## 4. 投影与 fail-closed 规则

projection 只做 typed mapping，不解析 prose 产生事实枚举。必须机械验证：

1. sidecar fingerprint 与 canonical snapshot 完全一致；
2. target node/result/attribution/source ref 全部存在且引用闭合；
3. explanation `status_code` 与 canonical target status 完全一致；
4. `PRESENT` fact 至少有一个有效 source ref；
5. `plain_summary` 的每个实体特定命题都能回指 fact ID；
6. `LIMITED` 缺少 typed gap 时只生成“具体原因尚未记录”；
7. `BLOCKED` 缺少 typed blocker 时只生成“阻断原因尚未记录”；
8. `PASS` 必须绑定被检查对象、检查名称、适用范围和非投资边界；
9. `TRADING-2481..2493` 进入 explanation lineage 即 `INVALID`；
10. primary research start 固定为 `2021-02-22`；
11. 同一输入 double-build 必须 byte-identical；
12. explanation bundle 不执行或触发任何状态转换。

## 5. 实施步骤

### S0：登记与 serial contract preflight

- 登记 task row 与本 requirement；
- 从 exact local main `13292726540dc78039a85f17a39f64ddbee956d1` 启动
  `SINGLE_LANE`，并声明 `contract_change=true`；
- 建立 task-owned 和 coordinator-owned claims；
- 若 task ID、checkout、lease、runner 或 consumer-visible overlap 冲突则停止。

### S1：Sidecar contract 与 authority policy

- 新增 strict typed contract、canonical serialization/seal/replay 与 content hash；
- 新增 exact authority policy，冻结 source kinds、缺失值、窗口、排除任务和安全边界；
- synthetic fixtures 覆盖 RUNNING/LIMITED/BLOCKED/PASS/NOT_DUE；
- 不修改现有 snapshot/diff/query schema。

### S2：Projection、validator 与 anti-fabrication tests

- 从 canonical snapshot 与 reviewed authority 构建 sidecar；
- 对当前八阶段生成 typed PRESENT/NOT_RECORDED 事实；
- fixture 只有状态而无 causal authority 时，禁止生成“OOS 不足、DQ 阻断、Owner 未决、阈值未满足”；
- 自由摘要包含关键词但无 typed binding 时，不生成 gap enum；
- source ref、fingerprint、status、excluded-task 或 authority drift 均 fail closed。

### S3：Governance 与 handoff

- 更新 `docs/system_flow.md`、architecture fragments/generated views、task shadow 与 append-only
  compatibility/deprecation authority；
- focused、Architecture、Contract、Integration、Reproducibility、exclusive Full 串行 PASS；
- validated branch 通过 governed integration、ff-only local main、ordinary push 与 cleanup；
- 只在 Owner 接受 explanation authority 后登记/启动 TRADING-2496 renderer consumer wave。

## 6. 路径与所有权

task-owned：

```text
docs/requirements/TRADING-2495_Atlas_Reader_Status_Explanation_Contract_V1.md
config/atlas/status_explanation_authority.yaml
src/ai_trading_system/contracts/strategy_research_status_explanation.py
src/ai_trading_system/atlas/status_explanation_projection.py
tests/test_strategy_research_status_explanation_contract.py
tests/atlas/test_status_explanation_projection.py
```

coordinator-owned：

```text
docs/task_register.md
docs/system_flow.md
src/ai_trading_system/contracts/__init__.py
src/ai_trading_system/atlas/__init__.py
config/architecture/fragments/modules/**
config/architecture/fragments/flows/**
inputs/architecture/**
registry/development_tasks_shadow/**
registry/development_tasks_shadow_v2/**
tests/test_arch_004_refactor_policy.py
tests/test_arch_004g_deprecation.py
```

不创建外部 cache/clone，不读取 known-unrelated exclusion。当前 ignored Atlas 页面由后继
TRADING-2496 deterministic renderer wave 重建，本合同波不提前修改页面 artifact。

## 7. 验收标准

1. sidecar contract/version/policy/schema identity 闭合；
2. 现有 snapshot/diff/query public schema exact bytes 不变；
3. 每个 PRESENT fact 有 authority 与 source refs；NOT_RECORDED 有 checked scope；
4. status、target、fingerprint、source refs 与 authority lineage 全部机械校验；
5. explanation 不升级 canonical status，不把 validator PASS 解释成策略 PASS；
6. anti-fabrication fixtures 能阻止由状态或 prose 生成具体原因；
7. `TRADING-2481..2493` 保持排除，historical PASS 不成为 current/investment PASS；
8. primary default 保持 `2021-02-22`，2022-12-01 只允许 historical/fallback role；
9. double-build byte-identical，tamper/conflict/orphan/fingerprint/status negatives fail closed；
10. focused/generated/compatibility/formal gates PASS；
11. Owner explanation-authority review token 仍为独立退出条件，不由自动测试伪造；
12. `investment_conclusion_generated=false`、`production_effect=none`、`broker_action=none`。

## 8. Stop conditions

- `NO_AUTHORIZED_SOURCE_FOR_CURRENT_WORK`；
- `NO_AUTHORIZED_TRANSITION_AUTHORITY`；
- `UNRESOLVED_AUTHORITY_CONFLICT`；
- `TASK_REGISTER_ID_NOT_RESERVED`；
- `EXCLUDED_TASK_DEPENDENCY_REQUIRED`；
- 必须解析自由摘要才能声称具体 blocker/gap/transition；
- snapshot fingerprint、status、source ref 或 authority drift；
- 需要改变研究窗口、DQ/PIT、阈值、投资结论或 canonical status；
- external platform、network、production 或 broker action；
- formal runner 与其他 heavyweight runner 并发。

任一 stop condition 命中即返回 `INSUFFICIENT_AUTHORITY` 或 `INVALID_CONTRACT_LINEAGE`，不能用
renderer 硬编码文案、手工 artifact 或弱化 validator 绕过。

## 9. 生命周期与进度

- governed mode：`SINGLE_LANE` serial contract wave；
- frozen base：`13292726540dc78039a85f17a39f64ddbee956d1`；
- planned branch：`codex/trading-2495-atlas-reader-status-explanation-contract`；
- workspace：`D:/Work/AITradingSystem`，复用当前 clean detached exact-main checkout；
- exit condition：合同、projection、验证、ff-only main、ordinary push、branch cleanup 与 2496 handoff；
- recoverability：tracked implementation 由 Git/main/SHA 恢复；无独立 ignored artifact。

- 2026-08-03：Owner 接受 Web Pro 的 contract-first 顺序；任务建立并进入 `IN_PROGRESS`。当前页面
  可读性目标仍为 `OWNER_REVIEW_REQUIRED`，不预写 PASS。
- 2026-08-03：sidecar contract、8-stage authority policy、typed projection/validator、public exports、
  architecture fragments 与 system flow 已实现。policy SHA-256 为
  `53264bb92fe5f0125990d9f936bced00a0e0a1a7c338d90acc28adbf86b11f10`，fixture bundle
  SHA-256 为 `bc424b8602ce219a82a84d43f580ae108517f6a729dced30bce97baaaa1f6f34`；bundle
  `validation_summary=INSUFFICIENT_AUTHORITY`，没有用默认句子填补未登记的现实工作、责任方或转变条件。
- 2026-08-03：focused contract/projection=`16 passed`，相邻 Atlas/contract=`40 passed`；首轮
  compatibility/deprecation 的 `103 passed / 83 failed` 为旧 2494 current-authority 路由导致的单根因级联，
  2495 successor/current-authority 修复后同一 `-n 16 --dist loadfile` 覆盖=`186 passed`。ARCH-004
  历史前缀保持 exact，ARCH-004G inventory=`arch_004g_deprecation_inventory_a165b03f07d720364a04`
 （1076 modules / 1243 tests / 856 direct writers）。当前进入 final-tree 五级正式门禁；门禁后 tracked
  bytes 保持冻结，随后执行 ff-only main、ordinary push、cleanup，并向 TRADING-2496 交接。
- 2026-08-03：latest-main 候选首次 Full 为 `8203 passed / 3 skipped / 2 failed`，父证据为
  `outputs/validation_runtime/full_20260802T182656Z/test_runtime_summary.json`。两个失败均来自
  `tests/test_trading2452_architecture_contract.py` 将 historical current-authority 的最高合法 section
  仍固定为 2494；2495 append-only section 成为最新 authority 后触发同一顺序断言。修复仅把该 consumer
  boundary 提升到 2495，并把测试文件纳入 2495 current-source authority；不改写历史 payload/hash。
  修复后必须从最终字节重跑全部五级，Full 使用 `failure_fix_rerun` 并绑定上述父证据。
