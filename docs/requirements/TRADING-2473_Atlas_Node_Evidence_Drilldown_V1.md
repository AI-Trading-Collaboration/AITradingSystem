# TRADING-2473：Atlas Node Evidence Drilldown V1

最后更新：2026-08-01

稳定任务 ID：
`TRADING-2473_ATLAS_NODE_EVIDENCE_DRILLDOWN_V1`

优先级：`P1`

状态：`BLOCKED_OWNER_INPUT`

Owner 决定：

```text
owner_decision:TRADING-2473:2026-08-01:advance_atlas_node_evidence_drilldown_v1
```

production effect：`none`

broker action：`none`

## 1. 决策与目标

TRADING-2471 已让低金融知识读者看见八阶段策略系统流程和当前页面位置；TRADING-2472
进一步让每个节点的状态具备 structured derivation 与 exact provenance。Owner 于 2026-08-01
确认按推荐方向继续推进：在同一 canonical 静态页面中增加节点级 evidence drilldown，读者无需
理解内部 schema，也能从流程节点直接展开并回答：

1. 这个节点在整个系统中负责什么；
2. 当前状态是什么、为什么是这个状态；
3. 状态依据来自哪类 authority 和哪个 exact reference；
4. 当前证据能说明什么、不能说明什么；
5. 下一步合法动作是什么，哪些动作仍未被页面执行或授权。

V1 采用浏览器原生 `<details>/<summary>`，不引入 JavaScript、HTTP server、authentication、
session、LLM、自由文本查询、external retrieval 或 write action。页面继续只读既有 Atlas
snapshot/diff/query evidence，不重算研究、评分、回测、归因或 DQ。

## 2. 冻结展示合同

八个既有 stage 必须各自形成一个可键盘操作的 native disclosure：

|字段|要求|
|---|---|
|stage identity|沿用 TRADING-2471 的八个 stable stage IDs 与固定顺序|
|summary|显示序号、中文节点名、角色、进展状态胶囊；现有颜色语义不变|
|purpose|面向低金融知识读者解释该节点在系统中的作用|
|status reason|直接复用 TRADING-2472 structured provenance 的通俗理由，不另建固定状态真相|
|source|显示 source kind 与 exact ref；不得省略、模糊化或伪造时间/lineage|
|can conclude|只描述当前 evidence view 支持的有限结论|
|cannot conclude|明确 strategy PASS、投资评级、promotion、production 与 broker 边界|
|next legal action|只描述 read/review/另立任务等合法动作，不执行上游研究或生产行为|

`CITATION_FIRST_QUERY` 是唯一 current stage，默认 `open`，并继续保留唯一
`aria-current=step`。其他节点默认收起，但其 summary 必须在不展开时仍能看见节点、角色与状态。

## 3. 数据与推导边界

- drilldown 的 status、reason、source kind 与 exact ref 必须消费同一份
  `StageStatusProvenance`，不得复制第二套状态映射；
- purpose、can/cannot conclude 与 next action 是冻结的 reader-facing stage semantics，不能从
  claim 文本、颜色、CSS class 或模糊关键词反推；
- stage/provenance 缺失、重复、顺序漂移、current stage 非唯一、exact ref 为空或 relation
  不闭合时 typed fail closed；
- HTML escaping、deterministic serialization、no-script/no-form/no-iframe/no-external/no-write
  合同继续适用；
- V1 不改变 snapshot/diff/query/request/response/citation public schema，不改变 DQ/PIT、研究窗口、
  heuristic、promotion 或投资解释政策。若实现证明必须改变公共合同，应停止并另立最小 serial
  contract wave。

## 4. 视觉与可访问性

- disclosure summary 保持完整点击目标，并支持键盘原生操作；
- open/closed 状态具有清晰的视觉 affordance，但不以颜色作为唯一信息载体；
- 当前节点默认展开，并用“你在这里”与 `aria-current=step` 双重表达；
- 展开内容使用短句和固定小标题，避免原始 schema dump；
- 1280px、900px 与 620px 布局无页面级横向溢出，长 exact ref 可安全换行；
- reduced-motion 下不依赖动画理解状态；打印/静态 capture 不隐藏关键边界说明。

## 5. 实施范围与 claims

Task-owned：

```text
src/ai_trading_system/atlas/cited_query_renderer.py
tests/atlas/test_cited_query_renderer.py
```

Coordinator-owned：

```text
docs/task_register.md
docs/requirements/TRADING-2473_Atlas_Node_Evidence_Drilldown_V1.md
docs/system_flow.md
docs/artifact_catalog.md
inputs/architecture/**
registry/development_tasks_shadow/**
registry/development_tasks_shadow_v2/**
tests/test_arch_004_refactor_policy.py
tests/test_arch_004g_deprecation.py
```

retained preview 继续复用：

```text
D:\Work\AITradingSystem\outputs\atlas\strategy_research_cited_query\trading_2470_v1
```

它是 canonical governed evidence；新构建成功并验证前不得覆盖其有效身份。V1 不创建 server、
data cache、strategy artifact 或额外长期 worktree。

## 6. 验收标准

1. 恰有八个 native disclosure，顺序与 stable stage IDs 一致；
2. `CITATION_FIRST_QUERY` 唯一 current 且默认展开，其他节点默认收起；
3. 每个 disclosure 展示 purpose、status reason、source kind、exact ref、can conclude、cannot
   conclude 与 next legal action；
4. status/reason/source/ref 直接来自 TRADING-2472 structured provenance，缺失、重复、不一致或
   空 ref typed fail closed；
5. 角色样式与状态颜色继续独立，状态不被解释为 strategy PASS 或投资评级；
6. no script/form/iframe/external/write、escaping、keyboard/native semantics 与 responsive contract
   PASS；
7. canonical actual-input double-build byte-identical，responses/validation identity 不因纯展示变化
   被静默改写；
8. renderer focused、Atlas/citation focused、task shadow、DevEx、compatibility/deprecation 与适用
   formal tiers PASS；
9. Owner 在 canonical preview 完成手工视觉复核后才进入正式 closeout；
10. `production_effect=none`、`broker_action=none`。

## 7. 工作区与安全

- governed mode：`SINGLE_LANE`；
- registration base：`818e49fe7bfd7a1064b737b5612f37a0ab712e31`；
- registration commit / lane frozen base：`eea2d61d5123220c98adf3448600357de6065f2a`；
- task branch：`codex/trading-2473-atlas-node-evidence-drilldown`；
- known-unrelated exclusion：
  `docs/research/growth_tilt_owner_diagnosis_pack.md`，不得读取、hash、复制、stage、修改或删除；
- 现有 `D:\Work\AITradingSystem_ops_runtime_20260725` 与
  `D:\Work\AITradingSystem_t2463_target_redesign` 不属于本任务，不得使用或清理；
- browser visual QA 不授权改变 production/broker 状态。

## 8. 进度记录

- 2026-08-01：TRADING-2472 完成 local-main/remote closeout 后，Owner 确认按推荐方向继续。
  登记 TRADING-2473，选择低风险的 native static drilldown；HTTP adapter 与自由文本/LLM consumer
  继续留待独立 Owner 决策。
- 2026-08-01：实现八个 native `<details>/<summary>` disclosure，`CITATION_FIRST_QUERY` 唯一
  current/open；每个节点展示职责、structured reason、source kind、exact refs、可/不可推出结论和
  下一合法动作。renderer focused=`7 passed`、Atlas/citation focused=`63 passed`、Ruff/Black PASS；
  static DOM audit=`8 details / 8 stages / 1 current / 1 current-open / 0 script/form/iframe/external`。
- 2026-08-01：使用 canonical actual inputs 双构建，三文件 byte-identical；`index.html` 更新为
  `59847 bytes`、SHA-256
  `0e3b1b2855975c0c54e1baee3965de1198a677b4d11793ed22694c2bbaef8bb3`，
  `responses.json`=`d3317e3f7a852a59d323181ed647f07eaf51bc63ec0d9f389cba054f85b32f07`、
  `validation.json`=`dd17b1819e48539d7f7d166199c31f80c401453d7c91b2f3176788d2a44f86b4`
  均未变化。两个固定临时构建目录已审计为空并删除，canonical evidence 保留，临时副本不可恢复。
- 2026-08-01：Browser URL policy 拒绝对本地 `file://` tab 执行 reload，且明确禁止 workaround；
  automation 记为 `NOT_EXECUTED_URL_POLICY`，未伪报 visual PASS。实现已完成，任务转为
  `BLOCKED_OWNER_INPUT`；解除条件是 Owner 手工刷新 canonical preview，确认节点展开/收起、
  current 默认展开和整体/窄屏可读性，然后再运行正式 closeout validation。
