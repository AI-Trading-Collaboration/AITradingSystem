# TRADING-2472：Atlas Node Status Provenance V1

最后更新：2026-07-31

稳定任务 ID：
`TRADING-2472_ATLAS_NODE_STATUS_PROVENANCE_V1`

优先级：`P1`

状态：`IN_PROGRESS`

Owner 决定：

```text
owner_decision:TRADING-2472:2026-07-31:advance_atlas_node_status_provenance_v1
```

production effect：`none`

broker action：`none`

## 1. 决策与目标

TRADING-2471 已让低金融知识读者一眼看见八阶段系统流程、当前页面位置和节点进展颜色，
但进展状态仍是 renderer 内的 display-only 固定映射。读者还不能直接回答：

1. 这个状态依据什么得出；
2. 它来自哪个 canonical snapshot entity、response 或 independent validation；
3. 为什么 `VALIDATED` 不代表策略 PASS、投资评级或 promotion readiness。

本任务把状态显示升级为 deterministic provenance V1。页面继续只读既有 Atlas
snapshot/diff/query evidence，不重算研究、评分、回测或归因，不读取 market/cache/external
data，不调用 LLM，不启动 HTTP 服务。

## 2. 冻结状态推导合同

状态推导只允许使用 structured fields 与页面执行边界，不得解析中文 claim、标题或摘要来猜测
状态。每个节点必须生成 `status_code`、中文标签、颜色语义、`source_kind`、exact reference
与中文理由。

|阶段|推导规则|source kind|预期状态|
|---|---|---|---|
|`DATA_INPUTS`|页面不读取或执行 market/macro/fundamental/manual inputs|`PAGE_EXECUTION_BOUNDARY`|`NOT_EXECUTED_BY_PAGE`|
|`DATA_QUALITY_GATE`|页面不运行 `aits validate-data` 或等价 DQ gate|`PAGE_EXECUTION_BOUNDARY`|`NOT_EXECUTED_BY_PAGE`|
|`RESEARCH_MAINLINE`|exact `RESEARCH_MAINLINE_SUMMARY` target 对应 snapshot node 的 `raw_status=RUNNING`|`CANONICAL_SNAPSHOT_FIELD`|`IN_PROGRESS`|
|`BACKTEST_AND_EVALUATION`|exact `RESULT_AND_STATUS` target 对应 result 的 `display_status=LIMITED`|`CANONICAL_SNAPSHOT_FIELD`|`LIMITED`|
|`RESULT_ATTRIBUTION`|exact attribution 的 `result_id` 必须指向上述 result；沿用该 result 的 `display_status=LIMITED`，并显示 attribution exact ID|`CANONICAL_SNAPSHOT_RELATION`|`LIMITED`|
|`ATLAS_SNAPSHOT_DIFF`|exact `SNAPSHOT_CHANGE_EXPLANATION` response 对应 independent validation `PASS`|`INDEPENDENT_VALIDATION`|`VALIDATED`|
|`CITATION_FIRST_QUERY`|五个 exact response 各自恰有一个 independent validation 且全部 `PASS`|`INDEPENDENT_VALIDATION_SET`|`VALIDATED`|
|`OWNER_DECISION_BOUNDARY`|页面无自动 promotion；必须等待人工复核|`OWNER_REVIEW_POLICY`|`PENDING_OWNER_REVIEW`|

### 2.1 Fail-closed 条件

以下任一情况必须停止生成 HTML，并输出稳定错误码，不能退回固定展示值：

- question 集合缺失、重复或出现未知项；
- validation 与 response 不能按 exact `response_id` 一一对应；
- snapshot entity 缺失、重复、kind 不匹配或状态字段不在冻结枚举中；
- attribution 的 `result_id` 不指向 `RESULT_AND_STATUS` exact target；
- `SNAPSHOT_CHANGE_EXPLANATION` validation 非 `PASS`；
- 五个 response validation 不是全部 `PASS`；
- provenance exact reference 为空或不能绑定到当前 showcase。

这些门禁只验证当前静态 evidence view 的结构和引用闭包，不验证策略收益、数据质量、OOS
充分性、投资适用性或生产 readiness。

## 3. 展示合同

- 每个流程节点仍显示状态胶囊，角色样式与进展颜色保持独立；
- 流程图下增加“状态依据台账”，按八阶段顺序展示：
  - 当前状态；
  - 通俗理由；
  - source kind；
  - exact response / validation / entity reference；
- 长 ID/hash 可安全换行，1280px 与窄屏无页面级横向溢出；
- 页面同时说明：
  - `VALIDATED` 只表示 response/diff 的 independent validation PASS；
  - `LIMITED` 保留既有证据限制；
  - `NOT_EXECUTED_BY_PAGE` 不是 DQ FAIL；
  - `PENDING_OWNER_REVIEW` 不代表自动 promotion；
- 保持 static HTML：无 script、form、iframe、external resource 或 write action。

## 4. 实现范围

Task-owned：

- `src/ai_trading_system/atlas/cited_query_renderer.py`；
- `tests/atlas/test_cited_query_renderer.py`。

Coordinator-owned：

- `docs/task_register.md`；
- 本 requirement；
- `docs/system_flow.md`；
- `docs/artifact_catalog.md`；
- ARCH-004 compatibility/deprecation authority；
- ARCH-004E / ARCH-005 generated views。

不修改 query/request/response/citation public schema，不改变 snapshot/diff contract。若实现证明
必须改变公共合同，应停止并另行执行最小 serial contract wave。

## 5. 验收标准

1. 八节点状态按第 2 节 structured rules 确定，不再由 renderer 固定 tuple 直接赋值；
2. 八条 provenance 台账顺序、状态、理由、source kind 与 exact ref 可审计；
3. exact response↔validation、node/result/attribution relation 缺失、重复或不一致 typed
   fail closed；
4. 页面明确 `VALIDATED`、`LIMITED`、`NOT_EXECUTED_BY_PAGE` 和 Owner review 的边界；
5. deterministic double-build byte-identical；
6. no script/form/iframe/external/write 与 escaping contract PASS；
7. renderer focused、Atlas focused、task shadow、DevEx、compatibility/deprecation 和适用
   formal tiers PASS；
8. reviewed local-capable browser 完成 1280px 与窄屏视觉复核；Owner 对 canonical preview
   验收后才进入正式 closeout；
9. `production_effect=none`、`broker_action=none`。

## 6. 工作区与安全

- governed mode：`SINGLE_LANE`；
- registration base：`77ef1e17817e7638d10f665963ed0a319c9ce448`；
- frozen base：登记提交后的 exact local `main`；
- task branch：`codex/trading-2472-atlas-node-status-provenance`；
- known-unrelated exclusion：
  `docs/research/growth_tilt_owner_diagnosis_pack.md`，不得读取、hash、复制、stage、修改或删除；
- retained preview 继续位于
  `outputs/atlas/strategy_research_cited_query/trading_2470_v1/`；
- 不创建额外 worktree、server、data cache 或 strategy artifact；
- browser visual QA 不授权改变 production/broker 状态。

## 7. 进度记录

- 2026-07-31：Owner 验收 TRADING-2471 visual 后确认继续推进后续任务；按已采纳顺序登记
  TRADING-2472，目标是让节点状态从“颜色与标签”升级为“可解释、可定位、可复核”的
  provenance view。当前无公共 schema、DQ、策略、production 或 broker 变更授权。
- 2026-07-31：八节点 structured-field/status-boundary derivation、八条 provenance ledger 与
  typed fail-closed tests 已实现。renderer focused=`7 passed`、完整 Atlas/citation focused=
  `63 passed`、Ruff/Black PASS；canonical double-build byte-identical，新 `index.html`=
  `44641 bytes / SHA-256 b0b4cea837a06bec55bb2ff55f7a6d3cb4b98ea219360b7242874238631ebba5`，
  `responses.json`/`validation.json` SHA 保持 `d3317e3f…`/`dd17b181…`。static DOM audit
  确认 `8 stages / 8 provenance rows / 1 current / no external` 与 900/620px breakpoints。
  Browser automation 刷新本地 `file://` 时仍被 URL policy 拒绝并禁止 workaround，明确记录
  `NOT_EXECUTED_URL_POLICY`；等待 Owner 手动刷新 canonical preview 完成 visual acceptance。
