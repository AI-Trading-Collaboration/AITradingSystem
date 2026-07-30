# TRADING-2470：Atlas Citation-First Query API V1

最后更新：2026-07-30

稳定任务 ID：
`TRADING-2470_ATLAS_CITATION_FIRST_QUERY_API_V1`

优先级：`P1`

状态：`IN_PROGRESS`

Owner 决定：

```text
owner_decision:TRADING-2470:2026-07-30:advance_atlas_citation_first_query_api_v1
```

production effect：`none`

broker action：`none`

## 1. 决策与目标

TRADING-2466、TRADING-2468 与 TRADING-2469 已形成 deterministic Atlas snapshot、
canonical source coverage 与 cross-snapshot diff。下一步采用 citation-first 顺序：

1. 先冻结只读 query/request/response/citation contract；
2. 再实现只消费已验证 snapshot/diff JSON 的 deterministic query engine；
3. 最后生成低金融知识读者可读的问答卡片与交互式 read API demo；
4. 自由文本或 LLM consumer 只能把输入映射到已批准问题，并表达既有回答，不成为事实、
   计算、policy、task、production 或 broker authority。

本任务不是投资建议系统、通用 RAG、搜索引擎或在线服务上线任务。V1 不启动长期服务，
不读取 market/cache/external data，不执行 DQ/model/backtest，不写入任何 tracked/runtime
决策状态。

## 2. 权威输入

只允许读取并验证：

- `strategy_research_explorer_snapshot.v1`；
- `strategy_research_explorer_diff.v1`；
- 它们绑定的 tracked Git source refs、entity ids、entity hashes、snapshot/diff ids；
- V1 fixture 与 TRADING-2468/2469 retained canonical preview，仅用于本地验收。

若输入 schema、identity、source hash、entity hash、lineage、safety 或 receipt 校验失败，
query 必须 fail closed，不得降级为未引用文本。

## 3. V1 产品合同

### 3.1 稳定问题目录

V1 冻结以下 `question_id`：

```text
RESEARCH_MAINLINE_SUMMARY
RESULT_AND_STATUS
ATTRIBUTION_AND_LIMITATIONS
SNAPSHOT_CHANGE_EXPLANATION
SOURCE_LINEAGE
```

请求至少包含：

- `question_id`；
- `target_entity_type` 与 `target_entity_id`；
- snapshot query 使用 `snapshot_id`；
- diff query 使用 `diff_id`，并绑定 before/after snapshot ids；
- `locale=zh-CN`；
- `reader_profile=LOW_FINANCE_KNOWLEDGE`。

V1 不允许 fuzzy entity matching、rename inference、隐式“最相关”排序或用自由文本改变
问题语义。未知 question/entity、ambiguous target、unsupported combination 必须返回
`BLOCKED`，不能猜测。

### 3.2 回答与引用

response 至少包含：

- `answer_status=ANSWERED|LIMITED|BLOCKED`；
- `question_id`、target identity 与 input snapshot/diff identity；
- ordered `claims`；
- ordered `citations`；
- `limitations`；
- `safety`；
- canonical response id 与 payload hash。

每个 claim 必须：

- 有稳定 `claim_id`；
- 使用低金融知识读者可读中文；
- 至少引用一个 `citation_id`，或在没有证据时不生成 claim；
- 不把 validation PASS 写成 strategy PASS、promotion ready 或 production ready；
- 不比较或推荐资产、权重、策略优劣。

每个 citation 必须绑定：

- Atlas entity type/id；
- entity before/after hash（适用时）；
- `source_ref_id` 与 tracked repository path；
- exact commit 与 source sha256；
- `as_of`，以及 source 原始记录中的 `known_at`、`available_at`；后二者缺失时必须保留
  显式 `null`，不得用 `as_of` 或其他时间代填；
- DQ/PIT/window/limitation context（适用时）；
- snapshot/diff id。

### 3.3 LIMITED / BLOCKED

- 证据存在但不足以回答全部问题：`LIMITED`，列出缺口；
- 任一 citation 的 `known_at` 或 `available_at` 缺失时，response 必须为 `LIMITED`，
  并包含 reason code=`SOURCE_TIME_CONTEXT_INCOMPLETE`；
- identity、schema、hash、citation closure 或 safety 不成立：`BLOCKED`；
- 不允许把空 citation、missing lineage 或 unsupported question 包装为自然语言答案；
- `BLOCKED` response 仍需给出机器可读 reason codes，但不得泄露未验证内容。

## 4. API 与交互边界

V1 的 `interactive API` 指 versioned read-only application API，不代表部署 HTTP 服务：

- pure request → response；
- 输入为已验证 serialized snapshot/diff；
- 不读取 moving `main`、cache、database、network 或环境隐式状态；
- 不写文件、task、policy、weight、production state 或 broker/order；
- deterministic serialization 与 double-build byte-identical；
- 可由 static HTML demo 或未来 HTTP/LLM adapter 消费。

V1 static demo：

- 提供问题目录与明确 target selector；
- 默认展示“简短回答 → 关键证据 → 限制 → 完整 lineage”；
- 每个 claim 的引用可见且可下钻；
- 无 script/form/external link/write/dispatch；如需要真实浏览器交互，另立 consumer 任务；
- 保持 1280px no-horizontal-overflow 与低金融知识解释。

自由文本/LLM、HTTP server、authentication、session、streaming、vector database、embedding、
external retrieval 与 production deployment 均不在 V1 范围。

## 5. 实施阶段

### S1：Serial public contract wave

- 新增 request/response/claim/citation/question-catalog contracts；
- 冻结 enum、identity、closure、safety 与 deterministic serialization；
- focused contract、round-trip、tamper、unknown/ambiguous fail-closed tests PASS；
- append-only compatibility authority 独立完成后 consumer 才能开始。

### S2：Deterministic query engine

- 独立 validator 验证 serialized snapshot/diff；
- exact question/entity dispatch；
- 从既有 entity fields 构建 claim/citation，不重算投资结论；
- citation closure、claim coverage、status 与 response identity 可独立重建。

### S3：Read API / static demo

- versioned read API facade；
- canonical JSON 与 static HTML 问答卡；
- no-script/no-form/no-external/no-write contract；
- fixture 与 retained Atlas preview 验收。

### S4：Closeout

- 更新 task register、system flow、artifact catalog 与 generated authority；
- focused、Architecture、Contract、Integration、Reproducibility 与风险相称的 Full PASS；
- V1 转 `BASELINE_DONE`；
- 自由文本/LLM consumer、HTTP/deployment 与真实交互另立任务。

## 6. Claims

计划 contract-wave task-owned paths：

```text
src/ai_trading_system/contracts/strategy_research_cited_query.py
src/ai_trading_system/contracts/__init__.py
tests/test_strategy_research_cited_query_contract.py
```

计划 consumer-wave task-owned paths：

```text
src/ai_trading_system/atlas/cited_query.py
src/ai_trading_system/atlas/cited_query_validation.py
src/ai_trading_system/atlas/cited_query_renderer.py
src/ai_trading_system/atlas/__init__.py
tests/atlas/test_cited_query.py
tests/atlas/test_cited_query_validation.py
tests/atlas/test_cited_query_renderer.py
```

coordinator-owned paths：

```text
docs/requirements/TRADING-2470_Atlas_Citation_First_Query_API_V1.md
docs/task_register.md
docs/system_flow.md
docs/artifact_catalog.md
inputs/architecture/**
registry/development_tasks_shadow/**
tests/test_arch_004_refactor_policy.py
tests/test_arch_004g_deprecation.py
```

module claim：

```text
new public cited-query contract, deterministic read-only Atlas query consumer
```

resource/evidence claim：

```text
validated serialized Atlas snapshot/diff only
tracked Git source refs and controlled fixtures
retained Atlas preview only for local acceptance
no external network, market cache, database, model, production or broker resource
```

retained preview lifecycle：

```text
owner task: TRADING-2470_ATLAS_CITATION_FIRST_QUERY_API_V1
purpose: 用 TRADING-2468 V1.1 snapshot 与 TRADING-2469 diff 验收五类 citation-first query
absolute path: D:\Work\AITradingSystem\outputs\atlas\strategy_research_cited_query\trading_2470_v1
exit condition: validation 失败、输入 identity 被 supersede，或 artifact catalog 指向新的 reviewed canonical preview 时删除或重建
retention boundary: task closeout 前保留；closeout 后仅在 artifact catalog 与 compatibility authority 记录 hash/size 时作为 canonical governed evidence 保留
```

## 7. 验收标准

1. serial public contract wave 独立完成；
2. 五个稳定 question ids 与 supported target matrix 冻结；
3. request/response/citation identity 可重算；
4. 100% ANSWERED/LIMITED claims 有 closed canonical citation；
5. source path/commit/hash/as-of/known-at/available-at（含显式 null）与 snapshot/diff
   identity 可下钻；缺失 source time 时只能 `LIMITED` 且不得合成时间；
6. unknown、ambiguous、tampered、missing citation、unsupported question fail closed；
7. 不进行 fuzzy/rename/semantic inference；
8. 不产生新 investment claim、ranking、recommendation、weight 或 readiness；
9. JSON/static HTML deterministic，double-build byte-identical；
10. no-recompute/no-network/no-cache/no-write/no-script/no-form；
11. task shadow、compatibility/current authority 与 applicable formal tiers PASS；
12. `production_effect=none`、`broker_action=none`。

## 8. Stop conditions

- 需要读取 market/cache/external data 或调用 LLM 才能形成 canonical answer；
- 需要 fuzzy matching、rename inference、推荐或投资判断；
- claim 无法闭包到 canonical citation；
- 输入 snapshot/diff 或 source lineage 无法验证；
- 需要改变 Atlas snapshot/diff V1 既有语义；
- 需要部署长期服务、写入状态或执行 production/broker action。

## 9. 工作区与顺序

- governed mode：`SINGLE_LANE`，public contract change；
- frozen base：任务登记提交后的 exact local-main；
- contract wave 必须 serial，consumer 只能从 contract authority exact commit 开始；
- known-unrelated exclusion：
  `docs/research/growth_tilt_owner_diagnosis_pack.md`，不得读取、hash、复制、stage、修改或删除；
- 如 local main 在 frozen lane 期间推进，按 `integration_revalidation_plan.v1` 处理，不创建
  v2/v3 replacement lane；
- temporary workspace、preview server 与 retained artifact 按项目 lifecycle 规则管理。

## 10. 进度记录

- 2026-07-30：TRADING-2469 `BASELINE_DONE` 且 main/remote=`74f3ff5de…` 后按既定路线
  登记本任务。Owner 采纳 citation-first 建议；当前授权 S1 public contract wave，
  不授权自由文本/LLM、HTTP deployment、empirical research、production 或 broker action。
- 2026-07-30：S1 contract source slice 已完成：五个 stable question ids、request、
  question/target matrix、claim、citation、ANSWERED/LIMITED/BLOCKED response 与 canonical
  identity contract 就绪；focused contract=`13 passed`，contract/deprecation=`22 passed`，
  Ruff PASS，task shadow=`933 tasks byte-identical`，DevEx=`1054 modules / 1225 tests PASS`。
  下一步冻结 exact source commit 并追加 contract-wave authority；consumer 在该 authority
  PASS 前不得开始。
- 2026-07-31：S1 source commit=`52e08f1a5ed532066b69ea48004739beee4038ca`，
  append-only contract-wave authority commit=`65a8906dcb3cc6ccc59bc4089314af821011af83`；
  compatibility/deprecation authority=`163 passed`。consumer 从该 exact head 开始，
  仍不授权自由文本/LLM、HTTP deployment、external retrieval、production 或 broker action。
- 2026-07-31：consumer preflight 后核验 retained canonical V1.1 snapshot，8 个 source 的
  `known_at/available_at` 均为显式 `null`；现有 contract 若强制 datetime 会阻断所有实际
  引用，而以 `as_of` 代填会伪造 lineage。consumer 暂停，先完成最小 serial contract
  amendment：保留 null，缺失 source time 的回答只允许 `LIMITED`，并固定 reason
  code=`SOURCE_TIME_CONTEXT_INCOMPLETE`；amendment authority PASS 后再恢复 consumer。
- 2026-07-31：最小 amendment source commit=`d331b034ebd70eca3642e9734fa914d2b5b4129d`，
  append-only amendment authority commit=`0dfafe641f3025a5e53d53fb8584cce6eb04bd41`，
  compatibility/deprecation=`164 passed`；consumer 从该 exact authority head 重新
  preflight PASS。deterministic query engine、independent serialized validator 与 static
  renderer 首轮 focused=`28 passed`，下一步生成上述 retained preview 并做真实浏览器验收。
- 2026-07-31：canonical preview 已使用显式 target IDs 生成，5/5 response independent
  validation PASS、double-build byte-identical；`index.html/responses.json/validation.json`
  SHA-256=`b4c5c6fd…/d3317e3f…/dd17b181…`。真实浏览器打开本地 `file://` artifact 时被当前
  Web Pro 决策审阅 URL policy 拒绝，且 policy 明确禁止切换浏览器、local server 或其他
  browser surface 绕过。未伪报 visual PASS；继续执行 static HTML/escaping/layout-contract 与
  formal validation。退出条件：可访问本地 artifact 的 reviewed browser surface 对 1280px
  no-horizontal-overflow、折叠引用和阅读层级完成人工复核。
