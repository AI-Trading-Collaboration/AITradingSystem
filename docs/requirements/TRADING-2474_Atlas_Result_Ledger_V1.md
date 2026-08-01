# TRADING-2474：Atlas Result Ledger V1

最后更新：2026-08-01

稳定任务 ID：
`TRADING-2474_ATLAS_RESULT_LEDGER_V1`

优先级：`P1`

状态：`BASELINE_DONE`

Owner 决定：

```text
owner_decision:TRADING-2474:2026-08-01:advance_atlas_result_ledger_v1
```

后续 Owner 决定：

```text
owner_decision:TRADING-2474:2026-08-01:accept_atlas_result_ledger_visual_v1
```

production effect：`none`

broker action：`none`

## 1. 决策背景与目标

TRADING-2470～2473 已在 cited-query 页面闭合五个固定问题、全系统流程、节点进展、
状态 provenance 与原生 evidence drilldown。但当前页面仅把一个 selected result
作为主问答对象，而 validated Atlas V1.1 snapshot 已包含 8 个 result 与 12 条
attribution。这使非金融读者无法在同一视图中直接回答“当前已纳入的所有研究结果
分别是什么，为什么是这个状态”。

本任务在同一静态页面增加 `Result Ledger V1`：

1. 逐项展示 validated snapshot 内的全部 result；
2. 同时显示 raw status 与 reader-facing display status，不把二者混同；
3. 展示通俗摘要、限制、source refs 与全部关联 attributions；
4. 明确声明“全部”只指当前 Atlas V1.1 代表性 campaign 覆盖范围，不代表全仓
   历史研究已完整接入。

本任务不读取新 source、runtime/cache/external data，不重算 DQ、model、backtest、
score、attribution 或 investment conclusion，不引入 HTTP、JavaScript、LLM 或自由文本查询。

## 2. 冻结展示合同

### 2.1 覆盖边界

ledger 仅允许消费 `load_validated_snapshot_payload` 返回的 typed snapshot。页面必须同时
显示：

```text
coverage_scope=ATLAS_V1_1_REPRESENTATIVE_CAMPAIGNS
historical_repository_coverage_complete=false
```

页面可以说“当前覆盖范围内的全部结果”，不得说“全仓所有历史结果已接入”。

### 2.2 结果卡

每个 result 必须稳定展示：

- `result_id` 与 `node_id`；
- `title` 与 `reader_summary`；
- `raw_status` 与 `display_status`；
- `assertion_kind`；
- `investment_facing=false`；
- 全部 `limitations`；
- 全部 `source_ref_ids`；
- 与该 `result_id` 关联的全部 attribution。

result 顺序继承 validated snapshot 的 canonical order，不做“最重要”、“最好”或“最相关”
排序。状态颜色仅辅助扫读，必须同时展示中文标签和原始 status code。

### 2.3 归因卡

每条 attribution 必须展示：

- `attribution_id`；
- direction 的中文标签与原始 code；
- `source_node_id`；
- `explanation` 与 `assertion_kind`；
- 全部 `source_ref_ids`。

attribution 只能按 exact `result_id` 绑定。缺失 result、孤立 attribution、重复 ID、空 source
refs 或 `investment_facing=true` 必须在生成 HTML 前 typed fail closed。

## 3. 分步计划

### S0：任务登记

- 新增 task-register row、supporting requirement 与 Owner token；
- 运行 `READ_ONLY` 安全审计，登记完成后运行 `SINGLE_LANE` preflight；
- 不在登记步骤写入产品代码。

### S1：Result ledger renderer

- 在 cited-query renderer 内复用 validated typed snapshot；
- 实现 all-in-scope result cards、status summary 与 coverage boundary；
- 在每个 result 下使用原生 `<details>/<summary>` 展示 limitations、sources 与
  attributions；
- 不改变 response JSON、validation JSON 或 public query contract。

### S2：Focused validation 与 preview

- 验证 8 个 result、12 条 attribution、精确状态计数和 full relation closure；
- 验证 escaping、deterministic double-build、no-script/no-form/no-iframe/no-external/no-write；
- 在现有 canonical path 重建 preview：
  `outputs/atlas/strategy_research_cited_query/trading_2470_v1/`。

### S3：Owner visual acceptance

- Owner 在 canonical local page 人工复核信息密度、状态颜色、展开层级与窄屏可读性；
- Browser automation 如仍受 `file://` URL policy 限制，记录
  `NOT_EXECUTED_URL_POLICY`，不得绕过或伪报 PASS；
- Owner 验收只是 display acceptance，不是投资、production 或 broker approval。

### S4：Governed closeout

- 同步更新 task register、requirement、system flow、artifact catalog 与 generated
  authority；
- 运行 focused、Architecture、Contract、Integration、Reproducibility 与风险相称的 Full；
- Owner visual PASS 且 formal gates 闭合后转 `BASELINE_DONE`，再执行 local-main / remote
  closeout。

## 4. 路径与所有权

task-owned paths：

```text
src/ai_trading_system/atlas/cited_query_renderer.py
tests/atlas/test_cited_query_renderer.py
```

coordinator-owned paths：

```text
docs/requirements/TRADING-2474_Atlas_Result_Ledger_V1.md
docs/task_register.md
docs/system_flow.md
docs/artifact_catalog.md
inputs/architecture/**
registry/development_tasks_shadow/**
```

resource claim：

```text
validated Atlas snapshot/diff/query payloads only
no market/cache/runtime/external source read
no DQ/model/backtest/attribution recompute
no network/server/LLM/write/production/broker resource
```

## 5. 验收标准

1. 页面完整展示 snapshot 内全部 8 个 result，无遗漏、重复或额外合成结果；
2. 全部 12 条 attribution 按 exact `result_id` 绑定且完整展示；
3. raw/display status 同时可见，并明确两者不是投资评级；
4. 每个 result 的 title、summary、limitations、source refs 与 assertion kind 全部可见；
5. 每条 attribution 的 direction、explanation、source node 与 source refs 全部可见；
6. 页面显式显示两个 coverage boundary fields，不宣称全仓历史覆盖；
7. 状态分布与 validated snapshot 精确一致；
8. 孤立/重复/空 lineage 或 `investment_facing=true` typed fail closed；
9. no script/form/iframe/external/write，HTML escaping 与 keyboard-native disclosure PASS；
10. deterministic double-build byte-identical；
11. focused/formal/Owner manual visual PASS；
12. `production_effect=none`、`broker_action=none`。

## 6. Stop conditions

- 必须新增或修改 public snapshot/query contract 才能实现；
- 需要解析自由文本或使用 fuzzy/relevance ranking 才能选结果；
- 需要重算 result、attribution、DQ、model 或 backtest；
- 页面无法保留 current coverage 与 full-history coverage 的区别；
- 需要读取 known-unrelated exclusion、runtime/cache/external data、启动 HTTP/LLM 或执行写操作。

## 7. 工作区生命周期

- governed mode：`SINGLE_LANE`；
- branch：`codex/trading-2474-atlas-result-ledger`；
- workspace：`D:/Work/AITradingSystem`，不创建额外 worktree；
- known-unrelated exclusion：`docs/research/growth_tilt_owner_diagnosis_pack.md`，不得读取、hash、
  复制、stage、修改或删除；
- retained preview 继续位于
  `D:/Work/AITradingSystem/outputs/atlas/strategy_research_cited_query/trading_2470_v1`，
  旧版可由 Git 实现与已记录 hash 重建；
- exit condition：Owner visual acceptance、formal validation、ff-only local-main、ordinary push 与
  merged branch cleanup 全部完成。

## 8. 开放问题与后续边界

- 全仓历史 campaign adapter inventory 仍是独立的 coverage 任务；
- HTTP adapter、自由文本或 LLM consumer 仍需独立 Owner 决策；
- 本任务只解决“已纳入的全部结果如何一眼看到”，不把“已纳入”伪写成
  “全部历史”。

## 9. 进度记录

- 2026-08-01：TRADING-2473 完成 local-main/remote closeout 后，Owner 要求继续。依原始
  “所有实际研究结果及归因”产品目标，选择先闭合 validated snapshot 内已有
  8 results / 12 attributions 的展示缺口；登记任务并进入 `IN_PROGRESS`。
- 2026-08-01：Result Ledger V1 已实现；两列 desktop / 单列窄屏卡片显示全部
  8 results，原生 disclosure 闭合全部 12 attributions。renderer focused=`8 passed`，
  Atlas/citation focused=`80 passed`，Ruff/Black PASS；actual-input double-build byte-identical，
  canonical `index.html`=`92180 bytes / SHA-256 b7540d87caf2…`，responses/validation SHA
  保持 `d3317e3f…/dd17b181…`。离线 DOM audit=`8 unique results / 12 unique attributions /
  29 details / 1 open / 0 script/form/iframe/external`。Browser 刷新本地 `file://` 仍被 URL
  policy 拒绝，已按规则停止且未绕过；当前等待 Owner 手工 visual acceptance，尚未运行
  closeout formal gates。
- 2026-08-01：Owner 手工复核 canonical local page 后回复“验证通过 继续推进”，
  单独记录 `OWNER_MANUAL_VISUAL_PASS` 和 token=
  `owner_decision:TRADING-2474:2026-08-01:accept_atlas_result_ledger_visual_v1`。本验收
  只确认信息密度、状态扫读、disclosure 层级与可读性，不是 strategy PASS、
  投资评级、production 或 broker approval。实现与视觉基线转 `BASELINE_DONE`，由
  coordinator 继续执行 append-only authority、formal gates 与 governed closeout；formal
  结果写回前不得视为完成 local-main/remote closeout。
- 2026-08-01：append-only compatibility authority 已以
  `d84ed6915e1f879f64c69683b320663ccce0e1e9` 的原始 baseline blob 作为 immutable prefix；
  前缀=`2297927 bytes / SHA-256 3a471f4d2f1e0cd03bb820bf5d6e780f1e26d5f84dc39cc57cb24acf14c956a7`，
  唯一 TRADING-2474 section 从精确 EOF 开始。task registry=`937 tasks`，generated
  architecture=`1057 modules / 1228 tests`，compatibility/deprecation=`169 passed`。
- 2026-08-01：pre-formal candidate=`d3d66f1b7bdc6ec9692faacdfce87f806d12ac3e`。正式验证
  PASS：Architecture=`816 passed`，runtime=`architecture-fitness_20260801T040918Z`；
  Contract 首次执行因外层命令 `124s` timeout 被终止，未形成 PASS 且不是测试断言失败；确认无残留
  pytest 后从同一 candidate 以更长受控时限重跑，`276 passed`，runtime=
  `contract-validation_20260801T041348Z`；Integration=`995 passed / 642 warnings`，runtime=
  `integration_20260801T041601Z`；Reproducibility=`24 passed`，runtime=
  `reproducibility_20260801T041652Z`；Full=`7872 passed / 3 skipped / 644 warnings`，runtime=
  `full_20260801T041725Z`。下一步仅写回这些证据并对 final tree 复跑 Architecture/Contract，
  不改变 research、DQ、investment、production 或 broker 边界。
