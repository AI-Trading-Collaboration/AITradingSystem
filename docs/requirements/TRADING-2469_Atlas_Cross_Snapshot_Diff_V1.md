# TRADING-2469：Atlas Cross-Snapshot Diff V1

最后更新：2026-07-30

稳定任务 ID：
`TRADING-2469_ATLAS_CROSS_SNAPSHOT_DIFF_V1`

优先级：`P1`

状态：`BASELINE_DONE`

Owner 决定：

```text
owner_decision:TRADING-2469:2026-07-30:advance_atlas_cross_snapshot_diff_v1
```

production effect：`none`

broker action：`none`

## 1. 目标

在 TRADING-2468 的 Atlas V1.1 上增加 deterministic、read-only 的跨快照差异解释层，让金融
知识较少的读者一眼区分：

- 新增了哪些研究主线、结果和归因；
- 哪些既有结论、状态、限制或证据引用发生了实质变化；
- 哪些变化只来自 exact Git commit / lineage 更新，不代表研究结论改变；
- 删除项与新增项不能被静默解释为“重命名”；
- diff PASS 只表示两个合法 snapshot 的差异计算可复现，不是 strategy PASS。

本任务不重算 DQ、coverage、model、backtest、metric、threshold、candidate、weight、
promotion 或 Owner decision，不读取 market cache、外部数据或 secret。

## 2. Exact preview inputs

V1 preview：

```text
path=outputs/atlas/strategy_research_explorer/trading_2466_mvp/snapshot.json
snapshot_id=ef098be7d4b2bf970aab04c6d19d06f26a47c70294ff9577ae6f4b7f2e90be14
file_sha256=1be1f13426fcc3d7f58fd8b7fb19b43fca4f355c20976139100727727e60b6cd
sources=3 nodes=7 edges=6 results=3 attributions=3
```

V1.1 preview：

```text
path=outputs/atlas/strategy_research_explorer/trading_2468_v1_1/snapshot.json
snapshot_id=917c0d388ffbc27ab864b33701625422748d037ac98b16b42be6dfe1bd9331b7
file_sha256=ca4ea41acb699405a685d8b0f5722b727ea535b52841e9c55707385f00fc8eac
sources=8 nodes=21 edges=22 results=8 attributions=12
```

这两份 retained preview 只用于本任务的本地 canonical demo。公共 builder 接收已经通过
`StrategyResearchExplorerSnapshot.from_dict` 重建的 typed snapshots；它不以 ignored output
作为运行时 source of truth。缺失、SHA drift、snapshot-id mismatch 或 contract invalid 必须
fail closed。

## 3. Serial public contract wave

本任务新增公共 contract：

```text
strategy_research_explorer_entity_change.v1
strategy_research_explorer_diff.v1
atlas_explorer_diff_validation.v1
```

因为这是 consumer-visible boundary，必须先完成最小 serial contract wave：

1. 冻结 enum、字段、canonical ordering、identity 与 read-only safety；
2. 完成 round-trip、tamper、duplicate、invalid transition 和 deterministic identity tests；
3. contract wave 单独提交并验证；
4. 后续 builder、validator、renderer 从该 exact contract commit 继续，不在 consumer wave
   中反向修改 contract。

## 4. Diff semantics

entity kinds：

```text
SOURCE
NODE
EDGE
RESULT
ATTRIBUTION
```

change kinds：

```text
ADDED
REMOVED
CHANGED
```

reader significance：

```text
SEMANTIC
LINEAGE_ONLY
STRUCTURAL
```

规则：

- 同一 entity kind + stable ID 才能比较；
- ID 只在 before 出现为 `REMOVED`，只在 after 出现为 `ADDED`；
- 不推断 rename，不做 fuzzy matching；
- 同 ID canonical payload 不同才为 `CHANGED`；
- `changed_fields` 必须为 exact top-level field names 且排序稳定；
- 仅 `exact_commit` / `as_of` / `known_at` / `available_at` 变化且 content identity 不变时为
  `LINEAGE_ONLY`；
- source content SHA、artifact identity、DQ/context/legacy/window/limitation 改变，以及
  node/result/status/assertion/attribution解释改变，均为 `SEMANTIC`；
- node/edge/result/attribution 新增或删除为 `STRUCTURAL`；
- before/after entity SHA-256 必须绑定 canonical JSON bytes；不存在的一侧为 null；
- `diff_id` 必须由 before/after snapshot IDs、排序后的 changes、summary 与 safety fields
  计算，不包含 runtime clock；
- empty diff 默认拒绝；同一 snapshot 与自身比较默认拒绝。

## 5. Reader presentation

输出：

```text
outputs/atlas/strategy_research_diff/trading_2469_v1/
  index.html
  diff.json
  validation.json
  input_receipt.json
```

页面最少包含：

- “从哪个 snapshot 到哪个 snapshot”；
- 新增 / 删除 / 实质变化 / lineage-only 四个摘要；
- 按 SOURCE / NODE / EDGE / RESULT / ATTRIBUTION 分组；
- 每项显示 stable ID、变化类型、changed fields、before/after status 或摘要；
- “这不意味着什么”固定边界；
- V1 → V1.1 的主要新增 campaign 解释；
- 无 script、form、external resource、write API 或 command dispatch。

`input_receipt.json` 绑定两个输入路径、文件 SHA-256、snapshot IDs、size 与读取时间之外的
全部 deterministic identity；不得把绝对 checkout path 写入 canonical identity。

## 6. 实施步骤

### S0：登记与 preflight

- 登记本任务、Owner token、P1、owner、blocker、acceptance；
- 使用 `SINGLE_LANE --contract-change`；
- frozen base=`dd7d8ec0082327d8da7efb4e207870e627d4b5d3`。

### S1：Serial contract wave

- 新增 diff contract 与 contract tests；
- 更新 contract exports；
- focused contract + Ruff + governed authority PASS；
- 单独提交 contract wave。

### S2：Builder / validator

- 新增 deterministic entity comparison；
- 独立 validator 从 serialized diff 重建 identity、summary 和 entity hashes；
- tamper、duplicate、wrong before/after、lineage misclassification fail closed。

### S3：Static renderer / artifact writer

- 生成 HTML / JSON / receipt；
- 默认按读者重要性排序，lineage-only 可折叠但不得隐藏；
- HTML escaping 与 no-script/no-form contract。

### S4：Preview / validation / closeout

- 使用第2节 exact preview inputs；
- double-build byte-identical；
- 更新 task register、system flow、artifact catalog、generated task shadow 与
  compatibility authority；
- focused、Architecture、Contract、Integration、Reproducibility 与风险相称的 Full PASS；
- 状态转 `BASELINE_DONE`，随后进入带引用问答 / interactive API 的独立任务。

## 7. Claims

contract-wave task-owned paths：

```text
src/ai_trading_system/contracts/strategy_research_explorer_diff.py
src/ai_trading_system/contracts/__init__.py
tests/test_strategy_research_explorer_diff_contract.py
```

consumer-wave task-owned paths：

```text
src/ai_trading_system/atlas/snapshot_diff.py
src/ai_trading_system/atlas/diff_validation.py
src/ai_trading_system/atlas/diff_renderer.py
src/ai_trading_system/atlas/__init__.py
tests/atlas/test_snapshot_diff.py
tests/atlas/test_diff_validation.py
tests/atlas/test_diff_renderer.py
```

coordinator-owned paths：

```text
docs/requirements/TRADING-2469_Atlas_Cross_Snapshot_Diff_V1.md
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
new public diff contract, deterministic read-only Atlas diff consumer
```

resource claim：

```text
two exact local retained snapshot files for preview only
tracked source and tests only
no external network
no market cache
no DQ/model/backtest execution
no production or broker resource
```

## 8. 验收标准

1. serial contract wave 独立完成并通过 contract tests；
2. two snapshot identities / file hashes / sizes 绑定；
3. stable-ID add/remove/change 规则完整；
4. no rename inference / no fuzzy matching；
5. lineage-only 与 semantic/structural 分栏；
6. entity before/after SHA、changed fields、summary counts 可重算；
7. diff identity 与 canonical bytes double-build 一致；
8. tamper、duplicate、same-snapshot、empty-diff、invalid safety fail closed；
9. HTML 面向低金融知识读者且无 script/form/external/write/dispatch；
10. V1 → V1.1 preview 与 input receipt validation PASS；
11. task shadow、compatibility/current authority 与 applicable formal tiers PASS；
12. `production_effect=none`、`broker_action=none`。

## 9. Stop conditions

- 需要读取 market/cache/external data 才能产生 diff；
- 需要推断 rename、策略好坏或投资结论；
- 无法区分 lineage-only 与 research-semantic change；
- 需要修改 Atlas snapshot v1 既有语义；
- 需要执行 DQ、coverage、model、backtest 或 candidate selection；
- 输入 snapshot identity / receipt 无法验证。

## 10. 工作区生命周期

- governed mode：`SINGLE_LANE`，public contract change；
- frozen base：`dd7d8ec0082327d8da7efb4e207870e627d4b5d3`；
- branch：`codex/trading-2469-atlas-diff`；
- workspace：`D:/Work/AITradingSystem`，不创建额外 worktree；
- known-unrelated exclusion：
  `docs/research/growth_tilt_owner_diagnosis_pack.md`，不得读取、hash、复制、stage、修改或删除；
- retained preview：第2节两份 input snapshot 与本任务第5节 output，作为 canonical local UX
  evidence 保留；
- exit condition：contract wave 与 consumer wave 均验证完成，final candidate ff-only 进入
  local main、ordinary push 完成、task branch 删除并 prune。

## 11. 进度记录

- 2026-07-30：TRADING-2468 `BASELINE_DONE` 后按既定优先级进入 cross-snapshot diff；
  Owner token 冻结，任务进入 `IN_PROGRESS`。当前只授权 contract / read-only diff，
  不授权带引用问答、interactive mutation、empirical research、production 或 broker action。
- 2026-07-30：S1 contract source slice 已完成：diff/entity/summary/field-change contract、
  public exports 与 fail-closed tests 就绪；focused contract=`16 passed`，Ruff PASS，
  task-shadow S0/S1/V2=`932 tasks byte-identical`，DevEx=`1050 modules / 1221 tests PASS`。
  append-only authority 首次运行按预期报告旧 TRADING-2468 current authority 无法覆盖新增
  `contracts/__init__.py`、task 932 与新 inventory identity；这不是可绕过的测试失败。
  先冻结本 source commit，再以其 exact Git blob 追加 TRADING-2469 contract-wave authority，
  旧 compatibility bytes 保持不变；authority PASS 后 consumer 才可开始。
- 2026-07-30：S1 source commit=`2398b144ad886db51a29ef4fbc6e5e2d555b0a66`，
  append-only contract-wave authority commit=`2310627e3`；compatibility/deprecation
  authority=`161 passed`，contract=`16 passed`。consumer 从该 exact head 开始。
- 2026-07-30：S2/S3 consumer 与实际 V1→V1.1 preview 已完成。diff
  `d5aa7e38ae23693682892aa91fc312f41a2246572b432b0483ded4412ea8005f`，
  共 55 条变化：50 added、1 removed、4 changed；2 semantic、2 lineage-only、
  51 structural。五类 before→after 为 SOURCE 3→8、NODE 7→21、EDGE 6→22、
  RESULT 3→8、ATTRIBUTION 3→12。focused consumer/contract=`19 passed`；
  in-app browser 首屏、完整 DOM、1280px no-horizontal-overflow、no script/form/external
  与 `<details open>` lineage section 检查 PASS；临时 preview server 已停止。
  实际 retained inputs 连续原地 double-build 与既存 canonical bytes 三方一致；
  `index.html/diff.json/validation.json/input_receipt.json` 均 byte-identical。
  Retained output：
  `outputs/atlas/strategy_research_diff/trading_2469_v1/`，receipt id
  `8043261428a57e5231fb0d9f4e010adefa675c9894338fd5086855504f819228`。
- 2026-07-30：consumer source commit=`1adb5cff1667b29733361cfc9d1f2694af146287`；
  focused consumer/contract/deprecation=`37 passed`，Ruff PASS，
  task shadow=`932 tasks byte-identical`，DevEx=`1053 modules / 1224 tests PASS`。
  正式门禁全部通过：Architecture=`809 passed`，Contract=`276 passed`，
  Integration=`995 passed / 643 warnings`，Reproducibility=`24 passed`，
  Full=`7832 passed / 3 skipped / 643 warnings`。任务转为 `BASELINE_DONE`；
  后续带引用问答与 interactive API 必须另立任务，不得借本任务扩张为 mutation、
  empirical research、production 或 broker action。
