# TRADING-2477：Atlas Historical Source Registration and Typed Adapters V1

最后更新：2026-08-02

稳定任务 ID：
`TRADING-2477_ATLAS_HISTORICAL_SOURCE_REGISTRATION_TYPED_ADAPTER_V1`

优先级：`P1`

状态：`IN_PROGRESS`

Owner 决定：

```text
owner_decision:TRADING-2477:2026-08-02:approve_five_historical_sources_for_registration_and_typed_adapter_v1
```

production effect：`none`

broker action：`none`

## 1. 决策背景

TRADING-2476 已从 291 条 tracked-unregistered queue 中形成首批六组 historical adapter
review pack。Owner 在理解候选语义后明确批准其中五组进入 `source registration + typed
adapter` 开发。该批准只允许：

1. 把五份 exact historical JSON 注册为 Atlas `PUBLISHED_ARTIFACT` source；
2. 建立从 Git canonical blob 到统一 typed historical record 的只读 adapter；
3. 验证 source registry、adapter registry、Git blob 与 typed output 的 exact crosswalk。

该批准不允许：

- 注册或适配 `next_research_program_roadmap`；
- 新增 Atlas node、edge、result 或 attribution；
- 重建或修改现有 cited-query HTML；
- 把历史状态解释成当前策略、投资建议或 production readiness；
- 运行 DQ、model、backtest、score、paper-shadow、production 或 broker 行为。

## 2. Owner-approved exact source set

|Candidate family|Role|JSON path|Git blob SHA-1|Git canonical SHA-256|
|---|---|---|---|---|
|`atlas_historical_candidate_b0_baseline_v1`|`BASELINE`|`docs/research/b0_static_strategic_baseline_result.json`|`056c341e58a7bd9e861cf58f99a006e80b634156`|`33a1c71b9aab8a307c2dacc890a5d1b776f28854ee1efba6bbe757a6d2614b2b`|
|`atlas_historical_candidate_b1_b4_attribution_v1`|`COMPONENT_ATTRIBUTION`|`docs/research/b1_b4_component_result_attribution.json`|`393fa6888fb9ef247fff8fdaf54fdd5f79a2dc62`|`ab8f6c7acbd3eeb01820e302e79282f47e7771d9d2268e627bda6226c588d840`|
|`atlas_historical_candidate_final_branch_decision_v1`|`BRANCH_DECISION`|`docs/research/final_branch_decision_snapshot.json`|`2dc441a29176438fd683985117dbb3a1bc24fe98`|`e7efb46b7b539bfce54b8163b70b7934c508b7653032955ae5ea7dfdbe4046e4`|
|`atlas_historical_candidate_monthly_review_v1`|`MONTHLY_REVIEW`|`docs/research/monthly_research_program_review.json`|`680901b0411ecbdd80d3a82d7e49fdfc5c987110`|`c66864a68153926e2edabe5e6c7699146c19d116db9007054b483983ade824d3`|
|`atlas_historical_candidate_program_snapshot_v1`|`PROGRAM_SNAPSHOT`|`docs/research/weight_research_program_v1_snapshot.json`|`f7bf07ed7857ce243a13dc07269f4fcb2204d33f`|`a367544cec737523716548a0642ad939d504ea06dfe7c08861c0ddf9ab6c095c`|

以上 identity 来自 review pack
`atlas_historical_adapter_review_c6762a379ab3eeb7bb49`，review exact commit 为
`b385f4140b54d936c57f889a55c6f5ba99d074f9`。2026-08-02 在 local main
`02d3e0b266c514af7dd4ebd9e36dfffe86bdba62` 复核时，五个 Git blob SHA-1 与 canonical
SHA-256 均未变化；Windows working-tree CRLF raw hash 不得替代 Git canonical identity。

明确排除：

```text
atlas_historical_candidate_next_roadmap_v1
docs/research/next_research_program_roadmap.json
```

原因：缺少可机械证明的 lineage slot，必须先完成独立 schema-normalization 任务。

## 3. Source registration contract

`config/atlas/source_registry.yaml` 新增五个 source ref，source kind 固定为
`PUBLISHED_ARTIFACT`。所有新 source 必须：

- `research_context_complete=false`；
- `data_quality_ready=false`；
- `legacy_history_partial=true`；
- limitation 明确其历史窗口、单窗或多窗诊断、DQ `PASS_WITH_WARNINGS`、未使用 untouched
  holdout 或不具备当前 primary conclusion 身份；
- 不被任何 node/result/attribution 引用，直到后续独立 page-projection 决策；
- 不改变 `primary_research_start=2021-02-22`。

注册完成后的 Atlas snapshot 结构目标为：

```text
13 sources / 21 nodes / 22 edges / 8 results / 12 attributions
```

source count 增长只表示来源目录扩展；结果、归因和页面展示保持不变。

## 4. Typed adapter contract

新增独立 registry 与 reader：

```text
config/atlas/historical_source_adapters.yaml
src/ai_trading_system/atlas/historical_source_adapters.py
```

Adapter 必须从调用者给定的 exact Git commit 读取 canonical blob，不直接信任 working-tree
换行 bytes。每个 entry 先验证：

1. candidate family、role、source ref、path 唯一；
2. `git rev-parse <commit>:<path>` 等于 approved blob SHA-1；
3. `git show <commit>:<path>` canonical SHA-256 等于 Owner-approved SHA；
4. source registry 存在 exact path/ref、`PUBLISHED_ARTIFACT` 与历史安全 flags；
5. JSON schema/task/report identity 与 role-specific required fields exact 匹配。

统一 typed record 至少保存：candidate/source identity、role、Git blob/SHA、task/report/status、
as-of/window records、lineage paths、reader summary/key result、limitations/blockers/next action 和
role-specific typed payload。必须固定：

```text
historical_record=true
current_primary_default=false
result_projection_allowed=false
page_projection_allowed=false
investment_conclusion_generated=false
production_effect=none
broker_action=none
```

不得使用文件名、自由文本或 LLM 猜测缺失字段；schema、hash、lineage 或 registry crosswalk
drift 必须 typed fail closed。

## 5. 分步计划

### S0：登记与 governed lane

- 本 requirement 与 task-register row 登记 Owner exact approval；
- registration commit 后从 exact local main 创建
  `codex/trading-2477-atlas-historical-source-adapters`；
- 采用 `SINGLE_LANE`，不创建额外 worktree/clone/cache。

### S1：Adapter registry 与 five typed adapters

- 冻结五个 Git canonical identities；
- 实现 exact-commit Git blob reader、source-registry crosswalk 与五种 role parser；
- 对 missing/path traversal/blob/hash/schema/task/report/lineage/roadmap/tamper fail closed；
- 保留 historical window 与 safety caveat，不生成新研究解释。

### S2：Atlas source registration

- `source_registry.yaml` 增加五个 historical refs；
- snapshot 只从 8 sources 变为 13 sources；
- nodes/edges/results/attributions 与现有 HTML bytes 不变；
- 更新 source coverage tests、system flow 和 artifact catalog。

### S3：Governed closeout

- 更新 task shadow、generated architecture、deprecation inventory 与 append-only compatibility；
- focused/static/generated/Architecture/Contract/Integration/Reproducibility/Full 按风险通过；
- ff-only local main、ordinary remote push、分支清理与 canonical evidence handoff。

## 6. 路径与所有权

task-owned：

```text
config/atlas/historical_source_adapters.yaml
src/ai_trading_system/atlas/historical_source_adapters.py
tests/atlas/test_historical_source_adapters.py
```

coordinator-owned：

```text
config/atlas/source_registry.yaml
src/ai_trading_system/atlas/__init__.py
tests/atlas/test_source_projection.py
tests/atlas/test_source_registry_coverage.py
tests/atlas/test_snapshot_builder.py
docs/task_register.md
docs/requirements/TRADING-2476_Atlas_Historical_Adapter_Review_Pack_V1.md
docs/requirements/TRADING-2477_Atlas_Historical_Source_Registration_and_Typed_Adapters_V1.md
docs/system_flow.md
docs/artifact_catalog.md
inputs/architecture/**
registry/development_tasks_shadow/**
registry/development_tasks_shadow_v2/**
tests/test_arch_004_refactor_policy.py
tests/test_arch_004f3_reporting_architecture.py
tests/test_arch_004g_deprecation.py
```

resource claim：五份 approved Git blobs、TRADING-2476 review pack、Atlas source registry、adapter
registry 和 local Git object database。不得读取 known-unrelated path、roadmap bytes、market/cache、
external source；不得启动 HTTP/browser/LLM/DQ/model/backtest/production/broker resource。

## 7. 验收标准

1. 五个 approved candidate id/path/blob/SHA exact 注册，roadmap 明确排除；
2. Git canonical reader 与 Windows working-tree EOL 无关；
3. 五种 typed adapter 可从真实 approved blobs 重建且 double-build deterministic；
4. source registry crosswalk exact，任一 ref/path/kind/safety flag drift fail closed；
5. missing/duplicate/traversal/blob/hash/JSON/schema/task/report/required-field/lineage tamper fail closed；
6. `13 sources / 21 nodes / 22 edges / 8 results / 12 attributions`；
7. current primary window 仍为 `2021-02-22`，历史 `2022-12-01` 不升级为默认；
8. current cited-query HTML bytes 不变，不新增 page result/attribution；
9. focused/generated/compatibility/formal gates PASS；
10. `result_projection_allowed=false`、`page_projection_allowed=false`、
    `production_effect=none`、`broker_action=none`。

## 8. Stop conditions 与后续边界

- 任一 approved Git blob identity 不匹配：停止并回到 Owner review；
- source registry 需要修改 public snapshot schema：另立最小 serial contract wave；
- 需要页面展示、node/result/attribution projection：另立 page-projection 任务并取得 Owner 决策；
- roadmap lineage 未标准化：继续排除；
- 当前任务不得据历史 `PASS`、return 或 branch decision 推导当前投资结论。

## 9. 工作区生命周期

- governed mode：`SINGLE_LANE`；
- registration base：`02d3e0b266c514af7dd4ebd9e36dfffe86bdba62`；
- planned branch：`codex/trading-2477-atlas-historical-source-adapters`；
- workspace：`D:/Work/AITradingSystem`，不创建临时 worktree/clone/cache；
- exit condition：validation、ff-only main、ordinary push、branch cleanup 全部完成；
- adapter registry 与 tracked source registration 为 canonical retained state，可由 Git 恢复。

## 10. 进度记录

- 2026-08-02：Owner 明确批准五份 review-ready historical sources 进入 source registration +
  typed adapter 开发；不批准 roadmap、页面投影或策略采纳。
- 2026-08-02：READ_ONLY preflight PASS；`local main=origin/main=
  02d3e0b266c514af7dd4ebd9e36dfffe86bdba62`，无 active lease。五个 Git blob SHA-1 与 Git
  canonical SHA-256 复核均与 TRADING-2476 review pack 一致；工作区 raw hash 差异仅来自 EOL，
  Git diff 为零。
- 2026-08-02：registration checkpoint=`9e90ccfb9a079473bab0e4f2af0c665c8a7a3ea1`；从该 exact
  local-main base 创建 `codex/trading-2477-atlas-historical-source-adapters`，START/LANE preflight
  均 PASS，无 active lease、path/module/contract/resource conflict，也未创建临时 worktree/clone/cache。
- 2026-08-02：S1/S2 已实现。Atlas registry V1.2 新增五个 `PUBLISHED_ARTIFACT` refs；typed adapter
  从 exact Git commit 读取 canonical blob，校验 blob SHA-1、SHA-256、source crosswalk、JSON identity、
  required fields、role payload、lineage 与固定安全边界。Program snapshot 没有原始 DQ receipt，输出
  显式 `data_quality=null`；未替它推断或补造 DQ 结论。Roadmap 未读取、未注册、未适配。
- 2026-08-02：parallel focused Atlas adapter/source/snapshot=`27 passed`；修正 review commit 的一次
  测试夹具转录错误后重跑全绿。Registry 结构实测为 `13 sources / 21 nodes / 22 edges / 8 results /
  12 attributions`，primary default 仍为 `2021-02-22`，新增 sources 未被 graph/result/attribution 引用。
- 2026-08-02：首次 combined compatibility 命令被 120 秒外层时限终止，没有改用 serial；同一
  `-n 16 --dist loadfile` 口径重跑后为 `190 passed / 73 failed`，失败全部来自新增/变更文件尚未进入
  append-only compatibility hash authority，以及格式化后 generated manifest 变旧。按既有治理合同追加
  TRADING-2477 current-hash authority 并重新生成，不改写任何历史 section；这不是 Atlas 语义失败。
- 2026-08-02：本任务实现状态在 final tree 标为 `BASELINE_DONE`；未完成项只剩需另行 Owner 授权的
  page/result projection，不是本任务实现 blocker。正式五级验证必须在该树全部 PASS 才允许
  integration。Runtime artifact ids 与 counts 在最终交付记录绑定，不为回写数字而制造 compatibility
  hash cycle；若任一 formal gate 失败，必须恢复 `IN_PROGRESS` 并停止集成。
