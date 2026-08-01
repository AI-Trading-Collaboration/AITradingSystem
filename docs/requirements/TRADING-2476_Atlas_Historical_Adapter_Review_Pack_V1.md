# TRADING-2476：Atlas Historical Adapter Review Pack V1

最后更新：2026-08-01

稳定任务 ID：
`TRADING-2476_ATLAS_HISTORICAL_ADAPTER_REVIEW_PACK_V1`

优先级：`P1`

状态：`IN_PROGRESS`

Owner 决定：

```text
owner_decision:TRADING-2476:2026-08-01:advance_atlas_historical_adapter_review_pack_v1
```

production effect：`none`

broker action：`none`

## 1. 决策背景与目标

TRADING-2475 已生成全仓历史研究 coverage inventory，并留下 `291` 条
`TRACKED_UNREGISTERED_REVIEW_REQUIRED` 路径。Owner 回复“继续”，因此本任务从该队列中选择
第一批结构化候选，形成可审计 adapter review pack，回答：

1. 候选文件是否仍与 TRADING-2475 exact inventory identity 和 Git blob identity 一致；
2. JSON 是否可解析、是否与同 basename Markdown companion 成对、顶层 schema/字段形态是什么；
3. 是否存在可机械映射的 identity、research window、lineage、result/status、attribution、limitation
   字段；
4. 哪些候选适合进入后续 typed adapter contract，哪些仍需 source registration、schema normalization
   或 Owner 语义复核；
5. 后续页面接入需要复用哪些原始字段，而不是从文件名、自由文本或 LLM 猜测结论。

本任务只生成 review pack，不修改 `config/atlas/source_registry.yaml`、Atlas snapshot/public query
schema 或 cited-query HTML，不生成新的 Atlas result/attribution，也不重算研究、DQ、model、backtest、
score 或 investment conclusion。

## 2. 首批 exact allowlist 与选择依据

TRADING-2475 inventory 只提供 path metadata；截至任务登记时，以下内容字节均未被本任务读取。
首批固定为六组 exact JSON/Markdown companion：

```text
docs/research/b0_static_strategic_baseline_result.json
docs/research/b0_static_strategic_baseline_result.md
docs/research/b1_b4_component_result_attribution.json
docs/research/b1_b4_component_result_attribution.md
docs/research/final_branch_decision_snapshot.json
docs/research/final_branch_decision_snapshot.md
docs/research/monthly_research_program_review.json
docs/research/monthly_research_program_review.md
docs/research/next_research_program_roadmap.json
docs/research/next_research_program_roadmap.md
docs/research/weight_research_program_v1_snapshot.json
docs/research/weight_research_program_v1_snapshot.md
```

选择依据是机械可审计的：

- 12 条路径均来自 TRADING-2475 的 `291` 条 tracked-unregistered queue；
- 每组具有唯一同 basename `.json` / `.md` companion；
- 六组共同覆盖 baseline、component attribution、branch decision、monthly review、roadmap 与 program
  snapshot 的研究主线角色，但文件名角色不等于内容结论；
- 优先读取 JSON typed shape，Markdown 只用于 companion identity、声明边界与人类可读一致性复核；
- 不扩大到 wildcard declaration、其他相似文件名或 known-unrelated path。

known-unrelated exclusion 继续固定为：

```text
docs/research/growth_tilt_owner_diagnosis_pack.md
```

不得打开、hash、复制、stage、修改或删除该路径。

## 3. Review contract

### 3.1 Authority inputs

只允许读取：

```text
outputs/atlas/historical_research_coverage_inventory/trading_2475_v1/inventory.json
config/atlas/historical_adapter_review_policy.yaml
上述 12 条 exact allowlist 文件
Git exact commit/blob metadata
```

所有输入保存 path、Git blob SHA-1、content SHA-256、size 与 inventory classification receipt。候选必须
仍为 `TRACKED_UNREGISTERED_REVIEW_REQUIRED`；缺失、重复、非 allowlist、inventory identity drift、
JSON parse failure、companion mismatch 或 receipt tamper 必须 fail closed。

### 3.2 Mechanical observations

每个 JSON candidate 只允许输出可重算的结构观察：

- 顶层 JSON kind 与 sorted field names；
- schema/id/version/date/window/lineage/source/ref/status/result/attribution/limitation 相关字段的 exact
  JSON pointer inventory；
- null/list/object/scalar shape，不解释字段值是否正确；
- Markdown companion 的 title、declared identifier/window/source-ref token presence 与 SHA receipt；
- JSON/Markdown 是否包含相同的 explicit identity token；若无法机械证明则标为 review required。

不得从 prose、文件名或字段值推导新的研究结论，也不得把 historical window 静默当作当前
`2021-02-22` active default。

### 3.3 Review disposition

V1 disposition 是 adapter readiness，不是投资或研究 verdict：

- `READY_FOR_OWNER_ADAPTER_REVIEW`：JSON 可解析、identity/window/lineage/result-or-status/limitation
  五类结构槽均存在，并有 companion；
- `NEEDS_SCHEMA_NORMALIZATION`：文件可解析但缺少一个或多个结构槽；
- `NEEDS_SOURCE_REGISTRATION`：结构槽齐全但尚无 reviewed Atlas/report source authority；
- `REJECTED_FROM_FIRST_BATCH`：identity/receipt/parse/companion 合同失败。

`READY_FOR_OWNER_ADAPTER_REVIEW` 不授权 source registration 或页面投影；最终 adopter 列表必须由
后续 Owner decision 精确列出 candidate id 与 artifact SHA。

## 4. Typed outputs

canonical output 目录：

```text
outputs/atlas/historical_adapter_review/trading_2476_v1/
```

输出：

- `review_pack.json`：candidate receipts、JSON pointer/shape inventory、mechanical disposition 与 blockers；
- `review_pack.md`：面向 Owner 的中文主线表和逐候选审阅问题；
- `validation.json`：独立重建 inventory binding、allowlist、hash、schema shape、disposition 与
  no-projection safety contract。

所有输出固定：

```text
source_registration_performed=false
atlas_result_projection_performed=false
investment_conclusion_generated=false
production_effect=none
broker_action=none
```

## 5. 分步计划

### S0：登记与 governed preflight

- 新增本 requirement 与 task-register row；
- 修正 TRADING-2475 requirement 页首状态漂移为 `BASELINE_DONE`；
- 从 registration commit 的 exact local `main` 创建
  `codex/trading-2476-atlas-adapter-review-pack`；
- 以 `SINGLE_LANE` 声明 task/coordinator paths，不创建额外 worktree。

### S1：Policy、typed reader 与 independent validator

- 冻结 12-path allowlist、inventory id、角色槽与 disposition rule；
- 只打开 allowlist bytes，生成 exact receipts 和 bounded JSON pointer inventory；
- Markdown 只提取 bounded title/token presence，不保存大段原文；
- validator 独立重建并对漏项、越界读取、hash/schema/disposition tamper fail closed。

### S2：Canonical review pack 与 Owner handoff

- actual-input double-build byte-identical；
- 输出中文主线表、每组 blocker 与下一合法动作；
- 明确推荐只是一份待 Owner 审批的 adapter shortlist；
- 不接入 source registry、snapshot、query renderer 或 HTML。

### S3：Governed closeout

- 更新 task shadow、generated architecture、artifact catalog/report registry（仅登记 review pack）与
  append-only compatibility authority；
- focused、Architecture、Contract、Reproducibility 与风险相称的 Full PASS；
- ff-only local-main、ordinary remote push 与任务分支清理。

## 6. 路径与所有权

task-owned：

```text
config/atlas/historical_adapter_review_policy.yaml
src/ai_trading_system/atlas/historical_adapter_review.py
tests/atlas/test_historical_adapter_review.py
```

coordinator-owned：

```text
src/ai_trading_system/atlas/__init__.py
config/report_registry.yaml
docs/task_register.md
docs/requirements/TRADING-2475_Atlas_Historical_Research_Coverage_Inventory_V1.md
docs/requirements/TRADING-2476_Atlas_Historical_Adapter_Review_Pack_V1.md
docs/system_flow.md
docs/artifact_catalog.md
inputs/architecture/**
registry/development_tasks_shadow/**
registry/development_tasks_shadow_v2/**
tests/test_arch_004_refactor_policy.py
tests/test_arch_004f3_reporting_architecture.py
tests/test_arch_004g_deprecation.py
```

resource claim：TRADING-2475 inventory、12 条 exact allowlist research artifact bytes、Git blob metadata 与
canonical review output directory。不得读取 known-unrelated、market/cache、其他 research artifact、
external source；不得启动 HTTP、browser、LLM、data acquisition、DQ、model、backtest、production 或
broker resource。

## 7. 验收标准

1. 12 条 candidate path 与 TRADING-2475 inventory id/classification exact 绑定；
2. candidate Git blob/content SHA/size 可重算，allowlist 外 research content read count 为 0；
3. JSON pointer/shape inventory 有界、稳定排序且不复制自由文本正文；
4. Markdown 仅保存 bounded title/token presence 与 receipt，不输出长引用；
5. identity/window/lineage/result-or-status/limitation 槽按 reviewed policy 机械判断；
6. disposition 只表达 adapter readiness，不表达策略优劣或研究正确性；
7. missing/duplicate/parse/companion/inventory/hash/schema/disposition tamper typed fail closed；
8. actual-input double-build byte-identical，JSON/Markdown/validation 数量一致；
9. current Atlas source/result/attribution identity 与 cited-query HTML bytes 不变；
10. focused/generated/compatibility/formal gates PASS；
11. `source_registration_performed=false`、`atlas_result_projection_performed=false`；
12. `production_effect=none`、`broker_action=none`。

## 8. Stop conditions 与后续边界

- 若候选必须依靠自由文本/LLM 推断才能识别 result/status/lineage，保持
  `NEEDS_SCHEMA_NORMALIZATION`，不得补造字段；
- 若后续需要改变 Atlas snapshot/public query schema，另立最小 serial contract wave；
- 若 Owner 未精确批准 candidate id + artifact SHA，禁止 source registration 与页面投影；
- 若历史 evidence window 与 active default 不同，只记录 exact historical role/caveat，不重算或升级；
- 实际 typed adapter、source registration 和 historical coverage panel 是后续独立任务。

## 9. 工作区生命周期

- governed mode：`SINGLE_LANE`；
- registration base：`f121017488affbf26113de03be47d9179a9c57b0`；
- branch：`codex/trading-2476-atlas-adapter-review-pack`；
- workspace：`D:/Work/AITradingSystem`，不创建临时 worktree/clone/cache；
- review outputs 保留至 Owner adapter decision，可由 exact commit 与 allowlist bytes 重建；
- exit condition：validation、ff-only main、ordinary push、branch cleanup 全部完成。

## 10. 进度记录

- 2026-08-01：TRADING-2475 closeout 后 Owner 回复“继续”。READ_ONLY preflight PASS，
  `local main=origin/main=f121017488affbf26113de03be47d9179a9c57b0`，无 active lease；只读取
  TRADING-2475 inventory path records，未打开任何 candidate research artifact bytes。
- 2026-08-01：从 291 条 queue 机械冻结六组 JSON/Markdown companion；选择只决定审阅范围，
  不代表文件内容正确、适合生产或已成为 Atlas source/result。
- 2026-08-01：S1 policy、exact-commit allowlist reader、bounded JSON pointer/Markdown title-token
  inventory、adapter-readiness disposition、canonical renderer 与独立重建 validator 已实现。实际结构观察
  为 5 组五类槽齐全、`next_research_program_roadmap` 缺可机械证明的 lineage 槽；未序列化任何
  status/result/limitation 字段值。Black、Ruff、strict mypy PASS，parallel focused=`5 passed`。
