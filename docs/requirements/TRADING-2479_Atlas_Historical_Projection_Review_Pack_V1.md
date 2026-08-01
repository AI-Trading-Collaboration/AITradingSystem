# TRADING-2479：Atlas Historical Projection Review Pack V1

最后更新：2026-08-02

稳定任务 ID：
`TRADING-2479_ATLAS_HISTORICAL_PROJECTION_REVIEW_PACK_V1`

优先级：`P1`

状态：`IN_PROGRESS`

Owner 决定：

```text
owner_decision:TRADING-2479:2026-08-02:advance_atlas_historical_projection_review_pack_v1
```

production effect：`none`

broker action：`none`

## 1. 决策背景与本任务边界

TRADING-2477 已把五份 Owner-approved historical JSON 注册为 Atlas
`PUBLISHED_ARTIFACT` source，并从 exact Git blob 构建 typed records。当前 canonical Atlas
snapshot 与已验收 cited-query 页面仍保持：

```text
13 sources / 21 nodes / 22 edges / 8 results / 12 attributions
```

Owner 要求继续推进相关任务，但在正式把历史材料投影为 node/result/attribution 前，需要先看清：

1. 五份材料将在整条策略研发主线的什么位置出现；
2. 原始 historical status 与面向普通读者的 Atlas display status 如何对应；
3. 哪些内容只是 provenance、阶段判断或历史限制，不能解释为当前策略有效性；
4. 投影后结构数量、stable IDs、来源绑定和页面信息密度是否可接受。

因此本任务只生成独立、deterministic、review-only 的 projection review pack 与 HTML preview。
本任务不修改或重建已验收 canonical 页面：

```text
outputs/atlas/strategy_research_cited_query/trading_2470_v1/index.html
```

也不激活 Atlas snapshot 的 node/result/attribution projection。Owner 对 review pack 的视觉和语义
验收不自动授权 canonical projection；任何实际投影必须在后续独立任务中记录新的 Owner 决策。

## 2. 输入权威与 exact source set

review pack 只能读取：

- `config/atlas/source_registry.yaml` 中五个 historical source registrations；
- `config/atlas/historical_source_adapters.yaml`；
- `src/ai_trading_system/atlas/historical_source_adapters.py` 从调用者给定 exact commit 重建的
  typed records；
- 当前 canonical Atlas snapshot 的结构计数与 stable-ID 集合；
- 当前 canonical cited-query HTML 的 path、byte count 和 SHA-256 identity。

五个 exact source refs：

|Role|Source ref|Historical raw status|
|---|---|---|
|`BASELINE`|`historical-b0-baseline`|`B0_MINI_BACKFILL_COMPLETE_CONTROL_ONLY`|
|`COMPONENT_ATTRIBUTION`|`historical-b1-b4-attribution`|adapter 中的 ready/completed historical status|
|`BRANCH_DECISION`|`historical-final-branch-decision`|`CONTINUE_B2_ONLY_PATH`|
|`MONTHLY_REVIEW`|`historical-monthly-program-review`|adapter 中的 ready/completed historical status|
|`PROGRAM_SNAPSHOT`|`historical-weight-program-snapshot`|`NEEDS_MORE_EVIDENCE`|

`atlas_historical_candidate_next_roadmap_v1` 与
`docs/research/next_research_program_roadmap.json` 继续排除；不得读取其 bytes、注册、适配或投影。
known-unrelated exclusion 继续保持零读取。

## 3. 候选投影设计

review pack 必须同时展示 current 与 proposed-after-later-approval 两组计数：

|Entity|Current|Candidate|Delta|
|---|---:|---:|---:|
|sources|13|13|0|
|nodes|21|27|+6|
|edges|22|28|+6|
|results|8|13|+5|
|attributions|12|17|+5|

候选只表达未来可能采用的结构，不进入 current snapshot：

### 3.1 Candidate nodes

|Stable ID|Kind|Assertion kind|Purpose|
|---|---|---|---|
|`campaign-historical-weight-research`|`CAMPAIGN`|`RESEARCHER_INTERPRETATION`|隔离历史 weight-research lane|
|`evidence-historical-b0-baseline`|`EVIDENCE`|`MODEL_RESULT`|control-only baseline historical evidence|
|`evidence-historical-b1-b4-attribution`|`EVIDENCE`|`RESEARCHER_INTERPRETATION`|B1-B4 component attribution review|
|`artifact-historical-weight-program-snapshot`|`ARTIFACT`|`RESEARCHER_INTERPRETATION`|historical program snapshot|
|`artifact-historical-monthly-program-review`|`ARTIFACT`|`RESEARCHER_INTERPRETATION`|monthly program review|
|`decision-historical-final-branch`|`DECISION`|`RULE_JUDGMENT`|historical branch decision；不得升级为 Owner decision|

### 3.2 Candidate edges

只允许六条 `CONTAINS` edge：current `program-strategy-research` contains historical group，group
contains 五个 historical nodes。不得新增 `DEPENDS_ON`、`PRODUCES` 或其他暗示因果、时序、
有效性传递的关系。页面卡片顺序只表示阅读顺序，不表示研究因果顺序。

### 3.3 Candidate results and attributions

每个 historical node 对应一个 candidate result 和一个 provenance attribution：

```text
result-historical-b0-baseline
result-historical-b1-b4-attribution
result-historical-weight-program-snapshot
result-historical-monthly-program-review
result-historical-final-branch-decision
```

Attribution direction 固定为 `NEUTRAL`，只表达 result 与 exact historical source 的 provenance
绑定，不表达支持、反对、收益贡献或当前策略有效性。所有 candidate result 固定：

```text
display_status=LIMITED
investment_facing=false
```

四份完成/ready/decision 类 source 的 proposed Atlas `raw_status=PASS`；
`historical-weight-program-snapshot` 的 proposed Atlas `raw_status=LIMITED`。review pack 必须在每张
卡片上同时保留 source original raw status、proposed Atlas raw status、display status 与映射理由；
不得把 `PASS` 解释为当前策略或投资结论 PASS。

## 4. 信息展示与普通读者体验

独立 HTML preview 至少包含：

1. 首屏醒目的“审阅包，不是当前结果页”边界；
2. current/candidate 数量对比；
3. 当前策略主线与隔离 historical lane 的流程视图，并标明“当前位置仍是现有 cited-query 主线”；
4. 五张 historical 卡片，展示原始状态、候选状态、DQ、requested/evaluated window、关键结果、
   limitations、candidate node/result/attribution IDs 与 exact source ref；
5. 状态图例，明确节点进展色、结果 display status、historical raw status 三者不同；
6. Owner review checklist：stable IDs、状态映射、结构数量、信息密度、阅读顺序、限制语言；
7. canonical 页面 path/bytes/SHA-256 只读 identity 与“本任务未修改”结论。

preview 必须为无脚本、无表单、无外部资源的静态 HTML；支持宽屏和窄屏，键盘阅读顺序清晰，
不得依赖 HTTP、browser session、LLM、network 或 write-capable consumer。

## 5. 输出与实现计划

canonical review artifacts 输出到：

```text
outputs/atlas/historical_projection_review/trading_2479_v1/index.html
outputs/atlas/historical_projection_review/trading_2479_v1/review_pack.json
outputs/atlas/historical_projection_review/trading_2479_v1/review_pack.md
outputs/atlas/historical_projection_review/trading_2479_v1/validation.json
```

计划 tracked paths：

```text
config/atlas/historical_projection_review.yaml
src/ai_trading_system/atlas/historical_projection_review.py
src/ai_trading_system/atlas/historical_projection_review_renderer.py
tests/atlas/test_historical_projection_review.py
src/ai_trading_system/atlas/__init__.py
docs/task_register.md
docs/requirements/TRADING-2479_Atlas_Historical_Projection_Review_Pack_V1.md
docs/system_flow.md
docs/artifact_catalog.md
registry/development_tasks_shadow/**
registry/development_tasks_shadow_v2/**
inputs/architecture/**
```

步骤：

### S0：登记与 governed lane

- 登记本 row/requirement，生成并验证 task shadow；
- 从 exact latest main 创建 `codex/trading-2479-atlas-historical-projection-review`；
- `SINGLE_LANE` START/LANE preflight PASS 后才允许实现写入。

### S1：Review contract 与 builder

- 配置冻结五个 source refs、candidate IDs、status mappings 与 expected counts；
- builder 从 exact typed adapter records 和 current snapshot 生成 review model；
- exact source set、ID collision、counts、status policy、安全 flags、primary start 全部 fail closed；
- 不调用 live snapshot projection 或 cited-query writer。

### S2：Independent preview 与验证

- 生成 deterministic JSON/Markdown/HTML/validation；
- double-build byte-identical；
- tamper、missing/duplicate source、ID collision、status mapping drift、canonical identity drift fail closed；
- static DOM 验证 no script/form/external resource、responsive contract 与五卡完整性。

### S3：Owner review 与 closeout

- 先交付独立 preview 供 Owner 手工视觉/语义验收；
- 本地 `file://` browser automation 若仍被 URL policy 阻止，显式记录
  `NOT_EXECUTED_URL_POLICY`，不得绕过或伪报 PASS；
- Owner 验收只关闭 review pack，不授权 canonical projection；
- 完成 focused/generated/compatibility/formal gates、ff-only main、ordinary push 与 cleanup。

## 6. 验收标准

1. review 输入 exact 五份 typed records，roadmap 与 known-unrelated path 零读取；
2. current counts 精确为 `13/21/22/8/12`，candidate counts 精确为 `13/27/28/13/17`；
3. 六个 node IDs、六条 `CONTAINS` edges、五个 result IDs、五个 `NEUTRAL` attributions 无 collision；
4. 五张卡片同时展示 original raw、proposed raw、display status 与显式 mapping rationale；
5. 四个 proposed raw `PASS`、program snapshot proposed raw `LIMITED`，全部 display `LIMITED`；
6. DQ receipt/window distinction 原样保留；缺失 DQ 显式为 `null/未提供`，不得补造；
7. primary default 仍为 `2021-02-22`；legacy `2022-12-01` 只作 historical window；
8. canonical cited-query HTML path/bytes/SHA 在 review build 前后一致，current snapshot 未投影；
9. JSON/Markdown/HTML/validation deterministic，writer double-build byte-identical，tamper fail closed；
10. HTML 无 script/form/external resources，宽窄屏静态检查与 Owner manual review 显式记录；
11. focused/generated/compatibility/formal gates PASS；
12. `page_projection_performed=false`、`result_projection_performed=false`、
    `investment_conclusion_generated=false`、`production_effect=none`、`broker_action=none`。

## 7. Stop conditions 与后续边界

- 任一 source/blob/typed-record identity drift：停止并回到 source registration 复核；
- candidate ID 与 current snapshot 冲突：停止，不自动重命名；
- status mapping 需要表达 current investment verdict：停止并另立 reviewed policy；
- canonical page 或 current snapshot 在本任务中发生变化：停止并恢复 review-only boundary；
- 需要实际 node/result/attribution/page projection：必须另立后续任务并取得新的 Owner token；
- browser URL policy 阻止本地自动化：保留人工验收，不切换 browser 或启动临时 HTTP workaround。

## 8. 生命周期与安全边界

- governed mode：`SINGLE_LANE`；
- registration base：`8a91a61711bb0d924a033a1049769b76b8969322`；
- planned branch：`codex/trading-2479-atlas-historical-projection-review`；
- implementation workspace：`D:/Work/AITradingSystem_trading2479_review`；purpose 为在独立
  task branch 构建 review-only artifacts，owner 为 TRADING-2479 coordinator；
- exit condition：review pack 验收、验证、ff-only main、ordinary push、canonical evidence handoff、
  worktree/branch audit cleanup 完成；
- review artifacts 为 retained governed evidence，可从 Git 与 deterministic builder 恢复；
- `production_effect=none`、`broker_action=none`。

## 9. 进度记录

- 2026-08-02：Owner 要求继续推进 Atlas 后续任务；根据 TRADING-2477 的显式 stop condition，先建立
  independent historical projection review pack，不直接激活 canonical projection。
- 2026-08-02：等待 OPS-073 与 TRADING-2478 按约定串行完成 shared main integration；TRADING-2478
  已普通 push，exact latest main=`8a91a61711bb0d924a033a1049769b76b8969322`，Atlas 现在独占
  shared task shadow/register 写入边界。
- 2026-08-02：READ_ONLY preflight PASS；main worktree audit PASS，`local main=origin/main`，无 active
  lease。原冻结任务 checkout 保持 clean 历史分支，不参与本任务写入。
- 2026-08-02：task registry generate/validate PASS，V1/V2 均为 958 tasks、byte-identical；focused
  task-shadow tests=`13 passed`，registration checkpoint=`b26a71c8973420674a2edf7656ea57c957968c5f`；
  lifecycle detail checkpoint=`276e896bffb2b775cd023d48afc366b5e9a362c5`。
