# TRADING-2523：Atlas 面向读者的术语首现解释合同 V1

最后更新：2026-08-15

- stable task id：`TRADING-2523_ATLAS_READER_FACING_TERMINOLOGY_FIRST_USE_CONTRACT_V1`
- priority：`P1`
- status：`DONE`（canonical terminal event 不可逆；尚未闭合的 base-drift integration 与最终门由 corrective successor `TRADING-2523A` 承接）
- governed mode：`SINGLE_LANE`
- contract change：`true`（consumer-visible reader contract；不改变策略、DQ/PIT、研究窗口或交易合同）
- exact registration base：`f876ec853c1431e760bc4cf5b89123265a32080f`
- predecessors：`TRADING-2505`、`TRADING-2506`、`TRADING-2508`、`TRADING-2509`、`TRADING-2522`
- production effect：`none`
- broker action：`none`

## 1. 用户目标与已确认缺口

Project Owner 要求继续提升策略研究页面的可读性，并确认页面中出现的名字、缩写、状态码、
模块名和研究概念是否都有明确易懂的解释。针对 exact commit
`f876ec853c1431e760bc4cf5b89123265a32080f` 的页面 authority 与渲染器复核表明：现有八阶段
reader-first 说明和 13 个 concept card 已经改善流程理解，但不能证明“所有面向读者的术语均已
解释”。页面仍直接暴露 `KEEP_CLOSED`、`PREREGISTRATION_ONLY`、`G2/G3`、`DQ/PIT`、
`primary-window`、`10-series collector`、`capability GO`、`strategy PASS`、`lineage`、
`canonical`、`validator`、`B0/B1–B4/B5/B6`、`bounded pilot`、`ingest`、`synthetic` 以及内部
stage/source/task identifiers；这些词并未全部在首次出现之前或当次提供通俗说明。

2523 冻结时另有一项 authority 不一致需要处理：当时 2522 只记录
`OWNER_V4_AUTHORIZATION_ADMITTED_UNUSED`，而 Atlas effectiveness source 仍写“exact v4 token 未提供”。
随后 2522 已完成唯一 Cloud run，current state 变为
`OWNER_V4_AUTHORIZATION_CONSUMED_RESULT_INVALID_INCOMPLETE`；1201/1201 个 chain session 被组合
transport gate 拒绝，evidence=`FAIL`、DQ/PIT=`NOT_EVALUATED`，且禁止第二次 run。2523A 必须投影该
latest-main 事实。旧 HTML/sidecar 的 source commit 和人工验收事实也不能自动迁移到新 HTML；页面不得
在这些身份未闭合时宣称 `CURRENT` 或复用旧页面视觉验收。

## 2. 冻结目标

建立一个可重放、fail-closed 的面向读者术语合同，使以下命题能够由 authority 与测试证明：

1. canonical 页面所有可达 reader-facing 文本均进入 deterministic inventory；
2. 每个需要解释的术语都被明确分类，并在首次出现之前或当次获得可访问的通俗解释；
3. 页面默认层不再以内部 identifier 代替读者语言，原始标识仍在审计层一对一保留；
4. 工程、研究、页面验收、Owner decision 与交易授权继续严格分轨，解释不得把工程 PASS 漂白为
   策略 PASS；
5. 新 HTML 的 source commit、manifest、sidecar、term inventory 和人工验收事实使用同一身份边界。

本任务不使用黑盒“可读性分数”或未经评审的通过阈值。验收依据是完整性、首现顺序、可访问性、
identity replay 与独立人工事实，而不是阅读时长、点击率或主观打分。

## 3. 分步合同

### W0：authority 与 publication identity 对账

- 将 2522 纳入 reviewed successor coverage，并按 current canonical registry/requirement 投影为
  `OWNER_V4_AUTHORIZATION_CONSUMED_RESULT_INVALID_INCOMPLETE`；
- 删除“token 未提供”“token 已 admitted-unused”与“授权已消费”在同一 current 页面同时成立的冲突；
- 新 HTML 必须使 `OWNER_VISUAL_REVIEW` 与 `READER_COMPREHENSION_REVIEW` 重新进入
  `PENDING_REVIEW`，除非存在明确绑定该新 HTML SHA-256 的独立人工 attestation；
- freshness 仍由 source/task/rendered identity 推导，不能通过手工改状态绕过。

### W1：读者画像与术语分类 authority

新增 reviewed `reader_profile` 与 `reader_terminology` authority。V1 目标读者是：了解基本投资语境、
但不要求熟悉本项目内部架构、状态码、QuantConnect transport 或 task naming 的研究页面读者。

每个候选术语必须属于且只属于以下一类：

- `COMMON_LANGUAGE`：目标读者无需项目专有说明即可理解；
- `INLINE_EXPLANATION`：首次出现处直接给出短说明；
- `ACCESSIBLE_DISCLOSURE`：通过可键盘进入、非 hover-only 的 tooltip/details/concept card 说明；
- `GLOSSARY`：在首次出现处具有明确可访问链接，并在 glossary 中解释；
- `AUDIT_ONLY`：默认读者层不展示，只在审计层保留原始 identifier；
- `PROHIBITED_UNEXPLAINED`：未知、拼写/大小写漂移或运行时拼装出的未登记术语，渲染必须失败。

### W2：完整 rendered-text inventory 与首现映射

inventory 至少覆盖：常规 DOM 文本、标题/摘要、默认折叠内容、可达 `details` 内容、链接与控件标签、
`aria-label`、`aria-describedby`、`title`、tooltip 文本及 glossary/concept card。每条记录绑定：

- exact HTML SHA-256；
- DOM locator 与稳定 sequence ordinal；
- interaction state（默认、展开、目标锚点或辅助说明）；
- normalized visible text 与发现的 term id；
- explanation locator、explanation kind 与首现先后关系。

隐藏 template、script、style、机器 sidecar 和不可达 DOM 不进入 reader-facing universe，但必须由规则
显式排除，不能静默漏掉。source/concept graph closure 与 rendered universe completeness 是两项独立
校验，不得互相代替。

### W3：renderer enforcement 与 progressive disclosure

- renderer 只从 typed authority 投影解释；不得以 ad hoc HTML string 修补单个词；
- reader-facing 首层优先使用中文目的/动作/边界，`KEEP_CLOSED`、task id、source id、schema、hash、
  locator 等内部标识默认折叠为审计详情；
- 保留英文 ticker、正式 status/contract identifiers 时，同时提供中文显示名或当次解释；
- 同义词、复数、大小写和标点变体由显式 alias 处理；未登记变体 fail closed；
- glossary/concept/tooltip 不能只依赖鼠标悬停，键盘与辅助技术必须可达；
- 所有 raw identifiers 在审计层仍与 reader-facing label 一对一绑定，禁止丢失可审计性。

### W4：验证与独立人工验收

- unit/property/golden 覆盖 inventory determinism、全量分类、首现顺序、unknown/alias/case/runtime
  composed negative cases、audit binding、cycle/unknown reference 与 exact replay；
- desktop/mobile loopback 浏览器复核默认层、展开层、键盘、锚点、ARIA、tooltip/details、overflow、
  console 与 screenshot；
- engineering validation 可由自动化签署；Owner visual 与 reader comprehension 必须由真实人工事实
  独立签署，并绑定新 HTML identity；
- 更新 `docs/system_flow.md`、artifact catalog、architecture/compatibility/generated authority 与
  canonical task projection；final latest-main tree 完成正式门禁后普通 non-force push。

浏览器自动化不得与 pytest formal/heavyweight gates 并发。

## 4. Path claims

Task-owned：

- `docs/requirements/TRADING-2523_Atlas_Reader_Facing_Terminology_First_Use_Contract_V1.md`；
- `config/atlas/reader_profile.yaml`；
- `config/atlas/reader_terminology.yaml`；
- `src/ai_trading_system/contracts/strategy_research_reader_terminology.py`；
- `src/ai_trading_system/atlas/rendered_term_inventory.py`；
- `src/ai_trading_system/atlas/reader_terminology_projection.py`；
- `tests/atlas/test_rendered_term_inventory.py`；
- `tests/atlas/test_reader_terminology_projection.py`。

Coordinator-owned：

- canonical task registry/index 与 generated task views；
- `src/ai_trading_system/atlas/cited_query_renderer.py`、package exports 与相关 renderer tests；
- `config/atlas/page_effectiveness.yaml`、source/acceptance/status bindings；
- canonical Atlas HTML/manifest/validation/work-progress/inventory sidecars；
- `docs/system_flow.md`、`docs/artifact_catalog.md`；
- architecture/module/test/deprecation manifests、compatibility authority 与 generated views。

## 5. 验收标准

1. 页面 reader-facing universe 有 deterministic 全量 inventory；primary、secondary、可达 disclosure、
   ARIA/title/tooltip 文本均有规则覆盖或显式排除。
2. 每个发现的术语 exact-once 分类；未知术语、大小写/别名漂移、重复 term id 或无法解析的解释目标
   fail closed。
3. `INLINE_EXPLANATION` 在同一首次出现处解释；其他 reader-facing 分类在首次出现处已有可访问解释
   入口；解释晚于首次出现必须失败。
4. 内部 task/source/stage/status identifiers 默认进入审计层；reader-facing 中文标签与 raw identifier
   保持一对一可重放绑定。
5. 页面不再把 v4 token 陈述为“未提供”或“admitted-unused”；current 页面只陈述
   `OWNER_V4_AUTHORIZATION_CONSUMED_RESULT_INVALID_INCOMPLETE`，并保持 2522 coverage、source commit、
   HTML、manifest、validation 和 inventory identity 一致。
6. `CAPABILITY_PROGRESS`、`LATEST_EXECUTION_STATUS`、`RESEARCH_EFFECT`、页面三轨验收与策略结论继续
   独立；`data-strategy-conclusion="PASS"` 保持为零，研究窗口继续从 `2021-02-22` 开始。
7. 键盘与辅助技术可到达全部解释入口；无 hover-only explanation，desktop/mobile 无关键水平溢出、
   遮挡、截断或不可达控件。
8. 新 HTML 不复用旧人工 PASS；本任务交付时 engineering 可自动验证，Owner visual/reader
   comprehension 保持 `PENDING_REVIEW`，直到 Project Owner 对 exact HTML 独立确认。
9. focused、compatibility/deprecation、Architecture、Contract、Integration、Reproducibility、exclusive
   Full 与 governed closeout 全部通过；local main 与 origin/main 最终一致。

## 6. 开放问题与退出条件

- 若术语需要解释新的投资阈值、策略判断或 Owner policy value，停止本任务并建立最小 reviewed policy
  wave；本任务不得自行创造答案。
- 若页面文本由运行时数据动态组成，必须先把其词汇/模板纳入 typed authority；不能以 allowlist wildcard
  绕过 completeness。
- 若浏览器无法通过 loopback HTTP 重放，engineering acceptance 保持未通过，不以 `file://` 截图代替。
- 若 current main 在 lane 期间前进，按 governed integration revalidation plan 处理，不自动 rebase/merge。

## 7. 进度记录

- 2026-08-15：Project Owner 授权继续推进可读性改造；任务按 exact main
  `f876ec853c1431e760bc4cf5b89123265a32080f` 建立。初始状态仅表示合同波开始，尚未修改 renderer、
  生成页面或签署任何人工验收事实。
- 2026-08-15：Project Owner 要求把外部可读性评审建议追加到后续计划。2523 继续严格限定为
  reader identity、术语分类、首现解释与 audit-only 边界；页面主路径、状态/日期语义、accessibility
  与 exact-HTML 人类理解验收分别登记为 `TRADING-2524` 至 `TRADING-2527` 的 `PROPOSED`
  follow-on，不在本任务中顺带实施。
- 2026-08-15：Project Owner 确认按优化后的并行拓扑推进后续任务链。2523 仍须先独立闭合；在其
  final validated commit 进入 local `main` 前，不创建 2524--2527 的实现 branch/worktree。2523
  关闭后，2524 先执行最小串行 `S0 reader_projection_contract.v1`，再从该 exact local-main commit
  启动 2525 engineering lane 与 2526/2527-A strategy-evidence lane。
- 2026-08-15：实现 reader profile、50 项术语 authority、raw-to-reader typed projection、完整 rendered
  surface/audit identifier inventory 与 fail-closed 首现检查；renderer 默认层改用中文读者表达，raw
  identifiers 保留在 glossary 定义或审计层。focused tests 为 `38 passed`；desktop/mobile loopback、
  keyboard、anchor、ARIA、disclosure、overflow 与 console 的工程复核已通过，Owner visual 与 reader
  comprehension 仍为 `PENDING_REVIEW`，且明确由后续 2527 exact-HTML pilot 承担。
- 2026-08-15：正式前置门通过：Architecture `865 passed`、Contract `276 passed`、Integration
  `995 passed`、Reproducibility `24 passed`。任务投影进入 `DONE` 以冻结最终候选；只有重新生成后的
  exact HTML/sidecars 完成 browser replay、exclusive Full、governed closeout 与普通 non-force push，
  该候选才可进入 local/remote `main`。任何最终门失败都必须追加纠正事件并重新打开任务。
- 2026-08-15：冻结 lane 上的完整 Full 为 `9024 passed, 2 failed, 3 skipped`；两条失败均为 Wave14/15
  carrier 检查发现 `main = origin/main = b20757326430152a1f3340cf5871773595194a8b` 已推进，而 lane
  `HEAD = f876ec853c1431e760bc4cf5b89123265a32080f` 仍停留在约定 frozen base。现行 canonical schema
  禁止 terminal task 重新打开，因此保留 2523 的 immutable `DONE` 历史，并登记 corrective successor
  `TRADING-2523A_ATLAS_READER_TERMINOLOGY_INTEGRATION_CORRECTION_V1` 为 `IN_PROGRESS`；该 successor
  不修改 carrier 测试，也不自动 rebase，只承接 `integration_revalidation_plan.v1`、唯一 latest-main
  coordinator candidate、exact-HTML identity 重建、browser replay、exclusive Full 与 closeout。
- 2026-08-15：为避免共享主检出区被并行任务切回 `main` 后丢失未提交候选，2523A 使用独立受治理
  worktree `D:\Work\AITradingSystem_trading2523a_integration` 保存旧协调候选。该 worktree 在新候选
  发布、证据迁入 canonical 位置且无进程依赖前保持原地只读；满足退出条件后才以
  `git worktree remove` 清理。
- 2026-08-15：2523B 已从 exact main `354bc020532584bf50b50f801322ed65684f5e2e` 发布
  page-effectiveness v2。对 frozen base `f876ec853c1431e760bc4cf5b89123265a32080f`、lane head
  `b8c71e0610c42e3fcb8fb8ba6872876e28bcc45a` 与该 latest main 重放的新 plan 为
  `integration-revalidation-df785dba6d283bd1f1c6`（validator `PASS`）；typed decision 为
  `RECONCILIATION_REQUIRED`，contract conflict 为零，剩余 18 项 domain overlap 与 8 项
  coordinator refresh。Project Owner 已授权继续 2523A；coordinator 只处理 plan 列明 overlap，
  page-effectiveness 保留 v2，generated/shared authority 在唯一 final tree 上重建一次。
- 2026-08-15：reviewed overlap 已在 latest-main candidate 完成，保留 page-effectiveness v2、49 项
  successor coverage 与 2522/2528 current fact。术语 authority 维持 50 项，并把 v4 状态纠正为
  “授权已消费，运行结果无效且不完整”，避免把单次 token 误读为可复用授权或把失败运行误读为
  有效策略证据。最终 focused 并行回归为 `99 passed in 130.31s`；provisional exact-page replay 已通过
  desktop/mobile reflow、keyboard disclosure、anchor、ARIA 与 console 工程检查，但因页面仍绑定
  pre-commit source identity，该次 replay 仅作为预检查而非 promotion evidence。Owner visual 与
  reader comprehension 继续为 `PENDING_REVIEW`，不会由自动化代签；提交候选后须按 exact commit/SHA
  重新生成页面、inventory 与浏览器证据，再进入正式门禁。
- 2026-08-15：一次 canonical progress update 的 notes 未保留 Markdown requirement link，导致 2523A
  `task_record.requirement_refs` 被投影为空并触发 page-effectiveness fail-closed；并行回归如实记录为
  `21 failed, 78 passed in 89.34s`。已通过新的 append-only corrective event 恢复 requirement binding，
  不以串行重跑掩盖失败；后续须重建派生 authority 与 provisional page 后再用同一并行集合验证。

## 8. 后续可读性路线图

以下任务均为后续计划，不表示已开始实现或已通过 Owner 验收：

1. [`TRADING-2524_ATLAS_READER_DECISION_PATH_AND_PROGRESSIVE_DISCLOSURE_V1`](TRADING-2524_Atlas_Reader_Decision_Path_And_Progressive_Disclosure_V1.md)：
   重排 reader-first 默认路径，并把页面内 disclosure 限制为 reader default 与 research drilldown 两级；
2. [`TRADING-2525_ATLAS_CHANGE_EVIDENCE_DATE_AND_STATE_SEMANTICS_V1`](TRADING-2525_Atlas_Change_Evidence_Date_And_State_Semantics_V1.md)：
   统一对象限定的状态、日期与 snapshot change 语义；
3. [`TRADING-2526_ATLAS_ACCESSIBLE_RESEARCH_DRILLDOWN_AND_AUDIT_LINKAGE_V1`](TRADING-2526_Atlas_Accessible_Research_Drilldown_And_Audit_Linkage_V1.md)：
   移除嵌套 disclosure，完成键盘、screen reader、reflow 与 audit destination；
4. [`TRADING-2527_ATLAS_HUMAN_COMPREHENSION_ACCEPTANCE_PILOT_V1`](TRADING-2527_Atlas_Human_Comprehension_Acceptance_Pilot_V1.md)：
   对冻结的 exact HTML 运行受治理的人类理解验收 pilot。

后续执行拓扑固定为：

`2523 -> 2523A corrective integration closeout -> 2524-S0 shared reader projection contract ->
[2525 engineering || 2526-A accessibility evidence || 2527-A protocol preparation] ->
2524 coordinator integration -> 2526-B final AT/mobile validation ->
2527-B exact-HTML human pilot`。

其中 2524 是 `DUAL_LANE` coordinator/integration scope，不新增只用于集成的 2528。2525 是
engineering lane；2526-A 与 2527-A 共用 strategy-evidence lane，但必须保持各自 task-owned path 与
evidence lineage。shared renderer、page config、generated HTML/manifest/sidecar、task registry、
`docs/system_flow.md`、catalog 与 formal validation artifacts 始终由 coordinator 单写。

硬串行门包括：2523 identity/terminology 闭合、2524-S0 consumer-visible contract 冻结、coordinator
生成唯一 final exact HTML、2526-B final candidate 的 browser/AT 验收，以及 2527-B 真实人类测试与
Owner attestation。heavyweight Full 只在最终 integration candidate 运行。任何 lane 发现需要改变共享
schema、public contract、DQ/PIT、cache identity、研究语义或 reader-facing enum，必须停止并回到最小
reviewed serial contract wave，不得在 worker branch 内隐式扩张合同。
