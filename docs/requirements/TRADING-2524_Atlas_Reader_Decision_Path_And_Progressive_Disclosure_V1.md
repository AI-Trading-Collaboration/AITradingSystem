# TRADING-2524：Atlas 读者决策路径与渐进披露 V1

最后更新：2026-08-16

- stable task id：`TRADING-2524_ATLAS_READER_DECISION_PATH_AND_PROGRESSIVE_DISCLOSURE_V1`
- priority：`P1`
- status：`IN_PROGRESS`（S0 serial contract wave）
- proposed governed mode：`DUAL_LANE` coordinator/integration scope
- contract change：`true`（consumer-visible reader contract）
- predecessor：`TRADING-2523_ATLAS_READER_FACING_TERMINOLOGY_FIRST_USE_CONTRACT_V1`
- production effect：`none`
- broker action：`none`

## 1. 问题与目标

当前 cited-query 页面把 `page_effectiveness`、`system_flow`、`qqq_options_projection` 和
`result_ledger` 放在五个 canonical reader questions 之前。测试能够证明问题与证据存在，却不能证明
普通读者在注意力耗尽前看到了答案。移动端仅完成无水平溢出也不能证明任务路径可用。

本任务建立最小、可回滚的 reader information architecture contract：先给结论边界、风险、阻塞与五个
问题的简答，再按需进入研究解释与审计证据；不删除 provenance，不复制 canonical facts，也不新建第二套
Reader Brief。

2026-08-16 的 Owner 复核进一步明确：页面不能要求读者先记住整张术语表，再进入研究叙事；页面必须
先解释“为什么要研究、为什么沿这条路径推进、每一步怎样改变下一步”，再展示“做了什么”。细节只有在
能说明主问题、约束、选择、证据、结果或下一步时才进入 reader layer；否则压缩到 research drilldown
或迁入 audit destination。术语解释改为在使用位置按需出现，集中 glossary 只保留索引和完整定义，不再
占据页面最前方。

Owner 的后续人工复核又发现：现有 why-first 虽然解释了 2522/2528 的局部诊断顺序，却仍从“研究重新开放”
这一中间状态起步，读者自然会追问“为什么此前关闭”。因此 reader entry 必须先回到原始需求：这套系统要
把策略想法变成可追溯、可复核且证据不足时会停止的决策过程。页面在任何局部 task 之前，必须先用简短
全局路径说明研究、证据门、人工决策与授权执行的关系，并解释 fail-closed 不是“已经证明策略无效”，而是
“尚无足够可信证据继续得出结论或进入执行”。随后才能把当前 per-axis 诊断定位为全局证据门中的一个局部
卡点；不得把局部任务编号和 transport 细节冒充系统级 why。

Web Pro exact-commit advisory 已提交到
`https://chatgpt.com/c/6a8092af-5a00-83e8-acde-0cb64554c925`，审阅快照固定为
`05e37edd42006d42f4736ddd4aa3797a12cf0f1f`。已取得且能与仓库事实交叉核对的建议是：当前 DOM 不是
why-first、集中 glossary 不能解决上下文内理解，现有 typed facts 可复用约束、影响和下一步，但缺少
`PROBLEM`、`CHOICE` 以及 source-bound causal edges。网页完整终稿仍保持 handoff，因此这些部分只作
advisory 输入；本 requirement、canonical task 与 executable guards 继续是实现 authority。

## 2. 冻结角色边界

- typed fact/evidence layer 是 task、snapshot、result、DQ/PIT、acceptance 与 lineage 的唯一事实源；
- Reader Brief 是唯一日常 reader entry，继续遵守 `conclusion -> evidence -> calculation -> links`；
- Atlas 是围绕具体研究问题的 reader-first 解释与 research drilldown；
- audit artifacts 保留 raw ID、hash、locator、receipt、manifest、sidecar 与 canonical bytes；
- reader、research、audit 可以有不同投影深度，但不得形成三份独立手写状态或复制 HTML 报告。

## 3. 推荐默认顺序

桌面与移动端共享同一 DOM/语义顺序：

1. 页面信任条与全局定向：先回答系统原始需求、`研究问题 -> 可信证据 -> 人工决策 -> 授权执行`
   四步关系、为什么 fail-closed、当前停在哪一步；随后显示 source commit、freshness、数据/证据/页面日期、三轴验收、
   `strategy conclusion pass count=0`、`production_effect=none`、`broker_action=none`；
2. reader overview：当前主线、最大 blocker、已有证据、不能推出什么、下一责任方与下一合法动作；
3. 五个 canonical questions 及默认可见的简答和核心限制；
4. 相对上一 snapshot 的 `CHANGED / UNCHANGED / UNKNOWN / STOPPED` 摘要；
5. 当前研究结论边界、QQQ aggregate 与关键停止原因；
6. engineering / research / page acceptance 三轴摘要；
7. 当前 system-flow 阶段、上一步与下一步；
8. QQQ projection、完整 result ledger、归因和计算的 research drilldown；
9. glossary index 与明确的 audit destinations。

页面级六个 reader questions 与五个 cited-query questions 必须有 typed 映射，不能表现为 11 个互不相关的
问题。移动端不得只把桌面多栏机械堆叠；视觉顺序必须与 DOM 顺序一致。

### 3.1 Why-first causal chain

局部 `PROBLEM -> CONSTRAINT -> CHOICE -> EVIDENCE -> RESULT -> NEXT_STEP` 之前必须先有一个不超过四步的
系统级定向，并默认可见地回答：

- 原始需求是什么，而不是当前 task 在做什么；
- 为什么系统设计为证据不足时保持研究关闭和 cash preservation；
- 当前关闭是“策略已被证明无效”还是“证据门尚未通过”；
- 重新开放至少还需要什么，以及当前页面只在处理其中哪个局部卡点。

这段定向必须复用现有 contract safety、readiness、primary-window evidence、external-action admission 和
当前 evidence 事实，不得另写一套状态。任务 ID、slot 数、transport axis 和 raw reason code 不进入系统级
定向；它们只能在后续局部链或 drilldown 中出现。

L0 必须由同一 typed projection 给出以下有序语义，不允许 renderer 从相邻文案猜测联系：

`PROBLEM -> CONSTRAINT -> CHOICE -> EVIDENCE -> RESULT -> NEXT_STEP`

- `PROBLEM`：当前真正要回答的研究问题；
- `CONSTRAINT`：为什么不能直接跳到策略结论、回测或下一阶段；
- `CHOICE`：在约束下选择这条研究路径的理由，以及明确未选择什么；
- `EVIDENCE`：该选择要求什么证据、当前已有什么、缺什么；
- `RESULT`：证据目前支持的最窄结论与禁止推断；
- `NEXT_STEP`：由当前结果触发的下一责任方、下一合法动作与重新进入条件。

每个 node 和 edge 都必须绑定 canonical source；缺少 `PROBLEM`、`CHOICE` 或 source edge 时返回 typed
`INSUFFICIENT`，不得用顺滑文案补齐。稳定 edge vocabulary 至少包含
`BOUNDED_BY`、`JUSTIFIES`、`REQUIRES_EVIDENCE`、`SUPPORTS`、`LIMITS` 与 `TRIGGERS`。

## 4. Progressive disclosure 合同

- L0 `READER_DEFAULT`：答案、边界、风险、日期、变化、停止原因与下一动作；
- L1 `RESEARCH_DRILLDOWN`：节点解释、结果、归因、比较与计算；
- `AUDIT_STRATUM`：通过独立 anchor、sidecar 或 audit artifact 到达，不再嵌套于 L1 disclosure；
- 页面内交互深度不得超过两级，不允许 `<details>` 嵌套 `<details>`；
- 每张 reader card 最多一个主要 disclosure，summary 必须说明将展开什么；
- 策略无效性、关键风险和下一合法动作不得因 disclosure 关闭而消失。

### 4.1 Inline terminology interaction

- governed reader term 在其所在 reader section 的首次出现必须形成可聚焦 inline trigger；hover、keyboard
  focus 与 touch/tap 打开同一短定义，Escape 或失焦关闭并恢复上下文；
- 同一 section 的重复出现可以避免新增 tab stop，但 hover/tap 仍须到达同一说明，screen reader 仍能取得
  等价 accessible description；
- trigger 不得使用裸 `title` 作为唯一信息源，也不得嵌套交互控件；短说明之外提供“完整定义” glossary
  anchor，但 glossary 不得抢在 why-first reader entry 之前；
- 长定义、带多条 lineage 的说明和 raw identifier 不进入 tooltip：前两者链接 glossary/L1，后者只进入
  `AUDIT_STRATUM`；
- 术语交互失败只能回退到可见 inline 定义或 glossary link，不能回退为 hover-only。

### 4.2 Reader attention budget

- 首屏 L0 只保留 trust strip、一个主问题、一条 why chain 摘要、最窄结论边界、最大 blocker、禁止推断
  与下一合法动作；
- 每张 L0 card 只表达一个 reader decision，标题必须是问题或因果作用，不使用模块名作为主要标题；
- L1 每张 card 最多一个主要 disclosure，summary 明确“展开后能回答什么”；
- raw ID、hash、locator、receipt、manifest、sidecar 和完整 ledger 不计入 L0/L1 信息预算，只通过独立
  audit destination 到达。

该设计参考 Shneiderman overview-first / details-on-demand、NN/g Progressive Disclosure、SEC Plain
English、Google PAIR explainability/trust、FCA consumer understanding 与 W3C clear-content/accessibility
方法，但仓库 authority、typed facts 与真实验收结果始终优先。

## 5. Owner 决策、允许动作与禁止动作

必须由 Owner 冻结：

- 默认读者与 Owner/researcher/operator/auditor 是否共用入口；
- Reader Brief、Atlas 与 artifacts 的 canonical role boundary；
- always-visible 的风险、日期、验收与授权字段；
- 使用单页渐进展开还是同一事实源的角色投影；
- 桌面与移动端的首要读者任务。

允许：section order、compact summary、anchor、disclosure boundary、mobile linear flow、typed
source-to-projection mapping 与可回滚 CSS/HTML。

禁止：第二套 Reader Brief、复制 canonical facts、修改研究结果、研究窗口、DQ/PIT、状态 enum、投资解释
阈值、production 或 broker 行为。

## 6. 预期制品与验证

预期制品：IA contract、desktop/mobile order、L0/L1/audit mapping、六问到五问 mapping、before/after
exact HTML、source coverage map、rollback plan 与独立人工 review plan。

验证至少覆盖：source/semantic replay、section/DOM order、关键风险默认可见、desktop/mobile visual、
keyboard、screen-reader smoke、no nested disclosure、audit evidence 可达与基线/候选定性 comprehension。

## 7. 分阶段并行拓扑

### W0：前置串行门

- 2523 必须先形成 validated final commit 并进入 local `main`；在此之前不创建本任务或 2525--2527
  的实现 branch/worktree；
- coordinator 复核 reader identity、terminology、publication identity 与现有页面 source binding；
- 若 2523 closeout 后 local `main` 又发生 consumer-semantic drift，先生成 governed
  `integration_revalidation_plan.v1`，不得让 lane 从不一致 base 启动。

### S0：最小 serial contract wave

coordinator 先冻结 `reader_projection_contract.v1`，至少明确：

- section slots、DOM/视觉顺序与 always-visible fields；
- 页面级六问到五个 cited-query questions 的 typed mapping；
- L0 `READER_DEFAULT`、L1 `RESEARCH_DRILLDOWN` 与 `AUDIT_STRATUM` 边界；
- state projection、date/change、accessibility semantics 的稳定接口，而非具体实现；
- canonical source binding、raw identifier 对应关系与 exact identity/replay 规则；
- worker 可消费的 fixtures、error types 与 coordinator remediation handoff 格式。

本轮 S0 还必须冻结：

- 上述六类 causal nodes、稳定 edge vocabulary、全链 source binding 与 `INSUFFICIENT` 处置；
- inline term 的 hover/focus/tap、tab-stop 去重、Escape/focus restoration、长定义与 raw identifier 路由；
- 首屏 attention budget、L0/L1/AUDIT 内容预算，以及 glossary 必须位于 reader mainline 之后；
- 任何人类理解、策略有效性、production 或 broker 状态都不能由 contract validator 自动升级。

S0 是 consumer-visible contract，必须由 coordinator 串行实现、review、focused validate、提交并进入
local `main`。只有此后才冻结一个新的 exact common base，启动两条 lane；不得把未提交的 S0 working
tree 当作共享合同。

### P1：同 base 双线并行

- engineering lane：执行 2525 的 typed state/date/change config、module 与 focused tests；
- strategy-evidence lane：执行 2526-A 的 accessibility validator/harness/evidence，以及 2527-A 的
  protocol/schema/scenario/truth-rubric 准备；
- 两条 lane 从 S0 后同一个 exact local-main commit 创建独立 branch/worktree，声明互斥的 path、module、
  runtime resource、public-contract 与 evidence-lineage claims；
- lane worker 只运行 focused/impact validation，不运行或争抢 heavyweight Full。

### I0：coordinator integration

coordinator 从 frozen common base（如存在兼容 base drift，则从 governed plan 批准的 latest-main）形成唯一
integration candidate，固定吸收顺序为：

`S0 contract -> 2525 state/date domain -> 2526/2527-A evidence tools -> shared renderer/page wiring ->
generated HTML/manifest/sidecars -> shared docs/registries -> formal validation`。

coordinator 只生成一次 final exact HTML，并据此执行 2526-B final browser/AT/mobile 验收。通过后才可用
同一 HTML identity 进入 2527-B human pilot。若 renderer remediation 改变 HTML bytes，2526-B 必须重跑，
已经执行的 2527-B 事实不得跨 identity 复用。

## 8. Path claims 与单写边界

2524 coordinator-owned：

- `docs/requirements/TRADING-2524_Atlas_Reader_Decision_Path_And_Progressive_Disclosure_V1.md` 以及集成时
  2525--2527 supporting requirements 的状态/进度更新；
- `config/atlas/reader_projection_contract.yaml`；
- `src/ai_trading_system/contracts/strategy_research_reader_projection.py`；
- `tests/atlas/test_reader_projection_contract.py`；
- `src/ai_trading_system/atlas/cited_query_renderer.py`、package exports、shared renderer tests 与
  `config/atlas/page_effectiveness.yaml`；
- canonical Atlas HTML、manifest、validation、work-progress 与 inventory/acceptance sidecars；
- canonical task registry/index、generated task views、`docs/system_flow.md`、`docs/artifact_catalog.md`、
  architecture/module/test/deprecation manifests、compatibility authority 与 formal validation artifacts。
- `tests/test_devx_006d_report_catalog_flow_authority.py` 的 exact system-flow seal 回归值；该随动只允许绑定
  本任务审阅后的 `docs/system_flow.md` bytes，不得放宽 byte-parity 或 source-seal 检查。
- `tests/test_arch_004g_deprecation.py` 的 current inventory identity 与 module/test exact count；仅允许反映
  本任务新增一个 contract module 和一个 focused test file，不改变任何 removal gate。

worker 禁止写入上述 coordinator paths，也不得自行生成“候选 final”HTML。若 2525/2526 需要 shared DOM、
CSS 或 page config remediation，提交 typed failing evidence 与 proposed behavior，由 coordinator 在 I0
一次性实现和回归。

启动 preflight 时必须把实际路径与本节对账；新增共享路径默认归 coordinator，除非先更新 requirement、
canonical task 和 path claims 并重新 preflight。

## 9. Exit、falsification 与 downstream gate

Exit criteria：五个问题位于重型模块之前；答案默认可见；页面内 disclosure 不超过两级；风险与禁止推断
始终可见；所有 reader claims 保持 canonical source binding；audit evidence 完整可追踪。

STOP CONDITION：任一可见 claim 失去 canonical source binding，或策略边界/风险被移入折叠层，立即停止；
不得通过复制事实或放宽验证继续。

2523 闭合后只允许进入本任务 S0；S0 经过 review、focused validation、commit 与 local-main integration
后，即可并行启动 2525、2526-A 与 2527-A，不要求先完成整个 2524。2524 的最终退出仍要求 coordinator
integration、唯一 exact HTML、2526-B final candidate 验收与适用 formal gates 全部完成。

## 10. 进度记录

- 2026-08-15：根据 Project Owner 要求，将外部可读性评审建议登记为后续计划。当前仅为
  `PROPOSED`，未修改 renderer、页面、研究语义或验收状态。
- 2026-08-15：Project Owner 确认采用“2524-S0 串行合同 + engineering/strategy-evidence 双线 +
  coordinator 单次集成 + final AT/human 串行验收”。本任务升级为后续 `DUAL_LANE`
  coordinator/integration scope；状态仍为 `PROPOSED`，2523 未闭合前不启动实现或 worktree。
- 2026-08-16：2523A 已在 exact main
  `05e37edd42006d42f4736ddd4aa3797a12cf0f1f` 闭合。Owner 新反馈冻结 why-first、上下文内术语解释、
  主次分层和流程因果联系，并授权提交 Web Pro exact-commit advisory 后推进实现。READ_ONLY preflight
  `PASS`；2524 DUAL_LANE START 得到预期 `SERIAL_CONTRACT_WAVE_REQUIRED`；随后从该 exact main 创建
  `codex/trading-2524-reader-projection-contract`，S0 SINGLE_LANE preflight `PASS`，任务进入
  `IN_PROGRESS`。当前只允许 S0 contract/config/tests/authority mutation；renderer、final HTML、2525、
  2526 与 2527-A 仍不得从未提交合同启动。
- 2026-08-16：S0 已实现 `reader_projection_contract.v1`、typed Python contract、package export、模块片段与
  fail-closed tests；单文件 focused=`14 passed`。首次跨层 focused=`56 passed / 2 failed`，分别准确暴露
  system-flow lossless shadow 测试仍绑定旧 SHA，以及最后代码格式修正后的 architecture manifest stale；
  该结果仅作 failure-fix evidence。扩展 coordinator claim 后更新 exact system-flow seal、重建 report/
  catalog/flow shadow、architecture manifest 与 compatibility authority，修正后同一四文件并行回归=
  `58 passed in 47.78s`；Ruff、strict mypy、task-source、architecture fitness、report-flow authority 与
  compatibility authority 均 PASS。当前仍未修改 renderer 或生成新 HTML，也未自动升级任何人工、策略、
  production 或 broker 状态。
- 2026-08-16：S0 final commit=`ece8d97373c1a8a70949aa0ae445b79593ee09b3`；Architecture 首轮因新增
  module/test 尚未刷新 deprecation inventory 而得到 `864 passed / 1 failed`，修正 inventory identity 与
  exact counts 后 targeted=`1 passed`、Architecture=`865 passed`、Contract=`276 passed`。该 commit 已
  ff-only 进入 local `main` 并 ordinary push，`HEAD=local main=origin/main`。随后 DUAL_LANE START 对 2525
  engineering 与 2526/2527-A strategy-evidence 的互斥 claims 得到 `PASS`，coordinator branch 固定为
  `codex/trading-2524-reader-ia-integration`；所有 downstream lane 必须从该 released exact base 启动。
- 2026-08-16：coordinator 已按固定顺序吸收 2525 lane commit `a5183f745` 与 2526/2527-A evidence
  commit `2f8f24031`，并把 renderer 重排为九段 why-first 主线：先显示问题、约束、选择、证据、结论与
  下一步，再由一个可访问的 research drilldown 展开八阶段流程、QQQ 投影和结果账本；glossary 与完整
  task/source identity 位于主线之后。术语首次出现支持 hover/focus/tap、Escape 和完整定义链接，重复出现
  不增加 tab stop。八文件 focused=`74 passed in 138.06s`；当前尚未生成 final exact HTML、运行 2526-B
  browser/AT/mobile 或启动 2527-B human pilot，人工验收继续 `PENDING_REVIEW`。
- 2026-08-16：coordinator implementation candidate=`02200cb4d`，从该 exact commit 生成的 HTML
  SHA-256=`72ac2710f966015a586b9700765bb96ead6d76974813384de00214e003244c17`；13 个 artifacts 均由
  canonical writer 原子写入，`reader_accessibility_validation.json` exact-bind 同一 HTML 并为 automated
  `PASS`。in-app Browser 在接管/重载既有 `file://` tab 时被其本地 URL 安全策略拒绝；未切换浏览器、
  未启动 loopback server 或以其他接口绕过。因此 2526-B 与 2527-B 均未完成，本任务保持
  `IN_PROGRESS`，Owner visual/reader comprehension 保持 `PENDING_REVIEW`。
- 2026-08-16：Project Owner 通过同一 `file://` 页面人工截图发现 inline term 面板排版失败：靠近视口顶部的
  “快照”解释仍向上展开而被裁切，独立绝对定位的“完整定义”链接又与短释义重叠。coordinator 将短释义与
  glossary link 合并为一个 fixed popover，由运行时按可用上下空间选择方向，并把 top/left 限制在 viewport
  inset 内；重复术语也进入同一 context，因而 hover/tap 共用同一面板。首轮四文件并行回归为
  `44 passed / 1 failed`，失败准确暴露隐藏面板文本污染静态正文提取；改为从 `data-term-short` 生成视觉文本、
  继续由既有 `aria-describedby` 提供读屏说明后，定向 `2 passed`，四文件并行回归=`45 passed in 110.64s`。
  新 exact HTML 尚待从最终 tracked commit 生成并由 Owner 刷新复核，visual/reader comprehension 继续
  `PENDING_REVIEW`，不得把本次修复或 automated PASS 当成人工代签。
- 2026-08-16：Owner 刷新后确认出现更高层的信息架构缺口：页面直接询问“现有证据是否足以重新开放研究”，
  却没有先解释系统原始需求、研究为什么会进入关闭状态，以及当前诊断在整个策略决策链中的位置。本轮将
  既有 `WHY_CONTEXT` 修正为“全局定向在前、局部六步因果链在后”；不新增 reader section、不改变任务/研究
  状态、不修改 DQ/PIT、策略阈值、production 或 broker 权限。新 HTML 仍需 Owner 对“首次阅读能否建立
  整体心智模型”进行人工复核。
- 2026-08-16：coordinator 已把首屏改为四步全局路径：策略问题、可信证据、人工决策、授权执行；默认可见地
  说明当前停在可信证据，并在局部六步链之前直接回答“研究关闭是证据门 fail-closed，不是策略已证伪”、
  重新开放所需条件及当前页面只处理哪个局部卡点。首轮定向测试准确拦下英文装饰标题 `EVIDENCE` 对既有
  `evidence` 术语造成的大小写漂移，随后拦下旧 hero 文案断言；修正标题和回归权威后 targeted=`2 passed`，
  renderer/term-inventory/accessibility/page-effectiveness 四文件并行回归=`45 passed in 139.97s`。数据流、
  research state、DQ/PIT 与 execution authority 均未改变，因此不更新 `docs/system_flow.md`；新 exact HTML
  仍须在最终 tracked commit 后重建并由 Owner 人工复核。
- 2026-08-16：Owner 对新页面再次复核后指出，首屏虽然补了全局上下文，但“主研究窗口、关键证据、准入门槛、
  数据质量、时点可得性、来源准入、可信证据、检查轴、严格离线诊断”等项目概念仍互相解释，hover 反而可能
  把结构问题拆成多个临时窗口。Owner 授权把该问题提交 Web Pro，并在收到建议后继续优化。新 exact-commit
  advisory=`https://chatgpt.com/c/6a8135db-8e1c-83ee-9617-9e360a6660e3`，仓库基线固定为
  `ece8d97373c1a8a70949aa0ae445b79593ee09b3`，本地候选 `1e1458d472641bb05b60ab6d113dbee00d1b3e31`
  仅作为 prompt-supplied newer candidate。经本地 authority reconciliation，本轮不改变已冻结合同，也不启动
  新 serial contract wave：首屏改为 compact trust strip、一个当前时态主问题、四张零术语决策卡、始终可见的
  禁止推断条和普通话流程线；局部六步因果链进入单层 L1 disclosure，raw identifier 与完整来源继续走独立
  AUDIT destination。L0 删除全部 tooltip 后仍必须独立成立；标题只解释“为什么现在仍不继续得出策略结论”，
  canonical source 不足时不得倒推或编造历史关闭原因。验收新增首屏未解释缩写/raw ID 为 0、四卡一决策、
  320/360/390px 可达，以及 desktop/mobile 分轨的 20 秒五问复述；工程 PASS 仍不代签 Owner visual 或 reader
  comprehension。
- 2026-08-16：coordinator 已落实 Pro 建议的 L0/L1/AUDIT 重排：compact trust strip 位于标题之前；首屏改为
  当前时态 H1、四张单决策卡、始终可见的禁止推断和普通话流程线；项目术语、缩写与 raw ID 从 L0 可见文本
  移除，局部六步链进入唯一 L1 disclosure，完整来源核对成为独立 AUDIT card。回归测试显式删除全部 L0
  tooltip 后验证正文仍成立，并拒绝“主研究窗口、关键证据、准入门槛、DQ/PIT、G2/G3、TRADING-*”等未解释
  概念重返首屏。首次扩展回归 `44 passed / 1 failed` 仅暴露旧标题快照，更新权威后最终四文件并行回归=
  `45 passed in 135.07s`。本轮不改变研究事实、DQ/PIT、production、broker 或系统数据流，因此不更新
  `docs/system_flow.md`；下一步从最终 tracked commit 原子重建 exact HTML，并进行 320/360/390px 与 desktop
  视觉复核，Owner visual/reader comprehension 继续 `PENDING_REVIEW`。
