# TRADING-2506 Atlas 工作进展与递归概念解释 V1

## 任务身份

- task id：`TRADING-2506_ATLAS_WORK_PROGRESS_RECURSIVE_EXPLANATION_V1`
- priority：`P1`
- status：`BASELINE_DONE`（工程实现、浏览器验收与 compatibility/deprecation 已通过；真实 Owner visual/reader comprehension 仍独立待验收）
- governed mode：`SINGLE_LANE`
- contract change：`true`（新增 consumer-visible 工作进展与概念解释合同；不改变策略、DQ/PIT、research window 或交易合同）
- exact registration base：`97421591119f68e46a091d0aca47c2a48aa9d317`
- predecessor：`TRADING-2505_ATLAS_PAGE_EFFECTIVENESS_FRESHNESS_VISUAL_REGRESSION_V1`
- next owner：Atlas page engineering owner；project owner 在新页面上重新执行 reader comprehension review

## Owner 反馈与失败事实

2026-08-10，project owner 在 canonical 页面节点 `ATLAS_SNAPSHOT_DIFF` 明确反馈：阅读现有“Atlas 快照与变化”说明后，仍不清楚具体工作内容、当前进展、为什么需要这一步以及预期结果；解释中出现 `snapshot`、`diff`、`source lineage`、独立结构校验与引用闭包等概念时，也没有可继续理解这些概念的递归入口。

该反馈是 `READER_COMPREHENSION_REVIEW` 的真实失败事实，不影响 2505 的 source/semantic/browser engineering PASS，也不得被解释为策略或研究结果失败。2505 应继续保持工程基线完成，但 reader comprehension 不能升级为 PASS；2506 负责修复该 reader-facing contract 缺口。

## 根因

现有 2495/2496 状态解释合同已经区分“一句话结论、正在做什么、已完成什么、还缺什么、为什么重要、状态变化和审计依据”，但仍存在四个结构性问题：

1. `CURRENT_WORK`、`COMPLETED_MILESTONE` 与节点 `status_code` 混合了模块研发进度、当前页面执行结果和策略研究结论；
2. “为什么重要”通常只说明不能推出什么，没有先说明流程为什么需要该节点；
3. 没有显式交付物和 downstream usage，读者无法判断完成后会产出什么、谁会使用；
4. 技术概念只以文本出现，没有 typed reference、闭包、cycle 检查或渐进式解释路径。

这不是单句文案问题，不能通过只改 `ATLAS_SNAPSHOT_DIFF` 的一段文本解决。所有流程节点必须使用同一 reader-first contract，并且递归概念解释必须由结构化 authority 驱动。

## 冻结设计

### 1. 三类状态分离

每个流程节点必须并列展示且不得互相提升：

- `CAPABILITY_PROGRESS`：该步骤所需工程能力是否尚未开始、开发中、已具备、受阻或不适用；
- `LATEST_EXECUTION_STATUS`：当前页面所绑定的最近一次节点/结果/validator 状态；
- `RESEARCH_EFFECT`：该步骤是否产生新的研究证据、策略结论或交易授权。

`CAPABILITY_PROGRESS=AVAILABLE` 或 `LATEST_EXECUTION_STATUS=PASS/VALIDATED` 不得推导策略有效。`RESEARCH_EFFECT` 在没有 canonical research authority 时必须明确为 `NONE` 或更严格的 unknown/blocked 状态。

### 2. Reader-first 工作说明

每个节点第一、二层说明必须回答：

1. `为什么需要这一步`：若省略该步骤，读者或研究链会面临什么具体风险；
2. `具体做什么`：以动作和对象描述工作内容，不以内部模块名代替动作；
3. `目前进展`：分别报告 capability 与 latest execution，不使用一个颜色合并两者；
4. `预期产物`：列出可观察的输出；
5. `后续如何使用`：说明 downstream consumer 或下一流程节点；
6. `不能说明什么`：保留研究、投资、下单与 production 边界；
7. `下一触发条件`：何时需要重跑、补证据或由 Owner 决策。

内部 task id、schema、hash、locator 与 authority binding 只进入审计层，不得作为第一层解释。

### 3. 递归概念解释

新增 typed concept graph。每个 reader-facing technical concept 至少包含：

- stable `concept_id` 与中文显示名；
- 一句话通俗定义；
- 为什么本流程需要它；
- 一个与当前页面相关的具体例子；
- `related_concept_ids`，用于继续展开相关概念。

每个节点通过 `concept_ids` 显式引用概念。validator 必须 fail closed：

- concept id 与 stage id 唯一；
- 每个引用都能解析；
- 不允许 self-reference、unknown reference 或 graph cycle；
- reader-first 主说明拒绝内部 identifier、hash/path/schema 形态；
- related concepts 通过可返回上一层的导航呈现，不能无限嵌套复制文本；
- technical evidence 仍保留 exact refs，但与概念解释视觉分层。

递归解释采用渐进式 disclosure：节点主卡 → 概念卡 → 相关概念 → 审计依据。概念图可以跨模块复用，但每次新增概念必须进入同一 graph authority 和闭包验证。

### 4. `ATLAS_SNAPSHOT_DIFF` 基准示例

主标题改为面向任务目的的“检查页面是否仍代表最新研究状态”。第一层至少表达：

- 为什么需要：研究任务和证据持续变化，旧页面即使能打开也可能已经过期；
- 具体工作：保存页面生成时使用的研究状态、与上一状态比较、记录展示内容来源；
- 当前进展：能力已具备，当前页面检查已通过，本次没有待修改内容；
- 预期产物：当前/需更新判定、变化清单、来源关系记录；
- 后续用途：决定页面是否可继续标记为当前，或必须重新生成；
- 边界：只证明页面记录与来源一致，不证明策略有效。

“快照”“变化比较”“来源关系”和“结构校验”必须通过概念卡继续解释。

## 实现范围

### Task-owned

- `config/atlas/work_progress_explanation.yaml`
- `src/ai_trading_system/contracts/strategy_research_work_progress.py`
- `src/ai_trading_system/atlas/work_progress_projection.py`
- `tests/atlas/test_work_progress_projection.py`
- `config/architecture/fragments/modules/atlas_work_progress_explanation.yaml`
- `config/architecture/fragments/modules/strategy_research_work_progress_contract.yaml`
- `config/architecture/fragments/flows/atlas_work_progress_explanation_page.yaml`
- 本 requirement

### Coordinator-owned

- canonical task registry fragment/index 与 generated `docs/task_register*.md`
- 2505 canonical task 状态说明与 2506 successor binding
- `src/ai_trading_system/atlas/cited_query_renderer.py`、package exports 与 renderer tests
- `config/atlas/page_effectiveness.yaml` 与 freshness source coverage
- `docs/system_flow.md`、`docs/artifact_catalog.md`
- architecture/module/test/deprecation manifests、compatibility authority 与 generated views

不得修改 TRADING-2481–2504 的 shared QQQ contract/policy，不填写任何 Owner investment threshold，不执行 QuantConnect、cloud、API、HTTP 外部动作、raw export、paper/live/broker/production。

## 分步计划

1. 登记 2506，并把 2505 reader comprehension 失败事实与 successor 写入 canonical task authority；
2. 冻结 work-progress 与 concept-graph typed contract、canonical seal/replay 和 strict policy loader；
3. 为全部 Atlas 流程节点建立 purpose/work/progress/output/downstream/boundary/trigger projection；
4. renderer 接入分层展示、概念导航和审计层；
5. page effectiveness 纳入新 config/module/sidecar identity，重新生成 canonical 页面；
6. 运行 focused contract/projection/renderer 测试；
7. 使用 loopback HTTP 与 Playwright CLI 验证 desktop/tablet/mobile、keyboard、anchor、concept navigation、返回路径、overflow、console 和截图；
8. 刷新 system flow、artifact catalog 与 generated authority，完成 final-tree gates、ordinary push 与 cleanup。

浏览器自动化不得与 pytest formal/heavyweight gates 并发。

## 验收标准

- 八个流程节点均能直接回答为什么、做什么、当前进展、预期产物、后续用途、边界和下一触发条件；
- capability progress、latest execution 与 research effect 三者在 contract、DOM 和视觉上独立；
- `ATLAS_SNAPSHOT_DIFF` 使用冻结基准语义，不再以 `diff/source lineage/validator` 作为第一层解释；
- concept graph exact replay、全引用闭包、acyclic、unknown/self/cycle negative tests PASS；
- 概念卡可通过键盘进入、跳转相关概念并返回发起节点；
- desktop/tablet/mobile 无关键水平溢出、遮挡、截断或不可达控件；
- 页面仍明确 `not investment advice`，不产生策略、收益、风险、下单、engine 或 production 结论；
- 真实 project owner reader review 在新页面上重新执行，自动化不得代签；
- focused、architecture/contract/integration/reproducibility/full 与 governed closeout PASS，ordinary push 后 local main 与 origin/main 相同。

## 开放问题与退出条件

- 本任务交付时 engineering validation 可由自动化写入；Owner visual 与 reader comprehension 仍只接受真实人工事实；截至 2026-08-10，`READER_COMPREHENSION_REVIEW=PASS`，`OWNER_VISUAL_REVIEW` 仍待独立确认；
- 若新的 reader text 需要解释投资阈值或策略判断，停止并另开最小 reviewed policy wave；
- 若 concept graph 只能靠硬编码 DOM 跳转而不能 canonical replay，保持任务未通过；
- 若任何新增 successor 影响当前页面而未分类，page freshness 必须 fail closed。

## 页面时效性运行边界

当前 freshness contract 已能把页面绑定到 exact repository commit、25 项任务覆盖、15 份语义来源及 rendered HTML identity，并区分 `CURRENT`、`REPOSITORY_AHEAD_NO_RELEVANT_DRIFT`、`STALE_REBUILD_REQUIRED` 与 `UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED`。这证明“执行校验时”可以发现相关漂移，但静态 HTML 不会自行感知仓库变化。

后续时效性保证必须采用事件驱动门禁，不以未经评审的“生成后 N 天”阈值代替来源事实：

1. 每次影响 Atlas 页面来源、任务 successor coverage 或 renderer identity 的 main 集成后运行 freshness validator；
2. 每次页面验收、分享或发布前再次运行同一 validator；
3. `STALE_REBUILD_REQUIRED` 或 `UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED` 必须阻断“当前页面”声明，并要求从 latest canonical sources 重建页面；
4. `REPOSITORY_AHEAD_NO_RELEVANT_DRIFT` 可以继续阅读，但页面必须展示 exact source snapshot commit、current repository commit 与判定时间，不能静默显示为 `CURRENT`；
5. 重建后重新校验 source/task/rendered identity；仅视觉来源发生变化时追加 browser regression，不能用旧截图替代新页面身份；
6. 定期运行只能作为漏检兜底，正式 authority 仍是相关来源变化事件与验收前门禁。

自动触发尚未接入统一集成/周期运行入口，因此在该 wiring 完成前，freshness validator 仍需由 Atlas page coordinator 在上述两个边界显式执行；不得把“已有校验函数”表述为“已自动保证时效性”。

## Progress notes

- 2026-08-10：Owner 提供 `ATLAS_SNAPSHOT_DIFF` 页面截图并明确指出工作内容、必要性、进展、预期结果和递归概念解释均不清楚；该事实构成 2505 reader comprehension failure，2506 获准继续推进。
- 2026-08-10：完成八阶段 typed work-progress/13-concept graph、strict projection/validation、reader-first renderer 与 freshness coverage；合同/renderer/effectiveness focused=`24 passed`。canonical 示例输入重建 exact snapshot=`51f2a7b4…`、diff=`34553e0e…`，新增两份 deterministic sidecar。
- 2026-08-10：loopback HTTP + Playwright CLI 工程验收 PASS。desktop 1440×1200 与 mobile 390×844 均无水平溢出；第 6 节为“页面可靠性检查”，主标题“检查页面是否仍代表最新研究状态”，展开后为 3 项工作、3 项产物、3 个独立进展维度、4 个概念入口，状态审计默认折叠。概念“页面快照”具备通俗定义、2 个继续解释链接、1 个返回节点链接和 `:target` 高亮。截图保存在 `visual_regression/trading_2506_desktop.png` 与 `trading_2506_mobile.png`；该自动化事实不代签 Owner visual/reader comprehension。
- 2026-08-10：最终字节 focused 首轮为 `9 passed / 15 failed`；15 个失败均由 2506 canonical task projection 缺少本 requirement 的 `requirement_refs` 引起，page effectiveness 按设计 fail closed，非页面语义回归。采用 append-only task update，在验收条件中加入 canonical Markdown requirement link，随后重建 task shadow、compatibility authority 并保持相同 24 项覆盖重跑。
- 2026-08-10：首次完整 Full 为 `5 failed / 8711 passed / 3 skipped`，parent artifact=`outputs/validation_runtime/full_20260810T075448Z/test_runtime_summary.json`。四项失败来自本次新增 `system_flow`/`artifact_catalog` 条目后 DEVX-006D lossless shadow 未重建；一项来自 historical canonical-page consumer 仍把 task coverage 冻结在 2481–2504，未接受 sidecar 中已验证的 2506 successor。两者均为生成 authority/consumer freshness 缺口，不是策略计算或 work-progress/concept-graph 语义失败；修复后必须从最终字节重跑五级，并以该首次 Full 作为 `failure_fix_rerun` parent。
- 2026-08-10：Project owner 在 canonical 页面上确认“目前可读性验收通过”，作为真实人工事实封存 `READER_COMPREHENSION_REVIEW=PASS`；该结论不自动代签 `OWNER_VISUAL_REVIEW`，也不构成策略有效、收益、风险、交易或 production 验收。对当前 main 的 freshness 复核为 `REPOSITORY_AHEAD_NO_RELEVANT_DRIFT`：仓库 commit 已前进，但受管的语义来源、任务覆盖与 rendered identity 未漂移。后续剩余边界是把 validator 接入相关 main 集成后与验收/分享前的自动门禁。
- 2026-08-10：修复人工验收事实的 artifact persistence：`build_page_effectiveness_manifest` 与 `build_cited_query_showcase` 现在只接受 typed `PageAcceptanceRecord`，track 不匹配 fail closed；`write_cited_query_artifacts` 重建最终 manifest 时保留两条 human review，而不是重置为 `PENDING_REVIEW`。canonical 页面/sidecar 已显示 engineering=`PASS`、reader comprehension=`PASS`、owner visual=`PENDING_REVIEW`，静态 HTML、manifest、validation sidecar 与 live validator 八项身份检查 PASS。focused 页面测试=`20 passed`；页面+historical consumer+compatibility/deprecation 同覆盖 replacement=`245 passed`，首次同覆盖仅为外层 timeout、无 node FAIL/无 terminal summary，不作验证证据。
