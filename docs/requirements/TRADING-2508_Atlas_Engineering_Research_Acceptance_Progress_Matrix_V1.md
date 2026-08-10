# TRADING-2508：Atlas 工程、研究与页面验收进度矩阵 V1

状态：`BASELINE_DONE`

稳定任务 ID：`TRADING-2508_ATLAS_ENGINEERING_RESEARCH_ACCEPTANCE_PROGRESS_MATRIX_V1`

优先级：`P1`

Owner：Atlas page engineering

依赖：`TRADING-2507_QQQ_OPTIONS_OWNER_DECISION_POLICY_ADOPTION_CONTRACT_V1`

## 1. 用户问题

当前八节点流程图在折叠状态下只展示一个彩色状态。这个状态描述页面正在展示的执行事实，
但读者容易把它同时理解成：工程已经稳定、研究证据已经充分、策略已经验收通过。尤其是
`VALIDATED` 容易被误读为“策略有效”，而页面真正能证明的可能只是结构、引用或页面投影
通过了工程校验。

用户需要在不展开全部节点的前提下明确区分：

1. 工程能力是否已经实现并可复用；
2. 当前节点是否新增了足以支持策略判断的研究证据；
3. 页面本身是否通过工程、读者理解与 Owner 视觉三条独立验收；
4. “当前研究关注”“你在这里”等徽标只是流程位置，不是成熟度评级。

## 2. 权威与不变量

页面不得再造一套手工进度状态。所有汇总必须从现有 typed authority 派生：

- `CapabilityProgress`：`AVAILABLE`、`IN_PROGRESS`、`BLOCKED`、`NOT_APPLICABLE`；
- `ResearchEffect`：`NO_NEW_RESEARCH_EVIDENCE`、`LIMITED_RESEARCH_EVIDENCE`、
  `OWNER_DECISION_ONLY`；
- `latest_execution_status`：本次 canonical 页面实际观察到的执行状态；
- `PageAcceptanceRecord`：`ENGINEERING_VALIDATION`、`OWNER_VISUAL_REVIEW`、
  `READER_COMPREHENSION_REVIEW` 三条独立验收事实。

工程 `AVAILABLE`、页面 `VALIDATED` 或验收 `PASS` 均不得推导出策略有效、收益稳健、
样本外充分、可下单或可生产部署。策略结论通过数在没有相应研究权威时必须明确为 `0`。

## 3. 页面设计

### 3.1 页面顶部三轴总览

在八节点流程图之前展示“工程、研究、页面验收分别看”的总览。总览必须由 typed records
实时计数，禁止把当前数字写死在 HTML：

- 工程能力：可用、进行中、阻塞、本页不适用；
- 研究证据：有限证据、本页无新增、仅人工决策；
- 页面验收：三条 acceptance track 的 PASS、PENDING、FAIL/NOT_EXECUTED；
- 单独显示“策略结论通过：0”，并解释工程稳定不等于策略通过。

### 3.2 每节点折叠摘要

每张卡片在不展开时至少同时显示：

- 工程：该节点能力的 `CapabilityProgress`；
- 研究：该节点对研究结论的 `ResearchEffect`；
- 本页状态：保留现有 canonical execution/status provenance，但视觉上降为第三个独立维度。

节点位置徽标继续表达上游、当前研究关注、当前页面位置或人工边界，不得与上述三维状态
共用语义或颜色说明。

### 3.3 展开层与递归解释

展开层继续回答“为什么需要、具体做什么、预期产物、完成后如何使用、不能说明什么、
什么时候再做”。若说明引入其他模块概念，继续使用 2506 的 concept graph 跳转与返回能力；
三轴状态标签本身要提供读者可读定义，不能要求读者先理解内部 enum。

### 3.4 页面验收呈现

页面验收是 page-level 事实，不伪装成每个策略节点的研究成熟度。当前 canonical authority
应显示工程验收与读者理解验收已经 PASS，Owner 视觉验收仍独立 PENDING；后续事实变化由
typed manifest 和 human attestation 驱动，不由 renderer 自行签署。

## 4. 实施边界

任务采用 `SINGLE_LANE`。task-owned scope：

- 本 requirement；
- `src/ai_trading_system/contracts/strategy_research_work_progress.py` 中最小 typed 汇总；
- `src/ai_trading_system/atlas/cited_query_renderer.py` 的总览和节点摘要；
- Atlas work-progress、renderer、page-effectiveness focused tests；
- 如 source coverage 需要，`config/atlas/page_effectiveness.yaml` 的 2508 reviewed successor。

coordinator-owned scope：canonical task registry、`docs/system_flow.md`、architecture fragments、
ARCH-004/004G compatibility authority、ARCH-005 index/task shadow、DevEx/generated state 与最终
formal evidence。

不改变研究窗口、DQ/PIT、数据源、策略阈值、信号、选券、执行、记账、生命周期、Owner
decision、外部 QuantConnect 行为、生产或 broker 行为。

## 5. 验收标准

1. 八个节点折叠状态均显示工程与研究两个独立标签，且保留独立本页状态来源；
2. 顶部三轴总览由 typed authority 派生，缺节点、重复节点、未知状态或 acceptance track
   不完整时 fail closed；
3. 页面明确写出工程稳定不等于策略有效，且 `data-strategy-conclusion="PASS"` 仍为零；
4. 01/02 能被读者理解为“本页不执行”而非失败，04/05 能被理解为“能力可用但证据有限”，
   06/07 能被理解为“页面工程能力已验证但未新增策略证据”，08 保持人工边界；
5. desktop 与 mobile 无水平溢出，颜色不是唯一信息载体，标签在折叠状态可读；
6. canonical HTML、page-effectiveness manifest/validation、work-progress sidecar 与 source commit
   一致，保留真实 reader PASS 和独立 visual PENDING；
7. focused、compatibility/deprecation、Architecture、Contract、Integration、Reproducibility、
   exclusive Full 与 ordinary push/cleanup 全部通过。

## 6. 进度记录

- 2026-08-11：Owner 明确要求增加说明，以同时理解工程线与策略线的具体进度；2507 已完成
  ordinary push/cleanup，exact base 为 `0680bae731e8e9c329378ace5324cc194b8e7672`。
- 2026-08-11：已实现 typed 八节点汇总、三轴总览和每节点工程/研究双标签；页面继续单独展示
  canonical 本页状态，且策略结论通过数保持 `0`。page-effectiveness reviewed coverage 从 26 个
  task 扩展到 27 个并纳入 2508，未知 successor 和 source drift 继续 fail closed。
- 2026-08-11：Atlas focused replacement 为 `42 passed`。本地 loopback HTTP + Playwright 在
  `1440x1200` 与 `390x844` 检查 3 张矩阵卡、16 个节点轴标签、页面验收 `2/3` 和水平溢出；
  首轮截图发现全局 `header` 样式污染矩阵标题对比度，改用 scoped card-head 后重建页面并复核
  desktop/mobile 截图通过。该自动化只形成工程验收证据，Owner 视觉验收仍保持独立
  `PENDING_REVIEW`。
- 2026-08-11：实现候选已达到 `BASELINE_DONE`；下一步是在最终 tracked bytes 上重建 compatibility、
  task shadow 与 canonical page，再完成五级正式门禁、ordinary push 和 cleanup。
- 2026-08-11：首次 Architecture 正式运行得到 `864 passed / 1 failed`；唯一失败是 2508 登记后
  canonical task 数已从 977 增至 978，而 ARCH-005 S5 自托管 exact-count 断言仍为 977。该失败不涉及
  Atlas 页面语义。已通过扩展后的 governed coordinator path 将断言更新为 978；首次 runtime artifact
  保留为 failure-fix 依据，修复后必须在最终 bytes 上重新执行完整五级门禁。
- 2026-08-11：修复后 Architecture/Contract/Integration/Reproducibility 分别 `865/276/995/24`
  PASS；首次 Full 为 `8736 passed / 3 failed / 3 skipped`。三个失败同属 DEVX-006D lossless shadow
  漂移：本任务更新 `docs/system_flow.md` 后，tracked shadow 仍绑定旧 SHA 与 938 entries。已用 canonical
  `architecture_report_catalog_flow_authority.py build` 重建为 2862 total entries / 192 fragments，
  `system_flow` 为 939 entries，并同步 compatibility authority；不得用跳过测试或手工伪造 shadow 代替。
  修复后需要以首次失败 Full 为 parent、`failure_fix_rerun` provenance 完整重跑五级。
