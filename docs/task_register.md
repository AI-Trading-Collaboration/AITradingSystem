# 任务登记与优先级机制

本文是未完成任务、后续优化、阻塞项、基础版遗留问题和 owner 配合事项的统一登记表。`docs/implementation_backlog.md` 继续负责长期模块路线图；本文负责具体任务的优先级、状态和下一步动作。

最后更新：2026-05-04

## 使用规则

- 所有非平凡 TODO、review 后续项、基础版遗留项、临时绕行方案和需要 owner 配合的数据源事项，都必须登记在本文。
- 不把任务只留在代码注释、聊天记录、临时 checklist 或个人记忆中。
- 后续讨论出的非平凡需求、bug 修复、评分调整、数据链路调整、报告行为调整，必须先在本文新增或更新任务，明确优先级、状态、下一责任方、阻塞项和验收标准，再进入实现。
- 只有不影响系统行为、投资解释、数据流、数据质量、评分、回测或报告输出的纯 housekeeping，才可以不先登记。
- 如果需求上下文太长，不适合塞进任务表格，应该在 `docs/requirements/` 或其他清晰命名的 `docs/` 子目录创建独立 Markdown 文档。任务表只保留简短摘要和文档链接；详细背景、设计决策、开放问题、验收标准、进展记录和状态迁移必须在该文档中维护。
- 如果任务需要拆成多个开发步骤，必须先创建或更新独立需求文档，记录阶段拆解、依赖关系、实施顺序、各阶段验收标准、开放问题和状态迁移；任务表必须引用该文档，而不是把完整计划塞进单个表格单元。
- 使用独立需求文档时，后续开发进展必须同步更新任务表的摘要/状态，以及被引用文档中的详细进展记录。
- 任务状态变化必须和触发变化的代码或文档改动一起更新。
- `BASELINE_DONE` 不等于完成。它表示已有可审计基础闭环，但仍存在数据源、验证、设计或覆盖缺口。
- 被 owner 输入、数据源、API 权限、样本时间窗口或成本限制阻塞的任务，必须明确 blocker 和解除条件。
- 每个任务必须有可验收的完成标准，避免长期停留在模糊“优化”状态。

## 优先级

|优先级|含义|处理原则|
|---|---|---|
|P0|影响正确性、数据质量门禁、未来函数、仓位硬约束或可能导致错误投资解释|优先于新功能；未解决前不能把相关输出当作完整交易结论|
|P1|显著改善评分、回测、数据源可信度、风险控制或审计解释能力|排在近期开发队列前列|
|P2|提升覆盖率、易用性、报告下钻、长期维护或开发效率|在 P0/P1 稳定后推进|
|P3|低风险清理、文档润色、非关键体验优化|有明确收益时再做|

## 状态

|状态|含义|
|---|---|
|PROPOSED|已记录，但需求、设计或验收标准还未充分明确|
|READY|需求和验收标准明确，可以实施|
|IN_PROGRESS|正在实现或验证|
|BLOCKED_OWNER_INPUT|需要项目 owner 提供数据源、权限、样本、业务判断或人工复核|
|BLOCKED_EXTERNAL|受外部 API、供应商、时间窗口、成本或依赖限制|
|BASELINE_DONE|基础版已完成，但仍有明确后续缺口|
|VALIDATING|实现已完成，等待真实数据、回测、审计报告或运行观察确认|
|DONE|验收标准已满足，且相关文档、测试和报告已更新|
|DEFERRED|明确暂缓，且暂缓原因已记录|
|DROPPED|明确不再需要，且原因已记录|

## 当前任务

|ID|领域|优先级|状态|下一责任方|阻塞或下一步|验收标准|备注|
|---|---|---|---|---|---|---|---|
|REPORT-001|报告/审计|P0|DONE|系统实现|已按 `docs/requirements/report_traceability.md` 第一版实现日报/回测 evidence bundle、run manifest、dataset/quality refs 和 `aits trace lookup` 反查入口|最终报告的每个核心结论都有稳定 `claim`、`evidence`、`dataset`、`quality` 引用；evidence bundle 记录 signal、配置快照、ticker、日期窗口、`ai_after_chatgpt` regime、数据质量门状态和数据集 provider/row count/checksum；可从报告反查原始或标准化数据快照；实现时同步更新 `docs/system_flow.md` 并补充测试或示例报告|2026-05-04: 从 READY 改为 IN_PROGRESS，原因：创建需求拆解文档并开始实现第一版报告可追溯链路。2026-05-04: 从 IN_PROGRESS 改为 DONE，原因：日报和回测已生成 trace bundle 并注入可追溯引用章节，`aits trace lookup` 可反查 claim/evidence/dataset/quality/run id；`python -m pytest -q` 通过 203 项测试。|
|REPORT-002|报告/执行解释|P1|DONE|系统实现|设计并实现日报/最终报告的变化原因树和“什么情况会改变判断”章节；详细拆解见 `docs/requirements/decision_support_improvements_2026-05-04.md#report-002`|报告明确展示本期仓位区间变化、分模块分数变化、thesis/风险事件状态、最终动作约束和加/减仓触发条件；能区分市场短期波动、thesis 承压、thesis 证伪、估值过高但基本面未坏、基本面恶化；实现时同步更新 `docs/system_flow.md` 并补充报告快照或文本测试|2026-05-04: 已完成首版实现，`scores_daily.csv` overall 行保存模型/最终/置信度调整仓位区间，日报新增变化原因树、分模块分数变化、最终动作约束和判断改变条件；来自 2026-05-04 报告解释优化讨论；与 `REPORT-001`、`SCORE-002`、`THESIS-001` 协同|
|SCORE-001|评分/仓位|P0|DONE|系统实现|已实现独立 `position_gate` 层，把评分仓位、风险事件、估值拥挤、thesis 状态、数据置信度和组合限制取最严格上限|`score-daily` 和 `backtest` 使用同一最终仓位约束；日报说明每个 gate 的来源、上限和是否触发；测试覆盖 L2/L3 风险压低仓位但不只依赖总分|2026-05-04: 从 READY 改为 IN_PROGRESS，原因：开始实现共享仓位硬闸门并接入日报、回测、文档和测试。2026-05-04: 从 IN_PROGRESS 改为 DONE，原因：`score-daily` 与 `backtest` 已共用最终仓位闸门；日报和回测报告输出 gate 摘要；`python -m pytest -q` 通过 203 项测试。|
|SCORE-002|评分/报告|P1|DONE|系统实现|已实现 AI 产业链评分与判断置信度拆分；日报、`scores_daily.csv`、decision snapshot、回测每日明细和回测报告均输出 confidence|`score-daily` 日报顶部同时输出 AI 产业链评分、判断置信度、模型评分仓位、置信度调整后建议仓位和限制原因；`scores_daily.csv` 或等价结构化输出可追踪整体置信度和主要扣减原因；回测每日明细和审计报告能按置信度分桶检查低置信度结论；实现时同步更新 `docs/system_flow.md` 并补充评分/报告/回测测试|来自 2026-05-04 评分解释讨论。目标是避免用户把 72/100 之类的市场分数误读为同等可靠的交易结论；低置信度不直接否定机会，但必须限制结论解释和仓位使用。2026-05-04: 从 READY 改为 IN_PROGRESS，原因：开始实现评分/置信度拆分、日报/CSV/回测输出和系统不变量测试。2026-05-04: 从 IN_PROGRESS 改为 DONE，原因：已完成组件级和整体置信度、日报/CSV/快照/回测输出、文档和测试；`python -m pytest -q` 通过 220 项测试。|
|SCORE-003|仓位/风险预算|P1|PROPOSED|系统实现|评估并设计从固定区间中点仓位升级为可审计风险预算模型：保留总分生成基础仓位区间，再由目标波动率、最大回撤预算、单一产业链节点暴露、单票/ETF 暴露、AI 资产与 QQQ/SMH 相关性、VIX/波动状态和估值拥挤度约束最终建议区间|形成风险预算设计文档或配置方案，明确哪些指标作为硬上限、哪些作为禁止加仓条件、哪些只进入报告解释；`score-daily` 和 `backtest` 输出模型基础区间、风险预算后建议区间、总资产折算、主要限制原因和不得加仓条件；回测覆盖同分不同风险状态下最终仓位不同；实现时同步更新 `docs/system_flow.md`|来自 2026-05-04 仓位映射优化讨论。该任务应和 `SCORE-001` 的 `position_gate`、`VALUATION-001` 的估值拥挤 gate、`MACRO-001` 的动态风险资产预算协同推进；避免直接引入不可审计黑箱优化器|
|COGNITION-001|认知模型/belief_state|P1|DONE|系统实现|建立只读 `belief_state` 认知状态层；详细拆解见 `docs/requirements/cognitive_model_2026-05-04.md#cognition-001`|每次 `score-daily` 通过质量门禁后生成机器可读 `belief_state`，结构化表达市场状态、产业链节点状态、估值状态、风险状态、thesis 状态、仓位边界、限制因素和多维置信度；核心判断必须引用数据质量、证据或人工复核来源；日报输出中文认知状态摘要；第一版不得改变正式评分、`position_gate`、回测仓位或交易建议；实现时同步更新 `docs/system_flow.md` 并补充 schema、报告、trace 引用和“belief_state 不直接改仓位”的测试|2026-05-04: 已完成首版实现，`score-daily` 写入 `belief_state` JSON 和历史索引，日报输出认知状态摘要，decision snapshot 写入 `belief_state_ref`，evidence bundle 增加 `belief_state` dataset/claim 引用，并测试只读状态不改变仓位；原因：将系统长期目标从评分器升级为可审计认知模型，需要显式中间状态承接 evidence、thesis、risk、valuation、industry chain、confidence、position boundary 和后续校准；与 `EVIDENCE-001`、`CHAIN-001`、`FEEDBACK-001/002`、`CAUSE-001`、`GOV-001` 协同|
|CHAIN-001|产业链/报告解释|P1|PROPOSED|系统实现|设计并实现产业链节点状态层，为每个节点输出 `node_heat`、`node_health`、覆盖率和集中度；详细拆解见 `docs/requirements/industry_chain_node_state.md`|`score-daily` 报告按产业链节点展示热度、健康度、覆盖率、集中度和解释；能区分行情是 GPU/HBM 等局部节点交易，还是完整产业链扩散；健康度只使用已通过门禁的基本面、估值、风险事件和 thesis 输入，数据不足必须显式标记；回测或审计能追踪历史节点状态；实现时同步更新 `docs/system_flow.md` 并补充测试|来自 2026-05-04 产业链节点热度/健康度讨论。第一版定位为解释和诊断层，不直接触发交易；后续可与 `SCORE-001`/`SCORE-003` 协同进入仓位限制|
|EVIDENCE-001|信息输入/证据账本|P1|DONE|系统实现 + 项目 owner|已实现 `market_evidence` schema、YAML/CSV 导入、校验报告、CLI 和 LLM/public_convenience 隔离|新市场信息进入系统前都有结构化证据记录、来源等级、采集时间、去重键、影响对象和人工复核状态；报告能区分已确认证据、待复核证据和不能进入自动评分的低可信证据；证据可追溯到 risk event、thesis、valuation、catalyst、industry_chain_node 或日报 claim；实现时同步更新 `docs/system_flow.md` 并补充 schema、去重、来源等级和 LLM 隔离测试|该任务补足“新信息进入闭环”的入口；与 `LLM-001` 边界明确：LLM 只能辅助分类和写入待复核证据，不得直接改评分或仓位。2026-05-04: 从 READY 改为 IN_PROGRESS，原因：开始实现 market_evidence schema、CSV/YAML 导入、校验、CLI 和 LLM 隔离测试。2026-05-04: 从 IN_PROGRESS 改为 DONE，原因：证据账本、导入、校验、报告、系统流图和 LLM 隔离测试已完成；`python -m pytest -q` 通过 220 项测试。|
|CAUSE-001|决策因果链/审计|P1|DONE|系统实现|已建立 `decision_causal_chain` ledger、构建命令和查询入口，把 evidence、模块变化、gate、decision snapshot、quality、outcome 和预留 rule candidate 串起来；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#cause-001`|每个核心仓位变化或结论降级都能对应至少一个 `chain_id`；因果链能连接 evidence、module score/confidence 变化、gate、decision snapshot、outcome 和后续规则复核；不允许用未来 outcome 改写 signal_date 当时的 causal chain，只能追加后验观察字段；实现时同步更新 `docs/system_flow.md` 并补充因果链和未来函数防护测试|2026-05-04: 开始实现 `decision_causal_chain` ledger、CLI 构建/查询入口和未来 outcome 隔离测试。2026-05-04: 从 IN_PROGRESS 改为 DONE，原因：新增 `aits feedback build-causal-chain`、`aits feedback lookup-chain`、`data/processed/decision_causal_chains.json` schema、中文因果链报告、系统流图和测试；后续 `linked_rule_candidate` 由 `LEARNING-001/EXPERIMENT-001` 回填。|
|LEARNING-001|反馈闭环/错误归因|P1|PROPOSED|系统实现 + 项目 owner|建立结果归因、错误分类与学习队列；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#learning-001`|校准报告和重大偏差样本能生成 review queue，记录 `data_issue`、`rule_issue`、`thesis_issue`、`execution_issue`、`universe_or_proxy_issue`、`exogenous_unforecastable`、`sample_limited` 等归因分类、证据、owner、下一步和是否需要规则候选；成功样本也能归因；样本不足不得强行生成规则修改建议；实现时同步更新 `docs/system_flow.md` 并补充归因分类测试|依赖 `FEEDBACK-002` 和 `CAUSE-001`；用于提升认知迭代质量，而不是让单次失败直接驱动规则改动|
|EXPERIMENT-001|规则实验/回放验证|P1|PROPOSED|系统实现|建立候选规则实验、历史重放和前向 shadow 验证流程；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#experiment-001`|每个候选规则都有触发原因、关联 causal chain、历史 replay、前向 shadow 计划、样本限制说明、风险和回滚条件；报告比较 production vs candidate 在收益、回撤、换手、置信度、触发 gate、结论等级和失败样本上的差异；候选规则未批准前不得影响正式评分、仓位或日报结论；实现时同步更新 `docs/system_flow.md` 并补充 candidate isolation 和 replay 测试|与 `GOV-001` 协同：该任务负责验证候选规则，`GOV-001` 负责规则生命周期、批准和版本治理|
|LOOP-001|反馈闭环/周期复核|P1|PROPOSED|系统实现|建立闭环复核编排与周期报告；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#loop-001`|生成闭环复核报告，覆盖 new evidence、decision snapshots、due outcomes、calibration findings、causal chains、learning queue、rule candidates、blocked owner inputs 和 task register 状态变化；报告声明市场阶段，默认以 `ai_after_chatgpt` 作为主要结论窗口；明确哪些结论可执行、哪些仅研究、哪些因数据或样本不足降级；实现时同步更新 `docs/system_flow.md` 并补充周期报告测试|该任务把 `EVIDENCE-001`、`FEEDBACK-001/002`、`CAUSE-001`、`LEARNING-001`、`EXPERIMENT-001` 和 `GOV-001` 串成固定运营节奏|
|FEEDBACK-001|反馈闭环/决策记录|P1|DONE|系统实现|已实现 `decision_snapshot`，`score-daily` 通过数据质量门禁后按 signal_date 覆盖写入确定路径，并保存 trace 关联|每次评分都保存 market regime、实际请求日期、总分、模块分、置信度、模型仓位区间、最终仓位区间、触发 gate、thesis 状态、风险事件、估值状态、主要解释因子、配置版本、数据质量引用和 evidence bundle 引用；重复运行同一 signal_date 具备幂等策略；实现时同步更新 `docs/system_flow.md` 并补充快照测试|来自 2026-05-04 反馈闭环强化讨论；是 `FEEDBACK-002`、规则校准和后续执行复盘的前置条件。2026-05-04: 从 READY 改为 IN_PROGRESS，原因：开始实现 score-daily 决策快照写入、trace 关联和幂等测试。2026-05-04: 从 IN_PROGRESS 改为 DONE，原因：score-daily 已写入包含评分、置信度、仓位、gate、质量、估值、风险和 trace 引用的 JSON 快照；`python -m pytest -q` 通过 220 项测试。|
|FEEDBACK-002|反馈闭环/评分校准|P1|DONE|系统实现|基于 `decision_snapshot` 生成 1D/5D/20D/60D/120D 结果观察和评分校准报告；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#feedback-002`|输出 `decision_outcomes` 和校准报告，按总分分桶、置信度分桶、gate 状态、thesis 状态、风险等级和估值状态统计未来收益、回撤、波动、胜率和相对 SPY/QQQ/SMH/SOXX 或 AI proxy 的超额收益；报告声明样本数量、市场阶段、数据质量状态和样本不足限制；实现时同步更新 `docs/system_flow.md` 并补充 outcome 计算测试|2026-05-04: 已完成首版实现，新增 `aits feedback calibrate`，复用数据质量门禁，从本地 decision snapshot 与 prices_daily 生成 outcome CSV 和中文校准报告；支持总分/置信度/gate/thesis/风险/估值分桶，并明确样本不足、窗口重叠和不得自动修改生产规则。|
|UNIVERSE-001|观察池/回测正确性|P0|DONE|系统实现|已建立 point-in-time watchlist lifecycle、校验命令和回测 signal_date 过滤|每个进入评分、回测或报告的 ticker 都有 `added_at`、`removed_at`、`reason`、`active_from`、`active_until`、`competence_status`、`node_mapping_valid_from` 和 thesis 要求可见日期；回测只能使用 signal_date 当时已进入观察池且映射有效的标的；实现时同步更新 `docs/system_flow.md` 并补充防幸存者偏差测试|优先级列为 P0，原因：扩展 universe 后如果没有 point-in-time 生命周期，回测会产生幸存者偏差并误导投资解释。2026-05-04: 从 READY 改为 IN_PROGRESS，原因：开始实现 watchlist lifecycle schema、校验、回测按 signal_date 过滤和防幸存者偏差测试。2026-05-04: 从 IN_PROGRESS 改为 DONE，原因：默认 lifecycle、`aits watchlist validate-lifecycle`、回测过滤、系统流图和防幸存者偏差测试已完成；`python -m pytest -q` 通过 220 项测试。|
|DATA-002|数据源/质量|P1|BASELINE_DONE|系统实现 + 项目 owner|已完成低成本初版：基于现有数据源目录和 download manifest 建立 provider health score、cache/manifest 检查和 reconciliation 覆盖报告；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#data-002`|初版可验收为 `BASELINE_DONE`：对价格、估值、财报和宏观数据输出 provider 新鲜度、字段漂移、row count 异常、checksum 变化、ticker alias 风险和可用来源差异；跨供应商冲突进入调查项而不是自动平滑；无合格第二来源的域必须标记 reconciliation 未覆盖；严重问题能停止下游或降级结论；实现时同步更新 `docs/system_flow.md` 并补充数据源健康测试|2026-05-04: 从 BLOCKED_OWNER_INPUT 改为 READY，原因：owner 确认先搭低成本框架版，后续再按需订阅 Intrinio、Sharadar、Databento 等生产级来源。2026-05-04: 从 READY 改为 IN_PROGRESS，原因：开始实现 `aits data-sources health`、provider health score、cache/manifest 检查和 reconciliation 未覆盖声明。2026-05-04: 从 IN_PROGRESS 改为 BASELINE_DONE，原因：新增数据源健康报告、checksum mismatch 失败、cache/manifest/freshness 检查、qualified source 覆盖表和测试；完整 `DONE` 仍依赖 owner 提供具体 API key、商业授权/再分发限制和长期口径策略。|
|SCENARIO-001|风险/压力测试|P1|PROPOSED|系统实现|建立 AI 产业链情景压力测试库和节点影响映射；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#scenario-001`|每个场景能映射到产业链节点、ticker、thesis、风险事件和仓位 gate；报告输出当前组合在云 CapEx 下修、GPU 毛利压缩、HBM 供给、先进封装、出口管制、地缘、利率、估值压缩、电力限制和 AI 应用需求不及预期等场景下的脆弱点、观察条件和人工复核要求；实现时同步更新 `docs/system_flow.md` 并补充场景映射测试|与 `BACKTEST-001` 互补：回测回答历史表现，场景压力测试回答当前组合在指定冲击下的暴露|
|CATALYST-001|事件/报告|P1|PROPOSED|系统实现 + 项目 owner|建立 upcoming catalyst calendar 和事件前/后复核模式；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#catalyst-001`|日报显示未来 5/20/60 天重要财报、指引、宏观、CapEx、芯片发布、监管、出口管制和行业会议等催化剂；重大事件前可触发 `pre_event_review`、禁止主动加仓或结论降级；事件后要求复核相关 thesis、risk event 或 valuation state；实现时同步更新 `docs/system_flow.md` 并补充日历和报告测试|自动化数据源需 owner 确认；第一版可从人工复核日历开始，但必须记录来源和采集时间|
|PORTFOLIO-002|组合风险/解释|P1|PROPOSED|系统实现|建立 AI 仓位的 ticker、产业链节点、地区、客户链、因子、ETF beta、相关性簇和集中度暴露分解；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#portfolio-002`|日报显示当前 AI 仓位不是只显示百分比，还显示单票集中度、节点暴露、地区暴露、客户链暴露、估值/成长因子、ETF beta、相关性簇和主要拥挤风险；第一版只做解释层，不直接改变仓位；实现时同步更新 `docs/system_flow.md` 并补充暴露计算测试|与 `SCORE-003` 有重叠但不重复；该任务先提供解释和输入数据，后续由 `SCORE-003` 决定是否进入风险预算约束|
|EXEC-001|执行纪律/报告|P1|PROPOSED|系统实现|设计 advisory execution policy，区分“仓位建议变化”和“是否值得执行调仓”；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#exec-001`|日报输出执行建议章节；再平衡阈值、最小调仓幅度、冷却期、禁止追涨、事件前不新增仓位、连续信号确认和高波动期降低交易频率等规则可配置、可回测、可审计；小幅变化不触发交易建议；实现时同步更新 `docs/system_flow.md` 并补充执行纪律测试|系统不是自动交易，但需要避免用户把每日小幅仓位区间变化误当成机械交易指令|
|COST-001|回测/执行成本|P2|PROPOSED|系统实现|扩展交易摩擦模型；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#cost-001`|回测支持 bid-ask spread、滑点、市场冲击、汇率、税费、融资利率、ETF 申赎或成交延迟、最小交易单位；报告显示成本敏感性，run manifest 和 evidence bundle 记录成本假设|当前回测已有基础单边成本和线性滑点；该任务有价值但不应挤占 P1 的 point-in-time、反馈闭环和数据源健康任务|
|PROXY-001|回测/基准|P1|PROPOSED|系统实现|定义 AI proxy basket 和 benchmark policy；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#proxy-001`|报告说明为什么使用 SPY、QQQ、SMH、SOXX 或自定义 AI basket；回测和校准可区分半导体 beta、纳指 beta、AI 主题 beta 和系统动态仓位价值；自定义 basket 必须 point-in-time 维护；实现时同步更新 `docs/system_flow.md` 并补充基准治理测试|避免把 SMH 这类半导体 ETF 表现误解释成完整 AI 产业链判断；与 `UNIVERSE-001` 协同|
|GOV-001|治理/规则管理|P1|PROPOSED|系统实现 + 项目 owner|建立 production / candidate / retired rule 生命周期和 rule card；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#gov-001`|scoring、position gate、risk event、valuation gate、thesis 状态机等核心规则有 rule card；每次规则变更可追溯到规则版本、适用范围、上线原因、验证报告、已知失败模式、最后复核时间、批准记录和回滚方式；`score-daily`、回测和 decision snapshot 记录 rule version；实现时同步更新 `docs/system_flow.md` 并补充规则版本测试|反馈闭环可提出改进，但不能无审计地自动改生产规则|
|OPS-001|运维/可靠性|P2|PROPOSED|系统实现|建立 pipeline health report；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#ops-001`|每次运行记录成功/失败、耗时、输入文件版本、质量门禁状态、API 错误、缺失数据和报告生成状态；失败时生成明确排查入口和日志引用；运行成功不等于投资结论有效|适合在日常自动化运行稳定后推进；当前优先级低于数据质量和投资解释正确性|
|ALERT-001|报告/告警|P2|PROPOSED|系统实现|定义 data/system 与 investment/risk 告警 schema 和触发规则；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#alert-001`|告警有等级、来源、触发条件、解除条件、对应 claim/evidence 引用和去重策略；覆盖数据源断更、估值快照过期、risk event 升级、L2/L3 风险触发、thesis 状态恶化、仓位上限大幅下降和未来重大事件；日报输出告警摘要|适合在报告、event、thesis 和 catalyst 触发条件稳定后推进，避免早期噪声过高|
|TEST-001|测试/正确性|P1|DONE|系统实现|已建立跨模块 system invariant test suite；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#test-001`|每条核心投资原则都有测试：`watch` 风险事件不直接重扣仓位、`public_convenience` 不自动评分、LLM 输出不直接影响仓位、`position_gate` 取最严格上限、回测不能使用未来估值快照、SEC 指标 `filed_date <= signal_date`、估值采集日之后才可见、数据质量未通过不能输出完整结论；新增关键规则时同步补测试或说明不适用原因|保护系统设计边界，防止后续改动破坏 source policy、point-in-time、gate 优先级、LLM 隔离和 watch/active 区分。2026-05-04: 从 READY 改为 IN_PROGRESS，原因：开始补充覆盖 source policy、PIT、gate、LLM 隔离和 watch/active 边界的系统不变量测试。2026-05-04: 从 IN_PROGRESS 改为 DONE，原因：新增 watch/active、public_convenience、LLM evidence、watchlist PIT、decision snapshot、confidence、valuation PIT 和 thesis 状态机测试；`python -m pytest -q` 通过 220 项测试。|
|STORAGE-001|数据工程|P3|PROPOSED|系统实现|评估从 CSV 逐步升级到 DuckDB / Parquet 的内部存储方案；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#storage-001`|不破坏现有 CSV 输入输出；内部可用 typed schema 存储；报告和回测仍可追溯 checksum、数据版本和来源；迁移计划包含兼容期、回滚方式和数据验证|当前 CSV 对本地 MVP 仍足够透明；当 schema 演进、join 性能、历史快照或类型约束成为实际瓶颈时再升级|
|UI-001|仪表盘/解释|P2|PROPOSED|系统实现|设计 evidence-first dashboard；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#ui-001`|仪表盘可从结论下钻到模块分、gate、risk event、thesis、数据源、原始证据和质量报告；支持本期/上期差异、仓位限制原因和未来观察条件；不替代 Markdown 报告的审计责任|应等 `REPORT-001` evidence bundle、`REPORT-002` 变化原因树和结论等级稳定后推进|
|SECURITY-001|安全/密钥|P2|PROPOSED|系统实现 + 项目 owner|建立 secret hygiene、日志脱敏和供应商权限治理；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#security-001`|API key 不进入报告、日志、manifest、trace bundle 或错误输出；支持 secret scan 或等价检查；供应商权限、缓存限制、再分发限制和 LLM 处理限制记录在 data source catalog；新数据源接入必须通过安全与权限检查|当前已有 FMP key 从环境变量读取且不写入报告的基础约束；扩展供应商后需要系统化治理|
|DOC-001|产品边界/解释|P2|PROPOSED|系统实现 + 项目 owner|定义 recommendation status taxonomy；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#doc-001`|每份日报和回测报告都有结论等级：`actionable`、`review_required`、`research_only`、`data_limited`、`backtest_limited` 或等价分类；低置信度、数据不足、来源不足、回测覆盖不足和事件前状态自动降级，并说明原因、解除条件和证据引用；实现时同步更新 `docs/system_flow.md` 并补充报告测试|与 `SCORE-002` 置信度拆分和 `REPORT-002` 解释输出协同；减少用户把所有分数都当成同等可靠结论的风险|
|RISK-001|风险事件|P1|DONE|系统实现|已扩展 `risk_event_occurrence` schema，增加 `evidence_grade`、`severity`、`probability`、`scope`、`time_sensitivity`、`reversibility`、`action_class`|校验报告显示来源可信度和风险本体分层；低可信高严重事件进入人工复核但不自动大幅扣分；高可信高严重事件可触发 position gate|保持向后兼容或提供迁移模板。2026-05-04: 从 READY 改为 IN_PROGRESS，原因：开始扩展 occurrence schema、导入模板、校验报告和评分资格规则。2026-05-04: 从 IN_PROGRESS 改为 DONE，原因：schema、CSV 导入、模板、校验/复核报告、评分资格规则和测试均已完成；`python -m pytest -q` 通过 220 项测试。|
|RISK-002|风险事件|P1|DONE|系统实现|已区分 `watch` 与 `active` 对评分和仓位的影响|`watch` 默认进入报告和人工复核；只有达到证据等级或确认条件后才进入自动扣分或仓位闸门；回测 point-in-time 切片保持一致|当前 `active_or_watch_l2_count` 和 `active_or_watch_l3_count` 会把两类状态合并评分。2026-05-04: 从 READY 改为 IN_PROGRESS，原因：开始把 watch 发生记录改为默认不可自动评分，只进入报告和人工复核。2026-05-04: 从 IN_PROGRESS 改为 DONE，原因：policy/geopolitics 评分和 risk_events gate 只使用可评分 active occurrence，watch 只进入报告/人工复核；`python -m pytest -q` 通过 220 项测试。|
|SOURCE-001|数据源/风险|P1|BLOCKED_OWNER_INPUT|项目 owner + 系统实现|确认可接受的风险证据等级和供应商/一手来源策略|形成 source policy：S/A/B/C/D/X 或等价等级；明确哪些来源可自动评分、哪些只能 report/manual review；更新数据源目录和导入校验|需要 owner 判断可购买或可长期使用的数据源|
|RISK-003|风险事件数据源|P1|BLOCKED_OWNER_INPUT|项目 owner|提供或选择正式风险事件来源：监管公告、公司披露、付费新闻/事件供应商或人工复核流程|风险事件发生记录能从更可信来源或结构化人工流程进入；报告记录 provider、endpoint/来源、captured_at、row count/checksum 或人工复核人|当前 CSV/YAML 机制已能审计，但真实来源仍依赖人工输入|
|LLM-001|新闻/LLM|P1|PROPOSED|系统实现 + 项目 owner|先设计低风险新闻/NLP/LLM 证据分类器 schema 和来源策略，只做信息抽取、节点映射、证据归档和人工复核提示；详细拆解见 `docs/requirements/decision_support_improvements_2026-05-04.md#llm-001`|LLM 输出必须是结构化证据分类结果，并保留原始来源、采集时间和可追溯引用；报告区分 LLM 辅助抽取、人工确认和自动评分输入；没有人工确认或明确证据等级前，LLM 输出不得直接改变仓位建议；实现时同步更新 `docs/system_flow.md` 并补充防止 LLM 交易建议直连评分的测试|来自 2026-05-04 新闻/LLM 接入边界讨论；明确禁止 LLM 直接输出看多/看空/加仓/减仓作为交易评分|
|VALUATION-001|估值/仓位|P1|DONE|系统实现|把估值拥挤从单纯评分升级为新增仓位限制或禁止加仓 gate|估值过热时日报输出“趋势仍强但新增仓位受限”等结论；回测使用同一规则；不把估值拥挤误写成基本面证伪|2026-05-04: 已完成，valuation `position_gate` 在共享 gate 层按 `EXPENSIVE_OR_CROWDED` / `EXTREME_OVERHEATED` 限制最终 AI 仓位，日报和回测共用同一最终仓位约束；补充测试验证估值拥挤 gate 压低仓位且不被误写成 thesis 证伪。|
|VALUATION-002|估值/数据可信度|P1|DONE|系统实现|已把估值输入显式分为真实 point-in-time、采集日快照和回填历史分布，并在估值复核、日报和回测审计中输出可信度等级和限制原因；详细拆解见 `docs/requirements/decision_support_improvements_2026-05-04.md#valuation-002`|日报、估值复核和回测审计显示估值分位可信度：高/中/低及原因；严格回测不能使用采集日之后才可见的估值回填数据得出历史时点结论；`eps_revision_90d_pct` 只允许真实可审计历史 analyst 快照支持；实现时同步更新 `docs/system_flow.md` 并补充估值校验和回测切片测试|来自 2026-05-04 估值历史快照伪 point-in-time 风险讨论；与 `BTINPUT-001`、`DATA-001` 协同。2026-05-04: 从 READY 改为 IN_PROGRESS，原因：开始实现估值 PIT 可信度字段、FMP historical 低可信标签、报告输出和切片测试。2026-05-04: 从 IN_PROGRESS 改为 DONE，原因：valuation snapshot、CSV/FMP 来源、校验、复核、日报、decision snapshot、回测输出和测试均已覆盖 PIT 可信度；`python -m pytest -q` 通过 220 项测试。|
|BTINPUT-001|回测输入覆盖|P1|READY|系统实现|补齐 AI regime 回测中的历史估值快照和风险事件发生记录覆盖；先实现可审计导入/种子数据工作流，并明确哪些数据能真实 point-in-time 进入回测|在提供合格历史估值/风险事件输入后，`backtest_audit_2022-12-01_2026-05-01.md` 中 `valuation` 和 `policy_geopolitics` 覆盖率不再长期为 0%；审计报告能区分“历史确无事件”和“事件库未建设”；估值历史不能用采集日伪造过去可见性；新增或更新导入模板、校验报告和测试|来自 2026-05-04 TSM IR 回填后的下一步建议；当前回测仍显示估值快照和风险事件发生记录全区间为空，限制投资解释力|
|BACKTEST-001|回测/稳健性|P1|READY|系统实现|实现防过拟合报告，覆盖参数敏感性、不同起止日期、再平衡频率、交易成本、仓位区间映射、模块权重扰动，以及 SMH/QQQ/固定 60% AI 仓位/趋势-only/趋势+风险情绪/同换手率随机策略等 baseline；详细拆解见 `docs/requirements/decision_support_improvements_2026-05-04.md#backtest-001`|报告声明市场阶段、实际请求日期范围和数据质量状态；所有实验使用相同 point-in-time 输入和交易成本假设；能说明动态系统价值来自绝对收益、回撤控制、尾部风险降低、仓位纪律或心理压力降低；实现时同步更新 `docs/system_flow.md` 并补充 baseline 和扰动实验测试|来自 2026-05-04 回测防过拟合讨论；优先默认使用 `ai_after_chatgpt` 窗口|
|MACRO-001|宏观/仓位|P2|READY|系统实现|把宏观流动性恶化映射到总风险资产预算，而不是只影响 AI 模块分数|当 VIX、利率、美元等触发流动性风险时，总风险资产预算可下调；AI 在风险资产内部的相对权重仍可单独解释|需要和 `config/portfolio.yaml` 的静态预算保持兼容|
|THESIS-001|交易 thesis|P1|DONE|系统实现|已把 thesis 从结构校验/复核升级为状态机：`draft -> active -> warning -> challenged -> invalidated -> closed`，并把 `warning/challenged/invalidated` 作为日报解释和 `position_gate` 候选输入；详细拆解见 `docs/requirements/decision_support_improvements_2026-05-04.md#thesis-001`|thesis 报告显示当前状态、上次状态、触发原因、证据来源、相关 ticker/产业链节点和人工复核要求；`draft` 不进入正式评分，`warning` 至少触发禁止主动加仓，`challenged/invalidated` 能进入仓位限制或人工复核流程；实现时同步更新 `docs/system_flow.md` 并补充 schema、复核、日报和回测相关测试|2026-05-04: 从 P2/PROPOSED 升级为 P1/READY，原因：状态机比单纯总分下降更能解释 thesis 承压、证伪、估值过高和基本面恶化的差异。2026-05-04: 从 READY 改为 IN_PROGRESS，原因：开始扩展 thesis 状态机字段、状态复核规则和仓位闸门联动。2026-05-04: 从 IN_PROGRESS 改为 DONE，原因：状态机 schema、状态迁移校验、复核健康状态、日报人工复核摘要、position_gate 联动、模板和测试均已完成；`python -m pytest -q` 通过 220 项测试。|
|DATA-001|估值数据|P1|BLOCKED_EXTERNAL|系统 + 时间窗口|等待 FMP 估值快照达到足够历史样本，analyst estimates 覆盖 90 天窗口|`valuation_percentile` 至少有 3 个历史点；`eps_revision_90d_pct` 有稳定 90 天历史；回测审计说明历史可见性|基础拉取、历史缓存和校验已完成，当前主要受样本积累限制|
|AUDIT-001|回测审计|P2|VALIDATING|系统实现|用真实历史数据持续检查 `backtest_audit_*.md` 和覆盖诊断是否足够可解释|如果按月聚合不足以定位问题，拆分或补充更细粒度诊断；若足够，则保持当前格式|当前基础版已输出机器可读覆盖诊断和中文审计报告|

## 更新模板

新增任务时使用下面格式补充到“当前任务”表：

```text
ID:
领域:
优先级:
状态:
下一责任方:
阻塞或下一步:
验收标准:
备注:
```

状态更新时至少记录：

```text
YYYY-MM-DD: 从 <旧状态> 改为 <新状态>，原因：<触发条件、实现范围、验证结果或 blocker>。
```
