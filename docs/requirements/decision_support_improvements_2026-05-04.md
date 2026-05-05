# 决策支持解释与稳健性改进需求

状态：PROPOSED

最后更新：2026-05-04

关联任务：`THESIS-001`、`LLM-001`、`RISK-004`、`BACKTEST-001`、`VALUATION-002`、`REPORT-002`

## 背景

本轮评估来自 2026-05-04 的系统优化讨论。核心目标不是提高模型复杂度本身，而是提升投资解释、审计可信度、回测稳健性和人工复核效率。所有改进都必须继续优先服务 `ai_after_chatgpt` 市场阶段，且不能削弱数据质量门禁、point-in-time 约束和来源审计。

结论：五个建议都有长期建设价值，应进入待办。其中估值 point-in-time 可信度和防过拟合报告直接影响回测解释风险，优先级较高；thesis 状态机和变化原因树能显著改善执行解释；LLM 接入只应作为证据分类器推进，不应进入自动交易评分。

## THESIS-001

标题：交易 thesis 状态机

价值判断：值得做。当前 thesis 校验和复核能检查结构、观察池引用、产业链节点、验证指标、证伪条件、复核新鲜度和已触发风险，但不能把“短期波动”“thesis 承压”“证伪触发”“估值过高但基本面未坏”“基本面恶化”区分成稳定状态。状态机能提高报告解释力，也能作为 `position_gate` 的可审计输入。

建议状态：

|状态|含义|仓位影响|
|---|---|---|
|draft|假设未完整|不进入正式评分|
|active|假设有效|正常参与仓位建议|
|warning|出现轻微信号冲突|禁止主动加仓|
|challenged|关键验证指标恶化|相关 ticker、节点或 AI bucket 仓位上限下降|
|invalidated|证伪条件触发|建议退出或人工复核|
|closed|交易结束|进入复盘归因|

分步开发：

1. 扩展 thesis schema，新增显式 `status`、`status_updated_at`、`status_reason`、`status_evidence`、`manual_review_required` 字段。
2. 在 `aits thesis validate/review` 中检查状态迁移合法性、证据引用、复核时效和证伪条件。
3. 将 `warning/challenged/invalidated` 输出为日报解释和 `position_gate` 候选输入，避免只靠总分下降表达 thesis 风险。
4. 在交易复盘中把 `closed` thesis 纳入归因，记录从建仓到结束的关键状态迁移。

验收标准：

- thesis 报告能显示当前状态、上次状态、触发原因、证据来源、相关 ticker/产业链节点和人工复核要求。
- `draft` 不进入正式评分；`warning` 至少触发禁止主动加仓；`challenged/invalidated` 能进入仓位限制或人工复核流程。
- 状态迁移必须可审计，不能由未通过校验的数据静默驱动。
- 实现时同步更新 `docs/system_flow.md`，并补充 schema、复核、日报和回测相关测试。

开放问题：

- 自动迁移规则应做到什么粒度，哪些状态只能人工确认。
- `challenged` 对 ticker、产业链节点和整体 AI bucket 的仓位上限映射应由固定规则还是配置控制。
- 旧 thesis 数据是否需要迁移脚本或兼容读取。

## LLM-001

标题：新闻/NLP/LLM 证据分类器

价值判断：有价值，但必须严格限权。LLM 不应直接输出看多、看空、加仓或减仓；它适合做信息抽取、节点映射、证据归档和人工复核提示。这样能提升处理效率，同时避免把交易评分变成黑箱。

2026-05-04 owner 决策：

- OpenAI API 的主要用途确定为关键信息校验、新闻/公告抽取、ticker/产业链节点映射、风险事件候选分类、重复线索合并和人工复核问题生成。
- 项目级使用目的限定为 owner 个人投资决策支持，不提供给其他人，不做对外再分发。
- 付费新闻和供应商内容可以进入 LLM 预审，但每个 provider 必须显式记录是否允许发送给外部 LLM API、是否允许缓存、是否允许摘要进入报告，以及授权依据；授权未知时不得发送全文或长摘录。
- LLM 输出统一保持 `llm_extracted` / `pending_review`，人工确认前不得作为自动评分、仓位闸门、thesis 状态迁移或日报可执行结论的输入。
- 预审 schema 采用“source-permission envelope + claim-centric payload”为通用底座；风险事件继续使用 `risk_event_candidate` 专用 payload。
- OpenAI 默认请求策略统一为 `gpt-5.5` 和 `reasoning.effort=high`，单请求失败最多重试 2 次；命令可显式覆盖 model/reasoning/timeout 用于对比实验，但报告和队列必须记录实际 model 与 reasoning effort。

允许任务：

- 判断新闻或公告影响哪个产业链节点。
- 区分短期情绪、长期现金流、供需、政策、竞争格局或估值影响。
- 判断信息是新增、确认、重复还是冲突。
- 判断是否触发已有 `risk_event` 规则、thesis 验证指标或证伪条件。
- 标记证据来源等级、来源类型、时间敏感性和是否需要人工复核。

禁止任务：

- 直接输出看多/看空结论。
- 直接输出买入、卖出、加仓、减仓建议。
- 在没有结构化证据和人工复核门槛时直接修改评分或仓位。

OpenAI API 使用边界：

- API 调用必须使用固定结构化输出 schema，优先采用 Responses API + Structured Outputs；非紧急批量分类可使用 Batch API。
- 默认请求不应要求 OpenAI 存储供应商内容；不得把付费全文写入 Assistants/Threads/Vector Stores/Files 或外部工具链。
- 本地运行记录必须保存 model、reasoning effort、prompt version、request id、request timestamp、输入 checksum、输出 checksum、source URL、provider、授权标记和是否包含付费供应商内容。
- 报告不得复制付费供应商长文或完整原文，只能输出来源引用、短摘要、结构化字段和人工确认结论。
- OpenAI 官方边界参考：API 数据默认不用于训练，但客户仍需确保自己有权把 Input 提供给服务；金融高影响场景必须保留人工复核，不得自动作出交易决策。

风险事件第一版落地边界：

- OpenAI API 可作为风险事件预审助手，使用 Responses API + Structured Outputs 生成固定 JSON schema；夜间或非紧急批量分类可使用 Batch API。
- 预审输出只能作为 `llm_extracted` / `pending_review` 线索，不能替代实际人工复核。
- 预审可建议 `risk_id` 匹配、`watch/active` 候选、L1/L2/L3 候选、影响 ticker/产业链节点和人工复核问题，但这些字段均不得自动写入可评分发生记录。
- L2、L3、`position_gate_eligible`、来源冲突、低置信度或无法追到一手来源的候选事件，必须进入人工复核；L3 或仓位闸门候选需要投资 owner 与系统/数据 owner 双确认。
- 详细流程和实施拆解见 `docs/requirements/risk_event_review_workflow_2026-05-04.md`。

预审 schema 确定方案：

第一版通用预审记录由两层组成。

### Source permission envelope

该层先决定内容能不能发送给外部 LLM API。建议字段：

|字段|含义|
|---|---|
|`provider`|来源 provider 或发布主体|
|`source_type`|`primary_source`、`paid_vendor`、`manual_input`、`public_convenience` 等|
|`license_scope`|订阅或来源授权范围|
|`personal_use_only`|是否限定 owner 个人使用|
|`external_llm_allowed`|是否允许发送给外部 LLM API；未知视为 false|
|`cache_allowed`|是否允许本地保存输入片段或仅保存 checksum|
|`redistribution_allowed`|是否允许在报告中展示摘要或引用|
|`content_sent_level`|`metadata_only`、`short_excerpt`、`summary_only`、`full_text`|
|`approval_ref`|授权依据、人工批准或供应商条款引用|

如果 `external_llm_allowed=false` 或未知，系统不得发送供应商全文或长摘录给 OpenAI API；只能进入人工复核，或在授权允许时发送元数据/人工摘要。

### Claim-centric payload

该层只承载 LLM 抽取出的待复核事实断言。建议字段：

|字段|含义|
|---|---|
|`precheck_id`|稳定预审记录 ID|
|`claims`|结构化 claim 数组|
|`claim_id`|单条 claim 的稳定 ID|
|`claim_text_zh`|中文事实断言，不得写成交易建议|
|`source_span_ref`|原文位置、段落编号或可审计片段引用|
|`published_at` / `captured_at`|发布时间和采集时间|
|`affected_tickers` / `affected_nodes`|影响 ticker 和产业链节点|
|`claim_type`|`risk_event`、`thesis_signal`、`catalyst`、`fundamental`、`valuation`、`supply_chain`、`macro` 等|
|`novelty`|`new`、`confirming`、`duplicate`、`conflicting`、`unclear`|
|`impact_horizon`|`intraday`、`short_term`、`medium_term`、`long_term`、`unclear`|
|`evidence_grade_suggestion`|仅供人工参考，不得自动写成最终等级|
|`confidence`|模型分类置信度|
|`conflicts_or_uncertainties`|冲突、不确定点和缺失来源|
|`required_review_questions`|人工复核必须回答的问题|
|`manual_review_status`|固定为 `pending_review`|
|`prohibited_actions_ack`|确认不得直接评分、不得触发仓位闸门|

### Risk-event candidate payload

风险事件仍使用更贴近 `RISK-004` 的专用 payload：`risk_id_candidate`、`status_candidate`、`level_candidate`、`severity_candidate`、`probability_candidate`、`scope_candidate`、`time_sensitivity_candidate`、`action_class_candidate`、`missing_confirmations`、`review_questions`。所有字段都是 candidate，不得自动写入正式 `risk_event_occurrence`。

分步开发：

1. 在数据源目录中增加 LLM 处理授权字段，覆盖 provider、授权范围、外部 LLM 处理、缓存、报告摘要和批准引用；授权未知时 fail closed。
2. 设计并实现通用 `source-permission envelope + claim-centric payload` schema，覆盖 source、published_at、captured_at、ticker、industry_chain_node、evidence_grade_suggestion、novelty、impact_horizon、risk_event_match、thesis_signal_match、manual_review_required。
3. 为风险事件保留 `risk_event_candidate` 专用 schema，并和现有 `risk_event_prereview_queue` 对齐。
4. 输出 evidence card 或结构化待复核队列，只进入报告和人工复核，不直接进入交易评分。
5. 经人工复核、供应商授权验证和回归测试稳定后，再评估是否允许人工确认后的高等级结构化证据触发既有 risk event 或 thesis 状态迁移。

验收标准：

- LLM 输出必须是结构化证据分类结果，并保留原始来源、采集时间和可追溯引用。
- 报告能区分 LLM 辅助抽取、人工确认和自动评分输入。
- 没有人工确认或明确证据等级前，LLM 输出不得直接改变仓位建议。
- 实现时同步更新 `docs/system_flow.md`，并补充防止 LLM 交易建议直连评分的测试。

开放问题：

- 新闻或公告数据源、授权范围和成本。
- 具体供应商条款如何记录为 `approval_ref`，以及哪些 provider 只能发送 metadata 或人工摘要。
- 人工复核队列的 owner、SLA 和留痕方式。

## BACKTEST-001

标题：防过拟合报告

价值判断：值得优先做。现有回测指标已经覆盖 CAGR、Max Drawdown、Sharpe、Sortino、Calmar、Turnover、Time in Market 和相对 SPY/QQQ/SMH/SOXX 的超额收益，但仍不足以判断动态系统是否只是半导体 beta、参数偶然性或复杂模块堆叠。防过拟合报告能把“收益增强”和“回撤控制/纪律改善”分开解释。

报告范围：

- 参数敏感性分析。
- 不同起点和终点稳定性。
- 不同再平衡频率结果。
- 不同交易成本和滑点假设结果。
- 不同仓位区间映射结果。
- 模块权重扰动结果。
- 与简单基线策略对比。

必须包含的 baseline：

|基线|用途|
|---|---|
|买入持有 SMH|判断是否只是半导体 beta|
|买入持有 QQQ|判断是否只是纳指 beta|
|固定 60% AI 仓位|判断动态仓位是否真有价值|
|只用趋势模块|判断复杂基本面模块是否增益|
|只用趋势 + 风险情绪|判断基本面/估值是否改善回撤|
|随机但同换手率策略|判断信号是否显著优于噪声|

分步开发：

1. 设计稳健性实验配置，允许在 `ai_after_chatgpt` 默认区间内批量运行窗口、成本、频率和权重扰动。
2. 增加 baseline 策略生成器，复用同一价格数据、交易成本和信号生效规则。
3. 输出 Markdown 报告和机器可读 CSV/JSON 摘要，展示每组实验的绩效、回撤、换手率和相对基线差异。
4. 在报告中明确系统价值来源：绝对收益、回撤控制、尾部风险降低、仓位纪律或心理压力降低。

验收标准：

- 报告声明市场阶段、实际请求日期范围和数据质量状态。
- 所有实验使用相同 point-in-time 输入和交易成本假设，且不引入未来函数。
- 如果动态系统跑不过 SMH 买入持有，报告能清楚说明剩余价值是否来自回撤、尾部风险、换手或纪律改善。
- 实现时同步更新 `docs/system_flow.md`，并补充 baseline 和扰动实验测试。

开放问题：

- 随机同换手率策略需要固定随机种子数量和显著性表达方式。
- 模块权重扰动范围应采用固定网格还是配置化。
- 稳健性报告是作为 `aits backtest` 参数，还是新增独立子命令。

## VALUATION-002

标题：估值 point-in-time 可信度分层

价值判断：值得做，并且关系到回测解释风险。当前文档已提醒 FMP historical 接口回填不等同于真实 point-in-time vendor archive，不能伪造 `eps_revision_90d_pct`。下一步应把这种限制变成数据模型、校验和报告中的显式可信度标签，避免用户误读估值分位。

建议分类：

|类别|用途|
|---|---|
|真实 point-in-time|可用于严格回测和评分|
|采集日快照|可用于当前评分，可用于采集日之后的历史|
|回填历史分布|只用于横向参考、当前分位辅助或压力测试，不用于严格历史回测结论|

报告标记：

- 估值分位可信度：高 / 中 / 低。
- 原因：例如“真实历史快照数量不足，仅使用供应商 historical 接口回填”。
- 回测可用性：严格 point-in-time / 采集日后可见 / 仅辅助参考。

分步开发：

1. 扩展 valuation snapshot 或派生元数据，记录 `point_in_time_class`、`history_source_class`、`confidence_level` 和限制原因。
2. 更新估值校验、复核、日报和回测审计，显示估值分位可信度和回测可用边界。
3. 在回测中继续禁止把采集日晚于 signal_date 的历史分布当作当时可见数据。
4. 对 FMP historical 生成的分布增加专门说明，防止被误读为 vendor archive。

验收标准：

- 日报、估值复核和回测审计都能显示估值输入的可信度等级和原因。
- 严格回测不能使用采集日之后才可见的估值回填数据得出历史时点结论。
- `eps_revision_90d_pct` 只允许真实可审计历史 analyst 快照支持，不允许 historical valuation 回填伪造。
- 实现时同步更新 `docs/system_flow.md`，并补充估值校验和回测切片测试。

开放问题：

- 可信度高/中/低的阈值是否只看来源类型，还是同时看历史样本数量和覆盖率。
- 是否需要在 `config/data_sources.yaml` 中集中声明每个 provider endpoint 的 PIT 等级。
- 历史分布用于当前估值辅助时，是否需要单独输出压力测试标签。

## REPORT-002

标题：报告变化原因树

价值判断：值得做。日报已经要求本期建议仓位、上期建议仓位、评分变化、触发项和下一期观察条件，但投资执行更需要解释“为什么仓位变化”以及“什么情况会改变判断”。变化原因树能减少事后解释和情绪化决策，也能把估值、趋势、风险事件和 thesis 状态的不同含义拆开。

目标结构：

```text
本期仓位变化：65%-80% -> 50%-65%

变化原因：
1. 趋势：-4 分，SMH 跌破 50 日均线
2. 风险情绪：-3 分，VIX 分位上升
3. 估值：-2 分，核心观察池估值拥挤
4. thesis：NVDA/AMD 相关 thesis 仍有效，但进入 warning
5. 风险事件：无 L3，但存在 L2 观察项

最终动作：
不主动加仓；已有仓位保留；若跌破 100 日线则降至 40%-50%

什么情况会改变判断：
转为加仓：SMH 收复 50 日线，VIX 回落，估值分位下降，云 CapEx 数据继续确认。
转为减仓：核心 thesis 证伪项触发，L3 风险事件活跃，趋势跌破 100/200 日线。
```

分步开发：

1. 在评分输出中保留上期与本期的模块级分数、仓位区间、gate、thesis 状态和风险事件变化。
2. 生成变化原因树，按趋势、风险情绪、估值、基本面、thesis、风险事件和数据质量归因。
3. 输出最终动作约束，明确主动加仓、保留、减仓、退出、人工复核和下一触发条件。
4. 增加“什么情况会改变判断”章节，按加仓、减仓、保持观察列出可验证条件。

验收标准：

- 报告能解释仓位变化来源，而不是只给总分或最终区间。
- 能区分市场短期波动、thesis warning/challenged、证伪触发、估值过高和基本面恶化。
- 变化原因引用可追溯输入，后续与 `REPORT-001` evidence card 链接兼容。
- 实现时同步更新 `docs/system_flow.md`，并补充报告快照或文本测试。

开放问题：

- 上期状态来源应从历史 `scores_daily.csv` 读取，还是读取最近日报 manifest。
- 原因排序应按分数变化绝对值、仓位影响、风险等级，还是固定模块顺序。
- “改变判断”的条件应完全配置化，还是由各模块输出推荐观察条件。

## 状态记录

- 2026-05-04：创建需求文档，登记并升级相关任务。当前仅完成需求评估和任务拆解，尚未实现。
- 2026-05-04：`THESIS-001` 已完成基础实现：交易 thesis schema 支持 `warning/challenged/invalidated` 状态、前状态、状态更新时间、变化原因、证据引用和人工复核要求；校验状态迁移，日报把 challenged/invalidated 纳入人工复核和 `position_gate` 输入。
- 2026-05-04：`VALUATION-002` 已完成基础实现：估值快照支持 `point_in_time_class`、`history_source_class`、`confidence_level`、`confidence_reason` 和 `backtest_use`；FMP historical 快照标记为低可信回填历史分布，估值校验/复核/日报/回测审计输出 PIT 可信度边界。
- 2026-05-04：`SCORE-002` 已完成基础实现：每日评分和回测将 AI 产业链评分与判断置信度分开输出，`scores_daily.csv`、日报、decision snapshot 和回测明细/报告均记录 confidence 字段与低置信度原因。
- 2026-05-04：`REPORT-002` 已完成首版实现：`scores_daily.csv` 的 overall 行保存模型/最终/置信度调整仓位区间和总资产 AI 仓位区间；日报新增“变化原因树”和“什么情况会改变判断”，从上期 overall 评分记录读取仓位、总分和置信度变化，并按趋势、风险情绪、估值、基本面、thesis、风险事件、数据质量和仓位闸门解释最终动作约束。
- 2026-05-04：补充风险事件 OpenAI 预审边界：OpenAI API 只做结构化预审和人工复核提示，不替代人工确认；`RISK-004` 承接具体实现，完整流程见 `docs/requirements/risk_event_review_workflow_2026-05-04.md`。
- 2026-05-04：owner 确认 `LLM-001` 使用边界：OpenAI API 用于关键信息校验和结构化预审；付费新闻/供应商内容可在 owner 个人使用目的下处理，但必须有 provider 级外部 LLM 授权标记；预审 schema 确定为通用 `source-permission envelope + claim-centric payload`，风险事件沿用专用 candidate payload。
- 2026-05-04：`LLM-001` 进入实现。owner 已把 OpenAI API key 设置为环境变量；剩余阻塞项按保守默认落地：第一阶段真实样本优先使用官方/一手来源和 owner 手工摘要，provider 授权未知时不得发送全文或长摘录，所有 LLM 输出只进入 `llm_extracted` / `pending_review` 队列。
- 2026-05-04：`LLM-001` 第一阶段达到 `BASELINE_DONE`：新增 provider 级 `llm_permission` schema、`aits llm precheck-claims`、OpenAI Responses API `store=false` 结构化调用、claim-centric 待复核队列、中文审计报告、输入模板和隔离测试；未执行真实生产样本调用，完整运行仍依赖 owner 批准样本、provider 授权条款覆盖和人工复核 SLA。
- 2026-05-04：`LLM-001` 再次进入实现：owner 指定 OpenAI 默认请求策略统一升级为 `gpt-5.5` 和 `reasoning.effort=high`；实现必须保持 Responses API Structured Outputs、`store=false`、provider 权限 fail closed、LLM 输出不得直连评分/仓位闸门，并把 reasoning effort 纳入请求审计、报告和待复核队列。
- 2026-05-04：`LLM-001` 模型策略更新达到 `BASELINE_DONE`：`aits llm precheck-claims` 和 `aits risk-events precheck-openai` 默认使用 `gpt-5.5` 与 `reasoning.effort=high`，CLI 可显式覆盖；claim/risk 预审队列、中文报告、数据源目录、示例和系统流图均记录 reasoning effort；`.venv\Scripts\python.exe -m pytest -q` 通过 311 项测试，`ruff check config src tests` 通过。
- 2026-05-05：owner 批准 OpenAI 单请求失败最多重试 2 次；第 3 次仍失败时保持 fail closed，不写部分队列、不进入评分或仓位闸门。
- 2026-05-04：`BACKTEST-001` 已完成第一阶段基础实现：`aits backtest` 新增 `--robustness-report` / `--robustness-report-path`，可生成中文回测稳健性报告，复用同一 point-in-time 输入对比基础动态策略、成本压力、起点后移和买入持有基准；报告明确 `production_effect=none`，完整防过拟合仍需权重扰动、固定仓位/趋势-only/随机基线、机器可读摘要和样本外验证。
- 2026-05-04：`BACKTEST-001` 第二阶段进入实现：增加固定 60% 总资产 AI exposure
  基线和机器可读 JSON 摘要；基线复用基础回测的下一交易日收益和显式成本假设，不读取额外数据；
  JSON 摘要输出 base/scenario/benchmark 指标、相对基础收益差、市场阶段和剩余缺口，便于后续复盘自动引用。
- 2026-05-04：`BACKTEST-001` 第二阶段基础版已完成：`aits backtest --robustness-report`
  会同时生成 Markdown 报告和 `.json` 摘要，新增 `--robustness-summary-path` 可单独指定机器可读输出；
  真实短区间验证显示固定 60% 总资产 AI exposure 在 2026-04-01 至 2026-05-01 样本中跑赢动态策略，
  因此当前动态仓位价值仍不能解释为收益增强，需要继续做趋势-only、趋势+风险情绪、权重扰动、随机同换手率和样本外实验。
- 2026-05-05：`BACKTEST-001` 第三阶段进入实现：补充再平衡频率稳健性场景，先覆盖每 5 个交易日和每 21 个交易日再平衡；该实验复用基础回测的每日目标仓位、下一交易日收益、显式成本假设和 PIT 输入，不新增数据源，也不改变 production scoring 或 `position_gate`。
- 2026-05-05：`BACKTEST-001` 第三阶段达到 BASELINE_DONE：`backtest_robustness` 默认新增 `rebalance_every_5d` 和 `rebalance_every_21d` 场景，Markdown 与 JSON 摘要均输出收益、回撤、Sharpe、换手和相对基础收益；系统流图和测试已同步，Ruff 与全量 pytest 371 项通过。剩余缺口继续保留为权重扰动、趋势-only/趋势+风险情绪、随机同换手率和样本外验证。
- 2026-05-05：`BACKTEST-001` 第四阶段进入实现：补充趋势-only 与趋势+风险情绪基线；基线复用基础回测已保存的模块分数、`config/scoring_rules.yaml` 模块权重、每日宏观总风险资产预算、下一交易日收益、最小调仓阈值和显式成本假设，不新增数据源，不重跑 production scoring，也不改变日报或 `position_gate`。
- 2026-05-05：`BACKTEST-001` 第四阶段达到 BASELINE_DONE：`backtest_robustness` 默认新增 `trend_only_baseline` 和 `trend_plus_risk_sentiment_baseline` 场景，Markdown 与 JSON 摘要均输出简化信号族相对基础动态策略的收益、回撤、Sharpe 和换手差异；系统流图和测试已同步，Ruff 与回测测试通过。剩余缺口继续保留为权重扰动、随机同换手率和样本外验证。
- 2026-05-05：`BACKTEST-001` 第五阶段进入实现：补充模块权重扰动；默认对每个已配置评分模块分别做上调/下调扰动，并使用扰动后的 `ScoringRulesConfig` 重新运行同一 PIT 输入、成本假设、宏观预算和 position gate 规则，避免只复用已生成 gate 上限造成近似偏差。
- 2026-05-05：`BACKTEST-001` 第五阶段达到 BASELINE_DONE：`backtest_robustness` 默认新增每个评分模块上下扰动场景，并记录 `weight_perturbation_pct`；Markdown 解释权重扰动是否明显改变本次收益方向，JSON 剩余缺口移除 `module_weight_perturbation`。剩余缺口继续保留为随机同换手率和样本外验证。
- 2026-05-05：`BACKTEST-001` 第六阶段进入实现：补充同换手率随机策略；默认固定随机种子起点和样本数，随机决定每个交易日加/减仓方向，但每日 absolute turnover 复用基础策略，收益、融资和交易成本仍按随机路径逐日计算。
- 2026-05-05：`BACKTEST-001` 第六阶段达到 BASELINE_DONE：`backtest_robustness` 默认新增同换手率随机策略场景，并记录 `random_seed_start` 和 `random_seed_count`；Markdown 解释动态策略相对随机同换手率路径的胜出比例，JSON 剩余缺口移除 `same_turnover_random_strategy`。剩余缺口继续保留为样本外验证。
- 2026-05-05：`BACKTEST-001` 第七阶段进入实现：补充时间顺序样本外验证；默认按基础回测信号日期做 70%/30% 切分，分别运行 in-sample 窗口和 out-of-sample holdout，样本不足时显式 SKIPPED。
- 2026-05-05：`BACKTEST-001` 第七阶段完成并标记 DONE：`backtest_robustness` 默认新增 `in_sample_window` 和 `out_of_sample_holdout` 场景，并记录 `oos_split_ratio`；Markdown 解释样本外 holdout 是否立即暴露方向性失效，JSON `remaining_gaps` 为空。`ruff check src tests` 和全量 `.venv\Scripts\python.exe -m pytest -q` 通过 371 项测试。
