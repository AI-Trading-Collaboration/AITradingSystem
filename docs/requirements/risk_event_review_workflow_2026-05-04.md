# RISK-003/RISK-004 风险事件来源与复核流程

状态：BASELINE_DONE

最后更新：2026-05-04

关联任务：`RISK-003`、`RISK-004`、`SOURCE-001`、`LLM-001`、`EVIDENCE-001`

## 结论

风险事件的生产流程采用“官方/一手来源 + OpenAI API 预审 + 结构化人工复核”的保守方案。

- 官方/一手来源和已授权的付费供应商是风险事件证据的正式来源。
- OpenAI API 只做预审：抽取、分类、去重、映射 ticker/产业链节点、列出人工复核问题。
- OpenAI API 输出不得直接确认事件、升级 L2/L3、修改 `action_class`、触发评分或仓位闸门。
- 任何无法确定、来源冲突、置信度不足、涉及 L2/L3 或仓位闸门的事件，都必须进入实际人工复核。
- 人工确认后的记录才能作为 `manual_input` 或合格 `primary_source` / `paid_vendor` 证据进入风险事件发生记录。

## 来源分层

|来源层级|source_type|用途|评分资格|
|---|---|---|---|
|SEC、公司 IR、监管公告、Federal Register/BIS/Commerce、OFAC、White House、交易所公告|`primary_source`|正式证据来源|满足 `S/A/B`、状态和校验规则后可进入评分；`S/A` 可支持仓位闸门|
|LSEG/Reuters、FactSet StreetAccount、Dow Jones、RavenPack 等付费供应商|`paid_vendor`|发现线索或正式供应商证据，取决于授权和字段审计能力|满足授权、字段定义、时间戳、导出审计和人工确认后可进入评分；默认不直接触发仓位闸门|
|公开聚合站、二次转载、公开便利 API|`public_convenience`|辅助发现和交叉检查|不得单独评分|
|OpenAI API 预审输出|`llm_extracted`|结构化待复核线索|不得评分，必须 `pending_review`|
|人工从可审计来源结构化录入|`manual_input`|正式复核后的结构化发生记录|满足证据等级、复核状态和校验后可进入评分|

## OpenAI API 预审

第一版应使用 OpenAI Responses API 和 Structured Outputs，要求模型只输出固定 JSON schema。非紧急批量分类可使用 Batch API；紧急事件仍使用同步调用并立即进入人工复核。

预审输入必须来自可审计文本或元数据：

- 原始 URL、标题、发布时间、发布主体；
- 文本摘要或允许处理的原文片段；
- 采集时间、输入 checksum；
- 当前 `config/risk_events.yaml` 中可匹配的 `risk_id` 列表；
- 当前观察池 ticker 和产业链节点列表。

预审输出建议包含：

|字段|含义|
|---|---|
|`precheck_id`|稳定预审记录 ID|
|`source_url` / `source_name`|被检查的原始来源|
|`matched_risk_ids`|可能匹配的已有风险事件规则|
|`status_suggestion`|`irrelevant`、`candidate`、`watch`、`active_candidate`、`resolved_candidate`|
|`level_suggestion`|`none`、`L1`、`L2`、`L3`|
|`affected_tickers` / `affected_nodes`|可能影响的标的和产业链节点|
|`evidence_grade_suggestion`|仅供人工参考，不得自动写成最终 `evidence_grade`|
|`confidence`|模型对分类的置信度|
|`uncertainty_reasons`|不确定或冲突原因|
|`human_review_questions`|人工复核必须回答的问题|
|`prohibited_actions_ack`|确认本输出不得直接评分、不得触发仓位闸门|

预审运行记录必须保存：

- model、prompt version、request timestamp；
- 输入来源、输入 checksum、输出 checksum；
- OpenAI response id 或等价请求追踪 ID；
- 是否包含付费供应商内容，以及是否有明确授权允许发送给外部 LLM API。

## 人工复核流程

### 角色

|角色|职责|
|---|---|
|投资 owner / 项目 owner|判断投资含义、风险等级、状态、影响范围和是否需要升级|
|系统/数据 owner|确认来源类型、URL、发布时间、采集时间、checksum、schema 和导入报告完整性|
|二级复核人|当事件可能为 L3 或 `position_gate_eligible` 时，进行第二确认|

如果实际复核人不是项目 owner，必须在后续配置或任务登记中记录具体 owner。

### 频率和 SLA

|复核类型|频率|
|---|---|
|每日例行复核|每个美股交易日 `score-daily` 前检查过去 24 小时官方来源、供应商线索和 OpenAI 预审队列|
|事件驱动复核|出口管制、制裁、地缘冲突、公司重大披露、云 CapEx/guidance 异常、核心 thesis 风险时立即复核|
|L2 候选|原则上 1 个交易日内确认或降级为 `watch`|
|L3 或仓位闸门候选|当日评分前必须双确认；不能确认时保持 `watch` / `review_required`，不得触发 gate|
|每周复核|清理 stale `watch`，确认升级、降级、resolved 或 dismissed|
|每月复核|复盘漏报、误报、L2/L3 触发质量和 source policy 是否需要调整|

### Checklist

人工复核每条候选事件时至少确认：

1. 原始来源是否为一手、已授权付费供应商或人工可审计来源。
2. `source_url`、`published_at`、`captured_at`、checksum 是否完整。
3. 是否匹配现有 `risk_id`；不匹配时是否需要新增候选规则，而不是硬塞进旧规则。
4. 状态应为 `watch`、`active`、`resolved`、`dismissed` 还是继续 `pending_review`。
5. 等级应为 L1、L2、L3，还是不足以形成风险事件。
6. `severity`、`probability`、`scope`、`time_sensitivity`、`reversibility` 是否合理。
7. `action_class` 是否只是 `manual_review` / `score_only`，还是可进入 `position_gate_eligible`。
8. 解除条件、下次复核日期和影响的 ticker / 产业链节点是否明确。
9. L2/L3 或仓位闸门候选是否完成双确认。
10. 导入后是否通过 `aits risk-events validate-occurrences`。

## 升级标准

|状态/等级|标准|下游影响|
|---|---|---|
|`candidate`|OpenAI、供应商或公开来源提示可能相关，但没有足够证据|进入待复核队列，不进入日报评分|
|`watch`|可信线索存在，但影响范围、证据等级或一手来源仍不充分|进入报告和人工复核，不进入评分或仓位闸门|
|L1 `active`|一手或人工确认来源支持，影响有限、局部或可逆|可进入普通风险评分|
|L2 `active`|官方/公司/合格供应商证据确认，影响核心 AI ticker、产业链节点、thesis 或云 CapEx 预期|人工确认后可进入普通评分；通常禁止主动加仓或限制仓位上限|
|L3 `active`|高可信一手证据确认，影响广、急、难逆，可能证伪核心 thesis 或冲击 AI 供应链|双确认后可触发快速降风险和 position gate|
|`resolved`|解除条件达成或官方撤销/澄清|从活跃风险中移除，保留审计记录|
|`dismissed`|证据错误、重复、无关或无法验证|不得评分，保留拒绝原因|

严重但只有单一非官方来源的事件，最多进入 `watch` / `review_required`；不能直接作为 L3 或仓位闸门输入。

## RISK-004 实施拆解

第一阶段只实现预审和复核队列，不改变现有评分、仓位闸门或回测行为。

1. 设计 OpenAI 预审 JSON schema 和 prompt version，输出只允许映射、分类和人工复核问题。
2. 增加预审运行记录，保存 model、prompt version、输入 checksum、输出 checksum、source URL 和 OpenAI response id。
3. 将预审结果写入 `market_evidence` 待复核记录或单独待复核队列；`source_type` 必须是 `llm_extracted`，`manual_review_status` 必须是 `pending_review`。
4. 增加人工复核字段和校验，确认后的风险事件发生记录必须包含 reviewer、reviewed_at、review_decision、rationale、next_review_due。
5. 更新报告，区分 OpenAI 预审、人工确认和可评分输入。
6. 补充测试，证明 OpenAI 预审输出不能直接评分、不能触发仓位闸门、不能把 L2/L3 自动写成 active。
7. 实现时同步更新 `docs/system_flow.md` 和相关 CLI 文档。

## RISK-004 基础实现进展

- 新增 `aits risk-events import-prereview-csv`，导入固定结构化预审结果 CSV。
- 新增 `data/processed/risk_event_prereview_queue.json` 队列格式，记录 schema version、source CSV checksum、model、prompt version、request id、request timestamp、输入/输出 checksum、source URL、候选 risk_id、ticker/节点映射和人工复核问题。
- 预审记录强制为 `source_type=llm_extracted` 和 `manual_review_status=pending_review`；`prohibited_actions_ack` 必须为 true。
- L2/L3 或 `active_candidate` 预审结果只生成警告和人工复核项，不写入正式 `risk_event_occurrence`。
- 正式风险事件发生记录增加 `reviewer`、`reviewed_at`、`review_decision`、`rationale`、`next_review_due` 校验；缺少元数据的 active/watch 记录失败。
- 未完成缺口：本阶段不在本地发起 OpenAI API 请求；live Responses API 调用适配器、真实请求追踪和真实授权来源样本验证仍需后续实现和 owner 提供 API key / 可发送外部 LLM 的来源样本。

## 验收标准

- 风险事件正式来源和人工复核 owner、频率、SLA、升级标准已文档化。
- OpenAI API 预审只产生结构化 `pending_review` 结果，并保留输入/输出审计信息。
- 付费新闻或供应商内容只有在授权允许外部 LLM 处理时才可进入 OpenAI API。
- 人工确认后的记录才能进入 `risk_event_occurrence` 发生记录。
- `S/A active position_gate_eligible` 仍是触发仓位闸门的最低条件；`B/C/D/X`、`public_convenience` 和 `llm_extracted` 不得触发 gate。
- 日报和校验报告能区分：预审候选、待人工复核、已确认可评分、已确认可触发 gate。
- 回测 point-in-time 切片只使用当时已人工确认且当时可见的发生记录。

## 状态记录

- 2026-05-04：owner 确认可引入 OpenAI API 做简单预审，但预审不替代实际人工复核；正式流程采用官方/一手来源、已授权供应商线索、OpenAI 结构化预审和人工确认的组合。新增 `RISK-004` 承接实施。
- 2026-05-04：`RISK-004` 进入实现；第一阶段先落结构化预审结果导入、`llm_extracted` / `pending_review` 复核队列和隔离测试，不接入生产评分、仓位闸门或回测输入。
- 2026-05-04：`RISK-004` 达到 `BASELINE_DONE`：固定结构化输出导入、预审队列、中文报告、人工复核元数据校验、系统流图和测试完成；`python -m ruff check src tests`、`python -m pytest -q` 通过。剩余 live OpenAI API 调用适配器和真实样本验证依赖 API key、来源授权与 owner 批准样本。
