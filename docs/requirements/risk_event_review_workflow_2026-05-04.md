# RISK-003/RISK-004 风险事件来源与复核流程

状态：BASELINE_DONE

最后更新：2026-05-05

关联任务：`RISK-003`、`RISK-004`、`RISK-005`、`RISK-006`、`SOURCE-001`、`LLM-001`、`EVIDENCE-001`

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

API 使用边界：

- OpenAI API 只用于关键信息校验、抽取、分类、去重、ticker/产业链节点映射和人工复核问题生成。
- 请求应显式关闭或避免不必要的服务端存储；付费供应商内容不得写入 Assistants/Threads/Vector Stores/Files 或外部工具链。
- 报告不得复制付费供应商长文或完整原文，只能输出来源引用、结构化摘要和人工确认结论。
- 任何模型输出都必须保留为 `llm_extracted` / `pending_review`，不能替代人工确认或触发生产评分、仓位闸门和交易动作。

预审输入必须来自可审计文本或元数据：

- 原始 URL、标题、发布时间、发布主体；
- 文本摘要或允许处理的原文片段；
- 采集时间、输入 checksum；
- 当前 `config/risk_events.yaml` 中可匹配的 `risk_id` 列表；
- 当前观察池 ticker 和产业链节点列表。
- source permission envelope，包括 provider、授权范围、`personal_use_only`、`external_llm_allowed`、`cache_allowed`、`redistribution_allowed`、`content_sent_level` 和 `approval_ref`。

付费新闻或供应商内容可以在 owner 个人投资决策支持目的下进入 OpenAI 预审，但必须满足 provider 级授权标记。授权未知或 `external_llm_allowed=false` 时，不得发送供应商全文或长摘录；只能进入人工复核，或在授权允许时发送元数据/人工摘要。

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

- model、reasoning effort、prompt version、request timestamp；
- 输入来源、输入 checksum、输出 checksum；
- OpenAI response id 或等价请求追踪 ID；
- 是否包含付费供应商内容，以及 provider、授权范围、`external_llm_allowed`、`content_sent_level` 和 `approval_ref`。

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
2. 增加预审运行记录，保存 model、reasoning effort、prompt version、输入 checksum、输出 checksum、source URL 和 OpenAI response id。
3. 将预审结果写入 `market_evidence` 待复核记录或单独待复核队列；`source_type` 必须是 `llm_extracted`，`manual_review_status` 必须是 `pending_review`。
4. 增加人工复核字段和校验，确认后的风险事件发生记录必须包含 reviewer、reviewed_at、review_decision、rationale、next_review_due。
5. 更新报告，区分 OpenAI 预审、人工确认和可评分输入。
6. 补充测试，证明 OpenAI 预审输出不能直接评分、不能触发仓位闸门、不能把 L2/L3 自动写成 active。
7. 实现时同步更新 `docs/system_flow.md` 和相关 CLI 文档。

## RISK-004 基础实现进展

- 新增 `aits risk-events import-prereview-csv`，导入固定结构化预审结果 CSV。
- 新增 `data/processed/risk_event_prereview_queue.json` 队列格式，记录 schema version、source CSV checksum、model、reasoning effort、prompt version、request id、request timestamp、输入/输出 checksum、source URL、候选 risk_id、ticker/节点映射和人工复核问题。
- 预审记录强制为 `source_type=llm_extracted` 和 `manual_review_status=pending_review`；`prohibited_actions_ack` 必须为 true。
- L2/L3 或 `active_candidate` 预审结果只生成警告和人工复核项，不写入正式 `risk_event_occurrence`。
- 正式风险事件发生记录增加 `reviewer`、`reviewed_at`、`review_decision`、`rationale`、`next_review_due` 校验；缺少元数据的 active/watch 记录失败。
- 未完成缺口：风险事件专用 `import-prereview-csv` 仍只导入结构化结果；通用 live OpenAI Responses API 调用已由 `LLM-001` 的 claim 预审入口提供，但风险事件专用生产样本验证、真实请求追踪复盘和真实授权来源样本仍需 owner 批准样本。

## RISK-004 第二阶段推进范围

状态：BASELINE_DONE

本阶段把风险事件整理从“先在外部生成 CSV 再导入”推进到“系统内调用 OpenAI Responses API 并直接写入风险事件待复核队列”。边界不变：OpenAI 只做候选整理，不能替代人工确认。

### 验收标准

- 新增 `aits risk-events precheck-openai` 或等价 CLI，读取 source-permission envelope 或数据源目录中的 `llm_permission`。
- OpenAI live 预审默认使用 `gpt-5.5` 和 `reasoning.effort=high`；单请求失败最多重试 2 次，第 3 次仍失败时整批 fail closed；CLI 可显式覆盖用于对比实验，但队列和报告必须记录实际 model 与 reasoning effort。
- provider 授权未知或 `external_llm_allowed=false` 时 fail closed，不发起 API 请求，不写入队列。
- API 请求使用固定结构化输出和 `store=false`，记录 model、reasoning effort、prompt version、OpenAI request/response id、输入/输出 checksum、source URL、source permission 和 request timestamp。
- 只把 `risk_event` claim 或含风险事件候选的输出转换成 `risk_event_prereview_queue.json` 记录。
- 转换后的记录强制为 `source_type=llm_extracted`、`manual_review_status=pending_review`，不得自动评分、触发仓位闸门或写入正式 occurrence。
- 中文报告区分 live API 预审、待人工复核候选和不可执行边界。
- 更新 `docs/system_flow.md`、示例输入和测试，覆盖权限 fail closed、成功写入待复核队列、高影响候选隔离和 CLI 行为。

### 第二阶段实现进展

- 新增 `aits risk-events precheck-openai`，读取与 `aits llm precheck-claims` 相同的 JSON/YAML source-permission 输入。
- live API 调用复用 `LLM-001` 的 provider LLM 权限检查、Responses API Structured Outputs、`store=false`、request id/response id、输入/输出 checksum、model 和 reasoning effort 审计。
- 新增从 LLM claim report 到 `risk_event_prereview_queue.json` 的转换，只保留 `risk_event` claim 或带风险事件候选的输出。
- 队列记录新增 `source_kind=openai_live`、`source_input_path`、`source_input_checksum_sha256`、`response_id`、`client_request_id` 和 `source_permission`；仍保留 CSV 导入兼容字段。
- 中文报告会区分 CSV 导入和 live API 预审；live 报告说明 Responses API 调用边界和 `llm_extracted / pending_review` 隔离。
- 新增 `docs/examples/risk_event_prereview/openai_live_precheck_template.yaml`。
- 测试覆盖 live API 成功写入待复核队列、不保存 source text/API key、provider 权限 fail closed、CLI 失败不写队列，以及高影响候选仍只进入人工复核。
- 未完成缺口：尚未用真实 owner 批准来源样本运行生产预审；provider 授权目录和每日人工复核纪律仍是 `RISK-003/RISK-005` 的生产前置条件。

## RISK-005

状态：BASELINE_DONE

### 问题

2026-05-04 日报中，风险事件发生记录目录不存在，政策/地缘模块只能输出数据不足。现有行为是正确的：空目录或空 YAML 不能证明当前没有政策/地缘风险，也不能让系统把监控规则配置误读成“无事件”。

生产前还需要一条可审计的“复核已完成”输入链路。该链路记录的是：复核人在指定覆盖窗口内检查了约定来源范围，未发现需要写入正式 occurrence 的未记录重大风险事件。它不是自动风险消除证明，也不能替代后续 active/watch occurrence。

### 设计边界

- 复核声明只说明“按声明列出的来源和窗口完成了检查”；不代表世界上不存在风险。
- 声明必须包含 `reviewer`、`reviewed_at`、`review_decision`、`coverage_start`、`coverage_end`、`next_review_due`、`checked_sources` 和 `rationale`。
- 只有 `review_decision=confirmed_no_unrecorded_material_events`、覆盖 `as_of`、未过期、没有未来日期且至少包含 `primary_source`、`paid_vendor` 或 `manual_input` 来源范围时，才算当前有效。
- 如果存在有效声明且没有可评分 active occurrence，政策/地缘模块可以把 L2/L3 active 计数视为 0，并以 `manual_input` 置信度进入评分；否则仍保持 `insufficient_data`。
- 有效声明不触发仓位闸门，不降低已记录 active 风险事件的影响，不改变 `watch` 默认只进报告和人工复核的规则。
- 系统不得自动生成真实声明；CLI 只能在用户显式提供复核人、来源范围和理由时写入声明文件。

### 第一阶段验收标准

- 支持风险事件复核声明 YAML schema 与加载。
- `aits risk-events record-review-attestation` 可写入声明文件。
- `aits risk-events validate-occurrences` 报告显示复核声明数量、当前有效声明数量、覆盖窗口、复核人、来源范围和状态。
- 空 occurrence 目录在存在当前有效声明时不再触发“空目录不能证明无风险”的警告；过期、未来日期、来源范围不足或 `needs_more_evidence` 声明仍不能解除降级。
- `score-daily` 在有效声明覆盖评估日时，将政策/地缘模块标为 `manual_input` 而不是 `insufficient_data`，但不触发风险事件仓位闸门。
- 更新 `docs/system_flow.md`，补充测试覆盖有效声明、过期声明、日报评分和 gate 隔离。

### 第一阶段实现进展

- 新增 `review_attestation` YAML schema，和正式 occurrence 共用 `data/external/risk_event_occurrences/` 加载入口。
- 新增 `aits risk-events record-review-attestation`，只有显式提供复核人、来源范围和理由时才写入声明文件。
- `aits risk-events validate-occurrences` 报告输出复核声明数量、当前有效声明数量、覆盖窗口、复核人、来源范围、过期状态和方法边界。
- `score-daily` 在无可评分 active occurrence 且存在当前有效复核声明时，将政策/地缘模块作为 `manual_input` 处理；没有有效声明时仍为 `insufficient_data`。
- point-in-time 回测切片会按 `review_date`、`reviewed_at` 和 checked source `captured_at` 过滤复核声明，避免未来复核声明进入历史信号日。
- 声明不触发风险事件仓位闸门，不覆盖已记录的 active/watch occurrence，也不由系统自动生成真实复核内容。

## RISK-006

状态：BASELINE_DONE

### 问题

当前 `policy_geopolitics` 的低置信度不是模型参数问题，而是输入证据问题：系统缺少可持续、授权清晰、可审计的政策/地缘事件来源，以及覆盖评估日的真实人工复核声明。没有这类输入时，系统只能把空发生记录解释为 `insufficient_data` 或低置信结论，不能把“没有记录到事件”当成“没有政策或地缘风险”。

### 已确认范围

- owner 已确认低成本官方来源组合：Federal Register/BIS/OFAC/USTR/Congress.gov/GovInfo/Trade.gov CSL。
- 第一版先不接付费供应商，不使用未授权新闻源或模型推断来修复置信度。
- owner 负责提供 `CONGRESS_API_KEY`、`GOVINFO_API_KEY` 和每日复核声明。
- 系统负责抓取、原始缓存、row count/checksum 审计、待人工复核候选和中文报告。

### 剩余生产前置条件

- Congress.gov 和 GovInfo API key 需要由 owner 配置在本机环境变量中。
- 每日复核声明仍需 owner 按真实复核窗口、来源范围和理由填写，系统不会自动生成“无事件”结论。
- 需要至少一批真实样本进入 `risk_event_occurrence` 或 `review_attestation` 流程，并通过 `aits risk-events validate-occurrences`。
- `score-daily` 继续只承认可评分 occurrence 或当前有效复核声明；官方来源候选本身仍是 `pending_review`。

### 第一版官方来源组合

|来源|第一版用途|凭证|
|---|---|---|
|Federal Register API|AI/半导体/出口管制政策文件发现；需回到 govinfo 或 PDF 复核法律版本|不需要 API key|
|BIS via Federal Register|Entity List、Unverified List、EAR、advanced computing 相关公告发现|不需要 API key|
|OFAC SDN XML|SDN 制裁清单快照监控|不需要 API key|
|OFAC Consolidated XML|Non-SDN / CMIC / sectoral sanctions 线索监控|不需要 API key|
|USTR press releases|Section 301、tariff 和贸易政策线索|不需要 API key|
|Trade.gov CSL JSON|多部门 restricted party screening cross-check；命中需回官方清单尽调|不需要 API key|
|Congress.gov bills|AI chip、出口管制、制裁和地缘相关法案 watch|需要 `CONGRESS_API_KEY`|
|GovInfo Federal Register collection|Federal Register 官方包级 metadata 校验|需要 `GOVINFO_API_KEY`|

### 完整 DONE 条件

1. owner 提供 API key，并完成至少一轮真实每日复核声明。
2. 抓取报告显示 provider、captured_at、row count/checksum、跳过来源和候选摘要。
3. 至少一批真实样本通过 source policy、occurrence/attestation 校验。
4. `score-daily` 能明确区分“已复核无重大事件”和“没有可靠来源输入”；前者可作为经复核低风险输入，后者必须继续降级。

### 边界

- 不使用 `public_convenience`、未授权供应商文本或 `llm_extracted` 输出直接修复置信度。
- 不用 OpenAI 预审替代人工确认；预审只能产生待复核候选和复核问题。
- 不因短期缺少事件记录而提高 `policy_geopolitics` 置信度。

### 进展记录

- 2026-05-05：新增本任务，原因：owner 指出当前 `policy_geopolitics` 置信度很低，后续需要可靠数据源才能正确分析。当前正确处理是保守降级并显式登记 owner 数据源阻塞，而不是引入临时新闻源或模型推断绕过。
- 2026-05-05：进入实现，原因：owner 确认采用低成本官方来源组合，第一版监控 Federal Register/BIS/OFAC/USTR/Congress.gov/GovInfo/Trade.gov CSL；owner 负责提供 API key 和每日复核声明，系统实现负责官方来源抓取、原始缓存、审计报告和待人工复核入口。
- 2026-05-05：达到 `BASELINE_DONE`，原因：新增 `aits risk-events fetch-official-sources`、官方来源 raw payload 缓存、`download_manifest.csv` 审计、待人工复核候选 CSV、中文抓取报告、数据源目录、系统流图、`.env.example` API key 提示、Federal Register live smoke test 和全量测试；完整生产使用仍依赖 owner API key、每日复核声明和真实样本验证。

## 验收标准

- 风险事件正式来源和人工复核 owner、频率、SLA、升级标准已文档化。
- OpenAI API 预审只产生结构化 `pending_review` 结果，并保留输入/输出审计信息。
- 付费新闻或供应商内容只有在 provider 级授权允许外部 LLM 处理时才可进入 OpenAI API；授权未知时 fail closed。
- 人工确认后的记录才能进入 `risk_event_occurrence` 发生记录。
- `S/A active position_gate_eligible` 仍是触发仓位闸门的最低条件；`B/C/D/X`、`public_convenience` 和 `llm_extracted` 不得触发 gate。
- 日报和校验报告能区分：预审候选、待人工复核、已确认可评分、已确认可触发 gate。
- 回测 point-in-time 切片只使用当时已人工确认且当时可见的发生记录。

## 状态记录

- 2026-05-04：owner 确认可引入 OpenAI API 做简单预审，但预审不替代实际人工复核；正式流程采用官方/一手来源、已授权供应商线索、OpenAI 结构化预审和人工确认的组合。新增 `RISK-004` 承接实施。
- 2026-05-04：`RISK-004` 进入实现；第一阶段先落结构化预审结果导入、`llm_extracted` / `pending_review` 复核队列和隔离测试，不接入生产评分、仓位闸门或回测输入。
- 2026-05-04：`RISK-004` 达到 `BASELINE_DONE`：固定结构化输出导入、预审队列、中文报告、人工复核元数据校验、系统流图和测试完成；`python -m ruff check src tests`、`python -m pytest -q` 通过。剩余风险事件专用真实样本验证依赖来源授权与 owner 批准样本；通用 live OpenAI 调用由 `LLM-001` 第一阶段入口承接。
- 2026-05-04：新增 `RISK-005`，原因：日报生产就绪复盘发现“空 occurrence 目录不能证明无风险”会持续压低政策/地缘模块置信度；第一阶段实现可审计复核声明链路，不由系统代填真实复核结论。
- 2026-05-04：`RISK-005` 达到 `BASELINE_DONE`：复核声明 schema、CLI、校验报告、日报识别、历史切片、数据源目录、系统流图和测试已完成；真实每日复核的 owner、来源清单和运行纪律仍是生产使用前置条件。
- 2026-05-04：`RISK-003` 达到 `BASELINE_DONE`：来源分层、预审隔离、人工确认元数据、复核声明、日报识别和回测 point-in-time 切片已由 `SOURCE-001/RISK-004/RISK-005` 形成基础闭环；完整 `DONE` 仍依赖真实授权来源样本、provider 级外部 LLM 授权记录、风险事件专用生产样本验证和 owner 每日复核运行纪律。
- 2026-05-04：owner 确认风险事件预审可使用 OpenAI API 做关键信息校验，并允许在个人使用目的下处理付费新闻/供应商内容；流程同步增加 source permission envelope，要求 provider 级外部 LLM 授权、缓存和报告摘要边界，授权未知时不得发送全文或长摘录。
- 2026-05-04：`RISK-004` 第二阶段达到 `BASELINE_DONE`：新增 `aits risk-events precheck-openai`、live API 到风险事件待复核队列转换、source permission 审计、中文报告、示例输入、系统流图和隔离测试；随后按 owner 模型策略补充默认 `gpt-5.5`、`reasoning.effort=high` 和 reasoning effort 队列/报告审计字段。2026-05-05：owner 批准单请求失败最多重试 2 次，仍失败则整批 fail closed。真实授权来源生产样本验证仍需 owner 批准样本。
