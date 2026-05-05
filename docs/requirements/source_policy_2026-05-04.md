# SOURCE-001 风险证据与数据来源策略

## 背景

本项目的风险事件、市场证据、估值快照和新闻输入会影响评分、报告解释、仓位闸门和后续复盘。来源可信度不能只作为报告修饰项，必须成为评分资格和人工复核边界。

2026-05-04，项目 owner 批准先采用保守 source policy：使用 `S/A/B/C/D/X` 六档证据等级，并明确 `S/A`、`B`、`C/D/X` 的自动评分边界。

## Source Type

| source_type | 含义 | 自动评分资格 |
|---|---|---|
| `primary_source` | SEC、公司 IR、监管机构、交易所、官方 API 或等价一手来源 | 可在证据等级和校验通过后评分 |
| `paid_vendor` | 有字段定义、时间戳和可审计导出的供应商数据 | 可在证据等级和校验通过后评分 |
| `manual_input` | 人工从可审计来源结构化录入 | 可在证据等级、复核状态和校验通过后评分 |
| `public_convenience` | 聚合站、公开便利 API、二次转载或口径不完整来源 | 不得单独评分，只能报告或辅助复核 |
| `llm_extracted` | LLM 从原始文本抽取或分类出的结构化线索 | 不得单独评分，只能待复核 |

## OpenAI API 预审边界

OpenAI API 可以用于风险事件和市场证据的预审，但预审输出在当前 source policy 下仍属于 `llm_extracted`：

- 允许：抽取结构化字段、匹配已有 `risk_id`、映射 ticker/产业链节点、判断是否需要人工复核、列出不确定点和复核问题。
- 禁止：直接确认风险事件、把候选事件升级为 L2/L3、修改 `action_class`、触发评分、触发仓位闸门、输出交易动作。
- 预审输出必须保持 `manual_review_status=pending_review`；人工确认前不得写成可评分证据。
- 如果 OpenAI 预审结果和 deterministic source policy 冲突，以 source policy 的保守规则为准。
- 付费新闻或供应商内容可以在 owner 个人投资决策支持目的下进入 OpenAI 预审，但必须先在数据源目录或请求 envelope 中显式记录 provider、授权范围、`external_llm_allowed`、`cache_allowed`、`redistribution_allowed`、`content_sent_level` 和 `approval_ref`。
- 如果供应商授权未知或 `external_llm_allowed=false`，系统不得发送供应商全文或长摘录给外部 LLM API；最多只能在授权允许时发送元数据或人工摘要，并保留 checksum 与人工复核记录。

风险事件的完整预审与人工复核流程见 `docs/requirements/risk_event_review_workflow_2026-05-04.md`。

## Evidence Grade

| 等级 | 定义 | 自动评分 | 仓位闸门 | 报告/复核 |
|---|---|---:|---:|---:|
| `S` | 一手权威且可复现，具备 URL、发布时间、采集时间、checksum 或 accession | 可以 | 可以 | 可以 |
| `A` | 可信 paid vendor 或人工从一手来源结构化录入，字段口径清楚 | 可以 | 可以 | 可以 |
| `B` | 人工确认的可信二手来源、供应商摘要或 point-in-time/字段口径受限输入 | 可以，限普通评分 | 不可以 | 可以 |
| `C` | 单一间接来源、未充分复核、口径不完整或时效性存疑 | 不可以 | 不可以 | 可以 |
| `D` | public convenience、聚合站、博客、社媒、转载且未追到原始来源 | 不可以 | 不可以 | 可以 |
| `X` | LLM 结论、传闻、不可验证、冲突未解决或被驳回证据 | 不可以 | 不可以 | 仅审计保留或 rejected |

## 评分资格规则

- 自动普通评分必须同时满足：`source_type` 不是 `public_convenience`/`llm_extracted`，证据等级为 `S/A/B`，记录日期不晚于 `signal_date/as_of`，schema 和 freshness 校验通过。
- `S/A` 可支持普通评分和仓位闸门，但仍必须满足对应模块的状态条件，例如风险事件必须是 `active` 且 `action_class=position_gate_eligible`。
- `B` 只能支持普通评分；即使风险事件设置了 `action_class=position_gate_eligible`，也不得单独触发仓位闸门。
- `C/D/X` 只能进入报告、belief_state、trace bundle 或人工复核队列，不得进入自动评分、仓位闸门或交易建议。
- `public_convenience` 无论人工填写何种 `evidence_grade`，默认不得单独评分。若追到原始来源，应新增一条 `primary_source` 或 `paid_vendor` evidence。
- `llm_extracted` 永远不能直接评分。LLM 只能辅助抽取、分类、去重、映射 ticker/产业链节点，并写入 `pending_review`。

## 风险事件执行规则

- `watch` 状态只进入报告和人工复核，不进入普通评分或仓位闸门。
- `active` 且 `S/A/B` 可进入政策/地缘普通评分；`B` 仍为 score-only。
- 只有 `active`、`S/A`、来源合格且 `action_class=position_gate_eligible` 的风险事件可触发 position gate。
- CSV 导入缺失 `action_class` 时默认 `manual_review`，不能默认扩大为仓位闸门。

## 验收标准

- `market_evidence` 校验报告显示保守 source policy 下可作为普通评分输入的证据数量，并对 `C/D/X`、未确认 `B`、`public_convenience` 和 `llm_extracted` 输出警告。
- `risk_event_occurrence` 复核报告区分普通评分资格和仓位闸门资格。
- 风险事件 position gate 只读取 `position_gate_eligible` 的 `S/A` active 记录，不能被 `B/C/D/X` 或 `public_convenience` 触发。
- `docs/system_flow.md` 和 `config/data_sources.yaml` 说明 source policy 对下游评分、报告和人工复核的约束。
- 测试覆盖 `B` 级风险事件 score-only、`C/D/X` 不评分、`public_convenience`/`llm_extracted` 隔离和缺省 action class 的保守行为。

## 状态记录

- 2026-05-04：创建 source policy 文档；owner 已批准保守方案，开始实现评分资格、仓位闸门和报告校验同步。
- 2026-05-04：基础实现完成：新增 `source_policy` helper；`market_evidence` 报告输出自动普通评分资格数量并警告 `C/D/X`、未确认 `B`、`public_convenience` 和 `llm_extracted`；`risk_event_occurrence` 复核报告区分普通评分资格和仓位闸门资格；position gate 只读取 `S/A` active 且 `action_class=position_gate_eligible` 的风险事件；CSV 导入缺省 `action_class` 改为 `manual_review`；测试覆盖保守边界；`python -m pytest -q` 通过 239 项测试，`python -m ruff check .` 通过。
- 2026-05-04：补充 OpenAI API 预审边界：OpenAI 只能生成 `llm_extracted` / `pending_review` 结构化线索，不能替代人工复核，不能直接评分或触发仓位闸门；风险事件细化流程转入 `RISK-003/RISK-004` 需求文档。
- 2026-05-04：owner 确认付费新闻/供应商内容可在个人使用目的下进入 OpenAI 预审；source policy 仍要求 provider 级外部 LLM 授权标记，未知授权 fail closed，避免把个人使用目的误当成所有供应商条款的通用许可。
- 2026-05-04：`LLM-001` 第一阶段实现把 provider LLM 授权落到 `config/data_sources.yaml` 的 `llm_permission`，并由 `aits llm precheck-claims` 在调用 OpenAI 前执行 fail closed 检查；待复核队列只保存结构化 claim、权限 envelope、request id、model、reasoning effort、prompt version 和 checksum，不保存未授权全文。2026-05-04：owner 指定默认 OpenAI 请求策略为 `gpt-5.5` 和 `reasoning.effort=high`。2026-05-05：owner 批准单请求失败最多重试 2 次，仍失败时 fail closed。
