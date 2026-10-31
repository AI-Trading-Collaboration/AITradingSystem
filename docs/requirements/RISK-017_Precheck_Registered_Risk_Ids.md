# RISK-017：风险事件预审只让模型选已登记的风险 ID

状态：IN_PROGRESS（2026-10-31）。`production_effect=none`；`broker_action=none`。

关联：RISK-012（未登记 ID 整批停写，2026-09-01）、LLM-005（日报预审 profile）、OPS-071（同日恢复）、OPS-085（17:30 取数重试）、DATA-030（上一次换包）；任务记录 `tasks/RISK-017.yaml`。

## 1. Owner decision

- `owner_decision:RISK-017:2026-10-31:registered_risk_ids`：Baker 2026-10-31 在「修复日报风险 ID 未登记导致停写」线程的卡片上选「给模型已登记清单并限定」：请求里附上已登记的风险事件，`risk_id_candidate` 只能从中选，都不符合就留空；RISK-012 的整批停写检查保留作兜底。
- `owner_decision:RISK-017:2026-10-31:as_of_2026_10_30_gap`：Baker 2026-10-31 在同一线程的卡片上选「记为缺口」：as_of 2026-10-30 的日报不补跑，记为已知缺口（同 2026-10-13、2026-10-27）。

## 2. 背景

2026-10-31 12:00 JST 的日报（as_of 2026-10-30，发布包 `20261029-be4febf55`，Run ID `daily_ops_run:2026-10-30:20261031T030006Z`）失败在 `score_daily`，调度摘要标 `CODE_DEFECT`。取数全部通过（DATA-030 的部分没问题），失败的是风险事件 LLM 正式评估：

- 官方候选 317 条，日报 profile `risk_event_daily_official_precheck`（gpt-5.5，reasoning medium）预审前 10 条。
- `precheck:official:official_congress_bills:official_congress_bills_46_Expressing_the_sense_of_the_House_of_Representatives_:claim_001` 的 `matched_risk_ids` 是 `ai_market_stress_bailout_restriction_policy`、`ai_financial_stability_policy_signal`，两个都不在 `config/risk_events.yaml`（只登记了 `cloud_capex_cut`、`ai_chip_export_control_upgrade`、`taiwan_geopolitical_escalation`、`ai_demand_inventory_build`）。
- `risk_event_llm_formal.py` 按 RISK-012 报 `llm_formal_assessment_unknown_risk_id`，整批不写 occurrence 和 attestation；下游 11 步 BLOCKED，Reader Brief 没生成。

代码核对（main `4db8d2cb`）：

- `llm_precheck.py` 的 `_build_openai_request_payload` 和 `_source_payload` 从来没把已登记的风险事件告诉模型；响应 schema 里 `risk_id_candidate` 是不受限的字符串数组。候选元数据里的 `matched_risk_ids` 是官方源按主题映射的（都是已登记的），但模型可以不用它、自己造名字。
- 2026-08-31 的日报（`data_center_farmland_federal_benefit_restriction_candidate`）是同一个原因。RISK-012 当时修的是“写出 validator 必然拒绝的 occurrence 之后才失败”：改成写件前整批停止，并且不许自动补 risk rule。模型为什么会造 ID 没有修，所以这次又停了。RISK-012 卡的下一步写着“以后再出现未登记 ID，按日报失败处理并另开任务”，这就是那张任务。

恢复：`config/operations/runtime_control.yaml` 的 `terminal_recovery_allowed_from_step_ids` 不含 `score_daily`；OPS-085 的 17:30 重试只在取数临时失败时从 `capture_daily_inputs` 重跑。as_of 2026-10-30 没有合法的同日补跑，按第 1 节记为缺口。

## 3. 做法

带风险事件配置调预审时（日报 `score-daily` 和 `aits risk-events precheck-openai` 都带），请求把已登记的风险事件告诉模型，schema 只允许这些 ID：

### A. `llm_precheck.py`

- 新常量 `LLM_CLAIM_PREREVIEW_REGISTERED_RISK_PROMPT_VERSION = "llm_claim_precheck_v2"`；`LLM_CLAIM_PREREVIEW_PROMPT_VERSION` 仍是 `llm_claim_precheck_v1`。
- 新常量 `REGISTERED_RISK_ID_RULE`：`risk_event_candidate.risk_id_candidate 只能填写 registered_risk_events 里的 event_id；没有符合的就输出空数组，不要自造 ID。`
- 新函数 `openai_llm_claim_response_format(registered_risk_ids=None)`：不给 ID 时返回原来的 `OPENAI_LLM_CLAIM_RESPONSE_FORMAT` 本身；给了 ID 时返回它的深拷贝，`name` 改成 v2，`risk_id_candidate.items` 改成 `{"type": "string", "enum": [去重排序后的 ID]}`，别的不变；空列表报 `ValueError`。原常量不被改动。
- `run_openai_claim_precheck` 加关键字参数 `risk_events: RiskEventsConfig | None = None`：
  - 配置里一条规则都没有时，不发请求，报 ERROR `llm_precheck_registered_risk_ids_empty`。
  - 有配置时，system 文本是原文接 `REGISTERED_RISK_ID_RULE`；user 内容是原来的来源 JSON 加一个键 `registered_risk_events`：每条规则的 `event_id`、`name`、`level`、`description`，按 `event_id` 排序；`text.format` 是 `openai_llm_claim_response_format(那些 ID)`；记录的 `prompt_version` 和输入 checksum 里的 `prompt_version` 是 v2。
  - 没有配置时请求、记录和以前一字不差（v1）。
- 请求内容变了，所以输入 checksum 和缓存键都变了，新请求不会读到旧请求的缓存。日报请求缓存 24 小时、每天都是新请求，费用不变。

### B. `risk_event_prereview.py`

- `run_openai_risk_event_prereview` 把已有的 `risk_events` 传给 `run_openai_claim_precheck`。日报的 `run_openai_risk_event_prereview_for_official_candidates` 经过它，`score_daily` 已经传了 `risk_events_config`，不用改。

结果：模型的回答只能是已登记的 ID 或空列表。空列表的记录在正式评估里是已有的 WARNING `llm_formal_assessment_record_without_risk_id`，不写 occurrence，其余记录照写，日报继续；这条线索仍留在预审队列里（`pending_review`）。

## 4. 不变的

- RISK-012 的规则不变：正式评估只接受 `config/risk_events.yaml` 登记的 ID，只含未登记 ID 的记录仍是 ERROR、整批零写件（例如换包前做的旧队列）；不自动新增 risk rule，不放宽 `unknown_risk_event_id` validator。
- `config/risk_events.yaml`、`config/llm_request_profiles.yaml`、`config/operations/runtime_control.yaml` 不改；同日恢复的允许清单不改。
- 不带风险配置的 `aits llm precheck` 请求不变；预审的权限、缓存、重试、visibility cutoff、候选上限不变。
- 已登记 ID 的选择规则不变：同时有已登记和未登记 ID 时选已登记的。

## 5. 验收

- 新测试 `tests/test_risk017_registered_risk_ids.py`（Claude 先写，11 条）全过：规则文字和两个版本号；不给 ID 时是原常量；给 ID 时只改 `name` 和 `risk_id_candidate.items`、去重排序、原常量不变；空列表报错；带配置的请求按 `event_id` 排序列出四个字段、system 文本接规则、schema 是枚举、记录 v2；不带配置的请求和以前一样、记录 v1；两种请求 checksum 和缓存键不同；空配置不发请求；日报官方候选预审带上 ID；一条候选回答空列表、另一条回答已登记 ID 时正式评估 PASS_WITH_WARNINGS 并写出那一条；队列里只有未登记 ID 时仍整批停。
- 已有的 `test_llm_precheck.py`、`test_risk_event_prereview.py`、`test_risk_event_llm_formal.py`、`test_risk_events.py`、`test_daily_scoring.py` 不改、全过；PR 套件通过。
- 生产：换包后第一个交易日日报 `score_daily` 通过；预审报告里的记录是 `llm_claim_precheck_v2`。

## 6. 没有解决的

- 模型仍可能把不相干的新闻硬塞进某个已登记的 ID（以前同时给已登记和未登记 ID 时也会选已登记的）。规则文字要求不符合就留空；L2/L3 记录在报告里照旧标“未经人工复核”。要不要按事件类型补登记新的风险规则（例如金融稳定、救助限制），改的是 `config/risk_events.yaml`（C 区），要 Baker 决定，另开任务。
- 已经停掉的 as_of 2026-10-30 不补，记为缺口。

## 7. 进展

- 2026-10-31：需求和验收测试写好，交 pi 实现（RISK-017-01）。
