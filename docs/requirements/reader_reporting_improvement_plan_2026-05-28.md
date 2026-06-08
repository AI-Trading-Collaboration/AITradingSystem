# 读者视角报告体系与计算解释升级

最后更新：2026-06-09

关联任务：`REPORT-047`、`REPORT-048`、`REPORT-049`、`REPORT-050`、`REPORT-051`、`REPORT-052`

来源材料：`G:/Download/AITradingSystem_reader_reporting_improvement_plan.md`（文档日期：2026-05-27）

## 评估结论

外部改进方案与当前系统阶段匹配。项目已经具备 `daily_score`、`evidence_dashboard`、`daily_task_dashboard`、`daily_decision_summary`、SEC PIT observe-only、shadow / weight governance 和回测审计链路，主要缺口不再是单个计算模块，而是读者入口、计算解释、周期报告索引和 research / production 边界的统一叙事。

适合进入后续开发计划的方向：

|任务|结论|优先级|原因|
|---|---|---|---|
|`REPORT-047` Unified Reader Brief|采纳|P1|统一每日阅读入口，降低读者在 Markdown 日报、evidence dashboard 和 daily task dashboard 之间拼上下文的成本。|
|`REPORT-048` Calculation Explainer Registry|采纳|P1|把已有 `docs/calculation_logic.md` 和当日实际数值绑定，减少对 score、confidence、gate、shadow 结果的误读。|
|`REPORT-049` Report Registry & Cadence Calendar|采纳|P2|有助于管理 daily / weekly / monthly / ad hoc 报告 freshness，但可以在 Reader Brief 和 explainer 之后推进。|
|`REPORT-050` Score Change Attribution|采纳|P1|直接回答“为什么今天和上一交易日不同”，对投资解释价值高。|
|`REPORT-051` Research Governance Summary Cards|采纳|P1|把 backtest、SEC PIT、shadow 和 weight candidate 的状态统一成只读治理摘要，降低 research-only 被误解为 production 的风险。|
|`REPORT-052` Documentation Hygiene & Generated Catalog|采纳但后置|P2|长文档维护压力真实存在，但应在 report registry 结构稳定后再做生成式目录和拆分。|

不建议单独立项、而应合并进上述任务的内容：

- `conclusion_use_level`：当前已有 `conclusion_boundary` / 结论使用等级基础版。后续应在 Reader Brief 中映射现有 taxonomy，而不是引入一套不兼容枚举。
- `gate release_condition`：当前日报已有解除条件表达基础。后续应在 `REPORT-047` 和 `REPORT-048` 中强化 gate 级解释，而不是新建独立评分规则。
- `outputs/runs/daily/<as_of>/<run_id>/` 目录重排：当前 daily-run 已有 canonical run bundle 和 legacy mirror。Reader Brief 首版应只读现有 artifact，并先镜像到既有报告目录；是否调整顶层目录优先级放到后续实现阶段再评估。
- daily task dashboard 全量重排：已有 `OPS-015` 处于 `VALIDATING`。本轮不重开该任务，只要求 `REPORT-047/051` 通过统一 summary 缓解卡片过载。

## 设计边界

- 新增报告层默认只读读取已有 artifacts，不重新计算 score，不修改 weights、position gates、paper trading、shadow configs 或 production 参数。
- 所有新增 JSON / HTML / Markdown 报告必须固定披露 `production_effect=none`，并显式区分 production-active、approved but inactive、observe-only、candidate / research-only、blocked / insufficient data。
- 依赖 cached market / macro 数据的摘要必须读取并展示已有 `aits validate-data` 质量状态或质量报告引用；不得绕过数据质量门禁。
- 报告默认中文输出，ticker、字段名、artifact 名、状态码和固定 schema 枚举保留 English。
- 缺少可选 backtest、shadow、SEC PIT 或 governance artifact 时只能降级为 `MISSING` / `LIMITED` / `INSUFFICIENT_DATA`，不得补造结论。
- 实现阶段只要新增 CLI、配置、报告输出、artifact schema 或 dashboard 消费路径，就必须同步更新 `docs/system_flow.md` 和 `docs/artifact_catalog.md`。

## 阶段拆解

|阶段|任务|状态|依赖|验收重点|
|---|---|---|---|---|
|1|`REPORT-048` Calculation Explainer Registry|VALIDATING|`docs/calculation_logic.md`、现有 score / snapshot / trace 字段|关键 metric 有 formula、inputs、source artifacts、PIT policy、limitations；缺失解释可被测试捕获。|
|2|`REPORT-047` Unified Reader Brief|DONE|`daily_score`、`evidence_dashboard`、`daily_task_dashboard`、`daily_decision_summary`、阶段 1 explainer|生成 `reader_brief_YYYY-MM-DD.html/json`，作为每日首选读者入口，只读汇总结论、市场状态、gate、data/PIT、backtest/shadow/weight 和人工复核队列。|
|3|`REPORT-050` Score Change Attribution|DONE|连续交易日 score / snapshot / gate / confidence artifacts|生成今日 vs 上一交易日变化归因，拆分 component、weight、coverage、gate、confidence 和 data quality 变化。|
|4|`REPORT-051` Research Governance Summary Cards|DONE|SEC PIT、backtest、shadow、weight governance 已有 artifacts|生成统一 research governance summary，明确哪些结果影响 production、哪些仅 observe-only / research-only。|
|5|`REPORT-049` Report Registry & Cadence Calendar|DONE|Reader Brief 首版字段稳定|新增 report registry / index，按 cadence、freshness、owner action 和 production effect 管理报告。|
|6|`REPORT-052` Documentation Hygiene & Generated Catalog|DONE|report registry 稳定、artifact catalog 生成规则明确|已新增 documentation contract 生成/校验链路；后续再评估 `docs/artifacts/*` 生成索引和 completed task 月度归档。|

## 开放问题

- `reader_brief` 是否应优先消费 `daily_decision_summary`，再从 trace / dashboard JSON 补充细节，还是建立独立 collector。倾向前者，避免重复解析和重复解释。
- `metric_explainers.yaml` 与 `docs/calculation_logic.md` 的一致性如何测试。倾向用 schema / required metric coverage 测试，不强行比较自然语言。
- `report_registry.yaml` 是否应成为 artifact catalog 的上游。倾向先只服务 report index 和 Reader Brief freshness，再逐步评估生成 artifact catalog 摘要。

## 进展记录

- 2026-05-28：根据外部改进方案完成评估并登记后续任务。结论是采纳 `REPORT-047` 至 `REPORT-052`，但将首版范围限定为只读解释层；不改变 production scoring、weights、position gates、paper trading、shadow configs 或现有数据流。
- 2026-05-28：`REPORT-048` 进入 VALIDATING。新增 `config/metric_explainers.yaml`、`src/ai_trading_system/reports/calculation_explainers.py` 和 `aits reports calculation-explainers`；`score-daily` 成功写出 decision snapshot 后自动生成 `outputs/reports/calculation_explainers_YYYY-MM-DD.json` 或显式 `--calculation-explainers-path`；JSON 固定 `production_effect=none`，覆盖 overall score、component score、effective weight、confidence、model / confidence-adjusted / final position、macro risk budget、position gate、RankIC、max drawdown 和 baseline coverage，其中非当日 snapshot 可得指标标记为 `DEFINITION_ONLY`。同步更新 `docs/system_flow.md` 和 `docs/artifact_catalog.md`；验证通过 `tests/test_calculation_explainers.py`、`tests/test_daily_scoring.py`、`tests/test_evidence_dashboard.py`、`tests/test_daily_task_dashboard.py` 共 53 项、触达文件 ruff、black 和 diff check。
- 2026-05-28：`REPORT-047` 进入 VALIDATING。新增 `src/ai_trading_system/reports/reader_brief.py`、`aits reports reader-brief` 和 direct dispatcher；输出 `reader_brief_YYYY-MM-DD.html/json`，只读读取 `daily_decision_summary`、Markdown 日报、evidence dashboard JSON、daily task dashboard JSON、calculation explainers、decision snapshot 和 trace bundle，展示核心结论、Score-to-Position funnel、component contribution、binding gate ladder、Data/PIT safety、governance 摘要、manual review queue 和 artifact links。缺少可选 artifact 时降级为 `PASS_WITH_WARNINGS` / `MISSING` / `LIMITED`，固定 `production_effect=none` 且不生成交易指令。同步更新系统流图、产物目录和专项测试；下一步观察真实 daily-run artifacts，并评估是否把 Reader Brief 纳入 daily-run 自动步骤。
- 2026-06-09：`REPORT-047` 从 `VALIDATING` 改为 `DONE`。latest 真实 artifact 复核通过：`aits reports reader-brief --as-of 2026-06-05` 与 `--latest` 均生成 `reader_brief_2026-06-05.html/json`，状态 `OK`、warnings=0、`production_effect=none`、`reader_entry_role=daily_reading_home`；JSON 字段级复核确认 run context、executive decision、Data/PIT safety、Score-to-Position funnel、component explainability、binding gate ladder、research governance、manual review queue、appendix/source links 均存在，funnel steps=7、components=6、gates=8、manual review items=53、appendix links=12。Reader Brief quality、documentation contract、docs freshness 和 `tests/test_reader_brief.py` 均通过。
- 2026-05-28：`REPORT-050` 进入 IN_PROGRESS。实现方向限定为只读比较今日与上一条 signal-time decision snapshot，拆分 score、effective weight、coverage、confidence、position gate 和 data quality 变化；缺少上一快照时输出 `INSUFFICIENT_DATA`，不从 Markdown 变化原因树补造结构化归因。
- 2026-05-28：`REPORT-050` 进入 VALIDATING。新增 `src/ai_trading_system/reports/score_change_attribution.py`、`aits reports score-change-attribution`、Markdown/JSON artifact 和 direct dispatcher；自动从 decision snapshot 目录发现上一条 prior snapshot，也可显式传入 previous snapshot。JSON 固定 `production_effect=none`，输出 overall score delta、component score/effective weight/coverage/confidence/contribution decomposition、gate cap/binding delta、position delta、data quality delta 和 top changes；缺少有效 prior 时输出 `INSUFFICIENT_DATA`。Reader Brief 已接入该 JSON 的 top changes 区块。
- 2026-06-09：`REPORT-050` 从 `VALIDATING` 改为 `DONE`。最新
  `aits reports score-change-attribution --latest` 输出
  `score_change_attribution_2026-06-05.md/json`，状态 `PASS`、
  `production_effect=none`、warnings 0，真实对比 2026-06-04 -> 2026-06-05
  的 signal-time decision snapshots；JSON 字段级复核包含
  `market_regime=ai_after_chatgpt`、6 个 component attribution、8 个 gate
  attribution、top negative drivers `risk_sentiment` / `macro_liquidity`、
  data quality delta、source snapshot paths 和
  `methodology.does_not_recompute_score=true`。专项测试、Reader Brief
  生成/质量校验和 documentation contract 作为收口验证。
- 2026-05-28：`REPORT-051` 进入 IN_PROGRESS。实现方向限定为统一只读 collector，读取已存在的 backtest / SEC PIT / shadow / weight governance artifacts，按 production-active、observe-only、candidate / research-only、blocked / insufficient data、rollback / warning 分组；缺失 artifact 只进入 `MISSING` / `LIMITED`，不得运行上游任务或补造 research 结论。
- 2026-05-28：`REPORT-051` 进入 VALIDATING。新增 `src/ai_trading_system/reports/research_governance_summary.py`、`aits reports research-governance-summary`、Markdown/JSON artifact、direct dispatcher、Reader Brief 接入和 daily task dashboard 只读卡片；summary 只读取 latest backtest / SEC PIT / shadow / weight governance artifacts，输出分组 card、source task、status、candidate_id、production_effect、manual_review_required 和 next action。缺失或受限 artifact 只进入 warning / `MISSING` / `LIMITED`，固定 `production_effect=none`，不运行上游任务、不修改 production scoring、weights、position gates 或 trading 行为。
- 2026-06-09：`REPORT-051` latest 字段级验证发现
  `weight_adjustment_candidates` 与 `daily_weight_adjustment_summary` 两张 missing
  cards 的 card-level `production_effect` 为空字符串。汇总层仍固定
  `production_effect=none`，但每张治理 card 也必须显式披露只读边界；本轮先把
  `REPORT-051` 拉回 `IN_PROGRESS` 修复该 schema/safety 缺口，再重新验收。
- 2026-06-09：`REPORT-051` 从 `IN_PROGRESS` 改为 `DONE`。missing JSON
  artifact 现在也显式写入 card/source artifact `production_effect=none`；
  latest `research_governance_summary_2026-06-05.md/json` 为
  `PASS_WITH_LIMITATIONS` / `PASS_WITH_WARNINGS`，`promotion_status=BLOCKED_BY_MISSING_ARTIFACTS`，
  cards 19、manual review items 13、production effect risk 0、card/source
  empty production effect 0。该结果按设计阻断 promotion，只读暴露 backtest、
  SEC PIT、shadow observe、weight governance、documentation / registry 的缺失、
  warning 和 manual-review 状态；Reader Brief / daily task dashboard focused tests、
  latest Reader Brief quality 和 documentation contract 作为收口验证。
- 2026-05-28：`REPORT-049` 进入 IN_PROGRESS。实现方向限定为只读 report registry / cadence index：`config/report_registry.yaml` 作为 report index 的输入，不替代 `docs/artifact_catalog.md`；`aits reports index` 只扫描已有 artifact，不运行上游报告命令、不补造 freshness 或 owner action。
- 2026-05-28：`REPORT-049` 进入 VALIDATING。新增 `config/report_registry.yaml`、`src/ai_trading_system/reports/report_index.py`、`aits reports index`、HTML/JSON artifact、direct dispatcher 和 Reader Brief freshness 摘要接入；registry 记录 cadence、freshness SLA rationale、owner action、audience、production_effect 和下游可见性。报告 index 只读扫描 latest artifact，输出 missing/stale/required_missing 和 owner action，不运行上游命令、不替代 artifact catalog。
- 2026-06-09：`REPORT-049` latest 验证发现 `etf_shadow_candidates`
  registry entry 的 `freshness_sla_days` 为 null，虽然 index 可生成，但不满足
  “每个 registry report 记录 freshness SLA”的验收口径。本轮先修正该
  presence-only runtime state 的显式 SLA，并补默认 registry 契约测试，防止后续
  新增 entry 静默缺少 SLA。
- 2026-06-09：`REPORT-049` 从 `VALIDATING` 改为 `DONE`。`etf_shadow_candidates`
  已补显式 90 天审计 SLA，`load_report_registry()` 对缺失或非整数
  `freshness_sla_days` fail closed。最新 `aits reports index --latest` 输出
  `report_index_2026-06-05.html/json`，状态 `PASS_WITH_WARNINGS`，
  `production_effect=none`、registry reports 166、required missing 0、
  production effect risk 0、bad SLA 0；warnings 仅暴露可选/ad hoc missing 与
  stale artifacts。`aits docs report-contract --latest` 为 PASS，Reader Brief
  生成与 `validate-reader-brief --latest` 均为 OK。
- 2026-05-28：`REPORT-052` 进入 IN_PROGRESS。详细拆解迁移到 `docs/requirements/documentation_hygiene_generated_catalog_2026-05-28.md`；第一阶段限定为只读 documentation contract，读取 report registry 与 artifact catalog，生成 `documentation_contract_YYYY-MM-DD.md/json`，用于发现 registry report 缺少文档覆盖、`production_effect` 或 common misread 的问题；暂不拆分现有长文档。
- 2026-05-28：`REPORT-052` 进入 VALIDATING。已完成 documentation contract builder、`aits docs report-contract`、Markdown/JSON writer、默认 registry 覆盖测试、report registry 登记、artifact catalog 和 system flow 更新；默认 `config/report_registry.yaml` 与 `docs/artifact_catalog.md` 契约检查为 PASS，验证通过 report/docs 目标 pytest 75 passed、ruff 和 black。
