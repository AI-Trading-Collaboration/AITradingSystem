# 日报置信度提升工作计划

状态：VALIDATING

最后更新：2026-05-11

关联任务：`DATA-016`、`RISK-013`、`FUND-001`、`THESIS-002`、`PROD-004`、`PROD-006`

## 背景

当前日报置信度主要受五类约束影响：

1. Marketstack 第二行情源坏点、复权/拆股日期和 adjusted close 口径差异仍只表现为质量警告或调查项，缺少可审计 reconciliation 结论。
2. 政策/地缘风险事件需要官方来源权限、OpenAI 预审队列/官方候选复核，以及覆盖当日的复核声明，才能从 `insufficient_data` 进入更高可信来源类型。
3. TSM 基本面不能被硬套 SEC companyfacts 季度覆盖预期；应以 TSMC IR 官方季度数据作为等价可审计来源，或显式调整缺口规则。
4. 核心 ticker 缺少活跃交易 thesis，日报只能报告趋势和观察状态，不能形成高置信主动交易假设。
5. forward-only PIT 和 outcome 样本受时间窗口限制，不能回填或伪造，只能持续运行、报告覆盖和真实结果标签。

## 决策

- 数据质量警告只能通过可审计 reconciliation 降级或解除；不得静默忽略 Marketstack 坏点、指数 volume 口径、拆股日期或分红复权差异。
- 能证明属于指数/ETF volume 或分红复权口径差异的，必须记录匹配规则、样本日期、主/二源数值、证据来源和规则版本。
- 无法归因的 raw close 冲突、OHLC 异常或非正价格仍保留为门禁错误或显式调查项。
- 风险事件真实人工复核不能由系统伪造。系统可以补齐权限、候选队列、复核报告和 `record-review-attestation` 写入能力；若没有真实 reviewer 和结论，不得写入“无重大事件”复核声明。
- TSM 基本面以 TSMC IR 官方季度 Management Report 为 primary/manual-audited 官方来源，进入统一 SEC-style metrics 后可满足 TSM 的季度基本面覆盖；不得用空 SEC companyfacts 行误伤。
- 核心 ticker thesis 先建立 active baseline，必须包含验证指标、证伪条件、复核频率、来源和适用边界；后续真实复核可更新状态。
- PIT/outcome 只做 forward-only 自然积累，输出成熟度报告和缺口，不回填历史视图。

## 阶段拆解

|阶段|任务|输出|验收标准|
|---|---|---|---|
|1|Marketstack reconciliation|可审计 reconciliation CSV/Markdown；数据质量报告引用|坏点、拆股/复权日期、adjusted close 口径差异被分类；可解释项有规则和证据；不可解释项不静默通过|
|2|风险事件复核闭环|官方权限 preflight、候选/队列复核报告、复核声明写入路径|能区分“真实复核无重大事件”“仍待复核”“仅 LLM formal”；没有真实 reviewer 时不得写人工复核声明|
|3|TSM 基本面覆盖|TSM IR 覆盖校验和 SEC-style merge 规则调整|TSM 缺 SEC companyfacts 季度不再误伤；缺 TSM IR 时明确报缺官方 IR 覆盖|
|4|初始主动 baseline thesis|原 6 个 ticker active thesis YAML；校验/复核报告|MSFT、GOOG、TSM、INTC、AMD、NVDA 均有 thesis、验证指标、证伪条件和复核频率；2026-05-11 扩展后的新增 ticker 暂按 `watch_only` 观察池处理，不要求主动交易 thesis|
|5|PIT/outcome 成熟度|PIT coverage 与 outcome/shadow 成熟度报告|报告真实样本数、缺跑、pending/missing 和 readiness；不得提升未成熟窗口可信度|

## 开放问题

- 风险事件人工复核需要真实 reviewer、复核时间、source scope 和结论；系统不能代替 owner 签署人工复核。
- 若 Marketstack/FMP raw close 出现无法解释冲突，需要 owner 决定是否重新拉取缓存、替换第二源、或保持质量门禁阻断；若冲突落在已核验 corporate action 窗口内，且数值比例能匹配拆股比例，可以按 corporate-action window 归因降级为可审计信息。
- 核心 ticker thesis 的内容属于投资假设 baseline，后续应由 owner 复核后再用于主动交易纪律。

## 进展记录

- 2026-05-10：新增工作计划并进入实现。原因：owner 指定五项能提升日报置信度的近期工作，且至少数据质量 reconciliation、TSM 覆盖和核心 thesis 可直接工程化。
- 2026-05-10：完成 Marketstack reconciliation 基础验证。`aits validate-data --as-of 2026-05-10` 状态从 `PASS_WITH_WARNINGS` 收敛为 `PASS`，错误 0、警告 0、信息 11；新增 companion CSV `outputs/reports/data_quality_2026-05-10_marketstack_reconciliation.csv`，记录 Marketstack 坏点和 adjusted close 口径差异的规则、证据和主/二源数值。
- 2026-05-10：风险事件系统侧闭环已复核。`CONGRESS_API_KEY`、`GOVINFO_API_KEY`、`OPENAI_API_KEY` 当前环境可见；`data-sources validate`、`risk-events fetch-official-sources`、`triage-official-candidates` 和 `validate-occurrences` 已运行。当前只有 LLM formal attestation，不存在真实人工 reviewer 的复核结论，因此未写入 manual_input 复核声明。
- 2026-05-10：TSM IR 基本面覆盖已合并。`fundamentals merge-tsm-ir-sec-metrics --as-of 2026-05-10` 后，SEC-style metrics 行数从 66 增至 72；`validate-sec-metrics` 与 `build-sec-features` 均为 PASS，TSM 季度缺口消失。
- 2026-05-10：补齐 TSM IR 日报链路。`ops daily-plan` 已在 `validate-sec-metrics` 前加入 `fundamentals merge-tsm-ir-sec-metrics`，`score-daily` 在本地 `data/processed/tsm_ir_quarterly_metrics.csv` 存在时会先按 as-of 合并 TSM IR，再执行 SEC metrics 校验和特征构建，避免只靠手工命令维持覆盖。
- 2026-05-10：已为 MSFT、GOOG、TSM、INTC、AMD、NVDA 写入 active baseline thesis；`thesis validate` PASS。`thesis review` 仍为 `PASS_WITH_WARNINGS`，原因是部分业务驱动指标保留 `pending`，需要 owner/人工业务复核，未伪装为 confirmed。
- 2026-05-11：核心观察池扩展到 17 个代表性 AI 产业链 ticker；本计划中的 thesis 项保留为原 6 个主动 baseline thesis，新增 ticker 暂不作为主动交易 thesis 候选。
- 2026-05-10：已刷新 forward-only maturity 报告。`backtest-pit-coverage` 为 `PASS_WITH_WARNINGS`，prediction outcome 可用样本 2，decision outcome 可用样本 6，shadow maturity 为 `PASS_WITH_LIMITATIONS`；继续受真实时间窗口约束。
- 2026-05-10：综合验证 `score-daily --as-of 2026-05-10 --skip-risk-event-openai-precheck` 通过，日报状态 `PASS_WITH_LIMITATIONS`，AI 产业链评分 74.9，判断置信度 83.0（high）。执行建议仍为 `wait_manual_review`，原因是风险/业务复核仍存在真实人工闭环缺口。
- 2026-05-11：expanded universe 真实 `download-data --full-universe` 后，价格覆盖已补齐，但 `validate-data --as-of 2026-05-10 --full-universe` 暴露 `NOW` 2025-12-17/18 拆股窗口内主源/Marketstack raw close 口径差异。ServiceNow 官方 IR 公告确认 5-for-1 split，2025-12-18 开始 split-adjusted 交易；本轮将补 `NOW` 已知拆股事件，并把拆股窗口内比例匹配的二源 raw close 日期口径差异归入可审计 reconciliation，而不是静默忽略。
- 2026-05-11：`NOW` 拆股窗口 reconciliation 已完成。`config/data_quality.yaml` 已记录 ServiceNow 官方 5-for-1 split，`validate-data` 新增 `known_split_raw_close_basis_difference` 分类，仅当主源/二源 raw close 比例匹配已配置拆股比例且落入 corporate-action window 时降级为 INFO。真实 `validate-data --as-of 2026-05-10 --full-universe` 为 PASS，错误 0、警告 0、信息 12；交易日 replay 内 `data_quality_2026-05-08` 同样 PASS。
