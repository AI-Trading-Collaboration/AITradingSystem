# UI-002 每日决策 Dashboard v2

状态：VALIDATING

最后更新：2026-05-13

关联任务：`UI-002`、`UI-001`、`REPORT-001`、`REPORT-002`、`REPORT-003`

## 背景

`UI-001` 已提供本地静态 evidence-first dashboard，能从日报 Markdown、evidence bundle、decision snapshot 和可选 belief_state 连接结论、证据、输入数据与质量门禁。Owner 认可继续推进数据展示层，但明确不应把当前阶段做成在线 Web App 或交易终端。

Dashboard v2 的定位仍是只读解释层：降低每日复核成本，让用户先看到“今天结论是什么、和上一交易日相比有没有变化、为什么、可靠性如何、需要人工复核什么”，再进入 evidence、dataset、quality 和 trace 下钻。

## 范围

- 沿用 `aits reports dashboard`，不新增平行 CLI 命令，避免文档和系统流命令漂移。
- 输出仍以 `outputs/reports/evidence_dashboard_YYYY-MM-DD.html` 为默认 HTML 入口。
- 新增可选 JSON payload 输出，用于测试、审计和后续前端演进。
- 可选读取 `outputs/reports/alerts_YYYY-MM-DD.md`，生成告警状态、严重度计数、Top alerts 和复核入口。
- 可选读取 `data/processed/scores_daily.csv`，生成近 20 个交易日趋势摘要，包括 overall score、final AI 仓位区间、总风险资产预算、置信度和触发 gate 数。
- 可选读取 `outputs/reports/market_feedback_optimization_YYYY-MM-DD.md`、`outputs/reports/feedback_loop_review_YYYY-MM-DD.md` 和 `outputs/reports/investment_weekly_review_YYYY-MM-DD.md`，展示反馈闭环、as-if 样本状态、学习队列、候选规则和本期投资复盘的关键结论。
- Dashboard 只读取已生成 artifact，不调用外部 API，不重新计算评分，不改变仓位闸门，不写 prediction ledger。

## 不在本阶段实现

- 不引入 React、FastAPI、Streamlit 服务、数据库、登录权限、远程部署或实时行情。
- 不提供交易下单、账户连接、交易确认或自动执行入口。
- 不在浏览器端直接调用外部数据源。
- 不把缺失的 alerts 或历史 CSV 作为阻断错误；这些输入缺失时页面显示限制说明。
- 不把反馈复盘报告缺失作为阻断错误；dashboard 应显示未接入限制，而不是在展示层重新跑复盘。

## 阶段拆解

### 阶段 1：Decision Card 与 JSON 契约

- 从既有日报结论卡、decision snapshot 和 trace bundle 组装 v2 decision payload。
- HTML 顶部展示今日动作、最终 AI 仓位、总风险资产预算、置信度、Data Gate、最大限制、变化摘要、market regime 和 `production_effect=none`。
- JSON 输出同一组字段，供测试和后续静态站点索引复用。

验收标准：

- `aits reports dashboard --json-output-path ...` 能同时输出 HTML 和 JSON。
- JSON 声明 `production_effect=none`，并记录输入 artifact 路径。
- HTML 不隐藏 Markdown 日报和 trace bundle 的审计源地位。

### 阶段 2：告警聚合

- 从 alerts Markdown 读取状态、活跃告警数、data/system、investment/risk、critical/high/warning 计数。
- 从告警表提取严重度最高的告警标题、来源和触发条件。
- 页面显示缺失或不可解析 alerts 报告的限制说明。

验收标准：

- Dashboard 能展示告警状态、严重度摘要和 Top alerts。
- 告警 section 明确 `production_effect=none`，不改变评分、仓位、回测或执行建议。

### 阶段 3：历史趋势摘要

- 从 `scores_daily.csv` 读取 `overall` 行，按 `as_of` 截止取最近 20 个交易日。
- 展示 overall score、confidence、final AI 仓位上限、总风险资产预算上限和 triggered gates 数。
- 不做复杂交互或图表缩放，先用可审计表格和简约 sparkline 文本。

验收标准：

- 历史趋势缺失时降级为限制说明。
- 历史趋势不把失败或缺失日期伪装为同等可信结论。

### 阶段 4：daily-run 自动触发

- 在 `ops daily-run` 中把 `aits reports dashboard --as-of YYYY-MM-DD` 接到 `score_daily` 之后、`pipeline_health` 之前。
- 该步骤只在交易日启用；休市日默认跳过，因为休市日不会生成新的 daily_score、trace、decision snapshot 或执行动作。
- 该步骤不需要额外环境变量，不调用外部 API，`blocks_downstream=False`。

验收标准：

- `daily-plan` 能显示 `reports_dashboard` 步骤及 HTML/JSON 输出路径。
- `reports_dashboard` 失败不应改变已生成评分、仓位、回测或执行建议，但执行报告应记录该步骤失败。
- 最近交易日全流程能实际生成 `evidence_dashboard_YYYY-MM-DD.html` 和 `.json`。

### 阶段 5：反馈复盘摘要

- 从市场反馈优化报告读取状态、readiness、复核窗口、as-if 回放窗口、decision/prediction 可用样本、学习队列分类、候选规则数和下一步。
- 从反馈闭环周期复核报告读取 warning 数、outcome/prediction 覆盖、因果链、学习队列和 task register 阻断摘要。
- 从投资周报读取本期评分/置信度/仓位变化、最新触发 gate、前三个证据和 production/shadow 覆盖。
- 页面和 JSON 均声明这些内容来自既有报告；缺失时只进入 dashboard 限制说明，不改变日报、评分或规则。

验收标准：

- `aits reports dashboard --as-of YYYY-MM-DD` 在相关报告存在时展示“反馈复盘与学习闭环”区块。
- JSON payload 含 `feedback_review`，记录关键结论、样本覆盖和输入报告路径。
- 未配置相关复盘报告时 dashboard 仍可生成并显示未接入；显式传入但缺失的路径应进入限制说明。

## 状态记录

- 2026-05-10：新增并进入实现。范围限定为既有静态 dashboard 的只读增强，不新增服务端、不新增交易能力。
- 2026-05-10：实现进入 `VALIDATING`。`aits reports dashboard` 已生成 HTML 与 JSON payload；页面增加今日决策视图、告警聚合和近 20 个交易日趋势摘要；单元测试、docs freshness、ruff、mypy 和真实 `2026-05-10` artifact 生成均通过。浏览器插件拒绝直接打开 `file://` 本地 HTML，视觉复核留到后续真实使用或安全允许的本地预览方式。
- 2026-05-10：根据 owner 要求回到 `IN_PROGRESS`，追加 daily-run 自动触发阶段，准备接入后运行最近交易日全流程查阻断点。
- 2026-05-10：自动触发阶段进入 `VALIDATING`。`reports_dashboard` 已接入 `ops daily-run`，交易日位于 `score_daily` 之后、`pipeline_health` 之前，休市日随 `score_daily` 跳过；`tests/test_ops_daily.py` 覆盖顺序、输出和休市日跳过。真实 `daily-run --as-of 2026-05-08 --run-id dashboard-auto-flow-2026-05-08` 通过 download/PIT/SEC/valuation 后阻断于 `score_daily`：`risk_event_prereview_openai_2026-05-08.md` 报告 `risk_event_prereview_request_in_future`，原因是 2026-05-10 对 2026-05-08 执行 live OpenAI 预审。该阻断发生在 dashboard 步骤前，不是 dashboard 自动触发本身的失败。
- 2026-05-10：历史复现入口修复后，`replay-day --as-of 2026-05-08 --mode cache-only --openai-replay-policy cache-only --compare-to-production --run-id dashboard-auto-flow-2026-05-08-replay-visibility` 已通过，replay bundle 内生成 `evidence_dashboard_2026-05-08.html/json`。`daily-run --as-of 2026-05-08 --run-id dashboard-auto-flow-2026-05-08-visibility-check` 现在会在入口返回 `BLOCKED_VISIBILITY`，不再先执行 live provider/LLM 步骤。
- 2026-05-11：run bundle 输出隔离修复进入实现。复核发现休市日
  `reports_dashboard` 被标记 `SKIPPED` 后，legacy mirror 仍可能把
  `outputs/reports/evidence_dashboard_<as_of>.html/json` 中旧的同日期文件复制到
  本轮 run 目录。修复目标：canonical run bundle 只接收本轮运行开始后实际产出的
  legacy artifact，skipped dashboard 不得出现在本轮 canonical 输出中。
- 2026-05-11：run bundle 输出隔离修复进入 `VALIDATING`。`daily-run`
  调用 `mirror_legacy_reports_to_run` 时传入本轮 `started_at`，mirror 只复制
  mtime 不早于运行开始时间的同日 legacy artifact；新增测试证明旧 dashboard
  不会进入休市日 run bundle，而本轮生成的 health/report 文件仍会被收录。
- 2026-05-11：`RUN-002` 将 `daily-run` canonical run bundle 调整为
  `outputs/runs/daily/<executed_at_utc>/as_of_<YYYY-MM-DD>__<safe_run_id>/`；
  dashboard 隔离规则继续适用，即 skipped dashboard 不得从 legacy 路径进入本轮
  timestamp-scoped run bundle。
- 2026-05-13：根据 owner 要求回到 `IN_PROGRESS`，追加反馈复盘摘要阶段。目标是在不重算复盘、不改变 production 逻辑的前提下，把 `market_feedback_optimization`、`feedback_loop_review` 和 `investment_weekly_review` 的关键结论与样本覆盖接入 dashboard HTML/JSON。
- 2026-05-13：反馈复盘摘要阶段进入 `VALIDATING`。`aits reports dashboard`
  已可接入同日 market feedback、feedback loop 和 investment weekly review
  报告；HTML 新增“反馈复盘与学习闭环”区块，JSON 新增
  `feedback_review` payload。已重生成 2026-05-12 dashboard；目标单测、全量
  pytest、ruff、窄范围 mypy 和 `git diff --check` 通过。全量 mypy 仍受仓库既有
  类型债阻断，非本次 dashboard 改动引入。
