# Reader Brief UX Quality & Missing Artifact Impact Layer

## 背景

`REPORT-053` 已把 Reader Brief 做成统一 HTML/JSON 阅读入口。真实
`2026-05-27` 运行证明方向可用，但页面仍偏表格化：首屏叙事不足、缺失
artifact 只有 `MISSING` 而缺少影响解释、manual review queue 不够可行动、
Market Situation 对真实市场信息披露有限，且主视图暴露了过长本地路径。

## 目标

在现有 `aits reports reader-brief` 基础上增强读者体验，不新建第二套报告。
读者应在 30 秒内看懂：

- 今日市场/系统结论是什么；
- 为什么形成该结论；
- 哪个 gate 或质量条件限制了最终决策；
- 哪些事项必须人工复核；
- 缺失 artifact 对今日结论是否重要；
- 下一步该打开哪些详细报告。

## 范围

- Reader Brief 保持只读，不运行 scoring、backtest、shadow、SEC PIT、weight
  iteration、数据抓取或 documentation contract 上游命令。
- 继续输出 `outputs/reports/reader_brief_YYYY-MM-DD.html` 与 `.json`。
- 新增只读质量输出：
  - `outputs/reports/reader_brief_quality_YYYY-MM-DD.json`
  - `outputs/reports/reader_brief_quality_YYYY-MM-DD.md`
- 新增可选校验命令：
  - `aits reports validate-reader-brief --date YYYY-MM-DD`
  - `aits reports validate-reader-brief --latest`

## 实施拆解

1. 叙事化 Executive Summary：在表格前新增 `today_conclusion`、
   `why_this_conclusion`、positive/negative drivers、binding constraint、
   manual review summary 和 `production_effect_statement`。
2. Missing / Limited Artifact Impact：对缺失或受限 artifact 输出
   `impact_level`、reader impact、decision impact、recommended action 和
   `production_effect`，impact 仅使用 `BLOCKING`、`IMPORTANT`、`OPTIONAL`、
   `INFO`。
3. Manual Review Queue 分级：按 Critical / Warning / Info 展示，并给出
   `recommended_next_action`、`decision_impact`、source artifact 和
   production impact。
4. Contribution Summary：在 component 明细前总结主要正贡献、零贡献/拖累、
   最大加分、最大拖累，以及 score 与 binding gate 的关系。
5. Market Situation Minimum Panel：缺少详细市场面板时也显式输出
   `MISSING_PRICE_PANEL`、可用 proxy 状态和后续建议。
6. Artifact Path Display Cleanup：主视图只展示短 artifact 名，完整路径放入
   `<details>` 或 JSON secondary fields。
7. Report Navigation Upgrade：按 Core decision artifacts、Detailed evidence、
   Governance / documentation、Missing but expected 分组，并说明为什么打开。
8. Task Cadence Calendar Fallback：缺少 `report_index` 时从
   `config/report_registry.yaml` 生成 `source=registry_fallback` 的 fallback。
9. Quality Report：校验 Reader Brief JSON/HTML 质量状态，区分 `OK`、
   `PASS_WITH_WARNINGS`、`LIMITED_READER_CONTEXT`、`FAILED`。

## 验收标准

- Reader Brief 首屏先给叙事结论，再给证据和计算解释。
- 缺失 artifact 都有 reader/decision impact 和 recommended action。
- Manual Review Queue 分组且每项可行动。
- Component Explainability 前有 contribution summary。
- Market Situation 明确已知、未知和价格面板缺失。
- 主表不直接暴露长绝对路径，但保留审计路径。
- Report Navigation 按目的分组。
- 缺少 `report_index` 时 Task Cadence Calendar 能从 registry fallback 渲染。
- `validate-reader-brief` 生成 quality JSON/Markdown。
- 相关测试、系统流图、artifact catalog、registry 和 runbook 更新。

## 进展记录

- 2026-05-28：新增并进入 `IN_PROGRESS`。原因：`REPORT-053` 已完成 Reader
  Brief HTML v1，真实运行暴露出读者体验、缺失影响解释和质量输出缺口。
- 2026-05-28：进入 `VALIDATING`。已新增 narrative summary、missing/limited
  artifact impact、manual review 分组行动、contribution summary、market minimum
  panel、短路径主视图、grouped report navigation、registry fallback cadence、
  `validate-reader-brief` 和 quality JSON/Markdown 输出。验证通过目标 pytest、
  `tests/test_daily_task_dashboard.py`、全量 pytest、ruff、black，并生成
  `reader_brief_2026-05-27.*` 与 `reader_brief_quality_2026-05-27.*`。
