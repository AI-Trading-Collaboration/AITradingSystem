# REPORT-058: Reader Brief Decision View

## 背景

2026-05-29 真实 `reader_brief_2026-05-28.html` 已经覆盖 daily score、market panel、
research governance、report index、documentation contract 和 manual review queue。但页面仍偏
系统状态 dump：顶部 `Status=OK` 容易被误解为今日结论可直接使用，缺失 artifact 在
daily score、Reader context 和 promotion chain 之间没有分层，Score Funnel 与 Manual
Review Queue 对读者不够收敛。

本任务把 Reader Brief 从“系统汇总页”进一步压缩为“5-10 分钟投资操作简报入口”。

## 范围

- Reader Brief 顶部拆分三层状态：
  - Reader Brief Build Status。
  - Decision Usability。
  - Research Promotion Status。
- 首页增加 Action Checklist，直接表达今日应做/不应做。
- 缺失 artifact 影响分为：
  - 今日评分链路。
  - 阅读上下文。
  - 研究/权重晋升链路。
- Score & Decision Funnel 展示人读版流程，完整审计字段放入 `<details>`。
- Manual Review Queue 增加 Top Review Items Today，并按影响类型分组。
- Task Cadence Calendar 使用明确 cadence label，而不是仅显示 freshness SLA 文案。
- 首页数字保留 1-2 位；详细审计仍保留原始 JSON/完整精度。
- 增加自然语言“今天相对上一期发生了什么”解释。

## 非目标

- 不改变 score、position gate、weights、promotion gate 或 trading 行为。
- 不生成、补造或重跑缺失的 weekly/biweekly/monthly artifact。
- 不写 production weights、active shadow weights、broker action 或 trading action。
- 不把 Reader Brief quality build OK 解释为投资结论可直接执行。

## 实施步骤

1. 更新 task register，登记本任务。
2. 扩展 Reader Brief payload：
   - `status_panel`
   - `action_checklist`
   - `artifact_impact_summary`
   - `score_change_narrative`
   - `top_review_items`
3. 调整 HTML：
   - 顶部 status panel。
   - 首页 Action Checklist。
   - 缺失 artifact 三层口径。
   - Funnel 人读版 + audit details。
   - Manual Review Top 3 + impact grouping。
4. 在 Reader Brief 显示层补充 runbook 口径的 cadence label，不改 scheduler。
5. 同步 `docs/system_flow.md`、`docs/artifact_catalog.md` 和专项测试。
6. 生成真实 latest Reader Brief 并用 in-app browser 验证首屏。

## 验收标准

- 页面顶部不再只有单一 `Status=OK`；必须同时展示 build / decision usability /
  promotion status。
- `wait_manual_review`、manual review critical 项或 data gate limitation 存在时，
  Decision Usability 必须为 `MANUAL_REVIEW_REQUIRED` 或更严格状态。
- Report Index missing 与 Research Governance missing 分别说明对今日评分、阅读上下文、
  promotion chain 的不同影响。
- 首页 Action Checklist 包含“不新增 AI 风险资产仓位”“保持/复核最终仓位上限”“复核关键数据
  warning”“不做 weight promotion”这类可执行语句。
- Funnel 首页数字降噪，完整 source artifacts / formulas / JSON-like fields 只在 details 中。
- Manual Review Queue 首页显示 Top 3，并按影响今日结论、影响研究晋升、仅审计/观察分组。
- Task Cadence Calendar 至少能清楚区分 daily、weekly、biweekly、monthly、ad hoc / on change。
- `tests/test_reader_brief.py` 覆盖新增 payload 和 HTML 关键结构。
- 真实 `aits reports reader-brief --latest` 与 `aits reports validate-reader-brief --latest`
  通过；输出 `production_effect=none`。

## 状态记录

- 2026-05-29：新增并进入 `IN_PROGRESS`。原因：owner 针对真实 Reader Brief 指出
  状态层级、missing artifact 口径、行动清单、funnel 噪音、manual review 收敛和 cadence
  可读性问题。
- 2026-05-29：已实现并进入验证。完成 payload/HTML 改造、专项断言和显示层 cadence
  override；尚需生成真实 latest Reader Brief、运行 validate-reader-brief 并做浏览器检查。
- 2026-05-29：验证通过：`tests/test_reader_brief.py`、`tests/trading_engine/test_reader_brief.py`、
  `tests/test_report_index.py`、`tests/test_documentation_contract.py` 通过；`ruff`、`black --check`、
  `git diff --check` 通过；`aits reports reader-brief --latest` 和
  `aits reports validate-reader-brief --latest` 均为 `OK`。已同步当前 daily run bundle 的
  Reader Brief / quality artifact。浏览器自动化读取本地 `file://` 页面被安全策略拦截，
  已改用 HTML 结构校验确认关键模块存在，未做自动视觉截图。
