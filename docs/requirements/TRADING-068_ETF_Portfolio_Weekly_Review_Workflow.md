# TRADING-068 ETF Portfolio Weekly Review Workflow

状态：VALIDATING

最后更新：2026-06-02

## 背景

TRADING-062 已建立 ETF allocation baseline，TRADING-063 完成 credibility
validation，TRADING-064 完成 calibration experiment pack，TRADING-065 完成 forward
simulation dashboard，TRADING-066 完成 AI Confirmation Score Calibration，TRADING-067
完成 Satellite Stock Replacement Policy。TRADING-068 在这些模块之上新增统一的周度人工复核入口。

核心问题：

```text
What should the user review each week before deciding whether to keep observing,
reject candidates, start new experiments, or adjust future research priorities?
```

本阶段不自动化生产决策，只把已有模块输出汇总为可追溯的 weekly review package。

## 安全边界

所有 TRADING-068 输出必须固定：

```text
observe_only=true
candidate_only=true
production_effect=none
broker_action=none
manual_review_required=true
```

允许输出：

```text
manual_review_actions
candidate_status_suggestions
watchlist_items
observation_notes
next_week_watch_items
links_to_reports
```

禁止输出：

```text
production_weight_update
broker_order
auto_promote_candidate
auto_reject_without_review
```

汇总层只读现有 artifact；缺失可选报告时输出 `missing_data` / `REPORT_NOT_FOUND`，不得补跑上游或伪造结论。

## 阶段拆解

|阶段|范围|状态|验收|
|---|---|---|---|
|TRADING-068A|Weekly Review Data Aggregator|DONE|读取 latest ETF brief、forward、experiment、AI confirmation、satellite、watchlist 和 validation artifacts，保留 source paths 与 missing sections|
|TRADING-068B|Portfolio Decision Summary|DONE|生成 regime、目标权重、权重变化、benchmark context 和 observe-only actionability note|
|TRADING-068C|Shadow Candidate Review Section|DONE|汇总 active shadow candidates、forward metrics、allowed actions 和 source evidence|
|TRADING-068D|AI Confirmation Review Section|DONE|汇总 AIConfirmationScore、score band、components、event risk、coverage 和 candidate-only impact|
|TRADING-068E|Satellite Replacement Review Section|DONE|汇总 eligible/watch/fallback stocks、replacement plan、constraints 和 ETF-first fallback|
|TRADING-068F|Risk / Watchlist / Constraint Summary|DONE|集中风险警告、watchlist、constraint hits、data quality、stale/missing report 和 validation gate failures|
|TRADING-068G|Manual Review Action Items|DONE|从 evidence 生成人工复核 action items，并阻断 unsafe action types|
|TRADING-068H|Weekly Review Report Generator|DONE|生成 JSON/Markdown weekly review report，包含安全 banner、source links 和 validation status|
|TRADING-068I|Reader Brief Weekly Review Navigation|DONE|Reader Brief 与 report registry 暴露 latest weekly review summary 和 detail link|
|TRADING-068J|Weekly Review Validation Gate|DONE|`aits etf weekly-review validate` fail-closed 校验 A-I 完整性、安全边界和 unsafe action block|

## 验收标准

- `aits etf weekly-review generate --as-of YYYY-MM-DD` 生成 JSON/Markdown weekly review package。
- `aits etf weekly-review validate` 输出 PASS，并确认：
  - aggregator、portfolio summary、shadow candidate section、AI confirmation section、satellite replacement section、risk/watchlist section、manual review actions、report generator 和 Reader Brief integration 可用；
  - source report traceability 存在；
  - unsafe actions 被阻断；
  - `observe_only=true`、`candidate_only=true`、`production_effect=none`、`broker_action=none`、`manual_review_required=true`。
- Weekly review 只读 existing artifacts，不写 production weights，不触发 broker action，不自动 promotion 或 rejection。
- 全量 `python -m pytest tests -q`、`python -m ruff check config src tests scripts docs`、`python -m compileall -q src tests scripts` 和 `git diff --check` 通过。
- README、`docs/system_flow.md`、`docs/artifact_catalog.md`、`config/report_registry.yaml` 和 task register 同步。

## 进展记录

- 2026-06-02：TRADING-068 新增为 P0 `IN_PROGRESS`。根据 owner 提供的开发计划开始按 A-J 顺序实现；本阶段固定 observe-only / candidate-only / no broker action，并优先复用 TRADING-064~067 现有 artifacts。
- 2026-06-02：TRADING-068A~J baseline implementation 完成并进入 `VALIDATING`。新增
  `src/ai_trading_system/etf_portfolio/weekly_review.py`、`aits etf weekly-review
  aggregate/generate/run/validate`、Reader Brief `Weekly Portfolio Review` 区块、
  report registry、artifact catalog、system flow、operations runbook 和测试覆盖。
  验证通过：`python -m pytest tests -q`（1811 passed）、`python -m ruff check
  config src tests scripts docs`、`python -m compileall -q src tests scripts`、
  `git diff --check`、`python -m ai_trading_system.cli etf weekly-review validate`
  （PASS）和 `python -m ai_trading_system.cli etf weekly-review generate --as-of
  2026-05-29`。所有 weekly review 输出保持 `observe_only=true`、
  `candidate_only=true`、`production_effect=none`、`broker_action=none`、
  `manual_review_required=true`，不写 production weights、不触发 broker action、
  不允许 automatic promotion 或 rejection。
