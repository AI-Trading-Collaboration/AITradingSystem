# TRADING-070 ETF Allocation Parameter Review from Forward Evidence

状态：IN_PROGRESS

最后更新：2026-06-02

## 背景

TRADING-062 建立 ETF allocation baseline；TRADING-063 完成 credibility validation；
TRADING-064 完成 calibration experiment pack；TRADING-065 完成 forward simulation
dashboard；TRADING-066 完成 AI confirmation；TRADING-067 完成 satellite replacement；
TRADING-068 weekly review 与 TRADING-069 decision journal 已进入真实人工复核验证阶段。

TRADING-070 在这些 forward evidence、weekly review 和 human journal 记录之上，生成
ETF allocation parameter review package，回答 candidate parameter set 是否值得继续观察、
拒绝、延期，或提交人工参数复核提案。

## 安全边界

所有 TRADING-070 evidence、comparison、proposal、scorecard、report、Reader Brief 摘要和
validation 输出必须固定：

```text
observe_only=true
candidate_only=true
production_effect=none
broker_action=none
manual_review_required=true
```

允许输出：

```text
parameter_review_evidence
evidence_scorecard
candidate_status_recommendation
parameter_change_proposal
manual_review_package
defer_or_reject_reason
```

禁止输出或执行：

```text
apply_parameter_change
overwrite_baseline_config
change_production_weights
enable_broker_action
promote_to_production_without_governance
broker_order
```

缺少足够 forward evidence 时必须输出：

```text
status=needs_more_data
reason=INSUFFICIENT_FORWARD_EVIDENCE
```

不得补造结论或静默把历史 backtest 结论当作 forward evidence。

## 阶段拆解

|阶段|范围|状态|验收|
|---|---|---|---|
|TRADING-070A|Parameter Review Evidence Schema|DONE|Evidence schema validates required fields, date range, nullable metrics with reasons, source links and safety fields|
|TRADING-070B|Forward Evidence Aggregator|DONE|`aits etf parameter-review aggregate --as-of YYYY-MM-DD` 生成 JSON/Markdown evidence package and preserves source paths|
|TRADING-070C|Baseline vs Candidate Evidence Comparison|DONE|Forward evidence compares candidate vs baseline, QQQ, SPY, SMH, backtest expectation, weekly status and journal outcome|
|TRADING-070D|Decision Journal Evidence Linker|DONE|Journal decisions become structured candidate evidence with support/conflict status and traceable links|
|TRADING-070E|Parameter Change Proposal Generator|DONE|Only proposal/review actions are generated; unsafe proposal types are rejected|
|TRADING-070F|Proposal Scoring and Governance Gate|DONE|Proposal scorecard is deterministic and fail-closed on insufficient/unsafe/under-evidenced proposals|
|TRADING-070G|Parameter Review Report Generator|DONE|JSON/Markdown report includes safety banner, source summary, comparisons, journal evidence, scorecard and proposals|
|TRADING-070H|Reader Brief Parameter Review Section|DONE|Reader Brief exposes parameter review status, counts, safety posture and detailed report link|
|TRADING-070I|Parameter Review Validation Gate|READY|`aits etf parameter-review validate` confirms proposal-only behavior and fails closed on unsafe states|

## 验收标准

- TRADING-070A 到 TRADING-070I 均完成并有 focused tests。
- `aits etf parameter-review aggregate/report/run/validate` 可运行。
- Evidence records 必须链接 forward dashboard、weekly review、decision journal、experiment report、candidate gate 和 validation gate；缺失 required source 时输出 `needs_more_data`。
- Baseline/candidate comparison 基于 forward evidence，而不是只基于 historical backtest。
- Proposal generator 只允许 `continue_observation`、`defer_parameter_change`、
  `reject_candidate`、`propose_candidate_for_extended_shadow`、
  `propose_baseline_parameter_review`。
- Governance gate 阻断 unsafe proposal type、`production_effect != none`、
  `broker_action != none`、insufficient forward days、missing baseline comparison、missing
  source links、failed credibility/forward/weekly validation、高 turnover 和高 drawdown。
- Reader Brief 只读展示 latest parameter review report；不运行上游、不写 production weights。
- README、`docs/system_flow.md`、`docs/artifact_catalog.md`、
  `config/report_registry.yaml`、operations runbook 和 task register 同步。
- 最终验证通过：
  `python -m pytest tests -q`、`python -m ruff check config src tests scripts docs`、
  `python -m compileall -q src tests scripts`、`git diff --check` 和
  `python -m ai_trading_system.cli etf parameter-review validate`。

## 进展记录

- 2026-06-02：TRADING-070 新增为 P0 `IN_PROGRESS`。根据 owner 提供的 ETF Allocation
  Parameter Review from Forward Evidence 计划开始按 A-I 顺序实现；本阶段只生成
  evidence-linked proposal 和 manual review package，不写 production weights、不触发 broker
  action、不自动 promotion 或 baseline replacement。
- 2026-06-02：TRADING-070A 完成。新增
  `src/ai_trading_system/etf_portfolio/parameter_review.py` evidence schema、
  required fields、required source links、nullable metric reason validation、date range
  validation 和 safety field enforcement；验证通过 `python -m pytest
  tests/test_etf_parameter_review.py -q`、目标 ruff 和 `git diff --check`。
- 2026-06-02：TRADING-070B 完成。新增
  `aits etf parameter-review aggregate --as-of YYYY-MM-DD`，只读聚合 forward dashboard、
  weekly review、decision journal、experiment comparison、candidate selection、watchlist 和
  validation gates，输出
  `reports/etf_portfolio/parameter_review/aggregation/parameter_review_evidence_YYYY-MM-DD.json/md`；
  缺少 required forward evidence 时输出 `needs_more_data` /
  `INSUFFICIENT_FORWARD_EVIDENCE`，缺失 optional source 保留 warning，不写 production weights
  或 broker state。
- 2026-06-02：TRADING-070C 完成。新增 candidate evidence comparison payload，比较
  candidate vs ETF baseline、QQQ、SPY、SMH、historical experiment expectation、weekly
  review status 和 decision journal outcome，输出
  `outperforming_with_acceptable_risk`、`outperforming_but_risky`、`underperforming`、
  `needs_more_data`、`mixed_evidence` 或 `blocked_by_governance`。比较 policy 是
  TRADING-070 observe-only pilot baseline，后续由 TRADING-070F governance gate 接管正式
  blocker/scorecard。
- 2026-06-02：TRADING-070D 完成。新增 decision journal evidence linker，把 journal
  entries、human decisions、rationale、confidence、follow-up tasks 和 conflict flags
  聚合为 candidate-level evidence，输出 `supportive`、`neutral`、`conflicted`、
  `negative` 或 `insufficient_review`；只读 journal report，不修改 journal state、shadow
  registry、baseline config、production weights 或 broker state。
- 2026-06-02：TRADING-070E 完成。新增 parameter change proposal generator，只允许
  `continue_observation`、`defer_parameter_change`、`reject_candidate`、
  `propose_candidate_for_extended_shadow` 和 `propose_baseline_parameter_review`；每个
  proposal 包含 current/candidate config hash、parameter delta placeholder、supporting
  evidence、blocking evidence、risk summary 和 safety fields，并阻断 unsafe proposal type、
  `production_effect != none` 与 `broker_action != none`。
- 2026-06-02：TRADING-070F 完成。新增 deterministic proposal scorecard 和 fail-closed
  governance gate，按 forward excess return、drawdown improvement、stability、turnover
  penalty、journal support 和 data quality 加权评分，并阻断 insufficient forward days、missing
  baseline comparison、missing journal link、unsafe production effect、broker action、high
  turnover 和 high drawdown。Scorecard status 只输出 `eligible_for_manual_review`、
  `needs_more_data`、`blocked`、`rejected` 或 `continue_shadow`，不应用参数变更。
- 2026-06-02：TRADING-070G 完成。新增 `aits etf parameter-review report/run --as-of
  YYYY-MM-DD`，生成
  `reports/etf_portfolio/parameter_review/reports/parameter_review_YYYY-MM-DD.json/md`；
  报告包含 safety banner、metadata、evidence source summary、candidate comparison、forward
  evidence summary、decision journal summary、proposal scorecard、generated/blocked/rejected
  proposals、manual review requirements、next steps 和 source report links，并登记为
  `etf_parameter_review_report`。
- 2026-06-02：TRADING-070H 完成。Reader Brief 新增 `ETF Parameter Review` 区块，只读
  report index 指向的 latest `etf_parameter_review_report`，展示 status、reviewed
  candidate count、eligible/continue/rejected/needs-more-data/blocked counts、main reason、
  safety posture 和 detailed report link；缺失 report 时显示 `MISSING`，不运行
  parameter-review CLI、不写 production weights 或 broker state。
