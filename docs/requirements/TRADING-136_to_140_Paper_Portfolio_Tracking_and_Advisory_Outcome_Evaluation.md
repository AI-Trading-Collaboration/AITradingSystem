# TRADING-136 to TRADING-140 Paper Portfolio Tracking and Advisory Outcome Evaluation

最后更新：2026-06-08

## 1. 背景

TRADING-131_to_135 已完成持续 shadow monitoring、daily position advisory、consensus
drift gate 和 owner review journal。当前系统可以回答每天建议了什么、owner 是否记录了
决定，但还不能评估“如果按建议做纸面动作，后续表现如何”。

本阶段把 daily advisory / owner review 升级为 paper portfolio tracking、advisory
outcome evaluation、owner attribution、shadow candidate aging 和 weekly advisory review。
所有输出仍固定：

- `production_effect=none`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `owner_approval_required=true`
- `manual_review_required=true`
- 不接入 broker API
- 不自动下单
- 不自动生成 production candidate
- 不修改真实 portfolio snapshot、official target weights、baseline config 或 production state

## 2. 子任务

|ID|标题|状态|验收重点|
|---|---|---|---|
|TRADING-136|Paper Portfolio State & Simulation Ledger|VALIDATING|从 manual snapshot 初始化 paper state，owner review 可追加 paper action ledger，ledger 可重建 state。|
|TRADING-137|Advisory Outcome Tracking|VALIDATING|从 daily advisory 创建 outcome tracker，1/5/10/20 trading-day 窗口可更新为 PENDING / AVAILABLE / INSUFFICIENT_DATA。|
|TRADING-138|Owner Decision vs Advisory Attribution|VALIDATING|聚合 owner review journal 与 outcome windows，分析接受/拒绝/手工决策和后续结果。|
|TRADING-139|Shadow Candidate Aging / Promotion Clock v2|VALIDATING|把 monitor runs、drift、outcomes 纳入 aging，输出 eligible / downgrade 但不自动 production。|
|TRADING-140|Weekly Position Advisory Review Report|VALIDATING|聚合本周 advisory / owner / outcome / paper / aging，生成 weekly review 和 Reader Brief section。|

## 3. Paper Portfolio 与真实 Portfolio 的区别

Paper portfolio 是 `mode=advisory_simulation_only` 的本地模拟状态。它只读取 manual
snapshot 或 owner-approved paper review，不代表 broker 持仓，也不替代
`current_portfolio_snapshot.yaml`。真实 portfolio 仍由项目 owner 手工维护；当前阶段没有
broker import、order ticket、自动 rebalance 或 production mutation。

`paper_portfolio_state.json` 是 latest paper simulation state；它的 `positions` 是权重，不是股数
或账户现金余额。`broker_action_taken` 必须始终为 false。

## 4. Paper Action Ledger 如何重建 State

`paper_action_ledger.jsonl` 追加 owner review 导出的 paper-only event。每条 event 记录：

- `review_id` / `daily_advisory_id`
- `owner_decision`
- `before_weights`
- `proposed_deltas`
- `applied_paper_deltas`
- `after_weights`
- `broker_action_taken=false`

重建时从 manifest 里的 initial weights 开始，按 ledger 顺序应用 `applied_paper_deltas`。
重建结果必须与 latest `paper_portfolio_state.json` 一致；不一致时
`validate-paper-portfolio` fail closed。

## 5. Advisory Outcome 如何跟踪

`advisory-outcome track` 从 `daily_advisory_manifest.json`、`daily_advisory_actions.json`、
candidate target weights、consensus weights 和可选 latest paper portfolio state 创建 tracker。
默认窗口为 1、5、10、20 个交易日，来自
`config/etf_portfolio/dynamic_v3_rescue/paper_portfolio_v1.yaml` 的
`outcome_tracking.windows_trading_days`。

`advisory-outcome update` 在通过 cached data quality gate 后，从价格缓存计算：

- paper action return
- no-trade return
- baseline return
- consensus target return
- relative return、max drawdown、realized volatility

未到期窗口保持 `PENDING`；价格或 symbol 覆盖不足时标记 `INSUFFICIENT_DATA`，不得补 0 或伪造
收益。

## 6. Owner Decision Attribution 如何理解

Owner attribution 不是评价 owner 对错，也不是自动调整系统规则。它只把 owner decision 与
系统 `recommended_action`、后续 outcome window 联系起来，回答：

- owner 最常选择什么；
- 哪些建议类型最常被接受或拒绝；
- 被接受和被拒绝建议的后续结果是否有足够证据；
- 数据不足时输出 `INSUFFICIENT_DATA`。

该报告只能作为人工复核材料，不生成 production recommendation。

## 7. Promotion Clock v2 如何计算

Promotion clock v2 从 paper policy config 读取阈值。基础门槛包括：

- `days_observed >= minimum_days_observed`
- `rebalance_count_observed >= minimum_rebalance_count`
- drift warning、high disagreement 和 downgrade warning 不超过配置上限
- outcome score 不低于配置 floor

达到 `eligible_for_review` 只表示可以进入人工 promotion review。即使 eligible，也不得自动写入
production candidate、baseline config、official target weights 或 broker state。

## 8. Weekly Advisory Review 如何阅读

Weekly review 汇总一周内：

- shadow monitor run 次数；
- daily advisory 次数；
- owner review 数量和 decision 分布；
- paper portfolio 当前状态；
- paper vs no-trade / baseline outcome；
- candidate disagreement、aging、downgrade 和 promotion review clock；
- 下周建议。

数据不足时报告必须显示 `INSUFFICIENT_DATA`。`reader_brief_section.md` 只提供压缩摘要，不运行
上游命令、不补造结论。

## 9. 新增 CLI

```bash
aits etf dynamic-v3-rescue paper-portfolio init --config config/etf_portfolio/dynamic_v3_rescue/paper_portfolio_v1.yaml
aits etf dynamic-v3-rescue paper-portfolio apply-review --review-id <review_id>
aits etf dynamic-v3-rescue paper-portfolio state --latest
aits etf dynamic-v3-rescue paper-portfolio report --latest
aits etf dynamic-v3-rescue validate-paper-portfolio --paper-portfolio-id <paper_portfolio_id>

aits etf dynamic-v3-rescue advisory-outcome track --daily-advisory-id <daily_advisory_id>
aits etf dynamic-v3-rescue advisory-outcome update --as-of YYYY-MM-DD
aits etf dynamic-v3-rescue advisory-outcome report --latest
aits etf dynamic-v3-rescue validate-advisory-outcome --outcome-id <outcome_id>

aits etf dynamic-v3-rescue owner-attribution run
aits etf dynamic-v3-rescue owner-attribution report --latest
aits etf dynamic-v3-rescue validate-owner-attribution --attribution-id <attribution_id>

aits etf dynamic-v3-rescue shadow-aging run --shadow-shortlist-id <shadow_shortlist_id>
aits etf dynamic-v3-rescue shadow-aging report --latest
aits etf dynamic-v3-rescue validate-shadow-aging --aging-id <aging_id>

aits etf dynamic-v3-rescue weekly-advisory-review run --week-ending YYYY-MM-DD
aits etf dynamic-v3-rescue weekly-advisory-review report --latest
aits etf dynamic-v3-rescue validate-weekly-advisory-review --weekly-review-id <weekly_review_id>
```

## 10. Artifact Contract

Paper portfolio:

```text
reports/etf_portfolio/dynamic_v3_rescue/paper_portfolio/<paper_portfolio_id>/
  paper_portfolio_manifest.json
  paper_portfolio_state.json
  paper_action_ledger.jsonl
  paper_position_history.jsonl
  paper_portfolio_report.md
```

Advisory outcome:

```text
reports/etf_portfolio/dynamic_v3_rescue/advisory_outcome/<outcome_id>/
  advisory_outcome_manifest.json
  advisory_event.json
  outcome_windows.jsonl
  advisory_counterfactuals.json
  advisory_outcome_report.md
```

Owner attribution:

```text
reports/etf_portfolio/dynamic_v3_rescue/owner_attribution/<attribution_id>/
  owner_attribution_manifest.json
  owner_decision_summary.json
  advisory_acceptance_matrix.json
  decision_outcome_comparison.json
  owner_attribution_report.md
```

Shadow aging:

```text
reports/etf_portfolio/dynamic_v3_rescue/shadow_aging/<aging_id>/
  shadow_aging_manifest.json
  candidate_aging_status.jsonl
  promotion_clock_v2_summary.json
  shadow_aging_report.md
```

Weekly advisory review:

```text
reports/etf_portfolio/dynamic_v3_rescue/weekly_advisory_review/<weekly_review_id>/
  weekly_review_manifest.json
  weekly_advisory_summary.json
  weekly_owner_decision_summary.json
  weekly_paper_portfolio_summary.json
  weekly_shadow_candidate_summary.json
  weekly_review_report.md
  reader_brief_section.md
```

## 11. Validation Plan

Focused tests:

- `tests/test_paper_portfolio.py`
- `tests/test_advisory_outcome.py`
- `tests/test_owner_attribution.py`
- `tests/test_shadow_aging.py`
- `tests/test_weekly_advisory_review.py`

Required gates:

```bash
python -m pytest tests/test_paper_portfolio.py tests/test_advisory_outcome.py tests/test_owner_attribution.py tests/test_shadow_aging.py tests/test_weekly_advisory_review.py -q
python -m ruff check src tests
python -m compileall -q src tests
git diff --check
aits etf dynamic-v3-rescue validate
aits etf dynamic-v3-rescue validate-paper-portfolio --paper-portfolio-id <paper_portfolio_id>
aits etf dynamic-v3-rescue validate-advisory-outcome --outcome-id <outcome_id>
aits etf dynamic-v3-rescue validate-owner-attribution --attribution-id <attribution_id>
aits etf dynamic-v3-rescue validate-shadow-aging --aging-id <aging_id>
aits etf dynamic-v3-rescue validate-weekly-advisory-review --weekly-review-id <weekly_review_id>
aits etf dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue
```

## 12. Progress Notes

- 2026-06-08: 新增需求文档并进入 `IN_PROGRESS`。本阶段目标是从 daily manual advisory
  和 owner review journal，升级为 paper portfolio tracking、advisory outcome evaluation、
  owner attribution、shadow aging v2 和 weekly advisory review。安全边界继续禁止 broker
  action、automatic production candidate 和 real portfolio mutation。
- 2026-06-08: 实现进入 `VALIDATING`。已新增 `paper_portfolio_v1.yaml`、
  `dynamic_v3_paper_tracking.py`、CLI/validate/report registry/artifact catalog/system flow/
  operations runbook/README/Reader Brief/tests；真实验收链路生成 paper portfolio
  `9302bb7ae33b05f1`、paper action `654bafc6a89ff1b2`、advisory outcome
  `19a6097ce18dbcd8`、owner attribution `ed392c821d7b6e8e`、shadow aging
  `4c8cb907918d9769`、weekly advisory review `47e497404be6281f`。验证通过 focused
  pytest、全量 pytest（2254 passed）、ruff、compileall、git diff check、documentation
  contract、dynamic-v3 root validation、新增五类 validate 和 dynamic-v3 artifact validation。
