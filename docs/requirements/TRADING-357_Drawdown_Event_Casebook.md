# TRADING-357 Drawdown Event Casebook

最后更新：2026-06-15

## 1. 背景

TRADING-356 已建立 reusable stress scenario library。后续 qualitative review 还需要一个可审计的
historical drawdown event casebook，用于解释候选方法在典型 drawdown event 中应如何被人工复核。

## 2. 目标

1. 新增 drawdown event casebook schema。
2. 每个 event 记录：
   - event name
   - start date
   - end date
   - max drawdown
   - recovery behavior
   - regime label
   - candidate response
   - benchmark response
   - review notes
3. 新增 report CLI。
4. 新增 Reader Brief summary。
5. 新增 validate CLI。
6. 新增 focused tests。

## 3. 非目标

- 不重新计算 historical drawdown。
- 不刷新 market data、macro data 或 benchmark data。
- 不运行 stress backfill、A/B review、candidate scoring 或 paper shadow。
- 不作为 trading signal、production approval 或 official target weight source。
- 不生成 order ticket、broker action 或 production mutation。

## 4. Artifact Contract

Runtime root:

- `reports/etf_portfolio/dynamic_v3_rescue/drawdown_event_casebook/<casebook_run_id>/`

Artifacts:

- `drawdown_casebook_manifest.json`
- `drawdown_event_casebook.json`
- `drawdown_event_casebook_report.md`
- `drawdown_event_casebook_reader_brief.md`
- `drawdown_event_casebook_validation.json/md`

Expected summary fields:

- `drawdown_casebook_event_count`
- `drawdown_casebook_worst_event`
- `drawdown_casebook_regime_coverage`
- `drawdown_casebook_next_action`

## 5. Source Discipline

Casebook source data must be explicit in
`config/etf_portfolio/dynamic_v3_rescue/drawdown_event_casebook_v1.yaml`.
The initial casebook is a manual diagnostic baseline. `max_drawdown` values are research
proxies used for qualitative review, not recalculated performance evidence. Any future
investment-facing conclusion must replace or validate the manual proxies with a data-backed
drawdown calculation artifact.

## 6. Safety Boundary

All outputs are read-only and fixed to:

- `production_effect=none`
- `manual_review_only=true`
- `drawdown_event_casebook_only=true`
- `research_diagnostic_only=true`
- `not_trading_signal=true`
- `data_downloaded_by_casebook=false`
- `pipelines_executed_by_casebook=false`
- `not_official_target_weights=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `production_state_mutated=false`
- `automatic_candidate_promotion=false`
- `auto_apply=false`

## 7. 验收标准

- `drawdown-event-casebook report` 可生成 casebook JSON、Markdown report 和 Reader Brief section。
- `validate-drawdown-event-casebook` 返回 PASS。
- Reader Brief 显示 event count、worst event、regime coverage 和 next action。
- README、operations runbook、system flow、artifact catalog、report registry、requirements 和
  task register 同步更新。
- focused pytest、contract-validation suite、ruff、compileall、git diff check、documentation
  contract、report index 和 Reader Brief quality 通过。

## 8. 进展记录

- 2026-06-15：新增任务并进入 IN_PROGRESS；本阶段只实现 manual diagnostic casebook
  contract、report/validation artifact 和 visibility，不重新计算 drawdown、不刷新数据、不修改
  production state。
- 2026-06-15：实现完成并转入 VALIDATING。真实链路生成 casebook
  `drawdown-event-casebook_ba52f43578aae612`，`event_count=5`，
  `worst_event=semiconductor_pullback_2024_07`，`regime_coverage=risk_off,
  semiconductor_pullback, sideways_choppy, strong_recovery, tech_drawdown`，
  `next_review_action=use_casebook_in_next_drawdown_mismatch_review`；report/latest/validate
  CLI 均通过，validator `status=PASS`、`failed_check_count=0`。Reader Brief JSON 已显示
  drawdown casebook fields；focused pytest 12 passed，contract-validation suite 29 passed /
  22.98s，documentation contract PASS，report index `PASS_WITH_WARNINGS`（既有 missing/stale
  visibility），Reader Brief OK，Reader Brief quality OK。保持 research diagnostic only /
  not trading signal / no data refresh / no stress or backtest executor / no official target /
  no broker / no production。
