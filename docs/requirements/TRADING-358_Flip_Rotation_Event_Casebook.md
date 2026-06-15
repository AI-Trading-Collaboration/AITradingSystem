# TRADING-358 Flip Rotation Event Casebook

最后更新：2026-06-16

## 1. 背景

TRADING-357 已建立 drawdown event casebook。后续 qualitative review 还需要一个可审计的
signal flip / rotation event casebook，用于讨论候选方法是否减少 noisy rotations，同时是否
错过 useful regime changes。

## 2. 目标

1. 新增 flip / rotation event casebook schema。
2. 每个 event 记录：
   - date
   - previous state
   - new state
   - trigger signal
   - whether flip was useful
   - whether flip was false-positive
   - turnover impact
   - candidate behavior
3. 新增 summary metrics。
4. 新增 report CLI 和 Reader Brief summary。
5. 新增 validate CLI。
6. 新增 focused tests。

## 3. 非目标

- 不重新计算 historical signal flips。
- 不刷新 market data、macro data 或 signal artifacts。
- 不运行 backtest、stress backfill、A/B review、candidate scoring 或 paper shadow。
- 不作为 trading signal、production approval 或 official target weight source。
- 不生成 order ticket、broker action 或 production mutation。

## 4. Artifact Contract

Runtime root:

- `reports/etf_portfolio/dynamic_v3_rescue/flip_rotation_event_casebook/<casebook_run_id>/`

Artifacts:

- `flip_rotation_casebook_manifest.json`
- `flip_rotation_event_casebook.json`
- `flip_rotation_event_casebook_report.md`
- `flip_rotation_event_casebook_reader_brief.md`
- `flip_rotation_event_casebook_validation.json/md`

Expected summary fields:

- `flip_rotation_casebook_event_count`
- `flip_rotation_useful_count`
- `flip_rotation_false_positive_count`
- `flip_rotation_dominant_trigger`
- `flip_rotation_next_action`

## 5. Source Discipline

Casebook source data must be explicit in
`config/etf_portfolio/dynamic_v3_rescue/flip_rotation_event_casebook_v1.yaml`.
The initial casebook is a manual diagnostic baseline. `turnover_impact` and useful /
false-positive labels are qualitative review classifications, not recalculated trading evidence.
Any future investment-facing conclusion must replace or validate manual labels with a data-backed
signal-event extraction artifact.

## 6. Safety Boundary

All outputs are read-only and fixed to:

- `production_effect=none`
- `manual_review_only=true`
- `flip_rotation_event_casebook_only=true`
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

- `flip-rotation-event-casebook report` 可生成 casebook JSON、Markdown report 和 Reader Brief section。
- `validate-flip-rotation-event-casebook` 返回 PASS。
- Reader Brief 显示 event count、useful count、false-positive count、dominant trigger 和 next action。
- README、operations runbook、system flow、artifact catalog、report registry、requirements 和
  task register 同步更新。
- focused pytest、contract-validation suite、ruff、compileall、git diff check、documentation
  contract、report index 和 Reader Brief quality 通过。

## 8. 进展记录

- 2026-06-15：新增任务并进入 IN_PROGRESS；本阶段只实现 manual diagnostic casebook
  contract、report/validation artifact 和 visibility，不重新计算 historical signal flips、
  不刷新数据、不修改 production state。
- 2026-06-15：实现完成并转入 VALIDATING。真实链路生成 casebook
  `flip-rotation-event-casebook_f99ccfc0ee89458d`，`event_count=5`，
  `useful_flip_count=3`，`false_positive_count=2`，
  `dominant_trigger_signal=v_shaped_recovery_confirmation`，
  `next_review_action=use_casebook_in_next_flip_rotation_review`；report/latest/validate CLI
  均通过，validator `status=PASS`、`failed_check_count=0`。Reader Brief JSON 已显示
  flip/rotation casebook fields；focused pytest 15 passed，contract-validation suite 32
  passed / 25.24s，documentation contract PASS，report index `PASS_WITH_WARNINGS`（既有
  missing/stale visibility），Reader Brief OK，Reader Brief quality OK。保持 research
  diagnostic only / not trading signal / no data refresh / no signal extraction or backtest
  executor / no official target / no broker / no production。
- 2026-06-16：复验完成并转 DONE；真实只读 report 生成
  `flip-rotation-event-casebook_146c806ccd4bb4b3`，`event_count=5`，
  `useful_flip_count=3`，`false_positive_count=2`，
  `dominant_trigger_signal=v_shaped_recovery_confirmation`，
  `next_review_action=use_casebook_in_next_flip_rotation_review`，
  `validate-flip-rotation-event-casebook` 返回 PASS / failed=0。验证通过
  `python -m pytest tests/test_flip_rotation_event_casebook.py tests/test_etf_dynamic_v3_parameter_research.py tests/test_documentation_contract.py tests/test_report_index.py -q`
  36 passed。
