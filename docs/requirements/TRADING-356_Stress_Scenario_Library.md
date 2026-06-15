# TRADING-356 Stress Scenario Library

最后更新：2026-06-15

## 1. 背景

TRADING-354 已建立 evidence staleness monitor，确保候选决策不会继续依赖陈旧证据。
下一步需要把 candidate validation 使用的 stress scenarios 固化为可复用、可审计、可报告的
Dynamic v3 rescue 专用库，避免每个 stress backfill 或 qualitative review 临时定义场景。

## 2. 目标

1. 定义标准 stress scenarios：
   - rapid drawdown
   - slow drawdown
   - V-shaped recovery
   - high volatility sideways market
   - false risk-off cluster
   - rate shock
   - AI sector correction
   - semiconductor-led selloff
   - liquidity squeeze
2. 为每个 scenario 增加 metadata、selection rationale、expected failure modes 和 required evidence。
3. 新增 report CLI，用于生成和展示可用 scenarios。
4. 新增 validation CLI。
5. 新增 focused tests。
6. 文档说明 stress scenarios 的选择方式。

## 3. 非目标

- 不运行 stress backfill。
- 不刷新 market data 或 macro data。
- 不执行 candidate scoring、parameter search、A/B review 或 paper shadow。
- 不修改 candidate decision ledger。
- 不生成 official target weights、order ticket、broker action 或 production mutation。

## 4. Artifact Contract

Runtime root:

- `reports/etf_portfolio/dynamic_v3_rescue/stress_scenario_library/<library_run_id>/`

Artifacts:

- `stress_scenario_manifest.json`
- `stress_scenario_library.json`
- `stress_scenario_reader_brief.md`
- `stress_scenario_report.md`
- `stress_scenario_validation.json/md`

Expected output fields:

- `stress_scenario_library_id`
- `scenario_count`
- `required_scenarios_present`
- `candidate_validation_use`
- `next_validation_action`

## 5. Selection Policy

Scenario selection must be documented in
`config/etf_portfolio/dynamic_v3_rescue/stress_scenario_library_v1.yaml` with owner,
version/status, rationale, intended effect, validation evidence, and review condition.
The library is a candidate validation taxonomy, not a calibrated probability forecast or
production threshold set.

## 6. Safety Boundary

All outputs are read-only and fixed to:

- `production_effect=none`
- `manual_review_only=true`
- `stress_scenario_library_only=true`
- `candidate_validation_only=true`
- `data_downloaded_by_library=false`
- `pipelines_executed_by_library=false`
- `not_probability_forecast=true`
- `not_official_target_weights=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `production_state_mutated=false`
- `automatic_candidate_promotion=false`
- `auto_apply=false`

## 7. 验收标准

- `stress-scenario-library report` 可生成 `stress_scenario_library.json` 和
  `stress_scenario_reader_brief.md`。
- `validate-stress-scenario-library` 返回 PASS。
- Reader Brief 显示 scenario count、required scenarios、candidate validation use 和 next action。
- README、operations runbook、system flow、artifact catalog、report registry、requirements 和
  task register 同步更新。
- focused pytest、contract-validation suite、ruff、compileall、git diff check、documentation
  contract、report index 和 Reader Brief quality 通过。

## 8. 进展记录

- 2026-06-15：新增任务并进入 IN_PROGRESS；本阶段只实现 stress scenario library contract、
  report/validation artifact 和 visibility，不运行 stress backfill、不刷新数据、不修改 production state。
- 2026-06-15：实现完成并转入 VALIDATING；真实链路生成 library
  `stress-scenario-library_991459f9a1c540e2`，`scenario_count=9`，
  `required_scenarios_present=True`，
  `candidate_validation_use=standardized_dynamic_v3_candidate_stress_validation`，
  `next_validation_action=use_library_ids_in_next_stress_backfill_or_case_review`，
  validator `status=PASS` / failed=0。Reader Brief JSON 已显示 stress scenario fields；
  focused pytest 9 passed，contract-validation suite 26 passed / 20.83s，documentation
  contract PASS，report index `PASS_WITH_WARNINGS` 仅保留既有 missing/stale visibility，
  Reader Brief OK，Reader Brief quality OK。安全边界保持 read-only / no stress backfill /
  no data refresh / no upstream rerun / no official target / no broker / no production。
