# TRADING-2393 Dynamic Strategy Component Attribution Targeted Ablation Retest

完成日期：2026-07-07

## 摘要

- 任务登记：`TRADING-2393_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST`
- 状态：`DONE`
- 真实 run status：`DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_READY`
- 数据质量：`PASS_WITH_WARNINGS` / errors=0（`aits validate-data --as-of 2026-07-05`）
- best reusable component：`growth_tilt_engine`
- 下一路由：`TRADING-2394_Dynamic_Strategy_Component_Ablation_Owner_Review_And_Recombination_Decision`

## 已交付

- 新增 `src/ai_trading_system/dynamic_strategy_component_attribution_targeted_ablation_retest.py`。
- 新增 CLI `aits research strategies dynamic-strategy-component-attribution-targeted-ablation-retest`。
- 输出 ablation retest result、component attribution matrix、reusable component decision 和 decision update。
- 更新 research docs、report registry、artifact catalog、system flow、task register 和 completed archive。
- 新增 focused builder / CLI / registry-doc tests。

## 关键结论

- `growth_tilt_engine=REUSABLE_COMPONENT`，是本轮最佳可复用组件。
- `lower_turnover_guardrail=USE_ONLY_AS_GUARDRAIL`，适合作为成本/换手防护，不构成 candidate-level approval。
- `guarded_turnover_transfer=OWNER_REVIEW_REQUIRED`，需要在 TRADING-2394 中由 owner 复核是否进入 recombination。
- `turnover_budgeting=CONTINUE_COMPONENT_RESEARCH`。
- `valid_until_strictness=CONTINUE_COMPONENT_RESEARCH`。
- combined turnover / valid-until 组合仍为 `CONTINUE_COMPONENT_RESEARCH`。

## 覆盖范围

- components tested：`turnover_budgeting`、`valid_until_strictness`、`growth_tilt_engine`、`lower_turnover_guardrail`、`guarded_turnover_transfer`。
- ablation candidates tested：`growth_tilt_only_reference`、`growth_tilt_plus_turnover_budget`、`growth_tilt_plus_valid_until_strict`、`growth_tilt_plus_turnover_budget_and_valid_until`、`lower_turnover_without_cooldown`、`lower_turnover_plus_growth_tilt_component`。
- primary cadence：`valid_until_window`。
- comparison cadences：`valid_until_window`、`cooldown_limited_event_driven`、`signal_event_driven`。
- monthly rebalance：只作为 legacy reference，不作为 primary decision。

## 安全边界

- `candidate_auto_accept_approved=false`
- `research_only_observation_approved=false`
- `paper_shadow_enabled=false`
- `paper_trade_created=false`
- `shadow_position_created=false`
- `event_append_enabled=false`
- `outcome_binding_enabled=false`
- `scheduler_enabled=false`
- `production_enabled=false`
- `broker_action_enabled=false`
- `daily_report_generated=false`
- `production_effect=none`
- `broker_action=none`

## 验证

- 初始实现验证通过 focused Ruff、`compileall -q src/ai_trading_system/dynamic_strategy_component_attribution_targeted_ablation_retest.py src/ai_trading_system/cli_commands/research_execution_semantics.py tests/research_strategies/test_dynamic_strategy_component_attribution_targeted_ablation_retest.py`、focused parallel pytest 3 passed、真实 `aits validate-data --as-of 2026-07-05` 和真实 2393 CLI run。
- 归档后验证通过 full Ruff、`compileall -q src tests`、focused parallel pytest 3 passed、`aits validate-data --as-of 2026-07-05` PASS_WITH_WARNINGS / errors=0、真实 CLI run、docs freshness 579 docs PASS、documentation contract 1290 reports PASS、task-register consistency run active=319 / completed=453 / failed=0、task-register consistency validate checks=5 / failed=0 / warnings=0、contract-validation 197 passed（runtime artifact=`outputs/validation_runtime/contract-validation_20260706T161550Z/test_runtime_summary.json`）和 `git diff --check`（仅 CRLF normalization warning）。
