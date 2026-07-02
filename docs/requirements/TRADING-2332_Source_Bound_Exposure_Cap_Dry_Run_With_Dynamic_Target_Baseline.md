# TRADING-2332 Source-Bound Exposure-Cap Dry-Run With Dynamic Target Baseline

最后更新：2026-07-03

## 状态

`VALIDATING`

## 背景

TRADING-2331 已完成 dynamic target baseline dry-run readiness with PIT caveat，真实 run 输出 `2332_allowed=true`，next task 指向 `TRADING-2332_Source_Bound_Exposure_Cap_Dry_Run_With_Dynamic_Target_Baseline`。本任务承接 TRADING-2330 timestamp-remediated wrapper、TRADING-2324 source binding、TRADING-2323 exposure-cap mechanics context、TRADING-2326 static ETF dry-run reference 和本地 validated market data，执行 dynamic target baseline 下的 source-bound exposure-cap dry-run。

本任务只回答 dynamic target no-cap 与 dynamic target capped by risk-cap / exposure-cap 的 research-only proxy diagnostics，不进入 paper-shadow、production 或 broker action。

## 实施范围

1. 新增 CLI `aits research trends source-bound-exposure-cap-dynamic-target-dry-run`。
2. Fail-closed 读取 TRADING-2331 readiness、TRADING-2330 timestamp wrapper、TRADING-2329 source remediation lineage、TRADING-2324 source binding、TRADING-2323 policy context 和 TRADING-2326 static dry-run reference。
3. 对 dynamic target wrapper 生成 effective-date exposure schedule，保留 `NEXT_SESSION_DECISION_POLICY`、PIT caveat、source hash、validity window 和 research-only safety fields。
4. 执行与 `aits validate-data --as-of 2026-06-29` 同源的数据质量门禁；若 market data quality FAIL，不执行完整 dry-run，并路由 data remediation。
5. 在 data quality PASS / PASS_WITH_WARNINGS 时执行 dynamic no-cap vs capped dry-run，生成 cap binding、exposure reduction、return/drawdown proxy、turnover/cooldown、false cost、missed upside、downside protection、dynamic strategy overlap 和 static-vs-dynamic comparison。
6. 输出 interpretation boundary 和 TRADING-2333 route。
7. 更新 report registry、artifact catalog、system flow 和 task register。

## 边界

- 不修改 dynamic target baseline wrapper。
- 不重新执行 timestamp remediation。
- 不修改 risk-cap trigger series、exposure-cap policy、cooldown policy 或 dynamic strategy。
- 不生成真实 target weight、rebalance instruction、buy/sell signal、paper-shadow-ready、production-ready 或 broker-ready artifact。
- 不读取真实券商账户或真实持仓。
- 不把 PIT caveat wrapper 标记为 strict PIT。

## 标签与启发式治理

本任务引入的 review labels 只用于 dry-run proxy diagnostic summary，不作为投资结论、promotion gate 或 production policy。标签判定优先使用符号关系和零阈值；需要比较重叠率时，仅使用本任务文档化的临时 review label policy，并在 outputs 中保留 `manual_review_only=true`。后续 TRADING-2333 必须复核这些标签，不能直接作为 owner final decision。

## 输出

运行目录：

`outputs/research_trends/source_bound_exposure_cap_dynamic_target_dry_run/`

主要 runtime artifacts：

- `dynamic_target_exposure_cap_dry_run_summary.json`
- `dynamic_target_baseline_source_report.json`
- `dynamic_target_baseline_exposure_schedule.json/.csv`
- `dynamic_target_risk_cap_trigger_alignment_matrix.json/.csv`
- `dynamic_target_exposure_cap_dry_run_result.json/.csv`
- `dynamic_target_cap_vs_no_cap_comparison.json/.csv`
- `dynamic_target_cap_binding_day_matrix.json/.csv`
- `dynamic_target_exposure_reduction_report.json`
- `dynamic_target_return_drawdown_proxy_report.json`
- `dynamic_target_turnover_impact_report.json`
- `dynamic_target_cooldown_impact_report.json`
- `dynamic_target_false_risk_cap_cost_report.json`
- `dynamic_target_missed_upside_cost_report.json`
- `dynamic_target_downside_protection_proxy_report.json`
- `dynamic_target_strategy_overlap_report.json`
- `dynamic_target_static_vs_dynamic_comparison.json`
- `dynamic_target_data_quality_report.json`
- `dynamic_target_pit_caveat_interpretation_boundary.json`
- `dynamic_target_2333_task_route.json`

Project-facing docs：

- `docs/research/dynamic_target_exposure_cap_dry_run_report.md`
- `docs/research/dynamic_target_cap_vs_no_cap_comparison.md`
- `docs/research/dynamic_target_strategy_overlap_report.md`
- `docs/research/dynamic_target_false_cost_downside_protection_review.md`
- `docs/research/dynamic_target_static_vs_dynamic_comparison.md`

## 验收标准

- CLI 可运行并生成上述 runtime artifacts 和 docs。
- 真实 run 执行 data-quality gate，并在 summary/report 中披露 `data_quality_gate_executed=true`、status、warning_count、error_count 和 report path。
- `dynamic_target_baseline_exposure_schedule` 保留 `target_exposure` research baseline field、`risk_asset_exposure`、`known_at_policy=NEXT_SESSION_DECISION_POLICY` 和 PIT caveat，不输出交易指令。
- cap binding day、cap binding rate、exposure reduction、return/drawdown proxy、turnover/cooldown、false cost、missed upside、downside protection、strategy overlap 和 static-vs-dynamic comparison 均可审计。
- 所有 outputs 固定 `promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。
- 若 data quality FAIL，输出 data remediation route 且不执行完整 dry-run。
- TRADING-2333 route 在 data quality PASS / PASS_WITH_WARNINGS 且 record_count > 0 时指向 diagnostics review。
- Focused tests、Ruff、compileall、data validation、docs freshness、report contract、contract-validation、task-register consistency、full parallel pytest 和 `git diff --check` 通过或在交付说明中明确阻塞原因。

## 进展记录

- 2026-07-03：新增需求文档并开始实现。当前 worktree 存在两个既有无关 research docs 改动，本任务必须 selective staging，不能混入提交。
- 2026-07-03：实现完成并进入验证。真实 run 生成 2490 条 dynamic dry-run records，data quality=`PASS_WITH_WARNINGS`、error_count=0、cap_binding_days=378、cap_binding_rate=`0.455422`、return_proxy_delta=`-0.187258`、drawdown_proxy_delta=`0.045294`、strategy_overlap_label=`RISK_CAP_BINDING_WHEN_DYNAMIC_MISSES_RISK`，route 到 `TRADING-2333_Dynamic_Exposure_Cap_vs_No_Cap_Diagnostics_Review`。
- 2026-07-03：验证通过 Ruff、compileall、focused parallel pytest 17 passed、真实 CLI run、`validate-data --as-of 2026-06-29` PASS_WITH_WARNINGS / error_count=0、docs freshness、documentation contract、task-register consistency run/validate、contract-validation 193 passed、full parallel pytest 4082 passed / 643 warnings 和 `git diff --check`。
