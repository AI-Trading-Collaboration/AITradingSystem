# TRADING-2326 Source-Bound Exposure-Cap Dry-Run With Static ETF Baseline

最后更新：2026-07-02

## Status

- task_id: `TRADING-2326_SOURCE_BOUND_EXPOSURE_CAP_DRY_RUN_STATIC_ETF_BASELINE`
- status: `VALIDATING`
- priority: `P0`
- owner: system implementation + project owner review
- last_update: 2026-07-02

## Background

TRADING-2324 已经生成 source-bound dry-run readiness，但使用的是
`synthetic_observe_only` baseline。TRADING-2325 已完成 portfolio baseline source
decision，并选择 `static_etf_allocation_baseline` 作为 TRADING-2326 第一轮
source-bound exposure-cap dry-run baseline。

TRADING-2326 的目标是在 static ETF allocation baseline 下执行真正的
source-bound dry-run simulation，生成 cap binding、exposure reduction、turnover、
cooldown、false cost、missed upside 和 downside protection proxy diagnostics。

## Scope

本任务新增：

```bash
aits research trends source-bound-exposure-cap-dry-run
```

命令读取：

- TRADING-2325 baseline decision outputs；
- TRADING-2324 source binding outputs；
- TRADING-2323 exposure-cap mechanics readiness package；
- `config/etf_portfolio` static ETF allocation config；
- cached market data。

由于本任务消费 cached market data，必须执行 `aits validate-data` 或同源
`validate_data_cache` data-quality gate，并在输出中披露 gate status。

## Non-Goals

- 不读取真实券商账户。
- 不读取真实持仓作为 baseline。
- 不生成真实 target weight。
- 不生成 rebalance instruction。
- 不生成 buy / sell signal。
- 不进入 paper-shadow。
- 不进入 production。
- 不产生 broker action。
- 不判断 exposure-cap 可实盘使用。

## Implementation Steps

1. 新增 loader，fail-closed 校验 2325 / 2324 / 2323 upstream safety fields。
2. 读取 static ETF allocation config，构造每日 baseline exposure schedule。
3. 执行 cached market data quality gate，失败时 fail closed。
4. 绑定 risk-cap trigger、baseline exposure 和 market returns。
5. 执行 static ETF baseline dry-run simulation。
6. 生成 exposure-cap vs no-cap proxy comparison。
7. 生成 turnover / cooldown / false-cost / downside-protection diagnostics。
8. 生成 interpretation boundary、data-quality report 和 TRADING-2327 route。
9. 写出 runtime JSON/CSV artifacts 和 research Markdown reports。
10. 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md` 和 `docs/task_register.md`。

## Design Decision

本轮 static ETF baseline 的 risk assets 为 `QQQ`、`SPY`、`SMH`，baseline risk
asset exposure 合计为 0.85，`CASH` 为 0.15。TRADING-2324 policy 中的 cap
解释为组合层面的总风险资产敞口上限，而不是每个 asset 独立上限；触发 cap 或
cooldown 时，QQQ / SPY / SMH 按 baseline risk weight 等比例缩放，CASH
保持 static baseline 权重。这样避免把 policy 中的 `low` / `medium` / `high`
阈值错误放大到逐资产层面，同时保持所有输出为 dry-run proxy diagnostics。

## Acceptance Criteria

- CLI `aits research trends source-bound-exposure-cap-dry-run` 可运行。
- 输出 static ETF baseline exposure schedule JSON / CSV。
- 输出 risk-cap trigger alignment matrix JSON / CSV。
- 输出 source-bound static ETF dry-run result JSON / CSV。
- 输出 exposure-cap vs no-cap static ETF comparison JSON / CSV。
- 输出 turnover、cooldown、false-cost、missed-upside、downside-protection reports。
- 输出 `exposure_cap_data_quality_report.json`。
- 输出 `exposure_cap_simulation_interpretation_boundary.json`。
- 输出 `exposure_cap_2327_task_route.json`。
- `selected_baseline=static_etf_allocation_baseline`。
- `promotion_allowed=false`。
- `paper_shadow_allowed=false`。
- `production_allowed=false`。
- `broker_action=none`。

## Validation Plan

- `ruff check .`
- `python -m compileall -q src tests`
- focused parallel pytest for all TRADING-2326 test files
- full parallel pytest through `python -m pytest -n 16 --dist loadfile tests -q`
- `aits validate-data --as-of 2026-06-29` or current equivalent
- docs freshness
- documentation contract
- contract-validation tier
- task-register consistency run / validate
- `git diff --check`

## Progress Notes

- 2026-07-02: 根据 owner 附件新增并进入 `IN_PROGRESS`。当前 worktree 存在两个既有无关 research docs 改动，本任务必须 selective staging，不得混入本次 commit。
- 2026-07-02: 实现完成并进入 `VALIDATING`。新增 `source-bound-exposure-cap-dry-run` CLI、static ETF baseline schedule、risk-cap alignment、source-bound dry-run result、cap vs no-cap comparison、turnover / cooldown / false-cost / missed-upside / downside-protection proxy reports、data-quality report、interpretation boundary、2327 route、registry / catalog / system-flow 文档更新和 TRADING-2326 focused tests。真实 run status=`SOURCE_BOUND_STATIC_ETF_EXPOSURE_CAP_DRY_RUN_READY_PROMOTION_BLOCKED`，data_quality_status=`PASS_WITH_WARNINGS`，record_count=3460，cap_binding_days=382，next_task=`TRADING-2327_Exposure_Cap_vs_No_Cap_Diagnostics_Review`。
- 2026-07-02: 验证通过：TRADING-2326 focused parallel pytest 12 passed、`python -m ruff check .`、`python -m compileall -q src tests`、`aits validate-data --as-of 2026-06-29` PASS_WITH_WARNINGS / 0 errors / 2 warnings、docs freshness 504 docs PASS、documentation contract 1224 reports PASS、task-register consistency run / validate PASS、contract-validation 193 passed、full parallel pytest 3965 passed。
