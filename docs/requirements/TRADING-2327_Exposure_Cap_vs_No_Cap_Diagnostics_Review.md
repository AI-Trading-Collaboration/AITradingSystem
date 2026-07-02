# TRADING-2327 Exposure-Cap vs No-Cap Diagnostics Review

最后更新：2026-07-02

## Status

- task_id: `TRADING-2327_EXPOSURE_CAP_VS_NO_CAP_DIAGNOSTICS_REVIEW`
- status: `VALIDATING`
- priority: `P0`
- owner: system implementation + project owner review
- last_update: 2026-07-02

## Background

TRADING-2326 已经在 `static_etf_allocation_baseline` 下完成 source-bound
exposure-cap dry-run。真实 run 输出 `record_count=3460`、`cap_binding_days=382`、
`data_quality_status=PASS_WITH_WARNINGS`，并保持 promotion、paper-shadow、
production 和 broker action 全部关闭。

TRADING-2327 的目标不是重新执行 simulation，而是系统化解读 2326 输出，判断
当前 exposure-cap mechanics 在 static baseline proxy 证据下是否值得继续研究，
以及下一步应该进入 forward observe joint review、policy refinement、dynamic target
baseline preparation、data remediation 或 archive route。

## Scope

新增命令：

```bash
aits research trends exposure-cap-vs-no-cap-diagnostics-review \
  --dry-run-dir outputs/research_trends/source_bound_exposure_cap_dry_run_static_etf_baseline \
  --source-binding-dir outputs/research_trends/exposure_cap_simulation_source_binding \
  --baseline-decision-dir outputs/research_trends/portfolio_baseline_source_decision \
  --simulation-policy-dir outputs/research_trends/exposure_cap_mechanics_simulation \
  --output-dir outputs/research_trends/exposure_cap_vs_no_cap_diagnostics_review \
  --mode diagnostics_review
```

命令读取并校验：

- TRADING-2326 static ETF baseline dry-run outputs；
- TRADING-2326 data-quality report 和 interpretation boundary；
- TRADING-2325 baseline source decision；
- TRADING-2324 source binding context；
- TRADING-2323 exposure-cap mechanics policy/readiness context。

## Non-Goals

- 不重新执行 exposure-cap simulation。
- 不修改 exposure-cap policy、cooldown policy 或 static ETF baseline。
- 不读取真实账户或真实持仓。
- 不生成 target weight、rebalance instruction、buy / sell signal。
- 不进入 paper-shadow、production 或 broker action。
- 不给出 owner final approval。
- 不改变 TRADING-2326 dry-run artifacts。

## Data Quality Policy

TRADING-2327 只读取 TRADING-2326 已生成的 dry-run artifacts，不重新消费 cached
market data。因此本任务的 data-validation policy 为：

```text
NOT_APPLICABLE_PRIOR_VALIDATED_DRY_RUN_ARTIFACTS_ONLY
```

实现必须确认 2326 `exposure_cap_data_quality_report.json` 存在，并把其中的
`data_quality_status` 带入全部 summary / decision outputs。如果该状态为 `FAIL`，
2327 仍可生成 blocked diagnostics，但 `diagnostics_status` 必须为
`DATA_QUALITY_BLOCKED`，下一步路由必须指向
`TRADING-2328_Static_Baseline_Data_Remediation`。

## Heuristic Governance

本任务引入的 cap-binding frequency、exposure-reduction materiality、return /
drawdown tradeoff、turnover / cooldown、false-cost / missed-upside 和 downside
protection labels 都属于 TRADING-2327 diagnostics pilot baseline。它们只用于
static baseline proxy review，不得解释为长期投资策略 policy、position cap 或
promotion threshold。后续若进入 policy refinement 或 dynamic baseline validation，
这些标签应迁移到受审配置或替换为有证据支持的 calibration。

## Implementation Steps

1. 新增 loader，fail-closed 校验 2326 / 2325 / 2324 / 2323 required artifacts 和 safety fields。
2. 校验 2326 interpretation boundary 必须包含 `research_only=true` 和 `dry_run_only=true`。
3. 生成 cap-binding diagnostics matrix。
4. 生成 exposure-reduction diagnostics matrix。
5. 生成 return / drawdown proxy diagnostics。
6. 生成 turnover / cooldown diagnostics。
7. 生成 false-cost / missed-upside diagnostics。
8. 生成 downside-protection diagnostics。
9. 生成 cap-binding period attribution。
10. 生成 policy sensitivity recommendation、dynamic baseline readiness recommendation、decision matrix 和 2328 route。
11. 写出 runtime JSON/CSV artifacts 和 research Markdown reports。
12. 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md` 和 `docs/task_register.md`。

## Acceptance Criteria

- CLI `aits research trends exposure-cap-vs-no-cap-diagnostics-review` 可运行。
- 缺少 required 2326 / 2325 / 2324 / 2323 artifact 时 fail closed。
- input 或 output 打开 promotion、paper-shadow、production、broker action、target weight、rebalance instruction、buy / sell signal 时 fail closed。
- 输出 `exposure_cap_diagnostics_review_summary.json`。
- 输出 cap-binding、exposure-reduction、return / drawdown、turnover / cooldown、false-cost / missed-upside、downside-protection diagnostics JSON / CSV。
- 输出 cap-binding period attribution JSON / CSV。
- 输出 policy sensitivity recommendation、dynamic baseline readiness recommendation、decision matrix、2328 task route 和 diagnostics interpretation boundary。
- 生成 5 份 research docs。
- 所有 outputs 固定 `promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。

## Validation Plan

- `ruff check .`
- `python -m compileall -q src tests`
- focused parallel pytest for all TRADING-2327 tests
- full parallel pytest through `python scripts/run_validation_tier.py full --write-runtime-artifact`
- docs freshness
- documentation contract
- contract-validation tier
- task-register consistency run / validate
- `git diff --check`

`aits validate-data` 不在 2327 中重跑；最终报告必须说明本任务只读取 prior validated
TRADING-2326 dry-run artifacts，并确认 2326 data-quality report 已存在。

## Progress Notes

- 2026-07-02: 根据 owner 附件新增并进入 `IN_PROGRESS`。当前 worktree 存在两个既有无关 research docs 改动，本任务必须 selective staging，不得混入本次 commit。
- 2026-07-02: 实现完成并进入 `VALIDATING`。新增 `exposure-cap-vs-no-cap-diagnostics-review` CLI、2326/2325/2324/2323 fail-closed loader、cap-binding / exposure-reduction / return-drawdown / turnover-cooldown / false-cost-missed-upside / downside-protection diagnostics、cap binding period attribution、policy sensitivity recommendation、dynamic baseline readiness recommendation、decision matrix、2328 route、diagnostics boundary、registry / catalog / system-flow 文档更新和 TRADING-2327 focused tests。真实 run status=`EXPOSURE_CAP_DIAGNOSTICS_REVIEW_READY_PROMOTION_BLOCKED`，data_quality_status=`PASS_WITH_WARNINGS`，cap_binding_rate=`0.441618`，return_proxy_delta=`-0.174103`，drawdown_proxy_delta=`0.06548`，overall_recommendation=`MOVE_TO_DYNAMIC_TARGET_BASELINE_PREPARATION`，next_task=`TRADING-2328_Dynamic_Target_Baseline_Preparation`。
- 2026-07-02: 验证通过：TRADING-2327 focused parallel pytest 21 passed、`python -m ruff check .`、`python -m compileall -q src tests`、docs freshness 505 docs PASS、documentation contract 1225 reports PASS、task-register consistency run / validate PASS、contract-validation 193 passed、full validation 3986 passed。TRADING-2327 没有重跑 `aits validate-data`，因为本任务只读取 prior validated TRADING-2326 dry-run artifacts；已确认 2326 `exposure_cap_data_quality_report.json` 存在并带入 `PASS_WITH_WARNINGS`。
