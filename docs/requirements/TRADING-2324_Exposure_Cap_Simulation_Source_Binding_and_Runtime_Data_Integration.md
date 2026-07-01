# TRADING-2324 Exposure-Cap Simulation Source Binding and Runtime Data Integration

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

TRADING-2323 已生成 exposure-cap mechanics simulation 的 source-blocked readiness
package。该 package 只证明 future simulation 的 objective、input requirement、metric
contract 和 safety boundary 已定义；它没有读取 cached market data、risk-cap trigger
series、portfolio baseline、turnover / rebalance history，也没有执行数值仿真。

TRADING-2324 的目标是把 2323 从 `source_blocked_no_simulation` 推进到可审计的
`source_bound_dry_run_readiness`。本任务仍然是 research-only / observe-only，不产生
target weight、rebalance instruction、paper-shadow、production 或 broker action。

## 目标

新增 CLI：

```bash
aits research trends exposure-cap-simulation-source-binding
```

默认读取：

```text
outputs/research_trends/exposure_cap_mechanics_simulation/
outputs/research_trends/scope_narrowed_candidate_generators_regenerated/
outputs/research_trends/scope_narrowed_candidate_actual_path_validation/
data/raw/prices_daily.csv
data/raw/rates_daily.csv
config/research/exposure_cap_simulation_source_binding_policy.yaml
```

若没有真实 portfolio baseline source，允许使用 policy-governed
`synthetic_observe_only` baseline，但必须将 `portfolio_source_mode`、simulation
readiness、comparison interpretation 和 next task route 明确标为 synthetic/proxy，不得
用于 promotion、paper-shadow、production 或 broker。

## 阶段拆解

1. Source binding loader。
   - 读取 TRADING-2323 summary / readiness / policy context。
   - 读取 2291 risk-cap trigger series 和 2292 risk-cap validation state。
   - 对 market price history 调用项目既有 cached data validation code path。
   - 读取真实 portfolio baseline，或生成 policy-governed synthetic observe-only baseline。

2. Source inventory and gap matrix。
   - 输出 risk-cap trigger、market price、portfolio baseline、rebalance calendar、
     turnover assumption、cooldown policy、exposure cap policy、simulation calendar 的
     availability、coverage、hash、data quality status 和 blocking semantics。

3. Source-bound dry-run readiness。
   - 判断 dry-run 是否可以执行。
   - 真实 portfolio 缺失时只允许
     `SOURCE_BOUND_DRY_RUN_READY_WITH_SYNTHETIC_BASELINE`。
   - full simulation 保持 blocked，直到真实 runtime / portfolio / turnover source 可用。

4. Dry-run simulation。
   - 对每个 trading date / target asset 绑定 baseline exposure、risk-cap trigger、
     max allowed exposure、cooldown state、turnover proxy 和 return proxy。
   - 输出 comparison diagnostics，但所有 return / drawdown / turnover 数值均为 proxy
     diagnostics，不能作为 promotion 或 production evidence。

5. 文档和注册表。
   - 同步 `docs/system_flow.md`、`docs/artifact_catalog.md`、
     `config/report_registry.yaml` 和本任务登记。

## 安全边界

- `research_only=true`
- `observe_only=true`
- `dry_run_only=true`
- `manual_review_only=true`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `portfolio_effect=none`
- `production_effect=none`

禁止输出 target weight、rebalance instruction、buy/sell signal、broker order、
paper-shadow-ready、production-ready 或 automatic promotion route。

## Data-quality gate

本任务会读取 cached market price data，因此必须调用 `aits validate-data` 等价的
项目内 data-quality validation code path，并在 outputs / docs 中披露结果。若只完成
source inventory 而不读取 market data，才允许标记
`NOT_APPLICABLE_SOURCE_INVENTORY_ONLY`；默认实现不使用该豁免。

## 验收标准

- CLI implemented: `aits research trends exposure-cap-simulation-source-binding`。
- 只允许 `--mode source_bound_dry_run_readiness`。
- 输出 source inventory、source gap matrix、risk-cap trigger binding、market data
  binding、portfolio baseline binding、turnover / rebalance assumption、dry-run readiness、
  safety boundary 和 next task route。
- 在 source 足够时输出 dry-run result JSON/CSV、cap vs no-cap comparison、turnover
  impact 和 cooldown impact report。
- Synthetic portfolio baseline 下 readiness 必须为
  `SOURCE_BOUND_DRY_RUN_READY_WITH_SYNTHETIC_BASELINE`，next task 指向
  portfolio baseline source decision。
- 所有 outputs 固定 promotion / paper-shadow / production / broker false/none。
- 同步 registry、artifact catalog、system flow 和 task register。

## 进展记录

- 2026-07-01: 根据 owner 附件新增并进入 `IN_PROGRESS`。当前 worktree 已有两个无关
  research 文档未提交改动，本任务必须 selective staging，不能混入无关改动。实现将优先
  绑定现有 TRADING-2323 source-blocked artifacts、2291/2292 risk-cap source 和 cached
  QQQ / SPY / SMH market data；如果真实 portfolio baseline 不存在，只允许 synthetic
  observe-only dry-run readiness。
- 2026-07-01: 实现完成并进入 `VALIDATING`。真实 CLI run：
  `aits research trends exposure-cap-simulation-source-binding`，输出 status=
  `EXPOSURE_CAP_SIMULATION_SOURCE_BOUND_DRY_RUN_READY_PROMOTION_BLOCKED`，
  `data_quality_status=PASS_WITH_WARNINGS`，`data_quality_gate_executed=true`，
  `actual_requested_date_range=2023-01-06..2026-06-18`，
  `risk_cap_active_trigger_record_count=373`，`dry_run_record_count=2595`，
  `cap_binding_days=382`，`portfolio_source_mode=synthetic_observe_only`，
  `dry_run_readiness_status=SOURCE_BOUND_DRY_RUN_READY_WITH_SYNTHETIC_BASELINE`，
  `full_simulation_allowed=false`，next task=
  `TRADING-2325_Portfolio_Baseline_Source_Decision`。验证通过 2324 focused
  parallel pytest 6 passed、Ruff、compileall、full parallel pytest 3925 passed、
  `aits validate-data --as-of 2026-06-29` PASS_WITH_WARNINGS、docs freshness、
  documentation contract、contract-validation 193 passed、task-register consistency
  run/validate 和 `git diff --check`。所有 outputs 保持 promotion / paper-shadow /
  production / broker false/none，不生成 target weight、rebalance instruction 或
  broker action。
