# TRADING-2312 Liquidity / Rates Pressure Generator POC

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

TRADING-2311 输出 `LIQUIDITY_RATES_FEASIBILITY_AUDIT_READY_PARTIAL_PROXY`：

- 可用 price anchors：`TLT`、`SHY`、`QQQ`、`SMH`；
- 可用 macro rates：`DGS10`、`DGS2`、`DTWEXBGS`；
- 缺失 price proxies：`IEF`、`UUP`、`HYG`、`LQD`；
- 缺失 macro / real-rate series：`DFII10`、`T10YIE`、`SOFR`；
- `partial_poc_possible=true`；
- `full_liquidity_pressure_poc_ready=false`。

因此 TRADING-2312 只能实现 partial rates-only generator POC。完整 liquidity /
credit headwind route 的 intended best solution 需要 `UUP` / DXY proxy、`HYG`、
`LQD` 和 real-rate source；当前被本地 source gap 阻断。本任务不得用
`DTWEXBGS` 单独替代完整 USD + credit liquidity proxy，也不得把 blocked
candidate 平滑成 generated candidate。

## 目标

新增 CLI：

```bash
aits research trends liquidity-rates-pressure-generator-poc
```

默认读取：

```text
outputs/research_trends/liquidity_rates_data_feasibility_audit/
data/raw/prices_daily.csv
data/raw/rates_daily.csv
config/research/liquidity_rates_pressure_generator_policy.yaml
```

## Candidate 范围

本轮生成：

- `duration_pressure_proxy_v1`
- `rates_pressure_exposure_cap_modifier_v1`

本轮阻断并显式报告：

- `liquidity_headwind_proxy_v1`

## 产物

- `liquidity_rates_pressure_generator_poc_summary.json`
- `liquidity_rates_generator_policy_summary.json`
- `liquidity_rates_generator_validation_summary.json`
- `liquidity_rates_generator_safety_boundary.json`
- `blocked_liquidity_rates_candidate_report.json`
- `<candidate_id>/candidate_signal_spec.json`
- `<candidate_id>/candidate_signal_series.csv`
- `<candidate_id>/candidate_prediction_artifact.json`
- `<candidate_id>/generation_summary.json`
- `<candidate_id>/validation_summary.json`
- `data_quality_*.md`
- `docs/research/liquidity_rates_pressure_generator_poc.md`

## 实施边界

1. 数据质量门禁。
   - CLI 必须调用与 `aits validate-data` 同源的 `validate_data_cache`。
   - Gate 覆盖 `QQQ`、`SMH`、`TLT`、`SHY` 和 configured rate series。
   - Gate error 时 fail closed，不生成 candidate-bound artifacts。

2. Policy governance。
   - Lookback、score scale、neutral band、confidence、component weights 和
     known-at penalty 必须定义在 policy manifest。
   - 本轮所有数值是 `pilot_research` 参数，不得解释为 validated effect size。

3. Candidate-bound artifact。
   - 生成的 signal spec、signal series 和 prediction artifact 必须通过现有
     `CandidateSignalBindingValidator`。
   - Signal series 必须披露 `market_regime=ai_after_chatgpt`、
     actual requested date range、data quality status、policy version 和 source
     hashes。

4. Source gap。
   - `liquidity_headwind_proxy_v1` 不生成 candidate-bound artifacts。
   - Blocked report 必须记录 blocker、缺失输入、behavioral impact、risk、
     validation coverage 和 exit condition。

5. 安全边界。
   - `research_only=true`
   - `generator_implemented=true`
   - `partial_rates_only_generator_poc=true`
   - `full_liquidity_pressure_poc_ready=false`
   - `liquidity_headwind_generator_implemented=false`
   - `actual_path_validation_ready=false`
   - `actual_path_validation_executed=false`
   - `promotion_allowed=false`
   - `paper_shadow_allowed=false`
   - `production_allowed=false`
   - `broker_action=none`
   - `dynamic_promotion_status=BLOCKED`

## 验收标准

- CLI implemented: `aits research trends liquidity-rates-pressure-generator-poc`。
- Summary 披露 selected market regime=`ai_after_chatgpt` 和实际 requested date range。
- Data quality gate 通过状态在 downstream artifacts 中可见。
- `duration_pressure_proxy_v1` 和 `rates_pressure_exposure_cap_modifier_v1`
  生成 candidate-bound artifacts 并通过 validator。
- `liquidity_headwind_proxy_v1` 进入 blocked candidate report，不生成 signal series。
- 输出不得执行 actual-path validation、scope review、promotion、paper-shadow、
  production 或 broker-ready 结论。

## 进展记录

- 2026-07-01: 根据 owner post-2302 roadmap 和 TRADING-2311
  `recommended_next_task` 新增并进入 `IN_PROGRESS`。当前 worktree 已有两个无关
  research 文档未提交改动，本任务必须 selective staging，不能混入无关改动。
- 2026-07-01: 实现完成并进入 `VALIDATING`。真实 CLI run
  `aits research trends liquidity-rates-pressure-generator-poc` 输出
  status=`LIQUIDITY_RATES_PRESSURE_GENERATOR_POC_READY_VALIDATION_BLOCKED`，
  data_quality_status=`PASS_WITH_WARNINGS`，actual_requested_date_range=`2022-12-01..2026-06-29`，
  generated_source_date_range=`2022-12-01..2026-06-26`。已生成
  `duration_pressure_proxy_v1` 和 `rates_pressure_exposure_cap_modifier_v1`
  的 candidate-bound artifacts，top-level validation status=`PASS`；
  `liquidity_headwind_proxy_v1` 仅进入 blocked candidate report，不生成 signal
  series。所有 promotion / paper-shadow / production / broker gates 仍为
  false/none。
- 2026-07-01: 验证通过 Ruff、compileall、TRADING-2312 CLI smoke、focused
  parallel pytest 76 passed、`aits validate-data --as-of 2026-06-29`
  `PASS_WITH_WARNINGS`、contract-validation 193 passed（runtime
  artifact=`outputs/validation_runtime/contract-validation_20260701T045713Z/test_runtime_summary.json`）
  和 `git diff --check`。最终边界不变：本任务只生成 partial rates-only
  generator POC，`liquidity_headwind_proxy_v1` 仍被 source gap 阻断，不进入
  actual-path validation、scope review、promotion、paper-shadow、production 或
  broker path。
