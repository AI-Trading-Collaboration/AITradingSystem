# TRADING-2313 Liquidity / Rates Actual-Path Validation

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

TRADING-2312 已生成 research-only partial rates-only generator POC：

- `duration_pressure_proxy_v1`
- `rates_pressure_exposure_cap_modifier_v1`

TRADING-2312 没有生成 `liquidity_headwind_proxy_v1`，因为 TRADING-2311 已确认
`UUP` / DXY proxy、`HYG`、`LQD` 和 real-rate inputs 存在 source gap。TRADING-2313
只能验证已经生成且 schema-valid 的 partial rates-only candidate-bound artifacts，不得
把 blocked liquidity / credit route 补成 actual-path evidence。

## 目标

新增 CLI：

```bash
aits research trends liquidity-rates-actual-path-validation
```

验证 owner roadmap 指定场景：

- QQQ / SMH valuation pressure；
- high duration asset drawdown；
- risk-on exposure cap；
- 10d / 20d / 1m horizon。

## 输入

默认读取：

```text
outputs/research_trends/liquidity_rates_pressure_generator_poc/
data/raw/prices_daily.csv
data/raw/rates_daily.csv
config/research/liquidity_rates_actual_path_validation_policy.yaml
```

## 产物

- `liquidity_rates_actual_path_validation_summary.json`
- `liquidity_rates_actual_path_matrix.json`
- `liquidity_rates_actual_path_matrix.csv`
- `liquidity_rates_prediction_outcome_matrix.json`
- `liquidity_rates_prediction_outcome_matrix.csv`
- `liquidity_rates_candidate_scorecard.json`
- `liquidity_rates_objective_coverage_matrix.json`
- `liquidity_rates_state_recommendation_matrix.json`
- `liquidity_rates_actual_path_safety_boundary.json`
- `data_quality_*.md`
- `docs/research/liquidity_rates_actual_path_validation.md`

## 实施边界

1. Data quality gate。
   - CLI 必须调用与 `aits validate-data` 同源的 `validate_data_cache`。
   - Gate 覆盖 `QQQ`、`SMH`、`TLT`、`SHY` 和 configured rate series。
   - Gate error 时 fail closed，不生成 actual-path validation artifacts。

2. Source validation。
   - 必须读取 TRADING-2312 summary 和 candidate-bound artifacts。
   - 上游 status 必须为
     `LIQUIDITY_RATES_PRESSURE_GENERATOR_POC_READY_VALIDATION_BLOCKED`。
   - `liquidity_headwind_proxy_v1` 必须保持 blocked report only。
   - 若 source artifacts、validator schema 或 safety fields 不匹配，fail closed。

3. Policy governance。
   - 样本下限、return / drawdown objective 阈值、alignment scoring 和 family
     status rule 必须在 policy manifest 中定义。
   - 本轮阈值是 `pilot_research` validation lens，不是 promotion gate。

4. 安全边界。
   - `research_only=true`
   - `actual_path_validation_executed=true`
   - `scope_review_ready=false`
   - `partial_rates_only_validation=true`
   - `liquidity_headwind_validation_executed=false`
   - `full_liquidity_pressure_validation_ready=false`
   - `promotion_allowed=false`
   - `paper_shadow_allowed=false`
   - `production_allowed=false`
   - `broker_action=none`
   - `dynamic_promotion_status=BLOCKED`

## 验收标准

- CLI implemented: `aits research trends liquidity-rates-actual-path-validation`。
- Summary 披露 selected market regime=`ai_after_chatgpt` 和实际 requested date range。
- Data quality gate 通过状态在 downstream artifacts 中可见。
- 只验证 `duration_pressure_proxy_v1` 和
  `rates_pressure_exposure_cap_modifier_v1`。
- `liquidity_headwind_proxy_v1` 只作为 source-blocked exclusion 出现在 summary /
  safety boundary，不生成 validation rows。
- 输出不得执行 scope review、promotion、paper-shadow、production 或 broker-ready 结论。

## 进展记录

- 2026-07-01: 根据 owner post-2302 roadmap 和 TRADING-2312
  `next_required_task` 新增并进入 `IN_PROGRESS`。当前 worktree 已有两个无关
  research 文档未提交改动，本任务必须 selective staging，不能混入无关改动。
- 2026-07-01: 实现完成并进入 `VALIDATING`。真实 CLI run
  `aits research trends liquidity-rates-actual-path-validation` 输出
  status=`LIQUIDITY_RATES_VALIDATED_INCONCLUSIVE`，
  data_quality_status=`PASS_WITH_WARNINGS`，actual_requested_date_range=`2022-12-01..2026-06-29`，
  actual_path_record_count=31968，validation_eligible_record_count=31248。Candidate
  scorecard 中 `duration_pressure_proxy_v1` 为 continue-research，
  `rates_pressure_exposure_cap_modifier_v1` 为 inconclusive；objective coverage 中
  `risk_on_exposure_cap` PASS、`qqq_smh_valuation_pressure`
  INCONCLUSIVE_OR_WEAK、`high_duration_asset_drawdown` FAIL。`liquidity_headwind_proxy_v1`
  未生成 validation rows；所有 scope review / promotion / paper-shadow /
  production / broker gates 仍为 false/none。
- 2026-07-01: 验证通过 Ruff、compileall、TRADING-2313 focused parallel pytest
  9 passed、liquidity / AI / docs focused parallel pytest 85 passed、
  `aits validate-data --as-of 2026-06-29` PASS_WITH_WARNINGS、contract-validation
  193 passed（runtime artifact=`outputs/validation_runtime/contract-validation_20260701T051830Z/test_runtime_summary.json`）
  和 `git diff --check`。最终边界不变：当前结论为 partial rates-only actual-path
  validation inconclusive，不是 scope review ready、promotion evidence、
  paper-shadow、production 或 broker readiness。
