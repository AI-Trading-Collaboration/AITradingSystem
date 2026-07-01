# TRADING-2314 Liquidity / Rates Scope Review

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

TRADING-2313 对 TRADING-2312 partial rates-only artifacts 执行 actual-path
validation，真实 run 结果为 `LIQUIDITY_RATES_VALIDATED_INCONCLUSIVE`：

- `risk_on_exposure_cap` objective 为 PASS；
- `qqq_smh_valuation_pressure` 为 INCONCLUSIVE_OR_WEAK；
- `high_duration_asset_drawdown` 为 FAIL；
- `duration_pressure_proxy_v1` candidate scorecard 为 continue-research；
- `rates_pressure_exposure_cap_modifier_v1` candidate scorecard 为 inconclusive；
- `liquidity_headwind_proxy_v1` 因 `UUP` / `HYG` / `LQD` source gap 没有
  signal series 或 validation rows。

因此 TRADING-2314 只能做 partial rates-only research scope review。当前不能把
inconclusive actual-path validation 解释为 promotion、paper-shadow、production 或
broker readiness，也不能把 `liquidity_headwind_proxy_v1` 补成 scope decision。

## 目标

新增 CLI：

```bash
aits research trends liquidity-rates-scope-review
```

判断 TRADING-2312 / TRADING-2313 证据是否适合以下用途：

- `risk_cap_modifier`
- `no_add_gate`
- `max_exposure_limiter`
- `diagnostic_only`

## 输入

默认读取：

```text
outputs/research_trends/liquidity_rates_actual_path_validation/
data/raw/prices_daily.csv
data/raw/rates_daily.csv
config/research/liquidity_rates_scope_review_policy.yaml
```

## 产物

- `liquidity_rates_scope_review_summary.json`
- `liquidity_rates_candidate_scope_matrix.json`
- `liquidity_rates_candidate_scope_matrix.csv`
- `liquidity_rates_horizon_scope_matrix.json`
- `liquidity_rates_horizon_scope_matrix.csv`
- `liquidity_rates_use_case_scope_matrix.json`
- `liquidity_rates_use_case_scope_matrix.csv`
- `liquidity_rates_recommended_scope.json`
- `liquidity_rates_scope_review_safety_boundary.json`
- `data_quality_*.md`
- `docs/research/liquidity_rates_scope_review.md`

## 实施边界

1. Source validation。
   - 必须读取 TRADING-2313 summary、prediction/outcome matrix、candidate
     scorecard、objective coverage 和 horizon coverage。
   - Source status 可以是 `LIQUIDITY_RATES_VALIDATED_CONTINUE_RESEARCH` 或
     `LIQUIDITY_RATES_VALIDATED_INCONCLUSIVE`，但 inconclusive source 不得被提升为
     scope-ready / promotion-ready。
   - `liquidity_headwind_proxy_v1` 必须保持 source-gap exclusion。

2. Data quality gate。
   - CLI 必须调用与 `aits validate-data` 同源的 `validate_data_cache`。
   - Gate 覆盖 `QQQ`、`SMH`、`TLT`、`SHY` 和 configured rate series。
   - Gate error 时 fail closed，不生成 scope-review artifacts。

3. Scope decision policy。
   - Candidate、horizon、use-case keep / diagnostic / reject / sample-blocked 规则必须
     写在 policy manifest。
   - 用途判断必须覆盖 `risk_cap_modifier`、`no_add_gate`、
     `max_exposure_limiter` 和 `diagnostic_only`。
   - 当前阈值是 `pilot_research` scope-review lens，不是 promotion gate。

4. 安全边界。
   - `research_only=true`
   - `actual_path_validation_consumed=true`
   - `scope_review_executed=true`
   - `forward_observe_started=false`
   - `owner_approval_required_before_forward_observe=true`
   - `partial_rates_only_scope_review=true`
   - `liquidity_headwind_scope_review_executed=false`
   - `promotion_allowed=false`
   - `paper_shadow_allowed=false`
   - `production_allowed=false`
   - `broker_action=none`
   - `dynamic_promotion_status=BLOCKED`

## 验收标准

- CLI implemented: `aits research trends liquidity-rates-scope-review`。
- Summary 披露 selected market regime=`ai_after_chatgpt`、actual requested date range
  和 source validation status。
- Data quality gate 通过状态在 downstream artifacts 中可见。
- Scope matrices 覆盖 generated candidates、10d / 20d / 1m horizons 和四个用途。
- `liquidity_headwind_proxy_v1` 只作为 source-blocked exclusion 出现，不生成
  keep/reject scope row。
- 输出不得启动 forward observe、promotion、paper-shadow、production 或 broker-ready 结论。

## 进展记录

- 2026-07-01: 根据 owner post-2302 roadmap 和 TRADING-2313 next task 新增并进入
  `IN_PROGRESS`。当前 worktree 已有两个无关 research 文档未提交改动，本任务必须
  selective staging，不能混入无关改动。
- 2026-07-01: 实现完成并进入 `VALIDATING`。真实 run status 为
  `LIQUIDITY_RATES_SCOPE_REVIEW_DIAGNOSTIC_ONLY`，data_quality_status 为
  `PASS_WITH_WARNINGS`，actual_requested_date_range 为 `2022-12-01..2026-06-29`，
  scope_review_result 为
  `DIAGNOSTIC_ONLY_WITH_LIMITED_RISK_CAP_RESEARCH_CANDIDATE`。
- 2026-07-01: Scope review 结论：`duration_pressure_proxy_v1` 保留 limited
  research candidate，`rates_pressure_exposure_cap_modifier_v1` 保持 diagnostic；
  `10d` keep，`20d` / `1m` diagnostic；`risk_cap_modifier`、
  `max_exposure_limiter` 和 `diagnostic_only` keep，`no_add_gate` reject。
  `liquidity_headwind_proxy_v1` 继续因 `UUP` / `HYG` / `LQD` source gap 不生成
  scope row。
- 2026-07-01: 验证通过 Ruff、compileall、TRADING-2314 focused parallel pytest
  9 passed、liquidity / AI focused parallel pytest 41 passed、registry/docs focused
  parallel pytest 27 passed、task-register/docs focused parallel pytest 10 passed、
  `aits validate-data --as-of 2026-06-29` PASS_WITH_WARNINGS、contract-validation
  193 passed（runtime artifact:
  `outputs/validation_runtime/contract-validation_20260701T054104Z/test_runtime_summary.json`）
  和 `git diff --check`。
