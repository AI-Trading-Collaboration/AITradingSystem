# TRADING-2311 Liquidity / Rates Pressure Data Feasibility Audit

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

附件路线在 AI / semiconductor leadership scope review 后进入 liquidity / rates
pressure candidate family。该方向用于研究高 duration / 科技股估值压力、利率压力和
liquidity headwind，后续可能作为 max exposure modifier / risk-cap enhancer，但当前
只允许 data feasibility audit。

本地缓存初查：

- price cache: `TLT`、`SHY`、`QQQ`、`SMH` 可用；
- price cache: `IEF`、`UUP`、`HYG`、`LQD` 缺失；
- rates cache: `DGS10`、`DGS2`、`DTWEXBGS` 可用；
- real rate / SOFR / breakeven 等宏观 series 缺失。

## 目标

新增 CLI：

```bash
aits research trends liquidity-rates-pressure-feasibility-audit
```

默认读取：

```text
data/raw/prices_daily.csv
data/raw/rates_daily.csv
```

## 输入候选

- `TLT`
- `IEF`
- `UUP` / `DXY proxy`
- `HYG` vs `LQD`
- `10Y yield proxy`
- `2Y yield proxy`
- real rate proxy if available

## 重点风险

- macro data revision / release timing；
- FRED observation date 与 market decision timestamp 的 known-at gap；
- price proxy 和真实 rates / liquidity condition 的经济含义差异；
- rates pressure 影响 horizon 通常长于短线 price signal；
- missing `IEF` / `UUP` / `HYG` / `LQD` 会阻断完整 liquidity / credit proxy。

## 产物

- `liquidity_rates_data_feasibility_summary.json`
- `rates_proxy_inventory.json`
- `rates_proxy_inventory.csv`
- `liquidity_rates_price_proxy_coverage_matrix.json`
- `liquidity_rates_price_proxy_coverage_matrix.csv`
- `macro_rates_coverage_matrix.json`
- `macro_rates_coverage_matrix.csv`
- `liquidity_pressure_candidate_design_sketch.json`
- `liquidity_rates_validation_route.json`
- `liquidity_rates_safety_boundary.json`
- `data_quality_*.md`
- `docs/research/liquidity_rates_data_feasibility_audit.md`

## 实施边界

1. 数据质量门禁。
   - CLI 必须调用与 `aits validate-data` 同源的 `validate_data_cache`。
   - Gate 只覆盖当前可用于 feasibility audit 的 required anchors：
     `QQQ`、`SMH`、`TLT`、`SHY` 和 configured rate series。
   - `IEF`、`UUP`、`HYG`、`LQD` 和 missing macro series 作为 source gap 进入
     inventory，不作为绕过 gate 的理由。
   - 若 required-anchor gate 有 error，命令 fail closed，不生成 feasibility
     artifacts。

2. Data source discipline。
   - 每个 price / macro candidate input 必须记录 provider class、local cache path、
     row count、date range、checksum、source status、PIT / known-at status 和 usage
     boundary。
   - 缺失输入必须记录 required owner / source action 和 blocked downstream task。

3. 安全边界。
   - `research_only=true`
   - `generator_implemented=false`
   - `candidate_artifact_generated=false`
   - `candidate_signal_series_generated=false`
   - `actual_path_validation_executed=false`
   - `scope_review_executed=false`
   - `promotion_allowed=false`
   - `paper_shadow_allowed=false`
   - `production_allowed=false`
   - `broker_action=none`
   - `dynamic_promotion_status=BLOCKED`

## 验收标准

- CLI implemented: `aits research trends liquidity-rates-pressure-feasibility-audit`。
- Summary 披露 selected market regime=`ai_after_chatgpt` 和实际 requested date range。
- Data quality gate 通过状态在 downstream artifacts 中可见。
- Missing `IEF` / `UUP` / `HYG` / `LQD` / real-rate series 必须显式进入 source
  gap，不得平滑成可用输入。
- 输出不得生成 candidate-bound artifacts、actual-path validation、scope review、
  promotion、paper-shadow、production 或 broker-ready 结论。

## 进展记录

- 2026-07-01: 根据 owner post-2302 roadmap 新增并进入 `IN_PROGRESS`。当前
  worktree 已有两个无关 research 文档未提交改动，本任务必须 selective staging，
  不能混入无关改动。
- 2026-07-01: 实现完成并进入 `VALIDATING`。真实 CLI run
  `aits research trends liquidity-rates-pressure-feasibility-audit --quality-as-of 2026-06-29`
  输出 status=`LIQUIDITY_RATES_FEASIBILITY_AUDIT_READY_PARTIAL_PROXY`，
  data_quality_status=`PASS_WITH_WARNINGS`，actual_requested_date_range=`2022-12-01..2026-06-29`。
  `TLT`、`SHY`、`QQQ`、`SMH` 和 `DGS10` / `DGS2` / `DTWEXBGS`
  可用；`IEF`、`UUP`、`HYG`、`LQD`、`DFII10`、`T10YIE`、`SOFR`
  记录为 source gap。`partial_poc_possible=true`，但
  `full_liquidity_pressure_poc_ready=false`，仍不得生成 generator、
  actual-path validation、scope review、promotion、paper-shadow、production
  或 broker-ready 结论。
- 2026-07-01: 验证通过 Ruff、compileall、TRADING-2311 focused parallel pytest
  7 passed、AI/docs focused parallel pytest 68 passed、`aits validate-data --as-of 2026-06-29`
  PASS_WITH_WARNINGS、contract-validation 193 passed（runtime artifact:
  `outputs/validation_runtime/contract-validation_20260701T043558Z/test_runtime_summary.json`）
  和 `git diff --check`。
