# TRADING-2308 AI Semiconductor Leadership Generator POC

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

TRADING-2307 已完成 AI / semiconductor leadership feasibility audit，结论为
`AI_SEMICONDUCTOR_LEADERSHIP_FEASIBILITY_AUDIT_READY_PRICE_PROXY_ONLY`。可继续推进
price-proxy generator POC，但不得把 feasibility 结论解释为 validation、promotion
或 production readiness。

当前数据检查：

- `data/raw/prices_daily.csv` 覆盖 `QQQ`、`SMH`、`NVDA`、`AMD`、`TSM`、`AVGO`、`ASML`
  至 2026-06-29；
- `aits validate-data --as-of 2026-06-29` 通过，状态为 `PASS_WITH_WARNINGS`；
- `aits validate-data --full-universe --as-of 2026-06-29` 因 `ASX` 缺失 fail-closed；
- TRADING-2308 不使用 `ASX`，因此本 POC 只能声明 required-symbol price-proxy gate，
  不能声明 full-universe data readiness。

## 目标

新增 CLI：

```bash
aits research trends ai-semiconductor-leadership-generator-poc
```

生成 research-only candidate-bound artifacts：

- `candidate_signal_spec.json`
- `candidate_signal_series.csv`
- `candidate_prediction_artifact.json`
- `generation_summary.json`
- `validation_summary.json`

## Candidate IDs

- `smh_relative_strength_leadership_v1`
- `ai_semiconductor_leadership_quality_v1`
- `ai_core_basket_leadership_v1`

## 实施边界

1. 数据质量门禁。
   - CLI 必须直接调用与 `aits validate-data` 同源的 `validate_data_cache`。
   - 默认使用 latest common required-symbol price date 作为 `quality_as_of`。
   - 若 required-symbol price gate 失败，命令 fail closed，不生成 candidate-bound
     artifacts。
   - 输出必须披露 data quality status、report path、checked symbols、warnings 和
     full-universe ASX limitation。

2. Policy manifest。
   - 新增 `config/research/ai_semiconductor_leadership_generator_policy.yaml`。
   - 所有 lookback、score scale、neutral band、confidence、basket membership 和
     candidate blend weights 均必须在 policy manifest 中记录 owner、status、rationale、
     intended effect、planned validation 和 review condition。

3. Signal POC。
   - 仅使用 adjusted close relative-strength price proxies。
   - 不使用 weights / market-cap concentration、earnings、capex、event outcome 或
     non-price PIT sources。
   - `ai_core_basket_leadership_v1` 使用 policy manifest 中的 pre-registered pilot
     basket；不得从结果中倒推 basket。

4. 安全边界。
   - `promotion_allowed=false`
   - `paper_shadow_allowed=false`
   - `production_allowed=false`
   - `broker_action=none`
   - `actual_path_validation_executed=false`
   - `actual_path_validation_ready=false`

## 验收标准

- CLI implemented: `aits research trends ai-semiconductor-leadership-generator-poc`。
- 三个 candidate id 均生成 candidate-bound POC bundle。
- `validation_summary.status=PASS` 且复用 candidate-bound validator。
- Summary 披露 selected market regime=`ai_after_chatgpt` 和实际 requested date range。
- Data quality gate 通过状态在 downstream artifacts 中可见。
- 不生成 actual-path validation、scope review、promotion、paper-shadow、production 或
  broker-ready 结论。

## 进展记录

- 2026-07-01: 根据 owner post-2302 roadmap 和 TRADING-2307 validation route 新增并进入
  `IN_PROGRESS`。当前 worktree 已有两个无关 research 文档未提交改动，本任务必须
  selective staging，不能混入无关改动。
- 2026-07-01: 实现 `aits research trends ai-semiconductor-leadership-generator-poc`
  并转入 `VALIDATING`。真实 run status=
  `AI_SEMICONDUCTOR_LEADERSHIP_GENERATOR_POC_READY_VALIDATION_BLOCKED`，
  candidate_count=3，validation_status=`PASS`，data_quality_status=`PASS_WITH_WARNINGS`，
  actual_requested_date_range=`2022-12-01..2026-06-29`；三组 candidate-bound POC
  bundle 均已生成。Full-universe validation 仍因 `ASX` 缺失阻断，本任务未声明
  full-universe readiness，所有 promotion / paper-shadow / production / broker
  字段继续 false/none。
- 2026-07-01: 验证完成。通过 Ruff、compileall、focused parallel pytest 45 passed、
  TRADING-2308 focused parallel pytest 7 passed、`aits validate-data --as-of 2026-06-29`
  `PASS_WITH_WARNINGS`（错误 0、警告 2、信息 12）、contract-validation 193 passed
  （runtime artifact=
  `outputs/validation_runtime/contract-validation_20260701T033144Z/test_runtime_summary.json`）
  和 `git diff --check`。
