# TRADING-2316 Regime Label Generator Diagnostic POC

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

TRADING-2315 已完成 diagnostic-only regime state machine design audit，并明确
TRADING-2316 才允许在 PIT / known-at guardrails 和 cached data quality gate 下生成
regime label series。

本任务生成的 label 只用于后续 validation segmentation / candidate interpretation。
它不是 direct strategy signal，不得进入 scoring、daily report recommendation、
portfolio weight、paper-shadow、production 或 broker path。

## 目标

新增 CLI：

```bash
aits research trends regime-label-generator-diagnostic-poc
```

生成 PIT-safe / PIT-approx regime labels，覆盖 TRADING-2315 taxonomy：

- `uptrend`
- `late_uptrend`
- `drawdown`
- `panic`
- `rebound`
- `failed_rebound`
- `range_bound`
- `high_volatility`
- `low_volatility`

## 输入

默认读取：

```text
config/research/regime_label_generator_policy.yaml
config/research/regime_state_machine_design_policy.yaml
data/raw/prices_daily.csv
data/raw/rates_daily.csv
data/raw/prices_marketstack_daily.csv if available
```

读取 cached market / macro data 前必须调用 `validate_data_cache` 同一路径，等价于
`aits validate-data` 的质量门禁，并在失败时 fail closed。

默认 requested date range 使用 AI regime：

```text
selected_market_regime: ai_after_chatgpt
anchor_event: ChatGPT public launch
anchor_date: 2022-11-30
default_start_date: 2022-12-01
```

## 产物

- `regime_label_series.csv`
- `regime_label_generation_summary.json`
- `regime_label_pit_policy.json`
- `regime_label_distribution_matrix.json`
- `regime_label_distribution_matrix.csv`
- `regime_label_transition_matrix.json`
- `regime_label_transition_matrix.csv`
- `regime_label_generation_safety_boundary.json`
- `docs/research/regime_label_generator_diagnostic_poc.md`
- `data_quality_YYYY-MM-DD.md`

## 实施边界

1. PIT / known-at policy。
   - Label 只可使用当前日期收盘后已知的 trailing price features。
   - 不允许 future return、future drawdown、future volatility、final peak/trough 或完整
     episode outcome 参与 label。
   - 每条 label row 必须带 `label_version`、`known_at_policy`、`feature_lag` 和
     `pit_policy_status`。

2. Label axis。
   - `primary_trend_regime` 输出 trend / drawdown / rebound 类主状态。
   - `volatility_overlay` 输出 `high_volatility` / `low_volatility`；中性波动状态允许为
     `normal_volatility`，但不得替代 taxonomy 中的 9 个 owner labels。

3. Heuristic governance。
   - 所有 lookback、threshold、precedence、minimum history 和 sample floor 必须定义在
     `config/research/regime_label_generator_policy.yaml`，包含 owner、status、rationale、
     intended effect、validation evidence 或 planned validation、review / expiry condition。
   - 代码不得新增未解释的投资解释阈值。

4. 安全边界。
   - `diagnostic_only=true`
   - `candidate_signal_generated=false`
   - `candidate_artifact_generated=false`
   - `actual_path_validation_executed=false`
   - `segmentation_only=true`
   - `direct_strategy_signal_allowed=false`
   - `promotion_allowed=false`
   - `paper_shadow_allowed=false`
   - `production_allowed=false`
   - `broker_action=none`

## 验收标准

- CLI implemented: `aits research trends regime-label-generator-diagnostic-poc`。
- 输出 summary 披露 selected market regime、actual requested date range、data quality
  status、policy id / version、label count、axis count 和 safety flags。
- 生成 `regime_label_series.csv`，至少覆盖 QQQ / SMH / SPY 在 AI regime window 中的
  PIT-eligible daily labels。
- 生成 `regime_label_pit_policy.json`，明确 no-future-outcome、no-hindsight-relabeling、
  known-at lag、missing-input fail-closed 和 label versioning。
- Data quality gate failure 必须阻断 label generation。
- 输出不得生成 candidate signal、actual-path validation、promotion、paper-shadow、
  production 或 broker-ready 结论。

## 进展记录

- 2026-07-01: 根据 owner post-2302 roadmap 和 TRADING-2315 next task 新增并进入
  `IN_PROGRESS`。当前 worktree 有两个无关 research 文档未提交改动，本任务必须
  selective staging，不能混入无关改动。
- 2026-07-01: 实现完成并进入 `VALIDATING`。新增
  `regime-label-generator-diagnostic-poc` CLI、policy-governed thresholds、PIT policy、
  label series、distribution matrix、transition matrix、safety boundary、report registry、
  artifact catalog、system flow 和 focused tests。
- 2026-07-01: 真实 run status 为
  `REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC_READY_SEGMENTATION_ONLY`，
  data_quality_status 为 `PASS_WITH_WARNINGS`，actual_requested_date_range 为
  `2022-12-01..2026-06-29`，label_row_count=5370，distribution_row_count=30，
  transition_row_count=119。Observed primary labels 覆盖 `uptrend`、`late_uptrend`、
  `drawdown`、`panic`、`rebound`、`failed_rebound`、`range_bound`；volatility overlay
  覆盖 `high_volatility`、`low_volatility` 和 neutral `normal_volatility`。
- 2026-07-01: 验证通过 Ruff、compileall、TRADING-2316 focused parallel pytest
  6 passed、相邻 2315/2316 focused parallel pytest 13 passed、docs/registry/task-register
  focused parallel pytest 40 passed、`aits validate-data --as-of 2026-06-29`
  PASS_WITH_WARNINGS、真实 2316 CLI run 和 contract-validation 193 passed（runtime
  artifact:
  `outputs/validation_runtime/contract-validation_20260701T061853Z/test_runtime_summary.json`）。
