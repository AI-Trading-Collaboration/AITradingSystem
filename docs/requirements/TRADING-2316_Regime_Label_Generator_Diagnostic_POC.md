# TRADING-2316 Regime Label Generator Diagnostic POC

最后更新：2026-07-26

## 状态

`BASELINE_DONE_CAPABILITY_RECEIPT_MIGRATION`

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

旧 direct-DQ 路径曾直接读取：

```text
config/research/regime_label_generator_policy.yaml
config/research/regime_state_machine_design_policy.yaml
data/raw/prices_daily.csv
data/raw/rates_daily.csv
data/raw/prices_marketstack_daily.csv if available
```

DATA-GOV-002 Phase B2 迁移后，runner 不再直接读取上述 canonical cache，也不在 runner
内部重新运行全局 DQ。它必须在 evaluator 和任何输出写入前取得 exact
`TRADING-2316_REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC@1.0.0` verified capability preflight，
并只从 receipt 绑定的 `scoped_prices` bytes 读取 QQQ、SMH、SPY trailing price inputs。
Full/scoped DQ 仍由同一 `validate_data_cache` code path 在 capability build 时运行并写入
receipt；missing pointer、非 strict PASS、wrong consumer/as-of、policy/source/report/input
tamper 必须在生成 label 或输出前 fail closed。

Tracked dependency 与 capability policy：

```text
config/data_quality/regime_label_generator_dependency_v1.yaml
config/data_quality/regime_label_generator_capability_v1.yaml
consumer: TRADING-2316_REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC@1.0.0
capability: regime_label_trailing_price_inputs@1.0.0
required_price_tickers: [QQQ, SMH, SPY]
required_rate_series: []
default_start_date: 2021-02-22
```

`required_rate_series=[]` 是 evaluator transitive input closure 的结果：rates 和 optional
marketstack 只出现在旧 global DQ gate/披露中，不参与 feature 或 label 计算。Scoped price
projection 仍保留 same-code-path DQ 所需的 date/ticker/OHLCV/source 结构字段，不能缩成只含
`adj_close` 的未验证旁路。

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

Runner summary/safety 必须引用而不是复制授权语义：

- consumer/version/as-of discovery pointer；
- content-addressed capability receipt；
- full/scoped DQ report status/path；
- verified `scoped_prices` path/SHA-256/size；
- `global_cache_pass_claimed`、isolated/unisolated issue codes；
- no-reuse/no-daily/no-production/no-broker safety boundary。

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
- 输出 summary 披露 selected market regime、actual requested date range、capability receipt、
  full/scoped data quality status、policy id / version、label count、axis count 和 safety flags。
- 生成 `regime_label_series.csv`，至少覆盖 QQQ / SMH / SPY 在项目 primary window 中的
  PIT-eligible daily labels。
- 生成 `regime_label_pit_policy.json`，明确 no-future-outcome、no-hindsight-relabeling、
  known-at lag、missing-input fail-closed 和 label versioning。
- Missing/non-PASS/tampered/wrong-consumer/wrong-as-of capability preflight 必须在 evaluator
  与任何业务输出前阻断 label generation。
- Full DQ 非 PASS 时必须保持 `global_cache_pass_claimed=false`；只有 scoped exact PASS 且
  无 unisolated error 才能运行。
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
- 2026-07-26: DATA-GOV-002 B2 启动。Exact transitive input closure 审计确认 evaluator
  只使用 QQQ、SMH、SPY 的 trailing `adj_close`；`rates_daily.csv`、configured rate
  universe 与 optional marketstack 只在旧 global DQ gate/披露中出现，不进入 feature 或
  label 计算。因此 capability policy 固定 `required_rate_series=[]`，同时保留 scoped DQ
  所需的完整 price schema projection。迁移必须让 CLI/runner 在 evaluator 与任何输出前
  fail-closed 验证 dependency/discovery/receipt，并完成 legacy 语义 characterization、
  tamper/wrong-consumer/wrong-as-of tests 和真实 canonical build/verify。
- 2026-07-26: B2 实现已完成 dependency loader、sealed preflight、verified materialized-input
  re-read，以及 runner/CLI 的 capability-only 消费路径。Focused parallel regression 当前为
  `16 passed`；missing as-of、materialized bytes tamper、wrong consumer 与失败不写 output
  均有覆盖。
- 2026-07-26: 真实 canonical build/verify 通过，receipt=
  `dq_capability_b453834493d1951868c5474f379942461cce29c61b74fd37b9aab69167759ab3`，
  requested window=`2021-02-22..2026-07-24`。Full DQ=`FAIL`，唯一隔离 ERROR 为范围外
  `prices_non_market_session_date`；scoped DQ=`PASS`、unisolated errors=`[]`、
  `global_cache_pass_claimed=false`、required rates=`[]`。真实 capability-backed runner
  status=`REGIME_LABEL_GENERATOR_DIAGNOSTIC_POC_READY_SEGMENTATION_ONLY`，
  label/distribution/transition rows=`7416/30/123`，production/broker=`false/none`。
  当前进入 formal validation；TRADING-2317、daily/periodic、production 与 broker 未迁移。
- 2026-07-26: DATA-GOV-002 B2 在最新 `main@281c8236b` 完成正式收口。Focused capability/
  authority=`16/3 passed`，Black/Ruff/strict mypy PASS；architecture 最终=`670 passed`，
  contract/report/reproducibility/integration=`275/57/23/995 passed`，report/integration
  warnings=`62/642`。Parent-bound Full=`7350 passed / 3 skipped / 643 warnings`，artifact=
  `outputs/validation_runtime/full_20260726T102147Z/test_runtime_summary.json`。任务转
  `BASELINE_DONE_CAPABILITY_RECEIPT_MIGRATION`；后续 owner 可以复核 diagnostic label
  分布，但本状态不授权 TRADING-2317、策略信号、daily/periodic、production 或 broker。
