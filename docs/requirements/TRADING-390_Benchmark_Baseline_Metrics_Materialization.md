# TRADING-390 Benchmark Baseline Metrics Materialization

最后更新：2026-06-17

## 背景

TRADING-370 的 benchmark baseline control 已可运行，但真实 artifact 仍为
`INSUFFICIENT_BASELINE_METRICS`，原因是缺少 explicit candidate metrics 和
baseline metrics JSON。TRADING-389 已补齐 cost review 所需 candidate cost metrics；
TRADING-390 负责生成 benchmark control 所需的 candidate / baseline metrics，并重新运行
benchmark baseline control pack。

## 范围

新增 research-only materialization artifact：

- `aits etf dynamic-v3-rescue benchmark-baseline-metrics-materialization run`
- `aits etf dynamic-v3-rescue benchmark-baseline-metrics-materialization report --latest`
- `aits etf dynamic-v3-rescue validate-benchmark-baseline-metrics-materialization --latest`

默认 source 组合：

- latest 或指定 `backtest_sim_outcome` 的 `simulated_variant_summary.json` 和
  `simulated_outcome_windows.jsonl`
- latest 或指定 TRADING-389 `candidate_cost_metrics.json`
- latest 或指定 cost-sensitivity review
- cached price/rate data after the same `aits validate-data` data quality code path

## Metric Contract

Materialization 必须尝试输出：

- candidate `net_performance_proxy`、`gross_performance_proxy`、`turnover`
- `static_allocation`
- `no_trade`
- `qqq_only`
- `spy_only`
- `equal_weight_etf`
- 每个 baseline 的 `gross_performance_proxy`、`net_performance_proxy`、`turnover`、
  sample count、missing count、source method 和 limitation

`equal_weight_shadow_candidates` 不能被偷换成 `equal_weight_etf`；后者必须用 SPY / QQQ /
SMH / SOXX equal weight 和同一 simulation event window 重新计算。所有 price-derived
metrics 必须披露 data quality status、price source、row/window count 和
`BACKTEST_SIMULATION` event-window limitation。

## Statuses

- `BASELINE_METRICS_AVAILABLE`
- `BASELINE_METRICS_PARTIAL`
- `INSUFFICIENT_BASELINE_METRICS`

这些 status 只表示 benchmark control inputs 是否可用，不表示 candidate 可 promotion。

## Safety

该任务只生成 research-only benchmark input metrics 并重跑 existing benchmark baseline
control。它不得把 benchmark comparison 当作 live allocation signal，不得刷新数据、优化
策略、写 official target weights、触发 broker/order、生成 execution model、修改 paper
account 或 production state。

## Progress

|日期|状态|记录|
|---|---|---|
|2026-06-17|IN_PROGRESS|新增需求文档；初步确认 `backtest_sim_outcome` 可提供 candidate/no-trade event windows，QQQ/SPY/equal-weight ETF/static baseline 可从 cached prices 按同一 5D windows 生成，且必须先通过数据质量门禁。|
|2026-06-17|DONE|新增 `benchmark-baseline-metrics-materialization` run/report/validate CLI、artifact family、Reader Brief、report registry、artifact catalog、README、operations runbook、system flow 和 focused tests。真实 artifact `benchmark-baseline-metrics-materialization_a84cbc04f0eb189d` 输出 `BASELINE_METRICS_AVAILABLE`，生成 candidate metrics 与 5 个 baseline metrics，并重跑 benchmark baseline control `benchmark-baseline-control_f236d7c827b26534`。Control validation PASS，但 status=`CANDIDATE_UNDERPERFORMS_BASELINES`、outperformed=0、underperformed=5，next action=`return_candidate_to_research_until_it_outperforms_baseline_controls`；该结果不是 promotion、extended shadow、official target、broker/order 或 live approval。|
