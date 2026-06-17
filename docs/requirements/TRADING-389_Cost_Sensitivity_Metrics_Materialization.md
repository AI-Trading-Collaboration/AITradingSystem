# TRADING-389 Cost Sensitivity Metrics Materialization

最后更新：2026-06-17

## 背景

TRADING-359 的 cost sensitivity review 已可运行，但真实 governance pack 中仍为
`INSUFFICIENT_COST_INPUTS`，原因是缺少 explicit numeric candidate metrics。TRADING-389
负责从既有 research artifacts materialize cost review 所需的 turnover / performance
metrics，再把生成的 candidate metrics JSON 交给既有 cost sensitivity review 重新运行。

## 范围

新增只读 materialization artifact：

- `aits etf dynamic-v3-rescue cost-sensitivity-metrics-materialization run`
- `aits etf dynamic-v3-rescue cost-sensitivity-metrics-materialization report --latest`
- `aits etf dynamic-v3-rescue validate-cost-sensitivity-metrics-materialization --latest`

默认 source 是 latest `backtest_sim_outcome` 的 `simulated_variant_summary.json`。当前
governance candidate 为 `median_plus_regime_mismatch_filter`，而 source artifact 的 numeric
variant row 为 `limited_adjustment`；因此 artifact 必须显式披露
`source_variant=limited_adjustment` 和 `candidate_to_source_mapping`，不得假装 source 中存在
同名 candidate row。

## Metric Contract

Materialization 必须尝试输出：

- `turnover`
- `gross_performance_proxy`
- `baseline_performance_proxy`
- `gross_improvement_proxy`
- `drawdown_proxy`
- `trade_rotation_count`
- source artifact id/path、outcome mode、PIT / simulation limitation

如果 required cost review inputs 无法从既有 artifact 推导，status 必须为
`INSUFFICIENT_COST_INPUTS`，并保持 cost review insufficient。不得发明 metrics，不得运行新
optimization/backtest 来补数。

## Statuses

- `COST_INPUTS_AVAILABLE`
- `COST_INPUTS_PARTIAL`
- `INSUFFICIENT_COST_INPUTS`

这些 status 只表示 cost review inputs 是否可用，不表示 candidate 可 promotion。

## Safety

该任务只写 research-only metrics materialization 和 cost review artifact。它不得刷新数据、
优化策略、写 official target weights、触发 broker/order、生成 execution model、修改 paper
account 或 production state。

## Progress

|日期|状态|记录|
|---|---|---|
|2026-06-17|IN_PROGRESS|新增需求文档，准备实现 cost sensitivity metrics materialization。|
|2026-06-17|DONE|新增 module、CLI、Reader Brief、validation、report registry、artifact catalog、operations runbook、system flow、README 和 focused tests。真实 artifact `cost-metrics-materialization_938cbce979c4f135` 从 backtest sim outcome `57c2eb4e71c3320d` 的 `source_variant=limited_adjustment` materialize turnover=`0.005945`、gross performance proxy=`0.00638`、gross improvement proxy=`0.001144`、drawdown proxy=`-0.042935`、trade/rotation count=`185`；rerun cost review `cost-sensitivity-review_c0ba1c397d10078e` validation PASS，status=`NOT_MEANINGFUL_UNDER_COSTS`，worst net improvement proxy=`0.0011291375`，因此 promotion 仍需 blocked / return to research。|
