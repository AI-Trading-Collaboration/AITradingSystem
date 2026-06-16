# TRADING-370 Benchmark Baseline Control Pack

最后更新：2026-06-16

状态：DONE

## 背景

Paper-shadow candidates 需要与稳定、可复核的 benchmark baselines 比较，避免 candidate
improvement 只相对某一个弱 baseline 成立。TRADING-370 在 cost-sensitivity review 后建立
standardized benchmark baseline control pack，作为 weekly review、cost-sensitivity review 和后续
monthly / promotion-board review 的 research-only 输入。

## 目标

- 定义 static allocation、no-trade、QQQ-only、SPY-only 和 equal-weight ETF baselines。
- 为每个 baseline 输出 baseline id、asset universe、rebalancing assumption、cost assumption、
  limitations 和 applicability。
- 读取 explicit candidate/baseline metrics 时输出 candidate-vs-baseline comparison summary。
- 只读链接 latest paper-shadow weekly review 和 cost-sensitivity review。
- 输出 monthly review pack input summary、Reader Brief section 和 validation artifact。
- 新增 `benchmark-baseline-control run/report` 和 `validate-benchmark-baseline-control` CLI。
- 同步 report registry、artifact catalog、README、operations runbook、system flow、requirements、
  task register 和 focused tests。

## 非目标

- 不运行 backtest、stress replay、market data refresh 或 baseline optimizer。
- 不补造 candidate metrics、baseline metrics、weekly review 或 cost-sensitivity review。
- 不接 broker、paper broker、订单、成交回报或真实执行系统。
- 不写 official target weights、candidate ledger decision、paper account、portfolio、broker/order 或 production state。
- 不把 baseline control pack 解释为 promotion approval。

## Policy Contract

Benchmark baseline definitions 必须在
`config/etf_portfolio/dynamic_v3_rescue/benchmark_baseline_control_v1.yaml` 中披露：

- policy id / version / status / owner；
- rationale / intended effect / validation evidence / review condition；
- baseline id、type、asset universe、rebalancing assumption、cost assumption、limitations；
- comparison summary policy；
- safety boundaries。

## Artifact Contract

目录：`reports/etf_portfolio/dynamic_v3_rescue/benchmark_baseline_control/<control_id>/`

- `benchmark_baseline_manifest.json`
- `benchmark_baseline_control_pack.json`
- `benchmark_baseline_report.md`
- `reader_brief_section.md`
- `benchmark_baseline_validation.json`
- `benchmark_baseline_validation.md`

所有输出固定：

- `research_only=true`
- `manual_review_only=true`
- `benchmark_control_pack_only=true`
- `execution_model_ready=false`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `paper_account_state_mutated=false`
- `production_state_mutated=false`
- `automatic_candidate_promotion=false`
- `official_target_weights=false`
- `not_official_target_weights=true`
- `auto_apply=false`
- `production_effect=none`

## 验收标准

- CLI run/report/validate 可运行，真实当前链路能生成 control pack 并 validate PASS。
- 五类 baseline 均输出 required metadata 和 applicability。
- Explicit metrics 下输出 candidate-vs-baseline comparison summary；缺 numeric metrics 时 fail closed / disclose insufficient metrics。
- Cost-sensitivity review summary 和 weekly review summary 只读链接到 artifact。
- Reader Brief 只读 latest artifact；缺失时显示 `MISSING`，不能补造 control pack。
- Report registry、artifact catalog、README、operations runbook、system flow、requirements 和 task register 同步。
- Focused tests、CLI smoke、Ruff、compileall、documentation contract、report index、Reader Brief 和 git diff check 通过。

## 进展记录

- 2026-06-16：任务创建并进入实现；范围限定为 research-only benchmark baseline controls，不运行 backtest、不刷新数据、不补造 metrics、不接 broker、不修改 official target weights / paper account / production state。
- 2026-06-16：实现完成并转为 `DONE`。真实 artifact `benchmark-baseline-control_c301611959070dd8` 输出 `benchmark_baseline_status=INSUFFICIENT_BASELINE_METRICS`、`candidate=median_plus_regime_mismatch_filter`、policy `dynamic_v3_rescue_benchmark_baseline_control_v1 / 2026-06-16`、baseline_count=5、insufficient_metric_baseline_count=5、blocking reasons `candidate_metrics:insufficient_metrics,baseline_metrics:insufficient_metrics`、warning `cost_sensitivity_review:insufficient_cost_inputs`、next action `provide_candidate_and_baseline_metrics_before_baseline_control_review`，validation `PASS` / failed=0。当前 latest weekly/cost chain 未提供 explicit numeric candidate/baseline metrics，因此不能给出真实 candidate-vs-baseline 结论；实现选择 fail closed，未补造 metrics、刷新数据、运行 backtest 或生成执行假设。Focused tests 覆盖 all-baselines outperformed、insufficient-input fail-closed、CLI run/report/validate 和 Reader Brief latest summary；documentation contract、report index 和 Reader Brief latest 在本变更中验证。
