# TRADING-061 Weight Search Stability Constraints

最后更新：2026-06-09

## 背景

TRADING-060 已把 TRADING-059 的 latest failure 收敛为：

```text
status=TURNOVER_FAILURE_EXPLAINED
root_cause_category=weight_search_too_aggressive
top_failure_reason=cost_drag_too_high
most_common_guardrail_failure=turnover_guardrail_failed
failed_by_turnover=240
production_effect=none
auto_promotion=false
production/current.yaml unchanged
```

这说明 restricted weight tuning 没有找到候选，不是因为系统无法运行，而是当前权重搜索空间过激，导致候选被 turnover / cost drag guardrail 拒绝。

## 目标

- 新增 `weight-tuning-v0.2-stability` 配置，使候选权重更接近 baseline。
- 限制单个 signal weight delta、总 L1 distance、trend / sector 合计权重、macro liquidity 下限和 fallback signal 固定值。
- 增加 turnover-aware candidate prefilter，在 backtest 前剔除明显高换手风险候选。
- 在 objective 中纳入 turnover penalty 和 cost drag penalty。
- 生成 stability diagnostics、stable candidate artifact、summary JSON/Markdown 和可选 shadow-only YAML。
- 接入 CLI、报告 alias、Dashboard、Reader Brief 和 shadow-backtest supporting artifact。

## 非目标和安全边界

- 不降低 turnover guardrail。
- 不修改 cost model。
- 不放开 fallback signals。
- 不修改 `config/parameters/production/current.yaml`。
- 不生成 production weights。
- 不自动 promotion。
- 不绕过 walk-forward。
- 不使用 mock 数据替代真实 cached inputs。
- fallback signals 可以被固定，但固定值必须对齐当前 production baseline；固定规则的目的
  是禁止 fallback 自由调参，不应把 neutral fallback 人为降到远离 baseline 的权重。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|1. 配置与候选诊断|DONE|新增 `config/parameters/weight_tuning_v0_2_stability.yaml`；候选输出 L1、max single delta、trend/sector combined、turnover prefilter 和 rejection reasons。|
|2. 稳定搜索执行|DONE|`aits parameters tune-weights-stable --latest` 生成 `artifacts/weight_stability/YYYY-MM-DD/weight_stability_summary.json/md` 与 `stable_weight_candidates.json`；2026-06-08 latest 已进入 candidate backtest，`candidates_backtested=166`、`candidates_passed_guardrails=17`，recommended candidate 为 shadow-only `watch`。|
|3. 报告与展示|DONE|`aits parameters validate-weight-stability --latest` 和 `aits reports weight-stability --latest` 可读取；Dashboard/Reader Brief 展示 Weight Search Stability。|
|4. Shadow backtest 引用|DONE|`aits parameters shadow-backtest --latest --dry-run` 的 supporting artifacts 引用 `weight_stability_summary.json`，promotion 仍 rejected。|
|5. 验证|DONE|专项 pytest、全量 pytest、ruff、compileall、diff check 通过；production config 未修改。|

## 输出字段

`weight_stability_summary.json` 必须包含：

- `metadata.status`
- `metadata.production_effect=none`
- `metadata.manual_review_required=true`
- `metadata.auto_promotion=false`
- `input_context.previous_failure_root_cause`
- `search_summary.candidates_generated`
- `search_summary.candidates_rejected_by_stability`
- `search_summary.candidates_rejected_by_turnover_prefilter`
- `search_summary.candidates_backtested`
- `search_summary.candidates_passed_guardrails`
- `recommended_candidate.status`
- `comparison_to_trading_059`
- `promotion_impact.can_support_candidate_promotion=false`
- `safety.production_config_modified=false`
- `safety.turnover_guardrail_modified=false`
- `safety.cost_model_modified=false`
- `safety.fallback_signals_free_tuned=false`

## 进展记录

- 2026-05-31：新增任务并进入 `IN_PROGRESS`。需求来自 owner 提供的 TRADING-061 规格，目标是在不降低 guardrail、不修改 production、不放开 fallback signals 的前提下收敛 TRADING-059 的 aggressive search。
- 2026-05-31：从 `IN_PROGRESS` 改为 `VALIDATING`。已完成 stable profile、engine diagnostics、CLI/report alias、Dashboard、Reader Brief、shadow backtest supporting artifact 和测试；真实 `aits parameters tune-weights-stable --latest` 生成 2026-05-28 artifact，但 `status=INSUFFICIENT_DATA`、`candidates_backtested=0`，原因是 upstream freshness / signal snapshot readiness 未满足，未把该结果解释为候选质量失败。`validate-weight-stability`、`reports weight-stability`、shadow backtest dry-run、全量 pytest、ruff、compileall 和 diff check 均通过。
- 2026-06-09：继续保持 `VALIDATING`，原因：当前真实 `aits parameters tune-weights-stable
  --latest` 已生成 2026-06-08 artifact，`status=LIMITED`、
  `recommended_status=no_candidate`、`candidates_generated=360`、
  `candidates_rejected_by_stability=193`、`candidates_backtested=0`。输入 readiness 已从
  2026-05-29 的多项 blocker 收窄为 `status=BLOCKED`、`can_run=false`、
  `reason=signal_snapshot_date_mismatch`，其中 freshness 为 OK、price coverage 为 OK、
  backtest manifest 为 LIMITED，但 signal snapshot 仍 DATE_MISMATCH；因此 stable tuning
  仍未进入有效 candidate backtest，不能归档。验证通过 `validate-weight-stability --latest`、
  `validate-weight-stability-readiness --latest`、`reports weight-stability --latest`、
  `reports weight-stability-readiness --latest`、shadow backtest dry-run 和 focused pytest
  20 passed；production 参数、turnover guardrail、cost model 和 fallback signal policy 未修改。
- 2026-06-09：从 `VALIDATING` 改回 `IN_PROGRESS`。按 readiness recovery plan 运行
  `aits signals build-snapshot --latest` 后生成 2026-06-08 signal snapshot，`validate-snapshot`
  和 `reports signal-snapshot --latest` 均可读取；`diagnose-weight-stability-inputs --latest`
  已恢复为 `status=LIMITED_READY`、`can_run=true`、`blocking_checks=none`。随后
  `tune-weights-stable --latest` 仍显示 `candidates_backtested=0`，新的原因不是 input
  readiness，而是 v0.2 配置把 `earnings_quality` / `event_risk` 固定到 `0.05 / 0.05`，
  相对 production baseline `0.15 / 0.10` 已产生 0.15 L1 偏离；归一化后最接近 baseline
  的候选 L1 至少为 0.30，必然超过 `max_total_l1_distance_from_baseline=0.25` 和
  turnover prefilter `0.25`。修复方向是把固定 fallback 权重对齐当前 production baseline，
  保持 fallback 不自由调参、实际 turnover guardrail 和 cost model 不变。
- 2026-06-09：从 `IN_PROGRESS` 改为 `DONE`。`config/parameters/weight_tuning_v0_2_stability.yaml`
  将固定 fallback 权重改为 production baseline：`earnings_quality=0.15`、
  `event_risk=0.10`；测试新增 baseline-feasible candidate 覆盖。真实
  `aits parameters tune-weights-stable --latest` 生成 2026-06-08 artifact：
  `status=LIMITED`、`candidate_status=watch`、`candidates_generated=357`、
  `candidates_backtested=166`、`candidates_passed_guardrails=17`、
  `candidates_rejected_by_stability=31`；recommended candidate `wts-0017` 的
  `stability_status=PASS`、`turnover_prefilter_status=PASS`，并生成
  `recommended_stable_shadow_weights.yaml`。readiness 为 `LIMITED_READY`、
  `can_run=true`、`blocking_checks=none`；promotion 仍为
  `can_support_candidate_promotion=false`，原因是 signal quality 仍 `LIMITED` 且需要人工复核。
  production 参数、actual turnover guardrail、cost model、fallback free tuning、broker/trading
  action 均未修改。
