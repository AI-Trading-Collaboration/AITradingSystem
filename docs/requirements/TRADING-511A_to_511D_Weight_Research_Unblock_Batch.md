# TRADING-511A to 511D Weight Research Unblock Batch

最后更新：2026-06-19

## 背景

TRADING-510 的 B0 static strategic baseline 已完成。当前 blocker 有效且必须保留：
不能用现有 P0 dynamic strategy 继续 B1-B6，因为该策略混合 trend、momentum、
relative strength、risk 和 regime 逻辑，会破坏每层只增加一个机制的 ablation protocol。

本批次先冻结 B1-B6 独立 runner scope、signal robustness entry contract、untouched holdout /
final gate policy，然后只实现 B1 execution-control runner。不得直接继续 TRADING-511~520 的
完整消融链。

## 范围

|任务|目标|输出|
|---|---|---|
|TRADING-511A|冻结 B1-B6 独立 runner scope|`docs/research/ablation_runner_scope_freeze.json/md`|
|TRADING-511B|定义 signal robustness entry contract 和验证 CLI|`docs/research/signal_robustness_entry_contract.json/md`、`aits etf weight-research validate-contracts`|
|TRADING-511C|冻结 untouched holdout 和 final gate policy|`docs/research/untouched_holdout_final_gate_policy.json/md`、同一验证 CLI|
|TRADING-511D|在 511A-C 验证通过后只实现 B1|`aits etf weight-research run-b1`、B1 mini-backfill/result/comparison/Reader Brief|

## Layer Scope

- B0：static strategic baseline only。
- B1：B0 + execution / no-trade / turnover control only。
- B2：B0 + fast asymmetric risk scaler only。
- B3：B0 + slow relative tilt only。
- B4：B2 + B3 combination only。
- B5：B4 + confidence shrinkage only。
- B6：B5 + regime information only。

## B1 Allowed And Forbidden Logic

Allowed mechanisms:

- deadband
- minimum benefit-over-cost threshold
- turnover budget
- max adjustment cap

Forbidden mechanisms:

- trend signal
- momentum signal
- relative strength signal
- risk scaler
- regime signal
- confidence shrinkage
- mixed dynamic allocation logic

## Safety Boundary

所有 artifacts 和 CLI 输出必须固定：

```text
research_only=true
manual_review_only=true
official_target_weights=false
paper_shadow_activation=false
broker_action_allowed=false
order_ticket_generated=false
live_trading_allowed=false
production_effect=none
owner_decision_appended=false
```

## Hard Stops

- `aits etf weight-research validate-contracts` 未 PASS 时不得运行 B1。
- B1 runner 如果需要 trend/momentum/relative strength/risk/regime/confidence 或 P0 allocator，
  必须停止并输出 blocked。
- B1 development / mini-backfill 不得读取或使用 untouched holdout。
- `aits validate-data` 或同一路径未通过时不得生成 B1 data-dependent result。
- 任何 official target weights、paper-shadow activation、broker/order/live/production mutation
  语义必须 fail closed。

## 验收标准

- 511A scope freeze 明确每层 exactly one mechanism，并列出 rejected mixed logic。
- 511B signal robustness contract 含 required inputs、required feature columns、required
  signal series、coverage threshold、stale-input behavior、schema compatibility、fail-closed
  behavior、allowed warnings 和 blocking conditions。
- 511C holdout policy 明确 development、mini-backfill、full-backfill、untouched holdout windows，
  并验证 B1-B6 development runs 不能用 holdout。
- 511D B1 runner 不调用 P0 allocator，不读取 signals/regimes/features，只用 B0 weights、
  price returns 和 execution-control policy。
- Focused tests 覆盖 contract validation、holdout early-use failure、mixed logic rejection、
  B1 result safety fields 和 B1 vs B0 comparison。

## 状态记录

- 2026-06-19：新增 unblock batch。按 owner 指示保留当前 blocker，不直接继续
  TRADING-511~520；先实现 511A-C，再尝试 511D B1-only runner。
- 2026-06-19：511A-C 已冻结并通过 `aits etf weight-research validate-contracts`；
  511D B1-only runner 已完成 `normal_market_regime` mini-backfill，状态为
  `B1_MINI_BACKFILL_COMPLETE_RESEARCH_ONLY`。B1 相对 B0 return +0.286pct，但 drawdown
  reduction 为 -0.121pct、turnover +0.1243，因此解释为 mixed research-only evidence。
  B2-B6 仍 blocked，未经 owner/system 复核不得继续。验证通过 focused pytest 64 passed、
  scoped ruff、compileall、JSON parse、`git diff --check`、CLI validate-contracts 和 CLI
  run-b1 verify。
