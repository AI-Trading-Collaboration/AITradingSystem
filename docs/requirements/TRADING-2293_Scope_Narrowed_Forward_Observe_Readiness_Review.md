# TRADING-2293 Scope-Narrowed Forward Observe Readiness Review

最后更新：2026-06-30

## 状态

`VALIDATING`

## 背景

TRADING-2292 已完成 scope-narrowed actual-path validation。真实 run 中：

- `baseline_plus_trend_structure_scope_narrowed_confirmation_v1` 为 `SCOPE_NARROWED_VALIDATED_REJECT_RECOMMENDED`。
- `volatility_regime_scope_narrowed_risk_cap_v1` 为 `SCOPE_NARROWED_VALIDATED_FORWARD_OBSERVE_CANDIDATE`。
- `risk_appetite_refined_confidence_v1` 继续 archive carry-forward，不参与验证。

TRADING-2293 只评估 `volatility_regime_scope_narrowed_risk_cap_v1` 是否具备进入 observe-only forward evidence collection 设计的条件；不得启动 forward observe runtime，不得生成 paper-shadow、production 或 broker action。

## 目标

新增 CLI：

```bash
aits research trends scope-narrowed-forward-observe-readiness-review
```

该命令读取 TRADING-2292 validation outputs、TRADING-2291 scope-narrowed generator artifacts 和 TRADING-2290 scope review context，生成 forward observe readiness checklist、evidence collection spec、daily/weekly report contract、stop/continue rules、risk-cap trigger interpretation spec、rejected/archived carry-forward matrix 和 next-task recommendation。

所有输出必须强制：

```yaml
promotion_allowed: false
paper_shadow_allowed: false
production_allowed: false
broker_action: none
forward_observe_started: false
```

## 实施拆解

1. 输入 loader 和 fail-closed safety validation。
   - 校验 TRADING-2292 required files、2291 risk-cap artifacts 和 2290 scope rationale files。
   - 拒绝 archived/rejected candidate 被作为 readiness candidate。
   - 输入若打开 promotion、paper-shadow、production、broker 或 owner-review gate，立即失败。

2. Readiness gate。
   - 只评估 `volatility_regime_scope_narrowed_risk_cap_v1`。
   - 读取 2292 state recommendation、risk-cap scorecard、active-vs-inactive comparison、sample sufficiency、false signal cost 和 data quality status。
   - 输出 `FORWARD_OBSERVE_READY_RECOMMENDED`、`FORWARD_OBSERVE_READY_WITH_WARNINGS`、`FORWARD_OBSERVE_NOT_READY` 或 `FORWARD_OBSERVE_BLOCKED`。

3. Forward observe design artifacts。
   - 生成 evidence collection spec、daily/weekly report contract、risk-cap metric spec、trigger interpretation spec、stop/continue/extend/escalation rules 和 operational boundary。
   - 明确 risk-cap trigger 不是 buy/sell/rebalance/broker signal。

4. Carry-forward 和文档。
   - baseline confirmation 记录为 rejected current form。
   - risk_appetite 记录为 archived current form。
   - 更新 research docs、report registry、artifact catalog、system flow 和 task register。

5. 验证。
   - 新增 focused tests 覆盖 loader、gate checklist、evidence spec、stop/continue rules、trigger interpretation、carry-forward 和 CLI。
   - 完成 Ruff、compileall、focused parallel pytest、full parallel pytest、docs freshness、report contract、contract-validation、task-register consistency 和 `git diff --check`。

## 验收标准

- CLI implemented: `aits research trends scope-narrowed-forward-observe-readiness-review`。
- risk-cap candidate 被 review，baseline confirmation rejected carry-forward，risk_appetite archived carry-forward。
- 生成所有 runtime artifacts 和 4 份 research docs。
- readiness 通过时也只允许进入 TRADING-2294 observe-only evidence collection 设计，不启动 runtime。
- 所有 output safety gates closed；不得输出 paper-shadow / production / broker ready recommendation。

## 进展记录

- 2026-06-30: 根据 owner 附件新增并进入 `IN_PROGRESS`。前置隔离已确认 worktree 中仍有 TRADING-1087 / ops / docs 相关既有未提交改动，TRADING-2293 提交必须 selective staging，不能纳入无关改动。
- 2026-06-30: 实现完成并转入 `VALIDATING`。新增 CLI / loader / readiness gate checklist / evidence collection spec / daily-weekly report contract / stop-continue rules / risk-cap trigger interpretation / rejected-archived carry-forward。真实 run 结果为 readiness gate=`FORWARD_OBSERVE_READY_WITH_WARNINGS`，warnings=`DATA_QUALITY_PASS_WITH_WARNINGS` / `TRIGGER_DIRECTION_SAMPLE_SPARSE`，next task recommendation=`TRADING-2294_Evidence_Accumulation_Extension_Plan`；`forward_observe_started=false`，promotion / paper-shadow / production / broker 仍全部 false / none。验证通过 Ruff、compileall、focused parallel pytest 30 passed、full parallel pytest 3742 passed、docs freshness、report contract、contract-validation 193 passed、task-register consistency run/validate 和 `git diff --check`。
