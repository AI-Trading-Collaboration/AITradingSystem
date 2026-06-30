# TRADING-2292 Scope-Narrowed Candidate Actual-Path Validation

最后更新：2026-06-30

## 状态

`VALIDATING`

## 背景

TRADING-2291 已生成两个 scope-narrowed candidate-bound artifact families：

- `baseline_plus_trend_structure_scope_narrowed_confirmation_v1`：usage=`confirmation_only`，active records=`3,667`，inactive records=`27,761`。
- `volatility_regime_scope_narrowed_risk_cap_v1`：usage=`risk_cap_only`，active records=`373`，inactive records=`31,991`。

`risk_appetite_refined_confidence_v1` current form 已归档，TRADING-2292 必须 carry forward archive status，不得重新纳入验证。

## 目标

新增 CLI：

```bash
aits research trends scope-narrowed-candidate-actual-path-validation
```

该命令读取 TRADING-2291 artifacts、TRADING-2290 scope review context 和 TRADING-2289 refined validation context，只对 `scope_active=true` records 执行主 actual-path validation，并为 inactive records 生成 reference-only comparison。输出必须强制保持：

```yaml
promotion_allowed: false
paper_shadow_allowed: false
production_allowed: false
broker_action: none
```

## 实施拆解

1. 输入 loader 和 fail-closed safety validation。
   - 校验 TRADING-2291 required top-level、candidate-level 和 risk appetite archive files。
   - 校验 TRADING-2290 scope review outputs 和 TRADING-2289 refined validation outputs 存在且 safety fields closed。
   - 复用 candidate-bound validator，并检查 scope-narrowed required lineage fields。
   - 若 archived candidate 被 include，或任何 input 打开 promotion / paper-shadow / production / broker gate，立即失败。

2. Actual-path 计算。
   - 复用 canonical cached market data validation code path 和 existing actual-path price matrix / calculator。
   - 只把 `scope_active=true` rows 纳入主 active actual-path matrix。
   - inactive rows 只生成 reference matrix，不作为 promotion 或 forward observe evidence。

3. Usage-specific validation。
   - `confirmation_only`：验证 trend confirmation / weakening 是否相对 inactive reference 有边际价值。
   - `risk_cap_only`：验证 downside / drawdown / stress / volatility risk capture 是否相对 inactive reference 有边际价值。
   - 风险阈值作为 research-only pilot baseline，代码中必须命名并说明不构成 promotion gate。

4. 输出和文档。
   - 生成要求的 JSON / CSV runtime artifacts。
   - 新增 5 份 research docs，并更新 report registry、artifact catalog、system flow 和 task register。
   - 如存在 `config/report_registry.yaml`，同步登记非 promotion artifact。

5. 验证。
   - 新增 focused tests 覆盖 loader、active path、confirmation-only、risk-cap-only、active-vs-inactive、sample sufficiency、state recommendation 和 CLI。
   - 完成 Ruff、compileall、focused pytest、full pytest、docs freshness、report contract、contract-validation、task-register consistency 和 `git diff --check`。

## 验收标准

- CLI implemented: `aits research trends scope-narrowed-candidate-actual-path-validation`。
- `baseline_plus_trend_structure_scope_narrowed_confirmation_v1` 按 `confirmation_only` 验证。
- `volatility_regime_scope_narrowed_risk_cap_v1` 按 `risk_cap_only` 验证。
- `risk_appetite_refined_confidence_v1` archive carry-forward，不参与验证。
- Active actual-path matrix、active prediction outcome matrix、inactive reference matrix、active-vs-inactive comparison、confirmation scorecard、risk-cap scorecard、sample sufficiency、false signal cost、state recommendation、error attribution seed 和 data quality report 均生成。
- 所有 output safety gates closed；不得输出 `PROMOTION_READY`、`PAPER_SHADOW_READY`、`PRODUCTION_READY` 或 `BROKER_READY`。
- 不改变 TRADING-2281 permanently inconclusive、TRADING-2285 original inconclusive、TRADING-2289 refined state decision 或 TRADING-2291 scope narrowing decision。

## 进展记录

- 2026-06-30: 根据 owner 附件新增并进入 `IN_PROGRESS`。开始实现 scope-narrowed active actual-path validation，前置隔离已识别 worktree 中既有 TRADING-1087 / ops / docs 相关未提交改动，TRADING-2292 提交必须 selective staging，不能纳入无关改动。
- 2026-06-30: 实现完成并转入 `VALIDATING`。新增 CLI / loader / active actual-path matrix / inactive reference / confirmation-only scorecard / risk-cap-only scorecard / sample sufficiency / state recommendation / risk appetite archive carry-forward。真实 run 输出 active records=`4,040`、eligible active=`3,108`、source data quality=`PASS_WITH_WARNINGS`；confirmation candidate 状态为 `SCOPE_NARROWED_VALIDATED_REJECT_RECOMMENDED`，risk-cap candidate 状态为 `SCOPE_NARROWED_VALIDATED_FORWARD_OBSERVE_CANDIDATE`，next task recommendation=`TRADING-2293_Scope_Narrowed_Forward_Observe_Readiness_Review`；promotion / paper-shadow / production / broker 仍全部 false / none。验证通过 Ruff、compileall、focused parallel pytest 35 passed、full parallel pytest 3712 passed、docs freshness、report contract、contract-validation 193 passed、task-register consistency run/validate 和 `git diff --check`。
