# TRADING-2334 High-Intensity-Only Risk-Cap Forward Observe Plan

最后更新：2026-07-04

## 状态

`VALIDATING`

## 背景

TRADING-2333 已完成 dynamic exposure-cap vs no-cap diagnostics review，真实 run 推荐 `HIGH_INTENSITY_ONLY_FORWARD_OBSERVE`，并 route 到 `TRADING-2334_High_Intensity_Only_Risk_Cap_Forward_Observe_Plan`。2333 结论显示 broad exposure-cap mechanics 触发过频、return / missed upside cost 明显，dynamic baseline 下也没有显著缓解 over-binding；但 high-intensity risk-cap trigger 仍可能作为 research-only 风险预警信号继续观察。

本任务把 risk-cap 从 automatic exposure limiter 收窄为 high-intensity risk warning / forward observe / manual-review-only candidate。本任务只生成 plan、schema、contract、threshold candidates 和 TRADING-2335 route，不启动 runtime observe，不重新执行 dry-run，不进入 paper-shadow、production 或 broker action。

## 实施范围

1. 新增 CLI `aits research trends high-intensity-risk-cap-forward-observe-plan`。
2. Fail-closed 读取 TRADING-2333 diagnostics review outputs。
3. Fail-closed 读取 TRADING-2332 dynamic dry-run context、data-quality report、risk-cap alignment、strategy overlap 和 PIT caveat boundary。
4. 读取 TRADING-2331 readiness / PIT caveat、TRADING-2330 timestamp remediation 和 TRADING-2323 simulation policy context。
5. 生成 high-intensity trigger selection criteria，明确 low / medium intensity 只 record-only，high intensity 只作为 forward observe event candidate。
6. 生成 threshold candidate matrix、backtest context、forward observe event schema、evidence contract、actual-path outcome contract、manual-review boundary、false warning / missed stress framework、stop / continue / archive rules、observe readiness checklist、safety boundary 和 TRADING-2335 route。
7. 输出 runtime artifacts、research docs，并更新 report registry、artifact catalog、system flow 和 task register。

## 边界

- 不重新执行 exposure-cap dry-run 或 dynamic baseline dry-run。
- 不修改 exposure-cap policy、cooldown policy、risk-cap trigger series 或 dynamic target wrapper。
- 不生成 target exposure、target weight、rebalance instruction、buy / sell signal、paper-shadow-ready、production-ready 或 broker-ready artifact。
- 不启动 forward observe runtime，不写入真实日报 runtime signal。
- 不读取真实券商账户或真实持仓。
- 不把 `NEXT_SESSION_DECISION_POLICY` / PIT caveat wrapper 标记为 strict PIT。
- 不判断 high-intensity trigger 可实盘使用，也不进入 owner final decision。

## Data Quality Policy

TRADING-2334 只读取 prior diagnostics、prior validated TRADING-2332 dynamic dry-run artifacts、static config / registry context，不直接读取 cached market data。因此本任务的 data-validation policy 为：

```text
NOT_APPLICABLE_PRIOR_VALIDATED_2332_2333_ARTIFACTS_ONLY
```

实现必须确认 TRADING-2332 `dynamic_target_data_quality_report.json` 存在，并把其中 `data_quality_status` 带入 summary / decision outputs。如果 2332 data quality 为 `FAIL`，plan status 必须为 `BLOCKED_BY_DYNAMIC_DRY_RUN_DATA_QUALITY`，next task 必须 route 到 `TRADING-2335_Dynamic_Target_Baseline_Data_Remediation`。

## Heuristic Governance

P90、P95、composite high-intensity rule、stop / continue / archive thresholds 都是 diagnostics / observe-plan pilot labels，只能用于 research-only interpretation 和后续 TRADING-2335 threshold selection / event logger route。它们不是生产 policy、position cap、promotion gate 或 owner final approval。若后续进入 runtime observe 或 policy refinement，相关阈值必须迁移到受审配置或替换为 evidence-backed calibration。

## 验收标准

- CLI 可运行并生成所有 TRADING-2334 runtime artifacts 和 research docs。
- 缺少 required 2333 / 2332 / PIT context artifact 时 fail closed。
- 2333 route 不是 `HIGH_INTENSITY_ONLY_FORWARD_OBSERVE` / `TRADING-2334_High_Intensity_Only_Risk_Cap_Forward_Observe_Plan` 时 fail closed。
- input 或 output 打开 promotion、paper-shadow、production、broker action、target weight、rebalance instruction、buy / sell signal 时 fail closed。
- 输出 high-intensity trigger selection criteria、threshold candidate matrix、event schema、evidence contract、actual-path outcome contract、manual-review boundary、false warning / missed stress framework、stop / continue / archive rules、readiness checklist、safety boundary 和 2335 route。
- Summary 明确 `runtime_observe_started=false`、`data_quality_gate_executed=false`，并说明未重跑 `aits validate-data`，因为本任务只读取 prior validated TRADING-2332 / TRADING-2333 artifacts。
- 所有 outputs 固定 `promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。

## 验证计划

- `ruff check .`
- `python -m compileall -q src tests`
- TRADING-2334 focused parallel pytest files
- `python scripts/run_validation_tier.py full --write-runtime-artifact`
- docs freshness
- documentation contract
- contract-validation tier
- task-register consistency run / validate
- `git diff --check`

## 进展记录

- 2026-07-04：根据 owner 附件新增并进入 `IN_PROGRESS`。当前 worktree 存在两个既有无关 research docs 改动：`docs/research/indicator_family_only_model_review.md`、`docs/research/layer1_selector_pause_or_continue_owner_pack.md`；本任务必须 selective staging，不得混入本次 commit。
- 2026-07-04：实现完成并进入 `VALIDATING`。新增 `high-intensity-risk-cap-forward-observe-plan` CLI、2333/2332/2331/2330/2323 fail-closed loader、high-intensity trigger selection criteria、threshold candidate matrix、candidate backtest context、forward observe event schema、evidence contract、actual-path outcome contract、manual-review boundary、false warning / missed stress framework、stop / continue / archive rules、observe readiness checklist、2335 route、安全边界、research docs、report registry、artifact catalog、system-flow 文档更新和 TRADING-2334 focused tests。真实 run status=`HIGH_INTENSITY_FORWARD_OBSERVE_PLAN_READY_PROMOTION_BLOCKED`，data_quality_status=`PASS_WITH_WARNINGS`，cap_binding_rate=`0.455422`，threshold_candidate_count=`3`，readiness_status=`THRESHOLD_SELECTION_REQUIRED`，overall_recommendation=`HIGH_INTENSITY_ONLY_PLAN_READY_FOR_2335`，next_task=`TRADING-2335_High_Intensity_Risk_Cap_Threshold_Selection`，runtime_observe_started=`false`。
- 2026-07-04：验证通过 Ruff、compileall、TRADING-2334 focused parallel pytest 16 passed、真实 CLI run、docs freshness 513 docs PASS、documentation contract 1232 reports PASS、task-register consistency run / validate PASS、contract-validation 193 passed、full parallel pytest 4123 passed / 643 warnings 和 `git diff --check`。`contract-validation` runtime artifact=`outputs/validation_runtime/contract-validation_20260703T173910Z/test_runtime_summary.json`；full runtime artifact=`outputs/validation_runtime/full_20260703T174232Z/test_runtime_summary.json`。本任务未重跑 `aits validate-data`，因为只读取 prior validated TRADING-2332 / TRADING-2333 artifacts。
