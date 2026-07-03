# TRADING-2335 High-Intensity Risk-Cap Threshold Selection

最后更新：2026-07-04

## 状态

`VALIDATING`

## 背景

TRADING-2334 已完成 high-intensity-only risk-cap forward observe plan，真实 run 输出 `readiness_status=THRESHOLD_SELECTION_REQUIRED`，并 route 到 `TRADING-2335_High_Intensity_Risk_Cap_Threshold_Selection`。2334 已生成 threshold candidates、event schema、evidence contract、actual-path outcome contract、manual-review boundary、false warning / missed stress framework 和 stop / continue / archive rules，但尚未确定后续 event logger 应使用的 final high-intensity trigger rule。

TRADING-2335 的目标是从 TRADING-2334 输出的候选规则中做 deterministic threshold selection，生成 selected trigger rule、selected trigger contract、event logger input contract、density guardrail 和 TRADING-2336 route。本任务不证明 high-intensity risk-cap 已有效，不启动 runtime observe，不进入 paper-shadow、production 或 broker action。

## 实施范围

1. 新增 CLI `aits research trends high-intensity-risk-cap-threshold-selection`。
2. Fail-closed 读取 TRADING-2334 forward observe plan outputs、threshold candidate matrix、event schema、evidence contract、actual-path outcome contract、manual-review boundary、readiness checklist、2335 route 和 safety boundary。
3. Fail-closed 读取 TRADING-2333 diagnostics context、TRADING-2332 prior validated dynamic dry-run context、TRADING-2331 readiness / PIT caveat 和 TRADING-2330 timestamp remediation context。
4. 对 P90 / P95 / score percentile / composite rule 等候选 threshold 做 deterministic ranking，生成 candidate scoring matrix。
5. 明确 high-intensity trigger density guardrail，默认 `max_trigger_density=0.10`、`density_warning_threshold=0.08`、`density_blocking_threshold=0.12`、`max_monthly_event_count=3`、`max_consecutive_trigger_days=5`。
6. 生成 threshold selection decision matrix、selected trigger rule、selected trigger contract、threshold selection caveat report、event logger input contract、selected rule backtest / false warning / missed stress context、manual review boundary、2336 readiness checklist、2336 task route、安全边界和 research docs。
7. 更新 report registry、artifact catalog、system flow 和 task register。

## 边界

- 不启动 forward observe runtime，不生成真实 observe event。
- 不重新执行 dynamic dry-run、exposure-cap simulation 或 risk-cap trigger series generation。
- 不修改 risk-cap trigger series、exposure-cap policy、cooldown policy 或 dynamic target wrapper。
- 不使用未来收益优化 threshold。
- 不生成 target exposure、target weight、rebalance instruction、buy / sell signal、paper-shadow-ready、production-ready 或 broker-ready artifact。
- 不读取真实券商账户、真实持仓或 broker data。
- 不判断 selected high-intensity trigger rule 已经有效，不进入 owner final decision。
- Selected trigger rule 只允许作为 TRADING-2336 observe-only event logger 的 research input，不能解释为自动减仓或 exposure cap 指令。

## Data Quality Policy

TRADING-2335 只读取 prior validated TRADING-2332 / TRADING-2333 / TRADING-2334 artifacts，不直接读取 cached market data。因此本任务的 data-validation policy 为：

```text
NOT_APPLICABLE_PRIOR_VALIDATED_2332_2333_2334_ARTIFACTS_ONLY
```

最终 summary 和报告必须明确：

```text
aits validate-data not applicable because TRADING-2335 only reads prior validated research artifacts.
```

如果后续实现改为重新读取 market data，则必须先执行 `aits validate-data --as-of 2026-06-29` 或同源 data-quality gate，并在输出中记录 gate status。

## Heuristic Governance

Density guardrail、candidate scoring、selection labels 和 readiness route 是 research-only threshold-selection pilot baseline。它们不是 production risk policy、position cap、promotion gate 或 owner final approval。若后续进入 runtime observe 或 policy refinement，相关阈值必须迁移到受审配置或替换为 evidence-backed calibration。

本批默认阈值来自 owner 附件，用于避免 selected high-intensity rule 重新退化成 broad exposure-cap：

- `max_trigger_density=0.10`
- `density_warning_threshold=0.08`
- `density_blocking_threshold=0.12`
- `max_monthly_event_count=3`
- `max_consecutive_trigger_days=5`

## 验收标准

- CLI 可运行并生成所有 TRADING-2335 runtime artifacts 和 research docs。
- 缺少 required 2334 / 2333 / 2332 / PIT context artifact 时 fail closed。
- 2334 route 不是 `TRADING-2335_High_Intensity_Risk_Cap_Threshold_Selection` 时 fail closed。
- 2334 `runtime_observe_started=true` 或任何 input / output 打开 promotion、paper-shadow、production、broker action、target weight、rebalance instruction、buy / sell signal 时 fail closed。
- Threshold candidate matrix 为空时输出 blocked report 并 route 到 `TRADING-2336_High_Intensity_Risk_Cap_Threshold_Candidate_Remediation`。
- 所有候选 threshold 均不可接受时 route 到 threshold remediation 或 archive route。
- 可接受候选存在时唯一选定 selected trigger rule，并生成 selected trigger contract 和 event logger input contract。
- Summary 明确 `runtime_observe_started=false`、`promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。
- Summary 和 research docs 明确 selected rule 是 research-only forward observe event logger input，不是 production rule、paper-shadow rule、target weight 或 broker action。

## 验证计划

- `ruff check .`
- `python -m compileall -q src tests`
- TRADING-2335 focused parallel pytest files
- 真实 CLI run
- `python -m ai_trading_system.cli docs validate-freshness`
- `python -m ai_trading_system.cli docs report-contract --latest`
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-07-01`
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `python scripts/run_validation_tier.py full --write-runtime-artifact`
- `git diff --check`

## 进展记录

- 2026-07-04：根据 owner 附件新增并进入 `IN_PROGRESS`。当前 worktree 存在两个既有无关 research docs 改动：`docs/research/indicator_family_only_model_review.md`、`docs/research/layer1_selector_pause_or_continue_owner_pack.md`；本任务必须 selective staging，不得混入本次 commit。
- 2026-07-04：实现完成并进入 `VALIDATING`。新增 `high-intensity-risk-cap-threshold-selection` CLI、2334/2333/2332/2331/2330 fail-closed loader、candidate scoring matrix、density guardrail、threshold selection decision matrix、selected trigger rule、selected trigger contract、threshold caveat report、event logger input contract、selected-rule backtest / false-warning / missed-stress context、manual-review boundary、2336 readiness checklist、2336 route、安全边界、research docs、report registry、artifact catalog、system-flow 文档更新和 TRADING-2335 focused tests。真实 run status=`HIGH_INTENSITY_THRESHOLD_SELECTION_READY_WITH_WARNINGS_PROMOTION_BLOCKED`，data_quality_status=`PASS_WITH_WARNINGS`，candidate_count=`3`，selected_threshold_id=`COMPOSITE_HIGH_INTENSITY_RULE`，selected_threshold_density=`0.06747`，density_guardrail_status=`PASS_WITH_WARNINGS`，warning=`MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL`，threshold_selection_status=`THRESHOLD_SELECTED_WITH_WARNINGS_PROMOTION_BLOCKED`，readiness_status=`READY_FOR_2336_EVENT_LOGGER_WITH_CAVEAT`，next_task=`TRADING-2336_High_Intensity_Risk_Cap_Forward_Observe_Event_Logger`，runtime_observe_started=`false`。本任务未重跑 `aits validate-data`，因为只读取 prior validated TRADING-2332 / TRADING-2333 / TRADING-2334 artifacts。
- 2026-07-04：验证通过 Ruff、compileall、TRADING-2335 focused parallel pytest 22 passed、真实 CLI run、docs freshness 514 docs PASS、documentation contract 1233 reports PASS、task-register consistency run / validate PASS、contract-validation 193 passed、full parallel pytest 4145 passed / 643 warnings 和 `git diff --check`。`contract-validation` runtime artifact=`outputs/validation_runtime/contract-validation_20260703T183319Z/test_runtime_summary.json`；full runtime artifact=`outputs/validation_runtime/full_20260703T183700Z/test_runtime_summary.json`。本任务未重跑 `aits validate-data`，因为只读取 prior validated TRADING-2332 / TRADING-2333 / TRADING-2334 artifacts。
