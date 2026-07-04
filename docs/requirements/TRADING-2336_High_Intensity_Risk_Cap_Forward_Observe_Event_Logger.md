# TRADING-2336 High-Intensity Risk-Cap Forward Observe Event Logger

最后更新：2026-07-04

## 状态

`DONE`

## 背景

TRADING-2335 已完成 high-intensity risk-cap threshold selection，并由 owner 确认 `DONE`。当前 selected rule 为 `COMPOSITE_HIGH_INTENSITY_RULE`，trigger density=`0.06747`，density guardrail=`PASS_WITH_WARNINGS`，warning=`MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL`，next task=`TRADING-2336_High_Intensity_Risk_Cap_Forward_Observe_Event_Logger`。

这表示 high-intensity observe 线已经从“候选规则计划”推进到“有确定 trigger rule，可以进入 event logger”的状态。但该 selected rule 仍未 forward validated，不能被解释为自动 exposure cap、减仓建议、paper-shadow 或 production 规则。TRADING-2336 的目标是打通未来证据收集管道，而不是证明 high-intensity risk-cap 已经有效。

## 实施范围

1. 新增 CLI `aits research trends high-intensity-risk-cap-forward-observe-event-logger`。
2. Fail-closed 读取 TRADING-2335 selected trigger rule、selected trigger contract、event logger input contract、2336 readiness checklist、2336 task route 和 safety boundary。
3. 读取 risk-cap trigger series / selected rule input，并按 `COMPOSITE_HIGH_INTENSITY_RULE` 生成 observe event。
4. 生成未去重 trigger-day log。
5. 对连续触发做 event cluster / de-dup / monthly concentration 标记。
6. 生成去重后的 observe event log，`event_status` 初始为 `OBSERVE_PENDING`。
7. 建立 pending outcome registry。
8. 记录 `1d` / `5d` / `10d` / `20d` outcome collection schedule。
9. 输出 `manual_review_observation_flag` 和 manual-review event queue。
10. 生成 logger data quality report、interpretation boundary、2337 readiness checklist、2337 task route 和 observe-only safety boundary。

## 核心事件簇字段

因为 TRADING-2335 已经出现 `MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL`，TRADING-2336 必须把事件簇处理作为核心验收点，至少输出：

```text
event_cluster_id
cluster_start_date
cluster_active_days
is_new_event
is_existing_cluster_continuation
monthly_event_count
consecutive_trigger_days
```

这些字段用于防止后续 actual-path review 把同一风险 episode 的连续触发当成独立样本，从而污染 false warning、downside capture、missed stress 和 missed upside 统计。

## 边界

- 不证明 selected rule 有效。
- 不进入 owner final decision。
- 不输出 target exposure、target weight、rebalance instruction、buy / sell signal、paper-shadow-ready、production-ready 或 broker-ready artifact。
- 不读取真实券商账户或真实持仓。
- 不启动 paper-shadow 或 production。
- 不产生 broker action。
- 不把 `runtime_observe_allowed_for_2336=true` 解释为 promotion、paper-shadow 或 production approval。

## Data Quality Policy

TRADING-2336 默认只读取 prior validated TRADING-2332 / TRADING-2334 / TRADING-2335 artifacts，不直接读取 cached market data，也不计算或绑定 future outcome。因此默认 data-validation policy 为：

```text
NOT_APPLICABLE_PRIOR_VALIDATED_RESEARCH_ARTIFACTS_ONLY_NO_OUTCOME_BINDING
```

最终报告必须说明：

```text
aits validate-data not applicable because TRADING-2336 only reads prior validated research artifacts and does not consume market data or bind future outcomes directly.
```

如果实现需要读取 cached market prices、macro data、outcome prices 或 runtime signal source，则必须先运行 `aits validate-data --as-of 2026-06-29` 或调用同源 data-quality gate，并在 summary / report 中披露 data quality status。

## 验收标准

- CLI 可运行并生成 observe event log、pending outcome registry、outcome requirement contract、cluster / de-dup diagnostics、manual-review boundary、安全边界和后续 route。
- 缺少 required TRADING-2335 selected rule / contract / event logger input contract 时 fail closed。
- 2335 route 不是 `TRADING-2336_High_Intensity_Risk_Cap_Forward_Observe_Event_Logger` 时 fail closed。
- TRADING-2336 不绑定 outcome，不读取未来收益，不根据 outcome 修改 event。
- 每条 observe event 固定 `event_status=OBSERVE_PENDING`。
- 每条 observe event 输出 cluster 字段：`event_cluster_id`、`cluster_start_date`、`cluster_active_days`、`is_new_event`、`is_existing_cluster_continuation`、`monthly_event_count`、`consecutive_trigger_days`。
- Pending outcome registry 记录 `1d` / `5d` / `10d` / `20d` outcome requirements。
- 2337 route 在正常有事件且只有 warnings 时指向 `TRADING-2337_High_Intensity_Risk_Cap_Actual_Path_Outcome_Binder` with caveat。
- 所有 outputs 固定 `promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。
- 不输出 target weight、rebalance instruction、buy / sell signal、paper-shadow-ready、production-ready 或 broker action。

## 验证计划

- `ruff check .`
- `python -m compileall -q src tests`
- TRADING-2336 focused parallel pytest files
- 真实 CLI run
- docs freshness
- documentation contract
- task-register consistency run / validate
- contract-validation tier
- full validation tier if implementation touches shared report / registry / CLI surface
- `git diff --check`

## 进展记录

- 2026-07-04：owner 确认 TRADING-2335 DONE 后新增为 `READY`。核心要求是 event logger 必须处理 cluster / de-dup / monthly concentration，避免同一风险 episode 连续触发污染后续 actual-path review 样本量。
- 2026-07-04：根据 owner 附件进入 `IN_PROGRESS`。本批实现范围扩大到完整 TRADING-2336 artifact contract：selected rule execution report、trigger-day log、de-duplicated observe event log、cluster registry、monthly concentration report、pending outcome registry、outcome collection schedule、manual-review event queue、logger data quality report、interpretation boundary、2337 readiness / route 和 safety boundary；仍不得绑定 outcome 或进入 paper-shadow / production / broker。
- 2026-07-04：实现完成并进入 `VALIDATING`。新增 `aits research trends high-intensity-risk-cap-forward-observe-event-logger`，真实 run status=`HIGH_INTENSITY_EVENT_LOGGER_READY_WITH_WARNINGS_PROMOTION_BLOCKED`，data_quality_status=`PASS_WITH_WARNINGS`，selected_rule_id=`COMPOSITE_HIGH_INTENSITY_RULE`，trigger_source_record_count=`2490`，trigger_day_count=`168`，trigger_day_density=`0.06747`，event_count_after_dedup=`60`，event_density_after_dedup=`0.024096`，cluster_count=`60`，monthly_concentration_warning=`MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL`，readiness_status=`READY_FOR_2337_OUTCOME_BINDER_WITH_WARNINGS`，next_task=`TRADING-2337_High_Intensity_Risk_Cap_Actual_Path_Outcome_Binder`；`aits validate-data` 不适用，因为本任务只读取 prior validated research artifacts 且不绑定 outcome。
- 2026-07-04：验证期间 full tier 初次暴露旧 B2 Research Campaign compute path 使用 wall-clock `generated_at.date()` 作为 data-quality `as_of`，在本地 cache 最新价格为 2026-06-29、FRED rates 为 2026-06-26 而当前日期为 2026-07-04 时被 `rates_stale` fail closed。该 blocker 不是 TRADING-2336 event logger 逻辑问题；同批直接修复为 TRADING-2338，B2 targeted/control/full diagnostic builders 默认使用 latest price cache date 作为 cache-effective data-quality `as_of`，并在 payload 中披露 `as_of` / `as_of_basis`，不放宽任何 data-quality rule。
- 2026-07-04：验证通过并归档为 `DONE`。验证包括 Ruff、compileall、TRADING-2336 focused parallel pytest 16 passed、真实 2336 CLI run、B2 research campaign focused parallel pytest 20 passed、docs freshness、documentation contract、task-register consistency run / validate、contract-validation 193 passed（runtime artifact=`outputs/validation_runtime/contract-validation_20260704T050956Z/test_runtime_summary.json`）、full parallel pytest 4161 passed / 643 warnings（runtime artifact=`outputs/validation_runtime/full_20260704T051224Z/test_runtime_summary.json`）和 `git diff --check`。最终 2336 输出保持 observe-only，`event_status=OBSERVE_PENDING`，event de-dup 后 60 个 events、60 个 clusters，monthly concentration warning 可见，route 到 `TRADING-2337_High_Intensity_Risk_Cap_Actual_Path_Outcome_Binder`，所有 promotion / paper-shadow / production / broker gates 仍关闭。
