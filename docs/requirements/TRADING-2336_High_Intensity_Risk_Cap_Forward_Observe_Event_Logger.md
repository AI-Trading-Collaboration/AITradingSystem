# TRADING-2336 High-Intensity Risk-Cap Forward Observe Event Logger

最后更新：2026-07-04

## 状态

`READY`

## 背景

TRADING-2335 已完成 high-intensity risk-cap threshold selection，并由 owner 确认 `DONE`。当前 selected rule 为 `COMPOSITE_HIGH_INTENSITY_RULE`，trigger density=`0.06747`，density guardrail=`PASS_WITH_WARNINGS`，warning=`MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL`，next task=`TRADING-2336_High_Intensity_Risk_Cap_Forward_Observe_Event_Logger`。

这表示 high-intensity observe 线已经从“候选规则计划”推进到“有确定 trigger rule，可以进入 event logger”的状态。但该 selected rule 仍未 forward validated，不能被解释为自动 exposure cap、减仓建议、paper-shadow 或 production 规则。TRADING-2336 的目标是打通未来证据收集管道，而不是证明 high-intensity risk-cap 已经有效。

## 实施范围

1. 新增 CLI `aits research trends high-intensity-risk-cap-forward-observe-event-logger`。
2. Fail-closed 读取 TRADING-2335 selected trigger rule、selected trigger contract、event logger input contract、2336 readiness checklist、2336 task route 和 safety boundary。
3. 读取 risk-cap trigger series / selected rule input，并按 `COMPOSITE_HIGH_INTENSITY_RULE` 生成 observe event。
4. `event_status` 初始为 `OBSERVE_PENDING`。
5. 建立 pending outcome registry。
6. 记录 `1d` / `5d` / `10d` / `20d` outcome requirements。
7. 对连续触发做 event cluster / de-dup / monthly concentration 标记。
8. 输出 `manual_review_observation_flag` 和 manual-review context。
9. 输出 observe-only safety boundary 和后续 actual-path evidence route。

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

TRADING-2336 如果只读取 prior validated TRADING-2335 / TRADING-2334 artifacts 和已有 risk-cap trigger series artifacts，不直接读取 cached market data，则 data-validation policy 应为：

```text
NOT_APPLICABLE_PRIOR_VALIDATED_RESEARCH_ARTIFACTS_ONLY
```

如果实现需要读取 cached market prices、macro data、outcome prices 或 trading calendar cache 来建立 pending outcome requirements，则必须先运行 `aits validate-data` 或调用同源 data-quality gate，并在 summary / report 中披露 data quality status。

## 验收标准

- CLI 可运行并生成 observe event log、pending outcome registry、outcome requirement contract、cluster / de-dup diagnostics、manual-review boundary、安全边界和后续 route。
- 缺少 required TRADING-2335 selected rule / contract / event logger input contract 时 fail closed。
- 2335 route 不是 `TRADING-2336_High_Intensity_Risk_Cap_Forward_Observe_Event_Logger` 时 fail closed。
- 每条 observe event 固定 `event_status=OBSERVE_PENDING`。
- 每条 observe event 输出 cluster 字段：`event_cluster_id`、`cluster_start_date`、`cluster_active_days`、`is_new_event`、`is_existing_cluster_continuation`、`monthly_event_count`、`consecutive_trigger_days`。
- Pending outcome registry 记录 `1d` / `5d` / `10d` / `20d` outcome requirements。
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
