# TRADING-046A: Shadow Monitor State Normalization

最后更新：2026-05-28

## 背景

TRADING-046 的真实 latest run 已经显示：

- `coverage_gate_passed=true`
- `monitoring_sample_count=5817`
- `monitoring_days_elapsed=852`
- `monitoring_days_remaining=0`
- `rollback_recommended=false`
- `warning_count=0`

但顶层 `monitor_status` 仍为 `INSUFFICIENT_MONITORING_SAMPLE`。这说明状态机把 monitor maturity
过度绑定到 rolling metric availability，导致已满足覆盖、样本和观察期要求的 observe-only lane 仍被解释为样本不足。

## 目标

规范 SEC PIT shadow monitor 状态迁移，让 `capex_intensity` observe-only lane 能区分：

- coverage gate 是否通过；
- minimum monitoring evidence 是否达到；
- rolling metrics 是否可用；
- 当前是仍在观察中、已达到监控成熟、warning 还是 rollback review。

本任务不新增因子、不修改 observe weight、不修改 production weights、不修改 active shadow weights、不触发交易动作。

## 状态语义

- `MONITORING_ACTIVE`：coverage gate 通过，但 minimum sample / observation-day evidence 仍在积累。
- `OK_MONITORING`：coverage gate 通过，minimum sample / observation-day evidence 已达到，且无 warning / rollback。
- `WARNING`：coverage gate 通过，达到 minimum evidence 后出现 warning event。
- `ROLLBACK_RECOMMENDED`：coverage gate 通过，达到 minimum evidence，rolling metrics 可用，且 factor deterioration 已按 RankIC + outcome 双重条件确认。
- `FAILED_VALIDATION`：输入 artifact 或 coverage gate 不满足 monitor 可信运行条件。
- `INSUFFICIENT_MONITORING_SAMPLE`：保留为兼容历史状态，不作为 coverage gate 已通过后的正常状态输出。

## 状态规则

若同时满足：

- `coverage_gate_passed=true`
- `rollback_recommended=false`
- `warning_count=0`
- `monitoring_days_remaining=0`
- `monitoring_sample_count >= min_monitoring_sample_count`

则 `monitor_status=OK_MONITORING`，即使部分 rolling metric 暂时不可用，也不得继续输出
`INSUFFICIENT_MONITORING_SAMPLE`。

若 coverage gate 通过但 minimum evidence 未达到，则 `monitor_status=MONITORING_ACTIVE`。

Rollback recommendation 仍必须同时满足：

- `coverage_gate_passed=true`
- `monitoring_sample_count >= min_monitoring_sample_count`
- `monitoring_days_remaining=0`
- rollback 所需 rolling metrics 可用；
- factor deterioration 由至少一个 RankIC rollback breach 和至少一个 outcome rollback breach 双重确认。

## 输出字段

Summary JSON / Markdown 和 dashboard 需新增或展示：

- `monitor_maturity`
- `rolling_metrics_available`
- `state_transition_reason`

`production_effect` 固定为 `none`，`manual_review_required` 固定为 `true`。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|1. 任务登记和需求文档|DONE|任务表链接本文，本文记录状态语义、rollback gate 和验收标准。|
|2. 状态机归一化|DONE|核心 monitor 拆分 maturity 与 rolling metric availability；真实 latest run 从旧样本不足口径转为 `OK_MONITORING`。|
|3. Dashboard 展示|DONE|只读卡片展示 monitor maturity、rolling metrics availability 和 state transition reason。|
|4. 测试与文档|DONE|专项测试覆盖状态迁移、rollback gate、dashboard 只读字段；runbook/system flow/artifact catalog 同步。|

## 测试计划

- `tests/trading_engine/test_sec_pit_shadow_monitor.py`
- `tests/test_daily_task_dashboard.py`

覆盖：

- `INSUFFICIENT_MONITORING_SAMPLE` 旧口径场景迁移到 `MONITORING_ACTIVE`；
- `MONITORING_ACTIVE` 在 minimum evidence 达到后迁移到 `OK_MONITORING`；
- healthy coverage 且无 warning 时不得 rollback；
- rolling metrics 不可用时不得 rollback；
- factor deterioration 经 coverage、minimum evidence、rolling metrics 和双重确认后才 rollback；
- dashboard 只读展示 maturity / availability / reason；
- deterministic output 和 no production config writes。

## 进展记录

- 2026-05-28：新增并进入 `IN_PROGRESS`。原因：TRADING-046 latest run 的 coverage、sample、days 和 warning/rollback
  均健康，但状态仍停留在 `INSUFFICIENT_MONITORING_SAMPLE`，需要修正状态解释口径，避免把 rolling metric availability
  误解释为 monitor sample insufficiency。
- 2026-05-28：从 `IN_PROGRESS` 改为 `DONE`。原因：已完成状态机归一化、summary schema 1.1、
  dashboard 只读展示、runbook、artifact catalog、learning path、system flow 和专项测试；真实 latest run
  已输出 `OK_MONITORING`，并明确披露 `rolling_metrics_available=false`，rollback 仍被阻断。

## 验证记录

- `python -m pytest tests/trading_engine/test_sec_pit_shadow_monitor.py -q`：9 passed。
- `python -m pytest tests/test_daily_task_dashboard.py -q`：21 passed。
- `python -m pytest -q`：1326 passed, 1 warning。
- `python -m ruff check src tests docs config scripts`：passed。
- `python -m black --check src\ai_trading_system\fundamentals\sec_pit_shadow_monitor.py src\ai_trading_system\daily_task_dashboard.py tests\trading_engine\test_sec_pit_shadow_monitor.py`：passed。
- `python -m ai_trading_system.cli sec-pit shadow-monitor --latest`：passed；
  latest monitor `monitor_status=OK_MONITORING`、`monitor_maturity=MINIMUM_EVIDENCE_ACHIEVED`、
  `rolling_metrics_available=false`、`monitoring_sample_count=5817`、`monitoring_days_remaining=0`、
  `warning_count=0`、`rollback_recommended=false`。
