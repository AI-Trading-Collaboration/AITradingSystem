# TRADING-385 Signal Input Root Cause And Restore

最后更新：2026-06-17

## 背景

`outputs/reports/TRADING-384_owner_review_2026-06-17.md` 确认当前 governance hold 是正确的 fail-closed 状态。主阻断包括 `signal_input_completeness=BLOCKING`、`paper_shadow_health=BLOCKED_SIGNAL_INPUTS` 和 `readiness_reports=BLOCKED_STALE_DATA`。本任务只处理最前置的 signal input root cause 和恢复审计；promotion、extended shadow、official target weights、broker/order 和 production mutation 仍禁止。

## 范围

- 定位 `etf_feature_matrix`、`etf_signal_series`、`daily_feature_records` 和 `latest_signal_snapshot` 的 canonical generation path。
- 通过 intended pipeline 恢复 ETF feature matrix、ETF signal series 和 latest signal snapshot visibility。
- 新增 recovery root-cause report，用 previous blocking monitor 和 current restored monitor 解释 root cause、restored artifact ids、remaining warnings 和 hard stop status。
- Recovery report 只读记录结果，不运行上游、不刷新数据、不补造 artifact、不放宽 completeness policy。

## Canonical Recovery Order

1. `aits validate-data --as-of 2026-06-17`
2. `aits etf data validate`
3. `aits etf features build --end latest`
4. `aits signals build-snapshot --latest`
5. `aits etf signals generate --date latest`
6. `aits reports signal-snapshot --latest`
7. `aits etf dynamic-v3-rescue signal-input-completeness run --as-of 2026-06-17`
8. `aits etf dynamic-v3-rescue signal-input-recovery run --as-of 2026-06-17 --restored-monitor-id <restored_monitor_id> --previous-monitor-id <blocking_monitor_id>`

## Acceptance Criteria

- Restored `etf_feature_matrix` artifact id and `etf_signal_series` artifact id are visible when available.
- Root-cause report outputs `SIGNAL_INPUTS_RESTORED`, `SIGNAL_INPUTS_RESTORED_WITH_WARNINGS`, or `SIGNAL_INPUTS_STILL_BLOCKED`.
- Missing feature matrix or signal series remains fail-closed and sets `hard_stop_triggered=true`.
- Reader Brief section exposes recovery status, root cause, blockers, warnings and next action.
- Report registry, artifact catalog, system flow, runbook, README and task register are synchronized.
- Focused tests, ruff, compileall, documentation contract, Reader Brief quality and diff check pass or record any residual limitation.

## Implementation Notes

新增 `src/ai_trading_system/etf_portfolio/dynamic_v3_signal_input_recovery.py` 和 CLI：

- `aits etf dynamic-v3-rescue signal-input-recovery run`
- `aits etf dynamic-v3-rescue signal-input-recovery report`
- `aits etf dynamic-v3-rescue validate-signal-input-recovery`

Recovery report reads existing signal input completeness artifacts and writes under `reports/etf_portfolio/dynamic_v3_rescue/signal_input_recovery/<recovery_id>/`. It does not execute the feature, signal or snapshot builders.

## Progress

- 2026-06-17: 新增并完成实现。Canonical recovery path 已恢复 `data/etf_portfolio/features.csv`、`data/etf_portfolio/signals.csv` 和 latest signal snapshot report visibility。Restored signal input completeness monitor 为 `signal-input-completeness_a0aacd1aac693cc0`，状态 `WARNING`、blocking=0、warning=1；warning 来自 latest signal snapshot status `LIMITED`，不是 missing/stale feature or signal file。Previous blocking monitor 为 `signal-input-completeness_2fe124e7367a3282`，阻断来自 stale `etf_feature_matrix`、`etf_signal_series` 和 `latest_signal_snapshot`。Recovery report 保持 no signal fabrication / no upstream execution by report / production_effect=none。
