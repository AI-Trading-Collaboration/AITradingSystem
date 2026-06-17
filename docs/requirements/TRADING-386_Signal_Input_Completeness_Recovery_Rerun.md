# TRADING-386 Signal Input Completeness Recovery Rerun

最后更新：2026-06-17

## 背景

TRADING-385 已通过 canonical feature / signal / snapshot path 恢复核心 signal inputs，并输出 `SIGNAL_INPUTS_RESTORED_WITH_WARNINGS`。本任务在恢复后重新运行 signal input completeness monitor，把是否仍 blocking、warning 和可进入 readiness/health recovery 的状态固化为独立 artifact。

## 范围

- 重新运行或读取 TRADING-371 signal input completeness monitor。
- 显式验证 required files、schema versions、non-empty signal series、required feature columns、market coverage 和 as-of consistency。
- 输出 `SIGNAL_INPUTS_RESTORED`、`SIGNAL_INPUTS_RESTORED_WITH_WARNINGS` 或 `SIGNAL_INPUTS_STILL_BLOCKED`。
- 写入 blocker list、warning list、hard stop flag、Reader Brief 和 validation artifact。
- 不运行 feature/signal/snapshot builders、不刷新数据、不补造 signal artifact、不放宽 completeness policy。

## Acceptance Criteria

- Recovery rerun artifact 可由 `run/report/validate` CLI 生成和读取。
- Restored inputs 输出 non-blocking status；warning 状态必须可见且不被解释为 approval。
- Missing feature matrix 或 signal series 输出 `SIGNAL_INPUTS_STILL_BLOCKED`，并设置 `hard_stop_triggered=true`。
- Report registry、artifact catalog、system flow、runbook、README、task register 和 focused tests 同步。
- Focused pytest、Ruff、compileall、documentation contract、Reader Brief quality 和 diff check 通过。

## Progress

- 2026-06-17: 新增 `src/ai_trading_system/etf_portfolio/dynamic_v3_signal_input_completeness_recovery.py`、CLI、report registry、Reader Brief section、requirements、system flow、runbook、artifact catalog、README 和 focused tests。真实 rerun `signal-input-completeness-recovery_bb5f861bba0db5da` 使用 2026-06-17 as-of 重新运行 completeness monitor `signal-input-completeness_227a681c2f632a6d`，输出 `SIGNAL_INPUTS_RESTORED_WITH_WARNINGS`、blocking=0、warning=1；warning 来源为 latest signal snapshot `LIMITED`。该 warning 继续阻止把恢复解释为 promotion 或 extended-shadow approval。
