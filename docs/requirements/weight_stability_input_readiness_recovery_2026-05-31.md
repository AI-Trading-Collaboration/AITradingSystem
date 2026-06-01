# TRADING-061A Stable Weight Tuning Input Readiness Recovery

最后更新：2026-05-31

## 背景

TRADING-061 的工程实现已经完成并推送，但最新真实运行结果为：

- `status=INSUFFICIENT_DATA`
- `candidates_backtested=0`
- 阻塞来自 upstream freshness / signal snapshot / backtest input readiness

这不能解释为“稳定约束后仍找不到候选”。稳定权重搜索尚未进入有效 candidate backtest。

当前复跑链路显示：

- `aits data freshness --latest`: `MISSING`，`tracking_date=2026-05-29`，`effective_data_date=2026-05-28`，`tracking_readiness=cannot_track`
- `aits data recover-freshness --latest`: 执行完成，但 `after.freshness_status=MISSING`
- `aits signals build-snapshot --latest`: `LIMITED`，`snapshot_date=2026-05-29`，`real_signals=2`，`fallback_signals=3`
- `aits data diagnose-backtest-inputs --latest`: `FAILED`，`can_run_shadow_backtest=false`
- 主要 blockers：历史覆盖只暴露 `2026-05-29` 单日；`GOOGL` / `BRK.B` / `SGOV` 缺失比例过高

## 目标

新增 stable weight tuning input readiness 诊断与恢复建议链路，使 `tune-weights-stable` 在上游输入不足时输出明确 blocker，而不是笼统 `INSUFFICIENT_DATA`。

本任务要求新增：

- `aits parameters diagnose-weight-stability-inputs --latest`
- `aits parameters diagnose-weight-stability-inputs --date YYYY-MM-DD`
- `aits parameters diagnose-weight-stability-inputs --config config/parameters/weight_tuning_v0_2_stability.yaml`
- `aits parameters diagnose-weight-stability-inputs --latest --dry-run`
- `aits parameters recover-weight-stability-inputs --latest --dry-run`
- `aits parameters validate-weight-stability-readiness --latest`
- `aits reports weight-stability-readiness --latest`

新增产物：

- `artifacts/weight_stability_readiness/YYYY-MM-DD/weight_stability_readiness_summary.json`
- `artifacts/weight_stability_readiness/YYYY-MM-DD/weight_stability_readiness_summary.md`

## Readiness 检查

必须覆盖：

- freshness readiness：`OK` 才允许 stable tuning 进入 backtest；`MISSING` / `STALE` / `FAILED` / `ACCEPTABLE_LAG` 均需阻塞或明确降级
- recover freshness result：识别 `COMPLETED_BUT_NOT_RECOVERED`
- signal snapshot readiness：允许 `LIMITED` 但要求 snapshot 存在、日期对齐、`real_signals >= 2`、`missing_signals = 0`
- backtest manifest readiness：要求 manifest / diagnostics 能证明 walk-forward 历史覆盖、资产覆盖、价格覆盖可用
- price coverage readiness：重点识别 `GOOGL` / `BRK.B` / `SGOV` 高缺失比例、单日 cache、mapping / registry 问题
- stable tuning eligibility：输出 `READY` / `LIMITED_READY` / `BLOCKED` / `RECOVERY_AVAILABLE` / `RECOVERY_FAILED` / `INSUFFICIENT_DATA` / `FAILED`

## 集成要求

- `tune-weights-stable` 若仍为 `INSUFFICIENT_DATA`，必须引用 readiness artifact，并显示 `reason=input_readiness_blocked` 与 `candidates_backtested=0`
- `shadow-backtest --latest --dry-run` supporting artifacts 必须引用 readiness summary
- Dashboard 展示 Stable Weight Tuning Readiness
- Reader Brief 展示 stable tuning readiness blocker 摘要
- 更新 `docs/task_register.md`、`docs/artifact_catalog.md`、`docs/system_flow.md`、`config/report_registry.yaml`

## 安全边界

必须保持：

- `production_effect=none`
- `manual_review_required=true`
- `auto_promotion=false`
- 不修改 `config/parameters/production/current.yaml`
- 不降低 data quality gate
- 不使用 mock / synthetic price history
- 不绕过 manifest / price coverage 检查
- 不让 stable tuning 在 `can_run=false` 时继续 backtest

## 验收

必须通过：

```bash
aits parameters diagnose-weight-stability-inputs --latest
aits parameters validate-weight-stability-readiness --latest
aits reports weight-stability-readiness --latest
aits parameters tune-weights-stable --latest
aits parameters shadow-backtest --latest --dry-run
python -m pytest -q
python -m ruff check scripts src tests
python -m compileall src scripts
git diff --check
```

`TRADING-061A` 完成后可标记为 `BASELINE_DONE`，但 `TRADING-061` 必须保持 `VALIDATING`，直到 stable tuning 真实进入 backtest 且 `candidates_backtested > 0`。

## 实施记录

2026-05-31：TRADING-061A 基础版实现完成。

- 新增 `weight_stability_readiness` 模块、JSON/Markdown artifact、校验、report alias 和 CLI。
- `tune-weights-stable` 在 `INSUFFICIENT_DATA` 时引用 readiness artifact，输出 `reason=input_readiness_blocked`，并保持 `candidates_backtested=0`。
- `shadow-backtest --dry-run` supporting artifacts 引用 readiness summary，promotion 仍 rejected。
- Dashboard / Reader Brief 已展示 stable weight tuning readiness blocker。
- 真实 latest readiness 为 `RECOVERY_FAILED`，主要 blocker 为 freshness `MISSING`、signal snapshot date mismatch、backtest manifest `FAILED`、`GOOGL` / `BRK.B` / `SGOV` price coverage 高缺失比例。
- 本任务不执行真实 cache 修复、不生成 synthetic price history、不修改 `config/parameters/production/current.yaml`。

后续若要自动执行恢复动作，应新增 TRADING-061B，而不是在 TRADING-061A 中静默恢复数据。
