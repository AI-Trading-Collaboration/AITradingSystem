# TRADING-050: Signal Snapshot Builder & Full-signal-limited Shadow Backtest

## 背景

`TRADING-049` 已完成 price history repair、`BRK.B -> BRK-B` provider symbol mapping、
backtest diagnostics、backtest input manifest、`price_only_shadow_backtest` 以及
Dashboard / Reader Brief 的 price-only 限制展示。当前 blocker 已从 price data 缺口收敛为
signal snapshots 缺失或质量有限。

本任务把 shadow backtest 从仅验证价格路径的 price-only 模式推进到可消费完整信号快照的
`full_signal_backtest_limited`。本阶段目标是先跑通可审计信号快照链路，不宣称 fallback
信号具备完整 alpha 质量。

## 范围

本阶段实现 6 类 v0.1 信号：

- `trend_momentum`：基于价格序列计算，质量标记为 `price_derived`。
- `sector_strength`：基于相对 QQQ 的价格相对强弱计算，质量标记为 `price_derived`。
- `macro_liquidity`：v0.1 使用 neutral/proxy fallback，质量标记为 `proxy_or_neutral`。
- `earnings_quality`：v0.1 使用 neutral fallback。
- `valuation_risk`：v0.1 使用 neutral fallback，除非后续明确引入可审计估值 proxy。
- `event_risk`：v0.1 使用 neutral fallback，不伪造事件数据。

输出：

- `artifacts/signal_snapshots/YYYY-MM-DD/signal_snapshot.json`
- `artifacts/signal_snapshots/YYYY-MM-DD/signal_snapshot.md`

CLI：

- `aits signals build-snapshot --latest`
- `aits signals build-snapshot --date YYYY-MM-DD`
- `aits signals build-snapshot --latest --dry-run`
- `aits signals build-snapshot --latest --price-derived-only`
- `aits signals validate-snapshot --latest`
- `aits reports signal-snapshot --latest`

## 模式与晋升规则

Backtest mode 判定：

- `price_only_shadow_backtest`：price data OK、signal snapshot 不存在；最多 `rejected`，
  `can_promote_candidate=false`。
- `full_signal_backtest_limited`：signal snapshot 存在且 required signals 齐全，但包含
  `LIMITED` / `NEUTRAL_FALLBACK`；最多 `watch`，`can_promote_candidate=false`。
- `full_signal_backtest`：required signals 全部 `OK`；最多 `candidate`，
  `can_promote_candidate=true`。

任何模式都保持：

- `production_effect=none`
- `manual_review_required=true`
- `auto_promotion=false`
- 不修改 `config/parameters/production/current.yaml`

## 实施步骤

1. 新增 signal snapshot builder / validator / Markdown renderer，生成标准 JSON / Markdown。
2. 新增 `signals` CLI 和 `reports signal-snapshot` alias。
3. 更新 diagnostics，使缺失 snapshot 显示 `MISSING`，存在 fallback 时显示 `LIMITED`，
   并选择 `full_signal_backtest_limited`。
4. 更新 backtest input manifest，把 `signal_snapshot.json` 纳入 `signal_snapshot_files`。
5. 更新 shadow backtest，使 snapshot 存在时按 production weights 读取信号值计算综合分、
   组合权重、score attribution 和 parameter contribution summary。
6. 更新 promotion constraints：price-only 只能 rejected，full-signal-limited 最多 watch，
   full-signal OK 才允许 candidate。
7. 更新 Dashboard / Reader Brief 显示 Signal Snapshot Summary、mode 和 promotion
   eligibility。
8. 更新 `docs/system_flow.md` 与 `docs/artifact_catalog.md`。
9. 补充专项测试并运行目标验证。

## 验收标准

- `aits signals build-snapshot --latest` 生成 JSON / Markdown，且生产参数文件未变化。
- `aits signals validate-snapshot --latest` 输出 `status=LIMITED`、
  `real_signals>=2`、`fallback_signals>=1`、`missing_signals=0`。
- `aits data diagnose-backtest-inputs --latest` 输出 `price_data_status=OK`、
  `signal_snapshots_status=LIMITED`、`backtest_mode=full_signal_backtest_limited`、
  `can_run_shadow_backtest=true`、`can_promote_candidate=false`。
- `aits parameters shadow-backtest --latest --dry-run` 输出
  `backtest_mode=full_signal_backtest_limited`、`production_effect=none`、
  `manual_review_required=true`、`auto_promotion=false`，且 promotion reason 明确说明
  signal quality limited 不能 candidate。
- `python -m pytest -q`
- `python -m ruff check scripts src tests`
- `python -m compileall src scripts`
- `git diff --check`

## 进展记录

- 2026-05-30：新增任务并进入 `IN_PROGRESS`，先补需求拆解和任务登记，再实现
  signal snapshot builder、full-signal-limited diagnostics、shadow backtest 集成和展示层。
- 2026-05-30：实现完成并进入 `DONE`。真实 latest artifact 日期为 2026-05-28；
  `build-snapshot --latest` 和 `validate-snapshot --latest` 生成/校验 `LIMITED` snapshot，
  `real_signals=2`、`proxy_signals=1`、`fallback_signals=3`、`missing_signals=0`；
  diagnostics 选择 `full_signal_backtest_limited`，shadow dry-run 输出
  `score_calculation`、`score_attribution` 和 `parameter_contribution_summary`，promotion
  reason 明确 signal quality limited 不能 candidate；全量 pytest、ruff、compileall 和
  diff check 通过。
