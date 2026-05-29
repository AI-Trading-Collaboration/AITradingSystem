# TRADING-052 Trend/Sector Signal Calibration

## 状态

- task id: `TRADING-052`
- priority: `P1`
- status: `VALIDATING`
- owner: system
- started: 2026-05-30

## 背景

`TRADING-051A` 已确认 `trend_momentum` 和 `sector_strength` 均进入 score，并且 ablation 会改变 score 和 portfolio。当前阻塞不是信号未接入，而是 price-derived real signal 的实际贡献低于 promotion-credit threshold。

## 目标

新增只读 Trend/Sector Signal Calibration 框架，用于比较多组 `trend_momentum` / `sector_strength` formula profile，输出 signal distribution、signal correlation、shadow backtest metrics、ablation contribution、portfolio impact 和 profile ranking。

## 非目标

- 不新增 `macro_liquidity`、`valuation_risk`、`event_risk` 正式数据源。
- 不自动调整 production weights。
- 不修改 `config/parameters/production/current.yaml`。
- 不解除 candidate promotion。
- 不引入 ML optimizer 或真实交易。

## 实施步骤

1. 新增 `config/signals/signal_calibration_profiles.yaml`，记录 profile、诊断阈值、ranking policy 和 safety fields。
2. 扩展 signal snapshot builder，使 `trend_momentum` / `sector_strength` 可按 profile 构建，同时保持默认 `TRADING-050` 行为不变。
3. 新增 calibration report builder，按 profile 生成 calibrated snapshot、shadow backtest metrics、remove-one-signal ablation、distribution/correlation diagnostics、ranking 和 recommended profile artifact。
4. 新增 CLI：`aits signals calibrate --latest` 与 `aits reports signal-calibration --latest`。
5. 将 latest calibration summary 只读接入 shadow promotion reason、Daily Task Dashboard 和 Reader Brief。
6. 更新 `docs/system_flow.md`、`docs/artifact_catalog.md`、`config/report_registry.yaml` 和测试。

## 验收标准

- `aits signals calibrate --latest` 生成 `artifacts/signal_calibration/YYYY-MM-DD/signal_calibration_summary.json` 和 `.md`。
- `aits reports signal-calibration --latest` 可读取 latest calibration summary 并写 alias。
- `aits parameters shadow-backtest --latest --dry-run` 仍保持 `promotion_status=rejected` 或更保守状态，且 `production_effect=none`、`manual_review_required=true`、`auto_promotion=false`。
- shadow promotion reason 可引用 latest calibration summary，但不得支持 candidate promotion。
- Dashboard 展示 Signal Calibration Summary 卡片。
- Reader Brief 展示 3-5 行 calibration 摘要。
- 不修改 `config/parameters/production/current.yaml`。
- 专项测试、`python -m pytest -q`、ruff、compileall 和 `git diff --check` 通过。

## 开放问题

- 若 best profile 仍然只改变 score/portfolio 但贡献低于阈值，下一步应进入 `TRADING-053 Portfolio Sensitivity Diagnostics`，而不是继续只补 signal formula。

## 进展记录

- 2026-05-30: 完成 v0.1 实现和验收。`aits signals calibrate --latest` 生成 2026-05-28 calibration summary，`best_profile=trend_long_bias`，整体 `status=LIMITED`，`can_support_candidate_promotion=false`。
- 2026-05-30: `aits reports signal-calibration --latest` 可读取 latest summary 并写 `outputs/reports` alias；`aits parameters shadow-backtest --latest --dry-run` 仍为 `promotion_status=rejected`，promotion reason 只读引用 calibration artifact，安全字段保持 `production_effect=none`、`manual_review_required=true`、`auto_promotion=false`。
- 2026-05-30: `python -m pytest -q`、`python -m ruff check scripts src tests`、`python -m compileall src scripts` 和 `git diff --check` 均通过；未修改 `config/parameters/production/current.yaml`。
