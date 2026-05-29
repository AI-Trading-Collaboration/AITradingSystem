# TRADING-051A Ablation Validation & Signal Contribution Diagnostics

状态：VALIDATING
优先级：P1
最后更新：2026-05-30

## 背景

TRADING-051 v0.1 已实现 signal ablation 框架、CLI、Dashboard、Reader Brief 和 shadow promotion decision supporting artifact。真实运行结果为 `LIMITED`，`full_signal_backtest_limited`，`promotion_credit_signals=0`，说明框架已运行，但仍需要解释为什么 real signals 没有形成 promotion-credit contribution。

重点问题不是继续新增信号，而是确认 `trend_momentum` 和 `sector_strength` 是否真的进入 score calculation，ablation 是否改变 score、portfolio weights 和 performance metrics，以及当前 `neutral` / `below threshold` 结论是否可信。

## 目标

- 输出 per-signal signal usage diagnostics。
- 输出 per-signal score impact diagnostics。
- 输出 per-signal portfolio impact diagnostics。
- 输出 per-signal threshold diagnostics 和 classification reason。
- 新增 `diagnostic_status`，区分 `VALID`、`BELOW_THRESHOLD`、`NO_SCORE_IMPACT`、`NO_PORTFOLIO_IMPACT`、`NOT_USED_IN_SCORE`、`FALLBACK_SIGNAL`、`INSUFFICIENT_DATA`、`IMPLEMENTATION_WARNING`。
- 增强 `aits signals ablation --debug` 或 `aits signals explain-ablation --latest`。
- Dashboard 和 Reader Brief 展示 no-promotion-credit reason 与 implementation warnings。
- 保持 `production_effect=none`、`manual_review_required=true`、`auto_promotion=false`。

## 非目标

- 不新增新的交易信号。
- 不修改 `config/parameters/production/current.yaml`。
- 不自动调整 weights。
- 不解除 candidate promotion。
- 不接入真实交易。
- 不引入 ML optimizer。
- 不把 ablation positive 结果用于自动晋升。

## 验收标准

- `aits signals ablation --latest --debug` 或 `aits signals explain-ablation --latest` 能输出 snapshot -> score -> portfolio -> metrics -> classification 诊断链路。
- `aits signals validate-ablation --latest` 输出 `diagnostics_present=true`、`real_signals_used_in_score=true`、`classification_reasons_present=true`，并保持安全字段。
- `aits reports signal-ablation --latest` Markdown 包含 `Why No Promotion-credit Signals?`。
- `aits parameters shadow-backtest --latest --dry-run` 仍保持 `promotion_status=rejected`。
- 新增或增强测试覆盖 diagnostic status、score/portfolio impact、threshold diagnostics、fallback status、debug CLI、Dashboard 和 Reader Brief。
- `python -m pytest -q`、`python -m ruff check scripts src tests`、`python -m compileall src scripts`、`git diff --check` 通过。

## 进展记录

- 2026-05-30：新增 TRADING-051A 需求文档，进入 IN_PROGRESS。
- 2026-05-30：完成 `diagnostic_status`、`classification_reason`、signal usage diagnostics、score impact、portfolio impact、threshold diagnostics、`--debug` / `explain-ablation` CLI、Dashboard 和 Reader Brief no-credit reason。
- 2026-05-30：真实 `aits signals ablation --latest --debug` 确认 2026-05-28 `trend_momentum` 与 `sector_strength` 均 `used_in_score=yes`，score 和 portfolio impact 均 non-zero；二者仍为 `BELOW_THRESHOLD`，所以 `promotion_credit_signals=0` 的主要原因是 real signal contribution below threshold。
- 2026-05-30：验证通过 `aits signals explain-ablation --latest`、`aits signals validate-ablation --latest`、`aits reports signal-ablation --latest`、`aits parameters shadow-backtest --latest --dry-run`、`python -m pytest -q`、`python -m ruff check scripts src tests`、`python -m compileall src scripts` 和 `git diff --check`，进入 VALIDATING。
