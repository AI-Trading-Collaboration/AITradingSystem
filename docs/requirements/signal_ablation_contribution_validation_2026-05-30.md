# TRADING-051 Signal Ablation & Contribution Validation

状态：VALIDATING
优先级：P1
最后更新：2026-05-30

## 背景

TRADING-050 已完成 v0.1 signal snapshot 和 `full_signal_backtest_limited` shadow backtest 链路。当前 snapshot 中 `trend_momentum`、`sector_strength` 为 price-derived real signals，`macro_liquidity` 为 proxy/neutral，`earnings_quality`、`valuation_risk`、`event_risk` 为 neutral fallback。

下一步不能继续叠加新信号，而应先验证已进入 score calculation 的信号是否对回测收益、回撤、Sharpe 和换手率有贡献。否则后续补充 valuation / event / earnings 信号时，解释复杂度会增加，但不一定提升有效性。

## 目标

- 对 required signals 执行 remove-one-signal ablation。
- 对比 full-signal-limited baseline 与移除单个信号后的结果。
- 输出全样本和 walk-forward window 贡献分类。
- 标记 `positive`、`negative`、`neutral`、`unstable`、`insufficient_data`。
- 单独标记 proxy / fallback 信号风险。
- 生成 `artifacts/signal_ablation/YYYY-MM-DD/signal_ablation_summary.json` 和 `.md`。
- 接入 `aits signals ablation`、`aits signals validate-ablation`、`aits reports signal-ablation`。
- 接入 Daily Task Dashboard、Reader Brief 和 shadow promotion decision supporting artifacts。
- 保持 `production_effect=none`、`manual_review_required=true`、`auto_promotion=false`。

## 非目标

- 不自动修改 signal weights。
- 不修改 `config/parameters/production/current.yaml`。
- 不解除 candidate promotion。
- 不接入真实交易。
- 不新增 ML optimizer。
- 不用 LLM 生成信号数值。

## 信号范围

第一版覆盖 signal snapshot required signals：

- `macro_liquidity`
- `trend_momentum`
- `sector_strength`
- `earnings_quality`
- `valuation_risk`
- `event_risk`

## Ablation 模式

v0.1 默认实现 `remove_one_signal`：

- 将被测信号 weight 设为 0。
- 剩余 signal weights 按原权重比例重新归一化。
- 使用同一 signal snapshot、同一价格缓存、同一 transaction cost 和同一 walk-forward window。

`neutralize_one_signal` 先记录为后续扩展，不作为本轮验收要求。

## 指标和分类

必须输出 baseline、ablation result 和 delta，至少覆盖：

- return：`cumulative_return`、`annualized_return`
- risk：`max_drawdown`、`volatility`、`downside_volatility`
- risk adjusted：`sharpe_ratio`、`sortino_ratio`、`calmar_ratio`
- behavior：`turnover`、`number_of_rebalances`
- decision quality：`drawdown_reduction_ratio`、`missed_upside_rate`、`false_risk_alert_rate`

默认阈值放入 `config/parameters/signal_ablation.yaml`，包括：

- `annualized_return_noise_band: 0.01`
- `sharpe_noise_band: 0.05`
- `max_drawdown_noise_band: 0.02`
- `turnover_noise_band: 0.10`
- `min_walk_forward_windows: 2`

分类解释：

- `positive`：移除该信号后 Sharpe 降低，且收益或回撤恶化。
- `negative`：移除该信号后 Sharpe 改善，且回撤改善或收益未明显受损。
- `neutral`：核心 delta 均在 noise band 内。
- `unstable`：walk-forward window 中同时出现 positive 和 negative。
- `insufficient_data`：walk-forward window 不足。

## Proxy / Fallback 处理

- proxy signal 可以参与 ablation，但不得作为 promotion 正面证据。
- fallback signal 必须单独标记。
- fallback signal 若显示 positive 或 negative 贡献，必须输出 suspicious warning。
- fallback signal 的 `promotion_credit_allowed=false`。

## CLI 验收

- `aits signals ablation --latest`
- `aits signals ablation --date YYYY-MM-DD`
- `aits signals ablation --signals trend_momentum sector_strength`
- `aits signals ablation --latest --dry-run`
- `aits reports signal-ablation --latest`
- `aits signals validate-ablation --latest`

`validate-ablation` 应显示：

- `status=LIMITED`
- `production_effect=none`
- `manual_review_required=true`
- `auto_promotion=false`

## 安全边界

- Ablation 是只读研究产物。
- 不写 production 参数。
- 不自动设置 candidate promotion。
- `full_signal_backtest_limited` 仍最多进入 watch / rejected。
- promotion decision 只能引用 ablation summary 作为 supporting artifact。

## 测试计划

- `tests/trading_engine/test_signal_ablation.py`
- `tests/trading_engine/test_signal_ablation_report.py`
- `tests/trading_engine/test_signal_ablation_dashboard.py`

覆盖 remove-one-signal、fallback promotion credit 禁止、proxy warning、positive/negative/neutral/unstable/insufficient classification、JSON schema、Markdown、Dashboard、Reader Brief、promotion decision supporting artifact 和 no auto-promotion。

## 进展记录

- 2026-05-30：新增任务和需求文档，进入 IN_PROGRESS。
- 2026-05-30：完成 v0.1 remove-one-signal ablation 核心实现、CLI、JSON/Markdown artifact、report alias、Dashboard/Reader Brief 接入、shadow promotion decision supporting artifact、system flow、artifact catalog 和 report registry 更新。
- 2026-05-30：真实 `aits signals ablation --latest` 生成 2026-05-28 `LIMITED` summary；`positive_signals=0`、`negative_signals=0`、`fallback_signals=3`、`promotion_credit_signals=0`，candidate promotion 仍 disabled。
- 2026-05-30：验证通过 `aits reports signal-ablation --latest`、`aits signals validate-ablation --latest`、`aits parameters shadow-backtest --latest --dry-run`、`python -m pytest -q`、`python -m ruff check scripts src tests`、`python -m compileall src scripts` 和 `git diff --check`，进入 VALIDATING。
