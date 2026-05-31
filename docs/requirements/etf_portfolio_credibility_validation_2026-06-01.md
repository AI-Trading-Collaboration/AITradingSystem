# ETF Portfolio Credibility Validation

最后更新：2026-06-01

## 背景

TRADING-062 已完成 ETF Portfolio Allocation System baseline。本阶段不新增 live trading、broker execution、production weight mutation 或 P2/live production influence；目标是验证 ETF allocation backtest、signal timing、simulation ledger、risk constraints、benchmark comparison 和 report explanations 是否足够可信，作为后续 research / simulation foundation。

默认市场阶段仍为 `ai_after_chatgpt`，anchor event 为 2022-11-30，默认回测起点为 2022-12-01。所有报告和回测输出必须披露实际请求日期范围和 regime。

## 安全边界

- `observe_only=true`。
- `production_effect=none`。
- `manual_review_only=true`。
- 不新增 broker action、order placement、account mutation 或 production weight mutation。
- P2/live modules 保持 `candidate_only=true`、`observe_only=true`、`production_effect=none`。
- 运行产物不得提交到源码；确定性测试 fixture 只能放在 `tests/fixtures/etf_portfolio/`。

## 阶段拆解

|任务|状态|目标|验收摘要|
|---|---|---|---|
|TRADING-063A|DONE|Runtime Artifact Hygiene & Fixture Policy|忽略 `data/etf_portfolio/`、`data/simulation/`、`reports/`，建立 `tests/fixtures/etf_portfolio/`，文档区分 runtime artifacts 与 committed fixtures|
|TRADING-063B|DONE|Benchmark Suite Hardening|B001~B008 benchmark registry/config、benchmark comparison schema 和测试覆盖|
|TRADING-063C|DONE|No-Lookahead Validation Framework|形式化 timing contract、校验 helper/module、决策字段 future leakage 防护测试|
|TRADING-063D|DONE|Toy Portfolio Accounting Tests|手工可验 deterministic toy prices、NAV/weights/cost/next-bar/drawdown/contribution 测试|
|TRADING-063E|READY|Risk Constraint Validation|asset/sleeve/equity/cash/rebalance/drawdown/volatility constraint tests 与 diagnostics|
|TRADING-063F|READY|Allocation Stability Diagnostics|turnover、weight delta、regime transition、constraint hit rate、exposure distribution 等 JSON/Markdown/CLI/report 输出|
|TRADING-063G|READY|Simulation Ledger Forward-Evaluation Hardening|decision-time record 与 delayed `evaluation_only` future return 字段隔离|
|TRADING-063H|READY|Backtest Metrics & Summary Report Standardization|统一 metrics schema、monthly table、benchmark excess、edge-case null reason|
|TRADING-063I|READY|ETF Daily Brief Explainability Upgrade|安全 banner、regime、weights/deltas、driver explanations、constraints、benchmark context 和 future field 防护|
|TRADING-063J|READY|Parameter Governance & Candidate Promotion Policy|model state、promotion gates、governance summary 和 unsafe candidate blocking tests|
|TRADING-063K|READY|End-to-End Credibility Gate|聚合 063A~J、P2/live safety、JSON/Markdown 输出和 fail-closed tests|

## TRADING-063A 验收标准

- `.gitignore` 忽略 `data/etf_portfolio/`、`data/simulation/`、`reports/`。
- `tests/fixtures/etf_portfolio/` 存在并作为唯一 ETF deterministic fixture 目录。
- `docs/artifact_catalog.md` 或等价文档明确说明 runtime artifacts 是本地生成并被忽略，deterministic fixtures 位于 `tests/fixtures/etf_portfolio/`，daily reports 不是 source artifacts。
- `docs/system_flow.md` 与 ETF runtime artifact policy 保持一致。
- `git status --short` 不再把现有 local ETF runtime outputs 显示为 source changes。
- 目标验证通过：`git diff --check`、`python -m pytest tests -q`、`python -m ruff check config src tests scripts docs`。

## TRADING-063C 验收标准

- ETF timing contract 文档化：raw market data date = `t`、feature snapshot date = `t`、signal date = `t`、allocation decision date = `t`、最早 execution date 为 `t` 之后下一交易日，portfolio return 使用 execution 之后价格。
- 新增 no-lookahead validation helper/module，覆盖 feature snapshots、signal records、allocation records、trade execution records、simulation ledger records 和 report decision sections。
- 校验能 fail closed 检出 `execution_date <= signal_date`、`feature_source_date > signal_date`、decision payload 中的 future/evaluation 字段，以及 daily brief decision section 中的 evaluation-only 字段。
- Simulation delayed evaluation 字段必须通过 `evaluation_only=true` 标记；decision-time record 不得因空 future 字段产生后验结论。
- 测试覆盖有效 `t -> t+1`、same-day execution failure、feature date after signal failure、decision payload future field failure、simulation delayed evaluation marker 和 report decision block 防泄漏。

## TRADING-063D 验收标准

- `tests/fixtures/etf_portfolio/toy_prices.csv` 提供手工可验 SPY / QQQ / CASH 价格。
- Backtest accounting 显式拆分 `signal_date < execution_date < return_date`；`execution_price=next_close` 时，信号日后第一段收益应从 execution date 到后续 return date 计算。
- Toy tests 覆盖 single-asset NAV、two-asset rebalance、cash return、target weight sum、transaction cost deduction、next-bar/next-close execution、portfolio drawdown、asset contribution 和 rebalance delta threshold 行为。
- 同步校验 no-lookahead helper 对 `return_date <= execution_date` fail closed。
- `daily.csv`、`weights.csv` 和 `trades.csv` 输出包含 `execution_date`，benchmark return series 使用同一 signal lag 口径。
- `max_rebalance_trade_weight`、`max_daily_turnover` 和完整 constraint diagnostics 属于 TRADING-063E 风险约束验收，本阶段不把它们伪装为已完成。

## 进展记录

- 2026-06-01: 新增本需求文档并把 TRADING-063 登记为 `IN_PROGRESS`；开始 TRADING-063A runtime artifact hygiene。
- 2026-06-01: TRADING-063A 完成。`.gitignore` 已忽略 `data/etf_portfolio/`、`data/simulation/` 和 `reports/`；新增 `tests/fixtures/etf_portfolio/.gitkeep`；`docs/artifact_catalog.md` 与 `docs/system_flow.md` 已说明 runtime artifact / fixture policy。验证通过 `git diff --check`、`python -m pytest tests -q`（1630 passed）和 `python -m ruff check config src tests scripts docs`。
- 2026-06-01: TRADING-063B 进入实现。当前缺口为 B004 / B005 / B006 / B008 benchmark、B001~B008 registry ID、config-driven static portfolio/risk-off policy，以及 `benchmark_comparisons` common metric schema。
- 2026-06-01: TRADING-063B 完成。`config/etf_portfolio/backtest.yaml` 已登记 B001-B008；backtest summary / metrics 输出 `benchmark_metrics` 和 `benchmark_comparisons`；真实 `aits etf backtest run --config config/etf_portfolio/backtest.yaml --fast` smoke 通过。验证通过 `python -m pytest tests -q`（1633 passed）、ruff、compileall 和 diff check。
- 2026-06-01: TRADING-063C 进入实现。当前缺口为 ETF timing contract 的统一校验 helper、`execution_date > signal_date` / `feature_source_date <= signal_date` / decision payload future-field 防护、simulation delayed evaluation 标记，以及 daily brief decision section 的 evaluation-only 字段隔离测试。
- 2026-06-01: TRADING-063C 完成。新增 `etf_portfolio/no_lookahead.py` timing-contract validation helper；backtest、simulation ledger 和 daily brief 已接入 no-lookahead 校验；simulation delayed evaluation 输出 `evaluation_only=true`；新增测试覆盖 valid `t -> t+1`、same-day execution failure、feature source date failure、decision payload future leakage、simulation marker 和 report decision section 防泄漏。真实 `aits etf backtest run --config config/etf_portfolio/backtest.yaml --fast` smoke 通过；验证通过 `python -m pytest tests -q`（1639 passed）、ruff、compileall 和 diff check。
- 2026-06-01: TRADING-063D 进入实现。toy accounting 梳理时确认当前 backtest 只有 `signal_date` / `return_date`，但 `execution_price=next_close` 的正确口径应显式拆为 `signal_date=t`、`execution_date=t+1`、`return_date=t+2`；本轮先修正 next-close accounting timing，再用手工可验 fixture 锁定 NAV、成本、贡献、drawdown 和 rebalance delta 行为。
- 2026-06-01: TRADING-063D 完成。新增 `tests/fixtures/etf_portfolio/toy_prices.csv` 和 toy accounting tests；backtest daily/weights/trades 输出显式 `execution_date`，收益窗口修正为 `execution_date -> return_date`，benchmark series 使用同一 signal lag；新增 `calculate_portfolio_accounting_step` 覆盖 NAV、交易成本、cash return、asset contribution、bad weight sum、same-day execution、return-date timing、drawdown 和 rebalance delta threshold。真实 `aits etf backtest run --config config/etf_portfolio/backtest.yaml --fast` smoke 通过；验证通过 `python -m pytest tests -q`（1645 passed）、ruff、compileall 和 diff check。
