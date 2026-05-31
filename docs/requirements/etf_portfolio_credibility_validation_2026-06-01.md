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
|TRADING-063C|READY|No-Lookahead Validation Framework|形式化 timing contract、校验 helper/module、决策字段 future leakage 防护测试|
|TRADING-063D|READY|Toy Portfolio Accounting Tests|手工可验 deterministic toy prices、NAV/weights/cost/next-bar/drawdown/contribution 测试|
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

## 进展记录

- 2026-06-01: 新增本需求文档并把 TRADING-063 登记为 `IN_PROGRESS`；开始 TRADING-063A runtime artifact hygiene。
- 2026-06-01: TRADING-063A 完成。`.gitignore` 已忽略 `data/etf_portfolio/`、`data/simulation/` 和 `reports/`；新增 `tests/fixtures/etf_portfolio/.gitkeep`；`docs/artifact_catalog.md` 与 `docs/system_flow.md` 已说明 runtime artifact / fixture policy。验证通过 `git diff --check`、`python -m pytest tests -q`（1630 passed）和 `python -m ruff check config src tests scripts docs`。
- 2026-06-01: TRADING-063B 进入实现。当前缺口为 B004 / B005 / B006 / B008 benchmark、B001~B008 registry ID、config-driven static portfolio/risk-off policy，以及 `benchmark_comparisons` common metric schema。
- 2026-06-01: TRADING-063B 完成。`config/etf_portfolio/backtest.yaml` 已登记 B001-B008；backtest summary / metrics 输出 `benchmark_metrics` 和 `benchmark_comparisons`；真实 `aits etf backtest run --config config/etf_portfolio/backtest.yaml --fast` smoke 通过。验证通过 `python -m pytest tests -q`（1633 passed）、ruff、compileall 和 diff check。
