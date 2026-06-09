# ETF Portfolio Credibility Validation

最后更新：2026-06-09

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
|TRADING-063E|DONE|Risk Constraint Validation|asset/sleeve/equity/cash/rebalance/drawdown/volatility constraint tests 与 diagnostics|
|TRADING-063F|DONE|Allocation Stability Diagnostics|turnover、weight delta、regime transition、constraint hit rate、exposure distribution 等 JSON/Markdown/CLI/report 输出|
|TRADING-063G|DONE|Simulation Ledger Forward-Evaluation Hardening|decision-time record 与 delayed `evaluation_only` future return 字段隔离|
|TRADING-063H|DONE|Backtest Metrics & Summary Report Standardization|统一 metrics schema、monthly table、benchmark excess、edge-case null reason|
|TRADING-063I|DONE|ETF Daily Brief Explainability Upgrade|安全 banner、regime、weights/deltas、driver explanations、constraints、benchmark context 和 future field 防护|
|TRADING-063J|DONE|Parameter Governance & Candidate Promotion Policy|model state、promotion gates、governance summary 和 unsafe candidate blocking tests|
|TRADING-063K|DONE|End-to-End Credibility Gate|聚合 063A~J、P2/live safety、JSON/Markdown 输出和 fail-closed tests|

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

## TRADING-063E 验收标准

- Allocation 执行 `config/etf_portfolio/risk.yaml` 中的 `max_rebalance_trade_weight` 和 `max_daily_turnover`，不能只停留在配置声明。
- Risk constraint tests 覆盖 asset cap/floor、SMH/SOXX semiconductor sleeve cap、Risk-Off equity/cash exposure、binding cash minimum、single-asset rebalance cap、daily turnover cap、weight sum normalization。
- `ETFAllocationRecord` / `target_weights.csv` 输出结构化 `constraint_diagnostics`，包含 `constraint_id`、`asset_or_sleeve`、`before_weight`、`after_weight`、`reason` 和 `severity`。
- Drawdown / volatility 风险惩罚作为 signal risk score 的约束输入进行测试；该惩罚先影响 composite score，不作为 allocation 层后验收益修补。
- 文档同步说明 `constraint_diagnostics` schema、allocation 约束执行范围和 signal 风险惩罚边界。

## TRADING-063F 验收标准

- 新增 allocation stability diagnostics，覆盖 `daily_turnover`、absolute weight delta、rebalance count/frequency、regime transition count、constraint hit rate、cash/equity/semiconductor exposure、asset exposure time 和可行的 average holding period。
- Diagnostics 同时进入 backtest `summary.json` / `summary.md`，并可通过 CLI 从既有 backtest run 重新生成 JSON/Markdown。
- Stability status 使用 ETF risk config 中的 `max_daily_turnover` 和 `max_rebalance_trade_weight` 作为 policy reference；首日从全现金建仓与后续 rebalance policy check 分开披露。
- Tests 覆盖 turnover、weight delta、regime transition、constraint hit rate、cash distribution、exposure time 和 schema stability。

## TRADING-063G 验收标准

- Simulation ledger 区分 `record_type=decision` 与 `record_type=evaluation`；decision rows 固定 `evaluation_only=false` 且不承载 forward return / benchmark outcome。
- Forward evaluation updater 只基于 decision rows 生成或更新 delayed evaluation rows，包含 `evaluation_as_of_date`、future return/drawdown、relative return、weight contribution 和 portfolio-vs-benchmark 字段，且 `evaluation_only=true`。
- Ledger decision rows 包含 decision date、model/config hash、feature/signal snapshot hash、asset score/target/previous/delta JSON、constraints、reason、`observe_only=true` 和 `production_effect=none`。
- Duplicate date/model/symbol handling deterministic；重复 record 不复制 decision row，同一 as-of evaluation 可重复生成但只保留一组 evaluation rows。
- Simulation report / daily brief summary 明确区分 decision-time 样本数和 evaluation 样本数。

## TRADING-063H 验收标准

- Backtest `summary.json` / `metrics.json` / `summary.md` 输出统一 `standardized_metrics` schema，至少包含 `start_date`、`end_date`、`trading_days`、`initial_nav`、`final_nav`、`total_return`、`CAGR`、`annualized_volatility`、`max_drawdown`、`Sharpe`、`Sortino`、`Calmar`、`best_month`、`worst_month`、`positive_month_ratio`、`turnover`、`average_equity_exposure`、`average_cash_weight`、`benchmark_excess_return` 和 `benchmark_drawdown_reduction`。
- 无法计算的指标必须输出 `null`，并在 `metric_null_reasons` 中记录原因；不得用 0 伪装 Sharpe、Sortino、Calmar、monthly 或 benchmark 缺失结果。
- 可行时输出 `monthly_returns` 表，包含 `month`、`strategy_return`、`benchmark_return`、`excess_return`、`max_drawdown_in_month` 和 `average_equity_exposure`。
- Benchmark excess / drawdown reduction 使用 primary benchmark 口径，并在 schema 中披露 benchmark id；primary benchmark 缺失时 fail visible。
- ETF backtest report / Reader Brief 可见层展示标准化摘要，并保留 report registry / report index 下钻入口。
- 测试覆盖 metric schema stability、CAGR、max drawdown、Sharpe zero-vol guard、Sortino no-downside guard、Calmar zero-drawdown guard、monthly aggregation、benchmark excess 和 Reader Brief/report visibility。

## TRADING-063I 验收标准

- ETF daily brief 必须包含明显安全 banner：`observe_only=true`、`production_effect=none`、`manual_review_only=true`、no broker action。
- Brief 必须展示当前 market regime、`ai_after_chatgpt` regime window、requested date、data quality、model version 和 config hash。
- Brief 必须包含 asset-level scores、target weights、previous target weights、weight deltas，并解释每个主要变动或未变动的原因。
- Driver explanations 必须结构化展示 top positive drivers 与 top negative drivers，至少覆盖 composite score、trend / momentum / relative strength / risk score、regime、constraint impact 和 rebalance threshold 语义。
- Risk constraints 必须展示配置 cap/min、已触发约束和结构化 constraint diagnostics；无触发时也要明确为 none。
- Benchmark context、simulation status、P2/live candidate-only note 和 actionability note 必须可见；报告不得被误读为交易指令或生产权重变更。
- Future evaluation fields（例如 `forward_return_*`、`relative_return_vs_*`、`evaluation_only`、`signal_hit_*`）不得泄漏到 decision sections；只允许在 Simulation Performance 摘要中作为已隔离 evaluation 结果出现。
- Reader Brief navigation 继续可用；daily brief report registry 契约不退化。
- 测试覆盖 safety banner、regime、target/previous/delta、constraints、driver explanations、benchmark context、simulation status、future evaluation field exclusion 和 Reader Brief navigation。

## TRADING-063J 验收标准

- 参数治理 policy 必须定义 `production_baseline`、`candidate`、`shadow`、`rejected`、`archived` model states，并声明当前 `production_effect=none`。
- Candidate promotion gate 必须 fail closed 检查 tests passed、shadow mode、最小样本、benchmark comparison、turnover、drawdown justification、no-lookahead validation、manual review required 和 production effect 边界。
- Governance summary 必须输出稳定 schema：`current_model_version`、`candidate_model_version`、`config_hash`、`sample_period`、`benchmark_comparison`、`turnover_comparison`、`drawdown_comparison`、`promotion_status`、`promotion_blockers` 和 `manual_review_required`。
- 通过所有 gate 时只能输出 `ELIGIBLE_FOR_MANUAL_REVIEW`，不得写 production baseline、target weights 或 broker/trading action。
- P2/live candidates 不得 self-promote；任何 production effect 请求都必须成为 blocker。
- 测试覆盖缺 benchmark、样本不足、turnover 过高、no-lookahead 未通过、全部 gate 通过、P2/live self-promotion blocked 和 schema stability。

## TRADING-063K 验收标准

- `aits etf credibility validate` 必须输出单一 JSON/Markdown gate，包含 `task=TRADING-063K`、overall `status`、11 项 subcheck status/details、`production_effect=none`、`manual_review_required=true` 和 no broker action。
- Gate 必须聚合 runtime artifact hygiene、benchmark suite、no-lookahead、toy accounting、risk constraints、allocation stability、simulation ledger schema、backtest metrics、daily brief explainability、parameter governance 和 P2/live safety。
- 任一关键检查失败必须 fail closed 并把 blocker 写入 `check_details`；PASS 只表示可继续 shadow evaluation，不代表 production approval。
- 测试覆盖 all-pass、no-lookahead failure、P2/live safety violation、benchmark suite missing、simulation schema invalid 和 JSON/Markdown output。

## 进展记录

- 2026-06-01: 新增本需求文档并把 TRADING-063 登记为 `IN_PROGRESS`；开始 TRADING-063A runtime artifact hygiene。
- 2026-06-01: TRADING-063A 完成。`.gitignore` 已忽略 `data/etf_portfolio/`、`data/simulation/` 和 `reports/`；新增 `tests/fixtures/etf_portfolio/.gitkeep`；`docs/artifact_catalog.md` 与 `docs/system_flow.md` 已说明 runtime artifact / fixture policy。验证通过 `git diff --check`、`python -m pytest tests -q`（1630 passed）和 `python -m ruff check config src tests scripts docs`。
- 2026-06-01: TRADING-063B 进入实现。当前缺口为 B004 / B005 / B006 / B008 benchmark、B001~B008 registry ID、config-driven static portfolio/risk-off policy，以及 `benchmark_comparisons` common metric schema。
- 2026-06-01: TRADING-063B 完成。`config/etf_portfolio/backtest.yaml` 已登记 B001-B008；backtest summary / metrics 输出 `benchmark_metrics` 和 `benchmark_comparisons`；真实 `aits etf backtest run --config config/etf_portfolio/backtest.yaml --fast` smoke 通过。验证通过 `python -m pytest tests -q`（1633 passed）、ruff、compileall 和 diff check。
- 2026-06-01: TRADING-063C 进入实现。当前缺口为 ETF timing contract 的统一校验 helper、`execution_date > signal_date` / `feature_source_date <= signal_date` / decision payload future-field 防护、simulation delayed evaluation 标记，以及 daily brief decision section 的 evaluation-only 字段隔离测试。
- 2026-06-01: TRADING-063C 完成。新增 `etf_portfolio/no_lookahead.py` timing-contract validation helper；backtest、simulation ledger 和 daily brief 已接入 no-lookahead 校验；simulation delayed evaluation 输出 `evaluation_only=true`；新增测试覆盖 valid `t -> t+1`、same-day execution failure、feature source date failure、decision payload future leakage、simulation marker 和 report decision section 防泄漏。真实 `aits etf backtest run --config config/etf_portfolio/backtest.yaml --fast` smoke 通过；验证通过 `python -m pytest tests -q`（1639 passed）、ruff、compileall 和 diff check。
- 2026-06-01: TRADING-063D 进入实现。toy accounting 梳理时确认当前 backtest 只有 `signal_date` / `return_date`，但 `execution_price=next_close` 的正确口径应显式拆为 `signal_date=t`、`execution_date=t+1`、`return_date=t+2`；本轮先修正 next-close accounting timing，再用手工可验 fixture 锁定 NAV、成本、贡献、drawdown 和 rebalance delta 行为。
- 2026-06-01: TRADING-063D 完成。新增 `tests/fixtures/etf_portfolio/toy_prices.csv` 和 toy accounting tests；backtest daily/weights/trades 输出显式 `execution_date`，收益窗口修正为 `execution_date -> return_date`，benchmark series 使用同一 signal lag；新增 `calculate_portfolio_accounting_step` 覆盖 NAV、交易成本、cash return、asset contribution、bad weight sum、same-day execution、return-date timing、drawdown 和 rebalance delta threshold。真实 `aits etf backtest run --config config/etf_portfolio/backtest.yaml --fast` smoke 通过；验证通过 `python -m pytest tests -q`（1645 passed）、ruff、compileall 和 diff check。
- 2026-06-01: TRADING-063E 进入实现。当前缺口为 `max_rebalance_trade_weight` / `max_daily_turnover` 尚未在 allocation 中执行，且 `constraints_applied` 只有代码列表，缺少包含 before/after/reason/severity 的结构化 diagnostics；本轮补齐约束执行和审计字段。
- 2026-06-01: TRADING-063E 完成。Allocation 已执行 `max_rebalance_trade_weight` / `max_daily_turnover`，`ETFAllocationRecord` 与 `target_weights.csv` 输出结构化 `constraint_diagnostics`；新增 `tests/test_etf_risk_constraints.py` 覆盖 asset cap、semiconductor sleeve cap、Risk-Off equity/cash、binding cash minimum、single-asset rebalance cap、daily turnover cap、weight sum normalization 和 signal 层 drawdown/volatility risk penalties。真实 `aits etf backtest run --config config/etf_portfolio/backtest.yaml --fast` smoke 通过；全量验证通过 `python -m pytest tests -q`（1652 passed）。
- 2026-06-01: TRADING-063F 进入实现。当前缺口为 backtest 只有 strategy turnover / weights / trades 原始表，缺少可直接复核 allocation 是否过度跳动的 stability JSON/Markdown/CLI 摘要；本轮新增 diagnostics 模块并接入 backtest summary。
- 2026-06-01: TRADING-063F 完成。新增 `etf_portfolio/stability.py`，backtest summary / metrics / Markdown 集成 `allocation_stability_diagnostics`，`write_backtest_run` 写出 `stability_diagnostics.json/md`，新增 `aits etf backtest diagnostics --latest` 从既有 run 重新生成 stability 摘要；测试覆盖 turnover、weight delta、regime transition、constraint hit rate、cash distribution、exposure time、holding-period schema 和 CLI artifact 写出。真实 `aits etf backtest run --config config/etf_portfolio/backtest.yaml --fast` 与 `aits etf backtest diagnostics --latest` smoke 通过；全量验证通过 `python -m pytest tests -q`（1656 passed）。
- 2026-06-01: TRADING-063G 进入实现。当前缺口为 `evaluate_simulation_ledger` 会把原 decision rows 直接改为 `evaluation_only=true`，不能保留纯 decision-time record；本轮改为 decision/evaluation record 分层并补 schema/report 测试。
- 2026-06-01: TRADING-063G 完成。Simulation ledger 已区分 `record_type=decision` 与 `record_type=evaluation`；decision rows 保留 config/snapshot hash、asset scores、target/previous/delta JSON、observe-only 和 production-effect 边界且不承载 future values；evaluation rows 按 `evaluation_as_of_date` deterministic upsert 并固定 `evaluation_only=true`；simulation report / daily brief summary 区分 decision/evaluation 样本。测试覆盖 decision creation 无 future value、forward updater delayed fill、evaluation marker、duplicate handling、config hash change 和 report separation；全量验证通过 `python -m pytest tests -q`（1657 passed）。
- 2026-06-01: TRADING-063H 进入实现。当前缺口为 ETF backtest 指标分散在 strategy metrics / extended metrics / benchmark comparisons 中，缺少稳定的跨策略比较 schema、月度收益表、null reason 语义和 Reader Brief 摘要可见性；本轮统一输出并补边界测试。
- 2026-06-01: TRADING-063H 完成。新增 `etf_portfolio/backtest_metrics.py`，backtest `summary.json` / `metrics.json` / `summary.md` 输出 `standardized_metrics`、`monthly_returns` 和 `metric_null_reasons`；`config/etf_portfolio/backtest.yaml` 显式配置 `primary_benchmark_id=B001`；Reader Brief 只读展示 ETF Backtest Summary 摘要；测试覆盖 schema、CAGR、max drawdown、Sharpe zero-vol、Sortino no-downside、Calmar zero-drawdown、insufficient volatility sample、monthly aggregation、benchmark excess 和 Reader Brief 可见性。真实 `aits etf backtest run --config config/etf_portfolio/backtest.yaml --fast` 与 `aits reports index --as-of 2026-05-31` smoke 通过；全量验证通过 `python -m pytest tests -q`（1664 passed）、ruff、compileall 和 diff check。
- 2026-06-01: TRADING-063I 进入实现。当前 daily brief 已有 summary、signal dashboard、target weights、risk constraints 和 simulation summary，但缺少显式 safety banner、结构化正负 driver、benchmark context、actionability/P2-live 边界小节，以及对 decision section future-field 泄漏的更细测试。
- 2026-06-01: TRADING-063I 完成。ETF daily brief 已新增 safety banner、AI regime/range、asset score reason codes、target/previous/delta、Weight Change Explanation、top positive/negative drivers、constraint diagnostics、benchmark context、simulation status、P2/live candidate-only note 和 actionability note；旧 target weights artifact 若缺结构化 constraint diagnostics 会显式标记 unavailable，不默认为 none；decision sections 继续调用 no-lookahead guard，测试用附加 `forward_return_20d` / `evaluation_only` 列验证不泄漏。真实 `aits etf report daily --date latest` smoke 通过；全量验证通过 `python -m pytest tests -q`（1664 passed）、`python -m ruff check config src tests scripts docs`、`python -m compileall -q src tests scripts` 和 `git diff --check`。
- 2026-06-01: TRADING-063J 进入实现。当前 ETF P0/P1/P2 已有 observe-only 边界和 backtest artifacts，但缺少专门的参数治理 policy、候选 promotion gate、固定 governance summary schema，以及对缺 benchmark、样本不足、turnover 过高、no-lookahead 失败和 P2/live self-promotion 的 fail-closed 测试。
- 2026-06-01: TRADING-063J 完成。新增 `config/etf_portfolio/governance.yaml`、`etf_portfolio/governance.py` 和 `aits etf governance summary`，输出稳定 `etf_parameter_governance` JSON/Markdown schema；promotion gate 覆盖 tests/shadow/sample/benchmark/turnover/drawdown/no-lookahead/manual-review/production-effect/P2-live self-promotion，全部通过时仅返回 `ELIGIBLE_FOR_MANUAL_REVIEW`。报告已登记到 report registry、artifact catalog 和 system flow；目标测试覆盖 required blockers、pass case 和 schema stability；`aits etf governance summary --date 2026-06-01` smoke 输出 `NO_CANDIDATE` 且 `production_effect=none`。
- 2026-06-01: TRADING-063K 进入实现。需要新增单一 credibility gate 聚合 runtime artifact hygiene、benchmark suite、no-lookahead、toy accounting、risk constraints、allocation stability、simulation schema、backtest metrics、brief explainability、parameter governance 和 P2/live safety，并输出 fail-closed JSON/Markdown 摘要。
- 2026-06-01: TRADING-063K 完成。新增 `etf_portfolio/credibility.py` 与 `aits etf credibility validate`，输出 `TRADING-063K` JSON/Markdown gate，聚合 11 项 subcheck、per-check source/blockers、`production_effect=none`、`manual_review_required=true` 和 `broker_action=none`。测试覆盖 all-pass、no-lookahead failure、P2/live safety violation、benchmark missing、simulation schema invalid 和 JSON/Markdown 输出；真实 `aits etf credibility validate --date 2026-06-01` smoke 为 PASS。
- 2026-06-09: 系统验证复跑 `python -m ai_trading_system.cli etf credibility
  validate --date 2026-06-01`，输出
  `reports/etf_portfolio/credibility/2026-06-01_credibility_gate.{json,md}`，
  status 仍为 `PASS`，11 项 checks 均为 `PASS`，`production_effect=none`、
  `broker_action=none`、`manual_review_required=true`、
  `safe_for_shadow_evaluation=true`。该 gate 只证明 ETF allocation foundation
  可信到可以继续 shadow evaluation；它不是 production approval，也不能替代后续
  shadow-evaluation artifacts 和 owner review。2026-06-09 刷新验证
  `python -m ai_trading_system.cli etf credibility validate --date 2026-06-01`
  仍为 PASS，`tests/test_etf_credibility.py` 6 passed。TRADING-063 从
  `VALIDATING` 归档为 `DONE`；后续真实 shadow evaluation artifacts、forward
  observation 和 owner review 由 TRADING-064+ / TRADING-070+ 链路继续承接。
