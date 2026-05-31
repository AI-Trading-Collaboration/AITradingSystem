# AITradingSystem Implementation Notes

最后更新：2026-05-31

## ETF Portfolio Allocation P0 Baseline

本轮根据 `G:/Download/AITradingSystem_ETF_Portfolio_Development_Document.md` 新增 ETF 主仓组合配置系统 P0 baseline。实现适配现有仓库事实：

- 现有包名和 CLI 为 `ai_trading_system` / `aits`，因此新增命令统一放在 `aits etf ...` 下，而不是创建新包名 `aitrading`。
- 新模块位于 `src/ai_trading_system/etf_portfolio/`，避免扰动既有个股评分、production 参数、shadow promotion 和真实交易链路。
- 默认回测主结论窗口使用 `ai_after_chatgpt`，从 2022-12-01 开始；更早价格只作为 warm-up。
- P0 使用 ETF OHLCV 和 CASH synthetic price，暂不依赖新闻、财报、期权、ML 或付费高级数据。
- 投资解释相关阈值集中在 `config/etf_portfolio/*.yaml`，并带 policy metadata；后续调整必须通过回测、模拟舱和权重治理。

## P0 Commands

- `aits etf validate-config`
- `aits etf data validate --date latest`
- `aits etf features build`
- `aits etf signals generate --date latest`
- `aits etf regime generate --date latest`
- `aits etf portfolio allocate --date latest`
- `aits etf backtest run --fast`
- `aits etf simulation record --date latest`
- `aits etf simulation evaluate --as-of latest`
- `aits etf simulation report`
- `aits etf report daily --date latest`
- `aits etf run daily --date latest --dry-run`

Simulation evaluation writes forward-return outcomes plus 20d SPY/QQQ-relative
returns, weight contribution, and portfolio-vs-benchmark fields once enough
future trading days exist. Missing forward windows or unavailable benchmark
rows remain null by design; they are not filled with zero.
Daily portfolio briefs read the simulation ledger for the Simulation
Performance section and show model-version records, 20d hit rate, and portfolio
vs SPY/QQQ summary. Missing ledgers, empty ledgers, insufficient forward
windows, or unavailable benchmarks remain explicit `n/a` / no-record states.

ETF backtest runs write `daily.csv`, `weights.csv`, `trades.csv`, `summary.json`,
`metrics.json`, and `summary.md` under the run directory. `daily.csv` includes
asset return and asset contribution JSON columns; `weights.csv` and
`trades.csv` are audit history only, not broker instructions. The configured
benchmark set includes buy-and-hold SPY/QQQ/SMH, static default portfolio, and
`ma_50_200_qqq`.
`aits features build --end latest` is accepted as a document-style alias for
using the latest available ETF price date as the feature end date. `aits etf
backtest run --config config/etf_portfolio/backtest.yaml` can explicitly select
the ETF backtest policy file; root `aits backtest` remains the existing
main-system backtest command.

## ETF Compatibility Aliases

The default ETF namespace remains `aits etf ...`. To match the development
document's shorter P0 workflow examples, these root-level aliases call the same
ETF implementations:

- `aits data ingest` / `aits data validate`
- `aits features build`
- `aits signals generate`
- `aits regime generate`
- `aits portfolio allocate`
- `aits simulation record` / `aits simulation evaluate` / `aits simulation report`
- `aits report daily`
- `aits run daily`
- `aits experiments register` / `aits experiments run` / `aits experiments compare`

Root `aits backtest` remains the existing main-system daily-scoring backtest.
ETF backtests stay under `aits etf backtest run/report` so the two result
families keep separate audit and investment-interpretation paths.

## P1 Observe-Only Commands

- `aits etf features build --include-satellites`
- `aits etf relative-strength report --date latest`
- `aits etf confirmation report --date latest`
- `aits etf satellite evaluate --date latest`
- `aits etf attribution report --date latest`
- `aits etf events risk-flag --date YYYY-MM-DD`
- `aits etf governance status`
- `aits etf experiments register --status candidate --notes "..."`
- `aits etf experiments run --config path/to/strategy_candidate.yaml`
- `aits etf experiments compare --baseline production`

P1 outputs are research/governance artifacts only and must keep `production_effect=none`. Market-data-dependent P1 reports rerun the ETF price quality gate and disclose `data_quality_status` plus the quality report path. Satellite candidates are suggestions only; they do not reduce ETF target weights or write production parameters.
Experiment run/compare commands are candidate-only registry operations. They record candidate config hashes, parameter diffs, optional backtest summary metrics, and manual-review status; they do not write production config, target weights, or promotion decisions.

## P2 Observe-Only Commands

- `aits etf p2 edgar-text --date YYYY-MM-DD`
- `aits etf p2 derive-edgar-events --date YYYY-MM-DD`
- `aits etf p2 fetch-edgar-text --date YYYY-MM-DD --symbol NVDA --filing-type 10-Q --limit 1`
- `aits etf p2 edgar-topics --date YYYY-MM-DD`
- `aits etf p2 derive-options-risk --date YYYY-MM-DD`
- `aits etf p2 import-source SOURCE_ID --input-path path/to/input.csv`
- `aits etf p2 normalize-holdings --input-path holdings.csv --etf-symbol SMH --date YYYY-MM-DD`
- `aits etf p2 normalize-news --input-path news.csv`
- `aits etf p2 normalize-options-risk --input-path options.csv`
- `aits etf p2 news-themes --date YYYY-MM-DD`
- `aits etf p2 options-risk --date YYYY-MM-DD`
- `aits etf p2 holdings-lookthrough --date YYYY-MM-DD`
- `aits etf p2 advanced-risk --date latest`
- `aits etf p2 walk-forward --date YYYY-MM-DD`
- `aits etf p2 ml-ranking --date latest`
- `aits etf p2 weight-optimizer --date latest`
- `aits etf p2 ensemble --date latest`
- `aits etf p2 live-preflight --date YYYY-MM-DD`

P2 uses `config/etf_portfolio/p2.yaml`. EDGAR filing metadata can be derived from the existing SEC PIT filing timeline through `p2 derive-edgar-events`; the derived feed is filing metadata only and keeps `sentiment_score=0.0` rather than inferring document tone. Official EDGAR filing text can be fetched through `p2 fetch-edgar-text`; it applies the same PIT availability gate, requires a SEC User-Agent for HTTP URLs, writes a local text cache plus `edgar_text_documents.csv`, and remains a cache/index layer only. Cached EDGAR filing text can be audited through `p2 edgar-topics`; topic keywords live in `p2.yaml`, and the report only emits counts / matched keywords / limitations, not sentiment, financial conclusions, weights, or trading advice. News themes can be normalized from vendor/manual CSV through `p2 normalize-news`; missing sentiment uses the configured neutral default and is disclosed as a limitation, not an LLM inference. `p2 news-themes` now emits symbol/theme tracking rows when canonical input exists, including event count、weighted sentiment、avg relevance、latest summary、source limitation and observe-only boundary; when input is missing it still reports `MISSING_INPUT`. Options risk can be derived from the local `^VIX` market cache through `p2 derive-options-risk`; it is an IV-rank proxy and explicitly leaves VXN/skew vendor fields blank. Vendor/manual options IV/VXN/skew CSV can be normalized through `p2 normalize-options-risk`; it requires PIT `available_at`, records source URL、download timestamp、row checksum and manifest, and remains observe-only. ETF holdings can be normalized from issuer/vendor/manual CSV through `p2 normalize-holdings`; `downloaded_at` remains the PIT availability field, so historical as-of reports fail if holdings were received later. `p2 import-source` validates required columns, writes canonical CSV, and appends `data/etf_portfolio/p2/source_manifest.csv` with provider、source URL、download timestamp、row count and checksum. Advanced risk and weight optimizer reuse the ETF price data quality gate. ML ranking、weight optimizer and ensemble are `candidate_only`; live preflight is read-only and keeps broker routing disabled.

ETF portfolio reports are registered in `config/report_registry.yaml` for read-only discovery by `aits reports index` and Reader Brief navigation. The registry covers the ETF portfolio brief, ETF data quality report, ETF backtest summary, and ETF P2 walk-forward readiness. This is visibility only: Reader Brief/report index do not rerun `aits etf ...`, do not write ETF target weights, and do not trigger trading actions.

## Assumptions

- Existing `data/raw/prices_daily.csv` is an acceptable local price cache when it includes SPY、QQQ、SMH、SOXX; the ETF data adapter standardizes `ticker` to `symbol` when needed and records this in the ETF data quality report.
- `CASH` is generated deterministically at price 1.0 for every observed trading date and is treated as an explicit synthetic cash asset, not a market data source.
- P0 `--dry-run` writes isolated artifacts under `artifacts/etf_portfolio/` and does not update the simulation ledger.
- Normal `aits etf run daily` writes ETF-specific outputs under `data/etf_portfolio/`, `data/simulation/etf_ledger.csv`, and `reports/etf_portfolio/`; it does not change production weights or broker/trading state.

## Known Limitations

- P2 modules are observe-only contracts, not production integrations. LLM-based EDGAR interpretation policy、live news vendor feed / LLM sentiment、connected true VXN/skew vendor feed、real-time issuer holdings API and live/multi-account trading still require provider/API/permission/cost/PIT policy decisions before they can become connected data sources. The current EDGAR adapter derives local filing metadata, can cache bounded official filing text, and can count governed topic keywords; it does not infer tone, extract financial statement values, alter signals, or make investment conclusions. The current weight optimizer emits candidate-only weights for review and never writes `target_weights.csv`. The current news/options/holdings adapters normalize audited local CSV inputs rather than downloading unapproved live feeds; the VIX proxy remains explicitly labeled when true VXN/skew data is absent.
- Event calendar starts as a configured observe-only risk flag. With no configured events, reports emit `NO_CONFIGURED_EVENTS` rather than inferring no event risk.
- The P0 backtest supports daily next-close execution lag and simple transaction cost assumptions; robustness and walk-forward governance should be added after P0 stabilizes.
