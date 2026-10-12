# OPS-080 日报 DQ 之后不再改写输入

最后更新：2026-10-12

稳定任务 ID：`OPS-080_DAILY_IMMUTABLE_INPUT_CLOSURE`

Owner 决定：`owner_decision:OPS-080:2026-10-12:no_input_rewrite_after_dq`

状态：`VALIDATING`

旧 v2 需求文档 `docs/requirements/OPS-080_Daily_Immutable_Input_Closure.md` 只在保全 ref
`refs/aits/source-preservation/ops080-v2-source-20260921-v1` 里；本次只参考了它的第 1、3 节，没有恢复旧源码。

## 1. 问题与证据

- main `8ffe97b25` 上，日报的 `market_data_recover_freshness` 排在 `validate_data` 和 `score_daily` 之后。
  2026-10-09 那次计划任务运行中，它从 FMP 抓了 GOOGL、BRK.B 写进 `data/raw/prices_daily.csv`，
  `updated_price_cache: true`（运行副本 `artifacts\data_refresh\2026-10-09\market_data_refresh_summary.json`）。
  也就是说，DQ 检查过的价格文件在 DQ 之后又被改写。
- 起因：`download-data` 只下载评分股票池（`configured_price_tickers`），并且只从上一份下载发布里复用这些标的的行；
  `config/data/market_data_freshness.yaml` 和 `market_data_refresh.yaml` 的 `assets.required` 却含 GOOGL、BRK.B。
  所以每天下载后 GOOGL、BRK.B 都不在价格文件里，freshness 判为缺失，补抓步骤在 DQ 之后再抓一遍。
- GOOGL、BRK.B 不是误配：它们来自影子回测的基准参数 `config/parameters/production/current.yaml`（`asset_universe`），
  两份 `assets.required` 与它一致。影子回测还要不要、用什么股票池，属于研究讨论，本任务不改。

## 2. Owner 决定

Claude 在线程"工程收尾遗留盘点"中的提问（2026-10-12 22:25 JST，节选）：

> 2. OPS-080：定价格缓存修复的范围。……我建议这样改：补抓挪到 DQ 之前，DQ 之后只检查、只报告；补抓清单和评分股票池不一致的问题一起处理。这一步改的是数据路径，属于 C 区，要你同意才能动手，具体实现交给 pi 那边。
>
> 两件都按上面的建议定，可以吗？（是 / 否）

Owner 回答（2026-10-12 22:28 JST）："是"

记为 `owner_decision:OPS-080:2026-10-12:no_input_rewrite_after_dq`。批准的范围：

1. 日报里 DQ 之后的步骤不再写 DQ 检查过的输入（价格文件、价格缓存登记、下载 manifest 等）；日报里的补抓改为只检查、只报告，手动恢复命令保留在日报链外。
2. 补抓需要的资产放到 DQ 之前取得，让 DQ 也检查它们。
3. 处理补抓清单和评分股票池的不一致；默认把 GOOGL、BRK.B 加进 DQ 之前的下载，不改 `current.yaml`，并核对 `config/ticker_aliases.yaml` 与 `config/data_sources.yaml` 的说明，避免它们被并成 GOOG。

要改 DQ profile 或阈值、`current.yaml`、评分股票池，或者超出上面三点，需要回到 owner。

## 3. 实现

1. **下载带上影子跟踪资产**（`cli_commands/data_cache.py`、`data/download.py`）：`download-data` 读取
   `market_data_freshness.yaml` 的 `assets.required`，不在评分股票池里的（现在是 GOOGL、BRK.B）作为辅助标的，
   只由主价格源（FMP）一起下载，进入同一份下载发布，DQ 检查它们的行。
   - 价格文件里没有的辅助标的从结束日前 1600 个日历日开始取：覆盖影子回测 `min_history_days` 的 1008 个交易日并留出余量，
     又在 FMP Starter 提供的 5 年历史之内；已有的只补尾部。
   - 第二价格源不下载辅助标的；下载发布的 `request_parameters` 记录 `auxiliary_tickers` 和 `auxiliary_history_start`。
   - FMP 把 BRK.B 列为 BRK-B，请求时用 BRK-B，行里仍是内部 ticker BRK.B（`FMP_DEFAULT_SYMBOL_ALIASES`）。
   - 评分股票池（`configured_price_tickers`）和 DQ 的预期 ticker 不变；DQ 不要求辅助标的覆盖 2018 年起的窗口。
2. **补抓步骤移到 DQ 之前，并且不改价格**（`ops_daily.py`、`config/scheduled_tasks.yaml` v8、
   `trading_engine/market_data_refresh.py`、`cli_commands/data.py`、`cli_direct.py`）：
   - 日报命令改为 `aits data recover-freshness --latest --no-price-writes`，排在 `capture_daily_inputs` 之后、`validate_data` 之前；休市日照旧跳过。
   - `--no-price-writes` 时不补抓、不写价格文件；缺历史的资产记为 `SOURCE_DELAYED`；
     只有全部资产都完整时才刷新价格缓存登记、backtest manifest、freshness 和候选跟踪；`actions.price_writes_allowed=false`。
   - `validate_data` 不依赖这一步；它失败时仍只阻断 artifact lineage 和 Reader Brief，不阻断 DQ 和评分（与改动前相同）。
   - 不带 `--no-price-writes` 的 `recover-freshness` 保留为手动工具，行为不变；手动补抓后要重跑 DQ。
3. **DQ 之后只读**：DQ 之后只剩只读的 `aits data freshness --latest`。有测试锁定：能写 DQ 输入的命令
   （`download-data`、`capture-daily-inputs`、`recover-freshness`、`refresh-market`、`reconcile-price-cache`、
   `refresh-backtest-manifest`、`repair-backtest-inputs`）都排在 `validate_data` 之前。
4. **不并成 GOOG**：`config/ticker_aliases.yaml` 的 `GOOGL: GOOG` 是 SEC PIT 的别名，`config/data_sources.yaml`
   第 39 行规定价格路径按内部 ticker、不复用这类别名。核对后发现 SEC PIT 评估的前瞻收益
   （`fundamentals/sec_pit_evaluation.py` 的 `_build_forward_labels`）会把价格文件里的 GOOGL 行并进 GOOG，
   价格文件有 GOOGL 后会算错。新增 `price_row_ticker_aliases`：别名只在目标 ticker 自己没有价格行时才用。
   `scoring/baseline_score_backfill.py` 也对价格行用了别名，但只用来取日期集合和"当天有没有价格"，
   GOOG 与 GOOGL 交易日相同，结果不变；它在 C 区，本次不改。
5. **评分不受影响**：`build_market_features` 对价格文件里的每个 ticker 生成特征，多出的只是 GOOGL、BRK.B 自己的行；
   评分、仓位门禁、宏观预算都按 (subject, feature) 取值，market panel、SEC PIT shadow observe 按配置的标的过滤。
   测试确认加入这两个标的后，原有特征行和警告不变。

## 4. 已知取舍

- GOOGL、BRK.B 进入每日下载后，和评分股票池的标的一样：FMP 取不到它们时下载失败，当天日报阻断。
- 每天多 2 个标的、4 次 FMP 请求；第一次运行各取约 1,100 行历史，价格文件多约 2,200 行。
- DQ 对它们只做行级检查，不检查 2018 年起的覆盖；影子回测需要的 1008 个交易日由补抓步骤的历史检查确认。

## 5. 验收

1. 测试：`tests/test_ops_daily.py`（DQ 之后无写输入步骤、命令带 `--no-price-writes`）、`tests/test_scheduled_tasks.py`、
   `tests/test_data_download.py`（辅助标的的起点、只补尾部、第二源不取、发布记录、1600 天窗口与 1008 交易日）、
   `tests/test_market_data.py`（BRK-B）、`tests/trading_engine/test_market_data_refresh.py`（下载后补抓不改价格；
   不允许写价格时只报告）、`tests/trading_engine/test_freshness_recovery_flow.py`、`tests/test_cli_direct.py`、
   `tests/test_market_features.py`（特征不变）、`tests/trading_engine/test_sec_pit_evaluation.py`（不并成 GOOG）。
2. 运行副本切到含本修复的 main 后，第一次日报：
   - `market_data_recover_freshness` 在 `validate_data` 之前运行；
   - `artifacts\data_refresh\<日期>\market_data_refresh_summary.json` 的 `actions.price_writes_allowed=false`、`updated_price_cache=false`；
   - DQ 严格 PASS，价格文件含 GOOGL、BRK.B，且与下载发布一致；DQ 之后没有步骤再写价格文件。
3. 之后删除保全位置（worktree `D:\Work\AITradingSystem_ops080_input_closure`、分支、保全 ref、`worktree_dirty\`、备份包），
   仍要 owner 在线程里打字确认。

## 6. 进度记录

- 2026-10-11：记录整理（`fa0ab59d`）把本任务从 DEFERRED 改为 PROPOSED，重定范围。
- 2026-10-12 22:28 JST：owner 决定 `owner_decision:OPS-080:2026-10-12:no_input_rewrite_after_dq`（第 2 节）。
- 2026-10-12：在 main `8ffe97b25` 上实现第 3 节；相关测试通过。状态改为 VALIDATING，等运行副本切换后的第一次日报核对。
