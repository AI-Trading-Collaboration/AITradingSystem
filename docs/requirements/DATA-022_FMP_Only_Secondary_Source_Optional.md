# DATA-022：价格只用 FMP，第二源对账改为可选

## Owner decision

- `owner_decision:DATA-022:2026-10-12:fmp_only_secondary_optional`
- 原话（Baker，2026-10-12 09:53 JST）：「只留 FMP，第二源对账改成可选」
- 含义：保留 FMP 订阅，退订 Marketstack；日报和 DQ 不再要求第二行情源；对账能力保留，作为手动可选步骤。

## 背景

迁移前日报链有 5 处硬要求 Marketstack，缺任何一处日报都 fail closed：

1. `aits download-data` 默认带 Marketstack，没有 `MARKETSTACK_API_KEY` 直接退出；
2. daily input capture 的 market_macro 快照在发布里没有第二源时报错；
3. DQ `daily_default.v1` 的输入形状固定要求 `prices_marketstack_daily.csv` 和 `require_secondary_prices=true`；
4. score-daily 的 DQ consumer authorization 要求收据角色为 prices、rates、secondary_prices；
5. 休市日判断"缓存已覆盖"时要求 Marketstack 日期也够新。

另外 score-daily、reports、backtest、feedback、trade-review、build-features 和两个研究 DQ gate 各自复制了一份 `_requires_marketstack_prices`，默认路径就强制要求第二源。

运行副本核对（2026-10-12，只读）：

- 2026-05-02 到 10-09 的 96 份 DQ 报告里有 8 次 FAIL，原因是未来日期、FRED 过旧、窗口和交易日历，没有一次来自第二源。
- 89 份对账明细只有 INFO 级：分红复权口径差 646,505 行、Marketstack 自身坏点 1,254 行、已知拆股口径 48 行。

## 设计

- 日报只用 FMP（加 Cboe VIX）和 FRED。
- `daily_default.v1` 保留原 profile 名，输入形状改为两种之一：
  - FMP-only：`secondary_prices_path=null`、`require_secondary_prices=false`。DATA-022 之后的日报都用这种。
  - 旧的 Marketstack 形状：DATA-022 之前写下的收据仍可验证。
- 收据里的 `require_secondary_prices` 和输入角色记录了用的是哪种形状。
- `validate-data` 在 daily 档不传第二源，即使 `data/raw` 里还有 Marketstack 文件。
- `validate-data` 在 manual 档会对账同目录下存在的 Marketstack 文件；raw close 冲突照旧判 FAIL。这就是"可选对账"。
- `aits download-data` 默认不下载 Marketstack。`--with-marketstack` 仍可用，需要 `MARKETSTACK_API_KEY`。
- score-daily 授权策略（`config/data_quality/arch_004_wave15_daily_score_consumer_authorization.yaml`）升到 1.1.0，角色改为 `[prices, rates]`。授权核对发布时只看收据里的角色，发布里多出的 Marketstack 产物会被忽略。
- capture policy 升到 `daily_input_capture_v7`，market_macro 改为 `market_macro_v5_fmp_only`（supersedes v4），只快照 prices、rates、manifest。
- 各命令里重复的第二源判断合并为 `ai_trading_system.data.secondary_prices.optional_secondary_prices_path`：文件存在就对账，不存在不报警。

## 改动清单

- 代码：
  - `cli_commands/data_cache.py`：下载默认 FMP-only；validate-data 的第二源选择。
  - `data/quality_execution.py`：daily 形状接受 FMP-only。
  - `data/quality_consumer_authorization.py`：发布角色核对。
  - `daily_input_capture.py`、`ops_daily.py`：capture、下载步骤所需环境变量、休市日覆盖判断。
  - `cli_commands/{score_daily,reports,backtest,feedback,trade_review,market_features}.py`、`research_framework/data_quality_gate.py`、`dynamic_strategy_growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution.py`：改用共享函数。
  - `reports/artifact_lineage.py`：日报数据族不再列 Marketstack。
  - 新文件 `data/secondary_prices.py`。
  - `cli_direct.py`（score_daily 授权）和 `cli_commands/ops.py`（周期 consumer 计划）核对的收据角色：第一次发布漏改，DATA-022-02 补上，见"进展"。
- 配置：
  - `config/data_sources.yaml`：Marketstack 改为 `inactive`；
  - `config/data_source_fallback_policy.yaml`：价格没有回退源；
  - `config/cache_catalog.yaml`：第二源缓存非必需；
  - `config/operations/daily_input_capture.yaml`；
  - 上面提到的授权策略。
- 脚本：`scripts/ops/daily_scheduler_run.ps1` 不再记录 `MARKETSTACK_API_KEY`。
- DQ 归因证据：`quality_execution.py` 的行号变了，DQ 归因就绪清单（`inputs/data_quality/dq_issue_attribution_readiness_inventory_v1*`）和利率归因复核包（`rate_issue_attribution_review_pack_v1*`）的内容 id 随之变化。
  - 处理按 2026-08-29 `a8676a9d` 的做法：用 `scripts/` 下两个脚本重新生成，再把新的 pack id 和 sha256 重新绑定到 `config/data_quality/rate_row_issue_attribution_decision_v1.yaml`、`contracts/rate_data_quality_attribution.py` 和对应测试。
  - 批准的 6 个 site 不变。
- 文档：`docs/system_flow.md` 第 2 节数据质量一行；`docs/data_source_subscription_inventory.md`；`docs/requirements/marketstack_second_price_source_2026-05-05.md` 顶部状态说明。
- 测试：
  - 授权：FMP-only 收据在发布带或不带 Marketstack 时都能授权；
  - DQ：daily 档 FMP-only 能跑通并验证，半套形状被拒；
  - CLI：daily 档忽略 Marketstack 文件，manual 档会对账；
  - 下载：默认不要 Marketstack key；
  - capture：只快照 3 个文件；
  - ops：休市日只看主源，下载步骤只要 `FMP_API_KEY`。

## 不改的

- 写死 `require_secondary_prices=True` 的已结束研究 runner：`research_restart`、`limited_non_pit_reconstruction`、first_layer 的 falsification 系列、`frozen_signal_value_confirmation`、o1、`foundation_consumer_migration`（ARCH-004 D0C 演练）。
  - 它们在新的 FMP-only 缓存上会 fail closed；以后的研究不再用它们。
- `historical_gap_recovery.py`（OPS-079）的冻结输入仍要 Marketstack，只用于补 2026-09-04 以前有 Marketstack 的日期。
- `quality.py` 的对账规则和 `config/data_quality.yaml` 的阈值不变。
- Marketstack 专用的命令不变：`aits data-sources yahoo-price-diagnostic`（Marketstack 坏行的 Yahoo 复查）和 `aits data source-qualification marketstack-*`。

## 副作用

- 第一次 FMP-only 下载会按发布机制设计，删掉兼容副本 `data/raw/prices_marketstack_daily.csv`（`download_publication.py` 的 legacy projection）。
- 历史保留在 `data/raw/.download_publications/members/` 里。2026-10-12 核对运行副本，有 30 份 Marketstack 快照。

## 验收

1. PR 测试套件通过（C 区门禁），commit 带 `Owner-Decision: owner_decision:DATA-022:2026-10-12:fmp_only_secondary_optional`。
2. FMP 订阅和 `FMP_API_KEY` 恢复后，第一个交易日的计划任务满足：
   - 在没有 `MARKETSTACK_API_KEY` 的情况下全链 PASS；
   - DQ 收据角色为 prices、rates；
   - score-daily 授权通过。
3. 之后可以删除环境变量 `MARKETSTACK_API_KEY`，退订 Marketstack。

## 进展

- 2026-10-12：方案和实现由 Claude 在云端完成并跑测试，交给本机 pi 应用、跑 ship 门禁并发布。真实日报验收等 FMP 恢复。production_effect=none；broker_action=none。
- 2026-10-13：发布后第一次真实日报（2026-10-12 17:30 JST，运行副本 `a35f20956`，as_of 2026-10-09）capture 和 validate-data 都 PASS，DQ 收据角色是 prices、rates；但 score_daily 授权失败：`DQ_INPUT_SET_MISMATCH: expected=['prices', 'rates', 'secondary_prices'] actual=['prices', 'rates']`，后面 13 步 BLOCKED，周期 consumer 计划也因同一原因 BLOCKED。
  - 原因：`cli_direct.py` 的 score_daily 授权（3 处）和 `cli_commands/ops.py` 的 `_DAILY_DEFAULT_DATA_QUALITY_INPUT_ROLES` 仍写死三种角色；第一次发布只改了授权策略。已有测试用的收据核对替身不看角色，所以没测出来。
  - DATA-022-02：score_daily 授权改为读已审定授权策略的 `required_input_roles`；ops 的常量改为 prices、rates；测试核对两者一致，并核对 score_daily 授权向收据核对传的角色。
  - `data/foundation_consumer_migration.py` 也写死三种角色，但它只用于 ARCH-004 D0C 演练脚本，不在日报链上，不改（见"不改的"）。
