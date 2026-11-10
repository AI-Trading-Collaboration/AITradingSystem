# DATA-034：拆股后整段重取价格历史，不再停日报或写坏历史

任务卡：`tasks/DATA-034.yaml`。分两批：DATA-034-01（B 区，下载核对和整段重取）、DATA-034-02（C 区，登记 SOXX 2026-11-05 拆股和随之重绑的哈希）。

production_effect=none；broker_action=none。

## 1. Owner decision

- `owner_decision:DATA-034:2026-11-10:split_history_refetch`：Baker 2026-11-10 21:19 JST（12:19:47Z）在线程「修复 SOXX 拆股导致日报停写」的卡片「SOXX 拆股的修法选哪个?」上选「自动重取历史」（推荐项），选项原话：「登记这次拆股;下载时核对最近20天复权价,对不上就整段重取该标的;11-11日报自动换掉SOXX历史」。另两个选项没有选：「重取+拆股日历确认」（再加一批，用 FMP 拆股日历确认过的新拆股不再停日报，属于放宽数据质量规则）和「只修这一次」（登记拆股，另加一次性重取命令在电脑上对 prod 跑一次）。第 3 节两件事都按这个决定做；DATA-034-02 的决定记录由 Baker 运行 approve.ps1 写下。

## 2. 背景

- 2026-11-10 12:00 JST 的日报（as_of 2026-11-09，发布包 20261103-ad63dc839）停在 `validate_data`：`prices_extreme_adj_close_move` ERROR，SOXX 2026-11-04 adj_close 150.14，单日 -67.7%；另有 `prices_adjustment_ratio_jump` WARNING（SOXX 11-04 比例 0.3333，11-05 回到 1.0）。不是新代码引起的，回滚没用。诊断：`knowledge/claude/runs/DATA-033-D1/rc-reports/dq-2026-11-09.md`（项目文件）。
- SOXX 三拆一：iShares 的 Form 497 补充说明写明登记日 2026-11-03、2026-11-05 起按拆后股数交易。
- FMP 主源的 `close` 来自 non-split-adjusted 端点（原始收盘），`adj_close` 来自 dividend-adjusted 端点（拆股和分红都复权）。日报的价格刷新只补尾部（`download.py` `_price_fetch_windows`：已有缓存的标的从最后一个缓存日的下一个交易日取起）。PC 11-04 至 11-07 离线，11-10 一次取了 11-04 至 11-09：这几行已按拆后口径复权，11-03 及以前的缓存行还是拆前口径，于是 adj_close 在 11-04 断了一截。
- 潜在问题：只补尾部时，如果新取的第一行正好是除权日，旧行拆前口径、新行拆后口径，adj_close 掉一截，但 adj_close/close 的比例不变（旧行取的时候还没复权，新行本来就是 1），复权比例跳变检查不报；3 拆 2（-33%）低于 50% 的极端阈值，只记 INFO，数据质量照样 PASS，历史里留下假的一天下跌。2 拆 1 正好在阈值上下。
- `config/data_quality.yaml` 的 `known_split_events` 只把匹配的复权比例跳变降为 INFO；adj_close 的极端单日波动没有拆股豁免（这是对的：复权收盘不该跳）。所以只登记拆股解决不了 11-10 的停写，缓存里的 SOXX 历史必须按新口径重取。
- FMP Starter 一次请求的日期范围有限（OPS-080 需求第 3 节写的是 5 年历史；2026-11-03 在电脑上取四只标的十年历史时分 3 段取）。

## 3. 做法

### 3.1 DATA-034-01：主源补尾部时核对缓存口径，对不上就整段重取（B 区）

- `download.py` 新的公开模块常量：`PRICE_HISTORY_REBASE_OVERLAP_ROWS = 20`、`PRICE_HISTORY_REBASE_ADJ_CLOSE_REL_TOLERANCE = 0.10`、`PRICE_HISTORY_REBASE_CHUNK_CALENDAR_DAYS = 1200`、`PRICE_HISTORY_REBASE_COVERAGE_SHORTFALL = "PRICE_HISTORY_REBASE_COVERAGE_SHORTFALL"`。
- 只对主价格源（`download_daily_data` 里的 primary 下载）打开核对；Cboe VIX 后备和 Marketstack 第二源照旧只补尾部。
- 取数窗口：已有缓存、最后一个缓存日早于结束日的标的，从它倒数第 20 个缓存日起取（缓存不足 20 行就从第一个缓存日起），到结束日；最后一个缓存日已到结束日的标的照旧不取；没有缓存的标的照旧。`_price_fetch_windows` 加参数 `overlap_rows: int = 0`（0 = 现在的行为，负数报 `ValueError`）。
- 核对：重读到的、缓存里也有的日期（不晚于该标的最后一个缓存日）逐行比 adj_close（两边都是正数才比）：`abs(新/旧 - 1) > 0.10` 的有一行，就认为这个标的的缓存口径过时。分红复权（通常不到 2%）不触发；拆股（3 拆 2 是 33%，5 拆 4 是 20%，合股也一样）触发。
- 没过时：重读的旧日期行丢掉，缓存行原样保留（不改近期历史），只加新日期的行。
- 过时：同一次运行里把该标的从它第一个缓存日到结束日整段重取，按 1200 个日历日一段分开请求；缓存里它的所有行换成重取的行（下载发布里这些行归到主源的 live 事件）。重取的结果必须包含该标的每一个缓存日期，少一个就抛 `ValueError`，消息以 `PRICE_HISTORY_REBASE_COVERAGE_SHORTFALL` 开头，写明标的、起点、缺几天和前三个日期；什么都不发布（当前指针不变），日报停在 download_data，原因清楚。
- 记录：下载发布 live 事件的 `request_parameters.incremental_refresh` 加 `history_rebase_check`（`overlap_rows`、`adj_close_rel_tolerance`）和 `history_rebases`（每个重取的标的一条：`ticker`、`check_date`（差得最多的那一行，一样多取最早的）、`cached_adj_close`、`provider_adj_close`、`refetch_start`、`refetch_end`、`replaced_row_count`、`refetched_row_count`）；`fetch_windows` 包括重取的各段。`DataDownloadSummary.price_history_rebases` 带同样的内容，`aits download-data` 每个重取的标的打印一行 `价格历史整段重取（DATA-034）：<ticker> check_date=… cached_adj_close=… provider_adj_close=… refetch=<起>..<止> replaced_rows=… refetched_rows=…`。
- 11-11 的日报（as_of 11-10）在新发布包上：SOXX 最后 20 个缓存行里有 11-03 及以前的拆前行，核对不上，SOXX 从 2018-01-02 整段重取（3 段），不用另外在电脑上改 prod 数据。

### 3.2 DATA-034-02：登记 SOXX 2026-11-05 拆股（C 区）

- `config/data_quality.yaml` 的 `known_split_events.SOXX` 在 2024-03-07 那条后面加 2026-11-05、ratio 3、REVIEWED、data_platform_owner、reviewed_at 2026-11-10，来源是 iShares Trust 的 Form 497 补充说明。阈值、窗口、profile、policy_version 都不改。
- 文件字节变了，照 OPS-080-02（GOOGL）的做法重绑：重建利率归因复核包（json、validation、md），`config/data_quality/rate_row_issue_attribution_decision_v1.yaml` 的 pack_id 和 sha256、`contracts/rate_data_quality_attribution.py` 的 pack id 常量和它的测试、`config/data_quality/regime_label_generator_dependency_v1.yaml` 的政策哈希。`tests/test_ops080_googl_known_split.py` 的拆股总数 12 改 13。
- 重取以后 SOXX 的复权比例在 2026-11-05 跳 3 倍，登记后是 INFO；不登记就是 WARNING，`score_daily` 拒收。

## 4. 不变的

- 数据质量的阈值、拆股匹配窗口和容差、profile、policy_version；adj_close 的极端波动照旧是 ERROR。
- 下载发布的结构、校验和行级来源绑定；DQ 之后不改输入（OPS-080）。
- 第二源和 VIX 后备的取数窗口；新标的的起点（含 OPS-080 的 1600 天）。
- 分红复权：缓存的 adj_close 不跟着以后的分红重算（差不到 10%），本任务不处理。

## 5. 验收

- DATA-034-01：`tests/test_data034_split_history_refetch.py`（窗口、整段重取、11-10 缓存形状在下一次运行自愈、只有拆股量级才重取、记录差得最多的一行、1200 天分段、缺日期就停、CLI 打印）；`tests/test_data_download.py` 里 5 处取数窗口的断言改成重读后的起点（第二源不变）。
- DATA-034-02：`tests/test_data034_soxx_known_split.py`（登记内容、阈值不变、重取后 SOXX 只有 INFO、口径断开的历史仍是 ERROR）；复核包新鲜度、决定绑定和 regime label 依赖的已有测试通过。
- 两批发布后打新发布包（从包含 ad63dc83 的 main 打），2026-11-11 12:00 JST 以前部署；当天日报的 download-data 打印 SOXX 那一行，validate_data PASS，score_daily 跑到。

## 6. 没有解决的

- 以后再有拆股：历史会自动整段重取，不会写坏；但没登记的拆股仍会让复权比例跳变成为 WARNING，日报停到登记的新包上线为止。用 FMP 拆股日历自动确认这一步要放宽数据质量规则，Baker 没有选，以后另议。
- as_of 2026-11-09 没有成功的日报，怎么记（照以往记为缺口）在收尾批里问 Baker。
- 缓存的 adj_close 不随以后的分红重算（第 4 节）。

## 7. 进展

- 2026-11-10：Baker 选「自动重取历史」；DATA-034-01 写需求和验收测试，pi 写代码。
