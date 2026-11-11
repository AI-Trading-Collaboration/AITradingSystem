# DATA-032：每天存一份全窗口的 FMP 财报日历

状态：DONE（2026-11-10）。`production_effect=none`；`broker_action=none`。

关联：DATA-031（财报日历按周分段取，main `ad4f11fc`）、DATA-003（FMP 估值和预期按时点留档）、OPS-086（日报发布包）、TRADING-2565（研究自动循环第 7 期章程 `research/_loop/charter_p7.md` 第 3.4 题、第 5.1 节）；任务记录 `tasks/DATA-032.yaml`。

## 1. Owner decision

- `owner_decision:TRADING-2565:2026-11-01:research_loop_p7`：Baker 2026-11-01 15:11 JST 签第 7 期章程，选「照推荐签,现在起四周」。第 5.1 节写明日历只给 17 只观察名单留行，第 3.4 题要的 AAPL 不在里面，另开一张卡（本卡）每天存一份全窗口的合并日历，能赶上就和 DATA-031 一起换一次包。
- 本卡 B 区，发布不要 Baker 批准；换包另要 Baker 的授权句。

## 2. 背景

`fetch_fmp_forward_pit_snapshots` 合并完整个窗口的日历以后，`_calendar_records_for_symbol` 只把 17 只观察名单（`config/universe.yaml` 的 `ai_chain.core_watchlist`）各自的行放进每股的 payload，别的行丢掉。第 3.4 题的「QQQ 前 5 大权重股」几乎一定有 AAPL（推断），拿不到日期。

代码核对（main `ad4f11fc`）：

- 每股 payload 的加载 `load_fmp_forward_pit_raw_payloads` 和 PIT manifest 的发现（`pit_snapshots.py` 的 `_discover_raw_json_records`）都只读 raw 目录里的 `*.json`。raw 目录里放一个不以 `.json` 结尾的文件，这两处都看不见它。
- 日报采集对 fmp_forward_pit 的 `source_owned_paths` 包括整个 `pit_raw_dir`，`_artifact_records` 用 `rglob("*")` 给目录里每个文件记 sha256 和大小。新文件自动进当天的承诺，不用改 `daily_input_capture.py` 的路径、expected、source-owned，也不用加命令参数，幂等键和 `source_revision` 都不变。
- 反过来也一样严：`_reusable_pass_component_result` 拿目录现在的文件清单和 state 记的比，多一个、少一个、大小或 sha256 变了，那天的 PASS 就不复用，报 `SOURCE_STATE_INVALID`（terminal PASS source state artifact drift）。所以这个目录写完以后只能读（第 5 节）。
- 密钥扫描按后缀挑文件（`secret_hygiene.py` 的 `SCANNED_SUFFIXES`，没有 `.gz`），不扫它。
- `fetch-fmp-forward` 只在抓取报告不是 FAIL 时写 raw payload、标准化 CSV 和报告。

## 3. 做法（`src/ai_trading_system/fmp_forward_pit.py`、`src/ai_trading_system/cli_commands/pit_snapshots.py`）

1. **报告带上合并后的日历。** `FmpForwardPitFetchReport` 加 `earnings_calendar_records`（合并、去重后的全部行，DATA-031 的规则，不排序，不按名单筛），`attach_fmp_forward_pit_report_artifacts` 带过去，重放不填。
2. **写一个文件。** `write_fmp_earnings_calendar_full(report, output_dir) -> Path`：放在 raw 目录的根上，名字 `fmp_earnings_calendar_full_<downloaded_at 的 timestamp token>.jsonl.gz`。内容是 `gzip.compress(data, mtime=0)` 压缩的 UTF-8 JSON Lines：第一行是头 `{"schema": "fmp_earnings_calendar_full.v1", "as_of", "captured_at", "downloaded_at", "provider", "endpoint": "earnings-calendar", "from", "to", "segments", "row_cap", "row_count", "rows_sha256"}`（`segments`、`row_cap` 和每股 payload 里那份一样）；后面每行一条记录。每一行都是 `json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))` 加一个 `\n`（最后一条也跟）。`rows_sha256` 是头那一行之后全部字节的 sha256。日历是空的也写，`row_count` 是 0。原子写。
3. **CLI 写它，写失败只记警告。** `fetch-fmp-forward` 在写 raw payload、attach 之后，调 `attach_fmp_earnings_calendar_full(report, raw_output_dir)`：它调第 2 条写文件，成功时把路径（相对项目根显示）、行数、「已写」填进报告；任何异常都只记一条 **WARNING** `fmp_forward_pit_earnings_calendar_full_write_failed`（「当天全窗口日历算缺」），状态 PASS_WITH_WARNINGS，raw payload、标准化 CSV、报告照写。不走记 ERROR 的 `fmp_forward_pit_artifact_stage_failed`：一个研究用的文件写不出来，不能让整天的 PIT 丢掉，日报停的日子只会变少。
4. **报告一行。** 在 DATA-031 的分段那一行后面：写了文件印 `- earnings-calendar 全窗口：N 行，<路径>`；写失败印 `- earnings-calendar 全窗口：写失败，当天算缺`。投影（`project_fmp_forward_pit_capture`）不读这个文件，只数 `raw_input_dir` 根上有几份 `fmp_earnings_calendar_full_*.jsonl.gz`：1 份印 `- earnings-calendar 全窗口：投影不读，当天 capture 的 raw 目录里有 1 份`，0 份印「……里没有，算缺」，多于 1 份印「……里有 N 份，算缺」；不加问题，状态不变。日报留下的正式报告是投影写的那份，所以写失败的日子从这一行看得出来。
5. **读它的函数。** `load_fmp_earnings_calendar_full(path) -> (头, 行)`，只读、不解压到盘上；`schema` 不对、`as_of`/`from`/`to` 不是日期、某行不是对象、`row_count` 和行数不一致、`rows_sha256` 对不上、最后没有换行，都报 `ValueError`。研究那两条线（第 3.4 题）用它读；本卡不接研究代码。

## 4. 不改的

`daily_input_capture.py`、采集命令的参数、`config/operations/daily_input_capture.yaml` 的 `source_revision`、每股 payload 的格式和文件名、标准化 CSV、PIT manifest、重放的问题清单、评分、HTTP 层、观察名单。

## 5. 读的规矩

- **只读。** 研究用 `load_fmp_earnings_calendar_full` 原地读，或者先复制到别处再读；输出写到别处。不在原地解压，不在旁边放别的文件，不用文件管理器去翻 capture 目录（`.DS_Store`、`Thumbs.db`、`desktop.ini` 这类文件一出现，那天的复用就作废）。
- **不许给旧日子补写。** 不能拿请求缓存里以前的整窗响应补出上线以前的全窗口日历放回当天的 raw 目录：一放进去那天的 PASS 就作废，补出来的也不是点时的、是截断的。真要补，放进研究数据目录，标明「非点时、截断的」。
- **一个目录正好一份。** 多于一份算缺，并报出来。没有文件有三种意思：上线以前、那天 FAIL、写失败，都算缺，不能当「没有财报」。
- 照 DATA-031 第 5 节：头里 `segments` 有 `error`、`skipped` 的段算缺，`truncated` 的段算可能不全。
- GOOG 和 GOOGL：FMP 日历里可能只列一个；上线第一天核对时记下头 5 大每一只在文件里叫什么，第 3.4 题把两个代码当一家。

## 6. 大小（推断，上线第一天核）

98 天窗口合并后估计 8,000–15,000 行，gzip 以后约 0.2–0.5 MB，一年约 50–125 MB。一年超过 1 GB 再按 STORAGE-001 处理。

## 7. 验收测试

新文件 `tests/test_fmp_earnings_calendar_full.py`（先写，pi 写代码）：名单外的股票在文件里、每股 payload 不变；头的键和 `segments`、行的写法和 `rows_sha256`；同样的报告写两次字节相同；改一行、删一行、`as_of` 不是日期、某行不是对象、`row_count` 不对都读不进来；空日历写 0 行；attach 以后字段还在；CLI 写出正好一份、payload 加载和 PIT manifest 看不见它、投影照样逐字节通过；日历全部出错时什么都不写；一段出错时头里那段标 `error`；空日历 CLI 也写；写失败是 WARNING、raw 照写、目录里没有这个文件；解压以后没有 API key；投影那一行在 1 份、2 份、没有三种情况下各对，别的行不变。

`tests/test_daily_input_capture.py` 加 4 条，钉住复用：旧 state 不带这个文件照样复用；新 state 带这个文件复用、artifacts 里有它；新 state 之后删掉或改掉它是 `SOURCE_STATE_INVALID`；旧 state 之后往目录里加一个是 `SOURCE_STATE_INVALID`。

## 8. 发布和上线

- B 区一批（DATA-032-03；DATA-032-01 的 brief 漏了 `docs/system_flow.md`，DATA-032-02 的 brief 把 DATA-031 的私有函数名 `_fetch_earnings_calendar_records` 算成了前提不满足，pi 两次都照 AGENTS.md 停下，没有改动），pi 写代码，Claude 审，pi 队列发布，不在日报窗口里（11:50–13:00、17:20–18:30 JST）。`docs/system_flow.md` 在 FMP forward PIT 那一节加一段，写 DATA-031 的分段和这份文件。
- 和 DATA-031 一起换一次日报发布包，叫 Baker 一次（哪一步在等他、他点了以后马上跑什么、日报停的日子只会变少、FMP 套餐的请求上限）。
- 第一天核对（只读，复制出去再读或用读的函数）：文件在、正好一份、头能读、`row_count` 是几千到一万多；AAPL 和头 5 大另外几只在里面，记下 GOOG/GOOGL 叫什么；记下文件大小。
- 回退：第一天要是因为这个文件 FAIL，当天请 Baker 换回上一个包。两张卡都要在 2027-01-15 以前上线，目标 2027-01-08。

## 9. 进展

- 2026-11-01：DATA-032-03 19:02 JST 发布到 main `62a93062`（-01、-02 见第 8 节）。
- 2026-11-07：和 DATA-031 一起随发布包 `20261103-ad63dc839` 23:00 JST 换上。
- 2026-11-10：第一个生产日（as_of 2026-11-09）只读核对通过：raw 目录里正好一份文件，头、`row_count`、`rows_sha256` 对得上，8878 行，124,783 字节（第 6 节估计 0.2–0.5 MB，实际约一半），窗口 2026-11-02..2027-02-07，没有出错、跳过或截断的段；AAPL 2026-11-02 和 2027-02-04，MSFT、META 2027-01-27，GOOG、GOOGL 两个代码都在（2027-02-03）。卡关 DONE。
