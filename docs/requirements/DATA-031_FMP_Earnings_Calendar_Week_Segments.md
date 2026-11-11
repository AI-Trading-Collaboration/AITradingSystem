# DATA-031：FMP 财报日历按周分段取

状态：DONE（2026-11-10）。`production_effect=none`；`broker_action=none`。

关联：DATA-003（FMP 估值和预期按时点留档）、DATA-024（可变数据源缓存 12 小时有效期，`earnings-calendar` 在列）、DATA-030（上一次换包）、OPS-086（日报发布包）、TRADING-2565（研究自动循环第 7 期章程 `research/_loop/charter_p7.md` 第 3.1 题、第 5.1 节）；任务记录 `tasks/DATA-031.yaml`。

## 1. Owner decision

- `owner_decision:TRADING-2565:2026-11-01:research_loop_p7`：Baker 2026-11-01 15:11 JST 在「研究循环 10-30」线程签第 7 期章程，选「照推荐签,现在起四周」，第 3.1 题照推荐「现在修，第 7 期第一件做」。章程写明这是日报的生产采集，签字就是批这次改动，发布以后换一次日报发布包；也写明规矩变一条（第 4 节最后一条）。签字以前循环线程在同一线程把这条告诉了 Baker。
- 本卡 B 区，发布不要 Baker 批准；换包另要 Baker 的授权句（第 8 节）。

## 2. 背景

FMP `stable/earnings-calendar` 一次最多回 4000 行（推断：缓存里 9 份原始响应都正好 4000 行，没查文档）。超过时留下的是日期远的行，截掉的是近的：9 份响应最早的日期都在 11-06 到 11-10，2026 年十月下旬的财报高峰整段没采到，GOOG、INTC、MSFT 在 10-19、10-23 两次采集里都是空的。

代码核对（main `a86c96b4`）：

- `fetch_fmp_forward_pit_snapshots` 的日历窗口是 `as_of` 前 7 天到后 90 天，两端都含，共 98 天（`earnings_calendar_lookback_days=7`、`earnings_calendar_forward_days=90`）。
- `_fetch_earnings_calendar_records` 对整个窗口只调一次 `provider.fetch_earnings_calendar(from_date, to_date)`；出错时记一条 ERROR `fmp_forward_pit_earnings_calendar_failed`，返回空。之后 `_calendar_records_for_symbol` 按股票分到每个 payload。全仓库只有这一处调 `fetch_earnings_calendar`。
- 只要有一条 ERROR，报告就是 FAIL：`fetch-fmp-forward` 只写抓取报告，原始 payload、标准化 CSV、manifest 一个都不写（`cli_commands/pit_snapshots.py`）；组件记 PARTIAL_CAPTURE，投影那一步再把 PIT 下游停掉。这个来源 `HISTORICAL_RECAPTURE_FORBIDDEN`，那一天只能记成缺口。所以今天日历一出错，丢的是 17 只观察名单股票整天的 FMP PIT，不止丢日历。日历只有研究在读，评分不读它。
- 原始 payload 的 `request_parameters_by_endpoint["earnings-calendar"]` 写 `from`、`to`（整个窗口）和 `filtered_symbol`。重放（`_fmp_forward_pit_report_from_retained_payloads`）要求一批 payload 的 `from`、`to` 各只有一个值，不带问题清单，所以重放出的报告永远是 PASS。加载（`load_fmp_forward_pit_raw_payloads`）用 `rglob("*.json")` 读整个目录，只核端点的键集合，不核参数里的键。
- 上线以前的存档永远是截断的，而且分不出截没截：每股的 payload 只留自己那几行，4000 这个总数不在里面。

## 3. 做法（都在 `src/ai_trading_system/fmp_forward_pit.py`）

1. **切段。** `_fetch_earnings_calendar_records` 把 `[from_date, to_date]`（两端都含）切成连续、不重叠的段，每段 7 天（`FMP_EARNINGS_CALENDAR_SEGMENT_DAYS = 7`），最后一段到 `to_date` 为止。默认窗口 98 天正好 14 段。`from_date > to_date` 时报 `ValueError`（公开入口走不到，只作防线）。
2. **到上限再切。** 一段返回的行数 ≥ `FMP_EARNINGS_CALENDAR_ROW_CAP = 4000` 时，这一段的行全不要，对半切成 `[a, a + (b − a) // 2]` 和其后到 `b`，两半各请求一次，照样递归，先左半后右半。一天的段还 ≥ 4000 行时，保留这一天返回的行，这一段在 `segments` 里加 `"truncated": true`，记一条 **WARNING** `fmp_forward_pit_earnings_calendar_truncated`（说明里写日期和行数），接着请求后面的段。
3. **一段出错。** 记一条 **WARNING** `fmp_forward_pit_earnings_calendar_failed`，说明里写这一段的起止日期和脱敏后的错误（`_sanitize_fmp_error_message`）；这一段在 `segments` 里写 `"rows": null, "error": true`；接着请求后面的段。每段的 try 只包 `provider.fetch_earnings_calendar` 那一次调用：切段、合并、去重里的代码错误照常抛出，由外层记成 ERROR，不能变成这一条警告。
4. **请求上限。** 一次采集的日历请求最多 `FMP_EARNINGS_CALENDAR_MAX_REQUESTS = 60` 次，被再切掉的父段也算在内。
   - 先留够预算再切：一段满 4000 行，只有在「已请求的次数（含这一次）+ 2 + 还在排队的段数 ≤ 60」时才对半切，这样两半和每个排队的段都至少能请求一次。
   - 切不了就留着：不满足时这一段不切，按第 2 条当截断的叶子收下（行留着，标 `"truncated": true`，记一条 truncated 的 WARNING）。请求过的段，行不会因为上限丢掉。
   - 只有窗口长到初始段数就超过 60（420 天以上）时，超出的段才不请求，每段在 `segments` 里写 `"rows": null, "skipped": true`，按原来的大小、按日期排。
   - 有段因为预算没切，或者有段被跳过，另记一条 **WARNING** `fmp_forward_pit_earnings_calendar_request_cap`，说明里写几段没再切、几段没请求（从哪天到哪天）。
   - 正常一次 14 次请求，高峰周再切一两次是 16–18 次。上限防的是 4000 认错时请求数失控（每段都切到一天约 180 次）。默认窗口每天都满 4000 时正好切 23 次、请求 60 次、留 37 段。
5. **全都没拿到。** 叶子段全是 `error` 或 `skipped` 时，另记一条 **ERROR** `fmp_forward_pit_earnings_calendar_unavailable`，状态 FAIL，和今天整窗请求出错一样。只有这种情况才让日历停掉当天的 PIT。
6. **合并。** 按段的先后把行接起来。前面某一段已经出现过的行（按 `_json_checksum` 算同一个规范 JSON），后面的段里再出现就不要；同一段里 FMP 自己回的重复行照原样留着。不排序，也不按日期去掉段外的行（FMP 的起止是不是两端都含没查文档），按股票分的规则不变。
7. **记下切了哪几段。** `request_parameters_by_endpoint["earnings-calendar"]` 的 `from`、`to`、`filtered_symbol` 照旧（`from`、`to` 仍是整个窗口），另加两个键：`segments`（叶子段，按请求先后排，每个是 `{"from": "YYYY-MM-DD", "to": "YYYY-MM-DD", "rows": 返回行数}`，出错、截断、跳过的段按第 2–4 条加标记）和 `row_cap`（4000）。同一批的每个 payload 写同一份。
8. **问题从段里来。** 第 2–5 条的问题由同一个函数按 `segments` 生成，采集和重放共用，所以两边的问题清单（严重程度、代码、哪几段）一样。采集时出错那段的说明带脱敏后的错误；重放时没有错误原文，说明写原因见当天 capture 目录里的抓取报告。这样走 WARNING 那条路时，投影写出的正式报告（`pipeline_health` 读的那份）也是 PASS_WITH_WARNINGS，并列出那几段。其他端点的问题在重放时不带，这是现在就有的，不在这次改。
9. **报告。** `render_fmp_forward_pit_fetch_report` 在「请求参数」那一行后面加一行 `- earnings-calendar 分段：N 段，单段最多 M 行（上限 4000），再切 K 次`：N 是叶子段数（出错、跳过的都算），M 是有行数的段里最多的那个，K = N − ⌈天数 / 7⌉。`FmpForwardPitFetchReport` 加三个字段 `earnings_calendar_segment_count`、`earnings_calendar_max_segment_rows`、`earnings_calendar_resplit_count`，默认都是 0；`attach_fmp_forward_pit_report_artifacts` 把它们带过去。重放时有 `segments` 就照它算；没有 `segments`（旧存档），或者根本没请求日历，这一行印 `- earnings-calendar 分段：无记录（旧存档，或没有请求日历）`。
10. **批次核对。** 重放的同批核对加上 `segments`（按规范 JSON 比）和 `row_cap`：整批各只能有一个值，否则照现在的 `FMP forward PIT retained payload batch metadata mismatch` 报错。新旧存档混在一批里也报这个错。

## 4. 不改的

- 其他六个端点、股票名单（`config/universe.yaml` 的 `ai_chain.core_watchlist` 17 只）、窗口的前 7 天和后 90 天、`FMP_FORWARD_PIT_NORMALIZATION_VERSION`（`fmp_forward_pit_v1`）、标准化的列、`available_time` 的口径、评分。
- `daily_input_capture.py`、采集命令的参数、`config/operations/daily_input_capture.yaml` 里 fmp_forward_pit 的 `source_revision`（`daily_input_capture_v3`）：动了它们，幂等键或旧的 PASS 复用就会变，这个来源又禁止事后补采。
- HTTP 层（`valuation_sources.FmpHttpValuationProvider.fetch_earnings_calendar`、请求缓存、重试）。每段是一次独立请求，缓存照参数分开；窗口每天挪一天，所以每段每天都是新键，缓存里日历的条目从每天 1 份变成约 14 份。429/5xx 缓存 300 秒、重试读到缓存里的旧错，是 DATA-030 需求第 7 节记下的同类问题，不在这次修。
- 研究数据、日报的其他步骤、券商。只改采集的完整性，不改任何评分或仓位。
- 规矩变了一条：今天「日历一出错，整天 PIT 停」；改完以后部分段出错、一天截断、请求到上限都只记警告，只有全部拿不到才停。日报停的日子只会变少。

## 5. 读的规矩

- `segments` 里 `error` 或 `skipped` 的段算「缺」，不能算「那几天没有财报」。
- `truncated` 的段算「可能不全」。
- 上线以前的存档：近 3–4 周可能缺，从存档里分不出来。

## 6. 名单以外的股票（不在这张卡）

日历合并以后只给 17 只观察名单的股票留行。章程第 3.4 题要的「前 5 大权重股」几乎一定有 AAPL（推断），它不在 17 只里，本卡做完也拿不到。全窗口日历要另存一份文件，不能放进 pit raw 目录（加载用 `rglob` 读整个目录），放在别处就要动 `daily_input_capture.py` 的路径、expected 和 source-owned 两组路径，碰旧日子的 PASS 复用，所以另开 DATA-032（B 区）在本卡之后写需求。DATA-032 能在本卡发布后一周内发布，就两张卡一起换一次包；赶不上就本卡先换。两张都要在 2027-01-15 以前上线，目标 2027-01-08。

## 7. 验收测试

新文件 `tests/test_fmp_forward_pit_calendar_segments.py`（先写，pi 写代码）。假 provider 按日期从合成的日历表里取 `from ≤ date ≤ to` 的行，超过 4000 行时照 FMP 的样子只回日期最晚的 4000 行，可以让某一段抛错、每天造满行、多回段后一天的行。覆盖：默认 14 段；过 4000 而每周不到时一行不少；一周满 4000 再切；一天满 4000 是一条 truncated WARNING、行留着；一段出错是一条 failed WARNING、别的段照收；全部出错是 ERROR；每天都满时请求 60 次、切 23 次、留 37 段、行都留着；441 天的窗口跳过最后 3 段；合并时的代码错误不变成警告；只去跨段的重复；原始 payload 的参数；新存档重放的分段数和问题清单；旧存档重放印「无记录」；新旧混批报 mismatch；报告和 attach 的三个数；短窗口；CLI 一段出错时退出 0 并写原始 payload。

`tests/test_fmp_forward_pit.py` 的 `_FakeFmpForwardPitProvider.fetch_earnings_calendar` 原来断言只被叫一次、参数是 2026-04-25 到 2026-07-31；改成断言每次的段不超过 7 天，并按日期只返回段内的行。原来的断言在 as_of 2026-05-04 那个测试里会抛错，被当成一条 ERROR 吞掉了，一直没人发现。

## 8. 发布和上线

- 发布：B 区一批（代码、两个测试文件、任务卡、本文件、路线图），pi 写代码，Claude 审，pi 队列发布，不在日报窗口里（11:50–13:00、17:20–18:30 JST）。
- 换包：照 DATA-030（2026-10-30）那次换一次日报发布包。叫 Baker 时只叫一次，写清哪一步在等他、他点了以后马上跑什么、日报停的日子只会变少，并请他在 FMP 控制台看一眼套餐的每分钟、每天请求上限（改完每次采集是 102 + 14 + 2K 次，K 是再切次数，一般 0，高峰周 1–2；日历最多 60 次）。
- 第一天核对：换包后第一次日报跑完，在电脑上只读核一次：当天原始 payload 里 `segments` 约 14 段，没有一段 `rows` ≥ 4000，没有 truncated、failed、request_cap、unavailable；挑两家下一季财报在窗口里的公司（例如 MSFT、META），日历里有它们的日期。
- 回退：第一天要是因为日历或这次的代码 FAIL，当天请 Baker 换回上一个包。

## 9. 进展

- 2026-11-01：需求和验收测试写好，DATA-031-01 交 pi；16:21 JST 发布到 main `ad4f11fc`。
- 2026-11-04：换包并进 DATA-033 的发布包，叫 Baker 一次。
- 2026-11-07：发布包 `20261103-ad63dc839` 23:00 JST 换上（电脑 11-04 到 11-07 离线，比原定晚了三天）。
- 2026-11-10：换包后第一个生产日（as_of 2026-11-09）只读核对通过：14 段，单段最多 2603 行，没有再切，没有 truncated、failed、request_cap、unavailable；MSFT、META 下一季的日期 2027-01-27 在日历里。当天日报停在 `validate_data`（SOXX 拆股，另有修复），和日历代码无关，没有回退。卡关 DONE。
