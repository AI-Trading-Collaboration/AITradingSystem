# DATA-033：官方源单个网址断线只记该来源缺口，不再丢掉整天日报

状态：IN_PROGRESS（2026-11-04）。`production_effect=none`；`broker_action=none`。

关联：OPS-072（官方源请求级重试）、DATA-027（读断重试）、DATA-028 / DATA-030（缓存协调层不再挡重试）、OPS-085（17:30 只补行情）、OPS-086（日报发布包）、RISK-017（同一发布包首次生产运行）；任务记录 `tasks/DATA-033.yaml`。

## 1. Owner decision

- `owner_decision:DATA-033:2026-11-04:fix_before_count`：Baker 2026-11-04 00:13 JST 在「收尾工作总览第 1 版并评估日报稳定性」线程的卡片上选「先修再数」：先修“一个官方网址断线就丢一天日报”，换发布包后再数连续 5 个交易日。
- `owner_decision:DATA-033:2026-11-04:official_source_gap_not_day_loss`：Baker 2026-11-04 01:23 JST 在本任务线程的卡片上选「只记该来源缺口」：单个官方网址在重试后仍不通时，当天日报照出，报告写明缺哪一家；“只抓一次、事后不补抓”的 PIT 规则不变；同一天 3 个以上来源不通或有解析错误仍整天停。另两个选项（17:30 当天重抓官方源，或两者都做）要改 PIT 规则，没有选。
- `owner_decision:DATA-033:2026-11-04:as_of_2026_11_02_gap`：Baker 2026-11-04 01:23 JST 在本任务线程的卡片上选「记为缺口」：as_of 2026-11-02 没有成功的日报，照 2026-10-13、10-27、10-30 的做法记成已知缺口，不做事后补抓或补算（现在补抓拿到的是美股 11-03 盘中的数据）。

## 2. 背景

2026-11-03 12:00 JST 的日报（as_of 2026-11-02，发布包 `20261031-e872a7a98`，RISK-017 换包后的第一次正式运行）整天没出：

- `official_policy_sources` FAIL（`PROVIDER_UNAVAILABLE`，1/1，563 秒）：`official_ofac_consolidated_xml transport exhausted; attempt_count=3; max_attempts=3; timeout_seconds=30; exception_type=URLError; exception_message=<urlopen error _ssl.c:1063: The handshake operation timed out>`。8 个官方网址里只有这一个连不上，其余 7 个都抓到了。
- 其余四个取数部分通过（`market_macro` 第 2 次通过）；`score_daily` 因 `CAPTURE_COMPONENT_NOT_PASS:official_policy_sources:FAIL` 被挡，下游 12 步 BLOCKED。
- 17:30 RESCUE 按规则拒绝（`RUN_CONTROL_BLOCKED_TERMINAL_RECOVERY_REQUIRED`，`WAITING_FOR_NEW_AS_OF`）：`config/operations/daily_input_capture.yaml` 给官方源 `max_attempts: 1`、`retryable_blocker_codes: []`、`HISTORICAL_RECAPTURE_FORBIDDEN`，OPS-085 只补 `market_macro`。

代码核对（main `cf6e6a33`）：`fetch_official_policy_sources` 逐个抓 8 个来源（Federal Register 两个、OFAC SDN 和合并名单两个 XML、USTR 页面、ITA CSL、Congress.gov、GovInfo）。任何一个 `ERROR` 问题都让报告 `FAIL`，CLI 退出码 1，采集组件 FAIL，整个采集是 `PARTIAL_CAPTURE`，`score_daily` 不消费。缺 Congress/GovInfo API key 时早已是 `WARNING`（来源跳过，报告 `PASS_WITH_WARNINGS`，退出码 0）。下游 `load_verified_official_policy_capture` 不要求 8 个来源都在，只核对下载清单非空、和 raw 文件一一对应。

2026-10-16 以来 13 个运行日里官方源失败 5 天：10-20（SEC 同时失败）、10-21（OFAC SDN 读断，DATA-027 已修）、10-22 和 10-28（缓存协调层挡住重试，DATA-028 / DATA-030 已修）、11-03（本次）。

缺一个来源一天的代价小：OFAC 两个名单和 CSL 每天给全量，Federal Register、Congress、GovInfo 按 `as_of` 前 3 天的窗口抓，第二天的抓取会覆盖漏掉的那天；只是那一天的待复核候选里少了这一家，晚一天进入风险预审。

## 3. 做法

`src/ai_trading_system/official_policy_sources.py`：

- 两个公开模块常量：`OFFICIAL_POLICY_MAX_GAP_SOURCES = 2`、`OFFICIAL_POLICY_SOURCE_GAP_CODE = "official_policy_source_unavailable_gap"`。
- `OfficialPolicySourceFetchReport` 加字段 `gap_sources: tuple[str, ...] = ()`：记为当天缺口的来源，按请求顺序。
- 抓完全部来源以后（问题仍照现在的写法逐条记下），满足下面全部条件时，把“不可用”的问题改记为缺口：
  1. 不可用的问题：`ERROR`、`blocker_code == "PROVIDER_UNAVAILABLE"`、带 `source_id`。也就是传输重试用尽（`official_policy_source_transport_exhausted`）、客户端直接抛出可重试的传输异常（`official_policy_source_download_failed`，阻断码 `PROVIDER_UNAVAILABLE`）、HTTP 5xx（`official_policy_source_http_error`，阻断码 `PROVIDER_UNAVAILABLE`）。
  2. 至少 1 个、最多 `OFFICIAL_POLICY_MAX_GAP_SOURCES`（2）个。
  3. 除此以外没有别的 `ERROR`（4xx、429、解析失败、未知来源、非传输类下载错误都算别的）。
  4. 至少有一个来源成功：有一个 payload 的 `status_code < 400`。
- 改记的写法：在原位置换成一条 `WARNING`，`code` 为 `OFFICIAL_POLICY_SOURCE_GAP_CODE`，`source_id`、`blocker_code`（`PROVIDER_UNAVAILABLE`）照旧，`message` 是原消息后面接 `；本来源记为当天缺口（DATA-033），不补抓，其余来源照常使用。`。别的问题和顺序不动。`gap_sources` 按这些问题的顺序列出来源。
- 任何一条不满足：问题原样保留（`ERROR`，原 code），`gap_sources` 为空，报告 `FAIL`，和现在一样。
- HTTP 5xx 的响应仍照现在写成 raw 文件并留在 `payloads` 和下载清单里（`row_count` 0、没有候选），raw 文件和清单仍一一对应；传输失败的来源没有 raw 文件、不进清单。
- 中文报告：`- 跳过来源数：` 下一行加 `- 当天缺口来源：<逗号分隔的 source_id>`，没有时写 `无`；「方法边界」加一条说明这条规则。

`src/ai_trading_system/cli_commands/risk_events.py`：`fetch-official-sources` 在打印 `blocker_code=` 行之后，`gap_sources` 非空时打印一行 `gap_sources=<逗号分隔>`。

结果：一个或两个网址断线时，报告 `PASS_WITH_WARNINGS`，命令退出码 0，采集组件 PASS，`score_daily` 照常消费其余来源；3 个以上来源不通、没有一个成功、或有别的错误时仍整天停。

## 4. 不变的

- `config/operations/daily_input_capture.yaml` 不改：`max_attempts: 1`、`retryable_blocker_codes: []`、`HISTORICAL_RECAPTURE_FORBIDDEN`、`source_revision` 都照旧；没有 17:30 对官方源的补抓，也没有事后补抓。`source_revision` 管的是采集状态的身份（同一 as_of 的状态能否复用），这次只改命令自己的通过条件，照 DATA-027、DATA-031 的先例不升版本；升版本会让同一 as_of 已有的状态变成身份不符。
- 请求级重试（3 次，退避 1 秒、2 秒）、缓存层、各来源的解析不变；下游 `load_verified_official_policy_capture` 不改。
- 缺 API key 的跳过规则不变，也不计入 2 个的上限。

## 5. 验收

- 新增测试 `tests/test_data033_official_source_gap.py`（Claude 写，19 条）全过；`tests/test_official_policy_sources.py`、`tests/test_data027_official_incomplete_read.py` 不改、照过。
- 换包后，下一次只有一两个官方网址断线的生产日，日报照出，官方源抓取报告是 `PASS_WITH_WARNINGS`，写明当天缺口来源。

## 6. 没有解决的

- SEC companyfacts、FMP 前瞻和估值仍是一次采集，断线就丢一天（10-20 那天 SEC 也失败了）；要不要同样处理是另一个 owner 决定。
- App 日报页和调度摘要只看到官方源抓取报告是 `PASS_WITH_WARNINGS`，缺口来源要打开该报告看；要不要在日报摘要里单列，换包后看实际用起来再定。

## 7. 进展

- 2026-11-04：Baker 定方案（第 1 节）；需求和验收测试写好，交 pi 实现（DATA-033-01）。
