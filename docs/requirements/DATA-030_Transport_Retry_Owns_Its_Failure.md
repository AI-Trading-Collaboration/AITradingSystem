# DATA-030：取数重试不再被自己上一次的失败挡住

状态：IN_PROGRESS（2026-10-30）。`production_effect=none`；`broker_action=none`。

关联：OPS-065（过期后的跨进程 singleflight）、DATA-024（可变数据源缓存 12 小时有效期）、DATA-026（SEC 断线重试）、DATA-027（官方源读断重试）、DATA-028（失败标记只挡 135 秒）、OPS-086（日报发布包）；任务记录 `tasks/DATA-030.yaml`。

## 1. Owner decision

- `owner_decision:DATA-030:2026-10-30:fix_before_count`：Baker 2026-10-30 01:25 JST 在「收尾工作总览第 1 版并评估日报稳定性」线程的卡片上选「先修再数」：先修这个问题、换发布包，工作总览第 2 版等换包后连续 5 个交易日一次通过再开。

## 2. 背景

2026-10-28 12:00 JST 的日报（as_of 2026-10-27，发布包 `20261023-63b5aa1a4`）整天没出。PRIMARY 窗口 12:00 到 12:09 失败在 `official_policy_sources`，生产状态 `outputs/daily_input_capture/source_control/2026-10-27/official_policy_sources/state.json` 只有一次尝试（03:07:11Z 开始，return_code 1，blocker_code `REQUEST_FAILED`），错误结尾是：

```
external_request_cache.py line 438 _coordinate_revalidation_if_required
external_request_cache_revalidation_coordination.py line 500 execute
RevalidationCoordinationError: OWNER_FAILURE_RETRY_DISABLED: policy forbids another live request for this causal state
```

17:30 的 RESCUE 窗口是 `RUN_CONTROL_BLOCKED_TERMINAL_RECOVERY_REQUIRED`（`WAITING_FOR_NEW_AS_OF`）：官方源的采集策略是 `max_attempts: 1`、`HISTORICAL_RECAPTURE_FORBIDDEN`，OPS-085 的 17:30 自动重试只管 `market_macro`。调度摘要把它标成 `CODE_DEFECT`，因为 `error_summary` 里有 Python traceback。

代码核对（main `c167f227`）：

- DATA-024 给官方政策源页面、SEC companyfacts、12 个 FMP 估值端点的成功响应设了 12 小时有效期。日报一天跑一次，所以每天第一次取这些 URL 时条目都已过期（`EXPIRED_REVALIDATE`），要走 OPS-065 的再验证协调层：取 lease 成为 owner，再联网。
- `UrllibOfficialPolicyHttpClient.get` 的重试（3 次，退避 1 秒、2 秒，DATA-027）包在 `cached_urllib_get` 外面。第一次联网失败时，协调层 `execute` 把 lease 记成 `FAILED`（`OWNER_FAILURE`），再把原异常抛给客户端；客户端 1 秒后重试，新一次 `cached_urllib_get` 是一个新的 owner，`acquire` 看到最后一个事件是同一 causal state 的 `FAILED`，而且还在 `waiter_timeout_seconds`（135 秒）以内，于是返回 `OWNER_FAILURE_BLOCKED`，`execute` 抛 `RevalidationCoordinationError`。它不是客户端认的传输错误，重试循环不接，异常一路出去，成了 traceback。第二次请求根本没发。
- DATA-028 把这个挡住的时间从“永远”改成 135 秒，当时就记下“DATA-027 的重试在生产上还没有被用到”，`tests/test_data028_cache_failure_recovery.py` 的 `test_failed_revalidation_blocks_the_same_episode_but_not_a_later_run` 也钉住了“同一轮里的第二次调用被拒”。135 秒比任何一个客户端的重试退避都长，所以只要条目在再验证，客户端的断线重试就一次也用不上。

## 3. 影响面

在云端用 main `c167f227` 复现（条目放成 13 小时前写入、第一次联网抛断线、第二次正常）：

| 取数 | 重试 | 条目会不会过期 | 结果 |
|---|---|---|---|
| 官方政策源 `UrllibOfficialPolicyHttpClient.get` | 3 次，1 秒、2 秒（DATA-027） | 12 小时，每天都过期 | 第二次被拒（10-28 就是这个） |
| SEC companyfacts `download_companyfacts_raw` | 3 次，5 秒、15 秒（DATA-026） | 12 小时，每天都过期 | 第二次被拒，还没碰上 |
| FMP 估值 `FmpHttpValuationProvider._get` | 3 次，0.5 秒乘次数 | 12 个端点 12 小时 | 第二次被拒，还没碰上 |
| EODHD `EodhdHttpEarningsTrendsProvider._get` | 同上 | 只有 4xx/5xx 条目过期 | 同样的写法；EODHD 已停用 |
| FRED `_request_fred_response` | `max_attempts` 次 | 键里有日期，每天新键；只有 4xx/5xx 条目过期 | 平时走冷 MISS，不受影响 |
| FMP 行情 `_fetch_fmp_price_records` | 3 次，1 秒乘次数 | 键里有日期；只有 4xx/5xx 条目过期 | 平时走冷 MISS，不受影响 |

冷 MISS（没有条目）不经过协调层，失败时什么也不写，所以重试能用上；FRED 和 FMP 行情平时就是这样。Marketstack 的分页循环没有重试，不在表里。

## 4. 做法

一个重试循环是一个 owner：调用方在循环之前生成一个 owner id，每次尝试都带上它；协调层让同一个 owner 在自己失败之后取新 lease，别的 owner 照旧被挡 135 秒。

### A. 协调层（`external_request_cache_revalidation_coordination.py`）

- 新函数 `new_revalidation_owner_id() -> str`：返回 `owner-` 加 32 位小写十六进制（`uuid4().hex`），符合已有的 `_OWNER_ID_PATTERN`。
- `ExternalRequestRevalidationCoordinator.execute` 加关键字参数 `owner_id: str | None = None`：给了就用它，没给就和以前一样每次调用生成一个新的。格式不对的 id 照旧由 `acquire` 报 `ValueError`，不发请求、不写事件。
- `acquire` 的 `FAILED` 分支多一个条件：最后一个事件的 `owner_id` 不等于这次的 owner 才挡。也就是说，挡住要同时满足：最后一个事件是 `FAILED`、causal fingerprint 相同、策略 `allow_waiter_retry_after_owner_failure` 为 false、还在 `occurred_at` 加 135 秒以内、并且失败的不是自己。自己失败后再来，按 `ACQUIRED` 取新 lease（generation 加 1，新的 owner token，hash 链照旧，事件转换校验本来就允许 `FAILED` 之后 `ACQUIRED`）。
- 只看最后一个事件：自己更早失败过、但最后一次失败是别人的，照样挡。最后一个事件是 `ACTIVE` 时同一个 owner 也只是 `WAITER`（不会接管自己还没结束的 lease）。stale takeover、`COMPLETED` 之后的路径都不变。

### B. 缓存（`external_request_cache.py`）

- `cached_requests_get` 和 `cached_urllib_get` 各加关键字参数 `revalidation_owner_id: str | None = None`，传给 `_coordinate_revalidation_if_required`，再传给 `execute(owner_id=...)`。
- 它不进请求身份：缓存键、metadata、body、negative observation 都和以前一样；冷 MISS、普通 HIT、缓存关闭时它不起作用。

### C. 六个重试循环

`official_policy_sources.py` 的 `UrllibOfficialPolicyHttpClient.get`、`fundamentals/sec_companyfacts.py` 的 `SecEdgarCompanyFactsProvider.download_companyfacts_raw`、`valuation_sources.py` 的 `FmpHttpValuationProvider._get` 和 `EodhdHttpEarningsTrendsProvider._get`、`data/market_data.py` 的 `FredRateProvider._request_fred_response` 和 `_fetch_fmp_price_records`：在 `for attempt in ...` 之前 `revalidation_owner_id = new_revalidation_owner_id()`，循环里每次调用缓存都带 `revalidation_owner_id=revalidation_owner_id`。一次方法调用一个 owner：同一个客户端再调一次（新的一轮）是新的 owner，在 135 秒内仍会被挡住，不发请求。

结果：同一轮里最多发出该循环的尝试次数那么多个请求（官方源、SEC、FMP 都是 3 个），每次失败照旧留下 `FAILED` 事件；第三次也失败时，调用方拿到的是最后一次的传输异常（官方源包成 `OfficialPolicyTransportError`，阻断码 `PROVIDER_UNAVAILABLE`，不再有 traceback），不再是 `OWNER_FAILURE_RETRY_DISABLED`。

## 5. 不变的

- `config/data/external_request_cache_revalidation_coordination_policy.yaml`（`allow_waiter_retry_after_owner_failure: false`、lease 120 秒、等待 135 秒、`policy_version`）和 `config/data/external_request_cache_lifecycle_policy.yaml` 一个字节不改。
- 每个循环的尝试次数、退避、哪些异常重试、HTTP 状态怎么处理都不改；采集策略 `config/operations/daily_input_capture.yaml`、OPS-085 都不改。
- 缓存的键、路径、文件格式、TTL、失效、DQ 和各来源对响应的解释都不改。
- DATA-028 的行为对别的 owner 不变：不带 owner id 的调用、另一轮循环、另一个进程，在别人失败后的 135 秒内照旧被挡；`tests/test_data028_cache_failure_recovery.py` 和其他已有测试不改。

## 6. 验收

- 新测试 `tests/test_data030_retry_owns_its_failure.py`（Claude 先写，29 条）全过：协调层（同一 owner 失败后能取新 lease、别的 owner 仍被挡、自己再失败后别人重新等 135 秒、只认最后一个失败、`ACTIVE` 时不插队、策略文件不变、`execute` 用给的 owner、不给就每次新的、格式不对报错）；缓存（同一 owner 的第二次真的联网、别的 owner 仍被拒、owner 不进缓存键）；六个循环（官方源、SEC、FMP 估值、EODHD 走真缓存端到端；六个循环都是一轮一个 owner；源码里每个 `for attempt` 循环里调缓存的地方都带循环外生成的 owner）；记录。
- 已有的 `test_data024…`、`test_data026…`、`test_data027…`、`test_data028…`、`test_external_request_cache.py`、`test_external_request_cache_revalidation_coordination.py`、`test_official_policy_sources.py`、`test_valuation_sources.py`、`test_market_data.py` 不改、全过；PR 套件通过。
- 生产：换包后第一个生产日官方源、SEC、FMP 估值三个部分一次通过；之后哪天某个来源第一次联网失败，状态文件里同一部分只有一次尝试、没有 `OWNER_FAILURE_RETRY_DISABLED`。

## 7. 没有解决的

- FMP 估值对 429 和 5xx 也重试，但 5xx 响应按生命周期策略缓存 300 秒（429 还看 Retry-After），0.5 秒、1 秒后的重试读到的是缓存里的同一个错误，没有真的再问 FMP（云端复现：3 次尝试只联网 1 次）。要不要让同一轮重试跳过自己刚存下的失败响应，是改缓存复用失败响应的规则，另开任务；这一批不改。
- 别的 owner 被挡时抛出的 `RevalidationCoordinationError` 仍然不是传输错误，官方源会以 traceback 结束，调度摘要会标 `CODE_DEFECT`。日报里同一个 URL 不会在 135 秒内被两轮循环取，所以这条路平时走不到；要不要让官方源把它记成一条来源错误，以后再看。
- 官方源和 SEC 当天失败仍不能在 17:30 补取（`HISTORICAL_RECAPTURE_FORBIDDEN`、`MANUAL_NON_PIT_RAW_REVIEW`），放宽 OPS-085 是 owner 决定，不在这里。
- 第一次联网为什么失败没有记下来：错误结尾只留了最后一个异常。修好以后最后一个异常就是真正的网络错误。

## 8. 进展

- 2026-10-30：需求和验收测试写好，交 pi 实现（DATA-030-01）。
