# DATA-028：请求缓存里的失败不再永久挡住后续运行，坏响应作废后重取

状态：IN_PROGRESS。`production_effect=none`；`broker_action=none`。

关联：OPS-065（过期后的跨进程 singleflight）、OPS-086（日报发布包）、DATA-024（可变数据源缓存有效期）、DATA-027（官方源读断重试）；任务记录 `tasks/DATA-028.yaml`。

## 1. Owner decision

- `owner_decision:DATA-028:2026-10-22:cache_failure_recovery`：Baker 2026-10-22 14:08 JST 在 OPS-086 线程的卡片上选「修好再切」

## 2. 背景

2026-10-22 12:00 JST，OPS-086 新入口（发布包 `20261021-19a16391c`）的第二个生产日（as_of 2026-10-21）失败，两个组件的原因都在联网缓存层，不是单纯的网络断开：

- `market_macro`（FRED DGS2）：服务器回了 HTTP 200，正文只有 898 字节，解析不成 CSV（`Expected 1 fields in line 5, saw 3`）。第一次尝试把这份响应当作正常结果存进缓存，第二次尝试 6 秒后读到同一份缓存（Cache status HIT），再次解析失败。FRED 的 200～399 响应没有有效期（DATA-024 只给键里没有日期的来源加了 12 小时期限），所以这份坏内容会一直留到有人作废它。
- `official_policy_sources`：6.3 秒后失败，`RevalidationCoordinationError: OWNER_FAILURE_RETRY_DISABLED: policy forbids another live request for this causal state`。没有发出任何真实请求，DATA-027 的重试因此没有机会用上。

代码核对：

- `ExternalRequestRevalidationCoordinator.acquire` 在 lease 链的最后一个事件是 `FAILED`、且它的 causal fingerprint 等于当前探测的指纹、策略又是 `allow_waiter_retry_after_owner_failure: false` 时，返回 `OWNER_FAILURE_BLOCKED`，**没有时间限制**。指纹是到期条目自己的状态（`status`、`generation_id`、`body_sha256`、`reason_code`）；联网刷新一直没有成功，条目就不变，指纹就不变，于是失败标记永远有效。2026-10-21 官方源的 `IncompleteRead` 就留下了这样一个标记，此后每次运行都被挡住，直到有人用 `invalidate-external-request-cache` 手动作废该条目。
- OPS-065 的意图是“其余进程在有界时间内复用 winner 发布的 generation”（策略文件 `intended_effect`），owner 失败后同一时刻等待的进程不再重复请求；没有想要让一次失败跨天封死这个 URL。
- 影响面：所有被 DATA-024 加了 12 小时期限的、键里没有日期的请求（SEC companyfacts 每家公司一个 URL、FMP 估值端点、官方政策源页面）。FRED 价格类请求的键里有日期，每天换新键，不受这一条影响。
- FRED 的解析发生在 `cached_requests_get` 返回之后，解析失败时 provider 不会作废刚存下的条目。

## 3. 做法

### A. 失败标记只在有界时间内挡住同一 causal state

`acquire` 里 `FAILED` 分支的挡住条件，在原来的三个条件之外加第四个：当前时刻早于该 `FAILED` 事件的 `occurred_at` 加 `policy.waiter_timeout_seconds`（现在是 135 秒）。窗口过去以后，同一指纹的新调用按 `ACQUIRED` 取新 lease（generation 加 1，hash 链延续，事件转换校验本来就允许 `FAILED` 之后 `ACQUIRED`）。

- `allow_waiter_retry_after_owner_failure` 仍是 `false`，`config/data/external_request_cache_revalidation_coordination_policy.yaml` 和 `policy_version` 都不改：窗口复用已有的 `waiter_timeout_seconds`，它本来就是“等待者的有界时间”。
- 窗口内行为不变（仍然 `OWNER_FAILURE_BLOCKED` / `OWNER_FAILURE_RETRY_DISABLED`）；`STALE_TAKEOVER_LIMIT_REACHED` 的路径不变；不同指纹、不同 key 的行为不变。
- 失败的新 owner 会再留下一个 `FAILED`，再挡自己的 135 秒，所以不会把重试放大成对服务器的连续请求。

### B. FRED 响应解析失败时作废缓存条目

`FredRateProvider._parse_fred_response` 在 `csv_parse` 和 `schema` 两种解析失败（不含 `http_status`）、在抛出 `ProviderDownloadError` 之前，用已有的 `invalidate_external_request_cache` 作废刚读到的 generation：

- 先 `lookup_external_request_cache` 取当前 generation，只有它的 `body_sha256` 等于这份响应内容的 sha256 才作废（避免作废别人刚发布的新条目）；
- `actor="fred_rate_provider"`，`reason="unusable_response_<阶段>"`，`reference="DATA-028"`；
- 作废过程中的 `OSError`、`ValueError` 吞掉，原来的 `ProviderDownloadError` 照常抛出；缓存目录为 `None`（缓存关闭）时不作废。

结果：条目进入 `INVALIDATED_REVALIDATE`，market_macro 的第二次尝试（以及 17:30 补取）会重新联网；全窗口基底条目被污染时也不会再每天被选作基底。其他来源（SEC、FMP、官方源）本批不改，见 §6。

## 4. 不变的

重试次数和退避、`config/data/external_request_cache_lifecycle_policy.yaml`、协调层策略文件、`positive_ttl_rules`、不可复用响应的处理、cache bytes/path/schema、4xx/5xx 规则、DQ 和各 provider 对 HTTP 状态的解释都不变。采集策略（`daily_input_capture.yaml`、OPS-085 补取）不变。

## 5. 验收

- 新增测试 `tests/test_data028_cache_failure_recovery.py`（Claude 写，11 条）全过；`tests/test_external_request_cache.py`、`tests/test_external_request_cache_revalidation_coordination.py`、`tests/test_data024_request_cache_ttl.py`、`tests/test_market_data.py` 不受影响。
- 下一个生产日：一次联网失败不会让同一 URL 在之后的运行里被缓存层拒绝；FRED 的一次坏响应不会让当天第二次尝试读到同一份坏内容。

## 6. 没有解决的

- SEC、FMP、官方政策源的 HTTP 200 坏内容同样会被缓存 12 小时，本批没有观察到也没有改（只改了观察到的 FRED）。更通用的做法是给 `cached_requests_get` 加一个发布前的校验钩子，是后续任务。
- 官方源和 SEC 的采集失败同日不能补取（`HISTORICAL_RECAPTURE_FORBIDDEN`、`MANUAL_NON_PIT_RAW_REVIEW`），OPS-085 只补行情。要不要放宽是 owner 决定，不在这个任务里。
- 这台电脑经代理出网，几天里五次失败落在不同主机、不同阶段，原因像是代理出口不稳，未验证。
- DATA-027 的重试在生产上还没有被用到（10-22 官方源在它之前就被缓存层拒绝）。

## 7. 进展

- 2026-10-22：需求和验收测试写好，交 pi 实现（DATA-028-01）。
