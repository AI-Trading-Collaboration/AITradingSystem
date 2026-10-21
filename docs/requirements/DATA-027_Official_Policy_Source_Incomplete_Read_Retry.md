# DATA-027：官方政策源下载“读到一半断开”时重试

状态：IN_PROGRESS。`production_effect=none`；`broker_action=none`。无需 owner 决定（B 区，只把一种已知的传输失败补进已有的请求级重试；采集策略和重试次数不变；Baker 的授权句 R 第 (6) 条已同意新入口失败时退回运行副本，补这个缺口属于同一件事）。

## 1. 背景

2026-10-21 12:00 JST，OPS-086 新入口（发布包 `20261020-84f585b98`）的第一个生产日（as_of 2026-10-20）失败：`official_policy_sources` 在 19.7 秒后 `FAIL`（`REQUEST_FAILED`，尝试 1/1），其余四个取数组件都通过，`score_daily` 被阻断，当天日报没有生成。DATA-025 新加的 `error_tail` 留下了真正的异常：

`http.client.IncompleteRead: IncompleteRead(7974061 bytes read, 21257126 more expected)`

7974061 加 21257126 是 29231187，正是 OFAC SDN XML 的大小。也就是下载这个约 29 MB 的文件时，连接在读到约 8 MB 处断开。栈在 `external_request_cache.py` 的 `fetch_live_response` 里的 `content = response.read()`。

官方政策源的下载客户端 `UrllibOfficialPolicyHttpClient`（OPS-072）本来就有 3 次尝试、退避 1 秒和 2 秒的请求级重试，但它只重试 `TimeoutError`、`ConnectionError`、`ssl.SSLError`、`urllib.error.URLError`。`http.client.IncompleteRead` 是 `http.client.HTTPException` 的子类，不是 `OSError`，所以不在其中：第一次读断就直接抛出，被归为 `REQUEST_FAILED`，三次尝试一次也没用上。2026-10-20 的三次失败（SEC 握手中断、官方源 392 秒、cboe `SSLEOFError`）和这一次都是连接在途中断，只是这一次落在了重试没覆盖的异常类型上。

这个组件的 `recovery_mode` 是 `HISTORICAL_RECAPTURE_FORBIDDEN`，OPS-085 的 17:30 补取不能重试它，所以一次读断就丢掉整天。

## 2. 做法

`src/ai_trading_system/official_policy_sources.py` 里，两处各加一个类型 `http.client.IncompleteRead`（并加 `import http.client`）：

- `UrllibOfficialPolicyHttpClient.get` 的 `except (...)` 元组。
- `_is_retryable_transport_error` 的 `isinstance` 元组。

结果：

- 响应正文读到一半断开，会按已有的重试表再试：最多 3 次，第 n 次失败后等 `retry_backoff_seconds[n-1]` 秒（默认 1 秒、2 秒）。每次重试重新下载整个文件。
- 3 次都断：抛 `OfficialPolicyTransportError`（`exception_type` 为 `IncompleteRead`，原异常是 `__cause__`），报告里是 `official_policy_source_transport_exhausted`，阻断码 `PROVIDER_UNAVAILABLE`（以前是 `official_policy_source_download_failed` 和 `REQUEST_FAILED`）。
- 其他异常（`HTTPError`、`http.client.InvalidURL`、`ValueError` 等）和 HTTP 状态仍然不重试。
- 失败的尝试不写请求缓存（`cached_urllib_get` 只在收到完整响应后才缓存）；成功的响应照常缓存。

## 3. 不变的

- 重试次数（3）和退避（1 秒、2 秒）不变；`config/operations/daily_input_capture.yaml`（`max_attempts`、`recovery_mode`、`source_revision`）和 capture 的重试规则不变，没有组件级重试，也没有 17:30 对官方源的补取。
- `cached_urllib_get`、SEC、FMP 和其他适配器不改。
- 成功路径零额外等待。

## 4. 验收

- 新增测试 `tests/test_data027_official_incomplete_read.py`（Claude 写，11 条）全过；`tests/test_official_policy_sources.py`、`tests/test_external_request_cache.py` 不受影响。
- 下一个生产日官方源下载遇到一次读断时不再让整个日报失败。

## 5. 没有解决的

- 同一个文件连续三次都读断（约 3 秒内）仍会失败。这台电脑经代理出网，2026-10-20 和 10-21 共四次失败的类型各不相同（TLS 握手、超时、`SSLEOFError`、`IncompleteRead`），都是连接中途断开，原因像是代理出口不稳，未验证。更长的退避、让 17:30 对官方源和 SEC 补取一次（OPS-085 现在只补行情），都要改 `recovery_mode` 和历史重取的边界，是 owner 决定，不在这个任务里。

## 6. 进展

- 2026-10-21：需求和验收测试写好，交 pi 实现（DATA-027-01）。
