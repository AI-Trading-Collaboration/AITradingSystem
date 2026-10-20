# DATA-026：SEC companyfacts 下载对“收到响应之前的网络失败”重试

状态：IN_PROGRESS。`production_effect=none`；`broker_action=none`。无需 owner 决定（B 区，只在下载适配器里补请求级重试，采集策略不变；Baker 2026-10-20 在 OPS-086 线程同意先补）。

## 1. 背景

2026-10-20 12:00 JST，OPS-086 步骤 7 的第一个生产日（as_of 2026-10-19）在发布包上失败：`sec_companyfacts` 在 TLS 握手处 4.9 秒失败，整个组件 `FAIL`，当日日报没有生成。原因是 `SecEdgarCompanyFactsProvider.download_companyfacts_raw` 对每家公司只发一次请求，一次握手中断就让整个命令失败；`config/operations/daily_input_capture.yaml` 里 `sec_companyfacts` 的 `max_attempts` 是 1，17:30 也不会重试。

同一天 13:10，用同一个发布包、同一个解释器、同样的用户变量在临时目录重跑，SEC 取数 24.5 秒成功（17 家公司、368044 条事实），没有代理或证书变量缺失，证书是真的（OPS-086 的 D1 诊断）。所以更像代理出口或网站一侧的短时故障。FMP 价格（`FMP_REQUEST_MAX_ATTEMPTS`）、FMP 估值、官方政策源（OPS-072）都已经有请求级的有界重试，SEC companyfacts 没有；运行副本一直靠永不过期的请求缓存，从来没有天天联网取它，这个缺口没有暴露。

## 2. 做法

`src/ai_trading_system/fundamentals/sec_companyfacts.py` 里：

- 新增常量 `SEC_REQUEST_MAX_ATTEMPTS = 3`、`SEC_REQUEST_RETRY_BACKOFF_SECONDS = (5.0, 15.0)`。
- `SecEdgarCompanyFactsProvider` 多三个仅关键字参数：`max_attempts`、`retry_backoff_seconds`、`sleeper`（默认 `time.sleep`，测试注入）。`max_attempts < 1`、退避秒数少于 `max_attempts - 1` 个、或含负数，构造时 `ValueError`。
- `download_companyfacts_raw` 对 `cached_requests_get` 抛出的“收到 HTTP 响应之前”的失败做有界重试：异常是 `requests.exceptions` 的 `Timeout`、`ConnectionError`（含 `SSLError`、`ConnectTimeout`）或 `ChunkedEncodingError`。第 n 次失败后先等 `retry_backoff_seconds[n-1]` 秒再试；最后一次仍失败就原样抛出最后一次的异常（类型、消息和调用栈不变，capture 的分类和 `error_tail` 看到的仍是真实异常）。
- 不重试：HTTP 错误状态（包括 429、5xx，由 `raise_for_status` 在重试循环之外抛出）、解析错误、其他任何异常。
- 失败的尝试不写请求缓存（`cached_requests_get` 本来就只缓存收到的响应）；成功的响应照常缓存，同一请求之后命中缓存不再联网、不再等待。
- 退避选 5 秒和 15 秒，比官方政策源的 1 秒和 2 秒长：这台电脑经代理出网，节点切换可能要几秒才恢复。最坏情况下一次失败的运行多等 20 秒。

## 3. 不变的

- `config/operations/daily_input_capture.yaml`（`policy_version`、`max_attempts`、`recovery_mode`）、`source_revision`、运行身份和 capture 的重试规则都不变，没有组件级重试，也没有 17:30 对 SEC 的补取。
- 官方政策源、FMP、`SecPitEdgarProvider` 和 `SecEdgarFilingArchiveProvider` 不改。
- 下载清单、输出文件、`download_sec_companyfacts` 的行为不变；成功路径零额外等待。

## 4. 验收

- 新增测试 `tests/test_data026_sec_transport_retry.py`（Claude 写，17 条）全过；`tests/test_sec_companyfacts.py`、`tests/test_external_request_cache.py`、`tests/test_data024_request_cache_ttl.py` 不受影响。
- 下一个生产日 SEC 取数遇到短时握手中断时不再让整个日报失败。

## 5. 没有解决的

- 故障持续超过约 20 秒时仍会失败。2026-10-20 的官方政策源已经有三次请求级尝试，仍在 392 秒后失败，说明故障可能持续数分钟；这个任务不处理，也不改官方政策源。下一次失败时 DATA-025 会留下异常的结尾，再据此决定。
- 让 17:30 对 SEC 和官方政策源也补取一次（OPS-085 现在只补行情）要改 `recovery_mode` 和历史重取的边界，是 owner 决定，不在这个任务里。

## 6. 进展

- 2026-10-20：需求和验收测试写好，交 pi 实现（DATA-026-01）。
