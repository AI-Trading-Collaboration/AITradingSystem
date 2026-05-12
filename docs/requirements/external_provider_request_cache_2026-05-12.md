# DATA-018 外部供应商请求级缓存

## 背景

外部供应商接口存在调用次数、速率或成本限制。当前系统已经在业务层落地多类
raw cache 和 manifest，但部分命令仍会对完全相同的 HTTP 请求重复调用供应商，
尤其是 `daily-run` 重试、直接命令诊断和 PIT/valuation 多端点抓取路径。

本任务把“相同外部请求不得重复发送”提升为 P0 运行约束。任何外部供应商请求
都必须先查询本地请求级缓存；缓存命中时直接复用缓存响应，不再访问供应商；
缓存缺失时才发送请求，并把响应体、脱敏请求元数据、状态码、headers 和 checksum
写入本地缓存。

## 范围

- 市场/宏观数据：
  - FMP EOD price；
  - Marketstack EOD；
  - Cboe VIX CSV；
  - FRED CSV；
  - Yahoo/yfinance 诊断或迁移路径。
- 估值和 forward-only PIT：
  - FMP quote、key metrics、ratios、analyst estimates、price target、grades、
    ratings、earnings calendar；
  - EODHD earnings trends。
- SEC/IR/官方来源：
  - SEC companyfacts；
  - SEC submissions 和 filing archive index；
  - TSMC IR 文本下载；
  - OFAC、USTR、Trade.gov、Congress.gov、GovInfo 等官方政策来源。
- OpenAI Responses API 已有 agent request cache；本任务需保留该缓存语义，并在
  文档中明确它同样属于外部请求缓存治理范围。

## 非目标

- 不伪造或补写缺失供应商数据。
- 不把缓存命中改写成新的 provider `downloaded_at` 事实；业务 manifest 可以记录
  本次处理时间，但请求级缓存必须保留原始响应首次捕获时间。
- 不在报告、metadata 或 cache manifest 中写入 API key、token、Authorization、
  Cookie 或付费内容原文摘要之外的敏感片段。
- 不引入自动 TTL 刷新。相同请求默认永久复用；需要刷新时必须改变请求参数或由
  owner 明确清理缓存。

## 设计

1. 新增通用请求缓存模块：
   - cache key 由 schema version、provider、api family、HTTP method、endpoint、
     脱敏 query/body/header identity 生成；
   - API key、token、Authorization、Cookie、User-Agent 等凭据或账户相关 header
     不进入 key，不落原文；
   - 响应体写入独立 `.body` 文件，metadata 写入 `.json`，避免大型 JSON 响应在
     cache 层二次 base64 或巨型字符串化。
2. 业务模块统一通过缓存 wrapper 获取 HTTP response：
   - wrapper 暴露 `status_code`、`ok`、`headers`、`content`、`text`、`json()` 和
     `raise_for_status()`；
   - 缓存命中和缓存缺失后真实请求返回同一个 response 接口；
   - 非 2xx 响应也缓存，避免相同失败请求在短时间内消耗额度；网络异常未收到
     response 时无法缓存，仍按原异常路径 fail closed。
3. yfinance 路径不是普通 HTTP response，第一版用 dataframe pickle 作为请求级
   cache，key 仍按 provider、tickers、start/end/interval 生成。
4. 业务 raw payload、PIT manifest、download manifest 和日报门禁继续保留；请求级
   cache 是更底层的供应商调用保护，不替代业务审计产物。
5. 网络异常或供应商连接在获得 HTTP response 前失败时，无法写入请求级 response
   cache。此类失败必须由 provider adapter 抛出结构化诊断，并由命令层写入
   脱敏失败报告，至少包含 provider、api family、endpoint、脱敏请求参数、cache
   key、cache metadata 预期路径、cache 状态、失败阶段、异常类型、已累计 row count
   和下游影响；不得保存 API key、token、Cookie、User-Agent、stdout/stderr 原文或
   供应商响应正文。

## 验收标准

- 相同 FMP、Marketstack、Cboe、FRED、SEC、TSM IR、official policy、EODHD 请求
  第二次调用时不触发 injected fake HTTP client。
- cache metadata 不包含 API key、token、Authorization 或 Cookie 原文。
- cache body checksum 与返回给业务模块的 bytes 一致。
- 现有业务 manifest、PIT raw payload、SEC raw JSON、valuation raw JSON 仍正常写入。
- `aits download-data` 在 Marketstack 或其他 provider 失败时写入
  `outputs/reports/download_data_diagnostics_YYYY-MM-DD.md`，daily-run 把该报告列为
  `download_data` 预期 artifact；报告内容可定位失败阶段和 cache 状态，但不包含敏感值
  或付费内容原文。
- 相关单测、`ruff check` 和目标测试通过。
- 系统流图和 README/runbook 说明请求级 cache 与业务 raw cache 的层次关系。

## 状态记录

- 2026-05-12：新增并进入 `IN_PROGRESS`。原因：owner 明确要求所有外部供应商请求
  必须 cache，且相同请求不得重复发送；该要求优先级高于继续重跑 daily-run。
- 2026-05-12：实现进入 `VALIDATING`。新增 `external_request_cache` 通用缓存模块，
  接入 FMP/Marketstack/Cboe/FRED/yfinance、FMP valuation/PIT、EODHD、SEC
  companyfacts/submissions/archive、TSMC IR 和官方政策来源；OpenAI 保留既有
  `agent_request_cache`。目标测试 123 passed，`ruff check src tests` 和
  `git diff --check` 通过；尚未用 live daily-run 观察真实供应商 cache HIT/MISS
  分布。
- 2026-05-13：回到 `IN_PROGRESS`。真实 `daily-run` 重跑显示 FMP、Cboe 和 FRED
  已有请求级 cache 记录，但 Marketstack 未产生 metadata 且 `download-data` 仅在
  daily-run 报告中留下 stderr 行数，无法定位是否为连接、HTTP、provider error、
  JSON/schema 或 normalization 阶段。当前范围补充 Marketstack provider 结构化诊断、
  `download-data` 脱敏失败报告和 daily-run 预期 artifact。
- 2026-05-13：重新进入 `VALIDATING`。已实现 Marketstack provider 结构化诊断，
  覆盖 HTTP response 前失败、HTTP status、provider error、JSON/schema 和
  normalization 阶段；`aits download-data` 在失败时写入脱敏
  `download_data_diagnostics_YYYY-MM-DD.md`，`daily-run` 的 `download_data` 步骤列出
  该报告为预期 artifact。验证：`ruff check src tests` 通过，
  `pytest -q tests/test_external_request_cache.py tests/test_data_download.py tests/test_ops_daily.py`
  34 passed，`git diff --check` 通过；尚未重新执行 live daily-run。
- 2026-05-13：再次回到 `IN_PROGRESS`。Marketstack 诊断增强后的真实 `daily-run`
  生成了 `download_data_diagnostics_2026-05-11.md`，阻断实际为 FRED
  `ReadTimeout`，且发生在获得 HTTP response 前；当前 FRED provider 尚未输出
  series_id/cache key/cache metadata 预期路径等结构化上下文，下一步补齐后重跑确认。
- 2026-05-13：重新进入 `VALIDATING`。FRED provider 已补齐 response 前失败、
  HTTP status、CSV parse 和 schema 阶段的结构化诊断，报告会包含 FRED series id、
  cache key、cache metadata 预期路径、cache status、失败阶段和累计 row count。
  验证：`ruff check src tests` 通过，
  `pytest -q tests/test_external_request_cache.py tests/test_data_download.py tests/test_ops_daily.py`
  35 passed，`git diff --check` 通过；随后执行真实 `daily-run` 观察。
- 2026-05-13：真实 `aits ops daily-run` 观察确认 `download_data` 仍 fail closed；
  `download_data_diagnostics_2026-05-11.md` 已明确定位到 FRED `DGS10`：
  stage=`http_request`、cache_status=`MISS_NO_RESPONSE`、exception=`ReadTimeout`、
  read timeout=30。`DGS2` 同范围请求已有 cache 200；`DGS10` 在获得 HTTP response
  前超时，因此没有可写入的 response cache。下游 PIT/SEC/valuation/score 未执行。
- 2026-05-13：实现 FRED 无响应超时韧性。`FredRateProvider` 默认
  timeout_seconds=60、max_attempts=2、retry_backoff_seconds=3；只在未获得 HTTP
  response 的网络异常上重试，若拿到 HTTP response 则继续由请求级 cache 记录响应并
  fail closed，不做额外重复请求。诊断报告新增 attempt_count、max_attempts 和
  timeout_seconds。验证：`ruff check src tests` 通过，
  `pytest -q tests/test_external_request_cache.py tests/test_data_download.py tests/test_ops_daily.py`
  35 passed，`git diff --check` 通过。
- 2026-05-13：真实 `daily-run` 复测先越过 `download_data`、PIT、SEC 和 valuation，
  确认 FRED `DGS10` 阻断已解除；随后暴露并修复 direct dispatcher `score-daily`
  参数漂移和 production `ops health` PIT visibility cutoff 误判。最终
  `daily_ops_run:2026-05-11:20260512T170255Z` 11/11 步 PASS，`score_daily` 二次
  运行耗时从约 615 秒降至 12.5 秒，说明 OpenAI/official policy 等重复请求已复用
  本地缓存。当前请求级 cache metadata 已覆盖 FMP、Marketstack、Cboe、FRED、SEC、
  TSMC IR 和 official policy；继续保留 `VALIDATING` 观察后续真实运行的 HIT/MISS。
