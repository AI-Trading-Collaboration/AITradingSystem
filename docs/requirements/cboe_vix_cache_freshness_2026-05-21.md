# Cboe VIX 请求缓存新鲜度修复

最后更新：2026-06-09

- 任务 ID：DATA-019
- 优先级：P0
- 状态：DONE
- 创建日期：2026-05-21
- 负责人：系统实现

## 背景

`aits ops daily-run` 在 as-of `2026-05-19` 连续停在 `score_daily` 的
`data_quality` 门禁：主价格缓存中的 `^VIX` 最新观测为 `2026-05-11`，
相对评估日过期 8 天。

只读诊断确认 Cboe 官方 `VIX_History.csv` 当前 HTTP 200，尾部已经包含
`05/19/2026`。项目未更新的原因不是供应商失效，而是外部请求级 cache 对
静态 URL 生成了稳定 cache key：2026-05-13 写入的旧 `response.body` 尾部
只到 `05/11/2026`，后续 `download-data` 命中该旧响应。

## 设计决策

`VIX_History.csv` 是可变静态文件，不能只按 URL 作为请求级 cache identity。
本修复在 Cboe VIX provider 层为 cache identity 加入业务窗口：

- ticker：`^VIX`
- start：请求开始日期
- end：请求结束日期
- interval：`1d`

实际 HTTP 请求仍访问原始 Cboe URL，不向供应商发送伪造参数。旧 cache 保留
审计价值，但不会再被新的评估窗口误命中。

## 验收标准

- 针对不同 `end` 日期的 Cboe VIX 请求必须生成不同 cache key。
- 旧 `2026-05-11` 响应不能被 `2026-05-19` 窗口复用。
- `prices_daily.csv` 中 `^VIX` 可覆盖到 `2026-05-19`。
- `aits validate-data --as-of 2026-05-19` 不再因 `^VIX` stale 失败。
- `aits ops daily-run` 能越过此前 `^VIX` stale 阻断；若还有新 blocker，必须按门禁停止并报告。

## 进展记录

- 2026-05-21：新增任务和需求文档，开始实现窗口感知 cache identity。
- 2026-05-21：实现完成并进入验证。`CboeVixPriceProvider` 现在用
  ticker/start/end/interval 作为请求级 cache identity；实际 HTTP 请求仍访问原始
  Cboe URL。同一请求窗口如果命中的缓存体未覆盖请求 `end`，会丢弃该缓存并重新请求，
  避免旧静态 CSV 在相同 as-of 窗口内继续阻断。验证通过聚焦测试、外部请求缓存相关测试、
  `ruff check src tests`、`git diff --check` 和全量 `pytest -q`（824 passed）。
  真实 `aits ops daily-run` 默认 as-of `2026-05-19` 已 15/15 PASS，Run ID 为
  `daily_ops_run:2026-05-19:20260520T165804Z`；`prices_daily.csv` 中 `^VIX` 最新观测为
  `2026-05-19`，`data_quality_2026-05-19.md` 为 PASS。
- 2026-06-09：从 `VALIDATING` 改为 `DONE`。当前主价格缓存中 `^VIX` 覆盖
  `2018-01-02` 至 `2026-06-05`；`aits validate-data --as-of 2026-06-05`
  为 `PASS`，错误 0、警告 0、信息 12；同日 `score-daily --as-of 2026-06-05
  --skip-risk-event-openai-precheck` 未被 VIX stale 阻断。复跑
  `tests/test_market_data.py tests/test_external_request_cache.py tests/test_data_download.py -q`
  为 25 passed，相关 ruff 和 compileall 通过。该后续真实缓存证据确认旧
  `VIX_History.csv` 响应不再把 `^VIX` 锁定在 2026-05-11。
