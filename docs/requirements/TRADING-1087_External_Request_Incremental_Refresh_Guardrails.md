# TRADING-1087: External Request Incremental Refresh Guardrails

## 背景

2026-06-26 外部请求审查确认，`Marketstack` 免费月度额度被每日价格
下载重复消耗。本地响应头显示 `x-quota-limit=10000`、
`x-quota-remaining=-688`；自 2026-06-13 起，本地缓存记录 451 个
`Marketstack / eod_daily_prices` page request，`x-increment-usage` 合计
10687。

根因不是外部请求缓存完全失效。`external_request_cache` 按 provider、
api family、endpoint、params 等精确 identity 命中缓存；但每日调度使用
`aits download-data --start 2018-01-01 --end <latest>`，价格 provider 又把
`date_to` / `to` 写入缓存 identity。每个交易日 `end` 改变后，历史全量
窗口变成新的缓存键，导致已下载过的历史页再次请求供应商。

同类风险不只存在于 `Marketstack`：

|Provider / API|审查结论|风险|
|---|---|---|
|`Marketstack / eod_daily_prices`|按全历史 `date_from`/`date_to` 和 `offset` 分页；2026-06-24～26 每日约 52 个 page request，额度消耗约 1300 units/day|P0：已超过免费额度|
|`Financial Modeling Prep / eod_daily_prices`|每个 symbol 每日请求 non-split-adjusted 与 dividend-adjusted 两个 endpoint，`to` 改变导致每日新缓存键|P0/P1：同类重复请求和供应商成本风险|
|`Cboe Global Markets / vix_daily_prices`|公开 full CSV URL 本身稳定，但缓存 identity 人为包含 `start`/`end`，导致每日新缓存键|P2：低配额风险但不必要|
|`Federal Reserve Economic Data / fredgraph_csv`|已有基于最新 observation 的 tail refresh 逻辑|低风险，应作为回归保护基线|
|FMP valuation / SEC fundamentals|多数 endpoint 缓存键稳定或每日请求量低|低风险，保留监控|

## 目标

1. 价格下载从全历史重复请求改为基于本地缓存覆盖范围的增量刷新。
2. 在每日调度前增加 provider request budget / quota preflight，额度不足时
   fail closed，而不是运行到一半才触发供应商超额。
3. 下游 manifest、quality report 或 daily ops summary 必须披露 request
   budget、cache hit/miss、provider quota status 和 data-quality status。
4. 保留 `FMP` 主价格与 `Marketstack` 第二行情源 reconciliation；不得把永久
   跳过 `Marketstack` 当作默认修复。

## 范围

### A. 增量价格刷新

- `FMP` 主价格 provider 必须基于已有 `prices_daily.csv` / manifest 覆盖
  计算每个 ticker 的缺口，只请求缺口区间或带审计说明的受控 overlap。
- `Marketstack` 第二行情源必须基于已有 `prices_marketstack_daily.csv`
  覆盖计算缺口，分页请求应随缺口窗口缩小，不再每天扫描 2018 起全历史页。
- `Cboe VIX` 应使用稳定 URL/content identity、conditional GET 或明确的
  coverage-aware 缓存策略，避免仅因 `end` 变化写入新 full CSV cache key。
- 新旧数据合并必须去重并保留 provider、endpoint、request params、row count、
  checksum、download timestamp 和 coverage 审计字段。

### B. 请求预算与 fail-closed 护栏

- 每次 live provider 请求前估算 request/page/unit 消耗。
- 读取可用的 quota header，并在输出中记录 `limit`、`remaining`、
  `increment_usage` 和估算剩余额度。
- 若预计额度不足以完成当前 data-quality-required workflow，命令必须停止并
  说明 blocker；不得生成看似新鲜但缺第二来源或缺主源的下游结果。

### C. 可观测性

- `aits download-data` 和 daily scheduler summary 应披露每个 provider/api
  family 的 cache hits、cache misses、live request count、estimated/live
  quota usage。
- `aits validate-data` 后续 report 应能链接或引用对应下载质量与请求预算状态。
- 实现会影响数据输入到质量门禁和报告输出的路径，必须同步更新
  `docs/system_flow.md`。

### D. 回归保护

- 增加 fixture 或 fake requests tests，证明连续两天同一 universe 运行时：
  第一天 seed cache，第二天只请求新增尾部窗口。
- 保留 `FRED` tail refresh 行为的回归测试，防止把已有增量逻辑退回全历史请求。
- 对 `Marketstack` quota header 解析、page cost 估算和 fail-closed 分支加测试。

## 非目标

- 不通过永久禁用 `Marketstack`、降低数据质量门禁、删除 reconciliation 或改写
  历史 cache 来规避本问题。
- 不在本任务内更换供应商或升级付费套餐；如需供应商/套餐决策，另行记录
  owner 输入任务。
- 不改变投资结论、策略权重、paper-shadow、production 或 broker 边界。

## 验收标准

- 在本地已有 cache 覆盖到上一交易日时，连续运行
  `aits download-data --start 2018-01-01 --end <next_trading_day>` 不再对
  `FMP` / `Marketstack` / `Cboe VIX` 生成全历史滚动 cache key。
- `Marketstack` 每日 live 消耗与新增缺口规模相关，而不是与 2018 至今全历史页数
  相关；quota 预估不足时 fail closed 并输出可读 blocker。
- `prices_daily.csv`、`prices_marketstack_daily.csv`、macro/rates 输出和
  manifest 仍通过 `aits validate-data` 同源质量门禁。
- 下游 reports 明确披露 data-quality status、provider request budget status、
  cache hit/miss 和实际请求日期范围。
- focused parallel pytest、Ruff、compileall、`git diff --check` 和至少一个
  不消耗真实 quota 的 dry-run / fake-provider 验证通过。

## 进展记录

- 2026-06-26: 新增审查记录。确认 `Marketstack` 超额邮件可信，直接原因是每日
  全历史窗口导致缓存键滚动；`FMP` EOD 与 `Cboe VIX` 有同类重复请求模式；
  `FRED` 已有 tail refresh，可作为修复参考。
- 2026-06-26: owner 要求先推进修复；任务进入 `IN_PROGRESS`。实现范围优先
  覆盖 `download-data` 的增量价格刷新、Cboe 稳定缓存 identity、request
  budget / cache hit-miss 可观测性，以及不消耗真实 quota 的回归测试。
- 2026-06-26: 实现完成并转入 `VALIDATING`。`download_daily_data` 现在先读取
  既有主价格/Marketstack CSV 覆盖范围，按 ticker 计算缺口窗口，只请求缺失尾部
  或新增 ticker；写回时合并去重完整 CSV。Marketstack 在 live request 前读取最近
  quota response header 并估算 `x-increment-usage`，不足时 fail closed。Cboe VIX
  cache identity 改为稳定 full-history CSV identity，不再随 `start/end` 滚动。
  `external_request_cache` 新增 trace context，`download-data` 成功摘要和 manifest
  记录 cache hits/misses、live request count、quota headers 和 request budget
  status。验证通过 Ruff、py_compile、focused parallel pytest；额外
  `tests/test_ops_daily.py tests/test_scheduled_tasks.py` 暴露 2 个与本任务无关的既有
  forward-evidence 调度期望失败，本批未修改调度顺序测试。
