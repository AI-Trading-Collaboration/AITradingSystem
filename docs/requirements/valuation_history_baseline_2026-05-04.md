# 估值历史样本 baseline 方案

状态：BASELINE_DONE

最后更新：2026-05-04

关联任务：`DATA-001`、`DATA-003`、`VALUATION-002`、`BTINPUT-001`

## 背景

`DATA-001` 原阻塞点是 FMP 本地估值快照和 analyst-estimates 历史样本需要自然积累。系统已经能拉取 FMP 当前估值、FMP historical key-metrics/ratios 回填分布，并能在报告中区分 `captured_snapshot`、`backfilled_history_distribution` 和严格历史可见性。

owner 决策：先实现低成本 baseline，提高当前日报的估值/预期覆盖；完整生产级 `DONE` 仍需要真实 point-in-time estimates/archive，不能用采集日回填数据伪造历史可见性。2026-05-04 更新：历史 PIT 数据采购、历史接入和历史补数暂缓；从当前日期开始自建 forward-only PIT 快照归档，由 `DATA-003` 承接。

## 方案判断

第一阶段采用 EODHD Earnings Trends 补 `eps_revision_90d_pct`。该接口返回当前 EPS consensus 以及 7/30/60/90 天前 trend 字段，可直接生成当前评分可用的 90 日 EPS 修正信号。

估值分位继续使用现有 FMP historical valuation 回填分布和本地估值快照历史。若后续接入 EODHD Fundamentals 或 historical market cap 补估值指标，也必须记录为采集日可见或回填历史分布，不得提升为严格 point-in-time。

## 数据可见性边界

|输入|baseline 用途|回测可见性|
|---|---|---|
|EODHD Earnings Trends|当前 `eps_revision_90d_pct`、EPS/revenue trend 审计|采集日后可见，不用于采集日前严格 PIT 回测|
|FMP historical key-metrics/ratios|当前估值分位历史分布辅助|`backfilled_history_distribution`，不伪装为 vendor archive|
|自建 forward-only PIT 快照|未来 estimates、price target、ratings 和 earnings calendar 样本|本系统成功采集、校验并写入后可见；不能倒用于采集日前|
|真实 PIT estimates archive|完整 `DONE` 所需|可用于严格历史回测，前提是供应商提供可审计 as-of 快照|

## 实施步骤

1. 新增 EODHD Earnings Trends provider、原始 payload 写入和中文拉取报告。
2. 新增 `aits valuation fetch-eodhd-trends`，将 EODHD trends 合并进当前可见基础 valuation snapshot 的 expectation metrics。
3. 合并快照必须记录 EODHD 和基础快照来源，写入 `source_type=paid_vendor`、`point_in_time_class=captured_snapshot`、`history_source_class=vendor_current_trend`、`backtest_use=captured_at_forward_only`。
4. 报告和数据源目录必须记录 endpoint、请求参数、下载时间、row count、checksum、provider symbol alias 和限制。
5. 补充测试，覆盖 EPS 90 日修正计算、raw payload 审计、CLI 写入和不能提升为严格 PIT 的元数据。

## 验收标准

- `score-daily` 可读取 EODHD trends 生成的 `eps_revision_90d_pct` 快照输入。
- EODHD 拉取报告显示 provider、endpoint、请求 ticker、row count、checksum、生成快照数、错误和警告。
- 缺少 `epsTrend90daysAgo`、当前 EPS 非正数或 90 日前 EPS 非正数时停止生成该指标并写入警告。
- `docs/system_flow.md` 和 `config/data_sources.yaml` 说明 EODHD baseline 的数据流、缓存和 PIT 限制。
- 完整 `DONE` 条件仍保留：只有真实 PIT estimates/archive、等价供应商快照，或自建 forward-only 快照自然积累出的可审计历史，才能支持严格历史回测中的 EPS 修正结论。

## 状态记录

- 2026-05-04：创建 baseline 方案文档。owner 批准先接入 EODHD Earnings Trends 补当前 EPS 90 日修正；严格历史回测仍依赖真实 PIT estimates/archive。
- 2026-05-04：baseline 已完成。新增 `aits valuation fetch-eodhd-trends`、EODHD 原始 payload 审计、合并估值快照、中文拉取报告、数据源目录、系统流图和测试；`python -m ruff check src tests` 与 `python -m pytest -q` 通过。完整 `DONE` 仍依赖真实 PIT estimates/archive。
- 2026-05-04：owner 决策不购买或伪造历史 PIT estimates/archive；历史采购、历史接入和历史补数继续暂缓，baseline 继续明确 `captured_at_forward_only` 和回填分布限制；forward-only 自建快照归档另由 `DATA-003` 和 `docs/requirements/pit_snapshot_archive_2026-05-04.md` 承接。
