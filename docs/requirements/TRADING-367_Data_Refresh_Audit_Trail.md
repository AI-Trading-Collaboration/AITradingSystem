# TRADING-367 Data Refresh Audit Trail

最后更新：2026-06-16

## 背景

TRADING-354B、TRADING-366 已让 paper-shadow freshness 使用 latest complete market date 和
explicit staleness policy，但 owner 仍需要看到数据刷新与数据验证的运行证据：何时尝试刷新、来源是
什么、校验是否通过、缓存 checksum 与 row count 是否变化，以及 warning/error 是否需要人工复核。

当前系统已有 `download_manifest.csv`、`market_data_refresh_summary.json` 和
`data_quality_YYYY-MM-DD.md`，但缺少一个面向 paper-shadow 的统一 audit trail。

## 目标

- 为 `aits validate-data` 生成结构化 validation audit sidecar。
- 新增 data refresh audit report/validate CLI，聚合 latest market refresh、data validation 和
  cache metadata。
- 每条 audit record 至少记录 data type、source、start/end time、as-of date、status、checksum、
  record count、warning count 和 error count。
- 固定状态集合：`SUCCESS|SUCCESS_WITH_WARNINGS|FAILED|SKIPPED_MARKET_CLOSED|SKIPPED_NO_NEW_DATA`。
- 接入 Reader Brief、report registry、artifact catalog、README、operations runbook、system flow 和
  task register。
- 添加 focused tests。

## 非目标

- 不刷新数据、不调用供应商、不补造 price cache 或 market panel。
- 不替代 `download_manifest.csv` 或 PIT source manifest。
- 不放宽 `aits validate-data` 门禁。
- 不写 official target weights、paper account、broker/order 或 production state。

## Artifact Contract

默认输出目录：`reports/data_governance/data_refresh_audit/<audit_id>/`

必需文件：

- `data_refresh_audit.json`
- `data_refresh_audit.md`
- `data_refresh_audit_validation.json`
- `data_refresh_audit_validation.md`
- `reader_brief_section.md`

Validation sidecar 默认写入 `artifacts/data_refresh_audit/validation/`，用于记录
`aits validate-data` 本次门禁运行。

## 验收标准

- `aits validate-data` 运行后写入 validation audit sidecar。
- `aits data refresh-audit report --as-of YYYY-MM-DD` 生成统一 audit artifact。
- `aits data refresh-audit validate --latest` 校验 schema、status、checksums、record counts 和
  safety boundary。
- Reader Brief 展示 latest audit status、record count、failed/skipped counts 和 next action。
- Focused tests 覆盖 validation sidecar、audit report、invalid status validation 和 Reader Brief。
- 文档、report registry、artifact catalog、operations runbook、system flow、task register 同步。

## 进展记录

- 2026-06-16：任务创建并进入实现；范围限定为只读 audit/report，不执行 refresh、不降低
  validate-data 门禁、不产生生产动作。
- 2026-06-16：实现 `src/ai_trading_system/data_refresh_audit.py`、`aits validate-data`
  validation sidecar、`aits data refresh-audit report/validate`、Reader Brief `Data Refresh Audit`
  摘要、report registry、artifact catalog、README、operations runbook、system flow 和 focused
  tests。Artifact 只读聚合 validation sidecar、latest market refresh summary、price cache
  checksum/row count 和 U.S. market calendar skip reason；缺 validation sidecar fail closed；
  固定 `production_effect=none`、不刷新数据、不补造 cache、不触发 broker/order。
