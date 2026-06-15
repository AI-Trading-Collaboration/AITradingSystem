# TRADING-355 Point-in-Time Source Manifest

最后更新：2026-06-16

## 背景

当前系统已有 `download_manifest.csv`、PIT raw snapshot manifest、data source catalog 和 data quality gate，但缺少一个 source-level、跨数据域的 point-in-time source manifest，无法统一说明每个数据源的 retrieval time、effective date、revision risk、PIT quality grade、cache/checksum、refresh policy 和 validation policy。

本任务建立 manifest contract，不试图一次性修复所有 PIT 数据问题。

## 目标

- 新增 source-level PIT source manifest schema。
- 每个 source 记录 source name、retrieval time、effective date、revision risk、PIT quality grade、cache path、checksum、refresh policy 和 validation policy。
- PIT quality grades 固定为 `STRONG_PIT|APPROX_PIT|NON_PIT|UNKNOWN`。
- 新增 report CLI 和 validate CLI。
- 接入 Reader Brief、report registry、artifact catalog、README、operations runbook、system flow 和 task register。
- 添加 focused tests。

## 非目标

- 不补造历史 retrieval time 或 vendor archive。
- 不改变 `download-data`、`validate-data`、PIT raw snapshot、score、backtest、paper-shadow 或 broker workflow。
- 不把 `APPROX_PIT`、`NON_PIT` 或 `UNKNOWN` 自动提升为 production-grade PIT。
- 不更改任何投资-facing score、threshold、position gate 或 backtest decision。

## Manifest Contract

默认输出目录：`reports/data_governance/pit_source_manifest/<manifest_id>/`

必需文件：

- `pit_source_manifest.json`
- `pit_source_manifest.md`
- `pit_source_manifest_validation.json`
- `pit_source_manifest_validation.md`
- `reader_brief_section.md`

每个 source record 必须包含：

- `source_id`
- `source_name`
- `retrieval_time`
- `effective_date`
- `revision_risk`
- `pit_quality_grade`
- `cache_path`
- `checksum`
- `refresh_policy`
- `validation_policy`

## 验收标准

- `aits data-sources pit-manifest report` 生成 manifest/report artifact。
- `aits data-sources pit-manifest validate --latest` 校验 schema、grade、cache/checksum、refresh/validation policy 和 safety boundary。
- Reader Brief 只读 latest manifest，显示 status、source count、grade counts、non-strong source ids 和 report path。
- Focused tests 覆盖 manifest generation、invalid grade fail、CLI report/validate 和 Reader Brief summary。
- 文档、report registry、artifact catalog、operations runbook、system flow、task register 同步。

## 进展记录

- 2026-06-16：任务创建并进入实现；范围限定为 source-level data governance manifest contract，不改变数据源、缓存或投资解释逻辑。
- 2026-06-16：实现 `src/ai_trading_system/pit_source_manifest.py`、`aits data-sources pit-manifest report/validate`、Reader Brief `PIT Source Manifest` 摘要、report registry、artifact catalog、README、operations runbook、system flow 和 focused tests。真实 artifact `pit_source_manifest_2026-06-16_8fe811d76617ecd9` 生成成功，`status=PASS_WITH_WARNINGS`、`source_count=30`、`STRONG_PIT=10`、`APPROX_PIT=18`、`NON_PIT=1`、`UNKNOWN=1`、`error_count=0`、`warning_count=36`；warning 作为治理调查项保留，不自动支持 backtest/scoring/paper-shadow/production 结论。
