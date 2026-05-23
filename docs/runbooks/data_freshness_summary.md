# Data Freshness Summary Runbook

最后更新：2026-05-23

## 1. 目的

TRADING-024 生成只读 Data Freshness Summary，用于回答关键数据源和运维 artifacts
是否存在、是否足够新鲜、哪些缺失或过期，以及当前数据 freshness 是否足以支持今日系统输出。

它只扫描已有文件，不下载数据、不刷新缓存、不运行 market/backtest/scoring 或 TRADING-018B
到 TRADING-023 的任何任务。

## 2. 生成命令

```bash
python scripts/run_data_freshness_summary.py --date 2026-05-23
```

常用参数：

```bash
python scripts/run_data_freshness_summary.py \
  --date 2026-05-23 \
  --data-root data \
  --lookback-days 7 \
  --freshness-days 2 \
  --market-date 2026-05-22 \
  --include-optional-sources
```

`--market-date` 用于周末或节假日，把数据 freshness 的参考日设为最新市场日期。
`--include-optional-sources` 会额外扫描 optional market/backtest/cache/support sources；optional
缺失只产生 warning，不应导致整体 `MISSING`。

## 3. 输出

- `data/derived/data_freshness/data_freshness_summary_YYYY-MM-DD.json`
- `data/derived/data_freshness/data_freshness_summary_YYYY-MM-DD.md`
- `data/derived/data_freshness/logs/data_freshness_summary_run_YYYY-MM-DD.json`
- `data/derived/data_freshness/logs/data_freshness_summary_run_YYYY-MM-DD.md`

所有输出固定：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "data_freshness_only": true,
  "read_only": true,
  "data_downloaded_by_freshness_check": false,
  "pipelines_executed_by_freshness_check": false,
  "apply_executed_by_freshness_check": false,
  "rollback_executed_by_freshness_check": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false,
  "safe_for_scheduler": true
}
```

## 4. freshness_status 含义

|Status|含义|处理|
|---|---|---|
|`OK`|所有 included required sources 存在且足够新鲜，无 critical。|继续观察。|
|`WATCH`|required sources 可用，但 optional 缺失/过期或存在 warning/unknown。|人工检查 warning 是否符合预期。|
|`STALE`|required source 存在但超过 freshness threshold，或 status field 表示 action/insufficient data。|不要依赖该 freshness 结论；按来源 runbook 重新生成上游。|
|`MISSING`|required source 缺失，且没有 critical。|先生成缺失的上游 artifact。|
|`CRITICAL`|artifact 出现 critical status 或异常执行/下载/交易字段。|暂停依赖相关输出，先做安全审查。|
|`ERROR`|TRADING-024 自身运行或 required artifact 解析失败。|检查 run log 和 invalid JSON。|

## 5. source status 含义

|Status|含义|
|---|---|
|`FRESH`|artifact found、data date fresh、decision value healthy 或 source 无 status field。|
|`WATCH`|artifact found 且未过期，但 decision value 属于 watch；或 optional artifact stale。|
|`STALE`|required artifact 过期，或 decision value 属于 action/insufficient data。|
|`MISSING`|required artifact missing。|
|`OPTIONAL_MISSING`|optional artifact missing。|
|`CRITICAL`|decision critical，或安全字段显示异常下载、pipeline、broker、replay、trading、apply/rollback/promotion。|
|`UNKNOWN`|artifact found，但无法派生 data date，或 status field 缺失/未映射。|
|`ERROR`|artifact JSON 无法解析或读取失败。|

## 6. artifact missing 判定

每个 source 使用 registry 中的 `expected_artifact_glob` 查找候选文件。找不到时：

- `required=true` 输出 `MISSING`，整体通常为 `MISSING`。
- `required=false` 输出 `OPTIONAL_MISSING`，整体最多进入 `WATCH`。

TRADING-024 不会为缺失 source 补造 artifact。

## 7. artifact stale 判定

每个 source 使用 `stale_after_days`，未配置时使用 CLI `--freshness-days`，默认 2 天。

如果 `age_days > stale_after_days`：

- required source 输出 `STALE`，整体为 `STALE`。
- optional source 输出 `WATCH`。

生命周期类 artifact 可以配置较长阈值，例如 lifecycle audit 30 天。

## 8. data_date 判定

日期抽取优先级：

1. artifact JSON 中的 `date_fields`，例如 `date`、`data_date`、`as_of_date`、`report_date`、`generated_for_date`。
2. 文件名日期，支持 `YYYY-MM-DD` 和 `YYYY_MM_DD`。
3. 文件 modified time。

如果仍无法派生日期，该 source 输出 `UNKNOWN` 并记录 `Unable to derive data date.`。

## 9. CRITICAL 处理

如果 Markdown 顶部出现 `CRITICAL: Data Freshness Issue Detected`：

1. 先查看 JSON 的 `alerts.critical` 和 `critical_sources`。
2. 检查是否出现 `broker_execution=true`、`replay_execution=true`、`trading_execution=true`、
   `data_downloaded_by_freshness_check=true` 或 `pipelines_executed_by_freshness_check=true`。
3. 如果是 source artifact 自身状态 critical，回到该 source 的 runbook 排查。
4. 不要通过手工编辑 TRADING-024 artifact 清除 critical。

## 10. STALE 处理

如果 Markdown 顶部出现 `Stale Required Data Detected`：

1. 查看 `stale_sources` 和每个 source 的 `data_date`、`age_days`、`stale_after_days`。
2. 判断 `--market-date` 是否应设为最近一个交易日。
3. 需要重新生成时，只能按上游 source 自己的 runbook 运行，不从 TRADING-024 内触发。

## 11. MISSING 处理

如果 Markdown 顶部出现 `Required Data Missing`：

1. 查看 `missing_required_sources`。
2. 确认该 required source 是否应在当天存在。
3. 需要生成时，按该 source 所属任务的 runbook 手动运行。

## 12. 为什么不下载数据

Data freshness 是检查层，不是数据生产层。它下载或刷新数据会模糊审计边界：

- 无法区分 freshness 检查前后的数据状态。
- 可能绕过 `aits validate-data` 的必需质量门禁。
- 可能让 scheduler 间接触发上游 pipeline。

因此 TRADING-024 只读扫描已有文件。

## 13. 为什么不运行 pipeline

TRADING-024 的作用是让 operator 和 dashboard 看到“当前已有输入是否新鲜”。如果它自动运行
018B-023、market、backtest 或 scoring pipeline，就会把观察层变成执行层，破坏安全边界和责任归属。

## 14. 后续接入 TRADING-022

TRADING-024 先生成稳定 artifact。后续 TRADING-025 可让 TRADING-022 只读读取：

- `data/derived/pipeline_health/pipeline_health_summary_YYYY-MM-DD.json`
- `data/derived/data_freshness/data_freshness_summary_YYYY-MM-DD.json`

这样 operator brief 可以把 `pipeline_health.status` 和 `data_freshness.status` 从 `UNKNOWN`
升级为明确状态，同时仍保持只读。
