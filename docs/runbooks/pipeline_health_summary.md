# Pipeline Health Summary Runbook

最后更新：2026-05-23

## 1. 目的

TRADING-023 生成只读 Pipeline Health Summary，用来回答近期 pipeline artifacts 是否存在、是否新鲜、
状态字段是否健康、是否出现 critical / action-required / insufficient-data / safety-blocked 状态，以及是否
存在需要人工检查的项目。

它只扫描已有 artifacts，不运行任何 pipeline，也不补造缺失结果。

## 2. 如何生成 pipeline health summary

```bash
python scripts/run_pipeline_health_summary.py --date 2026-05-23
```

默认输出：

```text
data/derived/pipeline_health/
  pipeline_health_summary_YYYY-MM-DD.json
  pipeline_health_summary_YYYY-MM-DD.md

data/derived/pipeline_health/logs/
  pipeline_health_summary_run_YYYY-MM-DD.json
  pipeline_health_summary_run_YYYY-MM-DD.md
```

常用参数：

```bash
python scripts/run_pipeline_health_summary.py \
  --date 2026-05-23 \
  --data-root data \
  --lookback-days 7 \
  --freshness-days 2 \
  --include-optional-pipelines
```

`--fail-on-critical` 会在写出 artifact 后，如果 `health_status=CRITICAL` 则以非零状态退出。

## 3. health_status 含义

|health_status|含义|
|---|---|
|`OK`|已扫描的 required artifacts 均存在、新鲜且状态健康。|
|`WATCH`|存在 optional missing/stale、required warning 或 status unknown，但未达到阻断级别。|
|`ACTION_REQUIRED`|required artifact stale，或 required pipeline 状态需要人工处理。|
|`CRITICAL`|发现 critical decision、unexpected execution flag、unexpected production effect 或 safety anomaly。|
|`INCOMPLETE`|required artifact 缺失，且未发现 critical artifact。|
|`ERROR`|required artifact 无法解析，或 summary 自身运行异常。|

## 4. pipeline status 含义

|status|含义|
|---|---|
|`HEALTHY`|artifact 存在、新鲜，且 status field 命中 healthy values。|
|`WATCH`|artifact 存在，但状态需要观察；optional artifact stale 也归入 WATCH。|
|`ACTION_REQUIRED`|状态字段命中 action values，需要人工检查。|
|`CRITICAL`|状态字段命中 critical values，或安全字段显示异常执行。|
|`MISSING`|required artifact 缺失。|
|`STALE`|required artifact 存在但超过 freshness threshold。|
|`OPTIONAL_MISSING`|optional artifact 缺失。|
|`UNKNOWN`|artifact 存在但无法映射状态字段。|
|`ERROR`|artifact 无法解析或读取失败。|

## 5. 如何判断 artifact missing

每个 pipeline 通过 registry 中的 `expected_artifact_glob` 查找候选文件。查找最新 artifact 时：

1. 优先从文件名提取 `YYYY-MM-DD` 或 `YYYY_MM_DD`。
2. 无法提取日期时使用 modified time。
3. 同日多个候选时选择 modified time 最新者。

找不到 required artifact 时，该 pipeline 为 `MISSING`，overall `health_status=INCOMPLETE`。
找不到 optional artifact 时，该 pipeline 为 `OPTIONAL_MISSING`，不会导致 `INCOMPLETE`。

## 6. 如何判断 artifact stale

每个 pipeline 可设置 `stale_after_days`。未设置时使用 CLI 的 `--freshness-days`，默认 2 天。

`age_days > stale_after_days` 时：

- required pipeline 标为 `STALE`，overall 升级为 `ACTION_REQUIRED`；
- optional pipeline 标为 `WATCH`。

apply result、rollback result 和 lifecycle audit 使用较长 freshness，因为这些 artifact 不一定每日生成。

## 7. 如何处理 CRITICAL

- 先阅读 Markdown 顶部 `CRITICAL: Pipeline Health Issue Detected`。
- 检查 `critical_pipelines` 和 `alerts.critical`。
- 确认是否有 `broker_execution=true`、`replay_execution=true`、`trading_execution=true`、只读任务
  `apply_executed=true` 或异常 `production_effect`。
- 不要通过编辑 health summary 清除异常；应回到源 artifact 和对应 runbook 调查。

## 8. 如何处理 ACTION_REQUIRED

- 检查 `missing_required_pipelines`、`stale_pipelines` 和相关 pipeline result 的 `blocking_reasons`。
- 如果需要重新生成上游 artifact，只能人工按对应 pipeline runbook 单独执行。
- TRADING-023 不得从 health check 内部调用任何上游脚本。

## 9. 为什么 TRADING-023 不运行任何 pipeline

Health summary 是状态读取层，不是状态创建层。让 health check 运行上游 pipeline 会把“发现问题”和
“改变系统状态”混在一起，破坏审计边界，也可能掩盖缺失、过期或失败的真实原因。

## 10. 后续如何接入 TRADING-022

当前 TRADING-023 已生成结构化 artifact。后续可以在 TRADING-022 中只读读取
`data/derived/pipeline_health/pipeline_health_summary_YYYY-MM-DD.json`，将 operator brief 的
`pipeline_health` 从 `UNKNOWN` 升级为结构化状态。

接入时仍必须保持 TRADING-022 只读，不得让 operator brief 触发 TRADING-023 或任何上游 pipeline。
