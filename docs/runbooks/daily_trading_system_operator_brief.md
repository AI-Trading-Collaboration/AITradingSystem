# Daily Trading System Operator Brief Runbook

最后更新：2026-05-24

## 1. 目的

TRADING-022 生成只读 Daily Trading System Operator Brief，把 TRADING-021 参数治理
daily digest、TRADING-023 pipeline health summary、TRADING-024 data freshness summary、
可选 market report 和 weight / governance artifacts 汇总成每日系统级运维简报。

它回答的是系统当天是否健康、数据是否新鲜、哪些 artifacts 缺失、是否需要人工处理，以及今天能否
信任系统输出。它不生成新的投资判断，也不执行任何 pipeline。

## 2. 如何生成 operator brief

```bash
python scripts/run_daily_trading_system_operator_brief.py --date 2026-05-23
```

默认输出：

```text
data/derived/operator_briefs/
  daily_trading_system_operator_brief_YYYY-MM-DD.json
  daily_trading_system_operator_brief_YYYY-MM-DD.md

data/derived/operator_briefs/logs/
  daily_trading_system_operator_brief_run_YYYY-MM-DD.json
  daily_trading_system_operator_brief_run_YYYY-MM-DD.md
```

常用参数：

```bash
python scripts/run_daily_trading_system_operator_brief.py \
  --date 2026-05-23 \
  --data-root data \
  --lookback-days 7 \
  --parameter-governance-digest-file data/derived/weight_iterations/governance/digests/parameter_governance_daily_digest_2026-05-23.json \
  --pipeline-health-summary-file data/derived/pipeline_health/pipeline_health_summary_2026-05-23.json \
  --data-freshness-summary-file data/derived/data_freshness/data_freshness_summary_2026-05-23.json
```

`--fail-on-critical` 会在写出 artifact 后，如果存在 critical alerts 则以非零状态退出。
`--include-optional-artifacts` 用于扩展扫描可选 artifact 家族；缺少 optional artifacts 不会阻断
brief 生成。

默认不传 023/024 路径时，operator brief 会自动查找 lookback window 内最新
`pipeline_health_summary_YYYY-MM-DD.json` 和 `data_freshness_summary_YYYY-MM-DD.json`。查找失败不会
运行 TRADING-023/024，也不会触发 `INPUT_MISSING`，但会把对应 section 标为 `UNKNOWN` 并把
`brief_status` 降级为 `WATCH`。

## 3. brief_status 含义

|brief_status|含义|
|---|---|
|`OK`|TRADING-021 digest、TRADING-023 health 和 TRADING-024 freshness 均为 OK，且无立即人工动作。|
|`WATCH`|系统可继续使用，但存在 warning、digest/health/freshness watch，或 023/024 缺失。|
|`ACTION_REQUIRED`|存在 pending manual action、digest action required、pipeline action/incomplete，或 freshness stale/missing。|
|`URGENT`|存在 critical alert、pipeline/freshness critical 或 digest urgent，需要立即人工检查。|
|`INPUT_MISSING`|找不到必需的 TRADING-021 daily digest。|
|`INPUT_INVALID`|digest 无法解析或 `task_id` 不是 `TRADING-021`。|
|`SAFETY_BLOCKED`|TRADING-021/023/024 任一已加载输入 artifact 顶层安全字段异常，operator brief 只能生成 blocked 简报。|
|`ERROR`|operator brief 运行异常。|

## 4. summary_level 含义

|summary_level|来源|
|---|---|
|`NORMAL`|`brief_status=OK`|
|`WATCH`|`brief_status=WATCH`|
|`ACTION`|`brief_status=ACTION_REQUIRED`|
|`URGENT`|`brief_status=URGENT`|
|`UNKNOWN`|input missing / invalid / safety blocked / error|

## 5. 如何解读 Executive Summary

Executive Summary 展示 `brief_status`、`summary_level`、`can_trust_outputs_today`、
`manual_action_required` 和 headline。

`can_trust_outputs_today=true` 只表示 brief 当前没有发现阻断性输入、安全或 critical alert。它不是交易
授权，也不是自动执行许可。

## 6. 如何解读 Parameter Governance

Parameter Governance 部分直接来自 TRADING-021 digest，展示 digest status、governance state、
action required、action level 和 headline。TRADING-022 不重新计算参数治理状态。

## 7. 如何解读 Pipeline Health

Pipeline Health 只读取 TRADING-023 `pipeline_health_summary_YYYY-MM-DD.json`。缺失时显示
`UNKNOWN`，并把 `brief_status` 降级为 `WATCH`；operator brief 不运行 TRADING-023。

如果 summary 报告 `CRITICAL`，brief 输出 `URGENT`。如果报告 `ACTION_REQUIRED` 或 `INCOMPLETE`，
brief 输出 `ACTION_REQUIRED`。Markdown 会展示 required、missing、stale、critical 和 warning
pipeline 计数以及 TRADING-023 Markdown 报告路径。

## 8. 如何解读 Data Freshness

Data Freshness 只读取 TRADING-024 `data_freshness_summary_YYYY-MM-DD.json`。缺失时显示
`UNKNOWN`，并把 `brief_status` 降级为 `WATCH`；operator brief 不运行 TRADING-024，也不下载或刷新数据。

如果 summary 报告 `CRITICAL`，brief 输出 `URGENT`。如果报告 `STALE` 或 `MISSING`，brief 输出
`ACTION_REQUIRED`。Markdown 会展示 required、missing、stale、critical 和 warning source 计数以及
TRADING-024 Markdown 报告路径。

## 9. 如何处理 ACTION_REQUIRED

- 阅读 Pending Manual Actions 表。
- 检查 TRADING-021 digest 和关联 governance summary / web view。
- 检查 TRADING-023 pipeline health summary 和 TRADING-024 data freshness summary。
- 不要在缺少单独 approval artifact、danger flag 和 hash gate 的情况下运行 apply 或 rollback。

## 10. 如何处理 URGENT

- 暂停依赖自动输出做后续判断。
- 检查 critical alerts、TRADING-021 digest、TRADING-023 health、TRADING-024 freshness、TRADING-019 summary 和 TRADING-018F lifecycle audit。
- 确认没有意外的 broker / replay / trading execution。
- 不要通过编辑 brief 或 digest artifact 清除异常。

## 11. 如何处理 SAFETY_BLOCKED

`SAFETY_BLOCKED` 表示 TRADING-021 digest 或已加载的 TRADING-023/024 summary 不满足
TRADING-022 的前置安全字段：

- `production_effect=none`
- `digest_only=true`
- `governance_only=true`
- `apply_executed_by_digest=false`
- `rollback_executed_by_digest=false`
- `broker_execution=false`
- `replay_execution=false`
- `trading_execution=false`

TRADING-023 还必须保持 `pipeline_health_only=true`、`read_only=true`、
`pipelines_executed_by_health_check=false`。TRADING-024 还必须保持 `data_freshness_only=true`、
`read_only=true`、`data_downloaded_by_freshness_check=false`、
`pipelines_executed_by_freshness_check=false`。

先修复或重新生成异常的输入 artifact，再重新生成 operator brief。

## 12. 为什么 operator brief 不执行任何 pipeline

Operator brief 是每日阅读和运维聚合层。它的职责是汇总已经存在的状态，不负责创建状态。

如果 brief 自动运行 market、backtest、scoring 或 parameter governance pipeline，就会把“读取状态”和
“改变系统状态”混在一起，破坏审计边界，也可能掩盖上游失败。

## 13. 为什么 operator brief 不触发 broker / replay / trading

TRADING-022 固定：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "operator_brief_only": true,
  "read_only": true,
  "apply_executed_by_operator_brief": false,
  "rollback_executed_by_operator_brief": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false,
  "safe_for_scheduler": true
}
```

这保证它可以进入 scheduler，但不能成为交易执行入口。

## 14. 与 TRADING-021 的关系

TRADING-021 是参数治理 daily digest，回答“参数治理是否安全、是否有 pending apply / rollback、是否有
safety anomaly”。

TRADING-022 是系统级 operator brief，读取 TRADING-021 作为必需输入，并只读消费 TRADING-023
pipeline health summary 与 TRADING-024 data freshness summary。TRADING-022 不运行 TRADING-021、
TRADING-023、TRADING-024，也不运行 TRADING-019 / 020、018B-018F、market、backtest、scoring、
data download、broker、replay 或 trading execution。
