# Daily Ops Runbook

最后更新：2026-05-10

本文是 `aits ops daily-run` 的人工可交接运行手册。它不替代数据质量门禁、日报、pipeline health、secret hygiene 或 evidence bundle；它只规定什么时候跑、失败时看什么、哪些输出是正式结论、哪些只是审计附录。

## 运行节奏

|频率|命令|目的|
|---|---|---|
|交易日前/盘前|`aits ops daily-plan --as-of YYYY-MM-DD --fail-on-missing-env`|确认环境变量、缓存路径、预期 artifact 和当日是否交易日。|
|交易日盘后|`aits ops daily-run --as-of YYYY-MM-DD`|执行下载、PIT、SEC、估值、日报、pipeline health 和 secret scan。|
|每周|`aits reports investment-review --period weekly --as-of YYYY-MM-DD`、`aits feedback loop-review --as-of YYYY-MM-DD`|复核结论变化、outcome、learning queue、shadow maturity 和 blocked tasks。|
|每月|`aits reports investment-review --period monthly --as-of YYYY-MM-DD`、必要时运行回测/覆盖诊断|复核规则、数据源、gate 松紧、样本成熟度和 owner action。|

建议盘后运行时间放在美股收盘且数据供应商 EOD 数据稳定后。具体云 VM 时区和时刻由 owner 后续决定；当前不在 GitHub Actions 中配置生产 cron。

## 正式输出

`daily-run` 的正式运行归档为：

```text
outputs/runs/YYYY-MM-DD/<run_id>/
  manifest.json
  reports/
  traces/
  metadata/
```

目录名使用 filesystem-safe run id；原始 run id 会写入 `manifest.json` 和 daily ops metadata。

过渡期仍可在 `outputs/reports/` 看到 legacy mirror。投资阅读入口优先级：

1. `daily_score_YYYY-MM-DD.md`：趋势判断日报和 Decision Card。
2. `data_quality_YYYY-MM-DD.md`：市场和宏观缓存质量门禁。
3. `pipeline_health_YYYY-MM-DD.md`：关键 artifact 健康，不等于投资结论有效。
4. `daily_ops_run_YYYY-MM-DD.md` 与 `daily_ops_run_metadata_YYYY-MM-DD.json`：运行步骤和脱敏元数据。
5. `manifest.json`：本次 run 的输入、输出、checksum、legacy mirror 和 visibility cutoff。

## 阻断规则

必须停止并先排查：

- `aits validate-data` 或 `score-daily` 内部同一路径数据质量门禁失败。
- SEC metrics、估值快照、风险事件发生记录、execution policy 或 rule card 校验失败。
- 必需环境变量缺失导致 `daily-plan` 或 `daily-run` 为 `BLOCKED_ENV`。
- `score-daily`、`pipeline health` 或 secret scan 报告状态为 `FAIL`。
- OpenAI 风险事件预审在启用状态下 fail closed。

可降级但必须披露：

- 显式 `--skip-risk-event-openai-precheck`，日报必须显示未执行自动预审。
- PIT 抓取入口层失败但已用 `--continue-on-failure` 写出失败报告；失败快照不得作为可用 PIT 输入。
- 休市日模式跳过 `score-daily`，只保留官方政策/地缘来源抓取和健康检查。
- 第二数据源覆盖不足，报告必须保留 source limitation，不能写成跨源核验完成。

## 排查入口

|现象|优先检查|
|---|---|
|数据质量失败|`outputs/reports/data_quality_YYYY-MM-DD.md`、download manifest、provider health。|
|PIT checksum mismatch|`pit_snapshots_validation_YYYY-MM-DD.md`、`fmp_forward_pit_fetch_YYYY-MM-DD.md`、raw payload 路径。|
|日报没有生成|`daily_ops_run_YYYY-MM-DD.md` 的阻断步骤和对应子命令报告。|
|报告存在但结论不可用|日报“结论使用等级”、Decision Card 的 Data Gate、人工复核摘要。|
|OpenAI 预审失败|`risk_event_prereview_openai_YYYY-MM-DD.md` 和本地 request cache，不保存 API key。|
|疑似 secret|`secret_hygiene_YYYY-MM-DD.md`，只输出脱敏片段。|

## 调度示例

systemd timer 示例只作为部署参考，真实路径、用户、环境文件和时间由 owner 在云 VM 决策后确认：

```ini
[Unit]
Description=AITradingSystem daily ops

[Service]
Type=oneshot
WorkingDirectory=/opt/AITradingSystem
EnvironmentFile=/opt/AITradingSystem/.env
ExecStart=/opt/AITradingSystem/.venv/bin/aits ops daily-run
```

```cron
# 美股盘后示例；具体时区和数据稳定窗口需 owner 确认
30 22 * * 1-5 cd /opt/AITradingSystem && .venv/bin/aits ops daily-run
```

凭据不得写入仓库；stdout/stderr 可由系统日志保存，但正式审计以 `outputs/runs/.../manifest.json`、daily ops metadata 和各质量报告为准。
