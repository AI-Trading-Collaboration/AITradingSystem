# 云端持续运行与每日调度

任务 ID：`OPS-003`

状态：`BASELINE_DONE`

最后更新：2026-05-06

## 背景

当前系统的定时运行仍依赖开发机开机、联网、环境变量可用以及本地缓存路径稳定。对 forward-only PIT 快照、市场数据质量门禁、日报和告警来说，缺跑日期会降低样本连续性和审计可信度，不能事后伪装为当时可见数据。

迁移到云 VM 可行，但不能先接入临时脚本绕过现有质量门禁。云端运行必须复用现有 CLI 和报告链路，并保留 provider、endpoint、请求参数、row count、checksum、质量报告和 artifact 路径。

## 设计原则

- 先做云厂商无关的每日运行入口，再做具体 VM 部署。
- 每个下游步骤必须看到 `aits validate-data` 或同一质量门禁的通过结果。
- 缺少关键凭据、输入缓存、PIT manifest 或质量报告时 fail closed，不做静默跳过。
- 运行健康只表示 pipeline artifact 可用，不等于投资结论有效。
- 报告和 CLI 摘要默认使用中文；ticker、命令、路径、状态码保持英文。

## 阶段拆解

|阶段|状态|目标|验收标准|
|---|---|---|---|
|1. 可调度每日运行计划|BASELINE_DONE|新增云厂商无关的 `aits ops daily-plan`，输出每日运行步骤、凭据需求、质量门禁、artifact 路径和阻断关系|命令可按 `as_of` 生成 Markdown 计划；缺少 `FMP_API_KEY`、`MARKETSTACK_API_KEY`、`OPENAI_API_KEY` 等关键凭据时在计划中明确显示；计划包含 `download-data`、PIT 抓取/校验、`score-daily`、`ops health`、`security scan-secrets` 的顺序|
|2. 结构化 run log|READY|记录每个步骤的开始/结束时间、退出码、耗时、stdout/stderr 摘要、报告路径和失败排查入口|失败时有机器可读 JSON/CSV run log；不会覆盖已有报告；可被 `ops health` 或后续告警读取|
|3. 可执行每日 orchestrator|READY|在 run log 基础上新增 `aits ops run-daily`，按计划顺序执行命令并在首个失败步骤停止|默认 fail closed；支持 `--dry-run`；支持离线排查时显式跳过 OpenAI 预审但必须在报告中声明|
|4. 云 VM 部署 runbook|READY|补 Linux VM 的 systemd timer/cron 示例、工作目录、Python 环境、secret 注入、日志路径和数据盘挂载策略|新机器能按 runbook 复现环境；不要求开发机开机；凭据不写入仓库|
|5. 持久化、备份和恢复|READY|定义 `data/raw`、`data/processed`、`outputs` 的云盘/对象存储备份策略和恢复演练|备份有保留周期、checksum 校验和恢复步骤；恢复后 `aits validate-data` 与 `aits ops health` 能复核|
|6. 通知和运营确认|BLOCKED_OWNER_INPUT|接入 owner 选择的邮件、IM 或监控渠道，并定义静默时间、确认/解除和重复抑制策略|失败告警能送达；告警状态可审计；通知策略不会改变评分、仓位、回测或执行建议|

## 推荐执行顺序

1. 先实现 `aits ops daily-plan`，让本地和云端共用同一份每日运行顺序说明。
2. 再实现结构化 run log，避免调度器只有纯文本日志。
3. 在 run log 稳定后实现真实 orchestrator。
4. 最后落地云 VM 的 systemd/cron、备份和通知。

## 开放问题

- 云厂商和区域：待 owner 确认。
- VM 规格和预算：待 owner 确认；当前系统适合先用普通 Linux VM。
- 调度时间：建议美股收盘后且供应商 EOD 数据稳定后运行，具体时间待 owner 确认。
- 通知渠道：待 owner 确认邮件、Slack、Telegram 或其他渠道。
- secret 管理：待 owner 确认使用云厂商 secret manager、环境文件还是外部密钥系统。
- 缓存持久化：待 owner 确认云盘、对象存储和备份保留周期。

## 进展记录

- 2026-05-06：创建 `OPS-003`，拆分为每日运行计划、run log、orchestrator、云 VM runbook、备份恢复和通知运营六个阶段。第一阶段进入实现。
- 2026-05-06：第一阶段完成基础版：新增 `ops_daily` 模块、`aits ops daily-plan`、`outputs/reports/daily_ops_plan_YYYY-MM-DD.md`、系统流图和 README 说明；计划能显示 `BLOCKED_ENV`、显式跳过 OpenAI 预审/PIT/secret scan 的限制，以及每个步骤的输出 artifact 和质量门禁。验证：`ruff check src tests` 通过，`pytest -q tests/test_ops_daily.py tests/test_pipeline_health.py` 通过。
