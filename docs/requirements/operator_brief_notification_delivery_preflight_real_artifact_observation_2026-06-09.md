# TRADING-031A：Operator Brief Notification Delivery Preflight Real Artifact Observation

状态：`BLOCKED_EXTERNAL`

最后更新：2026-06-09

关联任务：`TRADING-031A`

## 背景

`TRADING-031` 已完成只读 delivery preflight builder、CLI、JSON/Markdown/run log、
dashboard 只读卡片、system flow、artifact catalog 和专项测试。当前剩余缺口不是实现缺失，
而是需要在真实 `TRADING-030` notification draft artifacts 下观察 preflight 结果，确认
真实草稿链路、redaction、approval need 和 channel readiness 仍能 fail closed 或进入人工复核。

## 范围

1. 使用已有真实 `TRADING-030` metadata 与 draft files 运行：
   `python scripts/run_operator_brief_notification_delivery_preflight.py --date YYYY-MM-DD`。
2. 记录输出：
   - `preflight_status`；
   - `delivery_readiness`；
   - `notification_severity`；
   - metadata / email / chat / mobile artifact status；
   - draft validation、approval validation、channel readiness 和 safety validation；
   - dashboard 只读卡片是否读取同日 TRADING-031 artifact。
3. 对缺少、损坏、未脱敏或 safety 字段异常的真实输入保持 fail-closed，不补造草稿、不自动发送。

## 边界

- 不发送 email。
- 不创建或修改 Gmail draft。
- 不调用 SMTP、Slack、Discord、Telegram、webhook 或 mobile push。
- 不运行 operator brief。
- 不运行 notification draft generator。
- 不运行 market / backtest / scoring / data download pipeline。
- 不执行 apply、rollback、broker、replay 或 trading。
- 不读取 `.env` secrets 或私密邮箱。

## 验收标准

- 至少一个真实 `TRADING-030` artifact 日期完成 preflight 观察并记录字段级结果。
- 输出固定 `production_effect=none`、`manual_review_only=true`、
  `notification_delivery_preflight_only=true`、`read_only=true`。
- 所有 sent / Gmail / webhook / broker / replay / trading / pipeline flags 保持 false。
- dashboard 只读读取已生成 TRADING-031 artifact，不运行 preflight 或上游生成器。
- 若真实输入缺失或不安全，结论必须是 `INPUT_MISSING`、`INPUT_INVALID`、`BLOCKED` 或
  `SAFETY_BLOCKED`，不得绕过或自动发送。

## 状态记录

- 2026-06-09：新增并进入 `BLOCKED_EXTERNAL`。原因：`TRADING-031` 当前已通过
  数据质量门、临时 fixture PASS smoke、缺输入 fail-closed smoke、目标 pytest、Ruff 和
  scoped Black；但本机当前没有可审计真实 `TRADING-030` notification draft artifacts，
  不能把真实 artifact 观察并入基线完成。
