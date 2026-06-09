# TRADING-033A：Operator Brief Notification Approval Gate Real Artifact Observation

状态：`BLOCKED_EXTERNAL`

最后更新：2026-06-09

关联任务：`TRADING-033A`

## 背景

`TRADING-033` 已完成只读 approval gate builder、CLI、JSON/Markdown/latest/run.log、
稳定 preview hash、approval marker 校验、dashboard 只读卡片、system flow、artifact catalog 和专项测试。
当前剩余缺口不是实现缺失，而是需要在真实 `TRADING-032` dispatch preview artifacts 与真实本地
manual approval marker 下观察 approval gate 结果，确认真实 preview hash、marker metadata、
过期/不匹配/缺失状态和 safety block 仍能 fail closed 或进入人工复核。

## 范围

1. 使用已有真实 `TRADING-032` dispatch preview artifact 运行：
   `python scripts/run_operator_brief_notification_approval_gate.py --date YYYY-MM-DD`。
2. 对同一真实 preview 记录无 marker 场景：
   - `approval_gate_status`；
   - `allowed_to_enter_dispatch`；
   - `dispatch_preview_summary.final_status`；
   - `hashes.dispatch_preview_hash`；
   - dashboard 只读卡片是否读取同日 TRADING-033 artifact。
3. 在 owner 明确人工复核后，使用本地 manual approval marker 记录：
   - `approved`；
   - `approved_by`；
   - `approved_at`；
   - `expires_at`；
   - `preview_hash`；
   - `hash_matches`；
   - `expired`；
   - `approval_gate_status`。
4. 对 marker 缺失、过期、不匹配、非法 JSON、路径逃逸、preview safety blocked 或 preview blocked 的
   真实输入保持 fail-closed，不修改 preview、不自动审批、不自动发送。

## 边界

- 不发送 email。
- 不创建或修改 Gmail draft。
- 不调用 SMTP、Slack、Discord、Telegram、webhook 或 mobile push。
- 不运行 operator brief。
- 不运行 notification draft generator。
- 不运行 delivery preflight。
- 不运行 dispatch preview。
- 不运行 market / backtest / scoring / data download pipeline。
- 不执行 apply、rollback、broker、replay 或 trading。
- 不读取 `.env` secrets 或私密邮箱。
- 不把 `allowed_to_enter_dispatch=true` 解释为已发送。

## 验收标准

- 至少一个真实 `TRADING-032` artifact 日期完成 approval gate 观察并记录字段级结果。
- 若 owner 提供 manual marker，marker 必须匹配当前 preview hash 且未过期，才能输出 `APPROVED`。
- 无 marker 必须输出 `APPROVAL_REQUIRED`；过期 marker 必须输出 `APPROVAL_EXPIRED`；
  hash 不匹配必须输出 `APPROVAL_MISMATCH`；缺 preview 或 invalid marker 必须输出 `BLOCKED`；
  preview safety blocked 必须输出 `SAFETY_BLOCKED`。
- 输出固定 `production_effect=none`、`manual_review_only=true`、`approval_gate_only=true`、
  `read_only=true`。
- 所有 sent / Gmail / webhook / broker / replay / trading / pipeline flags 保持 false。
- dashboard 只读读取已生成 TRADING-033 artifact，不运行 approval gate 或上游生成器。

## 状态记录

- 2026-06-09：新增并进入 `BLOCKED_EXTERNAL`。原因：`TRADING-033` 当前已通过
  数据质量门、临时 fixture `APPROVED` / `APPROVAL_REQUIRED` / `BLOCKED` smoke、目标 pytest、Ruff 和
  scoped Black；但本机当前没有可审计真实 `TRADING-032` dispatch preview artifacts 和真实人工
  approval marker，不能把真实 artifact approval gate 观察并入基线完成。
