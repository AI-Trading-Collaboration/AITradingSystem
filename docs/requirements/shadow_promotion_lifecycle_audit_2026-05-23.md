# TRADING-018F：Promotion Lifecycle Audit Report

关联任务：`TRADING-018F`

最后更新：2026-05-23

## 背景

TRADING-018D/018E1/018E2/018E3 已形成 shadow promotion 的人工 proposal、
只读 preflight、显式 apply 和显式 rollback 闭环。当前仍缺少一个只读生命周期审计层，
把一次 promotion 从 proposal 到 preflight、apply、rollback 的 artifact chain 串起来，
回答每个阶段是否可追溯、weight lifecycle 是否一致、安全边界是否存在异常。

本任务不修改 production，不执行 apply，不执行 rollback，不重跑任何上游 pipeline。

## 目标

- 新增 `scripts/run_shadow_promotion_lifecycle_audit.py`。
- 新增核心模块
  `src/ai_trading_system/trading_engine/shadow_promotion_lifecycle_audit.py`。
- 读取 018D proposal、018E1 apply preflight、018E2 apply result、可选 018E3
  rollback result、可选 approval artifacts 和 snapshot metadata。
- 输出：
  - `data/derived/weight_iterations/promotion/audit/shadow_promotion_lifecycle_audit_YYYY-MM-DD.json`
  - `data/derived/weight_iterations/promotion/audit/shadow_promotion_lifecycle_audit_YYYY-MM-DD.md`
  - `data/derived/weight_iterations/promotion/audit/logs/shadow_promotion_lifecycle_audit_run_YYYY-MM-DD.json`
  - `data/derived/weight_iterations/promotion/audit/logs/shadow_promotion_lifecycle_audit_run_YYYY-MM-DD.md`
- Dashboard 新增只读 `Shadow Promotion Lifecycle Audit` 卡片。
- 新增 runbook、system flow、artifact catalog 和测试。

## 安全边界

所有 018F 输出必须固定：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "audit_only": true,
  "apply_executed_by_audit": false,
  "rollback_executed_by_audit": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false,
  "safe_for_scheduler": true
}
```

`safe_for_scheduler=true` 仅表示 audit report 可被定期重新生成。它不得触发 018B、018C、
018C2、018D、018E1、018E2、018E3、scoring、broker、replay 或 trading execution。

## 阶段拆解

|阶段|状态|验收标准|
|---|---:|---|
|1. 任务登记与需求文档|DONE|`docs/task_register.md` 登记 TRADING-018F，本文记录目标、边界、阶段和验收。|
|2. 核心 audit builder|DONE|只读加载 artifacts，输出 lifecycle JSON/Markdown/run log；不调用 apply/rollback 写入入口。|
|3. Artifact chain 校验|DONE|校验 proposal→preflight、preflight→apply、apply→rollback 的 path、sha256、日期、决策和 target profile/snapshot hash。|
|4. Safety boundary audit|DONE|扫描所有已找到 artifacts 的 broker/replay/trading、production_effect、manual_review_only、promotion/apply/rollback executed 等字段，异常输出 `SAFETY_ANOMALY`。|
|5. Weight lifecycle|DONE|提取 apply 前、apply 后、rollback 后 weights，计算 apply delta、rollback delta 和 lifecycle net delta；缺可选数据只 warning。|
|6. CLI|DONE|支持 `--date`、`--promotion-date`、artifact override、`--include-approval-artifacts`、`--fail-on-safety-anomaly`。|
|7. Dashboard|DONE|只读读取 latest audit artifact，展示 lifecycle decision、stage status、safety status、findings/warnings 和 report path，不触发任何 pipeline。|
|8. 文档|DONE|更新 `docs/system_flow.md`、`docs/artifact_catalog.md`，新增 runbook。|
|9. 测试与 smoke|DONE|覆盖 artifact coverage、apply/rollback 状态、chain mismatch、safety anomaly、weight lifecycle、dashboard import guard 和 output invariants。|
|10. 验证收尾|VALIDATING|目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest、ruff 已通过；全仓 black check 被既有无关 `tests/test_market_data.py` baseline 阻断，未混入无关格式化。|

## Lifecycle Decision

允许值：

- `COMPLETE_WITH_ROLLBACK`
- `COMPLETE_APPLIED_NO_ROLLBACK`
- `PROPOSAL_ONLY`
- `PREFLIGHT_ONLY`
- `APPLY_FAILED_OR_BLOCKED`
- `ROLLBACK_FAILED_OR_BLOCKED`
- `INCOMPLETE_ARTIFACTS`
- `SAFETY_ANOMALY`
- `ERROR`

## 验收重点

- 缺 rollback result 不视为错误；已 apply 且 rollback snapshot 存在时输出
  `COMPLETE_APPLIED_NO_ROLLBACK`，并记录 warning。
- 缺 required artifact 或 chain 无法链接时输出 `INCOMPLETE_ARTIFACTS`。
- hash/path/date/target/snapshot 安全矛盾、broker/replay/trading execution、preflight 非只读、
  proposal 已执行、apply/rollback 决策与 executed 状态矛盾等必须输出 `SAFETY_ANOMALY`。
- Dashboard 只能读取 audit artifact，不导入或调用 018B/018C/018C2/018D/018E1/018E2/018E3
  模块或脚本。

## 进展记录

- 2026-05-23：新增并进入 `IN_PROGRESS`。Owner 要求补齐 shadow promotion lifecycle
  audit report；本阶段只读读取已有 artifacts，不允许 production modification、
  apply、rollback、broker、replay 或 trading execution。
- 2026-05-23：从 `IN_PROGRESS` 改为 `VALIDATING`。已完成核心 builder、CLI、
  JSON/Markdown/run log、artifact chain 校验、安全边界审计、weight lifecycle、dashboard
  只读卡片、runbook、system flow / artifact catalog 更新和测试。Repo 外临时 fixture smoke
  验证完整 rollback、无 rollback 和 broker anomaly 三种路径；目标 pytest、dashboard pytest、
  `tests/trading_engine`、全量 pytest 和 ruff 通过；全仓 Black check 仅被既有无关
  `tests/test_market_data.py` baseline 阻断。
