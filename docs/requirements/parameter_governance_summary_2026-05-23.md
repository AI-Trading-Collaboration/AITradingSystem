# TRADING-019：Parameter Governance Dashboard

关联任务：`TRADING-019`

当前状态：DONE

最后更新：2026-05-23

## 背景

TRADING-018B 到 TRADING-018F 已形成 shadow weight / promotion governance 的闭环：
学习、对比、多日评估、提案、预检、应用、回滚和生命周期审计。当前状态分散在多个
artifact 和 dashboard 卡片中，缺少一个只读聚合视图回答 production / shadow 参数治理当前
处于什么状态、是否需要人工行动、是否存在 pending apply / rollback / lifecycle audit，以及
是否出现安全边界异常。

## 目标

- 新增 `scripts/run_parameter_governance_summary.py`。
- 新增核心模块
  `src/ai_trading_system/trading_engine/parameter_governance_summary.py`。
- 只读读取 production profile、`current_shadow_weights.json`、latest 018C2 review、
  018D proposal、018E1 preflight、018E2 apply result、018E3 rollback result 和 018F
  lifecycle audit。
- 输出：
  - `data/derived/weight_iterations/governance/parameter_governance_summary_YYYY-MM-DD.json`
  - `data/derived/weight_iterations/governance/parameter_governance_summary_YYYY-MM-DD.md`
  - `data/derived/weight_iterations/governance/logs/parameter_governance_summary_run_YYYY-MM-DD.json`
  - `data/derived/weight_iterations/governance/logs/parameter_governance_summary_run_YYYY-MM-DD.md`
- Dashboard 新增只读 `Parameter Governance Summary` 卡片，只读取 summary artifact。
- 新增 runbook、system flow、artifact catalog 和测试。

## 安全边界

所有 TRADING-019 输出必须固定：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "governance_only": true,
  "apply_executed_by_governance": false,
  "rollback_executed_by_governance": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false,
  "safe_for_scheduler": true
}
```

`safe_for_scheduler=true` 仅表示 summary artifact 可以定时生成。TRADING-019 不得触发
018B、018C、018C2、018D、018E1、018E2、018E3、018F、019 自身、scoring、broker、
replay 或 trading execution，不得写 production / shadow weights。

## 阶段拆解

|阶段|状态|验收标准|
|---|---:|---|
|1. 任务登记与需求文档|DONE|`docs/task_register.md` 登记 TRADING-019，本文记录目标、边界、阶段和验收。|
|2. 核心 summary builder|DONE|只读加载 artifacts，输出 governance JSON/Markdown/run log；不调用任何上游 pipeline 或执行器。|
|3. Governance state 推导|DONE|覆盖 `SAFE_OBSERVATION`、`SHADOW_LEARNING`、`SHADOW_REVIEW_READY`、`PROPOSAL_PENDING_REVIEW`、`PREFLIGHT_READY`、`APPLIED_NEEDS_MONITORING`、`ROLLBACK_COMPLETED`、`SAFETY_ANOMALY`、`INCOMPLETE_DATA`、`ERROR`。|
|4. Pending items 与 action level|DONE|输出 pending proposal/preflight/apply/rollback/lifecycle audit 和 `NONE` / `WATCH` / `REVIEW_REQUIRED` / `APPROVAL_REQUIRED` / `ROLLBACK_REVIEW_REQUIRED` / `URGENT`。|
|5. Safety boundary audit|DONE|扫描所有已找到 artifacts 的 execution / production_effect / scheduler-trigger 字段；异常输出 `SAFETY_ANOMALY`。|
|6. CLI|DONE|支持 `--date`、`--data-root`、`--production-profile`、`--shadow-weights-file`、`--lookback-days`、`--fail-on-safety-anomaly`。|
|7. Dashboard|DONE|只读读取 latest governance summary artifact 并展示状态、行动、weights、promotion status、safety status 和 report path。|
|8. 文档|DONE|更新 `docs/system_flow.md`、`docs/artifact_catalog.md`，新增 runbook。|
|9. 测试与 smoke|DONE|覆盖状态推导、安全异常、pending items、weights、dashboard 只读和 output invariants。|
|10. 验证收尾|DONE|目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest 和 ruff 通过；black check 只被既有无关 `tests/test_market_data.py` baseline 阻断。|

## Governance State

允许值：

- `SAFE_OBSERVATION`
- `SHADOW_LEARNING`
- `SHADOW_REVIEW_READY`
- `PROPOSAL_PENDING_REVIEW`
- `PREFLIGHT_READY`
- `APPLY_PENDING`
- `APPLIED_NEEDS_MONITORING`
- `ROLLBACK_COMPLETED`
- `SAFETY_ANOMALY`
- `INCOMPLETE_DATA`
- `ERROR`

`SAFETY_ANOMALY` 优先级最高；rollback completed 高于 applied monitoring；preflight / proposal /
review / shadow learning 按最新可用 artifact 向前推导。

## 进展记录

- 2026-05-23：新增并进入 `IN_PROGRESS`。Owner 要求新增只读 parameter governance
  summary/dashboard 聚合层；本阶段只读读取既有 governance artifacts，不允许 production /
  shadow modification、promotion、apply、rollback、broker、replay 或 trading execution。
- 2026-05-23：从 `IN_PROGRESS` 改为 `VALIDATING`。已完成只读 summary builder、CLI、
  JSON/Markdown/run log、dashboard 只读卡片、runbook、system flow / artifact catalog 更新和测试。
  Repo 外临时 fixture smoke 验证 `ROLLBACK_COMPLETED`、`PREFLIGHT_READY` 和 `SAFETY_ANOMALY`
  三条路径；目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest 和 ruff 通过。
  全仓 Black check 仍只被既有无关 `tests/test_market_data.py` baseline 阻断，未混入无关格式化 diff。
- 2026-05-23：从 `VALIDATING` 改为 `DONE`。最终收尾验证再次通过 repo 外临时 fixture
  三路径 smoke：`ROLLBACK_COMPLETED` 输出 `action_required=false` 和
  `safety_boundary_audit.status=PASS`，`PREFLIGHT_READY` 输出 `pending_apply=true` 和
  `action_level=APPROVAL_REQUIRED`，`SAFETY_ANOMALY` 输出 `action_required=true`、
  `action_level=URGENT` 且 critical findings 非空。三路径均确认 summary 顶层
  `production_effect=none`、`manual_review_only=true`、`governance_only=true`、
  `apply_executed_by_governance=false`、`rollback_executed_by_governance=false`、
  `broker_execution=false`、`replay_execution=false`、`trading_execution=false`。
  Dashboard import guard 验证 Parameter Governance Summary 卡片只读读取 governance summary
  artifact，不触发 018B/018C/018C2/018D/018E1/018E2/018E3/018F/019、scoring、broker、
  replay 或 trading。收尾验证通过目标 pytest、dashboard pytest、`tests/trading_engine`、
  全量 pytest 和 ruff；全仓 Black check 仍只被既有无关 `tests/test_market_data.py`
  baseline 阻断，未混入无关格式化 diff。
