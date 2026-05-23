# TRADING-020：Parameter Governance Web View

关联任务：`TRADING-020`

当前状态：DONE

最后更新：2026-05-23

## 背景

TRADING-019 已生成统一的 parameter governance summary，用 JSON / Markdown / dashboard
卡片汇总 production / shadow weights、018C2 review、018D proposal、018E1 preflight、018E2
apply、018E3 rollback 和 018F lifecycle audit。下一步需要一个只读静态 Web View，让人工复核者
更快看出当前治理状态、是否需要人工行动、是否存在 safety anomaly、production 与 shadow
差异、promotion lifecycle 进度、pending items 和最新 artifact 路径。

## 目标

- 新增 `scripts/render_parameter_governance_web_view.py`。
- 新增核心模块
  `src/ai_trading_system/trading_engine/parameter_governance_web_view.py`。
- 只读读取
  `data/derived/weight_iterations/governance/parameter_governance_summary_YYYY-MM-DD.json`。
- 输出：
  - `data/derived/weight_iterations/governance/web/parameter_governance_web_view_YYYY-MM-DD.html`
  - `data/derived/weight_iterations/governance/web/parameter_governance_web_view_YYYY-MM-DD.json`
- HTML 必须是单文件静态页面，内联 CSS，不依赖外部 CDN、不加载远程 JS/CSS、不访问网络、
  不触发 API。
- Dashboard 新增只读 `Parameter Governance Web View` 卡片，只读取 TRADING-020 render
  metadata artifact。
- 新增 runbook、system flow、artifact catalog 和测试。

## 安全边界

所有 TRADING-020 输出必须固定：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "governance_only": true,
  "web_view_only": true,
  "apply_executed_by_web_view": false,
  "rollback_executed_by_web_view": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false
}
```

TRADING-020 不得修改 production profile、production weights、shadow weights，不得执行
promotion、apply、rollback，不得触发 broker、replay runner、trading execution，也不得重跑
018B、018C、018C2、018D、018E1、018E2、018E3、018F 或 TRADING-019。

渲染前必须校验 summary：

- `task_id=TRADING-019`
- `production_effect=none`
- `governance_only=true`
- `apply_executed_by_governance=false`
- `rollback_executed_by_governance=false`
- `broker_execution=false`
- `replay_execution=false`
- `trading_execution=false`

安全字段异常时输出 `render_decision=SAFETY_BLOCKED` 和 blocked HTML，不得静默渲染正常页面。

## Render Decision

|render_decision|含义|
|---|---|
|`RENDERED`|summary 存在、可解析、任务与安全边界有效，HTML 正常生成。|
|`SAFETY_BLOCKED`|summary 安全边界字段异常，生成 blocked report。|
|`INPUT_MISSING`|summary 文件不存在，生成 missing report 和 metadata。|
|`INPUT_INVALID`|summary 无法解析、不是 JSON object 或 `task_id` 不符合 TRADING-019。|
|`ERROR`|运行异常，生成 error report 和 metadata。|

## 阶段拆解

|阶段|状态|验收标准|
|---|---:|---|
|1. 任务登记与需求文档|DONE|`docs/task_register.md` 登记 TRADING-020，本文记录目标、边界、阶段和验收。|
|2. 核心 renderer|DONE|只读加载 summary，验证安全边界，输出 HTML 和 metadata；不调用任何上游 pipeline 或执行器。|
|3. HTML 页面|DONE|展示 header、safety banner、weights table、shadow review、lifecycle timeline、pending items、safety audit、findings 和 artifact paths；所有 JSON 字符串 HTML escape。|
|4. CLI|DONE|支持 `--date`、`--data-root`、`--governance-summary-file`、`--output-file`、`--metadata-file`、`--open-browser false`。|
|5. Dashboard|DONE|只读读取 latest render metadata artifact 并展示 web view path、metadata path、governance state、action、safety 和 findings counts。|
|6. 文档|DONE|更新 `docs/system_flow.md`、`docs/artifact_catalog.md`，新增 runbook。|
|7. 测试与 smoke|DONE|覆盖正常渲染、safety block、missing/invalid input、HTML escape、状态渲染、dashboard 只读和 output invariants。|
|8. 验证收尾|DONE|目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest 和 ruff 通过；black check 只被既有无关 `tests/test_market_data.py` baseline 阻断。|

## 页面阅读要点

- Header 显示 `governance_state`、`action_required`、`action_level` 和
  `recommended_action`。
- `SAFETY_ANOMALY` 必须在顶部显示 `URGENT: Safety Anomaly Detected`，并重复展示
  critical findings。
- Production vs Shadow Weights 表必须对缺失权重显示 `NOT_AVAILABLE`，delta 带正负号；key
  不一致时显示 warning。
- Promotion lifecycle 按 Proposal → Preflight → Apply → Rollback → Lifecycle Audit 展示
  status、decision、executed 和 artifact path。
- Pending items 中 `pending_apply=true` 时必须提示 manual approval/apply may be required。
- Safety Boundary Audit 必须展示 broker、replay、trading execution 和 blocking reasons。
- Artifact links / paths 展示 summary Markdown、018C2、018D、018E1、018E2、018E3 和 018F
  artifact 路径。

## 进展记录

- 2026-05-23：新增并进入 `IN_PROGRESS`。Owner 要求在 TRADING-019 summary 后新增只读
  Parameter Governance Web View；本阶段只读读取 TRADING-019 summary artifact，不允许修改
  production/shadow，不执行 promotion/apply/rollback，不触发 broker、replay 或 trading
  execution，不重跑任何上游 pipeline。
- 2026-05-23：从 `IN_PROGRESS` 改为 `VALIDATING`。已完成只读 renderer、CLI、静态 HTML、
  render metadata、dashboard metadata 卡片、runbook、system flow / artifact catalog 更新和测试。
  Repo 外临时 fixture smoke 验证 normal `RENDERED`、`SAFETY_ANOMALY` urgent banner 和
  `broker_execution=true` 的 `SAFETY_BLOCKED` blocked report；三条路径均确认 TRADING-020
  输出固定 `production_effect=none`、`manual_review_only=true`、`governance_only=true`、
  `web_view_only=true`、`apply_executed_by_web_view=false`、
  `rollback_executed_by_web_view=false`、`broker_execution=false`、`replay_execution=false`、
  `trading_execution=false`。验证通过目标 pytest、dashboard pytest、`tests/trading_engine`、
  全量 pytest 和 ruff；全仓 black check 仍只被既有无关 `tests/test_market_data.py` baseline
  阻断，未混入无关格式化 diff。
- 2026-05-23：从 `VALIDATING` 改为 `DONE`。最终收尾验证再次通过 repo 外临时 fixture
  三路径 smoke：`RENDERED` 页面包含 `governance_state`、`action_level`、production vs shadow
  weights、promotion lifecycle timeline、pending items 和 safety boundary audit；`SAFETY_ANOMALY`
  页面顶部显示 urgent banner，critical findings 已 HTML escape；`SAFETY_BLOCKED` 对 summary
  `broker_execution=true` 明确显示 render blocked。三路径均确认 render metadata 顶层
  `production_effect=none`、`manual_review_only=true`、`governance_only=true`、
  `web_view_only=true`、`apply_executed_by_web_view=false`、
  `rollback_executed_by_web_view=false`、`broker_execution=false`、`replay_execution=false`、
  `trading_execution=false`。Dashboard import guard 验证 Parameter Governance Web View 卡片只读读取
  render metadata artifact，不触发 018B/018C/018C2/018D/018E1/018E2/018E3/018F/019/020、
  scoring、broker、replay 或 trading。收尾验证通过目标 pytest、dashboard pytest、
  `tests/trading_engine`、全量 pytest 和 ruff；全仓 black check 仍只被既有无关
  `tests/test_market_data.py` baseline 阻断，未混入无关格式化 diff。
- 2026-05-23：重复最终收尾验证通过。再次使用 repo 外临时 fixture 验证 `RENDERED`、
  `SAFETY_ANOMALY` 和 `SAFETY_BLOCKED`；三条路径均确认 TRADING-020 render metadata 安全字段
  固定为只读无执行；HTML 覆盖治理状态、action level、weights table、lifecycle timeline、
  pending items、safety audit、urgent banner、blocked report 和 JSON 字符串 HTML escape；Dashboard
  import guard 再次确认只读取 TRADING-020 render metadata artifact，不触发 018B/018C/018C2/018D/
  018E1/018E2/018E3/018F/019/020、scoring、broker、replay 或 trading。重复收尾验证通过目标
  pytest、dashboard pytest、`tests/trading_engine`、全量 pytest 和 ruff；全仓 black check 仍只被
  既有无关 `tests/test_market_data.py` baseline 阻断，未混入无关格式化 diff。
