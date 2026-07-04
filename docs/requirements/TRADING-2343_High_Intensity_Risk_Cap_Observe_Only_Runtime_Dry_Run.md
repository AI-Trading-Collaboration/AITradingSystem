# TRADING-2343 High-Intensity Risk-Cap Observe-Only Runtime Dry-Run

最后更新：2026-07-04

## 状态

`DONE`

## 背景

TRADING-2342 已完成 observe-only runtime integration plan，真实 route 为 `TRADING-2343_High_Intensity_Risk_Cap_Observe_Only_Runtime_Dry_Run`，readiness 为 `READY_FOR_2343_WITH_CAVEATS`。selected rule 继续是 `COMPOSITE_HIGH_INTENSITY_RULE`，但仍带有 PIT approximation、partial coverage 和 monthly concentration caveat。promotion、paper-shadow、production 和 broker action 均继续关闭。

TRADING-2343 的目标不是启动 runtime，而是用 prior validated research artifacts 和 historical trigger source 做 runtime dry-run，验证 event detection、append-only event append、cluster update、pending outcome registry update、manual-review context、monthly concentration monitoring 和 fail-closed safety gate 是否能闭环。

## 实施范围

1. 新增 CLI `aits research trends high-intensity-risk-cap-observe-only-runtime-dry-run`。
2. Fail-closed 读取 TRADING-2342 runtime integration plan outputs，并确认 2342 route 允许进入 observe-only runtime dry-run。
3. 读取 TRADING-2341 continuation decision、TRADING-2336 historical event logger outputs、TRADING-2335 selected rule outputs、TRADING-2334 forward observe contracts 和 TRADING-2332 dynamic dry-run trigger source。
4. 生成 contract validation、runtime input validation、event detection dry-run、event append dry-run、cluster update dry-run、pending outcome update dry-run、manual-review context dry-run、monthly concentration monitoring dry-run、fail-closed safety gate result、artifact registry dry-run、data-quality report、interpretation boundary、2344 readiness / route 和 safety boundary artifacts。
5. 输出 research docs，并更新 report registry、artifact catalog、system flow 和 task register。

## 边界

- 不启用 runtime scheduler。
- 不接入每日自动任务。
- 不写入 production daily report。
- 不修改 historical TRADING-2336 event log、cluster registry 或 pending outcome registry。
- 不绑定新的 actual-path outcome。
- 不重新读取 market data。
- 不重新选择 threshold 或修改 selected trigger rule。
- 不重新执行 exposure-cap dry-run。
- 不读取真实券商账户或真实持仓。
- 不生成 target exposure、target weight、rebalance instruction、buy / sell signal、reduce position instruction、paper-shadow-ready、production-ready 或 broker-ready artifact。
- 不进入 paper-shadow、production 或 broker action。
- 不把 manual-review context 解释为减仓建议。

## Data Validation Policy

TRADING-2343 默认只读取 prior validated research artifacts / historical trigger source，不直接消费 fresh market data，不绑定 outcome，因此默认不重跑 `aits validate-data`。必须读取并披露 source validation 信息：

```text
source_validate_data_executed=true
source_validate_data_as_of=2026-06-29
source_validate_data_status=PASS_WITH_WARNINGS
source_validate_data_error_count=0
```

如果实现中重新读取 market data 或 live runtime signal source，则必须运行 `aits validate-data --as-of 2026-06-29`，且不得放宽 future runtime 的 data-quality requirement。

## 验收标准

- CLI 可运行并生成附件要求的 runtime dry-run artifacts 和 research docs。
- 缺少 required TRADING-2342 contracts、2342 route 不是 observe-only runtime dry-run、selected rule 不是 `COMPOSITE_HIGH_INTENSITY_RULE`、prior event / cluster / pending registry 缺失、runtime trigger source 缺失，或任何 input artifact 打开 promotion / paper-shadow / production / broker / target weight / rebalance 时 fail closed。
- Runtime input validation matrix 覆盖 `date`、`target_asset`、`risk_cap_triggered`、`risk_cap_intensity`、`risk_cap_score`、`scope_active`、`signal_direction`、`as_of_timestamp`、`decision_timestamp`、`known_at_policy` 和 `pit_policy`。
- Event detection dry-run 只生成 observe detection records，不能输出 target weight 或减仓指令。
- Event append dry-run 全部是 would-append / would-extend，不得写回 historical event log。
- Cluster update dry-run 正确标记 new cluster、existing cluster continuation、monthly bucket、monthly cluster count 和 consecutive trigger days。
- Pending outcome dry-run 为每个 would-create event 生成 `1d` / `5d` / `10d` / `20d` pending outcome records，`outcome_binding_allowed_in_2343=false`。
- Manual-review context dry-run 只输出 risk warning context，禁止 target weight / rebalance / reduce-position / buy / sell / broker fields。
- Fail-closed safety gate 阻断 missing selected rule、missing known-at timestamp、missing PIT policy、target weight、rebalance、paper-shadow、production 和 broker action。
- 2344 route 只允许进入 observe-only scheduler integration plan、dry-run remediation、safety remediation、artifact registry remediation 或 archive runtime line。
- 所有 outputs 固定 `runtime_scheduler_enabled=false`、`new_event_logging_executed=false`、`event_append_dry_run_executed=true`、`outcome_binding_executed=false`、`promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。

## 验证计划

- `ruff check .`
- `python -m compileall -q src tests`
- TRADING-2343 focused parallel pytest files
- 真实 2343 CLI run
- docs freshness
- documentation contract
- task-register consistency run / validate
- contract-validation tier
- full validation tier because CLI / registry / docs contract surface changes
- `git diff --check`

## 进展记录

- 2026-07-04：根据 owner 附件新增并进入 `IN_PROGRESS`。本批承接 TRADING-2342 `READY_FOR_2343_WITH_CAVEATS` / observe-only dry-run route，只做 contract verification 和 dry-run；当前 worktree 有两个既有无关 research 文档未提交改动，本任务必须 selective staging，不能混入无关改动。
- 2026-07-04：实现完成并进入 `VALIDATING`。新增 `aits research trends high-intensity-risk-cap-observe-only-runtime-dry-run`、runtime dry-run artifacts、research docs、report registry / artifact catalog / system flow 记录和 focused tests。真实 run status=`OBSERVE_ONLY_RUNTIME_DRY_RUN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`，selected_rule=`COMPOSITE_HIGH_INTENSITY_RULE`，record_count=`2490`，detected_event_count=`168`，would_append_event_count=`0`，would_extend_cluster_count=`0`，would_create_pending_outcome_count=`0`，2344_readiness=`READY_FOR_2344_WITH_CAVEATS`，next task=`TRADING-2344_High_Intensity_Risk_Cap_Observe_Only_Runtime_Scheduler_Integration_Plan`。`0` append 来自 historical replay 命中既有 TRADING-2336 trigger-day / event log 去重，不表示 trigger rule 未触发。未重跑 `aits validate-data`，因为本任务只读取 prior validated research artifacts，不直接读取 cached market data 或绑定 outcome。
- 2026-07-04：focused parallel pytest 覆盖 loader、contract validation、event detection、append、cluster update、pending outcome、safety gate、2344 route 和 CLI，结果 `34 passed`。
- 2026-07-04：完整验证通过并归档 `DONE`。验证覆盖 Ruff、compileall、focused parallel pytest 34 passed、真实 CLI run、docs freshness 522 docs PASS、documentation contract 1240 reports PASS、task-register consistency run / validate、contract-validation 193 passed、full parallel pytest 4280 passed / 643 warnings 和 `git diff --check`；contract-validation runtime artifact=`outputs/validation_runtime/contract-validation_20260704T112907Z/test_runtime_summary.json`，full runtime artifact=`outputs/validation_runtime/full_20260704T113133Z/test_runtime_summary.json`。最终 route 进入 TRADING-2344 observe-only scheduler integration plan with caveats；这仍不是 scheduler enabled、event log mutation、outcome binding、paper-shadow、production 或 broker readiness。
