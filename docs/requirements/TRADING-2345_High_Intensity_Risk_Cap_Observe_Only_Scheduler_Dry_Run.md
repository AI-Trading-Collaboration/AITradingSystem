# TRADING-2345 High-Intensity Risk-Cap Observe-Only Scheduler Dry-Run

最后更新：2026-07-05

## 状态

- task_id: `TRADING-2345_HIGH_INTENSITY_RISK_CAP_OBSERVE_ONLY_SCHEDULER_DRY_RUN`
- status: `DONE`
- priority: `P1`
- owner: `codex`
- last_update: `2026-07-05`

## 背景

TRADING-2344 已生成 high-intensity risk-cap observe-only runtime scheduler integration plan，并 route 到 `TRADING-2345_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Dry_Run`。2344 的 scheduler 仍为 disabled-by-default，且 `scheduler_enabled=false`、`scheduler_default_enabled=false`、`event_append_executed=false`、`outcome_binding_executed=false`、promotion / paper-shadow / production / broker action 全部关闭。

TRADING-2345 只验证 scheduler cycle 和 job DAG 的 dry-run 闭环，不启用 scheduler，不写生产日报，不 append 真实 event log，不修改 historical cluster / pending registries，不绑定 actual-path outcome，不重新读取 fresh market data，不输出 target weight / rebalance / broker action。

## 输入

- TRADING-2344 scheduler integration plan artifacts。
- TRADING-2343 observe-only runtime dry-run artifacts。
- TRADING-2342 runtime contracts。
- TRADING-2341 continuation decision contracts。
- TRADING-2335 selected `COMPOSITE_HIGH_INTENSITY_RULE` lineage。
- TRADING-2336 observe event log / cluster registry / pending outcome registry / monthly concentration report。
- TRADING-2332 source-bound dynamic dry-run trigger source artifacts。

## 实施范围

1. 新增 CLI：`aits research trends high-intensity-risk-cap-observe-only-scheduler-dry-run`。
2. 读取并校验 2344 scheduler scope / cadence / input / job contracts、disabled policy 和 fail-closed gate。
3. 生成 scheduler cycle plan、job DAG validation、input snapshot validation matrix。
4. 执行 event detection / event append / cluster update / pending outcome / manual review context / monthly concentration dry-run。
5. 验证 outcome update job 只处于 planned dry-run，不绑定 outcome。
6. 生成 disabled policy validation、fail-closed safety gate、data quality、interpretation boundary、2346 readiness checklist 和 2346 route。
7. 更新 research docs、report registry、artifact catalog、system flow 和 task register。

## 安全边界

- `scheduler_enabled=false`
- `scheduler_default_enabled=false`
- `event_append_executed=false`
- `event_append_dry_run_executed=true`
- `outcome_binding_executed=false`
- `automatic_exposure_cap_allowed=false`
- `target_weight_action_allowed=false`
- `rebalance_instruction_allowed=false`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `portfolio_effect=none`
- `production_effect=none`
- `manual_review_only=true`

## Data Validation Policy

TRADING-2345 默认只读取 prior validated research artifacts，不绑定 outcome，不直接消费 fresh market data。因此不重新运行 `aits validate-data`。输出必须继承并披露 source validation 信息：

- `source_validate_data_executed=true`
- `source_validate_data_as_of=2026-06-29`
- `source_validate_data_status=PASS_WITH_WARNINGS`
- `source_validate_data_error_count=0`

如果实现改为读取 fresh market data 或 live runtime source，则必须先运行 `aits validate-data --as-of 2026-06-29` 并 stop on failure。

## 验收标准

- focused tests 覆盖 loader、contract validation、cycle plan、event detection、event append、cluster / pending jobs、disabled safety gate、2346 route 和 CLI。
- 真实 CLI run 生成附件列出的全部 JSON / CSV / Markdown artifacts。
- 2346 route 只允许 observe-only scheduler wiring plan、dry-run remediation、safety remediation 或 archive。
- 所有 artifacts 固定 observe-only / research-only safety boundary，不包含 target weight、rebalance、paper-shadow、production 或 broker action。
- `outputs/` runtime artifacts 不纳入 git。
- selective staging 不包含既有无关 research docs 改动。

## 进展记录

- 2026-07-05：根据 owner 附件新增任务文档并进入 `IN_PROGRESS`；开始实现 observe-only scheduler dry-run CLI、artifacts、docs、registry、catalog、system flow 和 tests。
- 2026-07-05：实现并验证完成，状态转为 `DONE`。新增 `aits research trends high-intensity-risk-cap-observe-only-scheduler-dry-run`，真实 run status=`OBSERVE_ONLY_SCHEDULER_DRY_RUN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`，record_count=`2490`，detected_event_count=`168`，would_append_event_count=`0`，reason=`DEDUP_AGAINST_EXISTING_HISTORICAL_EVENT_LOG`，source validation=`2026-06-29` / `PASS_WITH_WARNINGS` / error_count=0，readiness=`READY_FOR_2346_WITH_CAVEATS`，next task=`TRADING-2346_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Wiring_Plan`。验证通过 Ruff、compileall、focused parallel pytest 38 passed、真实 CLI run、docs freshness 524 docs PASS、documentation contract 1242 reports PASS、task-register consistency run/validate PASS、contract-validation 193 passed 和 full validation 4350 passed / 643 warnings。未重跑 `aits validate-data`，因为本任务只读取 prior validated research artifacts，不直接读取 fresh market data 或绑定 outcome。
