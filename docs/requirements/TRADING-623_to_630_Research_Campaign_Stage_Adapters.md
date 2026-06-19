# TRADING-623 to 630 Research Campaign Stage Adapters

版本：2026-06-19

## 背景

Research Campaign 控制面 v1 已完成 spec、registry、window/holdout policy、gate
policy、evidence budget、state machine、evidence store、next-action planner 和
owner packet。当前限制是 diagnostic/backfill 类 stage 尚未接入真实计算 adapter，
无 evidence 时会 fail-closed 为 `STAGE_ADAPTER_NOT_CONFIGURED`。

本阶段目标是把 Campaign 从“控制面可管理状态”推进到“至少能通过 adapter 接入真实
研究计算”的可用状态。B2 是首个真实接入案例，B3 只允许 signal-precheck 接入。

## 范围

- TRADING-623：定义通用 Campaign stage adapter contract。
- TRADING-624：建立 B2 旧 artifact 到 Campaign stage/evidence 的 parity map。
- TRADING-625：接入 B2 TARGETED_EVIDENCE、FULL_DIAGNOSTIC、ATTRIBUTION 和 GATE adapter。
- TRADING-626：验证 B2 Campaign 输出与旧 artifact 和迁移 evidence 一致。
- TRADING-627：接入 B3 signal-precheck adapter，不允许 weight/backfill。
- TRADING-628：强化 evidence budget，限制无限 `NEEDS_MORE_EVIDENCE`。
- TRADING-629：验证 Campaign next-action planner 与人工研究结论一致。
- TRADING-630：形成 Research Campaign Control Plane v1 validation pack。

## 非目标和安全边界

- 不新增 B4/B5/B6/v3 研究。
- 不调 B2 参数，不重设计 B3。
- 不进入 paper-shadow，不批准 extended shadow，不触发 live trading。
- 不生成 official target weights，不接入 broker/order。
- 不发生 production mutation。
- 不使用 untouched holdout。

所有 adapter 输出必须固定披露：

```text
research_only = true
manual_review_only = true
official_target_weights = false
paper_shadow_activation = false
broker_effect = none
order_effect = none
production_effect = none
```

## 实施顺序

1. 定义 adapter contract、状态码、输出 schema 和 fail-closed 行为。
2. 生成 B2 parity map，不重跑旧研究、不修改旧 artifact。
3. 实现 B2 adapter，从已有 B2 artifacts 生成 Campaign evidence records。
4. 跑 B2 Campaign parity validation，确认 old outcome、reason codes、blocked actions 一致。
5. 实现 B3 signal-precheck adapter，只表达 `B3_PRECHECK_MIXED` 和 redesign hypothesis。
6. 强化 evidence budget stop-rule 和 owner override 路径。
7. 生成 next-action parity review 和 v1 validation pack。
8. 更新 Reader Brief、artifact catalog、system flow 和 focused tests。

## 验收标准

- adapter contract validation PASS。
- 未配置 adapter、输入缺失、holdout 越权、输出 invalid 均 fail-closed。
- B2 migration parity PASS，且 Campaign evidence records 可解析。
- B3 signal adapter smoke PASS，weight/backfill/B4/B5/B6/v3 保持 blocked。
- evidence budget 阻止无限 `NEEDS_MORE_EVIDENCE`。
- next-action planner 与当前人工 B2/B3 结论一致。
- validation pack 输出 `RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_READY` 或
  `RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_READY_WITH_LIMITATIONS`。
- 无伪造 metrics，无 paper-shadow/live/broker/order/official weights。
- focused tests、ruff、compileall、JSON parse 和 `git diff --check` 通过。

## 当前状态

2026-06-19：进入 `IN_PROGRESS`。先实现 adapter 接入和 parity validation，不扩大研究
范围，不把 historical import 冒充新 backfill。

2026-06-19：实现完成并转入 `VALIDATING`。新增
`config/research/campaign_stage_adapters.yaml`、adapter contract validation、B2
audited-artifact adapter、B3 signal-precheck adapter、B2 parity map/validation、
evidence budget enforcement report、next-action parity review 和 Control Plane v1
validation pack。当前输出状态为
`RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_READY_WITH_LIMITATIONS`；限制是 B3 仍只支持
signal-precheck，且当前 adapter 读取并校验 audited artifacts，不替代所有旧
task-specific CLI。验证通过 focused pytest、Ruff、compileall、JSON parse、B2/B3 CLI
smoke 和 `git diff --check`。
