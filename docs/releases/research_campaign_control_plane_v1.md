# Research Campaign Control Plane v1

发布日期：2026-06-19

## 状态

`RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_READY_WITH_LIMITATIONS`

## 范围

- 新增 `aits research campaign init|validate|plan|run|diagnose|gate|status|packet|archive`。
- TRADING-623～630 新增 `validate-adapters` 和 `validation-pack`，并新增 `config/research/campaign_stage_adapters.yaml`。
- 新增 Campaign spec/schema validator、module capability registry、window/holdout policy、gate policy、evidence store、state machine、evidence budget、next-action planner、reproducibility manifest 和 owner packet。
- 新增 B2/B3 sample specs 与 config-driven migration，B2 当前可表示为 `TARGETED_EVIDENCE + NEEDS_MORE_EVIDENCE + reason_codes`，B3 保持 signal-only mixed precheck。
- 新增 B2 audited-artifact adapter，可把 B2 TARGETED_EVIDENCE / FULL_DIAGNOSTIC / ATTRIBUTION / GATE 旧 artifact 映射为 Campaign evidence records。
- 新增 B3 signal-precheck audited-artifact adapter，保持 `B3_PRECHECK_MIXED`，并阻断 B3 mini-backfill、B4/B5/B6/v3、paper-shadow 和 official target weights。
- 新增 `b2_campaign_adapter_parity_map`、`b2_campaign_parity_validation`、`evidence_budget_enforcement_report`、`campaign_next_action_parity_review` 和 `campaign_control_plane_v1_validation_pack`。
- 新增 E/R/T 消融与两两交互矩阵生成；C/G 未 gate 批准前不进入矩阵。
- 旧 B2/B3 task-specific runners 标记为 read-only historical compatibility，新功能只进入 Campaign 控制面。

## rc2 更新

- 新增 adapter run mode contract：`AUDITED_ARTIFACT_MODE`、`COMPUTE_MODE`、`DRY_RUN_MODE`、`VALIDATION_ONLY_MODE`。
- Adapter run artifact 现在披露 `compute_performed`、`imported_evidence`、`parity_source`、`failure_mode` 和 `holdout_touched=false`。
- 新增 B2 control-window compute adapter，调用既有 B2 risk overlay control-window rerun 计算路径，输出 `B2_COMPUTE_ADAPTER_SMOKE_PASS` 和 compute evidence records。
- 新增 B2 compute/audited parity validation，当前输出 `B2_COMPUTE_PARITY_PASS`。
- `campaign run --stage next` 现在按 plan 推荐阶段执行 safe B2 stage，并在 adapter missing、预算耗尽或 holdout 越权时 fail-closed。
- 新增 evidence budget forced transition report，证明 exhausted budget 不再继续泛化 `NEEDS_MORE_EVIDENCE`。
- 新增 B3 signal-only compute smoke，保持 `B3_PRECHECK_MIXED`，不生成 weights，不运行 mini-backfill，不允许 B4/B5/B6/v3。
- 新增 `campaign allowed-actions|blocked-actions|budget|source-artifacts|deprecation-plan`，`status/plan` 显示 adapter run mode、预算和 source artifacts。
- `validation-pack` rc2 写入 `outputs/research_campaigns/control_plane_v1_rc2_validation/`，当前状态仍为 `RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_READY_WITH_LIMITATIONS`。

## rc3 更新

- B2 compute adapter 覆盖 `TARGETED_EVIDENCE`、`FULL_DIAGNOSTIC` 和 `GATE`。
- Targeted compute 输出 `B2_TARGETED_EVIDENCE_COMPUTE_PASS`，并生成 trigger、drawdown、re-entry、utility、window-stability 和 safety evidence。
- Full diagnostic compute 同时验证 risk-heavy evidence 与 control-window no-trigger evidence，当前输出 `B2_FULL_DIAGNOSTIC_COMPLETE`。
- Gate compute 从 Campaign evidence store、evidence budget 和 stop rules 生成 constrained B2 decision；预算可用时输出 `B2_ONLY_CONTINUE_WITH_DEFINED_EVIDENCE_PLAN`，预算耗尽时输出 `OWNER_OVERRIDE_REQUIRED`，不再发出 generic `NEEDS_MORE_EVIDENCE`。
- 新增 B2 Campaign E2E compute、full-path parity、evidence-budget final-decision drill 和 legacy B2 runner deprecation readiness。
- `validation-pack` rc3 写入 `outputs/research_campaigns/control_plane_v1_rc3_validation/`，当前状态仍为 `RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_READY_WITH_LIMITATIONS`。

## 限制

- B2 Campaign compute 已覆盖 targeted/full/gate，但 legacy B2 task-specific CLI 仍保留到 owner deprecation review 后。
- Audited-artifact adapter 仍保留用于 parity 和历史 evidence 审计。
- 未配置 adapter、输入缺失、holdout 越权、输出 invalid 或 safety metadata 不合格时 fail-closed。
- B3 仍只支持 signal-precheck，不允许 weight generation、mini-backfill、B4/B5/B6/v3 或 paper-shadow。
- Owner packet 不自动 append owner decision。

## 安全边界

- `research_only=true`
- `manual_review_only=true`
- `official_target_weights=false`
- `paper_shadow_activation=false`
- `paper_shadow_allowed=false`
- `broker_effect=none`
- `order_effect=none`
- `production_effect=none`
