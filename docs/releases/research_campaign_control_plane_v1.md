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

## 限制

- 当前 adapter 是 audited-artifact adapter：读取并验证既有 B2/B3 JSON artifacts，不伪造 metrics，也不替代所有旧 task-specific CLI。
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
