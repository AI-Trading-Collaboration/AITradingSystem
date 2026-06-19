# Research Campaign Control Plane v1

发布日期：2026-06-19

## 状态

`RESEARCH_CAMPAIGN_CONTROL_PLANE_READY_WITH_LIMITATIONS`

## 范围

- 新增 `aits research campaign init|validate|plan|run|diagnose|gate|status|packet|archive`。
- 新增 Campaign spec/schema validator、module capability registry、window/holdout policy、gate policy、evidence store、state machine、evidence budget、next-action planner、reproducibility manifest 和 owner packet。
- 新增 B2/B3 sample specs 与 config-driven migration，B2 当前可表示为 `TARGETED_EVIDENCE + NEEDS_MORE_EVIDENCE + reason_codes`，B3 保持 signal-only mixed precheck。
- 新增 E/R/T 消融与两两交互矩阵生成；C/G 未 gate 批准前不进入矩阵。
- 旧 B2/B3 task-specific runners 标记为 read-only historical compatibility，新功能只进入 Campaign 控制面。

## 限制

- Diagnostic/backfill 类 stage 目前只提供控制面 runner；未配置真实 adapter 时 fail-closed 为 `STAGE_ADAPTER_NOT_CONFIGURED`，不伪造 metrics。
- B2/B3 migration 只导入既有 evidence 和状态 parity，不重跑历史研究逻辑。
- Owner packet 不自动 append owner decision。

## 安全边界

- `research_only=true`
- `manual_review_only=true`
- `official_target_weights=false`
- `paper_shadow_allowed=false`
- `broker_effect=none`
- `order_effect=none`
- `production_effect=none`
