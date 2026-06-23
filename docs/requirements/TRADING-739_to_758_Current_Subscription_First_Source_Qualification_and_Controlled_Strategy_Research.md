# TRADING-739 to TRADING-758: Current-Subscription-First Source Qualification and Controlled Strategy Research
最后更新：2026-06-23

## 背景

TRADING-738 已完成 current subscription entitlement / coverage audit。结果显示当前订阅已经覆盖多数 P0 source requirements，但仍缺 source manifest、available_time contract、as-of snapshot、lineage proof 和 current-view / research-label boundary 证明。

本批任务目标是先把“当前订阅可访问”推进到“当前订阅可资格认证”，再进入 diagnostic-only / controlled research 的策略研究 pilot。所有输出必须保持 validation-only / observe-only，不修改 production、paper-shadow、official weights，不触发 broker/order，不放宽 PIT / data-quality / lineage gate，不记录 API key 原文。

## 市场 regime

- regime：`ai_after_chatgpt`
- anchor event：ChatGPT public launch on 2022-11-30
- default backtest start：2022-12-01

任何策略研究报告必须披露 regime 和 requested date range；pre-2022 数据只能用于 warm-up、stress test 或 regime comparison。

## 阶段拆解

| Task | 阶段 | 目标 | 状态 |
|---|---|---|---|
| TRADING-739 | Source qualification | Data source usage guardrails | VALIDATING |
| TRADING-740 | Source qualification | FMP price / corporate action qualification | VALIDATING |
| TRADING-741 | Source qualification | Marketstack second-source reconciliation qualification | VALIDATING |
| TRADING-742 | Source qualification | Asset master / tradable universe qualification | VALIDATING |
| TRADING-743 | Source qualification | Forward evidence archive reclassification and capture contract | VALIDATING |
| TRADING-744 | Source qualification | SEC / fundamental PIT qualification | VALIDATING |
| TRADING-745 | Source qualification | FRED / Cboe macro-risk qualification | VALIDATING |
| TRADING-746 | Source qualification | Regime / event / cluster label boundary qualification | VALIDATING |
| TRADING-747 | Source qualification | Cost / liquidity / cash-yield model qualification | VALIDATING |
| TRADING-748 | Source qualification | Data foundation acceptance v2 and minimum research readiness | VALIDATING |
| TRADING-749 | Controlled research | Strategy research pilot readiness board | VALIDATING |
| TRADING-750 | Controlled research | Benchmark + controls batch | VALIDATING |
| TRADING-751 | Controlled research | Strategy pair reverse diagnostics pilot | VALIDATING |
| TRADING-752 | Controlled research | Regret casebook and failure taxonomy pilot | VALIDATING |
| TRADING-753 | Controlled research | Horizon-conditioned value surface prototype | VALIDATING |
| TRADING-754 | Controlled research | Regret-driven state machine prototype | VALIDATING |
| TRADING-755 | Controlled research | Simple strategy ensemble / selector prototype | VALIDATING |
| TRADING-756 | Controlled research | GBDT action-utility baseline | VALIDATING |
| TRADING-757 | Controlled research | Pilot batch review / kill-pause-pivot | VALIDATING |
| TRADING-758 | Vendor governance | Data vendor decision gate | VALIDATING |

## Implementation Plan

1. 新增 source qualification configs：usage policy、FMP、Marketstack、asset master、SEC PIT、macro-risk、label boundary、cost/liquidity、controlled strategy pilot。
2. 新增 current-subscription qualification runner，生成 JSON/Markdown artifacts 和 sidecar JSON contracts。
3. 在 CLI 中接入：
   - `aits data source-qualification usage-guardrails`
   - `aits data source-qualification fmp-price-corporate-action`
   - `aits data source-qualification marketstack-reconciliation`
   - `aits data source-qualification asset-master`
   - `aits forward-evidence classify-requirement`
   - `aits forward-evidence validate-capture-contract`
   - `aits data source-qualification sec-fundamental-pit`
   - `aits data source-qualification macro-risk`
   - `aits research labels boundary-qualification`
   - `aits trading-costs qualify-model`
   - `aits data foundation-acceptance run --include-qualified-sources`
   - `aits research strategy-pilot readiness-board|benchmark-controls|reverse-diagnostics|regret-casebook|value-surface|state-machine|ensemble-selector|gbdt-action-utility|batch-review`
   - `aits data source-qualification vendor-decision-gate`
4. 更新 report registry、artifact catalog 和 system flow。
5. 增加 focused tests，并纳入 `fast-unit` / `contract-validation` validation tier。

## Acceptance Criteria

- 739～748 artifacts 固定 `production_effect=none`、`broker_action=none`、`promotion_gate_allowed=false`、`paper_shadow_change_allowed=false`、`production_weight_change_allowed=false`、`lookahead_violation_count=0`。
- Current-view-only、research-label-only、blocked-until-qualified 数据不得进入 strategy input、promotion evidence 或 paper-shadow candidate。
- FMP 只能输出 `promotion_candidate_after_qualification` candidate，不直接升级为 promotion-grade source。
- Marketstack 只能作为 second-source reconciliation，不作为 primary source。
- Forward archive 默认重分类为 internal capture requirement，除非明确检测到 broker position、cash balance、live quote 或 portfolio statement 外部源需求。
- SEC accepted time 是 fundamental PIT primary proof；FMP/EODHD fundamentals 保持 diagnostic-only，除非 as-of contract 另行证明。
- Acceptance v2 的 minimum readiness 只允许进入 `DIAGNOSTIC_ONLY_READY` 或 `CONTROLLED_RESEARCH_READY`，不得批准 promotion/paper-shadow/production。
- 749～758 artifacts 全部为 diagnostic-only / controlled research，不允许 oracle/teacher/GBDT/selector/value surface/state machine 输出作为 promotion evidence。
- Vendor purchase recommendation 必须有 explicit blocker mapping；没有 blocker mapping 时输出 `DO_NOT_BUY_NEW_SOURCE_YET`。

## Progress Notes

- 2026-06-21：按 owner 附件新增本需求文档并进入实现；采用 validation-only runner + Typer CLI + JSON/Markdown artifacts + registry/catalog/system_flow + focused tests 模式。
- 2026-06-21：实现 TRADING-739～758 baseline artifacts 和 CLI wiring；所有 outputs 默认 no production effect / no broker action / no promotion gate。
- 2026-06-21：默认 CLI run 已生成 source qualification、forward capture contract、data foundation acceptance v2、controlled strategy pilot 和 vendor decision gate artifacts；acceptance v2 输出 `CONTROLLED_RESEARCH_READY`，vendor gate 输出 `DO_NOT_BUY_NEW_SOURCE_YET`，且 `status_upgrade_attempted=false`、`lookahead_violation_count=0`、`promotion_gate_allowed=false`、`paper_shadow_change_allowed=false`、`production_weight_change_allowed=false`。
- 2026-06-21：验证通过 focused parallel pytest、Ruff、scoped Black check、compileall、`git diff --check`、`fast-unit` 103 passed、`contract-validation` 102 passed 和 `report-validation` 55 passed；runtime artifacts 分别写入 `outputs/validation_runtime/fast-unit_20260621T041357Z/`、`outputs/validation_runtime/contract-validation_20260621T041444Z/`、`outputs/validation_runtime/report-validation_20260621T041536Z/`。
- 2026-06-21：本批任务保持 `VALIDATING`，因为真实 source qualification、owner 复核、长期 forward evidence 和策略有效性证据仍需后续运行观察；当前 baseline 只允许 diagnostic-only / controlled research，不构成 promotion、paper-shadow 或 production 证据。
