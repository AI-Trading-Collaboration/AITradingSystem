# TRADING-957 to 975 Layer-2 Strategy Component Readiness for Layer-1 Meta-Policy Research

## 背景

本需求把已完成的 simple baseline / forward-aging / QQQ-plus growth research
产物收敛为未来 Layer-1 meta-policy selector 可审计读取的 Layer-2 component
准备层。

目标架构仍保持：

```text
Layer 0: PIT market data and indicators
Layer 1: meta-policy selector research, not enabled yet
Layer 2: frozen strategy components
Final combiner: future deterministic QQQ/TQQQ/SGOV weights, not enabled yet
```

本批不是 paper-shadow 或 production 准入批次。所有输出继续固定：

```text
paper_shadow_allowed=false
production_allowed=false
broker_action=none
manual_review_required=true
```

## 最新前提调整

TRADING-947～956 的最新 owner decision 为：

```text
QQQ-plus growth owner decision = KEEP_GROWTH_RESEARCH_ONLY
```

因此本需求的 Layer-2 v1 formal pool 不包含 growth candidate。growth 只能作为
`research_only_inactive_reference` / reference evidence 保留，不能被 Layer-1 选择，
不能进入 paper-shadow 或 production，也不能改变 `equal_risk_qqq_sgov` 的 defensive
primary 定位。

当前 v1 formal pool：

```text
selectable:
- equal_risk_qqq_sgov
- 100_qqq

reference-only:
- qqq_50_sgov_50
- qqq_60_sgov_40

inactive research reference:
- qqq_plus_growth_research_candidate
```

## 阶段拆解

|任务|阶段|当前状态|验收标准|
|---|---|---|---|
|TRADING-957|component readiness reconciliation / role reconciliation|VALIDATING|`layer2_component_readiness_reconciliation.json/md` 对账 simple baseline、forward-aging 与 QQQ-plus closeout source artifacts；确认 defensive primary、hard benchmark、static references、growth inactive/reference 和 safety fields。|
|TRADING-958|layer2 component registry freeze|VALIDATING|新增 `config/research/layer2_strategy_component_pool_v1.yaml`；`layer2_component_pool_freeze.json/md` 输出 `component_pool_version`、formal/reference/inactive rows、`component_pool_hash` 和 growth 不进入 formal pool 的原因。|
|TRADING-959|policy definition lock|VALIDATING|`layer2_component_definition_lock.json/md` 为 formal components 生成稳定 `policy_definition_hash`，为 growth 生成 inactive-reference-only definition hash；变更规则要求 mapping/lookback/bounds/rebalance/execution/cost 变动必须新 version 或新 strategy_id。|
|TRADING-960|data quality check|VALIDATING|`layer2_component_data_quality_check.json/md` 调用与 `aits validate-data` 同源的 cached data gate，检查 QQQ/TQQQ/SGOV 与 DGS2/DGS10/DTWEXBGS，披露 row count、checksum、warning/error 和 as-of。|
|TRADING-961|component readiness matrix|VALIDATING|`layer2_component_readiness_matrix.json/md` 聚合 957～960；逐组件列出 role、selectable/reference/inactive membership、definition hash、data quality status、blockers 和 safety fields；`layer1_historical_research_allowed=false`。|
|TRADING-962|PIT historical weight path|BASELINE_DONE|为 selectable + reference components 生成 `layer2_historical_weight_path.parquet`、manifest 和 review；只包含 formal pool，不包含 inactive growth；逐行披露 decision date、holding date、definition hash、component pool hash、target weights、rebalance flag、data-quality status 和 warning codes。|
|TRADING-963|return / cost / exposure panel|BASELINE_DONE|基于统一 t+1 执行假设生成 `layer2_return_cost_exposure_panel.parquet`、manifest 和 review；逐日输出 gross/net return、cost、turnover、QQQ/TQQQ/SGOV exposure、effective beta/leverage 和 active return vs QQQ。|
|TRADING-964|independent forward outcome cube|BASELINE_DONE|生成 `layer2_forward_outcome_cube.parquet`、manifest 和 review；覆盖 5d/10d/20d/60d/120d，outcome 仅使用 decision_time 之后真实路径，regret/rank 只作为 outcome-side 字段。|
|TRADING-965|anti-leakage and time-boundary audit|BASELINE_DONE|生成 `layer2_anti_leakage_time_boundary_audit.json/md`；检查 feature/outcome separation、execution lag、same-bar risk、forward window boundary、definition hash 和 component pool hash。|
|TRADING-966|common robustness validation|BASELINE_DONE|生成 `layer2_common_robustness_validation.json/md`；按 period/regime/volatility/trend/drawdown/recovery/event windows 统一评估 formal components，缺 pre-2022 覆盖必须显式标记。|
|TRADING-967～975|后续 selector headroom / combiner / objective / dataset / handoff stages|READY|在 962～966 验证后继续拆分实施；不得跳过 combiner contract、walk-forward embargo、Layer-1 dataset reproducibility 和 final owner handoff gate。|

## 957～961 实现范围

新增 source config：

```text
config/research/layer2_strategy_component_pool_v1.yaml
```

新增 CLI：

```bash
aits research strategies layer2-component-readiness-reconciliation
aits research strategies layer2-component-pool-freeze
aits research strategies layer2-component-definition-lock
aits research strategies layer2-component-data-quality-check
aits research strategies layer2-component-readiness-matrix
```

新增 runtime artifacts：

```text
outputs/research_strategies/layer2_components/layer2_component_readiness_reconciliation.json
outputs/research_strategies/layer2_components/layer2_component_readiness_reconciliation.md
outputs/research_strategies/layer2_components/layer2_component_pool_freeze.json
outputs/research_strategies/layer2_components/layer2_component_pool_freeze.md
outputs/research_strategies/layer2_components/layer2_component_definition_lock.json
outputs/research_strategies/layer2_components/layer2_component_definition_lock.md
outputs/research_strategies/layer2_components/layer2_component_data_quality_check.json
outputs/research_strategies/layer2_components/layer2_component_data_quality_check.md
outputs/research_strategies/layer2_components/layer2_component_readiness_matrix.json
outputs/research_strategies/layer2_components/layer2_component_readiness_matrix.md
```

## Guardrails

- `growth` 不得出现在 formal `selectable_components` 或 formal `reference_components`。
- `selectable_components <= 3`，当前实际为 2。
- `reference_components <= 2`，当前实际为 2。
- `manual_review_required=true` 必须在 JSON 和 Markdown 中可见。
- `layer1_historical_research_allowed=false`，直到后续 Layer-1 dataset contract、
  reproducibility gate、walk-forward embargo 和 owner handoff gate 完成。
- `layer1_forward_aging_allowed=false`。
- `layer1_paper_shadow_allowed=false`。
- `production_allowed=false`。
- `broker_action=none`。

## Report Registry / Artifact Catalog

新增 report registry entries：

```text
layer2_component_readiness_reconciliation
layer2_component_pool_freeze
layer2_component_definition_lock
layer2_component_data_quality_check
layer2_component_readiness_matrix
layer2_historical_weight_path
layer2_return_cost_exposure_panel
layer2_forward_outcome_cube
layer2_anti_leakage_time_boundary_audit
layer2_common_robustness_validation
```

每个 entry 固定：

```text
artifact_selection_policy=latest_available
required_for_daily_reading=false
production_effect=none
broker_action=none
```

`docs/artifact_catalog.md` 必须记录 artifact path、producer command、source inputs、
schema contract、owner next action 和 safety boundary。

## 962～966 历史事实层与独立 outcome 范围

新增 CLI：

```bash
aits research strategies layer2-historical-weight-path-build
aits research strategies layer2-return-cost-exposure-panel
aits research strategies layer2-forward-outcome-cube-build
aits research strategies layer2-anti-leakage-time-boundary-audit
aits research strategies layer2-common-robustness-validation
```

新增 runtime artifacts：

```text
outputs/research_strategies/layer2_components/layer2_historical_weight_path.parquet
outputs/research_strategies/layer2_components/layer2_historical_weight_path_manifest.json
outputs/research_strategies/layer2_components/layer2_historical_weight_path_review.md
outputs/research_strategies/layer2_components/layer2_return_cost_exposure_panel.parquet
outputs/research_strategies/layer2_components/layer2_return_cost_exposure_panel_manifest.json
outputs/research_strategies/layer2_components/layer2_return_cost_exposure_panel_review.md
outputs/research_strategies/layer2_components/layer2_forward_outcome_cube.parquet
outputs/research_strategies/layer2_components/layer2_forward_outcome_cube_manifest.json
outputs/research_strategies/layer2_components/layer2_forward_outcome_cube_review.md
outputs/research_strategies/layer2_components/layer2_anti_leakage_time_boundary_audit.json
outputs/research_strategies/layer2_components/layer2_anti_leakage_time_boundary_audit.md
outputs/research_strategies/layer2_components/layer2_common_robustness_validation.json
outputs/research_strategies/layer2_components/layer2_common_robustness_validation.md
```

本阶段仍不允许 Layer-1 selector 训练：

```text
layer1_historical_research_allowed=false
layer1_forward_aging_allowed=false
layer1_paper_shadow_allowed=false
production_allowed=false
broker_action=none
manual_review_required=true
```

## 进展记录

- 2026-06-24: 新增仓库版需求文档并进入 `IN_PROGRESS`。根据 TRADING-956
  `KEEP_GROWTH_RESEARCH_ONLY` 调整外部草案前提：growth candidate 暂不进入 formal
  Layer-2 component pool，只能作为 inactive research reference。
- 2026-06-24: 957～961 实现完成并转入 `VALIDATING`。新增
  `layer2_strategy_component_pool_v1.yaml`、Layer-2 readiness module、5 个 CLI、
  report registry entries、artifact catalog row、system flow paragraph 和 focused tests。
  v1 pool formal selectable 为 `equal_risk_qqq_sgov` / `100_qqq`；`qqq_50_sgov_50`
  / `qqq_60_sgov_40` 为 reference-only；growth 行为
  `research_only_inactive_reference`。所有输出继续
  `paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`、
  `manual_review_required=true`。
- 2026-06-24: 962～966 进入 `IN_PROGRESS`，目标是为 formal selectable +
  reference components 构建 PIT historical weight path、return/cost/exposure panel、
  independent forward outcome cube、anti-leakage/time-boundary audit 和 common
  robustness validation。QQQ-plus growth 继续只作为 inactive research reference，
  不进入 selectable pool 或事实层计算。
- 2026-06-24: 962～966 baseline 实现完成并通过验证。新增 5 个 CLI、
  3 个 Parquet fact/outcome artifacts、2 个 JSON/Markdown audit artifacts、report
  registry entries、artifact catalog row、system flow paragraph 和 focused tests。
  真实 CLI 运行结果：`layer2-anti-leakage-time-boundary-audit` 为
  `LAYER2_ANTI_LEAKAGE_WARN`（latest forward windows 未全部成熟，无 hard blocker）；
  `layer2-common-robustness-validation` 为 `LAYER2_ROBUSTNESS_MIXED`（缺失/不足覆盖
  窗口显式披露）。Layer-1 historical research 仍保持 `false`，等待 967～975 的
  selector contract、dataset reproducibility、walk-forward embargo 和 owner handoff。
