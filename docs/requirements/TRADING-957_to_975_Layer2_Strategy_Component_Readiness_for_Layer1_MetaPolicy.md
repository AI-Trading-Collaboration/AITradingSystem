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
|TRADING-967|transition cost / latency review|BASELINE_DONE|`layer2_transition_cost_latency_review.json/md` 基于 PIT weight path 与 return/cost/exposure panel 评估组件间切换 turnover、1d/2d execution lag impact、monthly/weekly/threshold switch cost 和 cost-adjusted impact；真实 CLI 状态 `TRANSITION_COST_MATERIAL`。|
|TRADING-968|component distinctiveness review|BASELINE_DONE|`layer2_component_distinctiveness_review.json/md` 输出 weight/return/drawdown/exposure correlation、regime response、relative performance dispersion、turnover/risk-budget differences，并回答组件是否冗余、reference-only 是否保留、是否足以支持 Layer-1 selector research；真实 CLI 状态 `COMPONENTS_DISTINCT`。|
|TRADING-969|selector headroom oracle review|BASELINE_DONE|`layer2_selector_headroom_oracle_review.json/md` 仅估算 oracle 上限，覆盖 best 5d/20d/60d、drawdown reduction、Calmar、cost-adjusted、minimum holding 20d/60d variants；不得把 oracle 当作可实现策略表现；真实 CLI 状态 `SELECTOR_HEADROOM_MATERIAL`。|
|TRADING-970|switching constraint contract|BASELINE_DONE|`layer2_switching_constraint_contract.json/md` 定义 minimum holding period、max switches/turnover/cooldown/no flip-flop/reference-only not selectable 等 Layer-1 selector research 约束；真实 CLI 状态 `SWITCHING_CONSTRAINT_READY`。|
|TRADING-971|best component label builder|BASELINE_DONE|`layer2_best_component_label_builder.json/md` 基于 independent forward outcome cube 生成研究标签；真实 CLI 状态 `BEST_COMPONENT_LABELS_PARTIAL`，最新 forward windows 未全部成熟但成熟样本可用于 dataset。|
|TRADING-972|policy combiner contract|VALIDATING|`layer1_policy_combiner_contract.json/md` 定义 selector output、blend weight schema、final combiner、normalization 和 turnover constraint；真实 CLI 状态 `POLICY_COMBINER_CONTRACT_READY`。|
|TRADING-973|objective / outcome contract|VALIDATING|`layer1_objective_outcome_contract.json/md` 固定 primary/secondary/tertiary objectives；真实 CLI 状态 `LAYER1_OBJECTIVE_READY`。|
|TRADING-974|purged walk-forward split contract|VALIDATING|`layer1_purged_walk_forward_split_contract.json/md` 定义 embargo、train/validation/test splits、market regime split 和 unmatured exclusion；真实 CLI 状态 `PURGED_WALK_FORWARD_CONTRACT_READY`。|
|TRADING-975|research dataset builder|BASELINE_DONE|`layer1_research_dataset.json/md` 输出 831 行 research dataset；不训练模型、不输出策略结论；真实 CLI 状态 `LAYER1_RESEARCH_DATASET_READY`。|

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
layer2_transition_cost_latency_review
layer2_component_distinctiveness_review
layer2_selector_headroom_oracle_review
layer2_switching_constraint_contract
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

## 967～970 selector headroom 第一批范围

新增 CLI：

```bash
aits research strategies layer2-transition-cost-latency-review
aits research strategies layer2-component-distinctiveness-review
aits research strategies layer2-selector-headroom-oracle-review
aits research strategies layer2-switching-constraint-contract
```

新增 runtime artifacts：

```text
outputs/research_strategies/layer2_components/layer2_transition_cost_latency_review.json
outputs/research_strategies/layer2_components/layer2_transition_cost_latency_review.md
outputs/research_strategies/layer2_components/layer2_component_distinctiveness_review.json
outputs/research_strategies/layer2_components/layer2_component_distinctiveness_review.md
outputs/research_strategies/layer2_components/layer2_selector_headroom_oracle_review.json
outputs/research_strategies/layer2_components/layer2_selector_headroom_oracle_review.md
outputs/research_strategies/layer2_components/layer2_switching_constraint_contract.json
outputs/research_strategies/layer2_components/layer2_switching_constraint_contract.md
```

第一批必须先调用同源 cached data quality gate；若 gate 失败，状态必须 fail
closed。报告必须披露 `data_quality_status`、market regime、actual requested
date range、source artifact lineage、component pool hash 和安全字段。

本阶段输出只回答是否值得继续做 Layer-1 simple-rule research，不允许：

```text
paper_shadow_allowed=true
production_allowed=true
broker_action!=none
ML selector
QQQ-plus growth selectable
reference-only components selectable
```

## 971～975 Layer-1 dataset contract 范围

新增 CLI：

```bash
aits research strategies layer2-best-component-label-builder
aits research strategies layer1-policy-combiner-contract
aits research strategies layer1-objective-outcome-contract
aits research strategies layer1-purged-walk-forward-split-contract
aits research strategies layer1-research-dataset-builder
```

新增 runtime artifacts：

```text
outputs/research_strategies/layer1_meta_policy/layer2_best_component_label_builder.json
outputs/research_strategies/layer1_meta_policy/layer2_best_component_label_builder.md
outputs/research_strategies/layer1_meta_policy/layer1_policy_combiner_contract.json
outputs/research_strategies/layer1_meta_policy/layer1_policy_combiner_contract.md
outputs/research_strategies/layer1_meta_policy/layer1_objective_outcome_contract.json
outputs/research_strategies/layer1_meta_policy/layer1_objective_outcome_contract.md
outputs/research_strategies/layer1_meta_policy/layer1_purged_walk_forward_split_contract.json
outputs/research_strategies/layer1_meta_policy/layer1_purged_walk_forward_split_contract.md
outputs/research_strategies/layer1_meta_policy/layer1_research_dataset.json
outputs/research_strategies/layer1_meta_policy/layer1_research_dataset.md
```

Dataset builder 不训练模型、不输出策略结论。`best_component_labels` 和
`component_forward_outcomes` 只允许出现在 label/outcome 侧；feature 侧只能包含
`feature_time <= decision_date` 的 market features、component ids、definition hashes
和 decision-time target weights。

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
- 2026-06-24: 967～970 第一批进入 `IN_PROGRESS`。执行范围限定为
  transition cost / latency、component distinctiveness、selector headroom oracle
  和 switching constraint contract；若组件冗余或 headroom 很小，后续 971～985
  不应升级为复杂 Layer-1 research。本阶段仍固定
  `paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`、
  `manual_review_required=true`。
- 2026-06-24: 967～970 baseline 实现完成并通过验证。新增 4 个 CLI/artifacts、
  selector headroom research policy config、report registry entries、artifact catalog
  row、system flow paragraph 和 focused tests。真实 CLI 运行结果：
  `layer2-transition-cost-latency-review` 为 `TRANSITION_COST_MATERIAL`，
  `layer2-component-distinctiveness-review` 为 `COMPONENTS_DISTINCT`，
  `layer2-selector-headroom-oracle-review` 为 `SELECTOR_HEADROOM_MATERIAL`，
  `layer2-switching-constraint-contract` 为 `SWITCHING_CONSTRAINT_READY`；
  data quality 为 `PASS_WITH_WARNINGS`。由于 headroom material 但 transition cost
  material，971～985 可以继续推进为 simple-rule research readiness，但不得自动升级为
  复杂 ML selector、paper-shadow、production 或 broker action。
- 2026-06-24: 971～975 进入 `IN_PROGRESS`。目标是基于 independent forward outcome
  cube 构建 best-component labels、Layer-1 combiner contract、objective/outcome contract、
  purged walk-forward split contract 和 research dataset；dataset 只供历史研究准备，
  不训练模型、不输出策略结论。
- 2026-06-24: 971～975 baseline 实现完成并通过验证。真实 CLI 结果：
  `layer2-best-component-label-builder` 为 `BEST_COMPONENT_LABELS_PARTIAL`（831 个成熟
  labels，最新 forward windows 未全部成熟），`layer1-policy-combiner-contract` 为
  `POLICY_COMBINER_CONTRACT_READY`，`layer1-objective-outcome-contract` 为
  `LAYER1_OBJECTIVE_READY`，`layer1-purged-walk-forward-split-contract` 为
  `PURGED_WALK_FORWARD_CONTRACT_READY`（3 个 splits），`layer1-research-dataset-builder`
  为 `LAYER1_RESEARCH_DATASET_READY`（831 rows，data quality=`PASS_WITH_WARNINGS`）。
