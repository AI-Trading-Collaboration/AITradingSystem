# TRADING-976 to 985 Layer-1 Meta-Policy Research Readiness Gate

## 背景

TRADING-967～970 真实运行显示 Layer-2 component pool 具备继续做
Layer-1 simple-rule research readiness 的前提：

```text
transition_cost_status = TRANSITION_COST_MATERIAL
component_distinctiveness_status = COMPONENTS_DISTINCT
selector_headroom_status = SELECTOR_HEADROOM_MATERIAL
switching_constraint_status = SWITCHING_CONSTRAINT_READY
```

本批仍不是 paper-shadow、production 或 broker 准入。所有输出必须固定：

```text
paper_shadow_allowed=false
production_allowed=false
broker_action=none
manual_review_required=true
```

## 阶段拆解

|任务|阶段|当前状态|验收标准|
|---|---|---|---|
|TRADING-976|dataset lineage / leakage audit|VALIDATING|`layer1_dataset_lineage_leakage_audit.json/md` 检查 feature_time、label_time、forward outcome/label/oracle contamination、unmatured labels、definition hash 和 data-quality status；真实 CLI 状态 `LAYER1_DATASET_LEAKAGE_PASS`。|
|TRADING-977|naive selector baseline suite|VALIDATING|`layer1_naive_selector_baselines.json/md` 输出 always / alternate / trend / vol / drawdown / last-winner / seeded-random selectors 的 return、drawdown、Sharpe、Calmar、turnover、regret 和 cost-adjusted score；真实 CLI 状态 `NAIVE_SELECTOR_BASELINES_READY`。|
|TRADING-978|simple rule selector search|VALIDATING|`layer1_simple_rule_selector_search.json/md` 只搜索 QQQ 200DMA、drawdown、realized vol、vol expansion、SGOV carry proxy 和 trend strength 等简单规则，不使用 ML 或 future outcome features；真实 CLI 状态 `SIMPLE_RULE_SELECTOR_SEARCH_READY`。|
|TRADING-979|selector cost-adjusted evaluation|VALIDATING|`layer1_selector_cost_adjusted_evaluation.json/md` 对 naive/simple-rule selectors 输出 gross/net return、switch count、holding period、cost drag、latency drag、regret 和 penalty score；真实 CLI 状态 `SELECTOR_COST_EVAL_READY`。|
|TRADING-980|selector regime / period validation|VALIDATING|`layer1_selector_regime_period_validation.json/md` 按 2012-2015、2016-2019、2020-2021、2022、2023、2024、2025-to-latest、bull/bear/range/high-vol/low-vol/above/below-200DMA 分层；真实 CLI 状态 `SELECTOR_REGIME_MIXED`。|
|TRADING-981|selector failure case review|VALIDATING|`layer1_selector_failure_case_review.json/md` 复盘 selected_100qqq_before_drawdown、selected_equal_risk_before_rally、high_turnover_chop、missed_rebound、late_risk_off、late_risk_on；真实 CLI 状态 `FAILURE_CASE_REVIEW_READY`。|
|TRADING-982|historical research readiness gate|VALIDATING|`layer1_historical_research_readiness_gate.json/md` 聚合 967～981，决定是否允许 research-only Layer-1 historical research；真实 CLI 状态 `LAYER1_HISTORICAL_RESEARCH_ALLOWED_RESEARCH_ONLY`。|
|TRADING-983|owner decision pack|VALIDATING|`layer1_research_owner_decision_pack.json/md` 回答 owner 10 个问题并给出 recommendation；真实 CLI 状态 `LAYER1_OWNER_DECISION_PACK_READY`，recommendation=`START_LAYER1_SIMPLE_RULE_RESEARCH`。|
|TRADING-984|Reader Brief safety preview|VALIDATING|`layer1_reader_brief_safety_preview.json/md` 只允许显示 research status、component pool/headroom/scope 和安全字段，禁止交易建议；真实 CLI 状态 `LAYER1_READER_PREVIEW_SAFE`。|
|TRADING-985|master review|VALIDATING|`layer1_meta_policy_master_review.json/md` 汇总 967～984，给出阶段性结论和下一阶段最小任务；真实 CLI 状态 `LAYER1_SIMPLE_RULE_RESEARCH_READY`。|

## 新增 CLI

```bash
aits research strategies layer1-dataset-lineage-leakage-audit
aits research strategies layer1-naive-selector-baselines
aits research strategies layer1-simple-rule-selector-search
aits research strategies layer1-selector-cost-adjusted-evaluation
aits research strategies layer1-selector-regime-period-validation
aits research strategies layer1-selector-failure-case-review
aits research strategies layer1-historical-research-readiness-gate
aits research strategies layer1-research-owner-decision-pack
aits research strategies layer1-reader-brief-safety-preview
aits research strategies layer1-meta-policy-master-review
```

TRADING-971～975 的 prerequisite CLI 位于
`TRADING-957_to_975_Layer2_Strategy_Component_Readiness_for_Layer1_MetaPolicy.md`。

## Guardrails

- Layer-1 features 不得包含 future outcome、best component label、oracle signal 或
  unmatured label。
- Reference-only components 可以参与 regret/reference comparison，但不得作为
  selectable output。
- QQQ-plus growth、TQQQ-heavy、tail-risk fallback、LEAPS、Wheel 和 options 不得进入
  selectable label 或 final combiner。
- ML selector 禁止；本批最多允许 simple-rule selector research。
- 所有 cached-data dependent 命令必须先走同源 `aits validate-data` quality gate。

## 进展记录

- 2026-06-24: 新增需求文档并进入 `IN_PROGRESS`。967～970 的 material headroom
  支持继续推进 simple-rule research readiness；transition cost material 要求 979
  和 982 继续显式扣成本并保留 owner review。
- 2026-06-24: 976～985 实现完成并转入 `VALIDATING`。真实 CLI 结果：
  976=`LAYER1_DATASET_LEAKAGE_PASS`，977=`NAIVE_SELECTOR_BASELINES_READY`，
  978=`SIMPLE_RULE_SELECTOR_SEARCH_READY`，979=`SELECTOR_COST_EVAL_READY`，
  980=`SELECTOR_REGIME_MIXED`，981=`FAILURE_CASE_REVIEW_READY`，
  982=`LAYER1_HISTORICAL_RESEARCH_ALLOWED_RESEARCH_ONLY`，
  983=`LAYER1_OWNER_DECISION_PACK_READY` / recommendation=`START_LAYER1_SIMPLE_RULE_RESEARCH`，
  984=`LAYER1_READER_PREVIEW_SAFE`，985=`LAYER1_SIMPLE_RULE_RESEARCH_READY`。
  全部输出继续 `paper_shadow_allowed=false`、`production_allowed=false`、
  `broker_action=none`、`manual_review_required=true`。
