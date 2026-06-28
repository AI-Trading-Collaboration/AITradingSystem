# TRADING-1946～1975 Indicator Family Ablation and Channel-Specific Signal Evidence

最后更新：2026-06-28

## 背景

TRADING-1906～1945 已完成 boundary-aware two-layer optimization framework，但
`indicator_family_ablation` 仍停留在 registry-only diagnostic。当前系统具备 first-layer /
second-layer boundary、signal usage matrix v2、channel policy、base+overlay+veto schema、
policy compiler、error attribution 和 channel-aware evaluator，但还没有真实 family-level
PIT / coverage / action-value evidence。

本批把 indicator family ablation 从“框架 ready”推进到“证据 ready”。它只回答 family 是否
具备进入下一轮 channel-specific first-layer model 的资格，不恢复 dynamic promotion，不进入
paper-shadow / production / broker，不训练 universal first-layer，不优化 second-layer weights。

## 范围

覆盖以下 indicator families：

- `trend_persistence`
- `relative_strength`
- `volatility_compression`
- `drawdown_recovery`
- `breadth_participation`
- `rates_liquidity`
- `event_risk`

必须评估的 channel：

- defensive channel
- do_not_de_risk
- stay_constructive
- add_risk
- risk_on_veto
- return_seeking_diagnostic

## 固定输入

- `config/research/two_layer_strategy_boundary_contract.yaml`
- `inputs/research_reviews/first_layer_signal_usage_matrix_v2.yaml`
- `config/research/first_layer_channel_policy.yaml`
- `config/research/base_overlay_veto_policy_schema.yaml`
- `config/research/dynamic_second_layer_probe_registry_v2.yaml`
- `config/research/indicator_family_registry.yaml`
- `outputs/research_trends/pit_feature_matrix/pit_feature_matrix_v3.csv`
- `outputs/research_trends/trend_labels/upper_state_labels_v2.csv`
- `outputs/research_trends/action_value_matrix_v2/action_value_matrix_v2.csv`
- `outputs/research_trends/action_value_matrix_v2/action_value_summary_v2.json`

本批使用既有、已生成的 research artifacts 做二次证据审计，不直接刷新 cached market/macro
data；输出必须披露 `data_quality_contract=PREVIOUS_VALIDATED_RESEARCH_ARTIFACTS`，并保留上游
action-value summary 的 data quality status。

## 实施步骤

1. 新增 `config/research/indicator_family_ablation_selection_rule.yaml`，预注册 family selection
   gate，明确 PIT、primary window、target-path diagnostic-only、no-promotion、channel pass 和 fail-if
   条件。
2. 扩展 `aits research trends indicator-family-ablation`，读取 PIT feature matrix、labels 和
   action-value matrix，生成 scope、registry validation、PIT coverage、action-value by family、family-only
   simple model、channel review、2022 slice、2023+ dependence、beta/TQQQ dependence、interaction warning、
   selection matrix、channel-specific feature set、owner pack 和 closeout。
3. 对每个 family 回答 13 个问题：PIT、coverage、2022 slice、do_not_de_risk、stay_constructive、
   add_risk、false add-risk、false risk-off、risk_on_veto、2023+ dependence、TQQQ/beta dependence、
   actual-path vs classification、next model eligibility。
4. 更新 report registry、artifact catalog、system flow、task register 和 research audit metadata tests。
5. 新增 guardrail tests，确保 PIT blocked family 不能被选中，2023+ dependent 和 beta-only family
   不能通过 add-risk，selection matrix 记录 allowed/blocked channels，feature set 只使用已选 family。

## 验收标准

- 每个 required family 都有 PIT / coverage audit row。
- 每个 family 至少有一个 channel-specific action-value conclusion。
- do_not_de_risk、add_risk、risk_on_veto 有独立矩阵。
- 2022 slice、2023+ dependence 和 beta/TQQQ dependence 明确评估。
- `inputs/research_reviews/indicator_family_selection_matrix.yaml` 生成。
- `config/research/channel_specific_feature_set_v1.yaml` 生成。
- 没有 family 因 target-path only 或 beta-only evidence 通过 selection。
- dynamic promotion、paper-shadow、production、broker 继续 disabled。
- 相关 focused parallel pytest、Ruff、compileall、documentation contract 和 diff checks 通过。

## 安全边界

- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `dynamic_promotion_status=BLOCKED`
- `candidate_count=0`
- `can_emit_weights=false`

## 进展记录

- 2026-06-28：新增并进入 `IN_PROGRESS`。实现范围限定为 indicator family evidence 和
  channel-specific selection/rejection/diagnostic-only 结论；所有 allocation、promotion、paper-shadow、
  production、broker 路径继续关闭。
- 2026-06-28：实现完成并转入 `VALIDATING`。`aits research trends indicator-family-ablation`
  生成 scope、registry validation、PIT coverage、family action-value dataset、family-only threshold
  model、channel reviews、2022 slice、2023+ dependence、beta/TQQQ dependence、interaction warning、
  selection matrix、`channel_specific_feature_set_v1`、owner pack 和 final matrix。真实结论为 7 个
  families 中 5 个 PIT coverage pass、2 个 PIT blocked；`drawdown_recovery` 进入 do_not_de_risk，
  `volatility_compression` / `rates_liquidity` 进入 risk_on_veto，`trend_persistence` /
  `relative_strength` 仅进入 return-seeking diagnostic，add-risk allowed families 为空；candidate_count=0，
  promotion/paper-shadow/production/broker 继续 disabled。
