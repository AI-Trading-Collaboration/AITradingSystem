# TRADING-1976～2005 Channel-Specific First-Layer v3

最后更新：2026-06-28

## 背景

TRADING-1946～1975 已把 indicator family ablation 从 registry-only diagnostic 推进到
family-level evidence。当前可用于下一轮 channel-specific research model 的 families 为：

- `drawdown_recovery` -> `do_not_de_risk`
- `volatility_compression` / `rates_liquidity` -> `risk_on_veto`

`trend_persistence` 和 `relative_strength` 只能作为 return-seeking diagnostic，其中
`relative_strength` 还存在 beta / TQQQ dependency；`breadth_participation` 和 `event_risk`
因 PIT source blocker 不得进入模型。Add-risk selected families 为空。

## 范围

本批只实现两个 channel-specific first-layer v3 diagnostics：

- `do_not_de_risk`
- `risk_on_veto`

禁止范围：

- 不训练 universal first-layer。
- 不训练 add-risk allocation model。
- 不输出 portfolio weights、target allocation 或 trade action。
- 不启用 growth overlay / TQQQ allocation / gated integration。
- 不修改 frozen second-layer probe registry v2 或 probe weights。
- 不使用 target-path metrics 作为通过依据。
- 不使用 PIT-blocked breadth/event family。
- 不把 2023+ dependent 或 beta-only family 当作 primary evidence。

## 固定输入

- `config/research/two_layer_strategy_boundary_contract.yaml`
- `inputs/research_reviews/first_layer_signal_usage_matrix_v2.yaml`
- `config/research/first_layer_channel_policy.yaml`
- `config/research/base_overlay_veto_policy_schema.yaml`
- `config/research/indicator_family_registry.yaml`
- `config/research/channel_specific_feature_set_v1.yaml`
- `inputs/research_reviews/indicator_family_selection_matrix.yaml`
- `config/research/dynamic_second_layer_probe_registry_v2.yaml`
- `outputs/research_trends/pit_feature_matrix/pit_feature_matrix_v3.csv`
- `outputs/research_trends/trend_labels/upper_state_labels_v2.csv`
- `outputs/research_trends/action_value_matrix_v2/action_value_matrix_v2.csv`
- `outputs/research_trends/models/first_layer_composer_v2_predictions.csv`

本批 actual-path diagnostics 读取本地 validated price/rates cache，并必须运行同源 cached-data
quality gate；所有下游报告披露 `data_quality_status`。

## 实施步骤

1. 新增 `do_not_de_risk_v3_selection_rule.yaml` 与 `risk_on_veto_v3_selection_rule.yaml`。
2. 锁定 `channel_specific_feature_set_v1_locked.yaml`，只允许本批 selected families 进入模型。
3. 生成 do-not-de-risk label v3、risk-on veto label v3 和 channel PIT feature matrix v3。
4. 训练低复杂度 monotonic scorecard/threshold v3 models。
5. 生成 channel composer v3 predictions，不输出 weights 或 trade action。
6. 使用 boundary-aware policy compiler 做 dry-run，验证 risk-on veto 能阻断 growth overlay。
7. 将 channel outputs 接回 frozen probes 做 actual-path diagnostics。
8. 输出 2022 slice、2023+ dependence、false add-risk、false risk-off、selection rule result、owner pack 和 closeout。
9. 更新 report registry、artifact catalog、system flow、task register 和 guardrail tests。

## 验收标准

- 只使用 selected indicator families。
- `do_not_de_risk` 和 `risk_on_veto` 分别建模。
- 不输出 add-risk allocation。
- 不输出 weights / trade advice。
- policy compiler dry-run 能执行 veto。
- actual-path diagnostic 完成。
- 2022 slice / 2023+ dependence / false-add-risk / false-risk-off review 完成。
- selection rule result 明确。
- dynamic promotion、paper-shadow、production、broker 继续 disabled。

## 安全边界

- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `dynamic_promotion_status=BLOCKED`
- `candidate_count=0`
- `can_emit_weights=false`

## 进展记录

- 2026-06-28：实现完成并进入验证。新增 `aits research trends channel-specific-v3`，生成
  v3 scope、feature-set lock、channel labels、channel PIT matrix、monotonic scorecard
  models、composer predictions、policy compiler dry-run、actual-path diagnostics、2022
  slice、2023+ dependence、false add-risk / false risk-off reviews、selection result、
  owner pack 和 closeout/final matrix。真实 run 的 `data_quality_status=PASS_WITH_WARNINGS`，
  final status 为 `CHANNEL_V3_RISK_ON_VETO_ONLY`：`risk_on_veto` 通过 false-add-risk /
  compiler-veto gate，`do_not_de_risk` 未通过 false-risk-off / missed-upside /
  defensive-regression gate。`candidate_count=0`，promotion、paper-shadow、production、
  broker 继续 blocked/false/false/none。
- 2026-06-28：新增并进入 `IN_PROGRESS`。本批只构建 channel-specific first-layer v3 diagnostics，
  不创建 strategy candidate，不进入 owner review / promotion / paper-shadow / production / broker。
