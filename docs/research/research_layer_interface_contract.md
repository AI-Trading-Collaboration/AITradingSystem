# Research Layer Interface Contract

- Status：RESEARCH_LAYER_INTERFACE_CONTRACT_READY
- Generated At：2026-06-19T08:55:21.941572+00:00
- Production Effect：none

## Layers

| Layer | Artifact Family | Consumes | Produces |
|---|---|---|---|
| feature | feature_artifact | raw_price_data, volume_data, approved_macro_inputs | feature_matrix, feature_quality_flags |
| signal | signal_artifact | feature_artifact | signal_score, state, confidence, diagnostics |
| target | target_path_artifact | signal_artifact | research_only_target_path |
| execution | executed_weight_artifact | target_path_artifact | executed_hypothetical_research_weights |
| evaluation | evaluation_artifact | executed_weight_artifact, market_return_data | research_metrics, gate_result |

## Reader Brief

- Summary：Feature、Signal、Target、Execution、Evaluation 五层接口已冻结。
- Key Result：RESEARCH_LAYER_INTERFACE_CONTRACT_READY
- Blocking Issues：B2/B3 仍需通过 signal diagnostics 后才能进入 target mapping。
- Warnings：该合同只冻结边界，不代表 B2-B6 信号或目标路径已实现。
- Safety Boundary：research_only=true; official_target_weights=false; production_effect=none
- Next Action：运行 dependency boundary validation 并建立 signal diagnostics framework。
