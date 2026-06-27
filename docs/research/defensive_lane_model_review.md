# Defensive Lane Model Review

- 状态：`DEFENSIVE_LANE_MODEL_REVIEW_READY_PROMOTION_BLOCKED`
- 市场阶段：`ai_after_chatgpt`
- promotion_allowed：`False`
- paper_shadow_allowed：`False`
- production_allowed：`False`
- broker_action：`none`

## 摘要
- research_window_id: `exact_three_asset_validated`
- requested_start: `2021-02-22`
- actual_start: `2021-02-22`
- actual_portfolio_start: `2021-02-22`
- end: `latest`
- window_role: `primary_validated`
- data_quality_contract: `secondary_cross_checked`
- exact_or_proxy: `exact`
- data_quality_status: `PASS_WITH_WARNINGS`
- model_count: `3`
- prediction_count: `1343`
- label_count: `1323`
- predicted_state_distribution: `{'neutral': 998, 'risk_off': 174, 'defensive': 171}`
- label_state_distribution: `{'neutral': 771, 'risk_off': 522, 'defensive': 30}`
- add_risk_prediction_count: `0`
- high_confidence_risk_on_prediction_count: `0`
- tqqq_signal_allowed: `False`
- promotion_status: `blocked`

## Models

| model_id | predicted | actual | precision | recall | status |
|---|---:|---:|---:|---:|---|
| risk_off_detector_v4 | 174 | 522 | 0.787356 | 0.262452 | LOW_COMPLEXITY_DIAGNOSTIC_ONLY |
| do_not_de_risk_detector_defensive_v1 | 280 | 496 | 0.364286 | 0.205645 | LOW_COMPLEXITY_DIAGNOSTIC_ONLY |
| re_risk_allowed_detector_v1 | 709 | 566 | 0.698166 | 0.874558 | LOW_COMPLEXITY_DIAGNOSTIC_ONLY |
