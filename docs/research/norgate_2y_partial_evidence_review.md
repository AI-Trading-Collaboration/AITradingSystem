# Norgate 2Y Partial Evidence Review

- status: `NORGATE_2Y_PARTIAL_EVIDENCE_REVIEW_READY`
- local_signal_evidence_reason: `no_incremental_value`
- trial_2y_feature_value: `weak`
- full_history_needed_for_final_answer: `True`
- trial_based_purchase_recommendation: `no`
- stress_window_paid_experiment_recommendation: `conditional_yes`

## 复盘结论

- feature_variation_sufficient: `True`；nonflat_feature_count: `4`
- bucket_sample_sufficient: `True`；bucket_imbalance_ratio: `1.006667`
- outcome_dominated_by_few_days: `False`；dominated_benchmark_horizon_count: `0`
- event_count_sufficient: `True`；deterioration_event_count: `144`；recovery_event_count: `147`
- baseline_increment_direction: `worse_false_signal_rates`；false_risk_off_delta: `0.165655`；false_risk_on_delta: `0.060675`
- benchmark_signal_consistent: `False`；supporting_benchmark_count: `0`
- stress_2022_sample_available: `False`；earliest_price_date: `2024-06-28`

2Y trial 可以解释局部 feature 行为和购买 full-history 的必要性，但不能替代 2021 primary-window validation。
