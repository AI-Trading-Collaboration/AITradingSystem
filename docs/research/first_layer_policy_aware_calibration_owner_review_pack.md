# First-Layer Policy-Aware Calibration Owner Review Pack

- market_regime: `ai_after_chatgpt`
- default_backtest_start: `2022-12-01`
- data_quality_status: `PASS_WITH_WARNINGS`
- probe_count: `4`
- action_value_matrix_size: `66640`
- label_distribution: `{'constructive': 338, 'defensive': 35, 'neutral': 746, 'risk_off': 2032, 'risk_on': 181}`
- walk_forward_balanced_accuracy: `0.151597`
- final_status: `FIRST_LAYER_CALIBRATION_NO_MATERIAL_IMPROVEMENT_PROMOTION_BLOCKED`
- tqqq_diagnostic_status: `RESEARCH_ONLY_DIAGNOSTIC`
- promotion_status: `BLOCKED`

## Owner Questions

- Dynamic second-layer probes are trend-sensitive and frozen before label generation.
- Consensus labels are generated from action-value votes across multiple probes.
- The calibrated first layer is evaluated only on walk-forward validation windows.
- Probe backtests compare old scorecard vs new calibrated first layer on actual paths.
- TQQQ risk-on probe remains research-only diagnostic.
- Dynamic promotion remains BLOCKED because owner review and forward evidence are absent.
