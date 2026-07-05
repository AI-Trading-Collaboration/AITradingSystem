# Dynamic strategy observation gate threshold calibration review

## 1. Executive summary

- status：`DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_READY`
- current best candidate：`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`
- current best decision：`CONTINUE_OPTIMIZATION`
- observation-ready candidate found in 2386：`False`
- recommended policy action：`CALIBRATE_RESEARCH_ONLY_OBSERVATION_GATE_BEFORE_OWNER_PAUSE_DECISION`
- research-only gate may be too strict：`True`
- 本任务不批准 observation，不修改真实 gate，不进入 paper-shadow / production / broker。

## 2. Source findings from TRADING-2386

- data quality from 2386：`PASS_WITH_WARNINGS`
- reference candidates tested：`5`
- new candidates tested：`12`
- signal families tested：`6`
- top metrics：`{"candidate_id": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1", "candidate_type": "reference_candidate", "candidate_vs_guarded_ranking_top_gap": 0.000682, "candidate_vs_lower_turnover_gap": 0.019097, "conservative_cost_passed": true, "decision": "CONTINUE_OPTIMIZATION", "drawdown_gap_vs_static": 0.043574, "drawdown_not_materially_worse": false, "dynamic_vs_static_gap": 0.021302, "harsh_cost_passed": true, "rank": 1, "realistic_cost_passed": true, "regime_slice_pass_rate": 0.0, "return_advantage_retained": 1.0, "signal_family": "reference_ranking_top", "time_slice_pass_rate": 0.0, "turnover_budget_passed": true}`

## 3. Current observation gate rules

- current reference policy：`HARD_BLOCK_ACCEPTANCE`
- thresholds：`{"drawdown_worse_tolerance": 0.02, "observation_regime_slice_pass_rate_min": 0.5, "observation_time_slice_pass_rate_min": 0.6, "regime_slice_pass_rate_acceptable_min": 0.3, "return_advantage_retained_min": 0.5, "time_slice_pass_rate_acceptable_min": 0.4, "turnover_budget_max_monthly": 1.0}`

## 4. Why 2386 resulted in CONTINUE_OPTIMIZATION

- current decision reasonable under current rules：`True`
- primary blockers：`['reference_candidate_hard_block', 'time_slice_pass_rate_below_acceptance', 'regime_slice_pass_rate_below_acceptance', 'drawdown_not_materially_worse=false']`
- not a return failure：`True`
- not a cost failure：`True`
- interpretation：2386 的 CONTINUE_OPTIMIZATION 在当前规则下合理；2387 只指出 research-only observation 需要一个 owner-review 中间层，而不是自动放行。

## 5. Reference candidate policy review

- current policy：`HARD_BLOCK_ACCEPTANCE`
- recommended policy：`BLOCK_AUTO_ACCEPT_BUT_ALLOW_OWNER_REVIEW`
- owner review allowed：`True`
- auto accept allowed：`False`

## 6. Time-slice threshold review

- current acceptance threshold：`0.6`
- owner-review proposed threshold：`0.3`
- current best time slice：`0.0`
- 结论：0.0 是当前 best 的真实稳定性缺口，但 research-only gate 应增加 owner-review tier。

## 7. Regime-slice threshold review

- current acceptance threshold：`0.5`
- single global pass rate may be too blunt：`True`
- regime expectation policy：`{"high_volatility": "drawdown_control", "low_volatility": "capture_upside", "recovery": "reentry_not_too_slow", "risk_off": "not_materially_worse_than_static", "risk_on": "outperform_or_match_static", "trend_confirmed": "outperform_static"}`

## 8. Drawdown materiality review

- drawdown_not_materially_worse：`False`
- drawdown gap vs static：`0.043574`
- return per drawdown penalty：`0.48887`
- materiality tier：`owner_review_required`

## 9. Research-only observation vs paper-shadow gate separation

- finding：The current 2386 observation gate may be too close to a paper-shadow gate because it has no owner-review-only tier.
- research-only observation 是 artifact-only/no-side-effect；paper-shadow 会创建 paper trades 或 shadow positions，必须更高门槛和 explicit owner approval。

## 10. Candidate reclassification preview

- `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1` current=`CONTINUE_OPTIMIZATION` preview=`OWNER_REVIEW_REQUIRED` auto_accept=`False` owner_review=`True`
- preview 不等于真实改规则，也不等于批准 observation。

## 11. Recommended gate policy update

- policy update applied：`False`
- recommended intermediate state：`OWNER_REVIEW_FOR_RESEARCH_ONLY_OBSERVATION`
- next task：`TRADING-2388_Dynamic_Strategy_Calibrated_Gate_Owner_Review_And_Next_Decision`

## 12. Explicit non-goals

- safety：`{"append_historical_event_log": false, "bind_outcome": false, "call_broker_api": false, "create_paper_trade": false, "create_scheduled_task": false, "create_shadow_position": false, "enable_paper_shadow": false, "enable_production": false, "enable_scheduler": false, "generate_daily_report": false, "generate_new_signal": false, "mutate_outcome_store": false, "run_new_backtest": false, "send_order": false}`

## 13. Recommended next route

- `TRADING-2388_Dynamic_Strategy_Calibrated_Gate_Owner_Review_And_Next_Decision`
