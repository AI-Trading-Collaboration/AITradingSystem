# B1 B4 Multi Window Diagnostic Expansion

- Status: MULTI_WINDOW_DIAGNOSTIC_COMPLETE
- Data Quality: PASS_WITH_WARNINGS
- Production Effect: none

## Reader Brief

- Summary: B1-B4 multi-window diagnostic expansion completed without touching holdout.
- Key Result: MULTI_WINDOW_DIAGNOSTIC_COMPLETE
- Blocking Issues: none
- Warnings: Research-only diagnosis; B5/B6/v3 remain blocked unless checkpoint allows.
- Safety Boundary: research_only=true; manual_review_only=true; official_target_weights=false; production_effect=none
- Next Action: Do not run B5 unless b5_admission_checkpoint allows it.

## Windows

- normal_uptrend: B4_vs_B0 return_delta=0.003566; turnover_delta=0.871735
- rapid_drawdown: B4_vs_B0 return_delta=-0.002235; turnover_delta=0.221959
- slow_drawdown: B4_vs_B0 return_delta=-0.029420; turnover_delta=1.803341
- high_volatility_sideways: B4_vs_B0 return_delta=-0.000788; turnover_delta=0.530279
- v_shaped_recovery: B4_vs_B0 return_delta=0.000261; turnover_delta=0.173884
- semiconductor_correction: B4_vs_B0 return_delta=-0.001727; turnover_delta=0.128405
- false_risk_off_cluster: B4_vs_B0 return_delta=-0.000125; turnover_delta=0.244777
