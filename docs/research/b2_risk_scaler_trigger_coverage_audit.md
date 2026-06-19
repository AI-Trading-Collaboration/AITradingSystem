# B2 Risk Scaler Trigger Coverage Audit

- Status: B2_REQUIRES_RISK_HEAVY_WINDOWS
- Data Quality: PASS_WITH_WARNINGS
- Production Effect: none

## Reader Brief

- Summary: B2 trigger coverage was audited across diagnostic windows.
- Key Result: B2_REQUIRES_RISK_HEAVY_WINDOWS
- Blocking Issues: none
- Warnings: Research-only diagnosis; B5/B6/v3 remain blocked unless checkpoint allows.
- Safety Boundary: research_only=true; manual_review_only=true; official_target_weights=false; production_effect=none
- Next Action: Do not run B5 unless b5_admission_checkpoint allows it.
