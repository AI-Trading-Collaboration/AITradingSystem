# Indicator Family Beta / TQQQ Dependency Review

状态：`INDICATOR_FAMILY_BETA_TQQQ_DEPENDENCY_REVIEW_READY`

该报告为 research-only evidence，不产生 target weights，不触发 promotion、paper-shadow、production 或 broker。

| family_name | dependency_status | add_risk_allowed | blocked_usage |
|---|---|---|---|
| trend_persistence | FAMILY_NOT_TQQQ_BETA_DEPENDENT | False | allocation, promotion |
| relative_strength | FAMILY_TQQQ_BETA_DEPENDENT | False | add_risk, allocation, promotion |
| volatility_compression | FAMILY_NOT_TQQQ_BETA_DEPENDENT | False | allocation, promotion |
| drawdown_recovery | FAMILY_NOT_TQQQ_BETA_DEPENDENT | False | allocation, promotion |
| breadth_participation | FAMILY_NOT_TQQQ_BETA_DEPENDENT | False | allocation, promotion |
| rates_liquidity | FAMILY_NOT_TQQQ_BETA_DEPENDENT | False | allocation, promotion |
| event_risk | FAMILY_NOT_TQQQ_BETA_DEPENDENT | False | allocation, promotion |
