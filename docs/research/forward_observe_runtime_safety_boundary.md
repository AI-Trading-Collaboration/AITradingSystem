# Forward Observe Runtime Safety Boundary

- observe_mode: `observe_only`
- portfolio_effect: `none`
- production_effect: `none`
- broker_action: `none`

## Interpretation Boundary

- `risk_cap_trigger_is_not_buy_sell_or_rebalance_signal`
- `evidence_accumulation_status_is_not_promotion_readiness`
- `owner_precheck_candidate_is_not_owner_approval`

Promotion、paper-shadow、production、broker action 全部保持 false / none。
