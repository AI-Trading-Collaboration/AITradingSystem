# B2 B3 Branch Status Snapshot

- Status: CONTINUE_B2_ONLY_RESEARCH
- Market Regime: ai_after_chatgpt
- Requested Range: 2022-12-01 to 2026-06-18
- Data Quality: PASS_WITH_WARNINGS
- Production Effect: none

## Reader Brief

- Summary: Branch snapshot keeps B2-only research active and B4/B5/B6/v3 blocked.
- Key Result: CONTINUE_B2_ONLY_RESEARCH
- Blocking Issues: none
- Warnings: Research-only B2 full diagnostic and B3 signal resolution; no B2 tuning, B3 weights, B3 mini-backfill, B4/B5/B6/v3, paper-shadow, broker/order or production action.
- Safety Boundary: research_only=true; manual_review_only=true; official_target_weights=false; production_effect=none
- Next Action: Manual owner/research review before any subsequent gate.

## Branch

- B2: B2_ONLY_NEEDS_MORE_EVIDENCE

## Allowed Flags

- b5_allowed: False
- b6_allowed: False
- v3_allowed: False
