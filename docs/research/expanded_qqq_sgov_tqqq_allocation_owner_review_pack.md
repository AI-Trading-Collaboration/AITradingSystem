# Expanded QQQ / SGOV / TQQQ Allocation Owner Review Pack

- 状态：`EXPANDED_OWNER_REVIEW_PACK_READY_PROMOTION_BLOCKED`
- best_static_candidate：`simplex_qqq0000_sgov1000_tqqq0000`
- best_dynamic_candidate：`expanded_state_highest_return_under_max_dd_cap`
- surviving_candidate_count：`0`
- TQQQ data quality：`TQQQ_RESEARCH_ONLY_APPROVED`
- promotion_status：`BLOCKED`
- paper_shadow_allowed：`false`
- production_allowed：`false`
- broker_action：`none`

## Owner Questions

1. 放开三资产权重后是否发现明显更优候选：见 survival matrix。
2. 更优是否只是 TQQQ beta：见 TQQQ risk attribution 和 same-risk comparison。
3. 是否打败静态三资产 frontier：见 same-risk baseline comparison。
4. 同风险基准下是否仍有优势：见 `annual_return_edge`、`sharpe_edge`、`calmar_edge`。
5. 是否值得进入 watch-only forward：本批不自动批准，需 owner review。
6. dynamic promotion 为什么仍 blocked：TQQQ promotion review、walk-forward、stress 和 owner review 未通过。

## Remaining Blockers

- `owner_review_pending`
- `tqqq_promotion_universe_review_required`
- `walk_forward_validation_blocking_until_real_forward_evidence`
- `stress_review_blocks_high_tqqq_or_high_exposure_candidates`
