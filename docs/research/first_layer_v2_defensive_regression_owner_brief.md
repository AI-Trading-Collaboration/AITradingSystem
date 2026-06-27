# First-Layer V2 Defensive Regression Owner Brief

- 状态：`FIRST_LAYER_V2_RETURN_SEEKING_DIAGNOSTIC_ONLY`
- owner_review_allowed：`False`
- promotion：`blocked`
- paper_shadow_allowed：`False`
- production_allowed：`False`
- broker_action：`none`

## 结论

1. coverage 修复后，旧 `8/8 improvement` 不再成立为主证据，因为 `wf_504d_baseline` 与 `wf_378d_initial` 的有效窗口太晚，不能满足 2022 coverage-aware selection。
2. coverage-pass variants 的回归集中在 defensive / drawdown-control 相关 probes：`defensive_overlay_probe`、`drawdown_control_probe`，并在 `wf_252d_initial` 中连带 `balanced_dynamic_probe`。
3. 2022 切片显示 regression 与 2022 drawdown、recovery 和 post-ChatGPT transition 的状态切换有关，signal attribution 指向 false add-risk / false do-not-de-risk / early re-risk 的组合。
4. first-layer v2 不是可晋级的通用 first-layer；当前只支持 `RETURN_SEEKING_ONLY_DIAGNOSTIC`。
5. risk-off-only fallback 不支持，因为 defensive overlay 与 drawdown-control 在 coverage-pass variants 下仍回归。
6. owner review 不允许：coverage-aware selection pass count 仍为 0。
7. promotion 继续 blocked；paper-shadow、production、broker 均保持 disabled。

## 关键字段

- policy_variants_analyzed: `['wf_504d_baseline', 'wf_378d_initial', 'wf_252d_initial', 'wf_expanding_initial', 'wf_warm_start_diagnostic']`
- regressed_probes: `['balanced_dynamic_probe', 'defensive_overlay_probe', 'drawdown_control_probe']`
- final_diagnosis: `RETURN_SEEKING_ONLY_DIAGNOSTIC`
- remaining_blockers: `['DEFENSIVE_PROBE_REGRESSION', 'RISK_OFF_ONLY_FALLBACK_NOT_SUPPORTED', 'OWNER_REVIEW_DISABLED', 'PROMOTION_BLOCKED']`
