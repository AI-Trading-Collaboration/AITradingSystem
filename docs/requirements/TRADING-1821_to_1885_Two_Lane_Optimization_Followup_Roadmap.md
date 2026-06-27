# TRADING-1821 to 1885 Two-Lane Optimization Follow-up Roadmap

## 状态

- Parent scope: `TRADING-1821_to_1885_TWO_LANE_OPTIMIZATION_FOLLOWUP_ROADMAP`
- Status: `PROPOSED / READY by phase`
- Owner: system implementation + project owner review
- Date opened: 2026-06-28
- Market regime: `ai_after_chatgpt`
- Upstream policy: `two_lane_signal_policy_v1`
- Safety boundary: research-only, actual-path required for any candidate, target-path diagnostic-only, dynamic promotion blocked, no paper-shadow, no production, no broker.

## 背景

TRADING-1806 to 1820 已把 current first-layer v2 从 universal layer 降级为 `RETURN_SEEKING_DIAGNOSTIC_ONLY`，并建立 lane separation policy。后续不能直接把 add-risk 信号接入 defensive overlay，也不能跳到 gated integration。必须先独立验证 defensive preservation lane，再评估 return-seeking diagnostic lane；只有两者都有证据时才进入 gated overlay integration。

## 阶段与依赖

| Phase | Task id | Status | Dependency |
|---|---|---|---|
| Phase 2 | `TRADING-1821_to_1840_DEFENSIVE_PRESERVATION_LANE_RESEARCH` | `VALIDATING` | Defensive-only implementation complete; no material improvement, gated integration remains blocked |
| Phase 3 | `TRADING-1841_to_1860_RETURN_SEEKING_DIAGNOSTIC_LANE_RESEARCH` | `VALIDATING` | Upside exists but drawdown regresses; beta/TQQQ and 2023+ dependent; diagnostic-only |
| Phase 4 | `TRADING-1861_to_1875_TWO_LANE_GATED_OVERLAY_INTEGRATION` | `BLOCKED_OWNER_INPUT` | Blocked by Phase 2 no material improvement and Phase 3 no risk-adjusted diagnostic value |
| Phase 5 | `TRADING-1876_to_1885_MULTI_WINDOW_VALIDATION_AND_OVERFITTING_PRE_REVIEW` | `BLOCKED_OWNER_INPUT` | No locked candidate because Phase 4 is blocked; validation only, no tuning |

## Phase 2 Acceptance

- Defensive lane inputs may include risk-off probability, defensive probability, do-not-de-risk probability, event risk, volatility regime, drawdown/recovery features.
- Add-risk, high-confidence risk-on and TQQQ-related signals are disabled.
- Outputs include defensive lane scope/policy, label taxonomy, action-value policy, feature matrix/PIT audit, defensive model review, actual-path matrix, 2022 slice review and closeout.
- Passing criteria: defensive probes do not regress, drawdown does not worsen, false risk-off cost declines or no-material-improvement is documented.

## Phase 3 Acceptance

- Return-seeking signals remain diagnostic-only.
- Outputs include scope/policy, signal audit, return-seeking actual-path review, beta/TQQQ attribution, 2022 vs 2023+ contrast and closeout.
- Must answer whether signal value is just higher QQQ-equivalent exposure, TQQQ beta, or 2023+ AI/tech trend dependence.

## Phase 4 Gate

- Do not implement actual-path gated integration unless Phase 2 and Phase 3 both produce usable evidence.
- Defensive lane has veto power.
- Growth overlay only activates when defensive overlay is inactive and risk-off veto is clear.
- TQQQ is disabled or capped at a minimal diagnostic level.

## Phase 5 Gate

- Candidate must be locked before validation.
- Multi-window validation covers primary `2021-02-22`, legacy `2022-12`, and sensitivity `2020-05-28`.
- Overfitting review covers candidate count, label/feature/probe versions, same-risk frontier, TQQQ beta, 2022 slice stability and 2023+ dependence.
- Even a passing result can only enter observe-only forward watch.

## Progress Notes

- 2026-06-28: Registered Phase 2 to Phase 5 follow-up tasks after TRADING-1806 to 1820 lane separation closeout. Phase 4 and Phase 5 remain proposed because their prerequisites are not yet satisfied.
- 2026-06-28: Started Phase 2 implementation. Scope is defensive preservation lane only: add-risk, high-confidence risk-on and TQQQ-related signals remain disabled; gated integration remains blocked until Phase 2 and Phase 3 evidence exists.
- 2026-06-28: Implemented Phase 2 defensive preservation lane and moved to validation. Generated scope/policy, label taxonomy, defensive action-value policy, feature PIT audit, labels, predictions, model review, defensive actual-path review, 2022 slice review and closeout. Real final status is `DEFENSIVE_LANE_NO_MATERIAL_IMPROVEMENT`: defensive probes do not regress and drawdown is not worse, but `false_risk_off_cost_declined=false`; add-risk, high-confidence risk-on, TQQQ signals, owner review, promotion, paper-shadow, production, broker and gated integration remain disabled.
- 2026-06-28: Started Phase 3 implementation. Scope is return-seeking diagnostic only: `stay_constructive`, `add_risk` and `high_confidence_risk_on` may be audited against return-seeking probes and TQQQ/beta attribution, but cannot drive defensive overlay, full allocation, gated integration, promotion, paper-shadow, production or broker.
- 2026-06-28: Implemented Phase 3 return-seeking diagnostic lane and moved to validation. Real final status is `RETURN_SEEKING_DIAGNOSTIC_UPSIDE_DEPENDENT_DRAWDOWN_REGRESSED_PROMOTION_BLOCKED`: 7/7 return-seeking probes have positive return delta vs no-return-seeking reference, but 7/7 also have drawdown regression and diagnostic value count is 0; TQQQ beta dependency and 2023+ dependence are both true. Phase 4 remains blocked because Phase 2 did not establish material defensive improvement and Phase 3 did not establish risk-adjusted diagnostic value.
- 2026-06-28: Phase 5 is blocked with Phase 4 because there is no locked candidate to validate. Multi-window validation must not be used to keep tuning or manufacture a candidate after Phase 2/3 failed their gates.
