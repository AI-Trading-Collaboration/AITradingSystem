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
| Phase 2 | `TRADING-1821_to_1840_DEFENSIVE_PRESERVATION_LANE_RESEARCH` | `READY` | Must follow lane separation policy; add-risk disabled |
| Phase 3 | `TRADING-1841_to_1860_RETURN_SEEKING_DIAGNOSTIC_LANE_RESEARCH` | `READY` | Must remain diagnostic-only and cannot drive defensive overlay |
| Phase 4 | `TRADING-1861_to_1875_TWO_LANE_GATED_OVERLAY_INTEGRATION` | `PROPOSED` | Requires Phase 2 defensive no-regression and Phase 3 diagnostic value |
| Phase 5 | `TRADING-1876_to_1885_MULTI_WINDOW_VALIDATION_AND_OVERFITTING_PRE_REVIEW` | `PROPOSED` | Requires a locked candidate from Phase 4; validation only, no tuning |

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
