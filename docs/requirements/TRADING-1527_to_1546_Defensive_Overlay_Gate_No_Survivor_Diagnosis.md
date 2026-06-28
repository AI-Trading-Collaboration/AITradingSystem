# TRADING-1527 to 1546 Defensive Overlay Gate and No-Survivor Diagnosis

最后更新：2026-06-28

## 状态

- Task id: `TRADING-1527_to_1546_DEFENSIVE_OVERLAY_GATE_NO_SURVIVOR_DIAGNOSIS`
- Status: `VALIDATING`
- Owner: system implementation + project owner review
- Date opened: 2026-06-27
- Market regime: `ai_after_chatgpt`
- Safety boundary: research-only, watch-only diagnostics only; dynamic promotion, paper-shadow, production and broker remain blocked.

## 编号说明

Owner 附件使用 TRADING-1526～1545，但 `TRADING-1526_EXPANDED_ACTUAL_PATH_CANDIDATE_FAILURE_MATRIX` 已经存在并完成。为避免 ID 复用，本批登记为 TRADING-1527～1546。附件中的原 TRADING-1526 被视为已由候选失败矩阵提供输入，本批第一步从该矩阵生成 no-survivor diagnosis。

## 背景

Expanded QQQ / SGOV / TQQQ actual-path research 生成 11 个 candidates，但 full allocation survival count 为 0。失败矩阵显示：

- candidate_count: 11
- `STATIC_FRONTIER_DOMINATES`: 7
- `NO_MATERIAL_IMPROVEMENT`: 4
- same-risk not advantaged: 7
- net-of-cost failed: 7
- walk-forward failed or pending split evidence: 11
- stress risk too high: 1
- TQQQ beta-only: 0

该结果足以阻断 full allocation promotion，但不能自动回答 defensive overlay 是否具有风控观察价值。本批把 full allocation gate 与 defensive overlay gate 分开解释。

## 任务映射

| New ID | Attachment item | Scope |
|---|---|---|
| TRADING-1527 | original 1526 | No-survivor diagnosis from candidate failure matrix |
| TRADING-1528 | original 1527 | Candidate reclassification schema |
| TRADING-1529 | original 1528 | Defensive overlay gate policy config |
| TRADING-1530 | original 1529 | Overlay metrics engine |
| TRADING-1531 | original 1530 | Risk-off attribution |
| TRADING-1532 | original 1531 | Re-risk timing review |
| TRADING-1533 | original 1532 | Downside and stress review |
| TRADING-1534 | original 1533 | Static frontier domination overlay interpretation |
| TRADING-1535 | original 1534 | TQQQ overlay safety review |
| TRADING-1536 | original 1535 | Overlay candidate set |
| TRADING-1537 | original 1536 | Defensive overlay survival matrix |
| TRADING-1538 | original 1537 | Net-of-cost overlay review |
| TRADING-1539 | original 1538 | Stress-specific overlay gate |
| TRADING-1540 | original 1539 | Walk-forward split evidence completion |
| TRADING-1541 | original 1540 | Guardrail tests |
| TRADING-1542 | original 1541 | Owner review pack |
| TRADING-1543 | original 1542 | Forward watch plan |
| TRADING-1544 | original 1543 | Registry, catalog and system-flow updates |
| TRADING-1545 | original 1544 | Validation commands |
| TRADING-1546 | original 1545 | Commit summary |

## Expected Artifacts

- `config/research/defensive_overlay_gate.yaml`
- `docs/research/expanded_universe_no_survivor_diagnosis.md`
- `inputs/research_reviews/expanded_universe_no_survivor_diagnosis.yaml`
- `inputs/research_reviews/expanded_candidate_reclassification_matrix.yaml`
- `outputs/research_strategies/defensive_overlay/overlay_metrics.csv`
- `docs/research/defensive_overlay_risk_off_attribution_review.md`
- `inputs/research_reviews/defensive_overlay_risk_off_attribution.yaml`
- `docs/research/defensive_overlay_re_risk_timing_review.md`
- `docs/research/defensive_overlay_downside_stress_review.md`
- `docs/research/static_frontier_domination_overlay_interpretation.md`
- `docs/research/tqqq_overlay_safety_review.md`
- `inputs/research_reviews/defensive_overlay_candidate_set.yaml`
- `inputs/research_reviews/defensive_overlay_survival_matrix.yaml`
- `docs/research/defensive_overlay_survival_review.md`
- `docs/research/defensive_overlay_net_of_cost_review.md`
- `inputs/research_reviews/defensive_overlay_stress_gate.yaml`
- `docs/research/expanded_universe_walk_forward_split_evidence_review.md`
- `inputs/research_reviews/expanded_universe_walk_forward_split_evidence.yaml`
- `docs/research/defensive_overlay_owner_review_pack.md`
- `docs/research/defensive_overlay_forward_watch_plan.md`

## Acceptance Criteria

- Full allocation failure remains explicit and unchanged.
- Defensive overlay gate is governed by `config/research/defensive_overlay_gate.yaml` with owner, status, rationale, intended effect, validation evidence and review condition.
- Overlay candidate rows cannot pass if they rely on target-path metrics, lack actual-path evidence, have unbounded TQQQ exposure, fail net-of-cost defensive benefit, or lack walk-forward split evidence.
- TQQQ-heavy rows remain diagnostic-only / research-only.
- `limited_adjustment` can be classified only as watch-only if actual-path downside benefit is present and all safety fields remain false/none.
- Report registry, artifact catalog and system flow document the new research-only path.
- Focused guardrail tests cover no promotion, no broker universe eligibility, target-path rejection, TQQQ safety, and full-allocation-to-overlay reclassification boundaries.

## Validation Plan

- `python -m ruff check src tests`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/test_defensive_overlay_gate.py`
- `python -m pytest -n 16 --dist loadfile tests/test_expanded_allocation_universe.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_artifact_governance.py`
- `python -m pytest -n 16 --dist loadfile tests/test_execution_semantics.py`
- `python -m pytest -n 16 --dist loadfile tests/test_external_validation.py`
- `python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py`
- `git diff --check`
- `git diff --cached --check`

## Progress Notes

- 2026-06-27: Registered task and requirement document. Implementation must preserve dynamic promotion `BLOCKED`, `paper_shadow_allowed=false`, `production_allowed=false`, and `broker_action=none`.
- 2026-06-27: Implementation completed and moved to `VALIDATING`. Added `config/research/defensive_overlay_gate.yaml`, `aits research strategies defensive-overlay full-pack`, overlay metrics, no-survivor diagnosis, reclassification, risk-off/re-risk/downside/static-frontier/TQQQ/net-cost/walk-forward reviews, candidate set, survival/stress matrix, owner pack, forward watch plan, report registry, artifact catalog, system flow and guardrail tests. Real full-pack result: full allocation survivor count=0, overlay gate pass count=0, primary watch pending=`limited_adjustment`, drawdown-control diagnostics=4, TQQQ diagnostic=1, safety fields remain promotion/paper-shadow/production/broker blocked.
