# TRADING-2440 Growth Tilt Paper-Shadow Candidate Promotion Review

最后更新：2026-07-09

## 状态

- task register id：`TRADING-2440_GROWTH_TILT_PAPER_SHADOW_CANDIDATE_PROMOTION_REVIEW`
- status：`DONE`
- owner：系统实现 + 项目 owner 后续复核
- 创建日期：2026-07-09

## 目标

基于 TRADING-2431 至 TRADING-2439 的证据，判断是否有具体 Growth Tilt 策略候选
值得进入真正 paper-shadow candidate gate。当前 authoritative 2439 artifact 为
`GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_BLOCKED_BY_PIT_REPLAY_GATE`，并没有
nonzero forward aging candidate pack；因此 2440 必须 fail closed，不得把 upstream
gate blocker 误报为 `NO_CANDIDATE` 策略结论。

## 输入

- TRADING-2431 existing candidate evidence matrix
- TRADING-2432 candidate gauntlet harness
- TRADING-2434 defensive limited adjustment component validation
- TRADING-2437 regime slice attribution review
- TRADING-2438 top-3 candidate PIT replay
- TRADING-2439 forward aging candidate pack
- data quality gate status carried from 2439 or rerun by the command
- report registry
- artifact catalog
- system flow
- research docs

## 输出

- `outputs/research_strategies/growth_tilt_paper_shadow_candidate_promotion_review/promotion_review_result.json`
- `outputs/research_strategies/growth_tilt_paper_shadow_candidate_promotion_review/evidence_summary.json`
- `outputs/research_strategies/growth_tilt_paper_shadow_candidate_promotion_review/candidate_decision_matrix.json`
- `outputs/research_strategies/growth_tilt_paper_shadow_candidate_promotion_review/blocked_promotion_route.json`
- `outputs/research_strategies/growth_tilt_paper_shadow_candidate_promotion_review/no_effect_boundary.json`
- `docs/research/growth_tilt_paper_shadow_candidate_promotion_review.md`
- `docs/research/growth_tilt_paper_shadow_candidate_evidence_summary.md`
- `docs/research/growth_tilt_paper_shadow_candidate_decision_matrix.md`
- `docs/research/growth_tilt_paper_shadow_candidate_blocked_route.md`
- `docs/research/growth_tilt_paper_shadow_candidate_no_effect_boundary.md`
- blocked route doc `docs/research/dynamic_strategy_2440_blocked_route.md`

## CLI

```bash
aits research strategies growth-tilt-paper-shadow-candidate-promotion-review --as-of 2026-07-08
```

## 期望终态状态

如果没有合格候选：

```text
GROWTH_TILT_PAPER_SHADOW_CANDIDATE_PROMOTION_REVIEW_NO_CANDIDATE
```

如果有合格候选：

```text
GROWTH_TILT_PAPER_SHADOW_CANDIDATE_PROMOTION_REVIEW_CANDIDATE_FOUND
```

## 当前 Fail-Closed 状态

```text
GROWTH_TILT_PAPER_SHADOW_CANDIDATE_PROMOTION_REVIEW_BLOCKED_BY_FORWARD_AGING_GATE
```

## 安全边界

本任务不得启用 paper-shadow、schedule、production 或 broker，不得生成交易建议，
不得生成 broker order，不得修改组合权重。没有 READY 且 nonzero 的 forward aging
candidate pack 时，不得进入 candidate-specific paper-shadow gate，也不得输出
`NO_CANDIDATE` 作为策略结论。

## 验收标准

- CLI 可真实运行并输出 deterministic CANDIDATE_FOUND / NO_CANDIDATE / BLOCKED 状态。
- 明确 2431-2439 source status、forward aging gate status、paper-shadow candidate count、
  selected candidate rows、安全边界和 next route。
- 当前 2439 blocked 时必须输出 blocked status，`paper_shadow_candidate_count=0`，
  `paper_shadow_candidate_found=false`，next route 回到 2438A remediation。
- paper-shadow、schedule、production、broker、automatic execution 均保持 disabled。
- focused tests、Ruff、compileall、docs freshness、documentation contract、task
  register consistency、contract validation 和 diff check 通过。

## 验证计划

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_paper_shadow_candidate_promotion_review.py
aits validate-data --as-of 2026-07-08
aits research strategies growth-tilt-paper-shadow-candidate-promotion-review --as-of 2026-07-08
aits docs validate-freshness
aits docs report-contract --latest
aits reports task-register-consistency run
aits reports task-register-consistency validate --latest
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

## 进展记录

- 2026-07-09：实现完成并归档 `DONE`。真实 CLI 输出 `GROWTH_TILT_PAPER_SHADOW_CANDIDATE_PROMOTION_REVIEW_BLOCKED_BY_FORWARD_AGING_GATE`；`source_2438_ready=false`、`source_2439_forward_aging_ready=false`、`forward_aging_source_status=GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_BLOCKED_BY_PIT_REPLAY_GATE`、`pit_replay_source_status=GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP`、`data_quality_gate_passed=true`、`promotion_review_ready=false`、`paper_shadow_candidate_found=false`、`paper_shadow_candidate_count=0`、`selected_candidates=[]`、`next_route=TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation`。本任务没有把 upstream forward-aging/PIT-replay blocker 误标为 no-candidate 策略结论，也没有启用 paper-shadow / schedule / production / broker。
- 2026-07-09：根据 owner 2426-2440 roadmap 新增，进入 `IN_PROGRESS`；当前 2439 artifact blocked by PIT replay gate，2440 必须 fail-closed，不得生成 paper-shadow candidate 或输出 no-candidate 策略结论。
