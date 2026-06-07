# TRADING-121 to TRADING-125 Evidence Gate Diagnosis and Candidate Usability Recovery

最后更新：2026-06-08

## 状态

`VALIDATING`

## 背景

TRADING-114 to 120 已跑通真实 `medium_real` sweep
`sweep_20260607T102300Z_ae5ae1d8`：300/300 candidates completed，0 failures，
但全部为 `review_required`，evidence summary 业务状态为 `FAIL`，
`usable_for_research_count=0`，observe pool 为 0。

这说明工程执行链路可用，但候选可用性被 evidence gates 阻断。下一阶段不能直接把
`overnight_real` 作为研究默认结论；必须先明确哪些 gate 是真实 hard fail，哪些是
manual review warning，哪些是可通过 policy calibration 恢复为 observe-only 候选。

本阶段仍固定：

- `production_effect=none`
- `broker_action=none`
- `manual_review_required=true`
- `production_candidate_generated=false`
- 不自动 shadow enrollment
- 不自动 owner approval

## 子任务拆解

|ID|目标|状态|验收|
|---|---|---|---|
|TRADING-121|Medium Real Evidence Blocking Diagnosis|VALIDATING|读取 latest medium_real sweep/evidence summary，生成 blocking reason summary、candidate blocking matrix、gate category summary 和诊断报告；区分 hard block、soft block 和 warning。|
|TRADING-122|Gate Impact Matrix and Candidate Recovery Simulation|VALIDATING|在不修改原始 candidate result 的前提下模拟 scenario A-H，输出每个 gate 修复或降级后的 recovered/observe/manual review count。|
|TRADING-123|Evidence Gate Calibration|VALIDATING|新增可审计 gate policy manifest，明确 observe_only、promote_candidate 和 production_candidate 边界；policy apply 只生成 calibrated status artifact。|
|TRADING-124|Recovered Candidate Rerank and Observe Pool Rebuild|VALIDATING|基于 calibrated policy 恢复 observe_only/manual_review_required candidates，输出 recovered/rejected/leaderboard，并重建 observe pool。|
|TRADING-125|Research Decision Update and Overnight Go/No-Go|VALIDATING|汇总 diagnosis、impact、policy、recovery、observe pool 和 readiness，输出 go/no-go、recommended_action 和 Reader Brief section。|

## 关键输入

- sweep_id: `sweep_20260607T102300Z_ae5ae1d8`
- evidence_summary_id: `3d98dd79c7ab6b40`
- observe_pool_id: `1201681d0e290627`
- overnight_readiness_id: `c0755e2c263b0854`
- research_decision_id: `81ac692903a4c668`

## CLI 合同

新增：

- `aits etf dynamic-v3-rescue evidence-diagnosis run --sweep-id <sweep_id>`
- `aits etf dynamic-v3-rescue evidence-diagnosis report --latest`
- `aits etf dynamic-v3-rescue validate-evidence-diagnosis --diagnosis-id <diagnosis_id>`
- `aits etf dynamic-v3-rescue gate-impact run --diagnosis-id <diagnosis_id>`
- `aits etf dynamic-v3-rescue gate-impact report --latest`
- `aits etf dynamic-v3-rescue validate-gate-impact --impact-id <impact_id>`
- `aits etf dynamic-v3-rescue gate-policy validate`
- `aits etf dynamic-v3-rescue gate-policy report`
- `aits etf dynamic-v3-rescue gate-policy apply --sweep-id <sweep_id> --policy <policy_path>`
- `aits etf dynamic-v3-rescue candidate-recovery run --sweep-id <sweep_id> --policy-run-id <policy_run_id>`
- `aits etf dynamic-v3-rescue candidate-recovery report --latest`
- `aits etf dynamic-v3-rescue validate-candidate-recovery --recovery-id <recovery_id>`
- `aits etf dynamic-v3-rescue observe-pool rebuild --recovery-id <recovery_id>`
- `aits etf dynamic-v3-rescue research-decision update --sweep-id <sweep_id> --diagnosis-id <diagnosis_id> --impact-id <impact_id> --recovery-id <recovery_id>`
- `aits etf dynamic-v3-rescue validate-research-decision-update --decision-update-id <decision_update_id>`

## Artifact Contract

新增 artifact families：

- `reports/etf_portfolio/dynamic_v3_rescue/evidence_diagnosis/<diagnosis_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/gate_impact/<impact_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/gate_policy/<policy_run_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/candidate_recovery/<recovery_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/research_decision_update/<decision_update_id>/`

关键文件：

- `diagnosis_manifest.json`
- `blocking_reason_summary.json`
- `candidate_blocking_matrix.jsonl`
- `gate_category_summary.json`
- `evidence_diagnosis_report.md`
- `gate_impact_manifest.json`
- `gate_impact_matrix.json`
- `candidate_recovery_simulation.json`
- `gate_impact_report.md`
- `gate_policy_manifest.json`
- `applied_policy.yaml`
- `policy_effect_summary.json`
- `gate_policy_report.md`
- `recovery_manifest.json`
- `recovered_candidates.jsonl`
- `rejected_after_calibration.jsonl`
- `recovery_leaderboard.json`
- `recovery_report.md`
- `decision_update_manifest.json`
- `go_no_go_matrix.json`
- `next_action_recommendations.json`
- `research_decision_update_report.md`
- `reader_brief_section.md`

## Gate Calibration Boundary

不得放宽：

- `data_quality = FAIL`
- `date_range_status = FAIL`
- `date_range_status = INSUFFICIENT_DATA`
- missing `real_evaluation_artifact_path`
- missing `daily_weights`
- `overfit_status = HIGH_RISK`
- `tech_semiconductor_relevance = LOW`
- automatic `production_candidate`

可作为 manual review warning 处理：

- `data_quality = PASS_WITH_WARNINGS`
- `data_provenance_status = RECONSTRUCTED_MANIFEST`
- `candidate_attribution_status = PARTIAL`
- `overfit_status = REVIEW_REQUIRED`
- `regime_coverage = PASS_WITH_WARNINGS`

## Scenario Simulation

Gate impact 必须至少覆盖：

- Scenario A: current rules
- Scenario B: `ATTRIBUTION_PARTIAL` as manual review
- Scenario C: `DATA_PROVENANCE_RECONSTRUCTED` as warning
- Scenario D: `OVERFIT_REVIEW_REQUIRED` as manual review
- Scenario E: `REGIME_COVERAGE_PASS_WITH_WARNINGS` allows observe-only
- Scenario F: only true hard failures remain hard fail
- Scenario G: fix top 1 blocking reason
- Scenario H: fix top 3 blocking reasons

## 输出问题

最终报告必须明确回答：

1. 为什么没有可用候选？
2. 哪些 gate 阻断了候选？
3. 哪些 gate 可以作为 `manual_review_required`？
4. 修复或校准后能恢复多少候选？
5. 哪些候选值得进入 `observe_only`？
6. 是否建议跑 `overnight_real`？

## 验收标准

- diagnosis、gate impact、gate policy、candidate recovery、observe pool rebuild 和 research decision update CLI 可运行。
- 真实 sweep artifacts 不被原地修改。
- `production_candidate_generated=false`。
- 所有 recovered observe candidates 均 `manual_review_required=true`。
- focused tests PASS。
- `python -m ruff check src tests` PASS。
- `python -m compileall -q src tests` PASS。
- `git diff --check` PASS。
- `aits etf dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue` PASS。

## 运行记录

- 2026-06-08：新增并进入 `IN_PROGRESS`，原因：真实 medium_real 已跑通但 evidence gate 使
  300/300 candidates 全部 `review_required`，observe pool 仍为 0；本阶段目标是诊断阻断原因、
  模拟 gate calibration impact、恢复可人工复核的 observe-only 候选，并更新 overnight
  go/no-go 结论。
- 2026-06-08：真实 sweep `sweep_20260607T102300Z_ae5ae1d8` 已完成 rescue workflow：
  `diagnosis_id=eb8b1c33e26c990d`，`impact_id=16ebb1bf0834db22`，
  `policy_run_id=0a12556b2d7f9ee2`，`recovery_id=0124f4f6abdd503b`，
  rebuilt `observe_pool_id=01f0f0056e78f293`，
  `decision_update_id=e0788af9f403b9c4`。诊断结果为 300 candidates、0 hard blocked、
  300 soft blocked、0 usable；gate impact 最佳场景为 `true_hard_failures_only`，可恢复
  300 observe-only candidates；policy apply 与 recovery 均输出 300
  `observe_only/manual_review_required` candidates；research decision update 输出
  `go_no_go=GO_WITH_LIMITS`、`recommended_action=manual_review_recovered_candidates`。
  所有输出继续固定 `production_candidate_generated=false`，不写 shadow registry、不 approval、
  不自动启动 `overnight_real`。
- 2026-06-08：实现完成并进入 `VALIDATING`。验证通过：
  focused pytest（12 passed）、`python -m ruff check src tests`、`python -m compileall -q src tests`、
  `git diff --check`、`aits etf dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue`
  和全量 `python -m pytest tests -q`（2226 passed，330 warnings；warnings 为既有
  numpy/runtime/deprecation warnings）。剩余复核点：owner 需要人工确认
  `GO_WITH_LIMITS` 是否足以启动受限 `overnight_real`，以及 300 个 recovered candidates
  是否先进入人工筛选或 shadow registration review。
