# TRADING-864 Tail-Risk Follow-up Owner Review Pack

- 状态：`OWNER_REVIEW_PACK_READY`
- 生产影响：`none`；broker_action：`none`
- 工程提交状态：`NO_COMMIT_MIXED_WORKTREE_HANDOFF`；commit_created=`False`
- 真实 CLI 状态：`REAL_RUN_PASS`
- hard-block E2E：`HARD_BLOCK_E2E_PASS`；Reader Brief render：`DAILY_BRIEF_SAFE`
- readiness_score：`58`；baseline gate：`BASELINE_DOMINATED_BLOCKED`

## Owner 必答问题

1. 843～858 是否全部实现？

   是。真实 CLI 已全部运行；TRADING-849 用 `git diff --check` 覆盖，TRADING-855 由 847/856 baseline review/gate 覆盖。当前仍处于 owner review 前的验证完成状态，不等于策略可用。

2. 哪些文件尚未提交？

   - `M config/report_registry.yaml`
   - ` M config/research/controlled_strategy_next_stage_research.yaml`
   - ` M docs/artifact_catalog.md`
   - ` M docs/system_flow.md`
   - ` M docs/task_register.md`
   - ` M src/ai_trading_system/cli_commands/research.py`
   - ` M src/ai_trading_system/controlled_strategy_batch.py`
   - ` M src/ai_trading_system/reports/reader_brief.py`
   - ` M tests/test_controlled_strategy_batch.py`
   - ` M tests/test_tail_risk_independent_validation_governance.py`
   - `?? docs/requirements/TRADING-843_to_858_Tail_Risk_Fallback_Governance_Followup.md`
   - `?? docs/requirements/TRADING-859_to_864_Tail_Risk_Governance_Closeout.md`
   - `?? docs/research/tail_risk_fallback_governance_task_coverage_map.md`
   - `?? docs/research/tail_risk_fallback_next_decision.md`
   - `?? docs/research/tail_risk_followup_owner_review_pack.md`

3. 是否存在 pre-existing mixed worktree risk？

   是。TRADING-859 状态为 `BLOCKED_BY_MIXED_WORKTREE`；以下 mixed/high-risk 文件不得自动提交：

   - `config/report_registry.yaml`：File was already dirty before TRADING-843-858 work began and now also contains tail-risk follow-up/closeout edits; exact hunks require owner/manual review before commit.
   - `docs/artifact_catalog.md`：File was already dirty before TRADING-843-858 work began and now also contains tail-risk follow-up/closeout edits; exact hunks require owner/manual review before commit.
   - `docs/system_flow.md`：File was already dirty before TRADING-843-858 work began and now also contains tail-risk follow-up/closeout edits; exact hunks require owner/manual review before commit.
   - `src/ai_trading_system/cli_commands/research.py`：File was already dirty before TRADING-843-858 work began and now also contains tail-risk follow-up/closeout edits; exact hunks require owner/manual review before commit.
   - `src/ai_trading_system/controlled_strategy_batch.py`：File was already dirty before TRADING-843-858 work began and now also contains tail-risk follow-up/closeout edits; exact hunks require owner/manual review before commit.
   - `tests/test_controlled_strategy_batch.py`：File was already dirty before TRADING-843-858 work began and now also contains tail-risk follow-up/closeout edits; exact hunks require owner/manual review before commit.
   - `tests/test_tail_risk_independent_validation_governance.py`：File was already dirty before TRADING-843-858 work began and now also contains tail-risk follow-up/closeout edits; exact hunks require owner/manual review before commit.

4. 哪些 CLI 已真实运行？

   - `TRADING-843` `tail-risk-governance-artifact-snapshot` returncode=0 status=`TAIL_RISK_GOVERNANCE_ARTIFACT_SNAPSHOT_BLOCKED`
   - `TRADING-844` `tail-risk-status-matrix` returncode=0 status=`TAIL_RISK_RESEARCH_BLOCKED`
   - `TRADING-845` `tail-risk-real-data-validation-audit` returncode=0 status=`REAL_DATA_READY`
   - `TRADING-846` `tail-risk-independent-forward-outcome-result-review` returncode=0 status=`FORWARD_OUTCOME_USABLE_FOR_RESEARCH`
   - `TRADING-847` `tail-risk-counterfactual-baseline-result-review` returncode=0 status=`COUNTERFACTUAL_BASELINE_REVIEWABLE`
   - `TRADING-848` `tail-risk-artifact-determinism-check` returncode=0 status=`DETERMINISTIC_PASS`
   - `TRADING-849` `git-diff-check-newline-whitespace` returncode=0 status=`PASS`
   - `TRADING-850` `tail-risk-task-coverage-map` returncode=0 status=`TAIL_RISK_TASK_COVERAGE_MAP_COMPLETE`
   - `TRADING-851` `tail-risk-hard-block-mutation-tests` returncode=0 status=`HARD_BLOCK_MUTATION_PASS`
   - `TRADING-852` `tail-risk-report-registry-integrity-review` returncode=0 status=`REPORT_REGISTRY_INTEGRITY_PASS`
   - `TRADING-853` `tail-risk-daily-reading-safety-summary` returncode=0 status=`TAIL_RISK_DAILY_READING_SUMMARY_BLOCKED`
   - `TRADING-854` `tail-risk-independent-trigger-v2-input-quality-review` returncode=0 status=`TRIGGER_V2_INPUT_QUALITY_PARTIAL`
   - `TRADING-855` `baseline-expansion-covered-by-847-and-856` returncode=0 status=`COUNTERFACTUAL_BASELINE_REVIEWABLE`
   - `TRADING-856` `tail-risk-baseline-dominance-gate` returncode=0 status=`BASELINE_DOMINATED_BLOCKED`
   - `TRADING-857` `tail-risk-research-readiness-score` returncode=0 status=`TAIL_RISK_READINESS_RESEARCH_ONLY`
   - `TRADING-858` `tail-risk-next-decision-document` returncode=0 status=`TAIL_RISK_NEXT_DECISION_BLOCKED`

5. 当前 tail-risk fallback 是否仍 blocked？

   是。status matrix=`TAIL_RISK_RESEARCH_BLOCKED`，next decision=`TAIL_RISK_NEXT_DECISION_BLOCKED`；promotion/paper-shadow/production 均为 false，broker_action=`none`。

6. 当前 readiness score 是多少？

   `58`，band=`research-only`。

7. 是否存在 baseline dominated？

   是。baseline gate=`BASELINE_DOMINATED_BLOCKED`，dominant_baseline_count=`2`。

8. 是否值得继续 trigger v2？

   当前不值得自动继续。next decision 的 worth_building_trigger_v2=`False`；trigger v2 input quality=`TRIGGER_V2_INPUT_QUALITY_PARTIAL`。

9. owner 需要批准什么？

   - 批准是否接受 BLOCKED_BY_MIXED_WORKTREE 的提交边界；不批准则保持不提交。
   - 人工 review mixed/pre-existing 文件 hunks，决定哪些属于 TRADING-843～864，可否拆分提交。
   - 确认是否接受 baseline dominated blocker，并保持 tail-risk fallback 不进入 promotion/paper-shadow/production。
   - 决定 trigger v2 是暂停、重建输入数据，还是废弃当前 fallback 方向。
   - 如要提交，批准 staged candidate list 或指定更细 hunk-level patch 边界。

## Validation

- `pytest`: python -m pytest -n 16 --dist loadfile tests/test_tail_risk_independent_validation_governance.py tests/test_task_register_consistency.py tests/trading_engine/test_reader_brief.py -> 15 passed
- `ruff`: python -m ruff check src/ai_trading_system/reports/reader_brief.py tests/test_tail_risk_independent_validation_governance.py -> passed
- `compileall`: python -m compileall src/ai_trading_system/reports/reader_brief.py src/ai_trading_system/controlled_strategy_batch.py src/ai_trading_system/cli_commands/research.py -> passed
- `diff_check`: git diff --check -> passed

## Artifact Index

- `TRADING-859`: `outputs\research_strategies\value_surface_review\tail_risk_followup_change_attribution.json`
- `TRADING-860`: `outputs\research_strategies\value_surface_review\tail_risk_followup_safe_commit_handoff.json`
- `TRADING-861`: `outputs\research_strategies\value_surface_review\tail_risk_followup_real_run_summary.json`
- `TRADING-862`: `outputs\research_strategies\value_surface_review\tail_risk_promotion_hard_block_e2e_proof.json`
- `TRADING-863`: `outputs\research_strategies\value_surface_review\tail_risk_daily_brief_safety_render_review.json`
- `TRADING-858`: `outputs\research_strategies\value_surface_review\tail_risk_next_decision.json`
