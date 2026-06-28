# TRADING-1906 to 1945 Boundary-Aware Two-Layer Optimization Framework

最后更新：2026-06-28

## 状态

- Task id: `TRADING-1906_to_1945_BOUNDARY_AWARE_TWO_LAYER_OPTIMIZATION_FRAMEWORK`
- Status: `VALIDATING`
- Owner: system implementation + project owner review
- Date opened: 2026-06-28
- Market regime: `ai_after_chatgpt`
- Primary research window: `exact_three_asset_validated`
- Upstream evidence: `TRADING-1806_to_1885_TWO_LANE_OPTIMIZATION_MASTER_CLOSEOUT`
- Safety boundary: research-only, diagnostic-first, dynamic promotion blocked, no paper-shadow, no production, no broker.

## 背景

TRADING-1806 to 1885 已证明 universal first-layer 和 gated integration 不应继续推进：defensive lane 没有 material improvement，return-seeking lane 有收益迹象但 drawdown 回归、TQQQ/beta dependency 和 2023+ dependence 明显。本批不是恢复可交易策略，而是建设 boundary-aware two-layer framework，把第一层信号拆成 defensive、return-seeking diagnostic、risk veto 三类 channel，并把第二层改为 base + overlay + veto 的可审计框架。

## 任务映射

| ID | Scope |
|---|---|
| TRADING-1906 | Boundary contract specification |
| TRADING-1907 | Signal usage matrix v2 |
| TRADING-1908 | Channel policy config |
| TRADING-1909 | Boundary guardrail tests |
| TRADING-1910 | Base + overlay + veto schema |
| TRADING-1911 | Policy compiler |
| TRADING-1912 | Error-type attribution engine |
| TRADING-1913 | Indicator family registry |
| TRADING-1914 | Indicator family ablation runner |
| TRADING-1915 | Do-not-de-risk optimization track |
| TRADING-1916 | Risk-on veto optimization track |
| TRADING-1917 | Return-seeking diagnostic forward log |
| TRADING-1918 | Channel-aware actual-path evaluator |
| TRADING-1919 | Boundary-aware owner review template |
| TRADING-1920 | Selection rule pre-registration enforcement |
| TRADING-1921 | Research audit metadata channel extension |
| TRADING-1922 | System flow / registry / catalog / task-register update |
| TRADING-1923 | Validation |
| TRADING-1924 to 1945 | Closeout and final matrix |

## Expected Artifacts

- `docs/research/two_layer_strategy_boundary_contract.md`
- `config/research/two_layer_strategy_boundary_contract.yaml`
- `inputs/research_reviews/first_layer_signal_usage_matrix_v2.yaml`
- `docs/research/first_layer_signal_usage_matrix_v2.md`
- `config/research/first_layer_channel_policy.yaml`
- `config/research/base_overlay_veto_policy_schema.yaml`
- `docs/research/base_overlay_veto_policy_design.md`
- `src/ai_trading_system/two_layer_policy_compiler.py`
- `src/ai_trading_system/two_layer_error_attribution.py`
- `src/ai_trading_system/channel_aware_actual_path_evaluator.py`
- `config/research/indicator_family_registry.yaml`
- `docs/research/indicator_family_registry_review.md`
- `aits research trends indicator-family-ablation`
- `docs/research/indicator_family_ablation_review.md`
- `inputs/research_reviews/indicator_family_ablation_matrix.yaml`
- `docs/research/do_not_de_risk_optimization_track.md`
- `config/research/do_not_de_risk_selection_rule.yaml`
- `docs/research/risk_on_veto_optimization_track.md`
- `config/research/risk_on_veto_policy.yaml`
- `config/research/return_seeking_diagnostic_forward_log.yaml`
- `docs/research/return_seeking_diagnostic_forward_log_spec.md`
- `docs/research/boundary_aware_two_layer_owner_review_template.md`
- `docs/research/boundary_aware_two_layer_optimization_framework_closeout.md`
- `inputs/research_reviews/boundary_aware_two_layer_optimization_framework_final_matrix.yaml`

## Acceptance Criteria

- 第一层和第二层边界合同存在，并声明 `first_layer_contract`、`second_layer_contract`、`evaluation_contract`、`probe_vs_candidate_boundary`、`diagnostic_vs_allocation_boundary`。
- 每个第一层 signal 都有 `allowed_usage`、`blocked_usage`、`required_veto`、`diagnostic_only` 和 `can_emit_weights=false`。
- Defensive channel 不能输出或驱动 `add_risk`、`risk_on`、`TQQQ increase` 或 growth overlay。
- Return-seeking channel 默认 diagnostic-only，不能输出 weights，不能驱动 defensive overlay、promotion、paper-shadow、production 或 broker。
- Risk veto 优先级高于 growth overlay，active veto 会阻断 growth overlay、TQQQ delta 和 add-risk。
- 第二层 compiler 只消费 first-layer channel 输出，不直接使用 raw indicators；输出 long-only、sum-to-one target weights 和 audit trace。
- Error attribution engine 支持 false risk-off、missed risk-off、false add-risk、late re-risk 和 beta-only improvement。
- Indicator family registry 和 ablation runner 可生成 family-level action-value audit，且结果仍为 diagnostic-only。
- Do-not-de-risk 与 risk-on veto tracks 都有预注册 selection rule；没有 selection rule 的结果不能成为 candidate。
- Dynamic promotion、paper-shadow、production 和 broker 全部继续 disabled。

## Validation Plan

- `python -m ruff check src tests`
- `python -m compileall -q src tests`
- `python -m pytest -n 16 --dist loadfile tests/test_two_layer_boundary_contract.py`
- `python -m pytest -n 16 --dist loadfile tests/test_two_layer_policy_compiler.py`
- `python -m pytest -n 16 --dist loadfile tests/test_two_layer_error_attribution.py`
- `python -m pytest -n 16 --dist loadfile tests/test_channel_aware_actual_path_evaluator.py`
- `python -m pytest -n 16 --dist loadfile tests/test_boundary_aware_selection_rules.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_audit_metadata.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_window_contracts.py`
- `python -m pytest -n 16 --dist loadfile tests/test_research_artifact_governance.py`
- `python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py`
- `git diff --check`
- `git diff --cached --check`

## Progress Notes

- 2026-06-28: Registered task and requirement document from owner attachment. Implementation must build the boundary-aware framework only; it must not recover gated integration, owner review candidate status, dynamic promotion, paper-shadow, production or broker.
- 2026-06-28: Implemented boundary contract, signal usage matrix v2, channel policy, base+overlay+veto schema, policy compiler, error attribution engine, indicator family registry/ablation runner, do-not-de-risk and risk-on veto tracks, diagnostic forward log, channel-aware actual-path evaluator, owner review template, final matrix/closeout, report registry, artifact catalog, system flow and guardrail tests. Generated `indicator_family_ablation_matrix.yaml` and summary through `aits research trends indicator-family-ablation`. Safety boundary remains research-only / diagnostic-only with candidate_count=0.
- 2026-06-28: Validation passed: `python -m ruff check src tests`; `python -m compileall -q src tests`; focused parallel pytest for new boundary/compiler/error/evaluator/selection/audit tests; governance parallel pytest for research window, artifact governance, task register, report index and documentation contract.
