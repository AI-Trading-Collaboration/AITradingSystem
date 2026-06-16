# TRADING-372 Candidate Regression Replay

最后更新：2026-06-16

状态：DONE

## 背景

Recent paper-shadow governance changes added fallback, cache catalog, signal completeness,
canonical health, cost-sensitivity and benchmark baseline controls around the dynamic v3
rescue candidate. TRADING-372 adds a fixed replay guard so future governance changes cannot
silently alter research outputs, decisions, safety metadata, artifact schema, or Reader Brief
fields.

## 目标

- 定义一个小型固定 replay window，默认覆盖 ChatGPT 后 AI regime 的 recent paper-shadow window。
- 读取 current candidate behavior artifact 与 stored expected behavior。
- 对比 candidate outputs、decisions、safety metadata、artifact schema 和 Reader Brief fields。
- 输出 acceptable-change / breaking-change classification。
- 新增 `candidate-regression-replay run/report` 和 `validate-candidate-regression-replay` CLI。
- Reader Brief 展示 replay status、breaking change count、acceptable change count 和 next action。
- 同步 report registry、artifact catalog、README、operations runbook、system flow、requirements、
  task register 和 focused tests。

## 非目标

- 不优化 strategy behavior、score、gate 或 candidate selection。
- 不运行 backtest、stress replay、market data refresh、source refresh 或 upstream paper-shadow runner。
- 不补造 current artifact、expected behavior、metrics、decision artifact 或 Reader Brief artifact。
- 不写 official target weights、candidate ledger decision、paper account、portfolio、broker/order 或 production state。
- 不把 PASS 解释为 promotion approval。

## Policy Contract

Replay policy 由 `config/etf_portfolio/dynamic_v3_rescue/candidate_regression_replay_v1.yaml`
披露：

- policy id / version / status / owner；
- rationale / intended effect / validation evidence / review condition；
- replay window start/end、AI regime anchor、candidate id；
- expected behavior id、decision fields、output fields、safety fields、schema fields 和 Reader Brief fields；
- acceptable change reasons；
- safety boundaries。

## Artifact Contract

目录：`reports/etf_portfolio/dynamic_v3_rescue/candidate_regression_replay/<replay_id>/`

- `candidate_regression_replay_manifest.json`
- `candidate_regression_replay_report.json`
- `candidate_regression_replay_report.md`
- `reader_brief_section.md`
- `candidate_regression_replay_validation.json`
- `candidate_regression_replay_validation.md`

所有输出固定：

- `research_only=true`
- `manual_review_only=true`
- `regression_guard_only=true`
- `strategy_behavior_changed=false`
- `data_downloaded_by_replay=false`
- `pipelines_executed_by_replay=false`
- `execution_model_ready=false`
- `official_target_weights=false`
- `not_official_target_weights=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `paper_account_state_mutated=false`
- `production_state_mutated=false`
- `automatic_candidate_promotion=false`
- `auto_apply=false`
- `production_effect=none`

## 验收标准

- CLI run/report/validate 可运行，真实当前链路能生成 replay artifact 并 validate PASS。
- 固定 replay window、source current artifact、stored expected behavior、policy version 均在 JSON/Markdown 披露。
- Candidate outputs、decisions、safety metadata、artifact schema 和 Reader Brief fields 均有 comparison rows。
- Breaking changes 必须 fail closed 为 `BREAKING_CHANGE_DETECTED`，missing current/expected source 必须 fail closed。
- Acceptable changes 必须显式给出 reason，不能静默忽略。
- Reader Brief 只读 latest artifact；缺失时显示 `MISSING`，不能补造 replay。
- Report registry、artifact catalog、README、operations runbook、system flow、requirements 和 task register 同步。
- Focused tests、CLI smoke、Ruff、compileall、documentation contract、report index、Reader Brief 和 git diff check 通过。

## 进展记录

- 2026-06-16：任务创建并进入实现；范围限定为 research-only regression guard，不运行 backtest、不刷新数据、不补造 artifact、不接 broker、不修改 official target weights / paper account / production state。
- 2026-06-16：实现完成并转为 `DONE`。真实 artifact `candidate-regression-replay_4ff92c6de0b76488` 输出 `candidate_regression_replay_status=REGRESSION_REPLAY_PASS`、`candidate=median_plus_regime_mismatch_filter`、policy `dynamic_v3_rescue_candidate_regression_replay_v1 / 2026-06-16`、expected behavior `dynamic_v3_rescue_candidate_regression_expected_benchmark_baseline_v1`、comparison_count=38、breaking_change_count=0、acceptable_change_count=0、unchanged_count=38、next action `continue_research_governance_observation`，validation `PASS` / failed=0。当前 replay 只读 latest benchmark baseline control artifact；未补造 current behavior、expected behavior、metrics 或 Reader Brief artifact，未运行 strategy optimization、backtest、data refresh 或 upstream paper-shadow command。Focused tests 覆盖 expected behavior pass、breaking-change fail-closed、CLI run/report/validate 和 Reader Brief latest summary；documentation contract、report index 和 Reader Brief latest 在本变更中验证。
