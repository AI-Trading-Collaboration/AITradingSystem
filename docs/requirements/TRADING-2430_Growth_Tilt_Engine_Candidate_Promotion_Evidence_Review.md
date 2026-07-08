# TRADING-2430 Growth Tilt Engine Candidate Promotion Evidence Review

最后更新：2026-07-09

## 状态

- task register id：`TRADING-2430_GROWTH_TILT_ENGINE_CANDIDATE_PROMOTION_EVIDENCE_REVIEW`
- status：`DONE`
- owner：系统实现 + 项目 owner 后续复核
- 创建日期：2026-07-09

## 目标

在 TRADING-2426 至 TRADING-2429 完成 paper-shadow dry-run、schedule dry-run、
manual review packet、observe-only signal boundary 和 forward outcome binding
boundary 后，复核当前 Growth Tilt Engine 是否已有策略候选值得进入真正
paper-shadow candidate gate。

本任务专门区分工程 readiness 与策略 alpha evidence。工程链路可安全观察不等于
候选策略可以晋级 paper-shadow。

## 输入

- TRADING-2426 paper-shadow schedule dry-run result
- TRADING-2427 manual review packet dry-run result
- TRADING-2428 observe-only signal artifact boundary result
- TRADING-2429 forward outcome binding boundary result
- `config/research/equal_risk_growth_tilt_candidate_registry.yaml`
- prior candidate evidence / owner decision artifacts
- report registry
- artifact catalog
- system flow
- research docs

## 输出

- `outputs/research_strategies/growth_tilt_engine_candidate_promotion_evidence_review/promotion_evidence_review_result.json`
- `outputs/research_strategies/growth_tilt_engine_candidate_promotion_evidence_review/candidate_evidence_matrix.json`
- `outputs/research_strategies/growth_tilt_engine_candidate_promotion_evidence_review/candidate_decision_summary.json`
- `outputs/research_strategies/growth_tilt_engine_candidate_promotion_evidence_review/no_promotion_rationale.json`
- `outputs/research_strategies/growth_tilt_engine_candidate_promotion_evidence_review/no_effect_boundary.json`
- `docs/research/growth_tilt_engine_candidate_promotion_evidence_review.md`
- `docs/research/growth_tilt_engine_candidate_evidence_matrix.md`
- `docs/research/growth_tilt_engine_candidate_decision_summary.md`
- `docs/research/growth_tilt_engine_no_promotion_rationale.md`
- `docs/research/growth_tilt_engine_candidate_promotion_no_effect_boundary.md`
- `docs/research/dynamic_strategy_2431_route.md`

## CLI

```bash
aits research strategies growth-tilt-engine-candidate-promotion-evidence-review --as-of 2026-07-08
```

## 期望状态

如果没有候选值得进入 paper-shadow：

```text
GROWTH_TILT_ENGINE_CANDIDATE_PROMOTION_EVIDENCE_REVIEW_NO_PROMOTION_CANDIDATE
```

如果有候选值得进一步进入 candidate-specific paper-shadow gate：

```text
GROWTH_TILT_ENGINE_CANDIDATE_PROMOTION_EVIDENCE_REVIEW_PROMOTION_CANDIDATE_FOUND
```

当前默认健康结果：

```yaml
promotion_candidate_found: false
promotion_candidate_count: 0
next_route: TRADING-2431_Growth_Tilt_Existing_Candidate_Evidence_Matrix
```

原因：2420-2430 主要证明系统可以安全观察，不证明策略 alpha 已经足够强。

## 安全边界

本任务不得：

- 直接启用 paper-shadow
- 启用 schedule 或 scheduler
- 生成新 signal
- 回填真实 outcome
- 生成 trading advice
- 生成 actionable allocation change
- 生成 broker order
- 修改实际组合权重
- 运行 backtest、scoring 或 daily report
- 读取 fresh cached market data
- 运行 production
- 触发 broker/order

## Data Quality Gate

默认不运行 `aits validate-data`。原因：TRADING-2430 只读取 prior artifacts/docs、
existing candidate registry、prior candidate evidence、report registry、artifact
catalog 和 system flow，不读取 fresh cached market data，不运行 backtest/scoring/
daily report，不生成 feature/signal，不回填 outcome，也不生成交易建议。

如果实现阶段引入 fresh cached market/features/signals/outcome/event data 读取，本任务
必须重新引入 `aits validate-data` 或同等代码路径。

## 验收标准

- CLI 可真实运行并输出 deterministic no-promotion / candidate-found / blocked 状态。
- promotion evidence review、candidate evidence matrix、candidate decision summary、
  no-promotion rationale、no-effect boundary 和 2431 route 均生成。
- TRADING-2426 至 TRADING-2429 READY 状态被读取并继承。
- candidate registry safety boundary 和 prior owner/candidate evidence 被读取并披露。
- 不把 PIT/contract/paper-shadow wiring readiness 自动解释为 alpha evidence。
- 默认真实 run 在无明确 paper-shadow promotion evidence 时输出 no-promotion candidate。
- paper-shadow、schedule、production、broker、automatic execution 均保持 disabled。
- report registry、artifact catalog、system flow、task register 与 completed archive
  一致。
- focused tests、Ruff、compileall、docs freshness、documentation contract、task
  register consistency、contract validation 和 diff check 通过。

## 验证计划

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_engine_candidate_promotion_evidence_review.py
aits research strategies growth-tilt-engine-candidate-promotion-evidence-review --as-of 2026-07-08
aits docs validate-freshness
aits docs report-contract --latest
aits reports task-register-consistency run
aits reports task-register-consistency validate --latest
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

## 进展记录

- 2026-07-09：根据 owner 2426-2440 roadmap 新增，进入 `IN_PROGRESS`。
- 2026-07-09：实现完成并归档 `DONE`。真实 CLI run 输出
  `GROWTH_TILT_ENGINE_CANDIDATE_PROMOTION_EVIDENCE_REVIEW_NO_PROMOTION_CANDIDATE`，
  promotion_evidence_review_ready=true，promotion_candidate_found=false，
  promotion_candidate_count=0，candidate_count=6，candidate_evidence_matrix_ready=true，
  candidate_decision_summary_ready=true，no_promotion_rationale_ready=true，
  engineering_readiness_is_alpha_evidence=false，paper_shadow_promotion_allowed_by_registry=false，
  prior_owner_approved_paper_shadow=false，prior_owner_approved_observation=false，
  generated_signal=false，outcome_backfilled=false，paper_shadow_enabled=false，
  production_enabled=false，broker_enabled=false，next route 指向
  `TRADING-2431_Growth_Tilt_Existing_Candidate_Evidence_Matrix`。
  本任务未运行 `aits validate-data`，因为只读取 prior artifacts/docs、candidate
  registry、prior candidate evidence、registry、catalog 和 system flow，不读取 fresh
  cached market/outcome data、不运行 backtest/scoring/daily report、不生成 feature/
  signal 或交易建议。
