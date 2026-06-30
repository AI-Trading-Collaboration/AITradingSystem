# TRADING-2291 Scope-Narrowed Candidate Regeneration

最后更新：2026-06-30

## 状态

- task_id: `TRADING-2291_SCOPE_NARROWED_CANDIDATE_REGENERATION`
- priority: `P0`
- status: `VALIDATING`
- owner: 系统实现 + 项目 owner 后续复核
- last_update: 2026-06-30

## 背景

TRADING-2290 已完成 refined candidates 的 local edge / scope narrowing review。
真实结论为：

- `baseline_plus_trend_structure_refined_confidence_v1` 建议
  `SCOPE_NARROW_AND_REGENERATE`，usage=`confirmation_only`。
- `volatility_regime_refined_confidence_v1` 建议
  `SCOPE_NARROW_AND_REGENERATE`，usage=`risk_cap_only`。
- `risk_appetite_refined_confidence_v1` 建议 current-form reject / archive。

TRADING-2291 承接该结论，只生成 scope-narrowed candidate-bound artifacts，并正式
归档 `risk_appetite_refined_confidence_v1` 当前形态。Scope-narrowed artifacts 的
actual-path validation 由 TRADING-2292 执行。

## 非目标

- 不执行 actual-path validation。
- 不计算 scope-narrowed outcome score。
- 不生成 owner review package。
- 不做 promotion、paper-shadow、production 或 broker action。
- 不做组合层仓位回测。
- 不新增外部数据源。
- 不重新设计 candidate family。
- 不继续 refinement `risk_appetite`。
- 不改变 TRADING-2281 permanently inconclusive、TRADING-2285 original inconclusive 或
  TRADING-2289 refined validation 结论。
- 不声明 scope-narrowed candidate 已经有效。

## 实施步骤

1. 新增 `aits research trends scope-narrowed-candidate-generators-regenerate` CLI，只允许
   `mode=scope_narrowed_regeneration`。
2. 新增 loader，读取 TRADING-2290 local edge / scope narrowing outputs、TRADING-2288
   refined candidate artifacts 和 TRADING-2289 refined actual-path validation outputs。
3. 对 required inputs 做 fail-closed safety validation：promotion / paper-shadow /
   production / broker 必须保持 false/none，recommendation 不得出现 promotion-ready
   系列状态。
4. 生成 `baseline_plus_trend_structure_scope_narrowed_confirmation_v1`，保留 full trace
   rows，并区分 active / inactive prediction records；usage 固定为
   `confirmation_only`。
5. 生成 `volatility_regime_scope_narrowed_risk_cap_v1`，保留 full trace rows，并区分
   active / inactive prediction records；usage 固定为 `risk_cap_only`。
6. 为 `risk_appetite_refined_confidence_v1` 生成 current-form archive record，明确该
   archive 不是永久否定 risk appetite concept。
7. 生成 scope filter report、lineage report、refined-vs-scope delta 和 top-level
   original/refined/scope delta summary。
8. 写出 research docs，并更新 report registry、artifact catalog、system flow 和 task
   register。

## Safety Boundary

所有 TRADING-2291 outputs 必须固定：

- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `owner_review_required=false`
- `actual_path_validation_executed=false`
- `actual_path_validation_ready=false`
- `paper_shadow_recommendation_allowed=false`
- `production_recommendation_allowed=false`
- `broker_action_recommendation_allowed=false`
- `promotion_eligible=false`
- `trading_2281_permanently_inconclusive_decisions_changed=false`
- `trading_2285_original_inconclusive_decisions_changed=false`
- `trading_2289_refined_state_decisions_changed=false`

## Scope Rules

`baseline_plus_trend_structure_scope_narrowed_confirmation_v1` 只能作为
`confirmation_only` / trend confirmation artifact，不得输出 primary signal、target
weight、risk exposure 或 broker action。

`volatility_regime_scope_narrowed_risk_cap_v1` 只能作为 `risk_cap_only` / veto /
exposure limiter artifact，不得输出 buy/sell signal、target weight、production action 或
broker action。

每条 source row 必须保留 source lineage，并新增 `scope_active`、`scope_reason`、
`usage_role`、`scope_filter_version`、`source_refined_record_id` 等 scope 字段。不得在
未记录 inactive reason 的情况下静默丢弃 source rows。

## 输出产物

Runtime artifacts 写入
`outputs/research_trends/scope_narrowed_candidate_generators_regenerated/`：

- `scope_narrowed_regeneration_run_summary.json`
- `scope_narrowed_regeneration_validation_summary.json`
- `scope_narrowed_candidate_registry.json`
- `scope_narrowed_original_vs_refined_vs_scope_delta_summary.json`
- `baseline_plus_trend_structure_scope_narrowed_confirmation_v1/*`
- `volatility_regime_scope_narrowed_risk_cap_v1/*`
- `risk_appetite_archive/risk_appetite_current_form_archive_record.json`
- `risk_appetite_archive/risk_appetite_current_form_archive_record.md`

Research docs：

- `docs/research/scope_narrowed_candidate_regeneration_report.md`
- `docs/research/scope_narrowed_candidate_scope_filter_report.md`
- `docs/research/scope_narrowed_original_vs_refined_delta_summary.md`
- `docs/research/risk_appetite_current_form_archive_record.md`

## 验收标准

- CLI 能读取 TRADING-2290 / 2288 / 2289 inputs，并写出全部 required runtime
  artifacts。
- 缺少 scope recommendation matrix、risk appetite reject record 或 required refined
  artifacts 时 fail closed。
- Input artifact 打开 promotion、paper-shadow、production 或 broker action 时 fail
  closed。
- Included candidates 必须来自 2290 `SCOPE_NARROW_AND_REGENERATE` recommendation。
- `risk_appetite_refined_confidence_v1` 必须只生成 archive record，不生成
  scope-narrowed signal series。
- Scope-narrowed candidate id 不得等于 refined candidate id。
- Signal spec、signal series、prediction artifact、scope filter report、lineage report
  和 delta summary 必须完整生成。
- Prediction artifact 必须显式区分 `prediction_records`、
  `active_prediction_records` 和 `inactive_prediction_records`。
- 所有 outputs 固定 promotion/paper-shadow/production/broker false/none，并固定
  `actual_path_validation_ready=false` / `actual_path_validation_executed=false`。

## 验证计划

- `ruff check .`
- `python -m compileall -q src tests`
- focused parallel pytest for TRADING-2291 tests
- full parallel pytest
- `python -m ai_trading_system.cli docs validate-freshness`
- `python -m ai_trading_system.cli docs report-contract --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-06-30`
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`
- `git diff --check`

## 进展记录

- 2026-06-30: 根据 owner 附件新增并进入 `IN_PROGRESS`；范围限定为
  scope-narrowed candidate-bound artifact generation 和 risk_appetite current-form
  archive，不执行 actual-path validation、owner review、promotion、paper-shadow、
  production 或 broker action。
- 2026-06-30: 实现完成并转入 `VALIDATING`；新增
  `aits research trends scope-narrowed-candidate-generators-regenerate`，真实 run
  读取 TRADING-2290 scope review、TRADING-2288 refined generator artifacts 和
  TRADING-2289 refined validation outputs，生成
  `baseline_plus_trend_structure_scope_narrowed_confirmation_v1`
  active=3,667 / inactive=27,761、`volatility_regime_scope_narrowed_risk_cap_v1`
  active=373 / inactive=31,991，并归档
  `risk_appetite_refined_confidence_v1` current form；top-level validation
  status=`PASS`，所有 outputs 继续固定 actual-path validation、owner review、
  promotion、paper-shadow、production 和 broker action false/none。验证通过
  Ruff、compileall、focused parallel pytest 13 passed、full parallel pytest
  3677 passed、docs freshness、documentation contract、contract-validation
  193 passed、task-register consistency run/validate 和 `git diff --check`。
