# TRADING-2309 AI Leadership Actual-Path Validation

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

TRADING-2308 已生成 AI / semiconductor leadership 的 candidate-bound price-proxy
POC artifacts：

- `smh_relative_strength_leadership_v1`
- `ai_semiconductor_leadership_quality_v1`
- `ai_core_basket_leadership_v1`

TRADING-2308 只证明 generator POC 可以生成 schema-valid artifacts，并不证明 signal
有效。本任务必须在 research-only 边界下执行 actual-path validation，验证 AI /
semiconductor leadership 是否能解释：

- `SMH` future relative return；
- `QQQ` / `SMH` drawdown risk；
- AI leadership weakening windows；
- SMH overweight risk。

## 目标

新增 CLI：

```bash
aits research trends ai-leadership-actual-path-validation
```

默认读取：

```text
outputs/research_trends/ai_semiconductor_leadership_generator_poc/
data/raw/prices_daily.csv
data/raw/rates_daily.csv
```

## 输出状态

只允许输出以下研究状态：

- `AI_LEADERSHIP_VALIDATED_CONTINUE_RESEARCH`
- `AI_LEADERSHIP_VALIDATED_INCONCLUSIVE`
- `AI_LEADERSHIP_REJECT_RECOMMENDED`

不得输出 promotion、paper-shadow、production 或 broker-ready 状态。

## 产物

- `ai_leadership_actual_path_validation_summary.json`
- `ai_leadership_actual_path_matrix.json`
- `ai_leadership_actual_path_matrix.csv`
- `ai_leadership_prediction_outcome_matrix.json`
- `ai_leadership_prediction_outcome_matrix.csv`
- `ai_leadership_candidate_scorecard.json`
- `ai_leadership_objective_coverage_matrix.json`
- `ai_leadership_state_recommendation_matrix.json`
- `ai_leadership_actual_path_safety_boundary.json`
- `docs/research/ai_semiconductor_leadership_actual_path_validation.md`

## 实施边界

1. 数据质量门禁。
   - CLI 必须调用与 `aits validate-data` 同源的 `validate_data_cache`。
   - 默认对 TRADING-2308 required symbols 和 `QQQ` / `SMH` outcome assets 执行
     required-symbol gate。
   - 若 gate 有 error，命令 fail closed，不生成 actual-path validation artifacts。
   - 输出必须披露 data quality status、quality report path、checked symbols 和
     full-universe ASX limitation。

2. Validation policy。
   - 新增 `config/research/ai_leadership_actual_path_validation_policy.yaml`。
   - 所有 sample floor、relative return threshold、drawdown threshold、objective
     pass boundary、reject boundary 和 final recommendation rule 必须在 policy
     manifest 中记录 owner、status、rationale、intended effect、validation evidence
     或 planned validation、review / expiry condition。

3. Actual-path evidence。
   - 使用 TRADING-2308 candidate signal series 的 `decision_timestamp` 和 `horizon`
     计算 `QQQ` / `SMH` future return、SMH relative return、target drawdown、
     QQQ drawdown 和 SMH drawdown。
   - `SMH future relative return`、`QQQ / SMH drawdown risk`、`AI leadership
     weakening windows` 和 `SMH overweight risk` 必须分别形成 objective coverage
     row。
   - 本任务可以使用未来 outcome 计算 validation evidence，但不得把结果写回
     generator、daily scoring、portfolio weights 或 broker path。

4. 安全边界。
   - `promotion_allowed=false`
   - `paper_shadow_allowed=false`
   - `production_allowed=false`
   - `broker_action=none`
   - `actual_path_validation_executed=true`
   - `scope_review_ready=false`
   - `dynamic_promotion_status=BLOCKED`
   - `paper_shadow_recommendation_allowed=false`

## 验收标准

- CLI implemented: `aits research trends ai-leadership-actual-path-validation`。
- 三个 TRADING-2308 candidate id 均生成 candidate scorecard。
- Summary 披露 selected market regime=`ai_after_chatgpt` 和实际 requested date range。
- Data quality gate 通过状态在 downstream artifacts 中可见。
- 输出只使用允许的三种 AI leadership validation status。
- 不修改 TRADING-2308 generator artifacts，不生成 promotion / paper-shadow /
  production / broker-ready 结论。

## 进展记录

- 2026-07-01: 根据 owner post-2302 roadmap 和 TRADING-2308 recommended_next_task
  新增并进入 `IN_PROGRESS`。当前 worktree 已有两个无关 research 文档未提交改动，
  本任务必须 selective staging，不能混入无关改动。
- 2026-07-01: 实现完成并转入 `VALIDATING`。真实 run
  status=`AI_LEADERSHIP_VALIDATED_CONTINUE_RESEARCH`，
  data_quality_status=`PASS_WITH_WARNINGS`，
  actual_requested_date_range=`2022-12-01..2026-06-29`，
  actual_path_record_count=48330，validation_eligible_record_count=47646。三个
  candidate scorecard 均为 continue-research；四个 objective rows 中 2 个 PASS、
  2 个 `INCONCLUSIVE_OR_WEAK`。本状态不是 scope review ready，不允许
  promotion、paper-shadow、production 或 broker action。
