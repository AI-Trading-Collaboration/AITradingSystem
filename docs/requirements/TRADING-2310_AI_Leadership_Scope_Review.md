# TRADING-2310 AI Leadership Scope Review

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

TRADING-2307 / TRADING-2308 / TRADING-2309 已完成 AI / semiconductor
leadership 的 feasibility audit、candidate-bound generator POC 和 research-only
actual-path validation。TRADING-2309 真实 run 输出：

- status=`AI_LEADERSHIP_VALIDATED_CONTINUE_RESEARCH`
- data_quality_status=`PASS_WITH_WARNINGS`
- actual_requested_date_range=`2022-12-01..2026-06-29`
- validation_eligible_record_count=47646
- 三个 candidate 均为 continue-research
- objective rows 中 2 个 PASS、2 个 `INCONCLUSIVE_OR_WEAK`

TRADING-2310 不能把 continue-research 解释为 promotion evidence。本任务只做 scope
review，判断 AI leadership 更适合哪些研究用法和范围。

## 目标

新增 CLI：

```bash
aits research trends ai-leadership-scope-review
```

默认读取：

```text
outputs/research_trends/ai_leadership_actual_path_validation/
data/raw/prices_daily.csv
data/raw/rates_daily.csv
```

Scope review 必须明确回答：

- 是否适合 `SMH only`；
- 是否适合 `QQQ + SMH`；
- 是否适合 `10d` / `20d` horizon；
- 是否只适合 `confirmation_only`；
- 是否适合 `exposure_cap_modifier`。

## 输出状态

只允许输出以下研究状态：

- `AI_LEADERSHIP_SCOPE_REVIEW_READY_RESEARCH_ONLY`
- `AI_LEADERSHIP_SCOPE_REVIEW_INCONCLUSIVE`
- `AI_LEADERSHIP_SCOPE_REVIEW_REJECT_RECOMMENDED`

不得输出 promotion、paper-shadow、production 或 broker-ready 状态。

## 产物

- `ai_leadership_scope_review_summary.json`
- `ai_leadership_asset_scope_matrix.json`
- `ai_leadership_asset_scope_matrix.csv`
- `ai_leadership_horizon_scope_matrix.json`
- `ai_leadership_horizon_scope_matrix.csv`
- `ai_leadership_use_case_scope_matrix.json`
- `ai_leadership_use_case_scope_matrix.csv`
- `ai_leadership_recommended_scope.json`
- `ai_leadership_scope_review_safety_boundary.json`
- `docs/research/ai_semiconductor_leadership_scope_review.md`

## 实施边界

1. 数据质量门禁。
   - CLI 必须调用与 `aits validate-data` 同源的 `validate_data_cache`。
   - 默认对 TRADING-2309 required symbols 和 `QQQ` / `SMH` outcome assets 执行
     required-symbol gate。
   - 若 gate 有 error，命令 fail closed，不生成 scope-review artifacts。
   - 输出必须披露 data quality status、quality report path 和 full-universe ASX
     limitation。

2. Scope-review policy。
   - 新增 `config/research/ai_leadership_scope_review_policy.yaml`。
   - 所有 sample floor、alignment threshold、drawdown threshold、scope keep /
     reject boundary 和 final recommendation rule 必须在 policy manifest 中记录
     owner、status、rationale、intended effect、validation evidence 或 planned
     validation、review / expiry condition。

3. Scope evidence。
   - 使用 TRADING-2309 prediction/outcome matrix、candidate scorecard 和 objective
     coverage matrix。
   - 生成 asset scope、horizon scope 和 use-case scope matrices。
   - `SMH only`、`QQQ + SMH`、`10d`、`20d`、`confirmation_only` 和
     `exposure_cap_modifier` 必须各自有明确 decision row。

4. 安全边界。
   - `research_only=true`
   - `scope_review_executed=true`
   - `actual_path_validation_consumed=true`
   - `promotion_allowed=false`
   - `paper_shadow_allowed=false`
   - `production_allowed=false`
   - `broker_action=none`
   - `dynamic_promotion_status=BLOCKED`
   - `forward_observe_started=false`
   - `owner_approval_required_before_forward_observe=true`

## 验收标准

- CLI implemented: `aits research trends ai-leadership-scope-review`。
- Summary 披露 selected market regime=`ai_after_chatgpt` 和实际 requested date
  range。
- Data quality gate 通过状态在 downstream artifacts 中可见。
- 输出只使用允许的三种 AI leadership scope-review status。
- Asset / horizon / use-case matrices 覆盖 owner roadmap 的 scope questions。
- 不修改 TRADING-2308 generator artifacts 或 TRADING-2309 validation artifacts。
- 不生成 promotion / paper-shadow / production / broker-ready 结论。

## 进展记录

- 2026-07-01: 根据 owner post-2302 roadmap 和 TRADING-2309 next task 新增并进入
  `IN_PROGRESS`。当前 worktree 已有两个无关 research 文档未提交改动，本任务必须
  selective staging，不能混入无关改动。
- 2026-07-01: 实现完成并转入 `VALIDATING`。真实 run
  status=`AI_LEADERSHIP_SCOPE_REVIEW_READY_RESEARCH_ONLY`，
  data_quality_status=`PASS_WITH_WARNINGS`，
  actual_requested_date_range=`2022-12-01..2026-06-29`。Scope decision:
  `QQQ + SMH` 保留为 research-only scope，`SMH only` 仅 diagnostic；`10d`
  保留为 owner-reviewed horizon，`20d` 仅 diagnostic；recommended use cases 为
  `confirmation_only` 和 `exposure_cap_modifier`，`standalone_alpha` reject。
  所有 outputs 继续禁止 promotion、paper-shadow、production 和 broker action。
