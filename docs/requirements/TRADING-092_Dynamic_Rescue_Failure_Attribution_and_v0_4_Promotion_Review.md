# TRADING-092: Dynamic Rescue Failure Attribution and v0.4 Promotion Review

最后更新：2026-06-06

## 背景

TRADING-091 已基于真实 cached ETF price data 完成 dynamic v0.3 rescue real
evaluation。latest real evaluation 判定 best v0.3
`dynamic_regime_overlay_v0_3a_constraint_smooth` 为 `reject`：

- constraint hits 仅从 v0.4 的 568 降到 564，改善幅度不足以说明 constraint
  structure 已被修复；
- false risk-off delta vs v0.4 为 0，未暴露新的 false risk-off 问题；
- turnover 为 0.7983，低于 gate，但不能把 lower turnover 本身误读为 constraint
  risk 已解决；
- dynamic-vs-static gap 为 0.2005，长期差距没有恶化；
- drawdown degradation vs v0.4 为 -0.30pp，未显示 v0.3 相对 v0.4 明显牺牲
  drawdown protection；
- full overfit status 为 `REVIEW_REQUIRED`，promotion gate blocker 包含
  `constraint_hit_rate` 与 `robustness_overfit_status`。

本任务需要把 reject 原因拆成可复核的 failure attribution，并判断当前应优先把
v0.4 作为 rescue 主候选继续加 constraint guard，还是进入 v0.5 design。

## 目标

1. 基于 TRADING-091 latest real evaluation report 和真实价格驱动 robustness path，
   生成 v0.3 rejection attribution report。
2. 输出 v0.3 vs v0.4 metric delta table，覆盖 constraint hit、false risk-off、
   drawdown preservation、turnover、dynamic-vs-static gap 和 overfit review。
3. 按 constraint reason、market regime、ticker exposure 和 rebalance window 拆解
   constraint hit failure bucket。
4. 解释 v0.3 是否因为 smoothing 只降低切换噪音，却没有改变触发 constraint hit 的
   weight structure。
5. 解释 drawdown degradation attribution；若未发生 v0.3 vs v0.4 drawdown
   degradation，也要明确说明。
6. 解释 robustness overfit `REVIEW_REQUIRED` 的触发来源和人工复核边界。
7. 给出 v0.4 promotion review；如果 v0.4 不能 promote，给出 v0.5 design
   recommendation。

## 输入

- `reports/etf_portfolio/dynamic_v3_rescue/real_evaluation/dynamic-v3-real-evaluation-report_*.json`
  latest artifact；
- TRADING-091 source config
  `config/etf_portfolio/dynamic_v3_real_evaluation.yaml`；
- TRADING-090 source config
  `config/etf_portfolio/dynamic_v3_constraint_aware_rescue.yaml`；
- TRADING-086 robustness config
  `config/etf_portfolio/dynamic_robustness.yaml`；
- TRADING-084 dynamic allocation config
  `config/etf_portfolio/dynamic_allocation_policy.yaml`；
- TRADING-088 failure diagnostics config
  `config/etf_portfolio/dynamic_failure_diagnostics.yaml`；
- cached ETF price data, after `aits validate-data` equivalent gate。

Persisted TRADING-091 report carries aggregate comparison metrics but not full daily path
records. TRADING-092 therefore must rebuild the necessary in-memory robustness daily
paths from the same configs, date range and price cache before producing bucket-level
attribution. This is not a workaround; it is the intended auditable path because daily
records are derived from source data and configs rather than copied from a truncated
report artifact.

## 阶段拆解

|阶段|状态|交付物|验收标准|
|---|---|---|---|
|TRADING-092A Policy and registry|DONE|`config/etf_portfolio/dynamic_v3_failure_attribution.yaml`、task register、requirement doc|thresholds、promotion review labels、v0.5 recommendation labels 和安全字段可校验|
|TRADING-092B Attribution builder|DONE|`dynamic_v3_failure_attribution.py`|能从 latest TRADING-091 report 重建 v0.3 / v0.4 robustness daily paths，并输出所有必需分析 section|
|TRADING-092C CLI and reports|DONE|`aits etf dynamic-v3-rescue failure-attribution`、`failure-attribution-report --latest`、`validate-attribution`、JSON/Markdown artifacts|CLI 先跑 data quality gate，输出只读 report 和 validation gate|
|TRADING-092D Reader/docs integration|DONE|Reader Brief section、report registry、artifact catalog、system flow、operations runbook、README|Reader Brief 只读 latest attribution report；缺失时显示 `MISSING`，不得补跑上游|
|TRADING-092E Verification|DONE|tests and validation logs|focused pytest、ruff、compileall、diff check、`validate-attribution`、真实 latest smoke 和全量 pytest 通过|
|TRADING-092F Closeout|VALIDATING|status update, commit and push|任务转入 `VALIDATING`，提交到 `main` 并推送；不创建单独分支|

## Promotion Review Policy

v0.4 只能被判为 `promote_v0_4`、`observe_v0_4_with_constraint_guard` 或
`do_not_promote_v0_4`。该标签只代表人工复核优先级，不代表 owner approval、shadow
enrollment、baseline replacement 或 production mutation。

如果 v0.4 的 remaining blocker 主要集中在可单独 guard 的 constraint bucket，且
drawdown、false risk-off、turnover、dynamic-vs-static gap 没有 material failure，则
推荐 `observe_v0_4_with_constraint_guard` 并进入 v0.5 guard design。若 constraint
hit 分布广泛、与核心 exposure path 绑定或 overfit risk 不可解释，则推荐
`do_not_promote_v0_4` 并进入 v0.5 exposure redesign。

## 安全边界

所有 TRADING-092 artifacts 必须固定：

- `observe_only=true`
- `candidate_only=true`
- `production_effect=none`
- `broker_action=none`
- `manual_review_required=true`
- `production_state_mutated=false`
- `baseline_config_mutated=false`
- `official_target_weights_mutated=false`
- `automatic_candidate_promotion=false`
- `auto_enrollment_without_owner_approval=false`
- `shadow_enrollment_allowed=false`
- `automatic_enrollment_allowed=false`
- `owner_approval_executed=false`

禁止输出或写入 owner approval、shadow enrollment record、official target weights、
production baseline mutation、broker order 或自动 promotion。

## 进展记录

- 2026-06-06：新增并进入 `IN_PROGRESS`。原因：TRADING-091 latest real evaluation
  已将 dynamic v0.3 判定为 `reject`，需要结构化归因并判断 v0.4 guard path 与 v0.5
  design path 的优先级。
- 2026-06-06：baseline 实现完成并进入 `VALIDATING`。新增 failure attribution
  policy、builder、CLI、JSON/Markdown report、validation gate、Reader Brief section、
  report registry、artifact catalog、system flow、operations runbook、README 和 tests。
  真实 latest report 为
  `reports/etf_portfolio/dynamic_v3_rescue/failure_attribution/dynamic-v3-failure-attribution-report_893674979fd07e14.json`：
  v0.3 best candidate 仍为 `dynamic_regime_overlay_v0_3a_constraint_smooth`，
  constraint hits 为 564 vs v0.4 568，reduction=4 低于 material threshold=10；
  smoothing 主要降低 turnover / 切换噪音，但未结构性改变 constraint hit 权重结构；
  drawdown 未成为 reject 主因；overfit status 仍为 `REVIEW_REQUIRED`；v0.4 review 为
  `observe_v0_4_with_constraint_guard`，v0.5 recommendation 为
  `recommend_v0_5_constraint_guard`。验证通过 focused tests 13 passed、
  `validate-attribution` PASS、`failure-attribution-report --latest`、report index
  `PASS_WITH_WARNINGS`、Reader Brief OK、ruff、compileall、diff check 和全量
  pytest 2177 passed / 330 warnings。
