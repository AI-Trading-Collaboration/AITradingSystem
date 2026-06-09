# TRADING-060 Portfolio Turnover & Cost Drag Attribution for Weight Candidates

最后更新：2026-06-09

状态：DONE

## 背景

TRADING-059A 已把 latest weight tuning failure 解释为：

- `status=NO_CANDIDATE_EXPLAINED`
- `root_cause_category=portfolio_turnover_too_high`
- `top_failure_reason=cost_drag_too_high`
- `most_common_guardrail_failure=turnover_guardrail_failed`
- `recommended_next_action=review_portfolio_turnover_constraints`
- `production_effect=none`
- `auto_promotion=false`

这说明 TRADING-059 restricted backtest weight tuning 没有找到可用 shadow weight
candidate 的主要原因不是数据不可用，也不是 production safety 问题，而是候选组合在
回测中触发了过高换手或成本拖累。当前不应直接降低 turnover guardrail，也不应直接扩
大搜索空间或放开 fallback signals。TRADING-060 只解释原因，不让候选通过。

## 目标

1. 读取 latest 或指定日期的 `weight_tuning_summary.json`、`weight_tuning_candidates.json`
   和 `weight_tuning_failure_summary.json`。
2. 找出因 turnover / cost drag 被拒绝的 weight candidates。
3. 输出 candidate-level turnover、cost drag、guardrail distance 和 near-miss analysis。
4. 在输入足够时输出 asset-level turnover contribution、walk-forward window turnover
   和 rebalance event attribution。
5. 识别 root cause：`rebalance_threshold_too_low`、`score_volatility_too_high`、
   `weight_search_too_aggressive`、`asset_rotation_too_frequent`、`position_caps_binding`、
   `cost_model_too_punitive`、`small_trade_noise`、`regime_specific_choppiness`、
   `mixed` 或 `insufficient_details`。
6. 生成只读 JSON/Markdown artifact，并接入 report alias、Dashboard、Reader Brief 和
   shadow backtest supporting artifacts。

## 非目标与安全边界

- 不重新调参。
- 不降低 turnover guardrail。
- 不修改 cost model。
- 不修改 `config/parameters/production/current.yaml`。
- 不启用 rejected candidate，不生成新的 `recommended_shadow_weights`。
- 不放开 fallback signal。
- 不做真实交易。
- 不把高 turnover candidate 改成 watch。
- 不绕过 walk-forward，不用单一全样本收益判断 candidate。

所有输出必须保持：

```yaml
production_effect: none
manual_review_required: true
auto_promotion: false
production_config_modified: false
```

## 输入

主要输入：

- `artifacts/weight_tuning/YYYY-MM-DD/weight_tuning_summary.json`
- `artifacts/weight_tuning/YYYY-MM-DD/weight_tuning_candidates.json`
- `artifacts/weight_tuning_failure/YYYY-MM-DD/weight_tuning_failure_summary.json`
- `artifacts/portfolio_candidates/YYYY-MM-DD/portfolio_candidates_summary.json`
- `artifacts/portfolio_sensitivity/YYYY-MM-DD/portfolio_sensitivity_summary.json`
- `artifacts/signal_snapshots/YYYY-MM-DD/signal_snapshot.json`
- `artifacts/backtest_snapshots/YYYY-MM-DD/backtest_input_manifest.json`
- `config/parameters/weight_tuning_v0_1.yaml`
- `config/portfolio/portfolio_candidate_profiles.yaml`
- `config/parameters/production/current.yaml`

如果缺少 `weight_tuning_failure_summary.json`，输出 `status=BLOCKED`、
`reason=missing_weight_tuning_failure_summary`。如果缺少 candidate-level turnover
details，输出 `status=LIMITED`、`reason=insufficient_candidate_turnover_details`，并建议
增强 candidate turnover logging。

## CLI 与报告

新增命令：

```bash
aits portfolio explain-turnover --latest
aits portfolio explain-turnover --date YYYY-MM-DD
aits portfolio explain-turnover --weight-tuning artifacts/weight_tuning/YYYY-MM-DD/weight_tuning_summary.json
aits portfolio explain-turnover --latest --near-miss-only
aits portfolio explain-turnover --latest --debug
aits portfolio validate-turnover-attribution --latest
aits reports portfolio-turnover-attribution --latest
```

输出目录：

```text
artifacts/portfolio_turnover_attribution/YYYY-MM-DD/
```

输出文件：

- `portfolio_turnover_attribution_summary.json`
- `portfolio_turnover_attribution_summary.md`
- `portfolio_turnover_candidates_debug.json`，仅 debug 时写出。

## JSON 结构

核心字段：

- `metadata`
- `inputs`
- `summary`
- `candidate_turnover_summary`
- `candidate_turnover_attribution`
- `cost_drag_attribution`
- `asset_turnover_contribution`
- `walk_forward_turnover`
- `rebalance_attribution`
- `near_miss_turnover_analysis`
- `root_cause`
- `recommended_next_action`
- `promotion_impact`
- `diagnostic_quality`
- `safety`

## Dashboard / Reader Brief / Shadow Backtest

Dashboard 新增 `Portfolio Turnover Attribution` 只读卡片，展示 latest status、root cause、
top failure reason、guardrail failure、failed candidate count、near-miss count、top turnover
assets、cost drag delta、recommended next action、production effect 和 artifact links。

Reader Brief 的 `parameter_shadow_review` 展示 turnover attribution 摘要，说明 weight tuning
失败主要来自 rebalance threshold、score volatility、weight search distance、asset rotation
或 cost drag。

`aits parameters shadow-backtest --latest --dry-run` 仍保持 rejected，但 `supporting_artifacts`
应引用 `portfolio_turnover_attribution_summary.json`。

## 验收标准

完成后运行：

```bash
aits portfolio explain-turnover --latest
aits portfolio validate-turnover-attribution --latest
aits reports portfolio-turnover-attribution --latest
aits reports reader-brief --latest
aits parameters shadow-backtest --latest --dry-run
python -m pytest -q
python -m ruff check scripts src tests
python -m compileall src scripts
git diff --check
```

预期：

- turnover attribution artifact 成功生成。
- validation 输出 `TURNOVER_FAILURE_EXPLAINED`、`LIMITED` 或 `BLOCKED`。
- `production_effect=none`、`manual_review_required=true`、`auto_promotion=false`、
  `production_config_modified=false`。
- Reader Brief 展示 turnover attribution 摘要。
- shadow backtest dry-run 仍 `promotion_status=rejected`，并引用 turnover attribution artifact。
- `config/parameters/production/current.yaml` 未修改。

## 进展记录

- 2026-05-31：新增任务并进入 `IN_PROGRESS`。需求来自 owner 提供的 TRADING-060 规格，
  目标是在不改变 production 参数、不降低 guardrail 的前提下解释 TRADING-059
  `NO_CANDIDATE` 的 turnover / cost drag 来源。
- 2026-05-31：实现、文档和真实验收完成，状态推进到 `VALIDATING`。
  `aits portfolio explain-turnover --latest` 生成 2026-05-28 artifact，
  `status=TURNOVER_FAILURE_EXPLAINED`、`root_cause_category=weight_search_too_aggressive`、
  `top_failure_reason=cost_drag_too_high`、`failed_by_turnover=240`。Reader Brief 已展示
  turnover 摘要，shadow backtest dry-run 仍 `promotion_status=rejected` 并引用
  turnover attribution artifact；`config/parameters/production/current.yaml` 未修改。
- 2026-06-09：从 `VALIDATING` 改为 `DONE`，原因：latest turnover attribution 仍为
  `TURNOVER_FAILURE_EXPLAINED`，`root_cause_category=weight_search_too_aggressive`、
  `top_failure_reason=cost_drag_too_high`、`failed_by_turnover=240`，
  `recommended_next_action=tighten_weight_search_l1_distance`；该结论已由 TRADING-061
  search stability constraints 承接，且不降低 guardrail、不修改 cost model、不启用 rejected
  candidate。验证通过 `validate-turnover-attribution --latest`、`reports
  portfolio-turnover-attribution --latest`、`explain-turnover --latest`、Reader Brief、
  shadow backtest dry-run、focused pytest 24 passed、文档检查、Ruff、repo-wide Black
  check、`compileall` 和 `git diff --check`。
