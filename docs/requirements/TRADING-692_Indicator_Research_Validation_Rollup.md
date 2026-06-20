# TRADING-692 Indicator Research Validation Rollup

最后更新：2026-06-20

## 背景

TRADING-665～691 已完成 indicator research framework、coverage gap、multi-stage
weight trace、historical trace expansion、gate/lineage repair、horizon-specific
outcome maturity、scenario × horizon effectiveness matrix 和 masking robustness
review。本轮不新增复杂诊断框架，只生成面向复核的 validation rollup、pending
maturity tracker 和 rerun criteria。

当前 valuation/crowding masking 结论为：

- `final_validation_recommendation=keep_preliminary_short_horizon_only`
- 10d supports baseline masking
- 1d/5d neutral or incomplete
- 20d `insufficient_long_horizon_evidence`
- `promotion_gate_allowed=false`
- `production_weight_change_allowed=false`
- `paper_shadow_change_allowed=false`

## 安全边界

- 只生成 read-only / validation-only artifacts。
- 不修改 paper-shadow / live / broker / order / official weights。
- 不改变 production 权重计算逻辑。
- 不放宽 production data_quality_gate。
- `valuation_crowding_indicator` 继续保持 `HIGH_IMPACT_UNVALIDATED`、
  `PRELIMINARY_SHORT_HORIZON_ONLY`、`NO_PROMOTION_ALLOWED`。
- 所有 recommendation 只能作为 validation recommendation，不能进入
  promotion gate。

## 输出要求

新增 indicator research validation rollup，汇总：

- framework readiness
- coverage status
- trace status
- gate / lineage status
- outcome maturity status
- valuation/crowding masking current recommendation
- remaining limitations

新增 pending maturity tracker，至少包括：

- current mature cases by horizon
- pending 20d cases
- expected maturity dates
- by asset / by date / by cluster breakdown
- next recommended rerun date
- criteria to rerun robustness review

重新评估条件包括：

- 20d full_advisory_mature_cases 达到最低阈值；
- 或 1d/5d/10d 至少两个 horizon 一致支持同一 scenario；
- 或 capped/no-mask 在 full_advisory_only 和 cluster-equal-weight 下稳定优于 baseline；
- 或 missed_upside / false_risk_off 明显恶化。

## 验收

- rollup schema test 通过；
- pending maturity tracker schema test 通过；
- rerun criteria test 通过；
- valuation/crowding status tags 保持
  `HIGH_IMPACT_UNVALIDATED` / `PRELIMINARY_SHORT_HORIZON_ONLY` /
  `NO_PROMOTION_ALLOWED`；
- `promotion_gate_allowed=false`；
- `production_weight_change_allowed=false`；
- `paper_shadow_change_allowed=false`；
- 并行 pytest / validation tier 通过或明确记录阻塞。

## 状态记录

- 2026-06-20：新增并进入 `IN_PROGRESS`。
- 2026-06-20：实现完成并进入 `VALIDATING`。新增
  `indicator_research_validation_rollup` builder/CLI、validation-pack artifact、
  stability projection 和 focused tests；真实 expanded historical trace rerun 输出
  `status=PASS_WITH_WARNINGS`、`framework_readiness=READY_WITH_LIMITATIONS`、
  `coverage_status=PASS_WITH_WARNINGS`、`trace_status=PASS`、
  `gate_lineage_status=FULL_ADVISORY_AND_COMPONENT_TRACE_AVAILABLE_WITH_LIMITATIONS`、
  `outcome_maturity_status=SHORT_HORIZON_MATURE_LONG_HORIZON_PENDING`。
  当前 valuation/crowding masking 结论固定为
  `final_validation_recommendation=keep_preliminary_short_horizon_only`：
  10d supports baseline masking，1d/5d neutral or incomplete，20d
  `insufficient_long_horizon_evidence`。`valuation_crowding_indicator` 继续保持
  `HIGH_IMPACT_UNVALIDATED`、`PRELIMINARY_SHORT_HORIZON_ONLY`、
  `NO_PROMOTION_ALLOWED`。
- 2026-06-20：pending maturity tracker 当前输出 mature cases by horizon =
  1d/5d/10d/20d `1176/952/672/56`，pending 20d cases=`1176`，
  cluster breakdown 为 broad_index 10、mega_cap_software 16、
  semiconductor_ai 24（artifact 只展示前 50 条 expected maturity details 的
  breakdown），next recommended rerun date=`2026-06-18`（基于当前价格缓存和
  business-day maturity projection）。Rerun criteria 中
  20d full_advisory_mature_cases=`32`，低于 validation floor 50；1d/5d/10d
  尚未两个 horizon 一致支持同一 scenario；capped/no-mask 尚未在 full advisory
  和 cluster-equal-weight 下稳定优于 baseline；missed_upside/false_risk_off
  检查当前提示需持续复核。
- 2026-06-20：validation pack rerun 输出
  `INDICATOR_TO_SIGNAL_RESEARCH_FRAMEWORK_V1_READY_WITH_LIMITATIONS`，
  artifact_count=31；validation-pack-stability `PASS` stable=true，
  artifact_count=31，`validation_rollup_repeatable=true`。验证通过
  `ruff check`、`py_compile`、focused 并行 pytest 40 passed / 19.47s、
  `git diff --check` 和 full 并行 validation tier 2975 passed /
  642 warnings / 174.30s，runtime artifact 写入
  `outputs/validation_runtime/trading-692-full/test_runtime_summary.json`。
