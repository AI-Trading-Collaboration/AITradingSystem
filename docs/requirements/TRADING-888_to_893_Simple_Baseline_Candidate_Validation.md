# TRADING-888～893 Simple Baseline Candidate Validation
最后更新：2026-06-23

## 背景

TRADING-865～878 已建立 QQQ / TQQQ / SGOV simple baseline research CLI，
TRADING-879～887 已完成真实运行和初步 owner decision pack。当前真实结果显示
`equal_risk_qqq_sgov` 是 top recommended candidate，`dyn_tqqq_capped_trend`
仅有小幅 Calmar edge，master review 仍为 `PAUSE_TQQQ_HEAVY`。

本批任务不扩大搜索空间，只围绕真实 artifacts 做候选收敛和人工决策材料：

- `equal_risk_qqq_sgov` 是否值得作为 primary forward-aging candidate；
- `dyn_tqqq_capped_trend` 的小幅 edge 是否足够稳定；
- TQQQ-heavy 是否继续暂停；
- 5 个 watchlist 候选是否应收敛到 1～2 个长期观察对象。

默认解释窗口仍为 `ai_after_chatgpt` regime：anchor event 是 2022-11-30
ChatGPT public launch，默认回测开始日是 2022-12-01。pre-2022 period 或
drawdown episode 只用于 stress/regime comparison；若 QQQ/TQQQ/SGOV 缓存没有可审计覆盖，
必须输出 coverage limitation，不得使用未登记 proxy 或 cash substitute。

## 安全边界

所有新增 CLI 和 artifacts 固定为 research-only / observe-only：

- `production_effect=none`
- `broker_action=none`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `manual_review_required=true`

本批不得进入 paper-shadow，不得修改 production config，不得恢复 tail-risk fallback，不得
启动 LEAPS / Wheel，不得扩大 TQQQ-heavy 搜参，不得生成真实交易建议或 broker action。

## 任务拆解

|任务|范围|状态|
|---|---|---|
|TRADING-888|`equal_risk_qqq_sgov` deep dive，解释权重、收益/风险来源、SGOV carry、cash drag 和反弹错失风险|VALIDATING|
|TRADING-889|period split validation，检查主要候选是否依赖高利率、AI rally 或单一 regime|VALIDATING|
|TRADING-890|drawdown episode review，复盘 2018Q4、2020、2022、2023、2024 和最大 QQQ/TQQQ 回撤段|VALIDATING|
|TRADING-891|dynamic vs static edge significance，判断 `dyn_tqqq_capped_trend` 的小幅 edge 是否足以补偿复杂度、换手和 TQQQ 风险|VALIDATING|
|TRADING-892|TQQQ-heavy pause rationale，正式记录暂停原因和重新开启条件|VALIDATING|
|TRADING-893|watchlist owner decision，汇总 888～892，把 5 个 watchlist 候选收敛为 primary/challenger/paused|VALIDATING|

## 输出

- `outputs/research_strategies/simple_baselines/equal_risk_qqq_sgov_deep_dive.json/md`
- `outputs/research_strategies/simple_baselines/simple_baseline_period_split_validation.json/md`
- `outputs/research_strategies/simple_baselines/simple_baseline_drawdown_episode_review.json/md`
- `outputs/research_strategies/simple_baselines/dynamic_vs_static_edge_significance_review.json/md`
- `outputs/research_strategies/simple_baselines/tqqq_heavy_pause_rationale_report.json/md`
- `outputs/research_strategies/simple_baselines/simple_baseline_watchlist_owner_decision.json/md`
- `docs/research/simple_baseline_watchlist_owner_decision.md`

## Pilot decision heuristics

The following values are pilot review baselines for this task batch, not validated
investment thresholds. They must be revisited after forward-aging evidence matures:

- Dynamic Calmar edge below `0.15` is not material enough to justify added complexity by itself.
- TQQQ-heavy review floor uses average or target TQQQ weight `>=0.25`.
- Dynamic complexity and TQQQ risk penalties are used only to rank review burden, not to approve
  or reject live trading.
- At least `60` additional forward-aging trading days remain the minimum review window before any
  future paper-shadow review can be considered by the owner.

## 验收标准

- 新增 6 个 `aits research strategies ...` CLI，均写 JSON/Markdown artifacts。
- 新增 report registry entries，`artifact_selection_policy=latest_available`、
  `required_for_daily_reading=false`、`production_effect=none`、`broker_action=none`。
- 更新 artifact catalog，记录 artifact path、producer command、schema contract、status enum、
  owner next action 和 research-only safety note。
- 更新 `docs/system_flow.md`，说明 888～893 在 879～887 后的候选验证和决策路径。
- 更新 task register；实现完成时同步进展记录。
- 依赖缓存数据的 CLI 必须先调用与 `aits validate-data` 同源的 validation code path 并在输出中披露
  data quality status、row count、checksum 和 requested/actual date range。
- period split 和 drawdown episode 对缺失历史覆盖必须显式标记 `INSUFFICIENT_PRICE_COVERAGE`
  或 `PARTIAL_PRICE_COVERAGE`，不得静默补值。
- owner decision 最终必须回答 primary candidate、challenger、TQQQ-heavy、LEAPS/Wheel、
  tail-risk fallback、watchlist 收敛和最少 forward-aging days。
- focused pytest、CLI smoke、contract/document/report validation、Ruff、compileall 和
  `git diff --check` 通过或明确记录阻塞。

## 进展记录

- 2026-06-23: 新增本批任务文档并进入 IN_PROGRESS。当前已知真实输入为
  `equal_risk_qqq_sgov` top recommended、`dyn_tqqq_capped_trend` Calmar edge 约 `0.084`、
  master review `PAUSE_TQQQ_HEAVY`、options gate `OPTIONS_RESEARCH_BLOCKED`，所有 safety
  fields 保持 false/none。
- 2026-06-23: 实现完成并转入 VALIDATING。真实 CLI 输出：
  `EQUAL_RISK_DEEP_DIVE_READY`、`PERIOD_SPLIT_REGIME_DEPENDENT`、
  `DRAWDOWN_EPISODE_MIXED`、`DYNAMIC_EDGE_REGIME_CONCENTRATED`、
  `TQQQ_HEAVY_PAUSE_CONFIRMED`、`OWNER_DECISION_READY`。Owner decision 建议
  `equal_risk_qqq_sgov` 作为 primary forward-aging candidate，保留
  `dyn_tqqq_capped_trend` 为 challenger，继续暂停 TQQQ-heavy，继续阻塞
  LEAPS / Wheel 和 tail-risk fallback，下一阶段收敛到 1～2 个候选，最少仍需
  60 个 forward-aging days 才能进入任何 paper-shadow review。data quality 为
  `PASS_WITH_WARNINGS` / 0 errors；pre-2022 period/episode 因 QQQ/TQQQ/SGOV
  历史覆盖不足显式标记 coverage limitation。验证通过：真实 6 个 CLI smoke、
  focused parallel pytest 3 passed、combined report/document/task-register pytest
  30 passed、`python -m ruff check .`、`python -m compileall src tests scripts`
  和 `git diff --check`。
