# TRADING-1326～1485 Dynamic Strategy Remaining Hidden Limit Audit

最后更新：2026-06-27

## 状态

`VALIDATING`

## 背景

TRADING-1201～1325 已经把 dynamic strategy 从 target-path 诊断推进到
execution-semantics-aware actual-path rebacktest，并补充了 staleness repair 与
event override watch-only 研究。现有结论仍然是 dynamic promotion `BLOCKED`，原因不再
只是执行语义缺口，而是需要回答 actual-path 下的真实 edge 是否存在、如果存在适合什么
策略角色，以及后续 PIT / walk-forward / risk timing / cost / regime governance 是否足够
支撑 owner-controlled paper-shadow preflight。

## 安全边界

- Dynamic promotion 继续固定 `BLOCKED`，直到所有 owner review 与后续 gate 解除。
- actual-path metrics 只能用于 ranking、归因和 gate；target-path metrics 只能作为 diagnostic。
- 不改写已经执行的 historical actual position path。
- 不生成 production target weights、order ticket 或 broker action。
- 新实验必须记录 source commit、config hash、policy hash 和 data snapshot hash；缺项时输出
  review blocker，而不是隐式通过。
- 候选最多只能进入 `PAPER_SHADOW_PREFLIGHT_CANDIDATE`，且必须 owner-controlled。

## 阶段拆解

1. TRADING-1326～1345：Actual-path edge attribution 与 baseline underperformance
   diagnosis。复用 execution semantics / event override runtime artifacts，输出 per-strategy
   edge attribution、risk-off event attribution、risk-on recovery attribution、QQQ exposure drag、
   SGOV allocation benefit、tracked review 和 review matrix。
2. TRADING-1346～1360：Dynamic strategy objective 与 promotion gate v2。把 candidate role 明确
   拆成 full allocation strategy、defensive overlay 和 advisory diagnostic；新增 objective/gate
   配置、owner-readable review 与 matrix，且 gate 不得只看 annual return。
3. TRADING-1361～1400：PIT / walk-forward / ex-ante event taxonomy / risk timing quality 审计。
4. TRADING-1401～1430：Cost、cash yield、turnover、stress metrics 和 regime/baseline expansion。
5. TRADING-1431～1485：Artifact governance、tracked snapshots、review closeout 与 owner handoff。

## Batch 1 设计决策

- Batch 1 消费最新 `outputs/research_strategies/execution_semantics/` runtime artifacts；
  不在归因命令内重跑基础 execution semantics，以免混淆输入与诊断层，但会先执行同源
  cached data quality gate，失败时 fail closed。
- 对比范围固定为 `limited_adjustment`、`dynamic_v0_5_ai_trend_confirmed_only`、
  `limited_adjustment_event_override_v1` 和
  `dynamic_v0_5_ai_trend_confirmed_event_override_v1`，baseline 固定为 `no_trade`、`100_qqq`、
  `qqq_60_sgov_40` 和 `qqq_50_sgov_50`。
- Risk-off event attribution 使用 actual path 中 QQQ/TQQQ 风险资产权重下降且发生实际
  rebalance / event override 的交易日作为事件锚点；risk-on recovery 使用实际风险敞口恢复到
  事件前水平的首个交易行；权重 tolerance、sign/materiality verdict baseline 和 role mapping
  记录在 objective/gate 配置中，作为 pilot baseline policy。
- QQQ exposure drag 与 SGOV allocation benefit 以 actual-path realized return、daily actual
  weights 和 benchmark path 做可审计 decomposition；无法解释的剩余项保留为 residual，不强行平滑。
- Objective/gate v2 不给动态策略恢复 promotion，只给出 role classification、remaining blockers
  和 owner next action。

## Batch 1 验收标准

- 新增 CLI：
  - `aits research strategies actual-path-edge-attribution`
  - `aits research strategies dynamic-strategy-objective-gate-review`
- 新增 runtime artifacts：
  - `outputs/research_strategies/edge_attribution/<run_id>/edge_attribution_by_strategy.csv`
  - `outputs/research_strategies/edge_attribution/<run_id>/risk_off_event_attribution.csv`
  - `outputs/research_strategies/edge_attribution/<run_id>/risk_on_recovery_attribution.csv`
  - `outputs/research_strategies/edge_attribution/<run_id>/qqq_exposure_drag.csv`
  - `outputs/research_strategies/edge_attribution/<run_id>/sgov_allocation_benefit.csv`
  - `outputs/research_strategies/edge_attribution/<run_id>/edge_attribution_summary.json`
- 新增 tracked artifacts：
  - `docs/research/actual_path_edge_attribution_review.md`
  - `inputs/research_reviews/actual_path_edge_attribution_matrix.yaml`
  - `config/research/dynamic_strategy_objectives.yaml`
  - `config/research/dynamic_promotion_gate_v2.yaml`
  - `docs/research/dynamic_strategy_objective_gate_review.md`
  - `inputs/research_reviews/dynamic_strategy_objective_gate_matrix.yaml`
- Review verdict 只允许附件列出的 actual-path verdicts；dynamic promotion 继续 `BLOCKED`。
- `config/report_registry.yaml`、`docs/artifact_catalog.md` 和 `docs/system_flow.md` 同步。
- Focused tests、Ruff、compileall、parallel pytest 和 `git diff --check` 通过，或明确记录未通过原因。

## 进展记录

- 2026-06-27：新增总路线并进入 `IN_PROGRESS`；本轮先实现 TRADING-1326～1360 Batch 1，
  剩余 TRADING-1361～1485 保持后续阶段。
- 2026-06-27：TRADING-1326～1360 Batch 1 实现完成并转入 `VALIDATING`。新增
  `actual-path-edge-attribution` 与 `dynamic-strategy-objective-gate-review` CLI，真实运行生成
  `docs/research/actual_path_edge_attribution_review.md`、
  `inputs/research_reviews/actual_path_edge_attribution_matrix.yaml`、
  `docs/research/dynamic_strategy_objective_gate_review.md` 和
  `inputs/research_reviews/dynamic_strategy_objective_gate_matrix.yaml`。真实 date range 为
  `2022-12-01`～`2026-06-26`，market regime 为 `ai_after_chatgpt`，
  data_quality_status 为 `PASS_WITH_WARNINGS`；dynamic promotion 继续 `BLOCKED`，
  target-path metrics 继续 `diagnostic_only`，paper-shadow / production / broker 均不允许。
  验证通过 focused parallel pytest、相关 66 用例 parallel pytest、Ruff、compileall 和
  `git diff --check`。
