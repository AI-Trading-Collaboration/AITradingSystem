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
3. TRADING-1361～1400：PIT data availability 与 walk-forward / overfitting 审计。
4. TRADING-1401～1430：Event override ex-ante taxonomy 与 risk-off / risk-on timing quality。
5. TRADING-1431～1485：Cost / cash yield、stress risk、regime / baseline expansion 与 artifact governance。

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

## Batch 2 设计决策

- Batch 2 覆盖 TRADING-1361～1400：PIT data availability / no-lookahead audit 和
  dynamic strategy walk-forward / overfitting validation。
- PIT audit 的第一版以当前 dynamic execution semantics 可审计输入为边界，固定枚举 price、
  rate、strategy signal、event override、manual/owner review 和 external/manual evidence 等 signal
  families；每条 signal 必须输出 `available_to_system_at`、`decision_at`、`effective_at`、
  `revision_policy`、`pit_risk_level` 和 gate impact。
- PIT audit 不伪造 vendor release calendar。若当前系统没有 release timestamp 或 PIT archive，
  对应 signal 必须标记为 `PIT_UNKNOWN` / `PIT_BLOCKING` 或 `PIT_REVISED_DATA_RISK`，并阻断
  promotion gate。
- Walk-forward validation 只使用 actual-path metrics、edge attribution matrix 和 existing
  execution semantics runtime artifacts；target-path metrics 仍只能 diagnostic。
- Walk-forward 第一版使用已配置的 AI regime 时间窗口做 expanding / rolling / holdout slices，
  并输出 ranking stability、strategy verdict、sample limitations 和 promotion blockers。任何
  `INSUFFICIENT_OOS_EVIDENCE` / `PARAMETER_SENSITIVE` / `REGIME_OVERFITTED` 结论都不得进入
  paper-shadow preflight。

## Batch 2 验收标准

- 新增 CLI：
  - `aits research strategies pit-data-availability-audit`
  - `aits research strategies dynamic-strategy-walk-forward-validation`
- 新增 runtime artifacts：
  - `outputs/research_strategies/pit_audit/<run_id>/signal_pit_audit.csv`
  - `outputs/research_strategies/pit_audit/<run_id>/pit_risk_summary.json`
  - `outputs/research_strategies/walk_forward/<run_id>/walk_forward_leaderboard.csv`
  - `outputs/research_strategies/walk_forward/<run_id>/rolling_oos_metrics.csv`
  - `outputs/research_strategies/walk_forward/<run_id>/parameter_stability_heatmap.csv`
  - `outputs/research_strategies/walk_forward/<run_id>/regime_holdout_results.csv`
- 新增 tracked artifacts：
  - `inputs/research_reviews/pit_data_availability_inventory.yaml`
  - `docs/research/pit_data_availability_audit.md`
  - `docs/research/dynamic_strategy_walk_forward_validation.md`
  - `inputs/research_reviews/dynamic_strategy_walk_forward_matrix.yaml`
- PIT report 必须声明 `ai_after_chatgpt` regime、actual requested date range、PIT risk
  blockers、promotion gate impact、dynamic promotion `BLOCKED` 和 target-path diagnostic-only。
- Walk-forward report 必须声明 split policy、actual-path-only ranking、ranking stability、
  sample limitations、dynamic promotion `BLOCKED` 和 remaining owner/gate blockers。
- `config/report_registry.yaml`、`docs/artifact_catalog.md` 和 `docs/system_flow.md` 同步。
- Focused tests、Ruff、compileall、parallel pytest 和 `git diff --check` 通过，或明确记录未通过原因。

## Batch 3 设计决策

- Batch 3 覆盖 TRADING-1401～1430：event override ex-ante taxonomy / overfit guard 与
  risk-off / risk-on timing quality。
- Event taxonomy 必须从事件类型、source、known_at、trigger_rule、risk_score_rule、
  price_independent_trigger 和 future_return_independent 角度建立配置化审计面。第一版允许
  使用 pilot baseline taxonomy，但必须明确禁止从未来价格下跌或未来收益反推 event severity。
- Event override 输出仍为 watch-only research evidence；risk-off override 只能降低风险，
  risk-on override 默认禁用或慢确认。任何 taxonomy 未满足 ex-ante 条件时，只能输出 blocker，
  不能解锁 paper-shadow。
- Timing quality 只读取 actual-path position path 和同源价格，不使用 target-path metrics。Risk-off
  entry 与 risk-on exit 以实际 QQQ/TQQQ 风险敞口变化为锚点，输出 delay、avoided loss、
  false positive cost、missed upside 与 post-event 5d/20d return。
- Batch 3 继续继承 data quality gate、`ai_after_chatgpt` regime、actual requested date range、
  dynamic promotion `BLOCKED`、target-path diagnostic-only 和 no broker / no production 边界。

## Batch 3 验收标准

- 新增 CLI：
  - `aits research strategies event-override-ex-ante-taxonomy-review`
  - `aits research strategies risk-timing-quality-review`
- 新增 runtime artifacts：
  - `outputs/research_strategies/event_taxonomy/<run_id>/event_override_taxonomy_audit.csv`
  - `outputs/research_strategies/event_taxonomy/<run_id>/event_override_guard_summary.json`
  - `outputs/research_strategies/timing_quality/<run_id>/risk_off_entry_quality.csv`
  - `outputs/research_strategies/timing_quality/<run_id>/risk_on_exit_quality.csv`
  - `outputs/research_strategies/timing_quality/<run_id>/re_risk_delay_cost.csv`
- 新增 tracked artifacts：
  - `config/research/event_override_ex_ante_taxonomy.yaml`
  - `docs/research/event_override_ex_ante_taxonomy_review.md`
  - `inputs/research_reviews/event_override_ex_ante_taxonomy.yaml`
  - `docs/research/risk_off_risk_on_timing_quality_review.md`
  - `inputs/research_reviews/risk_timing_quality_matrix.yaml`
- Reports 必须声明 ex-ante / future-return-independent 检查、actual-path-only timing metrics、
  event override watch-only、dynamic promotion `BLOCKED` 和 target-path diagnostic-only。
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
- 2026-06-27：继续推进 TRADING-1361～1400 Batch 2，范围为 PIT data availability /
  no-lookahead audit 与 dynamic strategy walk-forward / overfitting validation。总任务从
  Batch 1 `VALIDATING` 回到 `IN_PROGRESS`；dynamic promotion 继续 `BLOCKED`，不得进入
  paper-shadow / production / broker。
- 2026-06-27：TRADING-1361～1400 Batch 2 实现完成并转入 `VALIDATING`。新增
  `pit-data-availability-audit` 与 `dynamic-strategy-walk-forward-validation` CLI，真实运行生成
  `docs/research/pit_data_availability_audit.md`、
  `inputs/research_reviews/pit_data_availability_inventory.yaml`、
  `docs/research/dynamic_strategy_walk_forward_validation.md` 和
  `inputs/research_reviews/dynamic_strategy_walk_forward_matrix.yaml`。真实 date range 为
  `2022-12-01`～`2026-06-26`，market regime 为 `ai_after_chatgpt`，
  data_quality_status 为 `PASS_WITH_WARNINGS`。PIT status 为
  `PIT_DATA_AVAILABILITY_REVIEW_READY_WITH_CAVEATS`，所有 core dynamic signals 均为
  date-level `PIT_APPROXIMATED` caveat，target-path metrics 仍 `diagnostic_only`。Walk-forward
  status 为 `WALK_FORWARD_VALIDATION_READY_WITH_BLOCKERS`：`limited_adjustment` 为
  `STABLE_ACROSS_WINDOWS`，`dynamic_v0_5_ai_trend_confirmed_only` 为 `REGIME_OVERFITTED`，
  两个 event override variants 为 `PARAMETER_SENSITIVE`。Dynamic promotion 继续 `BLOCKED`，
  paper-shadow / production / broker 均不允许。验证通过 focused parallel pytest、相关 69 用例
  parallel pytest、Ruff、compileall 和 `git diff --check`。
- 2026-06-27：继续推进 TRADING-1401～1430 Batch 3，范围为 event override ex-ante taxonomy /
  overfit guard 与 risk-off / risk-on timing quality。总任务从 Batch 2 `VALIDATING` 回到
  `IN_PROGRESS`；dynamic promotion 继续 `BLOCKED`，event override 继续 watch-only，
  不得进入 paper-shadow / production / broker。
- 2026-06-27：TRADING-1401～1430 Batch 3 实现完成并转入 `VALIDATING`。新增
  `event-override-ex-ante-taxonomy-review` 与 `risk-timing-quality-review` CLI，真实运行生成
  `docs/research/event_override_ex_ante_taxonomy_review.md`、
  `inputs/research_reviews/event_override_ex_ante_taxonomy.yaml`、
  `docs/research/risk_off_risk_on_timing_quality_review.md` 和
  `inputs/research_reviews/risk_timing_quality_matrix.yaml`。真实 date range 为
  `2022-12-01`～`2026-06-26`，market_regime 为 `ai_after_chatgpt`，
  data_quality_status 为 `PASS_WITH_WARNINGS`。Event taxonomy status 为
  `EVENT_OVERRIDE_EX_ANTE_TAXONOMY_READY_WITH_RUNTIME_GAPS`，原因是 runtime trace 仍缺
  event_type/source taxonomy provenance，因此 event override preflight 继续被阻断。Risk timing
  status 为 `RISK_TIMING_QUALITY_REVIEW_READY_WITH_BLOCKERS`，四个 dynamic variants 均为
  `RISK_OFF_TOO_NOISY`。Dynamic promotion 继续 `BLOCKED`，target-path metrics 继续
  `diagnostic_only`，paper-shadow / production / broker 均不允许。验证通过 focused parallel
  pytest、文档/registry contract parallel pytest、Ruff、compileall 和 `git diff --check`。
