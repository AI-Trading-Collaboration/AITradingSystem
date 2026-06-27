# TRADING-1306～1325 Pending Plan Supersede Event Override

最后更新：2026-06-27

## 状态

`VALIDATING`

## 背景

TRADING-1286～1305 的 staleness-aware repair 结论为
`NO_MATERIAL_IMPROVEMENT`，dynamic promotion 继续 `BLOCKED`。下一步不再主要
修补 signal staleness，而是明确 actual-path execution semantics：T 日已知重大风险
事件可以在 no-lookahead 条件下覆盖尚未执行的 pending plan，并在 T+1 生成新的
risk-aware target weights。

## 安全边界

- 只做 research-only / watch-only evidence。
- 不恢复 dynamic promotion。
- 不进入 paper-shadow、production 或 broker。
- 不改写已经执行的 historical actual path。
- 不允许 T 日之后才知道的信息影响 T 日或 T+1 决策。
- target-path metrics 继续只能作为 diagnostic，不作为 ranking 或 promotion 正向依据。

## 阶段拆解

1. TRADING-1306～1313：新增 pending plan 状态机、event override policy、
   `EventOverrideDecision`、no-lookahead evidence 和 risk-off-only fast override 语义。
2. TRADING-1314～1315：接入 `execution-semantics-rebacktest` 的可选
   `event_override_t_plus_1` mode，输出 pending plan ledger、supersede log、
   event override trace、summary 和 no-lookahead evidence。
3. TRADING-1316～1318：重跑 watch-only event override variants，生成 survival matrix
   和 owner-readable review，dynamic promotion 继续 `BLOCKED`。
4. TRADING-1319～1323：补测试、报告索引、artifact catalog、system flow 和 task
   register 覆盖。
5. TRADING-1324～1325：运行验证、提交并在普通 upstream 可用时推送。

## 设计决策

- `executed_position` 视为 immutable，不允许 supersede。
- `ADVISORY_GENERATED` 与 `PENDING_REBALANCE` 可以在 policy 允许时 supersede。
- 默认 fast override 只允许 risk-off：降低 QQQ/SMH/SOXQ/TQQQ 等风险资产或增加
  CASH/SGOV；risk-on 默认关闭，需确认期后才能考虑。
- Event override 的实际执行日期必须晚于 decision date；当前实现目标为 T 日 review、
  T+1 execution。
- 每个 override 都必须写出 `event_known_at`、`review_at`、`decision_at`、
  `effective_at` 和 no-lookahead checks。

## 验收标准

- `config/research/event_override_policy.yaml` 存在，包含 no-lookahead、pending plan
  supersede、risk-off、risk-on、cooldown 和 audit 配置。
- `execution-semantics-rebacktest` 支持 event override 参数和
  `event_override_t_plus_1` mode。
- 策略目录输出 `event_override_trace.json`、`pending_plan_ledger.csv`、
  `supersede_log.csv`、`event_override_summary.json` 和
  `no_lookahead_evidence.json`。
- 聚合输出 `event_override_leaderboard_actual_path.csv`、
  `event_override_vs_base_actual_path.csv`、`event_override_summary.json`、
  `event_override_gate.json`，并写入
  `inputs/research_reviews/event_override_survival_matrix.yaml`。
- `docs/research/event_override_execution_semantics_review.md` 回答 owner 复核问题。
- fast override 默认不能增加 QQQ/SMH/SOXQ/TQQQ、不能增加 leverage、不能降低
  SGOV/CASH 追涨。
- dynamic promotion 最终状态继续 `BLOCKED`；最多输出
  `PAPER_SHADOW_PREFLIGHT_CANDIDATE` 的 owner-controlled preflight status。
- focused tests、Ruff、compileall、并行 pytest 和 `git diff --check` 通过，或明确记录
  未通过原因。

## 进展记录

- 2026-06-27：新增任务文档并进入 `IN_PROGRESS`，开始实现 pending plan supersede
  与 event override execution semantics。
- 2026-06-27：实现完成并转入 `VALIDATING`；真实 dry-run 生成 event override
  runtime artifacts、survival matrix 和 owner review。聚合结果为 `event_review_count=36`、
  `override_trigger_count=36`、`pending_plan_supersede_count=36`、
  `t_plus_1_execution_count=36`、`blocked_override_count=0`；
  `limited_adjustment_event_override_v1` verdict 为
  `EVENT_OVERRIDE_INCREASES_TURNOVER_TOO_MUCH`，
  `dynamic_v0_5_ai_trend_confirmed_event_override_v1` verdict 为
  `EVENT_OVERRIDE_TOO_NOISY`；paper-shadow preflight candidate 未识别，dynamic
  promotion 继续 `BLOCKED`。
