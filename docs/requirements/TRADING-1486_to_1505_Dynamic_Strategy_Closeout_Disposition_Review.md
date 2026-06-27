# TRADING-1486～1505 Dynamic Strategy Closeout Disposition Review

最后更新：2026-06-27

## 状态

`VALIDATING`

## 背景

TRADING-1326～1485 已完成 dynamic strategy hidden-limit audit 的四个批次：

- Batch 1：actual-path edge attribution 与 objective gate v2。
- Batch 2：PIT data availability / walk-forward / overfitting validation。
- Batch 3：event override ex-ante taxonomy 与 risk-off / risk-on timing quality。
- Batch 4：transaction cost / cash yield、stress risk、regime baseline expansion 和 artifact governance。

最新证据显示 `limited_adjustment` 仍有一定 actual-path edge，但 dynamic strategy 主线没有形成足够证据作为 full allocation strategy。多数变体存在 static baseline underperformance、false risk-off、risk-on recovery delay、policy sensitivity、walk-forward/regime fragility、event override runtime provenance gap、cost/stress/regime blocker 或 target-path legacy misuse 风险。Dynamic promotion 继续 `BLOCKED`，paper-shadow / production / broker 均保持 disabled。

本批任务不再继续调参寻找历史最优版本，而是对 TRADING-1326～1485 的 blocker 与 surviving module 做正式 closeout / disposition review。

## 安全边界

- 不恢复 dynamic promotion。
- 不进入 paper-shadow、production 或 broker execution。
- 不以 target-path metrics 作为 promotion evidence。
- 不删除已有 research evidence，只标注旧证据的 closeout/legacy role。
- Defensive overlay 只能降低风险、输出 advisory 或触发 manual review；不能自动 risk-on 或下 broker order。
- 重新打开 full allocation research 必须满足新的 actual-path、locked sample、PIT、walk-forward、net-of-cost、stress 和 owner-approved preflight 条件。

## 阶段拆解

1. TRADING-1486：生成 blocker inventory，汇总 execution、staleness、event override、cost、stress、regime、PIT/data、overfitting 和 governance blocker。
2. TRADING-1487：生成 surviving candidate disposition matrix，覆盖 base、staleness-aware 和 event-override variants。
3. TRADING-1488：形成 full allocation viability assessment，判断是否暂停 full allocation route。
4. TRADING-1489：评估 defensive overlay / advisory diagnostic 的可行性。
5. TRADING-1490：定义 dynamic strategy final status。
6. TRADING-1491：生成 owner closeout decision pack。
7. TRADING-1492：定义 full allocation research reopen criteria。
8. TRADING-1493：新增 guardrail tests，防止 closeout 后误恢复 promotion / broker / target-path promotion。
9. TRADING-1494：在 policy registry 中登记 closeout status。
10. TRADING-1495：更新 report registry、artifact catalog、system flow 和 task register。
11. TRADING-1496：生成 machine-readable closeout snapshot。
12. TRADING-1497～1499：生成 owner reader brief、标注 legacy evidence role、验证 closeout artifact governance metadata。
13. TRADING-1500～1505：运行 focused validation，提交并按本地提交纪律推送。

## 验收标准

- `inputs/research_reviews/dynamic_strategy_blocker_inventory.yaml` 与 `docs/research/dynamic_strategy_blocker_inventory.md` 完成，并覆盖 TRADING-1326～1485 blocker。
- `inputs/research_reviews/dynamic_strategy_candidate_disposition_matrix.yaml` 完成，并为 surviving candidates 给出 disposition。
- `docs/research/dynamic_full_allocation_viability_assessment.md` 明确结论为 `PAUSE_FULL_ALLOCATION_RESEARCH`，除非证据支持更强结论。
- `docs/research/dynamic_defensive_overlay_feasibility_review.md` 与 `inputs/research_reviews/dynamic_defensive_overlay_feasibility.yaml` 明确 defensive overlay 只能 observe-only / advisory-only / risk-reduction。
- `inputs/research_reviews/dynamic_strategy_final_status.yaml` 和 `inputs/research_reviews/dynamic_strategy_closeout_snapshot.yaml` 固定 promotion/paper-shadow/production/broker disabled。
- `docs/research/dynamic_strategy_closeout_decision_pack.md` 可直接交 owner 审阅，默认推荐 `APPROVE_DOWNGRADE_TO_DEFENSIVE_OVERLAY`。
- `inputs/research_reviews/dynamic_full_allocation_reopen_criteria.yaml` 与 `docs/research/dynamic_full_allocation_reopen_criteria.md` 明确 reopen 条件。
- `tests/test_dynamic_strategy_closeout_guardrails.py` 覆盖 promotion、overlay、advisory、legacy evidence 和 target-path metrics guardrails。
- `config/research/dynamic_strategy_closeout_policy.yaml`、`config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md` 和 `docs/task_register.md` 同步更新。
- Closeout artifacts 均包含 source commit、review/run id、config/policy hash 或 artifact reference、metric namespace、actual-path-only marker、owner review status、promotion status 和 sha256 或 upstream artifact reference。
- 通过 focused validation：Ruff、compileall、guardrail tests、research artifact governance tests、execution semantics / external validation / report registry / documentation / task register contract tests、`git diff --check` 和 `git diff --cached --check`。

## 进展记录

- 2026-06-27：新增并进入 `IN_PROGRESS`。本批承接 TRADING-1326～1485 `VALIDATING` 证据，目标是完成 dynamic strategy closeout / disposition review；默认推荐 full allocation research paused、defensive overlay research active、advisory diagnostic active、dynamic promotion `BLOCKED`、paper-shadow / production / broker disabled。
- 2026-06-27：实现完成并转入 `VALIDATING`。新增 closeout policy、blocker inventory、candidate disposition matrix、full allocation viability assessment、defensive overlay feasibility review、final status、owner decision pack、reopen criteria、closeout snapshot、reader brief、legacy evidence label 和 guardrail tests；默认 owner recommendation=`APPROVE_DOWNGRADE_TO_DEFENSIVE_OVERLAY`，dynamic promotion 继续 `BLOCKED`，paper-shadow / production / broker disabled。验证通过 Ruff、compileall、focused parallel pytest、execution semantics / external validation / documentation contract parallel pytest；提交前仍需完成 `git diff --check` 与 `git diff --cached --check`。
