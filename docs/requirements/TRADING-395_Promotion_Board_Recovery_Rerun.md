# TRADING-395 Promotion Board Recovery Rerun

最后更新：2026-06-17

## 背景

TRADING-391 至 TRADING-394 已补齐 recovery evidence pack、owner hold decision、owner
review template v2，并重新运行 research monthly review pack。Monthly rerun 仍显示 cost
sensitivity `NOT_MEANINGFUL_UNDER_COSTS` 和 benchmark baseline
`CANDIDATE_UNDERPERFORMS_BASELINES`。TRADING-395 负责在这些恢复后 evidence 可见后重跑
paper-shadow promotion board，确认 promotion decision 是否保持 fail closed。

## 范围

- 使用 `outputs/reports/report_index_2026-06-17.json` 作为同日 report discovery input。
- 运行 `aits reports paper-shadow-promotion-board --as-of 2026-06-17`。
- 运行 `aits reports validate-paper-shadow-promotion-board --latest`。
- 汇总 board decision、required evidence checklist、blocking reasons、warnings、owner decision、
  safety boundary 和 Reader Brief section。
- 重建 report index 和 Reader Brief。

## 决策边界

Promotion board 的有效决策限定为：

- `CONTINUE_NORMAL_SHADOW`
- `HOLD_FOR_MORE_DATA`
- `RETURN_TO_RESEARCH`
- `REJECT`

`EXTEND_SHADOW` 只能在所有 extended-shadow prerequisite 显式满足时出现。本任务中的
monthly/cost/benchmark/owner hold evidence 不满足该条件，因此不得把恢复 evidence 可读误解释为
extended shadow approval。

## 安全边界

- 不运行上游 evidence generation。
- 不刷新 market/cache data。
- 不补造 missing artifacts。
- 不新增 waiver，不弱化 blocker。
- 不修改 strategy output、candidate state、paper-shadow state 或 production state。
- 不批准 normal shadow、promotion、extended shadow 或 live trading。
- 不生成 official target weights，不触发 broker action 或 order ticket。

## 验收标准

- Promotion board rerun artifact 和 validation artifact 生成。
- 若 cost/benchmark/monthly/owner hold 等 blocker 仍存在，decision 必须保持 conservative：
  `HOLD_FOR_MORE_DATA`、`RETURN_TO_RESEARCH` 或 `REJECT`。
- 不输出 `EXTEND_SHADOW`，除非所有 extended-shadow prerequisites 显式满足。
- Validation `PASS` 或 `PASS_WITH_WARNINGS` 且 failed checks=0。
- Reader Brief 暴露 board decision、blockers、warnings、safety boundary 和 next action。
- Focused tests、ruff、compileall、documentation contract、report index、Reader Brief quality
  和 git diff check 通过。

## 进展记录

|日期|状态|记录|
|---|---|---|
|2026-06-17|IN_PROGRESS|新增 requirements 和 task-register 行；准备用 latest report index 重跑 paper-shadow promotion board。|
|2026-06-17|DONE|收紧 promotion board decision gate：`EXTEND_SHADOW` 只有在所有 required extension evidence 均为 `PASS` 且 owner review 明确请求 extended shadow 时才允许输出；validator 新增同一 invariant，focused regression 覆盖 adverse cost/benchmark blocker、monthly/health source inclusion、warning prerequisite 下不 extension、all-pass owner extension 四种场景。真实 rerun `outputs/reports/paper_shadow_promotion_board_2026-06-17.json/md` 输出 `HOLD_FOR_MORE_DATA`、checks=11、blocked=3、warnings=5、safety=`SAFETY_PASS_WITH_WARNINGS`、readiness=`MANUAL_REVIEW_REQUIRED`、owner decision audit=`AUDIT_LOG_PASS`；blockers 为 monthly review `MONTHLY_REVIEW_BLOCKED`、cost sensitivity `NOT_MEANINGFUL_UNDER_COSTS` 和 benchmark baseline `CANDIDATE_UNDERPERFORMS_BASELINES`；warnings 为 paper-shadow health / weekly review / readiness `MANUAL_REVIEW_REQUIRED`、safety `SAFETY_PASS_WITH_WARNINGS` 和 owner review `monitor`。Validation `outputs/reports/paper_shadow_promotion_board_validation_2026-06-17.json/md` 输出 `PASS_WITH_WARNINGS`、checks=9、failed=0、source_blockers=3、source_warnings=5。Focused tests、ruff、compileall、report index、latest Reader Brief quality、report quality gate、documentation contract、data quality gate 和 git diff check 通过。同日 `reader-brief --as-of 2026-06-17` 被缺失 `decision_snapshot_2026-06-17.json` 阻断；未补造 snapshot，后续由 TRADING-399 decision snapshot lifecycle policy 处理。该 rerun 不授权 normal shadow、promotion、extended shadow、official target、broker/order、live trading 或 production mutation。|
