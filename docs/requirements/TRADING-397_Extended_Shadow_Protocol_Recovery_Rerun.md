# TRADING-397 Extended Shadow Protocol Recovery Rerun

最后更新：2026-06-17

## 背景

TRADING-396 已把 extended-shadow minimum observation period 拆成独立
`extended_shadow_observation_clock` artifact。附件要求在恢复证据和 observation clock 更新后
重跑 extended shadow protocol，并确保 protocol 继续 fail closed，不允许 live trading、
official target weights、broker action、order ticket 或 production mutation。

## 范围

- 更新 `aits reports extended-shadow-protocol --as-of YYYY-MM-DD` 的 recovery rerun
  status vocabulary。
- Protocol 输入包括 promotion board、readiness、safety audit、cost sensitivity、
  benchmark comparison、owner decision audit log、artifact lineage graph 和 observation clock。
- 支持 status：
  - `EXTENDED_SHADOW_BLOCKED`
  - `EXTENDED_SHADOW_NOT_READY`
  - `EXTENDED_SHADOW_REVIEW_REQUIRED`
  - `EXTENDED_SHADOW_ELIGIBLE`
- 当前真实 rerun 预期仍保持 blocked，因为 observation period 为 `0/20`，且 promotion /
  safety / cost / benchmark / owner review evidence 仍有 blocker 或 warning。

## 安全边界

- 不运行 upstream recovery、paper-shadow observation、promotion board 或 data refresh。
- 不补造 observation days、owner decision、cost metrics、benchmark metrics 或 safety clearance。
- 不允许 live trading、official target weights、broker action、order ticket、automatic
  position control 或 production mutation。
- `EXTENDED_SHADOW_ELIGIBLE` 也只表示 owner 可人工复核 extended paper-shadow plan，不是
  live trading approval。

## 验收标准

- Extended shadow protocol 使用 observation clock 的 current/required count。
- Observation-only gap 输出 `EXTENDED_SHADOW_NOT_READY`。
- Warning-only evidence 输出 `EXTENDED_SHADOW_REVIEW_REQUIRED`。
- Hard blocker evidence 输出 `EXTENDED_SHADOW_BLOCKED`。
- Full pass 输出 `EXTENDED_SHADOW_ELIGIBLE`。
- Real rerun `2026-06-17` 保持 blocked，且 validation `PASS` 或 `PASS_WITH_WARNINGS`、
  failed checks=0。
- Reader Brief、README、artifact catalog、operations runbook、system flow、task register 和
  focused tests 更新。
- Focused tests、ruff、compileall、documentation contract、report index、Reader Brief quality、
  data quality gate 和 git diff check 通过。

## 进展记录

|日期|状态|记录|
|---|---|---|
|2026-06-17|IN_PROGRESS|新增 requirements 和 task-register 行；准备更新 protocol recovery status classification，并重跑 2026-06-17 extended shadow protocol。|
|2026-06-17|DONE|Protocol recovery status taxonomy 已扩展为 `EXTENDED_SHADOW_BLOCKED|EXTENDED_SHADOW_NOT_READY|EXTENDED_SHADOW_REVIEW_REQUIRED|EXTENDED_SHADOW_ELIGIBLE`，并收紧 benchmark underperformance 为 hard blocker。真实 rerun `outputs/reports/extended_shadow_protocol_2026-06-17.json/md` 输出 `EXTENDED_SHADOW_BLOCKED`、candidate=`median_plus_regime_mismatch_filter`、observed_days=`0`、minimum_days=`20`、blocked=6、warnings=2。Blockers: promotion board `HOLD_FOR_MORE_DATA`、safety audit `SAFETY_PASS_WITH_WARNINGS`、cost sensitivity `NOT_MEANINGFUL_UNDER_COSTS`、benchmark comparison `CANDIDATE_UNDERPERFORMS_BASELINES`、observation clock `OBSERVATION_PERIOD_UNMET`、minimum observation period `0/20`。Warnings: weekly review / readiness `MANUAL_REVIEW_REQUIRED`。Validation `outputs/reports/extended_shadow_protocol_validation_2026-06-17.json/md` 输出 `PASS_WITH_WARNINGS`、failed=0、source_blockers=6、source_warnings=2。Focused tests 14 passed，ruff passed；该 rerun 不运行上游、不补造 evidence、不批准 extended shadow/live/official target/broker/order 或 production mutation。|
