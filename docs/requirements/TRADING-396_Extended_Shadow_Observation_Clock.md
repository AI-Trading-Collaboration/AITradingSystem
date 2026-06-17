# TRADING-396 Extended Shadow Observation Clock

最后更新：2026-06-17

## 背景

TRADING-381 extended shadow protocol 目前在 protocol 内部从既有 weekly evidence 推断
observed/minimum days。附件要求把 extended-shadow observation eligibility 拆成明确 artifact，
跟踪 `0/20` minimum observation period blocker，并供后续 protocol rerun 消费。

## 范围

- 新增只读 observation clock report：
  - `aits reports extended-shadow-observation-clock --as-of YYYY-MM-DD`
  - `aits reports validate-extended-shadow-observation-clock --latest`
- Clock 跟踪 observation start date、complete observation trading days、missing days、invalid
  days、invalid reason、current count 和 required count。
- Status 限定为：
  - `OBSERVATION_PERIOD_UNMET`
  - `OBSERVATION_PERIOD_PARTIAL`
  - `OBSERVATION_PERIOD_MET`
- Extended shadow protocol 必须读取 clock；clock 未满足时 protocol 继续 blocked。
- 重建 report index 和 Reader Brief latest validation。

## 安全边界

- 不运行 upstream paper-shadow observation。
- 不补造 observation days。
- 不修改 paper account、candidate state、paper-shadow state 或 production state。
- 不批准 extended shadow、live trading、official target weights、broker action 或 order ticket。
- 缺 observation source 时 fail closed 到 `OBSERVATION_PERIOD_UNMET`。

## 验收标准

- Observation clock report 和 validation artifact 生成。
- 当前真实状态必须披露 `0/20` 或可审计来源的实际 count，并保持未满足状态。
- Extended shadow protocol 使用 clock source，而不是仅靠内部推断。
- Validation `PASS` 或 `PASS_WITH_WARNINGS` 且 failed checks=0。
- Reader Brief section、report registry、artifact catalog、operations runbook、system flow 和
  focused tests 更新。
- Focused tests、ruff、compileall、documentation contract、report index、Reader Brief quality、
  data quality gate 和 git diff check 通过。

## 进展记录

|日期|状态|记录|
|---|---|---|
|2026-06-17|IN_PROGRESS|新增 requirements 和 task-register 行；准备实现 extended-shadow observation clock，并把 extended shadow protocol 接到该 clock。|
|2026-06-17|DONE|新增 `extended_shadow_observation_clock` report / validation、Reader Brief summary、report registry、artifact catalog、README、operations runbook、system flow 和 focused tests。真实输出 `outputs/reports/extended_shadow_observation_clock_2026-06-17.json/md` 为 `OBSERVATION_PERIOD_UNMET`，candidate=`median_plus_regime_mismatch_filter`，current=`0`，required=`20`，missing=`20`，invalid reasons=`minimum_observation_period_unmet_0_of_20` / `no_valid_extended_shadow_observation_days_found`。Validation `outputs/reports/extended_shadow_observation_clock_validation_2026-06-17.json/md` 为 `PASS_WITH_WARNINGS`、failed=0；extended shadow protocol 已改为消费 clock，rerun `outputs/reports/extended_shadow_protocol_2026-06-17.json/md` 输出 `EXTENDED_SHADOW_BLOCKED`、observed_days=`0`、minimum_days=`20`，validation `PASS_WITH_WARNINGS`、failed=0。Focused tests 11 passed，ruff passed；该任务不运行上游、不补造 observation days、不批准 extended shadow/live/official target/broker/order 或 production mutation。|
