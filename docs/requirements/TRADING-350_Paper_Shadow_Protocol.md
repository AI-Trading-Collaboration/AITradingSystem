# TRADING-350 Paper Shadow Protocol

最后更新：2026-06-15

## 1. 背景

TRADING-346 formal research method contract 当前输出
`paper_shadow_eligibility=ELIGIBLE_FOR_PROTOCOL_DESIGN`，但还没有独立协议说明进入
paper-shadow 前要观察什么、如何退出、哪些字段必须每日记录，以及如何确认该协议没有被
broker/order 系统消费。TRADING-350 只定义 protocol，不运行 daily paper-shadow runner。

## 2. 目标

1. 新增 `docs/paper_shadow_protocol.md`。
2. 定义 paper-shadow eligibility 条件。
3. 定义 required observation period。
4. 定义 daily review fields：
   `signal_output`、`hypothetical_weight_recommendation`、`risk_off_risk_on_state`、
   `drawdown_state`、`rotation_event`、`mismatch_event`、`benchmark_comparison`、
   `manual_reviewer_notes`。
5. 定义 exit conditions：`promote_to_extended_paper_shadow`、`return_to_research`、
   `reject`。
6. 新增 validate CLI、report CLI、Reader Brief section、artifact family 和 focused tests。
7. 保持 observation-only / no broker / no production / no official target weights。

## 3. 非目标

- 不运行 daily paper-shadow observation；TRADING-351 才负责 daily runner。
- 不初始化或修改既有 paper shadow account。
- 不输出 official target weights；hypothetical weight 只能是 paper-shadow-only 字段。
- 不接 broker、不生成 order ticket、不修改 portfolio、baseline 或 production state。

## 4. Protocol Policy

本阶段使用 `PAPER_SHADOW_REQUIRED_OBSERVATION_DAYS=20` 作为 pilot baseline。它只要求
后续 observation protocol 至少积累一个约 1 个月交易窗口的每日记录，不能作为 production
approval 条件。后续如果进入 extended paper-shadow 或 promotion milestone，应由单独任务把该
样本期迁移到 reviewed config/policy manifest 并用实际 observation evidence 校准。

## 5. Artifacts

`aits etf dynamic-v3-rescue paper-shadow-protocol build` 输出：

- `paper_shadow_protocol_manifest.json`
- `paper_shadow_protocol.json`
- `paper_shadow_protocol_report.md`
- `reader_brief_section.md`
- `paper_shadow_protocol_validation.json/md`

所有 artifact 必须披露：

- source formal research method contract；
- protocol status；
- eligibility conditions；
- required observation period；
- daily review fields；
- exit conditions；
- safety boundary：`production_effect=none`、`not_official_target_weights=true`、
  `broker_action_allowed=false`、`order_ticket_generated=false`、`manual_review_only=true`。

## 6. 验收标准

- `paper-shadow-protocol build/report` 可运行。
- `validate-paper-shadow-protocol` 返回 PASS。
- Reader Brief summary 能显示 protocol id、status、eligibility、observation period 和 next action。
- README、operations runbook、system flow、artifact catalog、report registry、task register
  和本文同步更新。
- focused pytest、ruff、compileall、git diff check、documentation contract 和 report index 通过。

## 7. 进展记录

- 2026-06-15：新增任务并进入 IN_PROGRESS；本阶段只定义 protocol/governance artifact，
  不实现 daily runner、不写 target weights、不接 broker。
- 2026-06-15：实现完成并进入 VALIDATING。新增 `docs/paper_shadow_protocol.md`、
  `paper-shadow-protocol build/report`、`validate-paper-shadow-protocol`、protocol manifest /
  JSON / report / Reader Brief / validation artifact、report registry、artifact catalog、system
  flow、operations runbook、README、Reader Brief summary fields 和 focused tests。真实链路生成
  `paper-shadow-protocol_8768e553832aa45d`，`protocol_status=PROTOCOL_READY`、
  `eligibility_status=ELIGIBLE`、`minimum_observation_trading_days=20`、
  `next_required_action=start_daily_paper_shadow_runner_design`，validator `PASS` / failed=0。
  Reader Brief JSON 已显示 protocol fields；focused pytest 3 passed，contract-validation suite
  20 passed / 16.83 秒，documentation contract PASS，report index `PASS_WITH_WARNINGS` 仅保留
  既有 missing/stale visibility，Reader Brief OK，Reader Brief quality OK。仍固定
  observation-only / no official target / no broker / no order / no production。
