# TRADING-351 Paper Shadow Daily Runner

最后更新：2026-06-15

## 1. 背景

TRADING-350 已定义 observation-only paper-shadow protocol，但还没有每日 runner 来记录候选
方法在指定日期的 paper-shadow-only 行为。TRADING-351 补齐 daily observation record，同时
保持 no official target weights / no broker / no production。

## 2. 目标

1. 新增 daily paper-shadow observation CLI。
2. 输入：
   - candidate id
   - date
   - market panel artifact
   - latest signal artifact
   - research method contract
3. 输出 daily paper-shadow JSON 和 Reader Brief section。
4. 输出显式 safety metadata：
   - `production_effect=none`
   - `broker_effect=none`
   - `order_effect=none`
   - `manual_review_only=true`
5. 注册 artifact family。
6. 新增 focused tests。

## 3. 非目标

- 不初始化或修改 paper account state。
- 不生成 official target weights。
- 不生成 broker order ticket。
- 不运行真实交易、回测、stress backfill 或 signal recomputation。
- 不刷新 market panel 或 signal artifact；输入必须显式传入并记录 checksum/provenance。

## 4. Artifact Contract

Runtime root:

- `reports/etf_portfolio/dynamic_v3_rescue/paper_shadow_daily/<observation_id>/`

Artifacts:

- `paper_shadow_daily_manifest.json`
- `paper_shadow_daily_observation.json`
- `paper_shadow_daily_report.md`
- `reader_brief_section.md`
- `paper_shadow_daily_validation.json/md`

Expected summary fields:

- `paper_shadow_daily_observation_id`
- `paper_shadow_daily_candidate`
- `paper_shadow_daily_date`
- `paper_shadow_daily_status`
- `paper_shadow_daily_signal_output`
- `paper_shadow_daily_risk_state`
- `paper_shadow_daily_next_action`
- `paper_shadow_daily_validation_status`

## 5. Input Discipline

The runner must fail closed if required input paths are missing. The daily record stores provider-neutral
input links, file sizes, and checksums where practical:

- market panel artifact path/checksum;
- signal artifact path/checksum;
- formal research method contract id/status;
- optional paper-shadow protocol id/status.

`hypothetical_weight_recommendation` is allowed only as a paper-shadow-only observation field and must
not be written to official target weight files.

## 6. Safety Boundary

All outputs are read-only and fixed to:

- `production_effect=none`
- `broker_effect=none`
- `order_effect=none`
- `manual_review_only=true`
- `paper_shadow_daily_only=true`
- `observation_only=true`
- `hypothetical_weight_paper_shadow_only=true`
- `not_official_target_weights=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `production_state_mutated=false`
- `paper_account_state_mutated=false`
- `automatic_candidate_promotion=false`
- `auto_apply=false`

## 7. 验收标准

- `paper-shadow-daily run/report` 可生成 daily JSON、Markdown report 和 Reader Brief section。
- `validate-paper-shadow-daily` 返回 PASS。
- Reader Brief 显示 observation id、candidate、date、status、signal output、risk state 和 next action。
- README、operations runbook、system flow、artifact catalog、report registry、requirements 和
  task register 同步更新。
- focused pytest、contract-validation suite、ruff、compileall、git diff check、documentation
  contract、report index 和 Reader Brief quality 通过。

## 8. 进展记录

- 2026-06-15：新增任务并进入 IN_PROGRESS；本阶段只实现 observation-only daily record
  contract、report/validation artifact 和 visibility，不初始化 paper account、不写 official
  target weights、不接 broker、不修改 production state。
- 2026-06-15：实现 `paper-shadow-daily run/report` 和
  `validate-paper-shadow-daily`，新增 artifact family、Reader Brief 摘要字段、report registry、
  artifact catalog、README、operations runbook、system flow 和 focused tests。已生成
  observation `paper-shadow-daily_c7945bcfdf91cd53`（2026-06-12）并通过 run/report/validate
  CLI，状态进入 VALIDATING。
- 2026-06-15：验证完成并归档 DONE。Focused pytest 7 passed；contract-validation suite
  35 passed / 26.84s；Ruff、compileall、documentation contract、report index、Reader Brief、
  Reader Brief quality 和 git diff check 通过。Report index 保持 `PASS_WITH_WARNINGS`，仅披露
  既有 missing/stale artifact visibility，不改变 TRADING-351 safety boundary。
