# TRADING-353 Paper Shadow Weekly Review

最后更新：2026-06-15

## 1. 背景

TRADING-351 已生成单日 paper-shadow observation，TRADING-352 已生成 read-only drift
monitor。下一步需要把一周内的 daily observations 和 drift monitors 聚合成人工周度复核层，
让 owner 判断是否继续 shadow、观察、退回 research 或拒绝 candidate。

## 2. 目标

1. 新增 paper-shadow weekly review CLI。
2. 输入 candidate id、week start/end、daily paper-shadow artifacts、drift monitor artifacts、
   formal research method contract 和 candidate decision ledger。
3. 输出 weekly review JSON、Markdown report、Reader Brief section 和 validation artifact。
4. 周度摘要覆盖 signal stability、hypothetical paper-shadow recommendation stability、
   turnover behavior、drawdown behavior、flip/rotation behavior、drift severity trend、
   benchmark comparison proxy、missing input artifacts 和 reviewer notes placeholder。
5. 输出 weekly decision：`CONTINUE`、`WATCH`、`RETURN_TO_RESEARCH` 或 `REJECT`。
6. 明确 safety metadata：`production_effect=none`、`broker_effect=none`、
   `order_effect=none`、`official_target_weights=false`、`manual_review_only=true`。

## 3. 非目标

- 不刷新 market data。
- 不运行 paper-shadow daily runner 或 drift monitor 上游。
- 不修改 candidate decision ledger。
- 不生成 official target weights、order ticket、broker action 或 production mutation。
- 不自动 approve、promote、reject 或切换 candidate。

## 4. Artifact Contract

Runtime root:

- `reports/etf_portfolio/dynamic_v3_rescue/paper_shadow_weekly_review/<weekly_review_id>/`

Artifacts:

- `paper_shadow_weekly_manifest.json`
- `paper_shadow_weekly_review.json`
- `paper_shadow_weekly_report.md`
- `reader_brief_section.md`
- `paper_shadow_weekly_validation.json/md`

Reader Brief fields:

- `paper_shadow_weekly_review_id`
- `paper_shadow_weekly_candidate`
- `paper_shadow_weekly_window`
- `paper_shadow_weekly_decision`
- `paper_shadow_weekly_missing_inputs`
- `paper_shadow_weekly_drift_trend`
- `paper_shadow_weekly_validation_status`

## 5. Safety Boundary

All outputs are read-only and fixed to:

- `production_effect=none`
- `broker_effect=none`
- `order_effect=none`
- `official_target_weights=false`
- `manual_review_only=true`
- `paper_shadow_weekly_review_only=true`
- `data_downloaded_by_review=false`
- `pipelines_executed_by_review=false`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `paper_account_state_mutated=false`
- `production_state_mutated=false`
- `automatic_candidate_promotion=false`
- `auto_apply=false`

## 6. 验收标准

- `paper-shadow-weekly-review build/report` 可读取 TRADING-351 daily runner 和 TRADING-352
  drift monitor artifacts。
- `validate-paper-shadow-weekly-review` 返回 PASS，并阻断缺失 source、非法 decision 或 unsafe
  payload。
- Missing inputs 必须显式记录，不能静默忽略。
- report registry、artifact catalog、README、operations runbook、system flow、task register 和
  Reader Brief 同步。
- focused pytest、documentation contract、report index、Reader Brief quality、ruff、compileall 和
  git diff check 通过。

## 7. 进展记录

- 2026-06-15：新增任务并进入 IN_PROGRESS；本阶段实现 paper-shadow weekly review
  aggregation，不运行上游、不刷新数据、不改变 strategy 或 production state。
- 2026-06-15：实现完成并转 DONE；新增 `paper-shadow-weekly-review build/report`
  和 `validate-paper-shadow-weekly-review`，输出 weekly manifest/review/report、Reader
  Brief section 和 validation artifact；Reader Brief、report registry、artifact catalog、
  README、system flow、operations runbook、task register 同步。Focused paper-shadow tests
  10 passed，combined validation 34 passed，documentation contract PASS，report index
  `PASS_WITH_EXPLICIT_WAIVERS` / `unwaived=0`，Reader Brief OK，Reader Brief quality OK，
  ruff、compileall 和 git diff check 通过；保持 manual weekly aggregation / read-only /
  no data refresh / no upstream rerun / no ledger mutation / no official target / no broker /
  no production。
