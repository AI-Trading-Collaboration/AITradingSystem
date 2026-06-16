# TRADING-373 Paper Shadow Outcome Attribution

最后更新：2026-06-16

状态：DONE

## 背景

Paper-shadow weekly review 已能汇总 daily observation、drift monitor、signal input completeness
和 coverage sufficiency，但 owner 仍需要知道 weekly outcome 主要由 data、signal、regime、
governance 还是人工 decision 驱动。TRADING-373 在 weekly review 后新增只读 attribution
层，解释 outcome drivers，而不改变 weekly decision。

## 目标

- 定义 paper-shadow outcome attribution schema。
- 对 weekly review outcome driver 分类：
  - market move
  - signal change
  - regime transition
  - data stale warning
  - fallback source used
  - signal input incompleteness
  - drift warning
  - weekly coverage warning
  - manual owner decision
- 输出 attribution confidence：`HIGH`、`MEDIUM`、`LOW`、`UNKNOWN`。
- 只读集成 latest paper-shadow weekly review，并可只读链接 paper-shadow health context。
- 新增 `paper-shadow-outcome-attribution run/report` 和
  `validate-paper-shadow-outcome-attribution` CLI。
- Reader Brief 展示 attribution status、dominant driver、active driver count、confidence 和 next action。
- 同步 report registry、artifact catalog、README、operations runbook、system flow、requirements、
  task register 和 focused tests。

## 非目标

- 不重算 weekly decision、signal、regime、drift、health、score、backtest 或收益。
- 不运行 data refresh、source repair、upstream paper-shadow daily/drift/weekly command。
- 不补造 missing weekly review、health、signal completeness、fallback 或 drift artifact。
- 不写 official target weights、candidate ledger decision、paper account、portfolio、broker/order 或 production state。
- 不把 attribution confidence 解释为 promotion approval。

## Artifact Contract

目录：`reports/etf_portfolio/dynamic_v3_rescue/paper_shadow_outcome_attribution/<attribution_id>/`

- `paper_shadow_outcome_attribution_manifest.json`
- `paper_shadow_outcome_attribution_report.json`
- `paper_shadow_outcome_attribution_report.md`
- `reader_brief_section.md`
- `paper_shadow_outcome_attribution_validation.json`
- `paper_shadow_outcome_attribution_validation.md`

所有输出固定：

- `research_only=true`
- `manual_review_only=true`
- `outcome_attribution_only=true`
- `read_only_attribution=true`
- `weekly_decision_mutated=false`
- `data_downloaded_by_attribution=false`
- `pipelines_executed_by_attribution=false`
- `official_target_weights=false`
- `not_official_target_weights=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `paper_account_state_mutated=false`
- `production_state_mutated=false`
- `automatic_candidate_promotion=false`
- `auto_apply=false`
- `production_effect=none`

## 验收标准

- CLI run/report/validate 可运行，真实当前链路能生成 attribution artifact 并 validate PASS。
- 九类 required driver 均有 attribution row，active/inactive/unknown 状态明确。
- Confidence 限定为 `HIGH|MEDIUM|LOW|UNKNOWN`，dominant driver selection 可审计。
- Missing weekly review 必须 fail closed，不能补造 source。
- Weekly review summary 和 optional health context 只读链接到 artifact。
- Reader Brief 只读 latest artifact；缺失时显示 `MISSING`，不能补造 attribution。
- Report registry、artifact catalog、README、operations runbook、system flow、requirements 和 task register 同步。
- Focused tests、CLI smoke、Ruff、compileall、documentation contract、report index、Reader Brief 和 git diff check 通过。

## 进展记录

- 2026-06-16：任务创建并进入实现；范围限定为 research-only attribution layer，不运行 backtest、不刷新数据、不补造 artifact、不接 broker、不修改 weekly decision / official target weights / paper account / production state。
- 2026-06-16：实现完成并转为 `DONE`。新增 policy config、outcome attribution module、`paper-shadow-outcome-attribution run/report` CLI、`validate-paper-shadow-outcome-attribution`、Reader Brief fields、report registry、artifact catalog、README、operations runbook、system flow 和 focused tests。真实 artifact `paper-shadow-outcome-attribution_d24c7700f3c5dada` 输出 `paper_shadow_outcome_attribution_status=OUTCOME_ATTRIBUTION_COMPLETE`、`candidate=median_plus_regime_mismatch_filter`、`weekly_review_id=paper-shadow-weekly-review_67b1b8ae09e18fab`、`weekly_decision=CONTINUE`、`dominant_driver=signal_input_incompleteness`、`dominant_confidence=HIGH`、`active_driver_count=5`、`unknown_driver_count=0`、`next_required_action=review_outcome_attribution_with_weekly_review`，validation `PASS` / failed=0。Focused pytest 13 passed，Ruff PASS，compileall PASS，documentation contract PASS，report index `PASS_WITH_EXPLICIT_WAIVERS` / unwaived=0，Reader Brief explicit 2026-06-16 report index 展示 `paper_shadow_outcome_*` fields，Reader Brief quality `LIMITED_READER_CONTEXT` / failed=0；LIMITED 原因是 2026-06-16 Reader Brief 显式复用 latest 2026-06-15 decision snapshot，不是 attribution artifact 缺失。保持 read-only / no weekly decision mutation / no data refresh / no upstream rerun / no official target / no broker / no paper account or production mutation。
