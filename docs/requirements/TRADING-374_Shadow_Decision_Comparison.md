# TRADING-374 Shadow Decision Comparison

最后更新：2026-06-16

状态：DONE

## 背景

Paper-shadow readiness、weekly review、health 和 outcome attribution 已能给出当前状态，
但 owner 仍需要知道当前 paper-shadow decision 相比上一期是改善、恶化、阻断还是恢复。
TRADING-374 新增只读 comparison layer，比较 current 和 previous decision state，而不改变
任何 upstream decision。

## 目标

- 读取 current 与 previous paper-shadow decision artifacts。
- 比较：
  - safe_to_continue_shadow
  - readiness status
  - weekly decision
  - drift severity
  - stale artifact list
  - missing artifact list
  - signal input completeness
  - fallback status
  - safety boundary status
- 输出 delta summary。
- 输出 decision change classification：`NO_CHANGE`、`IMPROVED`、`DETERIORATED`、
  `BLOCKED`、`RECOVERED`。
- 输出 `decision_changed`、`change_reason`、`previous_state`、`current_state` 和
  `recommended_owner_action`。
- 新增 `shadow-decision-comparison run/report` 和
  `validate-shadow-decision-comparison` CLI。
- Reader Brief 展示 comparison status、classification、changed fields、reason 和 owner action。
- 同步 report registry、artifact catalog、README、operations runbook、system flow、requirements、
  task register 和 focused tests。

## 非目标

- 不重算 readiness、weekly decision、drift、health、signal input、fallback、staleness 或收益。
- 不运行 data refresh、source repair 或 upstream paper-shadow commands。
- 不补造 missing current/previous artifacts。
- 不写 official target weights、candidate ledger decision、paper account、portfolio、broker/order 或 production state。
- 不把 improved/recovered classification 解释为 promotion approval。

## Artifact Contract

目录：`reports/etf_portfolio/dynamic_v3_rescue/shadow_decision_comparison/<comparison_id>/`

- `shadow_decision_comparison_manifest.json`
- `shadow_decision_comparison_report.json`
- `shadow_decision_comparison_report.md`
- `reader_brief_section.md`
- `shadow_decision_comparison_validation.json`
- `shadow_decision_comparison_validation.md`

所有输出固定：

- `research_only=true`
- `manual_review_only=true`
- `shadow_decision_comparison_only=true`
- `read_only_comparison=true`
- `decision_mutated=false`
- `data_downloaded_by_comparison=false`
- `pipelines_executed_by_comparison=false`
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

- CLI run/report/validate 可运行，真实当前链路能生成 comparison artifact 并 validate PASS。
- latest/previous resolver 能显式记录 source artifact ids 和 missing/insufficient-source blocker。
- Required fields 均有 previous/current/delta visibility。
- Classification 限定为 `NO_CHANGE|IMPROVED|DETERIORATED|BLOCKED|RECOVERED`。
- Missing current 或 previous source 必须 fail closed，不能补造 source。
- Reader Brief 只读 latest artifact；缺失时显示 `MISSING`，不能补造 comparison。
- Report registry、artifact catalog、README、operations runbook、system flow、requirements 和 task register 同步。
- Focused tests、CLI smoke、Ruff、compileall、documentation contract、report index、Reader Brief 和 git diff check 通过。

## 进展记录

- 2026-06-16：任务创建并进入实现；范围限定为 read-only decision comparison layer，不运行 backtest、不刷新数据、不补造 artifact、不接 broker、不修改 readiness / weekly decision / official target weights / paper account / production state。
- 2026-06-16：实现完成并转为 DONE；新增
  `src/ai_trading_system/etf_portfolio/dynamic_v3_shadow_decision_comparison.py`、
  `shadow-decision-comparison run/report`、`validate-shadow-decision-comparison`、
  Reader Brief fields、report registry、artifact catalog、README、operations runbook、
  system flow 和 focused tests。真实 artifact
  `shadow-decision-comparison_203b03460ab35985` 比较 current
  `shadow-continuation-readiness_0061ea84c77efcd8` 与 previous
  `shadow-continuation-readiness_e9cea6cc1a221007`，输出
  `shadow_decision_comparison_status=DECISION_COMPARISON_COMPLETE`、
  `decision_changed=False`、`change_classification=NO_CHANGE`、
  `change_reason=no tracked shadow decision fields changed`、previous/current state 均为
  `BLOCKED_STALE_DATA`、`recommended_owner_action=continue_existing_owner_review_cadence`；
  validation PASS / failed=0。验证通过 focused pytest 18 passed、Ruff、compileall、
  documentation contract PASS、report index `PASS_WITH_EXPLICIT_WAIVERS` / unwaived=0、
  Reader Brief `LIMITED_READER_CONTEXT` / failed=0；LIMITED 原因是显式复用 latest
  2026-06-15 decision snapshot，不是 comparison artifact 缺失。
