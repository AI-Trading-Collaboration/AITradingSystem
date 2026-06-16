# TRADING-359 Cost Sensitivity Review

最后更新：2026-06-16

## 背景

Paper-shadow candidate 目前已有 daily / drift / weekly / health 证据，但 candidate
improvement 仍可能被 turnover-driven transaction cost drag 吃掉。TRADING-359 需要一个
research-only cost-sensitivity review，把候选的 gross performance proxy、turnover 和
configured transaction-cost assumptions 放在同一个可审计 artifact 中，供 weekly review 和
后续 promotion board 使用。

## 目标

- 新增 configurable cost assumptions：`zero`、`low`、`medium`、`high`。
- 读取已有 paper-shadow weekly review 和可显式传入的 candidate metrics，不刷新或补造 source artifact。
- 对每个 cost scenario 输出 turnover、cost drag、gross performance proxy、net performance proxy 和
  meaningful improvement classification。
- 输出 aggregate `cost_sensitivity_status`、promotion-board input summary、Reader Brief section 和
  validation artifact。
- 新增 `cost-sensitivity-review run/report` 和 `validate-cost-sensitivity-review` CLI。
- 同步 report registry、artifact catalog、README、operations runbook、system flow、requirements、
  task register 和 focused tests。

## 非目标

- 不接 broker、paper broker、订单、成交回报或实时盘口。
- 不建立 production-ready execution model，不估计账户规模、税务、融资、ETF 成交延迟或市场冲击细节。
- 不刷新 market data、signal data、paper-shadow daily/drift/weekly/health artifacts。
- 不写 official target weights、candidate ledger decision、paper account、portfolio、broker/order 或 production state。
- 不把 cost review 解释为 promotion approval。

## Policy Contract

Cost assumptions 和 meaningful improvement threshold 是投资解释启发式，必须在
`config/etf_portfolio/dynamic_v3_rescue/cost_sensitivity_review_v1.yaml` 中披露：

- policy id / version / status / owner；
- rationale / intended effect / validation evidence / review condition；
- scenario id、display name、total_cost_bps、commission/slippage/spread/market-impact notes；
- meaningful improvement threshold；
- safety boundaries。

## Artifact Contract

目录：`reports/etf_portfolio/dynamic_v3_rescue/cost_sensitivity_review/<review_id>/`

- `cost_sensitivity_manifest.json`
- `cost_sensitivity_review.json`
- `cost_sensitivity_report.md`
- `reader_brief_section.md`
- `cost_sensitivity_validation.json`
- `cost_sensitivity_validation.md`

所有输出固定：

- `research_only=true`
- `manual_review_only=true`
- `cost_sensitivity_review_only=true`
- `execution_model_ready=false`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `paper_account_state_mutated=false`
- `production_state_mutated=false`
- `automatic_candidate_promotion=false`
- `official_target_weights=false`
- `not_official_target_weights=true`
- `auto_apply=false`
- `production_effect=none`

## 验收标准

- CLI run/report/validate 可运行，真实当前链路能生成 cost-sensitivity review 并 validate PASS。
- 四个 cost scenario 均输出 turnover、cost drag、gross/net performance proxy 和 meaningful classification。
- 成本假设和 meaningful threshold 从 policy config 读取并进入 artifact，不得硬编码为未说明阈值。
- Weekly review 能只读暴露 latest cost-sensitivity source summary；promotion-board input summary 在 artifact 中可读。
- Reader Brief 只读 latest artifact；缺失时显示 `MISSING`，不能补造 cost review。
- Report registry、artifact catalog、README、operations runbook、system flow、requirements 和 task register 同步。
- Focused tests、CLI smoke、Ruff、compileall、documentation contract、report index、Reader Brief 和 git diff check 通过。

## 进展记录

- 2026-06-16：任务创建并进入实现；范围限定为 research-only cost-sensitivity review，不接 broker、不补造执行假设、不刷新数据、不修改 official target weights / paper account / production state。
- 2026-06-16：实现完成并转为 `DONE`。真实 artifact `cost-sensitivity-review_2da1efa5169ea62b` 输出 `cost_sensitivity_status=INSUFFICIENT_COST_INPUTS`、`candidate=median_plus_regime_mismatch_filter`、policy `dynamic_v3_rescue_cost_sensitivity_review_v1 / 2026-06-16`、blocking reason `candidate_metrics:insufficient_cost_inputs`、warnings `paper_shadow_health:blocked_signal_inputs,candidate_metrics:limited_source`、next action `provide_numeric_turnover_and_performance_metrics_before_cost_review`，validation `PASS` / failed=0。当前 latest weekly review 不含 numeric turnover / performance metrics，因此不能给出真实 net improvement 结论；实现选择 fail closed，未补造 metrics、刷新数据或生成执行假设。Focused tests 覆盖 explicit numeric metrics 下 zero/low/medium/high scenario math、high-cost fragility、insufficient-input fail-closed 和 CLI run/report/validate；documentation contract、report index 和 Reader Brief latest 验证通过，Reader Brief JSON/HTML 已显示 `cost_sensitivity_*` 字段。
