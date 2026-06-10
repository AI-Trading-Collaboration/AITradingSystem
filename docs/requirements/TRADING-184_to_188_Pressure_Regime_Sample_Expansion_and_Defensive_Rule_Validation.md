# TRADING-184 to 188 Pressure Regime Sample Expansion and Defensive Rule Validation

最后更新：2026-06-11

## 背景

TRADING-179 到 TRADING-183 已建立 confirmation-cycle weekly operations、pressure-regime-tag、confirmation dashboard 和 rule-review queue。真实 pressure tag artifact `1bc4775787a6606d` 显示价格窗口中存在 `tech_drawdown`、`risk_off` 和 `semiconductor_pullback` 样本，但 `defensive_validation_relevant_outcomes=0`。

这会阻塞 `defensive_limited_adjustment_drawdown` 的判断：没有 pressure outcome 样本时，系统不能证明 `defensive_limited_adjustment` 在压力窗口里降低 drawdown，也不能把 simulation 中的整体表现自动解释成 defensive 规则可批准。

## 范围

|任务|名称|状态|验收重点|
|---|---|---|---|
|TRADING-184|Pressure Tagging Threshold Diagnosis|VALIDATING|解释 relevant outcomes 为 0 的主因，输出 threshold hit / near-miss / outcome mapping diagnostics，不自动改阈值。|
|TRADING-185|Pressure Outcome Backfill from Simulation / Replay|VALIDATING|从 forward、historical replay 和 backtest simulation 中回填 pressure outcome inventory，并明确 `FORWARD_OUTCOME` / `HISTORICAL_REPLAY` / `BACKTEST_SIMULATION` source mode。|
|TRADING-186|Defensive Variant Pressure-window Comparison|VALIDATING|基于 pressure outcome inventory 比较 `defensive_limited_adjustment`、`limited_adjustment`、`consensus_target` 和 `no_trade` 的压力窗口收益、drawdown、turnover。|
|TRADING-187|Defensive Rule Status Review|VALIDATING|生成 owner review checklist 和 decision matrix，默认 `rule_approval_allowed=false`、`auto_apply=false`。|
|TRADING-188|Weekly Operations Decision Update|VALIDATING|把 diagnosis / backfill / comparison / review 接回 weekly operations decision，保持 `policy_change_allowed=false`、`broker_action_allowed=false`。|

## 设计决策

- `pressure-regime-tag` 的 forward outcome mapping 与 backtest simulation pressure evidence 分开处理，避免把 simulation 样本伪装成 forward confirmation。
- `BACKTEST_SIMULATION` 样本只能作为 research evidence，统一标记 `SIMULATION_NOT_PIT` 和 `can_support_production=false`。
- `HISTORICAL_REPLAY` 样本最多作为中等证据，必须保留 PIT 状态；当前样本不足或 pending 时不得提升为 production evidence。
- `defensive_limited_adjustment` 只有在高质量 forward 或 PIT-safe replay pressure evidence 足够时，才可能进入 rule approval；本阶段默认不批准。
- 所有新增 artifacts 固定 `production_effect=none`、`broker_action_allowed=false`、`policy_change_allowed=false`、`auto_apply=false`，不得修改 `position_advisory_v1.yaml`、official target weights、portfolio、broker state 或 production policy。

## 目标 CLI

```bash
aits etf dynamic-v3-rescue pressure-tag-diagnosis run --tag-id <tag_id>
aits etf dynamic-v3-rescue pressure-tag-diagnosis report --latest
aits etf dynamic-v3-rescue validate-pressure-tag-diagnosis --diagnosis-id <diagnosis_id>

aits etf dynamic-v3-rescue pressure-outcome-backfill run --start 2022-12-01 --end YYYY-MM-DD
aits etf dynamic-v3-rescue pressure-outcome-backfill report --latest
aits etf dynamic-v3-rescue validate-pressure-outcome-backfill --backfill-id <pressure_backfill_id>

aits etf dynamic-v3-rescue defensive-pressure-compare run --pressure-backfill-id <pressure_backfill_id>
aits etf dynamic-v3-rescue defensive-pressure-compare report --latest
aits etf dynamic-v3-rescue validate-defensive-pressure-compare --comparison-id <comparison_id>

aits etf dynamic-v3-rescue defensive-rule-review run --comparison-id <comparison_id>
aits etf dynamic-v3-rescue defensive-rule-review report --latest
aits etf dynamic-v3-rescue validate-defensive-rule-review --review-id <review_id>

aits etf dynamic-v3-rescue weekly-ops-decision-update run --weekly-cycle-id <weekly_cycle_id> --pressure-backfill-id <pressure_backfill_id> --defensive-review-id <review_id>
aits etf dynamic-v3-rescue weekly-ops-decision-update report --latest
aits etf dynamic-v3-rescue validate-weekly-ops-decision-update --decision-update-id <decision_update_id>
```

## Artifact 计划

- `reports/etf_portfolio/dynamic_v3_rescue/pressure_tag_diagnosis/<diagnosis_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/pressure_outcome_backfill/<pressure_backfill_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/defensive_pressure_compare/<comparison_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/defensive_rule_review/<review_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/weekly_ops_decision_update/<decision_update_id>/`

每个 artifact 必须有 manifest、machine-readable details、Markdown report、latest pointer 和 validate CLI。

## 验收

- 新增五组 run/report/validate CLI 可运行。
- `pressure tag diagnosis` 能解释 `defensive_validation_relevant_outcomes=0` 的主因，并区分 threshold、near-miss、mapping 和未扫描 simulation/replay 的影响。
- `pressure outcome backfill` 能从已有 simulation artifacts 提取 pressure samples，且 source mode 与 evidence quality 标记正确。
- `defensive pressure compare` 明确区分 simulation 与 forward evidence，默认不能支持 rule approval。
- `defensive rule review` 生成 owner checklist，`rule_approval_allowed=false`、`auto_apply=false`。
- `weekly ops decision update` 输出 `continue_tracking`、`policy_change_allowed=false`、`broker_action_allowed=false` 和 next actions。
- README、operations runbook、system flow、report registry、artifact catalog、Reader Brief 和 task register 同步更新。
- Focused tests、`ruff check src tests`、`compileall src tests`、`git diff --check` 和相关 validation CLI 通过。

## 进展记录

- 2026-06-11：新增需求文档和任务登记，进入 IN_PROGRESS。实现前已确认现有 `pressure-regime-tag` 找到 pressure windows，但 forward outcome 未映射到 pressure window；现有 backtest simulation outcome 已含 pressure regime labels，但不属于 forward evidence。
- 2026-06-11：baseline 实现完成并进入 VALIDATING。真实链路 artifacts：
  diagnosis `ca86a051c36a9e14`、pressure backfill `af2d09c3b5aabc6e`、defensive comparison `6d3ea9b43618c7db`、rule review `1eee7bc87fd3acab`、weekly ops decision update `5d43a54bbe2d8a5f`。
  当前结论为 forward pressure outcome=0、historical replay pressure outcome=0、backtest simulation pressure outcomes=116、defensive validation relevant=87；
  `defensive_limited_adjustment` 只能保持 `RESEARCH_ONLY` / `continue_tracking`，`rule_approval_allowed=false`、`policy_change_allowed=false`、`broker_action_allowed=false`。
