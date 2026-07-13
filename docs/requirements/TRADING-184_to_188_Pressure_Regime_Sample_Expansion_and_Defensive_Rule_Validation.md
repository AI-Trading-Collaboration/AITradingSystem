# TRADING-184 to 188 Pressure Regime Sample Expansion and Defensive Rule Validation

## ARCH-004G2.4CE contract hardening

2026-07-13 进入`IN_PROGRESS`。本slice保留原15个CLI path/options/default/help/exit contract，但不沿用baseline浅验证作为当前结论依据：

- Diagnosis：Pressure Tag及其config必须在写件前通过content-derived consumer validation和cutoff，historical/simulation availability只能来自semantic-selected、完整性可验证的bounded source commitments；invalid source不得按0处理。
- Backfill：显式冻结Pressure Tag、全部被选中的forward outcome、zero-or-one historical backfill和simulation outcome。只接收cutoff内、identity唯一、`AVAILABLE`且finite的event/window/variant；simulation固定`SIMULATION_NOT_PIT/can_support_production=false`，historical固定PIT warning，缺variant或PENDING不计防御样本。
- Compare：绑定reviewed `sim_defensive_validation_v1`与`forward_pressure_capture_v1` policy；cohort必须以source/event/as-of/window/regime严格配对defensive与no-trade，distinct-event floor及return/drawdown边界来自policy；空样本指标为null/`INSUFFICIENT_DATA`，不得补0或用单event形成`PROVEN_DEFENSIVE`。
- Rule Review：重验same Compare和policy，决策只由冻结summary+policy推导；`rule_approval_allowed=false`、`auto_apply=false`始终保持，任何“support owner review”都不是policy approval。
- Weekly Update：Weekly、Backfill、Compare、Review必须全部content-derived PASS、cutoff内且满足Backfill→Compare→Review exact lineage/chronology；Pressure Tag仅在确实缺失时可标optional absence，存在但invalid必须FAIL。Weekly recommendation与next actions只从冻结source/policy重算。
- Validation：五类artifact分别写`pressure_tag_diagnosis_input_snapshot.v2`、`pressure_outcome_backfill_input_snapshot.v2`、`defensive_pressure_compare_input_snapshot.v2`、`defensive_rule_review_input_snapshot.v2`、`weekly_ops_decision_update_input_snapshot.v2`；validator校验snapshot commitments/live source/policy并逐byte重建所有JSON/JSONL/Markdown。Legacy unsnapshotted artifact只读warning，不参与当前结论。

退出要求：15 callback迁canonical且legacy root definition/import为0；focused覆盖invalid/future/duplicate/PENDING/non-finite/missing-null/sample-floor/lineage/chronology/tamper；CLI parity、Ruff、architecture、contract、research execution chain、system flow、runbook、registry/catalog、manifests/deprecation/attribution全部通过。仅生成manual research evidence，`production_effect=none`；完成后G2.4仍继续，不触发phase-level handoff。

最后更新：2026-07-13

## 背景

TRADING-179 到 TRADING-183 已建立 confirmation-cycle weekly operations、pressure-regime-tag、confirmation dashboard 和 rule-review queue。真实 pressure tag artifact `1bc4775787a6606d` 显示价格窗口中存在 `tech_drawdown`、`risk_off` 和 `semiconductor_pullback` 样本，但 `defensive_validation_relevant_outcomes=0`。

这会阻塞 `defensive_limited_adjustment_drawdown` 的判断：没有 pressure outcome 样本时，系统不能证明 `defensive_limited_adjustment` 在压力窗口里降低 drawdown，也不能把 simulation 中的整体表现自动解释成 defensive 规则可批准。

## 范围

|任务|名称|状态|验收重点|
|---|---|---|---|
|TRADING-184|Pressure Tagging Threshold Diagnosis|COMPLETE|解释 relevant outcomes 为 0 的主因，输出 threshold hit / near-miss / outcome mapping diagnostics，不自动改阈值。|
|TRADING-185|Pressure Outcome Backfill from Simulation / Replay|COMPLETE|从 forward、historical replay 和 backtest simulation 中回填 pressure outcome inventory，并明确 `FORWARD_OUTCOME` / `HISTORICAL_REPLAY` / `BACKTEST_SIMULATION` source mode。|
|TRADING-186|Defensive Variant Pressure-window Comparison|COMPLETE|基于 pressure outcome inventory 比较 `defensive_limited_adjustment`、`limited_adjustment`、`consensus_target` 和 `no_trade` 的压力窗口收益、drawdown、turnover。|
|TRADING-187|Defensive Rule Status Review|COMPLETE|生成 owner review checklist 和 decision matrix，默认 `rule_approval_allowed=false`、`auto_apply=false`。|
|TRADING-188|Weekly Operations Decision Update|COMPLETE|把 diagnosis / backfill / comparison / review 接回 weekly operations decision，保持 `policy_change_allowed=false`、`broker_action_allowed=false`。|

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
- 2026-07-13：G2.4CE完成15 callback canonical迁移与contract hardening。五类producer均为pre-output content validation、timezone-aware cutoff、semantic selection、bounded input snapshot与atomic output；validator重验live source/policy并逐byte重建全部views。Backfill只纳入`AVAILABLE`/finite/unique rows并同时计算distinct events；Compare绑定reviewed simulation/forward policy，以同event/window/regime pair计算return/drawdown/win-rate，任一configured regime未达到5 distinct events时source保持`INSUFFICIENT_DATA`。当前fixture为2 simulation events、0 forward/historical events，因此总体`NOT_PROVEN_DEFENSIVE`、Weekly=`continue_tracking`。2026-06-11旧真实116/87 rows artifact缺CE snapshot，只保留legacy历史观察，不能作为current conclusion。累计focused 590、CE及下游兼容回归20、architecture-fitness 266、contract-validation 203均parallel PASS；generated 895 modules/1,114 tests/858 writers/0 violations。详细输入输出、公式、日期窗口解释与优化进入条件见`docs/research/current_research_strategy_execution_chain.md` 7.14。
