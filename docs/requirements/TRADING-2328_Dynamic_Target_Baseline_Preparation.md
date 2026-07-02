# TRADING-2328 Dynamic Target Baseline Preparation

最后更新：2026-07-02

## 状态

`VALIDATING`

## 背景

TRADING-2326 已用 `static_etf_allocation_baseline` 完成 source-bound exposure-cap dry-run。TRADING-2327 对该 static baseline dry-run 做 diagnostics review，真实结论为：

- `overall_recommendation=MOVE_TO_DYNAMIC_TARGET_BASELINE_PREPARATION`
- `next_task=TRADING-2328_Dynamic_Target_Baseline_Preparation`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`

static ETF baseline 只能回答 risk-cap 机制在固定 ETF 权重 proxy 下的约束成本和防守能力，不能回答它对系统自身 dynamic strategy target exposure 是否仍有边际价值。因此 TRADING-2328 只做 dynamic target baseline source preparation / audit / routing，为 TRADING-2329 是否能执行 dynamic baseline dry-run 做前置判断。

## 目标

新增命令：

```bash
aits research trends dynamic-target-baseline-preparation \
  --diagnostics-dir outputs/research_trends/exposure_cap_vs_no_cap_diagnostics_review \
  --static-dry-run-dir outputs/research_trends/source_bound_exposure_cap_dry_run_static_etf_baseline \
  --baseline-decision-dir outputs/research_trends/portfolio_baseline_source_decision \
  --source-binding-dir outputs/research_trends/exposure_cap_simulation_source_binding \
  --simulation-policy-dir outputs/research_trends/exposure_cap_mechanics_simulation \
  --candidate-artifact-roots outputs/research_trends,paper_portfolio,outputs \
  --output-dir outputs/research_trends/dynamic_target_baseline_preparation \
  --mode dynamic_target_baseline_preparation
```

命令必须：

1. fail-closed 校验 TRADING-2327 已推荐进入 dynamic baseline preparation。
2. 读取 TRADING-2326 static ETF dry-run、TRADING-2325 baseline decision、TRADING-2324 source binding 和 TRADING-2323 policy/readiness context。
3. 扫描 dynamic strategy target exposure / paper advisory / daily advisory / ETF allocation / risk budget / manual reference 候选 artifacts。
4. 生成 source inventory、gap matrix、PIT/replayability audit、field coverage matrix、risk-cap alignment readiness、market-data alignment readiness、candidate matrix、recommended spec、2329 readiness matrix、2329 route 和 safety boundary。
5. 在 dynamic target source 缺失或不可 replay 时输出 remediation/schema-adapter route，而不是伪造 baseline。

## 非目标

- 不执行 dynamic baseline exposure-cap simulation。
- 不重新执行 static ETF dry-run。
- 不修改 risk-cap trigger series、exposure-cap policy、cooldown/decay policy 或 dynamic strategy。
- 不生成新的 strategy target exposure、target weight 指令、rebalance instruction、buy/sell signal、paper-shadow、production 或 broker action。
- 不读取真实 broker account 或真实持仓作为 baseline。
- 不改变 TRADING-2327 diagnostics 结论。

## 输入和门禁

必需输入：

- TRADING-2327 diagnostics review outputs。
- TRADING-2326 static ETF dry-run outputs。
- TRADING-2325 baseline source decision outputs。
- TRADING-2324 source binding context。
- TRADING-2323 exposure-cap mechanics simulation policy/readiness artifacts。

以下情况必须 fail closed：

- 2327 summary/route 缺失。
- 2327 route 不是 `TRADING-2328_Dynamic_Target_Baseline_Preparation`。
- 2327 recommendation 不是 `MOVE_TO_DYNAMIC_TARGET_BASELINE_PREPARATION`。
- 2326/2325/2324/2323 required artifact 缺失。
- 任一上游 artifact 打开 promotion、paper-shadow、production 或 broker action。

dynamic target exposure artifact 缺失不是命令失败条件；应输出 `DYNAMIC_BASELINE_SOURCE_REMEDIATION_REQUIRED` 或 schema-adapter route。

## 数据质量政策

本任务只读取 prior research outputs、static config 和 registry/candidate artifacts，用于 source preparation。默认：

- `data_quality_status=NOT_APPLICABLE_SOURCE_PREPARATION_ONLY`
- `data_quality_gate_required=false`
- `data_quality_gate_executed=false`
- `aits_validate_data_executed=false`

若未来实现开始读取 market data cache 或 runtime dynamic target exposure data，必须先运行 `aits validate-data --as-of 2026-06-29` 或同源 validation code path。

## 输出

Runtime outputs 写入 `outputs/research_trends/dynamic_target_baseline_preparation/`：

- `dynamic_target_baseline_preparation_summary.json`
- `dynamic_target_source_inventory.json/.csv`
- `dynamic_target_source_gap_matrix.json/.csv`
- `dynamic_target_pit_replayability_audit.json/.csv`
- `dynamic_target_field_coverage_matrix.json/.csv`
- `dynamic_target_risk_cap_alignment_readiness.json/.csv`
- `dynamic_target_market_data_alignment_readiness.json/.csv`
- `dynamic_target_baseline_candidate_matrix.json/.csv`
- `recommended_dynamic_target_baseline_spec.json`
- `dynamic_target_baseline_2329_readiness_matrix.json`
- `dynamic_target_baseline_2329_task_route.json`
- `dynamic_target_baseline_safety_boundary.json`

Research docs：

- `docs/research/dynamic_target_baseline_preparation_report.md`
- `docs/research/dynamic_target_source_inventory.md`
- `docs/research/dynamic_target_pit_replayability_audit.md`
- `docs/research/dynamic_target_risk_cap_alignment_readiness.md`
- `docs/research/recommended_dynamic_target_baseline_spec.md`

## 安全边界

所有 outputs 必须保持：

- `research_only=true`
- `source_preparation_only=true`
- `simulation_executed=false`
- `portfolio_effect=none`
- `production_effect=none`
- `broker_action=none`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `manual_review_only=true`

不得输出 trading instruction、rebalance instruction、buy/sell signal、paper-shadow-ready、production-ready 或 broker-ready 结论。

## 实施步骤

1. 新增 `src/ai_trading_system/dynamic_target_baseline_preparation.py`。
2. 在 `src/ai_trading_system/cli_commands/research_trends.py` 增加 CLI command。
3. 补充 focused tests 覆盖 loader、inventory、gap、PIT audit、field coverage、alignment、recommendation 和 CLI。
4. 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md`、`docs/task_register.md`。
5. 运行真实 CLI，记录 readiness/route。
6. 运行 focused parallel pytest、Ruff、compileall、docs freshness、documentation contract、task-register consistency、contract-validation、full validation 和 `git diff --check`。

## 验收标准

- CLI 能生成全部 2328 runtime artifacts 和 research docs。
- 缺少 dynamic target source 时不失败，但必须生成 blocking source gap 和 2329 remediation route。
- 可用 strict/PIT source fixture 能进入 `DYNAMIC_BASELINE_READY_FOR_2329`。
- schema 可适配但缺字段的 source fixture 能进入 schema-adapter route。
- 所有输出固定 promotion/paper-shadow/production/broker false/none，且 `simulation_executed=false`。
- 本任务提交不包含既有无关 research 文档改动。

## 进展记录

- 2026-07-02：根据 owner 附件新增任务并开始实现。当前 worktree 已有两个无关 research 文档未提交改动；TRADING-2328 必须 selective staging，不能混入无关改动。
- 2026-07-02：实现完成并进入 `VALIDATING`。真实 run status=`DYNAMIC_TARGET_BASELINE_PREPARATION_READY_PROMOTION_BLOCKED`，available_source_count=`34`，inventory_source_count=`34`，pit_ready_source_count=`0`，blocking_gap_count=`136`，recommended_candidate_count=`0`，readiness_status=`DYNAMIC_BASELINE_SOURCE_REMEDIATION_REQUIRED`，next_task=`TRADING-2329_Dynamic_Target_Baseline_Source_Remediation`。验证通过 focused parallel pytest 19 passed、Ruff、compileall、真实 CLI run、docs freshness、documentation contract、task-register consistency、contract-validation 和 full validation；`aits validate-data` 不适用，因为本任务只读取 prior research outputs、static config 和 registry/candidate artifacts，不读取 cached market data 或 runtime dynamic target data。
