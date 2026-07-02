# TRADING-2331 Dynamic Target Baseline Dry-Run Readiness With PIT Caveat

最后更新：2026-07-03

## 状态

`VALIDATING`

## 背景

TRADING-2330 已完成 dynamic target baseline timestamp remediation。真实 run 生成 2682 条 timestamp-remediated wrapper records，`known_at_policy=NEXT_SESSION_DECISION_POLICY`，`latency_policy=NEXT_TRADING_DAY_DECISION`，wrapper validation=`PASS_WITH_WARNINGS`，readiness=`TIMESTAMP_REMEDIATED_READY_WITH_WARNINGS_FOR_2331`，`2331_allowed=true`，next task 为 `TRADING-2331_Dynamic_Target_Baseline_Dry_Run_Readiness_With_PIT_Caveat`。

TRADING-2331 不是 dynamic dry-run。本任务只检查 TRADING-2332 是否可以在明确 PIT caveat、known-at policy、risk-cap timestamp alignment、market-data alignment 和 input contract 后执行 source-bound dynamic target baseline dry-run。只要 caveat 或安全边界不能被显式接受，就必须 fail closed 或路由到 remediation。

## 目标

新增命令：

```bash
aits research trends dynamic-target-baseline-dry-run-readiness-with-pit-caveat \
  --timestamp-remediation-dir outputs/research_trends/dynamic_target_baseline_timestamp_remediation \
  --source-remediation-dir outputs/research_trends/dynamic_target_baseline_source_remediation \
  --dynamic-preparation-dir outputs/research_trends/dynamic_target_baseline_preparation \
  --source-binding-dir outputs/research_trends/exposure_cap_simulation_source_binding \
  --simulation-policy-dir outputs/research_trends/exposure_cap_mechanics_simulation \
  --static-dry-run-dir outputs/research_trends/source_bound_exposure_cap_dry_run_static_etf_baseline \
  --output-dir outputs/research_trends/dynamic_target_baseline_dry_run_readiness_with_pit_caveat \
  --mode dry_run_readiness_with_pit_caveat
```

命令必须读取 2330 timestamp-remediated wrapper、wrapper validation、PIT caveat、known-at semantics、validity / latency / rebalance reports、risk-cap timestamp alignment、2331 readiness / route 和 safety boundary，并交叉检查 2329 source remediation、2328 preparation、2324 source binding、2323 simulation policy 和 2326 static dry-run context。

## 非目标

- 不执行 exposure-cap simulation、static ETF dry-run 或 dynamic target dry-run。
- 不读取 cached market data、runtime exposure data、broker account 或真实持仓。
- 不修改 risk-cap trigger series、exposure-cap policy、cooldown / decay policy 或 dynamic strategy。
- 不生成新的 dynamic strategy target、真实 target weight、rebalance instruction、buy/sell signal、paper-shadow、production 或 broker action。
- 不判断 exposure-cap 对动态策略是否有效。
- 不把 PIT approximation 标记为 strict PIT。

## 输入和门禁

必需输入：

- TRADING-2330 timestamp remediation outputs。
- TRADING-2329 source remediation outputs。
- TRADING-2328 dynamic target baseline preparation outputs。
- TRADING-2324 source binding outputs。
- TRADING-2323 simulation policy/readiness outputs。
- TRADING-2326 static dry-run context。

必须 fail closed：

- 2330 summary、readiness、route、wrapper、wrapper validation、PIT caveat、known-at、risk-cap alignment 或 safety boundary 缺失。
- 2330 route 不是 `TRADING-2331_Dynamic_Target_Baseline_Dry_Run_Readiness_With_PIT_Caveat`。
- `2331_allowed=false`。
- 任何输入打开 promotion、paper-shadow、production、broker action，或输出 actionable target/rebalance/paper/production ready。

如果 wrapper validation=`FAIL`，应输出 blocked readiness 并路由到 `TRADING-2332_Dynamic_Target_Baseline_Wrapper_Remediation`。`PASS_WITH_WARNINGS` 可以进入 2332，但 PIT、known-at、market-data、risk-cap 和 source-lineage caveat 必须 carry forward。

## 数据质量政策

TRADING-2331 默认只读取 prior research outputs、static config 和 wrapper artifacts，不读取 market data cache 或 runtime exposure data。因此默认：

- `data_quality_status=NOT_APPLICABLE_DRY_RUN_READINESS_ONLY`
- `data_quality_gate_required=false`
- `data_quality_gate_executed=false`
- `aits_validate_data_executed=false`

若未来实现读取 market data cache 或 runtime exposure data，必须先运行 `aits validate-data --as-of 2026-06-29` 或同源 validation code path。

## 输出

Runtime outputs 写入 `outputs/research_trends/dynamic_target_baseline_dry_run_readiness_with_pit_caveat/`：

- `dynamic_dry_run_readiness_summary.json`
- `dynamic_dry_run_gate_checklist.json`
- `dynamic_dry_run_pit_caveat_acceptance_report.json`
- `dynamic_dry_run_wrapper_field_validation_matrix.json/.csv`
- `dynamic_dry_run_timestamp_alignment_matrix.json/.csv`
- `dynamic_dry_run_risk_cap_alignment_matrix.json/.csv`
- `dynamic_dry_run_market_data_alignment_matrix.json/.csv`
- `dynamic_dry_run_policy_compatibility_matrix.json/.csv`
- `dynamic_dry_run_input_contract.json`
- `dynamic_dry_run_data_quality_precheck.json`
- `dynamic_dry_run_interpretation_boundary.json`
- `dynamic_dry_run_2332_readiness_matrix.json`
- `dynamic_dry_run_2332_task_route.json`

Research docs：

- `docs/research/dynamic_target_baseline_dry_run_readiness_with_pit_caveat.md`
- `docs/research/dynamic_dry_run_pit_caveat_acceptance_report.md`
- `docs/research/dynamic_dry_run_input_contract.md`
- `docs/research/dynamic_dry_run_2332_readiness_route.md`

## 安全边界

所有 outputs 必须保持：

- `research_only=true`
- `dry_run_readiness_only=true`
- `simulation_executed=false`
- `portfolio_effect=none`
- `production_effect=none`
- `broker_action=none`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `manual_review_only=true`

`target_exposure` 只能作为 research baseline field，不得被解释为 trading target weight 或 rebalance instruction。

## 实施步骤

1. 新增 `src/ai_trading_system/dynamic_target_baseline_dry_run_readiness.py`。
2. 在 `src/ai_trading_system/cli_commands/research_trends.py` 注册 CLI command。
3. 补充 focused tests 覆盖 loader、gate checklist、PIT caveat acceptance、wrapper field validation、timestamp / risk-cap / market-data alignment、input contract、2332 route 和 CLI。
4. 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md`、`docs/task_register.md`。
5. 运行真实 CLI，记录 readiness status、PIT caveat、2332 route 和 safety boundary。
6. 运行 focused parallel pytest、Ruff、compileall、docs freshness、documentation contract、task-register consistency、contract-validation、full validation 和 `git diff --check`。

## 验收标准

- CLI 能生成 TRADING-2331 required runtime artifacts 和 research docs。
- 2330 route mismatch、`2331_allowed=false`、opened safety gate、broker action 或 actionable target/rebalance instruction 必须 fail closed。
- wrapper field validation 能识别 identity、date、target exposure、timestamp、policy、safety 和 lineage 字段覆盖。
- PIT caveat acceptance 必须明确 strict PIT=false、research-only approximation=true、allowed/blocked usage 和 carry-forward caveat。
- timestamp / risk-cap / market-data alignment matrix 必须披露 coverage、warning、blocking status 和 2332 recheck requirement。
- input contract 必须列出 2332 所需 wrapper fields、source lineage、policy semantics、data-quality boundary 和 safety constraints。
- 2332 route 只能在允许集合内；ready route 必须指向 `TRADING-2332_Source_Bound_Exposure_Cap_Dry_Run_With_Dynamic_Target_Baseline`。
- 所有 outputs 固定 `simulation_executed=false`、promotion/paper-shadow/production/broker false/none。
- 本任务提交不包含既有无关 research 文档改动。

## 进展记录

- 2026-07-03：根据 owner 附件新增任务并进入 `IN_PROGRESS`。当前 worktree 有两个无关 research 文档未提交改动：`docs/research/indicator_family_only_model_review.md` 和 `docs/research/layer1_selector_pause_or_continue_owner_pack.md`；TRADING-2331 必须 selective staging，不能混入无关改动。
- 2026-07-03：实现完成并进入 `VALIDATING`。新增 `dynamic-target-baseline-dry-run-readiness-with-pit-caveat` CLI、2330 fail-closed loader、PIT caveat acceptance、wrapper field validation、timestamp / risk-cap / market-data / policy alignment matrix、input contract、data-quality precheck、interpretation boundary、2332 readiness route 和 focused tests。真实 run status=`DYNAMIC_TARGET_BASELINE_DRY_RUN_READINESS_READY_PROMOTION_BLOCKED`，wrapper_record_count=`2682`，wrapper_validation_status=`PASS_WITH_WARNINGS`，pit_caveat_acceptance_status=`PIT_CAVEAT_ACCEPTED_FOR_RESEARCH_DRY_RUN_WITH_WARNINGS`，gate_status=`DYNAMIC_DRY_RUN_READY_WITH_PIT_CAVEAT`，readiness_status=`DYNAMIC_DRY_RUN_READY_FOR_2332_WITH_PIT_CAVEAT`，`2332_allowed=true`，next_task=`TRADING-2332_Source_Bound_Exposure_Cap_Dry_Run_With_Dynamic_Target_Baseline`；`aits validate-data` 不适用，因为本任务只读取 prior research outputs、static config 和 wrapper artifacts。
- 2026-07-03：系统验证通过 Ruff、compileall、TRADING-2331 focused parallel pytest 18 passed、真实 CLI run、docs freshness 509 docs PASS、documentation contract 1229 reports PASS、task-register consistency run/validate PASS、contract-validation 193 passed、full parallel pytest 4065 passed / 643 warnings 和 `git diff --check`。contract-validation runtime artifact=`outputs/validation_runtime/contract-validation_20260702T164629Z/test_runtime_summary.json`。
