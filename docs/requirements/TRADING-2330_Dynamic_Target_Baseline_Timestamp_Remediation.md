# TRADING-2330 Dynamic Target Baseline Timestamp Remediation

最后更新：2026-07-03

## 状态

`VALIDATING`

## 背景

TRADING-2329 已完成 dynamic target baseline source remediation。真实 run 显示 `remediable_source_count=4`、`blocked_source_count=30`、`wrapper_record_count=2682`、`wrapper_validation_status=PASS_WITH_WARNINGS`，但 `2330_allowed=false`，next task 为 `TRADING-2330_Dynamic_Target_Baseline_Timestamp_Remediation`。

当前 wrapper 可作为 research-only dynamic target baseline candidate，但其 timestamp / known-at / validity / latency / rebalance timing 语义仍不足以直接进入 source-bound dynamic baseline dry-run。TRADING-2330 只修复或明确标注这些时间语义，不执行 simulation，不判断 risk-cap 或 exposure-cap 是否有效，不进入 paper-shadow、production 或 broker action。

## 目标

新增命令：

```bash
aits research trends dynamic-target-baseline-timestamp-remediation \
  --source-remediation-dir outputs/research_trends/dynamic_target_baseline_source_remediation \
  --dynamic-preparation-dir outputs/research_trends/dynamic_target_baseline_preparation \
  --diagnostics-dir outputs/research_trends/exposure_cap_vs_no_cap_diagnostics_review \
  --source-binding-dir outputs/research_trends/exposure_cap_simulation_source_binding \
  --simulation-policy-dir outputs/research_trends/exposure_cap_mechanics_simulation \
  --output-dir outputs/research_trends/dynamic_target_baseline_timestamp_remediation \
  --mode timestamp_remediation
```

命令必须读取 TRADING-2329 wrapper、PIT caveat、schema adapter、alignment readiness 和 2330 route，识别时间字段缺口，定义 timestamp remediation policy，生成 timestamp-remediated derivative wrapper、known-at semantics、validity window、latency / rebalance timing、risk-cap timestamp alignment 和 TRADING-2331 route。所有推导 timestamp 必须披露 derivation mode 和 caveat；不得把推导 timestamp 伪装成 strict PIT。

## 非目标

- 不执行 exposure-cap simulation、static ETF dry-run 或 dynamic target dry-run。
- 不修改 risk-cap trigger series、exposure-cap policy、cooldown / decay policy 或 dynamic strategy。
- 不生成新的 dynamic strategy target、真实 target weight、rebalance instruction、buy/sell signal、paper-shadow、production 或 broker action。
- 不读取 broker account 或真实持仓。
- 不判断 exposure-cap 对动态策略是否有效。
- 不覆盖 TRADING-2329 原始 wrapper，只生成 timestamp-remediated derivative artifact。

## 输入和门禁

必需输入：

- TRADING-2329 source remediation outputs。
- TRADING-2328 dynamic target baseline preparation outputs。
- TRADING-2327 diagnostics context。
- TRADING-2324 source binding context。
- TRADING-2323 simulation policy/readiness context。

必须 fail closed：

- 2329 summary、route 或 safety boundary 缺失。
- 2329 route 不是 timestamp remediation。
- wrapper artifact 打开 promotion、paper-shadow、production 或 broker action。
- wrapper artifact 输出 actionable `target_weight_action`、`rebalance_instruction`、`paper_shadow_ready` 或 `production_ready`。
- wrapper 缺少 `baseline_id`、`source_id`、`source_family`、`source_path`、`source_hash`、`baseline_schema_version` 或 `source_artifact_hash`。

如果 wrapper artifact 缺失，不应伪造 wrapper；应输出 blocked report 和 source generation requirements。

## 数据质量政策

TRADING-2330 默认只读取 prior research outputs、static config、registry 和 candidate artifacts，不读取 market data cache 或 runtime exposure data。因此默认：

- `data_quality_status=NOT_APPLICABLE_TIMESTAMP_REMEDIATION_ONLY`
- `data_quality_gate_required=false`
- `data_quality_gate_executed=false`
- `aits_validate_data_executed=false`

若未来实现读取 market data cache 或 runtime exposure data，必须先运行 `aits validate-data --as-of 2026-06-29` 或同源 validation code path。

## 输出

Runtime outputs 写入 `outputs/research_trends/dynamic_target_baseline_timestamp_remediation/`：

- `dynamic_target_timestamp_remediation_summary.json`
- `dynamic_target_timestamp_gap_matrix.json/.csv`
- `dynamic_target_timestamp_source_priority_matrix.json/.csv`
- `dynamic_target_timestamp_remediation_policy.json`
- `dynamic_target_timestamp_derivation_matrix.json/.csv`
- `dynamic_target_known_at_semantics_report.json`
- `dynamic_target_validity_window_remediation_report.json`
- `dynamic_target_latency_policy_report.json`
- `dynamic_target_rebalance_timing_report.json`
- `dynamic_target_timestamp_remediated_wrapper_artifact.json/.csv`
- `dynamic_target_timestamp_wrapper_validation_summary.json`
- `dynamic_target_timestamp_pit_caveat_report.json`
- `dynamic_target_risk_cap_timestamp_alignment_report.json`
- `dynamic_target_2331_readiness_matrix.json`
- `dynamic_target_2331_task_route.json`
- `dynamic_target_timestamp_remediation_safety_boundary.json`
- `dynamic_target_timestamp_remediation_blocked_report.json`
- `dynamic_target_timestamp_source_generation_requirements.json`

Research docs：

- `docs/research/dynamic_target_baseline_timestamp_remediation_report.md`
- `docs/research/dynamic_target_timestamp_gap_matrix.md`
- `docs/research/dynamic_target_known_at_semantics_report.md`
- `docs/research/dynamic_target_validity_window_remediation_report.md`
- `docs/research/dynamic_target_2331_readiness_route.md`

## 安全边界

所有 outputs 必须保持：

- `research_only=true`
- `timestamp_remediation_only=true`
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

1. 新增 `src/ai_trading_system/dynamic_target_baseline_timestamp_remediation.py`。
2. 在 `src/ai_trading_system/cli_commands/research_trends.py` 注册 CLI command。
3. 补充 focused tests 覆盖 loader、timestamp gap、policy、derivation、known-at、validity / latency / rebalance、wrapper、2331 route 和 CLI。
4. 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md`、`docs/task_register.md`。
5. 运行真实 CLI，记录 timestamp remediation status、wrapper validation、PIT caveat 和 2331 route。
6. 运行 focused parallel pytest、Ruff、compileall、docs freshness、documentation contract、task-register consistency、contract-validation、full validation 和 `git diff --check`。

## 验收标准

- CLI 能生成 TRADING-2330 required runtime artifacts 和 research docs。
- 2329 route mismatch、opened safety gate、broker action 或 actionable target/rebalance instruction 必须 fail closed。
- timestamp gap matrix 能统计 as_of / decision / valid_from / valid_until / rebalance / known-at 缺口。
- source priority / remediation policy 禁止 future outcome inference、market result based timestamp 和 undocumented manual timestamp。
- 非 native timestamp 必须披露 derivation mode 和 PIT caveat。
- timestamp-remediated wrapper 保留 source identity、hash、schema version 和 research-only `target_exposure` 角色。
- 2331 route 只能在允许集合内。
- 所有 outputs 固定 `simulation_executed=false`、promotion/paper-shadow/production/broker false/none。
- 本任务提交不包含既有无关 research 文档改动。

## 进展记录

- 2026-07-03：根据 owner 附件新增任务并进入 `IN_PROGRESS`。当前 worktree 有两个无关 research 文档未提交改动：`docs/research/indicator_family_only_model_review.md` 和 `docs/research/layer1_selector_pause_or_continue_owner_pack.md`；TRADING-2330 必须 selective staging，不能混入无关改动。
- 2026-07-03：实现完成并进入 `VALIDATING`。新增 `dynamic-target-baseline-timestamp-remediation` CLI、2329 fail-closed loader、timestamp gap matrix、source priority matrix、remediation policy、derivation matrix、known-at semantics report、validity / latency / rebalance reports、timestamp-remediated wrapper、timestamp wrapper validation、PIT caveat、risk-cap timestamp alignment、2331 readiness route、safety boundary 和 focused tests。真实 run status=`DYNAMIC_TARGET_BASELINE_TIMESTAMP_REMEDIATION_READY_PROMOTION_BLOCKED`，wrapper_input_record_count=`2682`，timestamp_remediated_wrapper_record_count=`2682`，wrapper_validation_status=`PASS_WITH_WARNINGS`，known_at_policy=`NEXT_SESSION_DECISION_POLICY`，latency_policy=`NEXT_TRADING_DAY_DECISION`，readiness_status=`TIMESTAMP_REMEDIATED_READY_WITH_WARNINGS_FOR_2331`，`2331_allowed=true`，next_task=`TRADING-2331_Dynamic_Target_Baseline_Dry_Run_Readiness_With_PIT_Caveat`；`aits validate-data` 不适用，因为本任务只读取 prior research outputs、static config、registry 和 candidate artifacts。
- 2026-07-03：系统验证通过 TRADING-2330 focused parallel pytest 23 passed、Ruff、compileall、真实 CLI run、docs freshness 508 docs PASS、documentation contract 1228 reports PASS、task-register consistency run/validate PASS、contract-validation 193 passed、full parallel pytest 4047 passed / 643 warnings 和 `git diff --check`。contract-validation runtime artifact=`outputs/validation_runtime/contract-validation_20260702T155530Z/test_runtime_summary.json`。
