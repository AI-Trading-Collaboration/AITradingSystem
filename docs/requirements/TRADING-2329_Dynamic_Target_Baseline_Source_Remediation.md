# TRADING-2329 Dynamic Target Baseline Source Remediation

最后更新：2026-07-02

## 状态

`VALIDATING`

## 背景

TRADING-2328 已完成 dynamic target baseline preparation。真实 run 发现 `candidate_artifacts_found=34`，但 `pit_ready_source_count=0`、`recommended_candidate_count=0`，结论为 `DYNAMIC_BASELINE_SOURCE_REMEDIATION_REQUIRED`，下一步为 `TRADING-2329_Dynamic_Target_Baseline_Source_Remediation`。

这说明仓库内已有 candidate / advisory / target 类 artifacts，但当前没有任何 artifact 能直接作为 source-bound dynamic target baseline dry-run 的 PIT-ready source。主要缺口集中在 `target_exposure` 语义、`as_of_timestamp` / `decision_timestamp`、`valid_from` / `valid_until`、rebalance timing、source hash / registry reference、known-at 语义和 risk-cap trigger series 对齐。

## 目标

新增命令：

```bash
aits research trends dynamic-target-baseline-source-remediation \
  --dynamic-preparation-dir outputs/research_trends/dynamic_target_baseline_preparation \
  --diagnostics-dir outputs/research_trends/exposure_cap_vs_no_cap_diagnostics_review \
  --static-dry-run-dir outputs/research_trends/source_bound_exposure_cap_dry_run_static_etf_baseline \
  --source-binding-dir outputs/research_trends/exposure_cap_simulation_source_binding \
  --simulation-policy-dir outputs/research_trends/exposure_cap_mechanics_simulation \
  --candidate-artifact-roots outputs/research_trends,paper_portfolio,outputs \
  --output-dir outputs/research_trends/dynamic_target_baseline_source_remediation \
  --mode source_remediation
```

命令必须：

1. fail-closed 校验 TRADING-2328 route 确认为 source remediation。
2. 从 2328 inventory / candidate matrix 和 candidate artifact roots 中筛选最接近 dynamic target baseline 的 source family。
3. 生成 source family ranking、gap-to-schema matrix 和 remediation action matrix。
4. 定义统一 dynamic target baseline schema contract。
5. 为可 remediation source 生成 schema adapter spec。
6. 生成 research-only remediated baseline candidates 和 baseline wrapper artifact；如果无可 remediation source，则生成 no-remediable-source report 和 source generation requirements。
7. 生成 PIT / known-at / replayability caveat、risk-cap trigger alignment readiness、TRADING-2330 readiness matrix 和 task route。
8. 保持 `simulation_executed=false`，并固定 promotion / paper-shadow / production / broker 全部关闭。

## 非目标

- 不执行 exposure-cap simulation。
- 不重新运行 static ETF dry-run。
- 不修改 risk-cap trigger series、exposure-cap policy、cooldown / decay policy 或 dynamic strategy。
- 不生成新的 dynamic strategy target，不训练或搜索策略参数。
- 不生成真实 target weight 指令、rebalance instruction、buy/sell signal、paper-shadow、production 或 broker action。
- 不读取真实 broker account 或真实持仓。
- 不改变 TRADING-2328 的 source remediation 结论，只对其进行 remediation attempt。

## 输入和门禁

必需输入：

- TRADING-2328 dynamic target baseline preparation outputs。
- TRADING-2327 diagnostics review route context。
- TRADING-2326 static ETF dry-run context。
- TRADING-2324 source binding context。
- TRADING-2323 exposure-cap mechanics simulation policy/readiness context。
- Candidate artifact roots。

以下情况必须 fail closed：

- 2328 summary、candidate inventory、task route 或 safety boundary 缺失。
- 2328 status/route 不是 dynamic target source remediation。
- 2328 已经存在 ready dynamic baseline source。
- 任一输入 artifact 打开 `promotion_allowed`、`paper_shadow_allowed`、`production_allowed` 或 `broker_action != none`。
- 任一输入 artifact 输出 actionable `target_weight` 或 `rebalance_instruction`。

所有 dynamic target candidate artifacts 都不可 remediation 时，不应让命令失败；应输出 blocked report 并路由到 `TRADING-2330_Dynamic_Target_Baseline_Source_Generation`。

## 数据质量政策

TRADING-2329 默认只读取 prior research outputs、static config、registry 和 candidate artifacts，不读取 market data cache 或 runtime exposure data。因此默认：

- `data_quality_status=NOT_APPLICABLE_SOURCE_REMEDIATION_ONLY`
- `data_quality_gate_required=false`
- `data_quality_gate_executed=false`
- `aits_validate_data_executed=false`

若未来实现开始读取 market data cache 或 runtime exposure data，必须先运行 `aits validate-data --as-of 2026-06-29` 或同源 validation code path。

## 输出

Runtime outputs 写入 `outputs/research_trends/dynamic_target_baseline_source_remediation/`：

- `dynamic_target_source_remediation_summary.json`
- `dynamic_target_source_family_ranking.json/.csv`
- `dynamic_target_gap_to_schema_matrix.json/.csv`
- `dynamic_target_remediation_action_matrix.json/.csv`
- `dynamic_target_baseline_schema_contract.json`
- `dynamic_target_schema_adapter_spec.json/.csv`
- `dynamic_target_remediated_baseline_candidates.json/.csv`
- `dynamic_target_baseline_wrapper_artifact.json/.csv`
- `dynamic_target_wrapper_validation_summary.json`
- `dynamic_target_wrapper_pit_caveat_report.json`
- `dynamic_target_wrapper_alignment_readiness.json`
- `dynamic_target_remediation_blocked_sources.json`
- `dynamic_target_2330_readiness_matrix.json`
- `dynamic_target_2330_task_route.json`
- `dynamic_target_source_remediation_safety_boundary.json`

若没有任何可 remediation source，则额外生成：

- `dynamic_target_no_remediable_source_report.json`
- `dynamic_target_source_generation_requirements.json`

Research docs：

- `docs/research/dynamic_target_baseline_source_remediation_report.md`
- `docs/research/dynamic_target_baseline_schema_contract.md`
- `docs/research/dynamic_target_schema_adapter_spec.md`
- `docs/research/dynamic_target_wrapper_pit_caveat_report.md`
- `docs/research/dynamic_target_2330_readiness_route.md`

## 安全边界

所有 outputs 必须保持：

- `research_only=true`
- `source_remediation_only=true`
- `simulation_executed=false`
- `portfolio_effect=none`
- `production_effect=none`
- `broker_action=none`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `manual_review_only=true`

Wrapper artifact 可以包含 `target_exposure` 作为 research baseline field，但不得被解释为 trading target weight 或 rebalance instruction。

## 实施步骤

1. 新增 `src/ai_trading_system/dynamic_target_baseline_source_remediation.py`。
2. 在 `src/ai_trading_system/cli_commands/research_trends.py` 注册 CLI command。
3. 补充 focused tests 覆盖 loader、source family ranking、gap-to-schema、remediation action、schema adapter、wrapper、2330 route 和 CLI。
4. 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md`、`docs/task_register.md`。
5. 运行真实 CLI，记录 remediation status / wrapper status / 2330 route。
6. 运行 focused parallel pytest、Ruff、compileall、docs freshness、documentation contract、task-register consistency、contract-validation、full validation 和 `git diff --check`。

## 验收标准

- CLI 能生成 TRADING-2329 required runtime artifacts 和 research docs。
- 2328 route mismatch、opened safety gate、broker action 或 actionable target/rebalance instruction 必须 fail closed。
- 有 target exposure semantics 且 timestamp/hash 可映射的 fixture 能生成 wrapper candidate。
- 缺 PIT / validity 的 source 必须披露 caveat。
- 无可 remediation source 时生成 blocked report 和 source generation requirements。
- 2330 route 只能在 allowed route 集合内。
- 所有 outputs 固定 `simulation_executed=false`、promotion/paper-shadow/production/broker false/none。
- 本任务提交不包含既有无关 research 文档改动。

## 进展记录

- 2026-07-02：根据 owner 附件新增任务并进入 `IN_PROGRESS`。当前 worktree 有两个无关 research 文档未提交改动；TRADING-2329 必须 selective staging，不能混入无关改动。
- 2026-07-02：实现完成并进入 `VALIDATING`。新增 `dynamic-target-baseline-source-remediation` CLI、2328 fail-closed loader、source family ranking、gap-to-schema matrix、remediation action matrix、baseline schema contract、schema adapter spec、research-only wrapper、wrapper validation、PIT caveat、alignment readiness、2330 readiness route、safety boundary 和 focused tests。真实 run status=`DYNAMIC_TARGET_BASELINE_SOURCE_REMEDIATION_READY_PROMOTION_BLOCKED`，remediable_source_count=`4`，blocked_source_count=`30`，wrapper_generated=`true`，wrapper_record_count=`2682`，wrapper_validation_status=`PASS_WITH_WARNINGS`，pit_policy=`PIT_APPROXIMATION_READY`，alignment_readiness_status=`WRAPPER_ALIGNMENT_BLOCKED`，readiness_status=`DYNAMIC_WRAPPER_SCHEMA_ADAPTER_REQUIRED`，`2330_allowed=false`，next_task=`TRADING-2330_Dynamic_Target_Baseline_Timestamp_Remediation`；`aits validate-data` 不适用，因为本任务只读取 prior research outputs、static config、registry 和 candidate artifacts。
- 2026-07-02：系统验证通过 focused parallel pytest 19 passed、Ruff、compileall、docs freshness 507 docs PASS、documentation contract 1227 reports PASS、task-register consistency run/validate PASS、contract-validation 193 passed、full parallel pytest 4024 passed / 643 warnings 和 `git diff --check`。contract-validation runtime artifact=`outputs/validation_runtime/contract-validation_20260702T144631Z/test_runtime_summary.json`。
