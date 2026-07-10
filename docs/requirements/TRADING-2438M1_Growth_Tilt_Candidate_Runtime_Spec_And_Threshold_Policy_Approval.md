# TRADING-2438M1 Growth Tilt Candidate Runtime Spec And Threshold Policy Approval

最后更新：2026-07-10

## Context

- task register id: `TRADING-2438M1_GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_APPROVAL`
- source task: `TRADING-2438M`
- default as-of: `2026-07-08`
- market regime: `ai_after_chatgpt`
- status: `BLOCKED_OWNER_INPUT`
- production boundary: validation-only / owner-review-only / candidate-only

TRADING-2438M completion audit 已用真实 strict CLI 证明三个 top-3 candidate 的第一个
失败阶段均为 `RUNTIME_CONTRACT_RESOLVED`：2438K 的 `runtime_executable=true` 只是
source readiness claim，而 candidate-specific executor id/version/input contract version
全部为 null。当前 candidate config 也只有 id、family、research question 和 rationale，
没有参数化 executable spec；现有 2438B entrypoint 是 contract builder，不是
compute-plane replay runner；2432/2433 threshold values 全部为 null。

M1 不负责替 owner 发明参数或阈值。它负责把 owner 决策所需字段固化为可审计输入
契约，校验每个 candidate 的 APPROVE / REDEFINE / WITHDRAW 决策，并在 owner 输入
不完整时精确 fail-closed。

## Authoritative Candidate Set

候选身份和顺序必须与 2438M/2438L 一致：

1. `recovery_reentry_speedup_guard`
2. `false_risk_off_confirmation_relaxation`
3. `missed_upside_reentry_accelerator`

M1 不允许静默改名、重排、增加或删除 candidate。若 owner 选择 WITHDRAW，必须显式
route 到 candidate reselection；若选择 REDEFINE，必须显式 route 到 candidate
definition design。两者都不能被当作 APPROVE。

## Owner Review Input Contract

Tracked owner-review input：

`inputs/research_reviews/growth_tilt_candidate_runtime_spec_threshold_policy_review.yaml`

顶层必须包含：

- `schema_version`
- `task_id`
- `status`
- `owner_review_status`
- `as_of`
- `market_regime`
- `source_task_id`
- `candidate_ids`
- `candidate_reviews`
- `safety_boundary`

每个 candidate review 必须包含：

- `candidate_id`
- `decision`: `PENDING | APPROVE | REDEFINE | WITHDRAW`
- `decision_rationale`
- `review_owner`
- `reviewed_at`
- `review_condition`
- `expiry_condition`
- `runtime_spec`
- `metric_specs`
- `threshold_specs`

APPROVE 必须额外满足：

- runtime spec 有 `approved=true`、executor id/version、input contract version、source
  policy ref 和非空 parameters；
- 六个 required metric 均有 source field、unit、normalization rule、calculator
  id/version；
- threshold set 非空，每条有 threshold id、metric binding、显式 operator/value、
  policy owner/version/status、rationale、validation evidence、review condition 和 expiry
  condition；
- threshold value 必须是 owner 提供的 finite value；M1 不从 current metrics、候选名称、
  fixture、旧 run 或 registry 中自动填值。

REDEFINE / WITHDRAW 必须有明确 rationale 和 next route，不要求伪造 runtime spec 或
threshold policy。

## Threshold Governance

现有 `config/research/threshold_registry.yaml` 中与 dynamic allocation 相关的 threshold
可作为 owner review evidence，但不能自动批准。registry 当前明确记录多项
`UNCALIBRATED_DEFAULT` / `calibration_required=true` /
`no_promotion_dependency_without_review=true`。若 owner 选择复用其中某项，review input
必须写明 threshold id、适用 metric、为何适用于本 candidate PIT replay、policy status、
validation evidence 和 review/expiry 条件。

## Status And Routes

- 全部 candidate 为 APPROVE 且所有 contract checks PASS：
  `GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_APPROVAL_READY`，route 到
  `TRADING-2438M2_Growth_Tilt_Candidate_Runtime_Compute_Plane_Binding`。
- 任一 candidate 为 PENDING 或 APPROVE 但字段不完整：
  `GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_APPROVAL_BLOCKED_OWNER_INPUT`，
  route 保持 M1 owner input。
- 任一 candidate 为 REDEFINE：
  `GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_CANDIDATE_REDEFINITION_REQUIRED`，
  route 到 narrowly-scoped candidate definition design。
- 任一 candidate 为 WITHDRAW：
  `GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_WITHDRAWAL_RESELECTION_REQUIRED`，
  route 到 top-3 candidate reselection。

WITHDRAW 优先于 REDEFINE，REDEFINE 优先于 PENDING，PENDING/incomplete 优先于 READY。

## CLI And Outputs

```bash
aits research strategies growth-tilt-candidate-runtime-spec-threshold-policy-approval \
  --as-of 2026-07-08 \
  --strict
```

Primary：

- `outputs/research_strategies/growth_tilt_candidate_runtime_spec_threshold_policy_approval/approval_readiness_result.json`
- `docs/research/growth_tilt_candidate_runtime_spec_threshold_policy_approval.md`

Supporting：

- `candidate_runtime_spec_review_matrix.json`
- `metric_contract_review_matrix.json`
- `threshold_policy_review_matrix.json`
- `owner_action_checklist.json`
- `no_effect_boundary.json`

## Safety Boundary

- 不运行 replay、backtest、scoring 或 market-data experiment；
- 不读取 fresh market/outcome data；data-quality gate 标记为 not applicable，并说明只读取
  prior validated artifacts、config 和 owner input；
- 不修改 candidate parameters 或 thresholds；
- 不启用 paper-shadow / schedule / production / broker；
- 不生成 trading advice、signal、allocation 或 portfolio mutation；
- `production_effect=none`、`broker_action=none`。

## Acceptance Criteria

- source 2438M schema/status/route、`blocked_count=3`、
  `candidate_replay_outcome_rechecked=true`、固定 top-3 identity/order、source hashes 和
  as-of 可追溯；owner-review task/source/regime/as-of/status lineage 必须完整；
- tracked owner-review template 覆盖三个 candidate 和所有 required fields；
- APPROVE / REDEFINE / WITHDRAW/PENDING 的 status/route 语义有 focused tests；
- APPROVE 路径验证六个 required metrics、至少一个 threshold、finite value、operator、
  calculator/evaluator provenance 和 heuristic governance metadata；
- missing/null/NaN/Inf、unknown metric/operator、candidate drift、duplicate id 全部 fail-closed；
- 默认真实 template run 必须是 BLOCKED_OWNER_INPUT，不得伪造 approval；
- registry、catalog、system flow、task register、docs freshness 和 contract validation 通过；
- git diff 仅包含 M1 attributable changes。

## Progress Notes

- 2026-07-10: 由 2438M 真实 BLOCKED route 建立并进入 `IN_PROGRESS`。已确认现有
  threshold registry 仅能作为 review evidence，不能自动提供本任务的 approved threshold
  values；开始实现 owner-review template、schema validator、readiness report 和 CLI。
- 2026-07-10: 系统侧 owner-input contract 与 validator 实现完成，状态转回
  `BLOCKED_OWNER_INPUT` 等待实际 owner decision。新增 tracked review YAML、APPROVE /
  REDEFINE / WITHDRAW / PENDING resolver、runtime/metric/threshold review matrices、owner
  checklist、no-effect boundary、CLI、registry/catalog/system flow 和 41 项 focused tests。
  真实 strict CLI status=
  `GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_APPROVAL_BLOCKED_OWNER_INPUT`，
  source ready=true、candidate count=3、approved/pending/redefine/withdraw=`0/3/0/0`、
  runtime spec ready=`0/3`、metric contract ready=`0/3`、threshold policy ready=`0/3`、
  owner input gaps=27、source/strict/evidence errors=`0/0/0`，data quality status=
  `NOT_APPLICABLE_PRIOR_VALIDATED_ARTIFACTS_CONFIG_OWNER_REVIEW_ONLY`，next route 保持
  M1。threshold/parameter changes、paper-shadow、production、broker 和 weight mutation
  全部 false/none。
