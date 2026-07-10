# TRADING-2438M1 / M2 Growth Tilt Candidate Research Contract and PIT Replay Development Plan
最后更新：2026-07-10

## 候选研究合同审批、计算面绑定与真实 PIT Replay 开发文档（修订版）

**文档日期：** 2026-07-10
**适用仓库：** `AI-Trading-Collaboration/AITradingSystem`
**前置状态：** TRADING-2438M 第三次审计后正式 `BLOCKED`
**前置提交：** `05aa0eae`
**权威输入：**
- `research/configs/growth_tilt/false_risk_off_missed_upside_2433.yaml`
- `docs/research/growth_tilt_top3_candidate_selection.md`
- `docs/research/growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution.md`
- `inputs/research_reviews/growth_tilt_candidate_runtime_spec_threshold_policy_review.yaml`
- `docs/requirements/TRADING-2438M1_Growth_Tilt_Candidate_Runtime_Spec_And_Threshold_Policy_Approval.md`

---

# 0. 修订说明

此前把三名候选描述为“runtime remediation 后已具备 executable contract”的策略变体并不准确。第三次审计已经确认：

```text
三名候选均为研究假设，而非已实现策略
selection order 来自 candidate config 顺序，不是收益/回撤/评分排名
pit_candidates_tested=0
candidate status=PENDING x 3
runtime readiness=0/3
metric readiness=0/3
threshold readiness=0/3
owner-input gaps=27
```

因此，当前 blocker 不是“某段已存在的计算链没有输出数值”，而是：

```text
研究假设尚未被批准为可执行实验
→ 没有参数化策略语义
→ 没有 callable compute-plane contract
→ 没有 executor mapping
→ 没有六项 metric contract
→ 没有 threshold policy
→ replay runner 与 threshold evaluator 均不应被调用
```

在 owner 输入完成前继续实现 M2，会产生以下任一错误：

1. 系统自行虚构投资参数、阈值或风险边界；
2. 为尚不存在的策略增加新的 control-plane wrapper；
3. 把配置中的名称误当作可执行逻辑；
4. 把 `null` 误写为 `0`、`FAIL` 或“未改善”；
5. 以“top-3”名义强迫三个重复或未定义假设进入 replay。

本修订版将后续拆分为：

```text
TRADING-2438M1
Owner research decision + executable experiment contract approval

TRADING-2438M2
Approved candidates only:
compute-plane implementation/binding + real PIT replay

TRADING-2438M3
Metric materialization + threshold decision + evidence interpretation

TRADING-2438N
Post-replay candidate disposition / next research route
```

---

# 1. 核心决策

## 1.1 不再使用“排名”语义

兼容旧文件名时可以保留 `top3`，但所有新 artifact 和字段必须明确：

```yaml
candidate_selection:
  selected_candidate_count: 3
  selection_basis: CONFIG_DECLARATION_ORDER
  performance_ranked: false
  pit_evidence_available: false
```

禁止使用以下没有证据支持的字段或措辞：

```text
best candidate
rank 1 / rank 2 / rank 3
top performer
highest-scoring candidate
leading candidate
```

推荐使用：

```text
selection_order=1/2/3
candidate_set_member
research_hypothesis
approved_for_pit_replay
```

## 1.2 首轮 M2 不要求凑满三名候选

候选数量不是 readiness gate。

合法状态包括：

```text
APPROVE=2, REDEFINE=1
APPROVE=1, WITHDRAW=2
APPROVE=0, REDEFINE=2, WITHDRAW=1
```

M2 只接受 `APPROVE` 候选。`REDEFINE` 和 `WITHDRAW` 不得被 replay runner 调用。

## 1.3 推荐 owner 决策

| 候选 | 推荐决策 | 原因 |
|---|---:|---|
| `recovery_reentry_speedup_guard` | `APPROVE` | 保留为“提前开始 re-entry”的受限实验；必须与 ramp acceleration 分离 |
| `false_risk_off_confirmation_relaxation` | `APPROVE` | 研究轴最独立，处理防御触发而非 recovery ramp |
| `missed_upside_reentry_accelerator` | `REDEFINE` | 当前与第一候选同义；建议改为“baseline 已确认后加快 exposure ramp”，不改变触发时间 |

首轮 M2 推荐只运行前两个 `APPROVE` 候选。第三个候选完成重定义和二次 owner approval 后，再作为独立 replay wave 接入。

---

# 2. 三个研究方向的正交化定义

# 2.1 Candidate A — `recovery_reentry_speedup_guard`

## 研究角色

```yaml
candidate_role: RECOVERY_REENTRY_TIMING_ACCELERATOR
primary_failure_target: missed_upside
secondary_failure_target: slow_growth_recovery_reentry
changes_trigger_timing: true
changes_ramp_speed: false
changes_defensive_entry: false
```

## 必须保持的结构边界

该候选只研究：

> 在 baseline recovery 最终确认之前，是否可以在硬风险条件全部解除、且仅剩一个指定 soft confirmation 尚未满足时，有限度地提前恢复一小部分风险敞口。

它不得同时修改：

```text
baseline target exposure
baseline confirmed-state ramp rule
hard risk-off veto
portfolio risk cap
production allocation
transaction-cost model
```

## 推荐的 v0 实验参数

以下数值是 **owner 可批准的预注册 screening experiment boundary**，不是历史回测结论，也不是 production 参数：

```yaml
parameters:
  recovery_signal_id: OWNER_MUST_MAP_TO_EXISTING_SIGNAL
  lagging_soft_confirmation_id: OWNER_MUST_SELECT_EXACTLY_ONE
  hard_veto_ids: OWNER_MUST_MAP_ALL_BASELINE_HARD_VETOES

  lead_steps: 1
  provisional_exposure_fraction_of_remaining_gap: 0.25
  provisional_exposure_absolute_cap: OWNER_MUST_SET_IN_BASELINE_EXPOSURE_UNIT
  max_active_steps: 3

  confirmed_state_ramp_multiplier: 1.0
  target_exposure_override_allowed: false
  hard_veto_bypass_allowed: false
```

## 触发语义

```text
1. baseline recovery core signal=true
2. all hard vetoes=false
3. all required confirmations except one named soft confirmation=true
4. named soft confirmation is the only unmet recovery condition
5. candidate may enter provisional re-entry state one evaluation step early
6. provisional exposure cannot exceed the approved cap
```

## 退出 / 回滚语义

任何一项发生即终止 provisional state：

```text
baseline recovery becomes fully confirmed
core recovery signal becomes false
any hard veto becomes true
max_active_steps reached
owner-approved expiry condition reached
input provenance or PIT validity fails
```

当 recovery invalidates 时：

```text
candidate must return to baseline-prescribed exposure path
candidate must not invent an emergency path
rollback must be visible in decision trace
```

## 与 Candidate C 的强制区别

```text
Candidate A:
  changes when re-entry starts
  does not accelerate ramp after baseline confirmation

Candidate C after redefinition:
  does not change trigger time
  changes only post-confirmation ramp speed
```

---

# 2.2 Candidate B — `false_risk_off_confirmation_relaxation`

## 研究角色

```yaml
candidate_role: DEFENSIVE_ENTRY_SOFT_CONFIRMATION_GRACE
primary_failure_target: false_risk_off
secondary_failure_target: false_defensive_days
changes_trigger_timing: true
changes_reentry_ramp: false
changes_hard_vetoes: false
```

## 必须保持的结构边界

该候选只允许放宽 **一项明确命名的 soft confirmation**。不得使用“放宽 confirmation”作为不可审计的整体操作。

必须明确：

```yaml
baseline_confirmation_id:
baseline_rule:
candidate_rule:
relaxation_mode:
applicable_regime_ids:
hard_veto_ids:
```

推荐选择：

> 对历史 regret cases 贡献最大、但不承担 tail-risk 防护责任的单一 soft confirmation。

禁止放宽：

```text
hard downside veto
data quality/PIT gate
portfolio hard risk cap
extreme volatility or drawdown emergency condition
missing-data blocker
```

## 推荐的 v0 实验参数

```yaml
parameters:
  relaxed_soft_confirmation_id: OWNER_MUST_SELECT_EXACTLY_ONE
  baseline_required_state: OWNER_MUST_MAP
  applicable_regime_ids: OWNER_MUST_SET_NON_WILDCARD_SCOPE
  hard_veto_ids: OWNER_MUST_MAP_ALL_NON_BYPASSABLE_VETOES

  relaxation_mode: ONE_STEP_GRACE
  grace_steps: 1
  remove_confirmation_entirely: false
  defensive_exposure_override_allowed: false
  hard_veto_bypass_allowed: false
  max_active_steps: 1
```

## 触发语义

```text
1. baseline would enter a defensive state because the selected soft confirmation fired
2. no hard risk-off veto is active
3. candidate is inside an explicitly approved regime scope
4. candidate delays the soft-confirmation effect by at most one evaluation step
5. candidate does not increase exposure beyond the pre-defensive baseline state
```

## 结束语义

```text
selected confirmation persists for the next step
any hard veto activates
regime scope ends
input becomes invalid or missing
grace_steps exhausted
```

此时必须恢复 baseline defensive rule，不允许继续自动延长 grace。

## 主要风险

必须显式评估：

```text
max drawdown deterioration
downside capture deterioration
whipsaw increase
turnover increase
false-risk-off improvement
defensive-day reduction
```

---

# 2.3 Candidate C — `missed_upside_reentry_accelerator`

## 推荐决策

```yaml
decision: REDEFINE
reason: CURRENTLY_SEMANTICALLY_OVERLAPS_WITH_RECOVERY_REENTRY_SPEEDUP_GUARD
```

当前名称无法证明其与 Candidate A 在触发、速度、约束或风险边界上存在结构差异。不得仅通过修改参数名继续进入 M2。

## 推荐的新研究定义

建议重命名为：

```text
post_confirmation_reentry_ramp_accelerator
```

新角色：

```yaml
candidate_role: POST_CONFIRMATION_EXPOSURE_RAMP_ACCELERATOR
primary_failure_target: missed_upside
changes_trigger_timing: false
changes_ramp_speed: true
changes_defensive_entry: false
```

## 推荐的 v0 重定义参数

```yaml
parameters:
  trigger_source: EXACT_BASELINE_RECOVERY_CONFIRMATION
  trigger_lead_steps: 0

  ramp_step_multiplier: 1.5
  max_fraction_of_remaining_gap_per_step: 0.50
  acceleration_steps: 2

  target_exposure_override_allowed: false
  hard_veto_bypass_allowed: false
  reset_to_baseline_ramp_on_veto: true
```

## 与 Candidate A 的正交性检查

只有全部为真时才可从 `REDEFINE` 转为 `APPROVE`：

```text
trigger timestamp exactly equals baseline confirmation timestamp
no soft confirmation is removed or shortened
only exposure ramp step size/speed changes
target exposure is unchanged
hard veto set is unchanged
artifact can demonstrate distinct decision traces versus Candidate A
```

若 owner 不希望研究“触发后 ramp mechanics”，推荐直接选择 `WITHDRAW`，而不是保留一个同义候选。

---

# 3. TRADING-2438M1 — Owner Review Contract

# 3.1 M1 的唯一目标

M1 负责把研究假设变成 **owner 明确授权的可执行实验合同**。

M1 不实现 replay engine，不计算指标，也不产生 PASS/FAIL。

推荐完成状态：

```text
GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_APPROVED
```

若仍有缺口：

```text
GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_BLOCKED
```

## 3.2 每个候选的 decision contract

```yaml
candidate_id:
selection_order:
selection_basis: CONFIG_DECLARATION_ORDER
decision: APPROVE | REDEFINE | WITHDRAW
decision_owner:
decision_version:
decision_timestamp:
decision_rationale:
```

## 3.3 APPROVE 候选的必要字段

```yaml
runtime_spec:
  candidate_role:
  baseline_config_ref:
  operation_type:
  parameters:
  hard_veto_ids:
  applicable_regime_ids:
  expiry_conditions:
  rollback_conditions:

executor_mapping:
  executor_family:
  operation_type:
  planned_entrypoint:
  input_contract_version:
  output_contract_version:

metric_contract_ref:
threshold_policy_ref:
```

注意：

- `planned_entrypoint` 可以是 M2 将实现的正式 entrypoint；
- 但 `operation_type`、输入输出和行为语义必须先由 owner 批准；
- executor 不得通过 candidate name 中的单词推断行为。

## 3.4 REDEFINE 候选的必要字段

```yaml
redefinition:
  old_candidate_id:
  proposed_candidate_id:
  overlap_with:
  old_semantics_rejected_reason:
  new_candidate_role:
  changes_trigger_timing:
  changes_ramp_speed:
  changes_defensive_entry:
  required_second_owner_review: true
```

## 3.5 WITHDRAW 候选的必要字段

```yaml
withdrawal:
  reason_code:
  rationale:
  replacement_candidate_id: optional
  future_reopen_condition: optional
```

---

# 4. Shared Six-Metric Contract

三个候选使用相同的六个 metric ID，但必须共享一个明确版本化的 contract，不能只注册名称。

推荐：

```text
growth_tilt_candidate_replay_metric_contract_v1
```

所有 metrics 必须在相同的：

```text
PIT input snapshot
replay date range
calendar
benchmark
transaction-cost model
missing-data policy
baseline configuration
```

下比较 candidate 与 baseline。

# 4.1 `return_delta_vs_baseline`

```yaml
metric_id: return_delta_vs_baseline
definition: candidate_net_return - baseline_net_return
unit: percentage_point
higher_is_better: true
cost_adjusted: true
required_inputs:
  - candidate_net_return
  - baseline_net_return
missing_policy: BLOCKED
```

# 4.2 `max_drawdown_delta_vs_baseline`

```yaml
metric_id: max_drawdown_delta_vs_baseline
definition: abs(candidate_max_drawdown) - abs(baseline_max_drawdown)
unit: percentage_point
lower_is_better: true
interpretation:
  positive: candidate drawdown is worse
  negative: candidate drawdown is better
missing_policy: BLOCKED
```

# 4.3 `turnover_delta_vs_baseline`

```yaml
metric_id: turnover_delta_vs_baseline
definition: (candidate_turnover - baseline_turnover) / max(abs(baseline_turnover), epsilon)
unit: relative_fraction
lower_is_better: true
epsilon_policy: OWNER_APPROVED_CONSTANT
missing_policy: BLOCKED
```

# 4.4 `false_risk_off_delta`

```yaml
metric_id: false_risk_off_delta
definition: >
  (candidate_false_risk_off_measure - baseline_false_risk_off_measure)
  / max(abs(baseline_false_risk_off_measure), epsilon)
unit: relative_fraction
lower_is_better: true
required_owner_fields:
  - false_risk_off_definition_id
  - evaluation_horizon_steps
  - reference_asset_or_benchmark
  - false_risk_off_outcome_rule
  - event_or_cost_measure
missing_policy: BLOCKED
```

Owner 必须选择 `event_count`、`defensive_days` 或 `opportunity_cost` 中的一种正式 measure；禁止 replay 后再更换口径。

# 4.5 `missed_upside_delta`

```yaml
metric_id: missed_upside_delta
definition: >
  (candidate_missed_upside_measure - baseline_missed_upside_measure)
  / max(abs(baseline_missed_upside_measure), epsilon)
unit: relative_fraction
lower_is_better: true
required_owner_fields:
  - missed_upside_definition_id
  - evaluation_horizon_steps
  - reference_exposure_path
  - upside_outcome_rule
  - event_or_cost_measure
missing_policy: BLOCKED
```

推荐使用 opportunity-cost measure，但必须与既有研究口径兼容。

# 4.6 `whipsaw_delta`

```yaml
metric_id: whipsaw_delta
definition: >
  (candidate_whipsaw_measure - baseline_whipsaw_measure)
  / max(abs(baseline_whipsaw_measure), epsilon)
unit: relative_fraction
lower_is_better: true
required_owner_fields:
  - whipsaw_definition_id
  - reversal_window_steps
  - minimum_exposure_change
  - event_or_cost_measure
missing_policy: BLOCKED
```

# 4.7 共同 provenance

每个 runtime metric 必须输出：

```yaml
metric_id:
value:
unit:
finite:
numerator:
denominator:
baseline_value:
candidate_value:
sample_count:
event_count:
window_start:
window_end:
as_of:
source_artifact_refs:
contract_version:
calculator_version:
status: COMPUTED | BLOCKED
blocker_codes:
```

禁止：

```text
null -> 0
missing denominator -> epsilon without policy
empty event set -> zero improvement
static config value -> runtime metric
```

空事件集必须根据 contract 明确选择：

```text
BLOCKED_INSUFFICIENT_EVENTS
或
COMPUTED_NOT_APPLICABLE
```

不得静默记为 0。

---

# 5. Recommended Screening Threshold Policy

以下 policy 用于 **首次 PIT replay screening**，不是 promotion、paper-shadow 或 production gate。

推荐 ID：

```text
growth_tilt_candidate_pit_screening_policy_v1
```

## 5.1 通用 readiness gate

所有候选首先必须满足：

```yaml
required_metric_count: 6
computed_metric_count: 6
finite_metric_count: 6
threshold_evaluation_count: 6
pit_valid: true
baseline_and_candidate_comparable: true
minimum_primary_event_count: 5
```

任一不满足：

```text
candidate_status=BLOCKED
```

而不是 FAIL。

## 5.2 风险非劣化阈值

推荐 owner 预注册以下 screening 容忍边界：

```yaml
return_delta_vs_baseline:
  comparator: GTE
  value: 0.0
  unit: percentage_point

max_drawdown_delta_vs_baseline:
  comparator: LTE
  value: 0.50
  unit: percentage_point

turnover_delta_vs_baseline:
  comparator: LTE
  value: 0.10
  unit: relative_fraction

whipsaw_delta:
  comparator: LTE
  value: 0.10
  unit: relative_fraction
```

解释：

```text
候选不得降低净收益
最大回撤绝对值最多恶化 0.50 个百分点
turnover 相对 baseline 最多增加 10%
whipsaw measure 相对 baseline 最多增加 10%
```

这些数值是低权限的研究筛选边界。owner 可以修改，但必须在 replay 前写入 rationale，不能查看结果后调整。

## 5.3 Candidate A 主指标

```yaml
candidate_id: recovery_reentry_speedup_guard
primary_metric: missed_upside_delta
primary_threshold:
  comparator: LTE
  value: -0.05
  unit: relative_fraction
secondary_thresholds:
  false_risk_off_delta:
    comparator: LTE
    value: 0.05
```

含义：

```text
missed-upside measure 至少改善 5%
false-risk-off measure 不得恶化超过 5%
同时通过所有通用风险非劣化阈值
```

## 5.4 Candidate B 主指标

```yaml
candidate_id: false_risk_off_confirmation_relaxation
primary_metric: false_risk_off_delta
primary_threshold:
  comparator: LTE
  value: -0.05
  unit: relative_fraction
secondary_thresholds:
  missed_upside_delta:
    comparator: LTE
    value: 0.00
```

含义：

```text
false-risk-off measure 至少改善 5%
missed-upside 不得恶化
同时通过所有通用风险非劣化阈值
```

## 5.5 Candidate C 重定义后的主指标

只有完成二次 approval 才注册：

```yaml
candidate_id: post_confirmation_reentry_ramp_accelerator
primary_metric: missed_upside_delta
primary_threshold:
  comparator: LTE
  value: -0.05
  unit: relative_fraction
secondary_thresholds:
  false_risk_off_delta:
    comparator: LTE
    value: 0.00
```

## 5.6 Policy governance

```yaml
owner: OWNER_MUST_SET
version: 1.0.0
policy_class: PIT_REPLAY_SCREENING_ONLY
effective_from: 2026-07-10
expires_after_completed_replay_rounds: 1

review_required_when:
  - metric_contract_changes
  - candidate_semantics_change
  - baseline_config_changes
  - transaction_cost_model_changes
  - replay window set changes
  - primary event count is insufficient
  - owner requests redesign

prohibited:
  - threshold_change_after_result_visibility
  - automatic threshold relaxation
  - reuse_as_promotion_gate
  - reuse_as_production_risk_limit
```

---

# 6. TRADING-2438M2 — Compute-Plane Implementation

# 6.1 M2 输入 gate

M2 只能读取通过 M1 validation 的 owner-review artifact。

对每个 candidate：

```text
decision=APPROVE
runtime spec complete
operation type registered
all parameters typed and bounded
hard veto mapping resolvable
metric contract resolvable
threshold policy resolvable
owner/version/evidence/review conditions present
placeholder count=0
```

否则不得调用 executor。

# 6.2 推荐实现：通用 Overlay Executor

不要为三个 candidate 各复制一套 growth-tilt strategy。实现一个 typed overlay executor：

```text
GrowthTiltCandidateOverlayExecutor
```

支持 operation type：

```text
EARLY_REENTRY_PROVISIONAL_EXPOSURE
DEFENSIVE_SOFT_CONFIRMATION_GRACE
POST_CONFIRMATION_RAMP_ACCELERATION  # 仅重定义批准后启用
```

## 输入

```yaml
as_of:
baseline_config_ref:
baseline_state:
baseline_target_exposure:
baseline_decision_trace:
pit_signal_snapshot:
hard_veto_state:
regime_state:
candidate_runtime_spec:
transaction_cost_model_ref:
```

## 输出

```yaml
candidate_id:
operation_type:
candidate_state:
candidate_target_exposure:
baseline_target_exposure:
exposure_delta:
trigger_reason_codes:
guard_check_results:
veto_check_results:
expiry_state:
rollback_state:
decision_trace:
input_provenance:
runtime_status:
blocker_codes:
```

## 设计要求

```text
baseline is source of truth
overlay changes only owner-approved dimensions
no candidate name parsing
no implicit defaults
no production side effects
deterministic under same PIT inputs
every exposure difference has a reason code
```

# 6.3 Replay Orchestrator

流程：

```text
load approved owner contract
→ load baseline and PIT data
→ run baseline replay
→ run candidate overlay replay on identical timeline
→ materialize candidate decision/exposure path
→ apply identical cost model
→ compute six metrics
→ evaluate versioned threshold policy
→ persist artifact
→ reload and verify
```

必须证明：

```text
same dates
same data snapshot
same benchmark
same cost assumptions
same missing-data policy
candidate differs only in approved operation
```

# 6.4 Candidate status semantics

```text
PENDING:
  owner decision or experiment contract not complete

BLOCKED:
  approved experiment could not be evaluated because runtime evidence is missing/invalid

FAIL:
  all evidence is complete, but one or more required thresholds failed

PASS:
  all evidence is complete and all required screening thresholds passed
```

禁止：

```text
PENDING -> FAIL because metric is null
BLOCKED -> FAIL because event count is zero
PASS -> promotion candidate
```

`PASS` 仅表示：

```text
PIT screening passed; owner review required
```

---

# 7. CLI and Artifacts

# 7.1 M1 CLI

建议：

```bash
aits research strategies \
  growth-tilt-candidate-runtime-spec-threshold-policy-approval \
  --review inputs/research_reviews/growth_tilt_candidate_runtime_spec_threshold_policy_review.yaml
```

输出至少包括：

```text
selected_candidate_count
performance_ranked=false
approve_count
redefine_count
withdraw_count
approved_runtime_spec_count
approved_metric_contract_count
approved_threshold_policy_count
owner_input_gap_count
m2_eligible_candidate_ids
status
next_route
```

# 7.2 M2 CLI

建议：

```bash
aits research strategies \
  growth-tilt-approved-candidate-pit-replay \
  --as-of YYYY-MM-DD \
  --owner-review <path> \
  --candidate-id <optional>
```

默认只运行 `decision=APPROVE` 的 candidate。

# 7.3 Required artifacts

```text
owner_review_validation.json
approved_candidate_runtime_specs.json
candidate_executor_binding_report.json
baseline_replay_summary.json
candidate_replay_decision_trace.json
candidate_metric_runtime_output.json
candidate_threshold_evaluation.json
candidate_pit_replay_summary.json
candidate_blocker_summary.json
post_replay_next_route.json
```

Artifact 必须记录：

```text
selection_basis=CONFIG_DECLARATION_ORDER
performance_ranked=false
owner_decision
owner/version
contract versions
code commit
run_id
as_of
PIT data refs
baseline ref
candidate ref
```

---

# 8. Focused Test Plan

建议至少覆盖以下测试组。

## 8.1 Owner decision tests

1. 三名候选都必须有 decision。
2. `APPROVE` 缺 runtime spec 时 blocked。
3. `REDEFINE` 不得进入 M2 eligible list。
4. `WITHDRAW` 不得生成 executor mapping。
5. selection order 不得被解释成 performance rank。
6. 首轮只有两个 APPROVE 时可以 readiness pass。
7. placeholder/TBD/null 参数必须 rejected。

## 8.2 Orthogonality tests

8. Candidate A 只能改变 trigger timing。
9. Candidate A 不得改变 confirmed-state ramp multiplier。
10. Candidate B 只能放宽一个指定 soft confirmation。
11. Candidate B 不得绕过 hard veto。
12. Candidate C 旧定义不得 approval。
13. Candidate C 重定义后 trigger timestamp 必须等于 baseline。
14. Candidate C 重定义后只允许改变 ramp speed。

## 8.3 Executor tests

15. executor 不得解析 candidate name 推断行为。
16. 同一输入必须 deterministic。
17. hard veto 激活时 A/B 均回到 baseline。
18. A 的 provisional exposure 不得超过 cap。
19. A 到期后不得自动延长。
20. B grace 只能持续一个批准 step。
21. B soft confirmation 持续时必须恢复 baseline defensive action。
22. overlay 不得改变 baseline target exposure definition。

## 8.4 Metric tests

23. 六项 metric 全部来自 runtime replay。
24. null 不得转换成 zero。
25. baseline denominator 为零时必须按 policy 处理。
26. 空事件集不得静默视为改善。
27. metric unit 与 threshold unit 不一致时 blocked。
28. candidate/baseline window 不一致时 blocked。
29. candidate/baseline cost model 不一致时 blocked。
30. metric provenance 必须完整。

## 8.5 Threshold tests

31. primary metric 改善不足时 FAIL。
32. primary metric 改善但 MDD 超限时 FAIL。
33. metric 缺失时 BLOCKED，不是 FAIL。
34. event count 少于 5 时 BLOCKED。
35. policy expired 时 BLOCKED。
36. replay 后修改 policy version 必须被 lineage check 检出。
37. PASS 不得设置 promotion/paper-shadow/production flag。

## 8.6 Artifact and regression tests

38. artifact reload 后结果一致。
39. runtime status count 满足 pass+fail+blocked=approved candidate count。
40. `PENDING` candidate 不计入 M2 replay denominator。
41. old control-plane report 不得作为 metric producer。
42. `pit_candidates_tested` 必须等于真实 replay invocation 数。
43. worktree/report registry/catalog/system flow/task archive 更新一致。

---

# 9. Acceptance Criteria

# 9.1 TRADING-2438M1 DoD

```text
three owner decisions present
recommended outcome is APPROVE/APPROVE/REDEFINE or another fully reasoned owner choice
all APPROVE candidates have typed runtime specs
all APPROVE candidates have executor operation mappings
shared six-metric contract is versioned and complete
at least one numeric threshold policy exists per APPROVE candidate
owner/version/rationale/validation evidence/review and expiry conditions present
no placeholders
M2 eligible candidate list generated
```

推荐真实结果：

```text
selected_candidate_count=3
performance_ranked=false
approve_count=2
redefine_count=1
withdraw_count=0
m2_eligible_candidate_count=2
owner_input_gap_count=0
status=GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_APPROVED
```

# 9.2 TRADING-2438M2/M3 DoD

对所有 M2 eligible candidates：

```text
executor invoked
baseline replay invoked
candidate replay invoked
six runtime metrics computed or exact blocker emitted
threshold evaluator invoked where metrics are complete
no static/default values masquerade as runtime evidence
candidate status resolved to PASS/FAIL/BLOCKED
```

以下均为合法成功结果：

```text
pass/fail/blocked=0/2/0
pass/fail/blocked=1/1/0
pass/fail/blocked=0/1/1
pass/fail/blocked=2/0/0
```

没有候选 PASS 不等于工程任务失败。

---

# 10. Recommended Task Sequence

## TRADING-2438M1A — Owner Decision Finalization

```text
填写三名候选 decision
确认 A/B APPROVE
确认 C REDEFINE 或 WITHDRAW
记录 selection_basis=config order / performance_ranked=false
```

## TRADING-2438M1B — Shared Metric and Threshold Policy Approval

```text
选择 false-risk-off / missed-upside / whipsaw 正式定义
确认单位和 denominator policy
批准 screening threshold values
确认 policy governance
```

## TRADING-2438M2A — Typed Overlay Executor

```text
实现 generic overlay executor
实现 A/B operation types
禁止 candidate-name inference
加入 decision trace 和 hard-veto checks
```

## TRADING-2438M2B — Real PIT Replay Binding

```text
baseline/candidate same-timeline replay
cost model binding
runtime artifact persistence
```

## TRADING-2438M3 — Metrics and Threshold Resolution

```text
materialize six metrics
execute threshold policy
resolve PASS/FAIL/BLOCKED
route to evidence interpretation
```

## TRADING-2438N — Post-Replay Research Decision

按结果路由：

```text
PASS:
  owner evidence review; not automatic promotion

FAIL with clear targeted improvement but risk breach:
  targeted redesign only

FAIL with no primary improvement:
  withdraw or replace family

BLOCKED:
  exact compute/data/contract remediation

Candidate C:
  second owner approval after structural redefinition
```

---

# 11. Implementation Guardrails

必须始终保持：

```text
observe_only=true
promotion_allowed=false
paper_shadow_allowed=false
production_allowed=false
broker_action=none
```

不得：

```text
通过名字推断策略逻辑
通过 config order 推断优劣
为了维持 top-3 而批准重复假设
根据 replay 结果反向调阈值
把 screening PASS 当成策略合格
把缺失 metric 当作 0 或 FAIL
在 owner approval 前实现投资参数
```

---

# 12. Recommended Commit Structure

建议拆分提交：

```text
1. Define 2438M1 candidate research contracts
2. Approve growth-tilt PIT screening policy
3. Implement typed growth-tilt overlay executor
4. Bind approved candidates to PIT replay
5. Materialize candidate metrics and threshold decisions
6. Complete 2438M research artifacts and task routing
```

推荐任务完成说明应明确：

```text
candidate order is not performance ranking
only owner-approved candidates were replayed
all metrics are runtime-produced
thresholds were pre-registered
no promotion or production behavior was enabled
```

---

# 13. Owner Decision Summary

本轮最合理的研究组合不是三个含义模糊的“top candidates”，而是两个正交的首轮实验：

```text
A. recovery re-entry timing:
   可以提前一小步开始恢复吗？

B. defensive entry confirmation:
   可以对一个 soft confirmation 提供一小步 grace 吗？
```

第三个方向只有在重定义后才有独立价值：

```text
C. post-confirmation ramp mechanics:
   baseline 已确认 recovery 后，可以更快到达同一个 target 吗？
```

这三条轴分别是：

```text
when to start re-entry
when to enter defense
how fast to ramp after confirmed re-entry
```

只有按这三个维度严格分离，后续 PIT replay 才能解释“哪种机制有效”，而不是再次测试三个名称不同但行为相同的假设。

---

# 14. Implementation Progress

## 2026-07-10 — M1A / M1B contract framework and M2A executor

已完成：

```text
selection_basis=CONFIG_DECLARATION_ORDER
performance_ranked=false
A decision=APPROVE
B decision=APPROVE
C decision=REDEFINE with post-confirmation ramp contract
shared six-metric contract schema and provenance contract
recommended PIT screening policy schema and numeric proposal
mixed-decision validator and approved-only M2 handoff
GrowthTiltCandidateOverlayExecutor typed operations
```

真实 M1 strict run：

```text
status=GROWTH_TILT_CANDIDATE_RUNTIME_SPEC_AND_THRESHOLD_POLICY_BLOCKED
owner_decision_complete_count=3
approved_candidate_count=2
redefine_candidate_count=1
runtime_spec_ready_count=0
metric_contract_ready_count=0
threshold_policy_ready_count=0
m2_eligible_candidate_count=0
owner_input_gap_count=6
source_validation_error_count=0
strict_validation_error_count=0
evidence_gap_count=0
next_route=TRADING-2438M1B_GROWTH_TILT_SHARED_METRIC_AND_SCREENING_POLICY_APPROVAL
```

保持 BLOCKED 的 owner 输入：

```text
A recovery signal / one lagging soft confirmation / all hard veto ids / regime scope / exposure cap
B one relaxed soft confirmation / baseline required state / all hard veto ids / regime scope
relative-delta epsilon and empty-event selection
false-risk-off / missed-upside / whipsaw measure, horizon, reference and outcome definitions
metric contract owner and APPROVED status
screening policy owner and pre-result preregistration
```

现有 governed configs 和 event casebook 未提供足以消除上述 blocker 的 exact mappings；因此未猜值、未执行真实 replay、未生成 runtime metrics 或 PASS/FAIL。M2A executor 只完成 deterministic fixture validation：A confirmed ramp 保持不变，B grace 只允许一步，C second approval 前 BLOCKED，hard veto / expiry 回 baseline，candidate name 不参与 operation dispatch，所有非零 exposure delta 都有 reason code。

验证结果：M1/M2A focused parallel pytest 69 passed，合并 docs/registry/task consistency 后 101 passed；fast tier 198 passed；contract-validation 197 passed；Ruff、compileall、真实 strict CLI 和 diff check 通过。full parallel pytest 为 5166 passed / 46 failed / 643 warnings；失败来自本次未修改且 HEAD 已存在的通用 CLI 参数契约错配，未用串行运行覆盖该结果。

## M1D disposition amendment — 2026-07-10

`TRADING-2438M1D` supersedes the earlier first-wave assumption that Candidate B remains APPROVE. The current governed disposition is A=`APPROVE`, B=`REDEFINE`, C=`REDEFINE`. Candidate B is proposed as `non_hard_defensive_entry_persistence_guard`, but the repository has no callable aggregate non-hard defensive request producer; it remains implementation-blocked and requires a second owner decision to WITHDRAW or separately approve new baseline behavior. Earlier `approved/redefine=2/1`, B soft-confirmation-grace, and first-wave A/B statements in this document are historical pre-M1D context, not the current replay handoff. Current real M2 eligibility remains zero.
