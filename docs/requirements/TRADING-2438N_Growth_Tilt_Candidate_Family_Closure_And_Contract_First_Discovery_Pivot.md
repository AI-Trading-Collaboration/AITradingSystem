# TRADING-2438N Growth Tilt Candidate Family Closure and Contract-First Discovery Pivot
最后更新：2026-07-10

**Base commit:** `f4805190b1d4965911addce7c7dcfa740ca13a1a`

## 1. 当前事实

```text
Owner decisions: APPROVE/REDEFINE/WITHDRAW = 0/2/1
Baseline adapters: ready/blocked = 0/4
Replacement-A prerequisites: PASS/BLOCKED = 2/8
M2 eligible candidates = 0
Real PIT replay executed = false
Runtime metrics materialized = false
Baseline/candidate behavior added = false
```

当前 A/B/C family 已达到有效的 terminal research state：

```text
GROWTH_TILT_CANDIDATE_FAMILY_CLOSED_NO_EXECUTABLE_PIT_CANDIDATE
```

该结论是完整的 negative research evidence，不是实现失败。不得继续增加 baseline behavior 或 control-plane wrapper 来让旧 candidate name 变得 executable：

```text
A: 原 speedup hypothesis 没有 governed baseline recovery-consumption rule
B: callable PIT soft confirmation 与 aggregate non-hard request 均不存在
C: 未形成独立、正交、可执行 contract
replacement A: 仍有 8 项 prerequisite blocked
baseline adapters: 0/4 runtime-ready
```

后续研究路线由 name-first 改为：

```text
existing governed runtime capabilities
→ executable mutation grammar
→ contract-complete candidates
→ cheap discriminative replay
→ full PIT qualification
```

## 2. Task split 与依赖

```text
TRADING-2438N1 Candidate Family Closure and Negative Evidence Ledger
TRADING-2438N2 Baseline Capability Graph
TRADING-2438N3 Contract-First Candidate Generator
TRADING-2438N4 Multi-Fidelity Candidate Screening
```

不得为了维持三个 candidate 而恢复或补造候选集合。执行顺序固定为 N1 → N2；只有 N2 的 `mutation_ready_capability_count > 0` 时才可启动 N3，N4 又必须依赖 N3 的 contract-ready candidate。

## 3. TRADING-2438N1 — Candidate family closure

输出：

```text
docs/research/growth_tilt_candidate_family_closure.md
outputs/research_strategies/growth_tilt_candidate_family_closure.json
outputs/research_strategies/growth_tilt_candidate_negative_result_ledger.json
```

Family closure contract：

```yaml
family_id: growth_tilt_false_risk_off_missed_upside_2433
closure_status: CLOSED_NO_EXECUTABLE_PIT_CANDIDATE
closure_reason_codes:
  - NO_APPROVED_CANDIDATE
  - BASELINE_ADAPTERS_NOT_RUNTIME_READY
  - REPLACEMENT_CANDIDATE_PREREQUISITES_BLOCKED
  - NO_REAL_PIT_REPLAY_EXECUTED
candidate_dispositions:
  recovery_reentry_speedup_guard: REDEFINE
  false_risk_off_confirmation_relaxation: WITHDRAW
  missed_upside_reentry_accelerator: REDEFINE
  capped_recovery_permission_overlay: KEEP_REDEFINED_BLOCKED
pit_candidates_tested: 0
runtime_metrics_materialized: false
promotion_allowed: false
paper_shadow_allowed: false
production_allowed: false
broker_action: none
```

Closure 必须原样复制 M1E prerequisite matrix 的 `2 PASS / 8 BLOCKED` identity、order、status 和 blocker codes，不得重新发明 blocker summary，也不得把 null metric 推断为 FAIL。

### Reopen policy

Family 仅可在 candidate-independent baseline project 产生以下至少一项新证据时重新评估：

```text
recovery-permission producer with valid PIT lineage and governed semantics
governed baseline recovery-transition consumption path
runtime-ready authoritative hard-veto aggregate
runtime-ready governed native exposure scalar
independently motivated callable non-hard defensive request
```

Reopen metadata 必须包含：

```yaml
new_evidence_refs:
baseline_change_task_ids:
baseline_change_motivation:
candidate_independent_change: true
owner_reapproval_required: true
screening_policy_refreeze_required: true
```

Candidate work 本身不得成为 baseline change 的动机或证据来源。

### Negative-result ledger

每个 hypothesis 一条记录：

```yaml
hypothesis_id:
intended_failure_target:
original_candidate_role:
terminal_disposition:
non_executability_reason:
missing_baseline_capabilities:
research_design_lesson:
prohibited_future_reuse:
reopen_condition:
```

Ledger 必须保留以下 lessons：

```text
config declaration order is not performance ranking
a callable producer does not imply baseline consumption
a candidate cannot be a delta from a nonexistent baseline rule
soft confirmation cannot be inferred from a conceptual label
candidate delta units must come from governed exposure semantics
hard-veto completeness is an admission prerequisite, not a late replay detail
```

## 4. TRADING-2438N2 — Baseline capability graph

N2 只读生成 baseline 实际可 expose、modify、replay、measure 的 machine-readable inventory：

```text
config/research/growth_tilt_baseline_capability_graph.yaml
docs/research/growth_tilt_baseline_capability_graph.md
outputs/research_strategies/growth_tilt_baseline_capability_graph.json
```

### Capability node schema

```yaml
capability_id:
capability_type: SIGNAL | DECISION_REQUEST | PERSISTENCE_RULE | HARD_VETO |
  REGIME_TRANSITION | EXPOSURE_SCALAR | EXPOSURE_CAP | RAMP_RULE |
  COOLDOWN_RULE | EXPIRY_RULE | METRIC | REPLAY_RUNNER
runtime:
  producer_entrypoint:
  output_path:
  callable:
  consumed_by_baseline:
  deterministic:
  schema_version:
pit:
  lineage_status:
  as_of_supported:
  source_refs:
  missing_policy:
governance:
  owner:
  contract_ref:
  semantics_approved:
  mutable_dimensions:
  immutable_dimensions:
replay:
  runner_binding:
  baseline_comparison_supported:
  metric_refs:
readiness:
  status: READY | BLOCKED | DIAGNOSTIC_ONLY | NOT_APPLICABLE
  blocker_codes:
```

### Edge schema

```yaml
from_capability:
to_capability:
relation: PRODUCES | CONSUMED_BY | GUARDED_BY | REQUESTS | APPLIES | CAPS |
  MEASURED_BY | REPLAYED_BY | SUPERSEDES
runtime_resolvable:
pit_valid:
```

### Candidate-mutation-ready gate

Capability 只有同时满足以下条件才是 mutation-ready：

```text
callable=true
consumed_by_baseline=true
PIT lineage valid
semantics approved
missing policy explicit
runner binding available
at least one mutable dimension declared
required hard-veto dependencies ready
required transition dependencies ready
required exposure dependencies ready
```

Callable 但未被 baseline consumed 的 producer 绝不是 mutation-ready。N2 不得新增 transition、persistence、veto、confirmation、exposure unit、threshold 或 baseline behavior。

## 5. TRADING-2438N3 — Conditional contract-first generator

N3 只有在 N2 得到至少一个 mutation-ready capability 后才可启动。初始 typed operator grammar：

```yaml
operators:
  - id: ADJUST_EXISTING_PERSISTENCE
    requires: [READY_PERSISTENCE_RULE]
    integer_delta: [-1, 1]
  - id: ADJUST_EXISTING_RAMP_STEP
    requires: [READY_RAMP_RULE, READY_EXPOSURE_SCALAR]
    multiplier_range: [0.75, 1.50]
  - id: ADD_COOLDOWN_TO_EXISTING_TRANSITION
    requires: [READY_TRANSITION, READY_EXPIRY_OR_COOLDOWN_SEMANTICS]
    steps: [1, 3]
  - id: TIGHTEN_EXISTING_EXPOSURE_CAP
    requires: [READY_EXPOSURE_CAP, READY_EXPOSURE_SCALAR]
    relative_multiplier: [0.50, 0.90]
  - id: RELAX_EXISTING_NON_HARD_THRESHOLD
    requires: [READY_NON_HARD_THRESHOLD, READY_HARD_VETO_AGGREGATE]
    registered_grid_only: true
```

不支持 baseline capability 未 ready 的 operator。Candidate contract 必须解析：

```yaml
candidate_id:
family_id:
parent_baseline_contract:
operator_id:
target_capability_id:
parameter_delta:
unchanged_dimensions:
hard_veto_ref:
transition_ref:
exposure_scalar_ref:
runner_ref:
metric_contract_ref:
threshold_policy_ref:
pit_lineage_refs:
expected_failure_target:
behavior_descriptor:
```

Placeholder 不得进入 selected set。Structural fingerprint 必须覆盖 changed capability、operator、trigger/timing/ramp/cap/expiry/hard-veto axes，并拒绝同 operator、target、axis 且参数落入 near-duplicate tolerance 的候选。不得使用 `top-3` 作为 evidence-free 标签；candidate count 允许为 0 或 1。

Status model：

```text
DRAFT
CONTRACT_READY
SCREENING_ELIGIBLE
SCREENED_PASS
SCREENED_FAIL
SCREENING_BLOCKED
FULL_PIT_ELIGIBLE
```

## 6. TRADING-2438N4 — Conditional multi-fidelity screening

N4 仅消费 N3 contract-ready candidate。

F0 在 replay 前检查：

```text
baseline target capability ready
candidate delta typed and bounded
hard-veto dependency ready
transition dependency ready
exposure unit ready
PIT lineage complete
runner mapping resolvable
six metrics computable
threshold policy pre-registered
structural duplicate=false
```

F1 使用预注册 casebook：false-risk-off、slow-recovery、re-entry reversal、high-volatility veto、rate shock、turnover/whipsaw stress。F1 可 reject，不能 promotion qualification。

Successive halving：

```text
contract-ready candidates
→ F1 casebook
→ behaviorally distinct survivors
→ F2 actual-path slices
→ threshold-feasible survivors
→ F3 full PIT replay
```

Retention 同时考虑 hard-gate feasibility、primary metric improvement、risk non-inferiority、behavioral novelty 和 evaluation cost；return improvement 不得覆盖 hard-risk failure。

## 7. Test plan

N1：family closes with `pit_candidates_tested=0`；exact M1E blockers preserved；A/C REDEFINE、B WITHDRAW；null metrics不推断FAIL；reopen需独立 baseline evidence；promotion/production禁用。

N2：callable-but-unconsumed不是 mutation-ready；missing PIT→BLOCKED；diagnostic-only不能生成候选；hard-veto/exposure dependency缺失阻断；current/requested/applied保持独立；不改变 baseline outputs；artifact reload保真。

N3：blocked capability不能建候选；全部 ref可解析；placeholder、immutable change、hard-veto removal、exposure substitution、structural duplicate均拒绝；A/C-like duplicate被去重；declaration order不是 ranking；candidate count可0/1。

N4：F0拒绝 missing policy/metrics；F1不能 promotion；empty event/null metric fail-closed；successive halving保留 behavioral novelty；return不能覆盖 hard-risk fail；F1/F2 fail不得调用F3。

Governance：N3/N4 不集成新 signal、不创建 baseline behavior；registry/catalog/system flow/task register一致；无 production/broker effect。

## 8. Definition of done

N1：current family formally closed；negative-result ledger updated；exact blockers preserved；reopen policy registered；closed-family M2 route disabled。

N2：graph来自实际 runtime/config contract；所有 node 分类；mutation-ready list显式；无 baseline behavior change。

N3：仅在 conditional gate 通过后实现 typed grammar；only capability-ready candidates；refs fully resolvable；duplicate gate active；zero candidates为有效结果。

N4：仅在 N3 有 screening-eligible candidate 时实现 F0/F1；full PIT只保留给 survivors；所有 evidence observe-only。

## 9. Research KPI 与 immediate target

```text
contract_ready_candidate_rate
candidate_blocked_before_replay_rate
structural_duplicate_rejection_rate
high_cost_replays_per_screened_survivor
median_time_to_first_exact_blocker
full_pit_runs_per_qualified_candidate
```

```text
zero placeholder candidates
zero candidates requiring new baseline behavior
100% of selected candidates contract-ready before selection
```

## 10. Final route

先执行 N1，再执行 N2。只有 N2 识别出至少一个 mutation-ready capability 时才允许 N3 生成新 candidate set；否则 N3/N4 保持 `NOT_STARTED_NO_MUTATION_READY_CAPABILITY`，不得补造候选或 replay。

## 11. 2026-07-10 实施结果

N1 strict closure 已完成：

```text
status = GROWTH_TILT_CANDIDATE_FAMILY_CLOSED_NO_EXECUTABLE_PIT_CANDIDATE
candidate dispositions = 0 APPROVE / 2 REDEFINE / 1 WITHDRAW
replacement-A prerequisites = 2 PASS / 8 BLOCKED
baseline adapters = 0 READY / 4 BLOCKED
pit_candidates_tested = 0
M2 eligible candidates = 0
```

N2 strict capability graph 已完成：

```text
nodes / edges = 21 / 15
READY / BLOCKED / DIAGNOSTIC_ONLY / NOT_APPLICABLE = 6 / 9 / 2 / 4
mutation_ready_capability_count = 0
n3_status = NOT_STARTED_NO_MUTATION_READY_CAPABILITY
n4_status = NOT_STARTED_NO_CONTRACT_READY_CANDIDATE
candidate_generation_run = false
replay_run = false
```

因此本轮条件路由终止于 N2。N3/N4 记录为 `DEFERRED`，不是失败，也不是已执行后无候选；只有 candidate-independent baseline work 产生受治理且通过 N2 全部门禁的新 capability evidence 后，才可重新打开。当前未新增 baseline behavior、candidate、threshold、transition、persistence、veto、exposure unit、PIT replay、promotion、paper-shadow、production weight 或 broker action。

验证结果：N1/N2 focused 29 项、M1C/D1A/D2/M1E/N1/N2 相邻链 84 项、`fast-unit` 198 项、`contract-validation` 197 项、`report-validation` 55 项、documentation contract 1358 reports 与 task-register consistency 13 checks 均为 PASS。
