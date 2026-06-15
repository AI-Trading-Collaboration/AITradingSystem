# Research Method Contract

本文件定义 TRADING-346 的 research-only formal method contract。它只把
TRADING-336 到 TRADING-345 的 filtered candidate evidence chain 转成可审计决策契约，
不实现正式 target method，不写 official target weights，不改 portfolio / baseline /
production state，不生成 order ticket，也不触发 broker。

## 适用范围

- candidate: `median_plus_regime_mismatch_filter`
- regime: `ai_after_chatgpt`
- anchor event: ChatGPT public launch on 2022-11-30
- default conclusion window: 2022-12-01 之后的 AI regime evidence
- CLI:
  - `aits etf dynamic-v3-rescue research-method-contract build`
  - `aits etf dynamic-v3-rescue research-method-contract report --latest`
  - `aits etf dynamic-v3-rescue validate-research-method-contract --contract-id <contract_id>`

## Source Artifacts

Contract build 默认读取以下 latest artifacts，也可以通过显式 id 固定输入：

- filtered candidate evidence: expected `PROMISING`
- median regime filter spec: expected `PASS`
- filtered candidate stress backfill: expected `STRONG`
- drawdown mismatch reduction: expected `IMPROVED`
- flip / rotation reduction: expected `flip=IMPROVED; rotation=IMPROVED`
- filtered candidate A/B review: expected `PROMISING`
- signal gate confirmation: expected target count `>=3`
- filtered formalization readiness: expected `READY_FOR_FORMAL_RESEARCH_IMPLEMENTATION`
- owner filtered candidate review: expected `formalize_research_method`
- filtered next decision: expected `FORMALIZE_RESEARCH_METHOD`

`>=3` confirmation targets comes from TRADING-346 scope and is treated as a
contract input for this task. TRADING-348 owns later calibration review.

## Promotion States

| State | Meaning | Allowed next action |
|---|---|---|
| `REJECTED` | Safety boundary failed or core evidence is rejected/missing | Stop or rework evidence |
| `NEEDS_MORE_EVIDENCE` | Required objective gates are incomplete | Collect missing research evidence |
| `PROMISING` | Core evidence is positive but formal gates are incomplete | Collect owner or confirmation evidence |
| `PAPER_SHADOW_ELIGIBLE` | Core paper-shadow gates pass but formal method gates are incomplete | Design a separate paper-shadow protocol |
| `FORMAL_RESEARCH_READY` | All objective gates and safety boundary pass | Implement a research-only formal method in a later task |

`FORMAL_RESEARCH_READY` is not production approval.

## Objective Gates

Every gate records source artifact id/path, observed status, required status,
pass/fail, failure condition, and safety fields. A failed gate creates an active
failure condition and blocks formal research. Paper-shadow eligibility uses the
core evidence gates only: evidence, stress, drawdown mismatch, flip/rotation,
A/B, confirmation count, and safety boundary.

## Failure Conditions

The validation artifact must expose active and inactive failure conditions:

- `safety_boundary_failed`
- `evidence_rejected_or_missing`
- `spec_contract_failed`
- `stress_weak_or_missing`
- `drawdown_mismatch_not_improved`
- `flip_rotation_not_improved`
- `ab_review_weak_or_missing`
- `confirmation_targets_missing`
- `formalization_readiness_not_ready`
- `owner_did_not_request_formalization`
- `next_decision_not_formalize`

No failure condition may be silently smoothed over. If a blocker appears, the
contract must fail closed and the next action must describe the missing evidence.

## Paper-Shadow Eligibility

`paper_shadow_eligibility=ELIGIBLE_FOR_PROTOCOL_DESIGN` means only that a later
TRADING-350-style protocol may be designed. It does not enroll a candidate, write
paper state, switch a primary research candidate, create target weights, execute
orders, or mutate production.

## Safety Boundary

All contract artifacts must expose:

- `research_target_only=true`
- `paper_shadow_only=true`
- `research_screening_only=true`
- `manual_review_only=true`
- `formal_research_contract_only=true`
- `not_formal_research_method=true`
- `not_official_target_weights=true`
- `official_target_weights_mutated=false`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `production_candidate_generated=false`
- `automatic_candidate_promotion=false`
- `auto_apply=false`
- `production_state_mutated=false`
- `baseline_config_mutated=false`
- `production_effect=none`

Validation must fail if the contract, decision, or source payloads expose broker,
order, production mutation, or official target weight side effects.
