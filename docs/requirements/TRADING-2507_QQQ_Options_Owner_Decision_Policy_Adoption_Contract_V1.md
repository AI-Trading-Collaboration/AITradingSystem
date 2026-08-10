# TRADING-2507：QQQ Options Owner Decision Policy Adoption Contract V1

最后更新：2026-08-10

稳定任务 ID：`TRADING-2507_QQQ_OPTIONS_OWNER_DECISION_POLICY_ADOPTION_CONTRACT_V1`

优先级：`P0`

状态：`BLOCKED_OWNER_INPUT`

计划模式：`SINGLE_LANE`

合同变化：`true`

## 1. 背景与目标

TRADING-2502 已冻结 28 个 selection、execution、accounting、lifecycle/acceptance policy slots，
TRADING-2504 已冻结 canonical slot catalog、G1–G5 语义、group/PER_SLOT 展开、G2 typed value、
G5 rationale 与 deterministic manifest/resolution。当前缺口不是 slot inventory，而是系统仍可由调用者分别传入
`group_choices`、`slot_choices`、`owner_policy_values` 与 `not_applicable_rationales`；尚无独立、canonical、
exact-hash-bound 的 Owner attestation bytes 作为唯一输入事实。

本任务实施 Web Pro 对 exact commit `ab8becdadb9a9dad6c09025007a7b9883448c23f` 的预审建议，
并以本地 latest-main authority 为最终约束。目标是建立最小 serial contract wave：严格解析 canonical Owner
attestation，从 attestation 事实派生 2504 manifest，再生成 contract-only policy adoption plan。没有真实
Owner attestation 时必须在第一处 admission gate 停止，不能把工程默认、聊天文本、fixture、旧页面或独立函数参数
解释为 Owner 决定。

## 2. 继承与不可重定义边界

实现必须继承 TRADING-2481–2504、TRADING-2499 与 TRADING-2502 的 current exact authority，尤其是：

- 2481 shared records、envelope、canonical seal/replay 与 safety boundary；
- 2482 DQ/PIT、UNKNOWN/NOT_EVALUATED fail-closed 与 evidence identity；
- 2485–2488 selector/execution/accounting/lifecycle mechanics，继续不得填入 policy thresholds；
- 2499 DAILY primary contract、primary start=`2021-02-22`、daily-close/same-bar fill 禁止；
- 2502 decision pack 的 28-slot inventory、evidence requirements 与 cash-preservation；
- 2504 `qqq_options_owner_decision_manifest.v1`、canonical group/action/value schema、slot catalog、
  dependency audit 与 resolution safety fields。

本任务不得复制或改写 2502/2504 frozen bytes，不得重新命名 G1–G5，不得让 slot 名称前缀替代
`slot_id -> canonical_group` authority，不得从 attestation contract 推导 DQ/PIT PASS、engine readiness、
selection authorization、orders、fills、投资解释或 production readiness。

## 3. Task-owned contract

计划新增：

- `config/research/qqq_options_owner_policy_adoption_contract_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/owner_policy_adoption_contract.py`；
- `tests/test_qqq_options_owner_policy_adoption_contract.py`；
- 本 requirement。

必要的 task registry、`docs/system_flow.md`、architecture fragments、DevEx manifests、task shadow 与
compatibility authority 由 coordinator 在最终树统一重建；不得由并行 lane 分别修改。

## 4. Public API 目标

最小 public API：

- `OwnerDecisionAttestationRecord`；
- `OwnerDecisionAttestationLoadResult`；
- `OwnerDecisionAttestationError`；
- `QQQOptionsPolicyAdoptionPlan`；
- `QQQOptionsPolicyAdoptionResolution`；
- `SlotCatalogAmendmentResolution`；
- `load_owner_decision_attestation`；
- `build_policy_adoption_plan`；
- `resolve_policy_adoption_plan`。

所有 public record 必须提供 canonical bytes/hash/replay authority，拒绝 duplicate JSON keys、unknown fields、
non-canonical bytes、hash tamper、wrong repository commit、wrong pack/policy/catalog identity 与输入排列导致的
identity 漂移。

## 5. Canonical Owner attestation admission

`OwnerDecisionAttestationRecord` 至少 exact-bind：

- schema/version、record id、created-at UTC；
- `owner_decision_id`、decision date、`owner_id`、independent reviewer id；
- exact repository code SHA；
- 2502 pack LF SHA、authority-set SHA；
- 2504 policy file SHA、canonical SHA、slot catalog SHA；
- exact group choices、PER_SLOT choices、G2 values、G5 rationales；
- reviewed catalog-amendment dispositions；
- `confirmed_no_engine_activation=true` 与 `confirmed_no_external_action=true`。

Loader 必须只从调用者提供的 raw attestation bytes 派生上述事实，记录 raw byte SHA 与 canonical semantic SHA，
再把派生值传给 2504 builder。禁止同时接受另一组 caller-supplied choices/values/rationales；否则两份事实可能
分叉。attestation 缟失或 identity 不匹配时，第一处 typed stop 必须发生在 adoption plan 之前。

## 6. Adoption plan 语义

`build_policy_adoption_plan` 只能消费已通过 admission 的 attestation 与 2504 canonical manifest/resolution，
并且：

1. 每个 frozen slot 恰好一次，总数严格为 28；
2. group/PER_SLOT 选择只从 attestation bytes 派生；
3. G2 必须具备 2504 value schema 所需字段，并把 owner、policy id/version/status、rationale、intended effect、
   evidence、review/expiry condition exact-bound；
4. G5 必须具备 reviewed rationale 与 impact scope；
5. G3/G4 只产生 calibration/sensitivity planning metadata，不能成为 policy value、reality baseline 或 engine
   authorization；
6. G1 保持 unresolved；
7. dependency、corporate-action hard stop 与 downstream group completeness 必须复用 2504 resolution；
8. 最大有效状态为 `VALID_POLICY_ADOPTION_CONTRACT_ONLY`。

真实 executable policy 的写入、2502/2504 status 变更、2485–2488 activation 与 DAILY engine implementation
必须另立后继任务；2507 不隐式执行这些动作。

## 7. Versioned slot-amendment proposal

Web Pro 指出若干 composite slots 同时混合 mechanic invariant 与 Owner policy。2507 只记录 proposal，不能改写
2502/2504 frozen v1 catalog。`SlotCatalogAmendmentResolution` 必须为每项返回 typed disposition：
`OWNER_REVIEW_REQUIRED`、`ACCEPTED_FOR_VERSIONED_SUCCESSOR` 或 `REJECTED_WITH_RATIONALE`。

候选 amendment：

- 拆分 `LIFE_CLOSE_HOLD_ROLL`，保证 roll 不能随 close/hold 被隐式解锁；
- 拆分 `LIFE_EXERCISE_ASSIGNMENT`，区分 long exercise/expiry 与 assignment/underlying hard stop；
- 拆分 `ACC_IDENTITY_ROUNDING` 的 deterministic identity invariant 与 policy rounding/reconciliation；
- 拆分 `ACC_SETTLEMENT_COST_BASIS` 的 legal settlement timing 与 research P&L convention；
- 显式区分 selection quote observation 与 execution quote observation identity；
- 增加 `LIFE_TERMINAL_VALUATION -> ACC_RESULT_INCLUSION` dependency；
- 评审候选新轴：`LIFE_POSITION_STATE_TRANSITION`、`EXE_EXECUTION_OBSERVATION_SOURCE`、
  `ACC_CASH_CARRY_BENCHMARK`、`ACC_METRIC_BENCHMARK_IDENTITY`、
  `ACC_RESEARCH_MULTIPLICITY_CONTROL`。

在 Owner 未对 amendment proposal 给出 canonical disposition 前，v1 catalog 继续是唯一运行时 authority；proposal
不得增加第 29 个 frozen decision，也不得被 adoption plan 当作已批准 schema。

## 8. Typed failure taxonomy

至少覆盖：

- `OWNER_ATTESTATION_MISSING`；
- `OWNER_ATTESTATION_NOT_CANONICAL`；
- `OWNER_ATTESTATION_PAYLOAD_MISMATCH`；
- `OWNER_IDENTITY_NOT_BOUND`；
- `SLOT_INVENTORY_INVALID`；
- `G2_METADATA_INCOMPLETE`；
- `G5_RATIONALE_INCOMPLETE`；
- `CATALOG_AMENDMENT_REQUIRED`；
- `AUTHORITY_BINDING_MISMATCH`。

错误不得降级为 warning 后继续 adoption。UNKNOWN、NOT_EVALUATED、unreviewed amendment 或缺失 Owner field
永远不能产生 PASS。

## 9. 默认安全状态

在真实 canonical Owner attestation 与后继 executable-policy wave 完成前，所有结果必须保持：

- task status=`BLOCKED_OWNER_INPUT`；
- policy/plan status 最大为 `VALID_POLICY_ADOPTION_CONTRACT_ONLY`；
- DQ/PIT=`NOT_EVALUATED_BY_THIS_CONTRACT`；
- engine=`POLICY_BLOCKED_CASH_PRESERVATION`；
- selection authorized=`false`；
- orders=`0`、fills=`0`；
- investment interpretation/promotion/paper/live/broker/production=`false/none`；
- external QuantConnect/cloud/API/CLI/HTTP/Object Store/raw export action=`none`。

## 10. 验收标准

1. raw attestation bytes、canonical semantic bytes、2504 manifest 与 adoption plan 都有 deterministic identity；
2. token/date/owner/reviewer/repository SHA 与 2502/2504 authority exact-bound；
3. 28 slots 恰好一次，group/PER_SLOT 仅从 attestation 派生；
4. G2/G5 附件完整，G3/G4/G5 不授权 engine；
5. missing/duplicate/unknown/wrong-group/wrong-commit/wrong-hash/non-canonical/tamper 全部 fail closed；
6. input permutation 不改变 replay/hash；
7. amendment proposal 与 frozen v1 catalog 严格分轴；
8. unit/property/golden/tamper、QQQ adjacent、compatibility/deprecation、Architecture、Contract、Integration、
   Reproducibility 与 exclusive Full 在同一 final tree PASS；
9. task 关闭时仍为 `BLOCKED_OWNER_INPUT`，下一责任人明确为 project owner 提供 canonical typed attestation；
10. 不填阈值、不代签 Owner、不实现真实 DAILY engine、不执行外部或生产动作。

## 11. 实施阶段

1. R0 registration boundary：canonical task event、本文、generated task views/shadow、focused validation、
   ordinary push；
2. R1 contract wave：policy config、attestation loader、adoption plan/resolution 与 tests；
3. R2 authority wiring：system flow、architecture/DevEx/compatibility/task shadow 重建；
4. R3 final validation：focused、adjacent、compatibility 与正式五级；
5. R4 handoff：保持 `BLOCKED_OWNER_INPUT`，向 project owner 交付 canonical attestation schema 与签署说明；
6. R5 successor：只有收到有效 attestation 后，另立 executable policy adoption/engine task。

## 12. Registration boundary

- exact base：`25c24409c3ed58941dbc512419458382e5f72817`；
- registration branch：`codex/trading-2507-owner-policy-adoption-contract-registration`；
- registration-only contract change：`false`；
- 首次允许写入：本文、canonical task registry event 及其 generated views/shadow；
- implementation 必须在 registration ordinary push 后从新 exact main 重新执行
  `SINGLE_LANE + contract_change=true` preflight；
- production effect=`none`，broker action=`none`。
