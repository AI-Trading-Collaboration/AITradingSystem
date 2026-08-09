# TRADING-2504：QQQ Options Owner Decision Manifest Canonicalization V1

最后更新：2026-08-09

稳定任务 ID：`TRADING-2504_QQQ_OPTIONS_OWNER_DECISION_MANIFEST_CANONICALIZATION_V1`

优先级：`P0`

状态：`BASELINE_DONE`

计划模式：`SINGLE_LANE`

合同变更：`true`

## 1. 背景与目标

TRADING-2502 已冻结 28 个 Owner 待决 slot、五种治理动作 G1–G5、默认
`POLICY_BLOCKED_CASH_PRESERVATION` 与真实 DAILY engine 的解锁前置条件。Web GPT Pro 对 exact
commit `ebe08339f16fef2c3f27d6c3cbb36665d4bb7d45` 的预审指出：当前 reader-safe pack 尚缺少一个
可机械验证、可 canonical seal/replay 的 Owner decision manifest 合同，尤其需要消除 accounting 与
acceptance 同用 `ACC_*` 前缀造成的 group 推断歧义，并把 `PER_SLOT` 完整性、G2 typed value、G5 rationale
与跨层 dependency 约束变成机器可拒绝的规则。

Owner 已明确“采纳 GPT Pro 建议”。本任务只完成最小串行合同波：冻结 slot catalog、真实 G1–G5 语义、
canonical Owner decision manifest、strict resolver、确定性 identity 与验证。它不替 Owner 选择任何动作或
数值，不生成正式策略 policy，不实现或运行真实 DAILY engine。

## 2. 本地权威优先与审阅纠错

Web GPT Pro 是 advisory reviewer，不是项目 authority。实现必须保留 TRADING-2502 已冻结的真实语义：

- G1：`KEEP_UNRESOLVED_BLOCKED`；
- G2：`OWNER_SUPPLIED_REVIEWED_POLICY`；
- G3：`EVIDENCE_CALIBRATION_REQUIRED`；
- G4：`SENSITIVITY_ONLY_NOT_REALITY_BASELINE`；
- G5：`NOT_APPLICABLE_WITH_REVIEWED_RATIONALE`。

预审答案中对 G1–G5 的任何重命名或重新解释均明确不采纳。实现不得用 Web 文本覆盖 TRADING-2502、
2481–2499 或 AGENTS.md 的 frozen authority。

## 3. 继承边界

本任务从 exact registration base
`ebe08339f16fef2c3f27d6c3cbb36665d4bb7d45` 继承：

- TRADING-2481 shared record/envelope/enum/canonical identity；
- TRADING-2482 DQ/PIT、UNKNOWN fail-closed、cache/evidence identity；
- TRADING-2484 adapter no-pretend-engine boundary；
- TRADING-2485–2488 selection/execution/accounting/lifecycle mechanics 及 policy blockers；
- TRADING-2493 aggregate `NO_GO_KEEP_BLOCKED` 对 broader minute/license/export/range 路径的支配；
- TRADING-2499 strictly offline DAILY primary contract 与 exact primary start=`2021-02-22`；
- TRADING-2500 `GO_FOR_DAILY_ENGINEERING_ONLY`，不得扩展为 policy、完整历史或投资结论 authority；
- TRADING-2502 pack LF SHA-256=
  `afdcb44f44032fee958d4f6b1e8e4b56c1edb2faefa44026e16aff7153968588`；
- TRADING-2502 authority-set SHA-256=
  `1702d50c135204f1d92405cfaf4da7c3a06dae0bb09f2095d68ea388390e687c`。

本任务不得复制或重定义 2481 shared records、2482 DQ/PIT、2484 adapter、2485 selector、2486 execution、
2487 accounting、2488 lifecycle 或 2499 DAILY request/descriptor。新合同只描述 Owner 决策输入及其
materialization，不是 engine 参数对象。

## 4. Canonical slot catalog

必须冻结恰好 28 个 slot。每个 catalog entry 至少包含：

- `slot_id`；
- `canonical_group`，只能是 `selection`、`execution`、`accounting`、`lifecycle`、`acceptance`；
- `evidence_class`；
- `requires`；
- `blocks`；
- G2 `value_schema`；
- `investment_interpretation_relevant=true`。

`slot_id -> canonical_group` 是 immutable authority。resolver 不得再从 `SEL_`、`EXE_`、`ACC_`、`LIF_`
前缀猜 group；因此 `ACC_*` 同时出现在 accounting 与 acceptance 不会产生歧义。slot 缺失、重复、未知、
额外、group mismatch 或 catalog hash mismatch 一律 `INVALID`。

catalog 必须同时表达跨层 dependency DAG，例如 selection 依赖 DQ/PIT；execution 依赖 selection 与 quote；
accounting 依赖 fill identity；lifecycle 依赖 position/accounting；acceptance 依赖上述各层完整结果。DAG 只能
用于 fail-closed 验证，不能把上游 PASS 推断为下游已评估或已授权。

Corporate action 继续继承 TRADING-2488 的 hard stop；它不是 2502 中可由 Owner 选择 G1–G5 绕开的 slot。

## 5. Group choice 与 PER_SLOT 全函数

每个 canonical group 必须恰好选择一种输入模式：

1. group-level G1、G3 或 G4；或
2. `PER_SLOT`。

group-level 模式禁止附带任何 slot override，以消除优先级歧义。`PER_SLOT` 必须为该 group 的每个 catalog
slot 恰好提供一个 G1–G5 action；少一个、重复一个、跨 group、未知或额外 slot 均为 `INVALID`。输入顺序
不得改变 materialized result、canonical bytes 或 SHA-256。

G2 和 G5 只允许在 `PER_SLOT` 中逐项选择，因为它们分别需要 slot-specific typed value 或 rationale。

## 6. G2 typed Owner value

每个 materialized G2 slot 必须恰好携带一个 typed Owner value，并严格匹配 catalog 的 `value_schema`。
payload 至少包含：

- `value_kind` 与 canonical `value`/rule；
- `owner`、`policy_id`、`policy_version`、`policy_status`；
- `rationale`、`intended_effect`；
- `evidence_refs`；
- `reviewed_at_utc`、`review_condition`；
- `expires_at_utc` 或显式 reviewed no-expiry rationale。

非 G2 slot 携带 Owner value、G2 value 缺失/重复、空值、类型错误、未 reviewed status、缺 evidence 或缺
review/expiry condition 均为 `INVALID`。本任务的 fixture 只能使用明显的合同测试值，不能成为 reviewed
策略阈值、现实 baseline 或投资建议。

## 7. G5 signed rationale

每个 materialized G5 slot 必须恰好携带 `rationale` 与 `impact_scope`，并进入 signed/canonical payload。
缺失、空白、重复或未说明“不适用如何不产生隐式默认”均为 `INVALID`。非 G5 slot 携带 G5 rationale 也必须
拒绝，避免游离文字改变解释而不改变 identity。

## 8. Canonical Owner decision manifest

manifest 至少绑定：

- schema/version 与 exact repository commit；
- 2502 pack LF SHA、2502 authority-set SHA、2504 catalog/policy SHA；
- primary requested/evaluated start=`2021-02-22` 及 role=`PRIMARY`；
- 五个 group input modes；
- 展开后的恰好 28 个 materialized actions；
- G2 typed values 与 G5 rationale/impact；
- Owner decision token、decision date、independent reviewer；
- `confirmed_no_engine_activation=true`；
- `confirmed_no_external_action=true`；
- `content_sha256`。

canonical serialization 固定为 UTF-8、LF、sorted keys、无 BOM、确定性 separators。`canonical_bytes`、
`canonical_sha256`、`seal` 与 `from_json_bytes` 必须互相闭合；未知字段、重复 JSON key、非 canonical bytes、
tampered content hash、wrong pack/authority/catalog/repository identity 均 fail closed。

Owner 未真实提交 exact decision 时，系统只能构造未签署模板/验证错误，不能生成“已 reviewed”的默认
manifest。当前 TRADING-2502 状态继续为 `BLOCKED_OWNER_INPUT`。

## 9. Resolver 输出与安全状态

strict resolver 只接受通过 canonical manifest parser 的事实，不信任调用者自报 PASS。其输出至少包含：

- `validation_status`；
- 28-slot materialized decision inventory；
- unresolved/calibration/sensitivity/not-applicable/reviewed-policy 分类；
- blocking reasons；
- dependency audit；
- deterministic content identity；
- `engine_status=POLICY_BLOCKED_CASH_PRESERVATION`；
- `selection_authorized=false`；
- `orders=0`、`fills=0`；
- investment/paper/live/broker/production=`false/none`。

即使 28 slot 全部形式有效，本任务也不得把结果解释为 engine activation authority；正式 policy adoption、
engine implementation、回测执行和投资结论分别需要独立后继任务与 Owner authority。

## 10. Public API 与文件范围

计划新增：

- `config/research/qqq_options_owner_decision_manifest_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/owner_decision_manifest.py`；
- `tests/test_qqq_options_owner_decision_manifest.py`。

Public API 已冻结为：

- `DEFAULT_QQQ_OPTIONS_OWNER_DECISION_MANIFEST_POLICY_PATH`；
- `OwnerDecisionCanonicalGroup`、`OwnerDecisionAction`、`OwnerDecisionGroupMode`、
  `OwnerDecisionValueKind`、`OwnerDecisionEvidenceClass`、`OwnerDecisionResolutionStatus`；
- `OwnerDecisionValueSchemaPolicy`、`OwnerDecisionSlotPolicy`、
  `OwnerDecisionActionSemanticPolicy`、`OwnerDecisionDependencyEdgePolicy`、
  `OwnerDecisionCorporateActionHardStop`、`OwnerDecisionManifestSafety`、
  `QQQOptionsOwnerDecisionManifestPolicy`、
  `QQQOptionsOwnerDecisionManifestPolicyLoadResult`；
- `OwnerDecisionGroupChoice`、`OwnerDecisionSlotChoice`、
  `OwnerDecisionMaterializedSlot`、`OwnerReviewedPolicyValue`、
  `OwnerDecisionNotApplicableRationale`；
- `QQQOptionsOwnerDecisionManifest`、`OwnerDecisionDependencyAudit`、
  `QQQOptionsOwnerDecisionResolutionResult`、
  `QQQOptionsOwnerDecisionManifestContractError`；
- `load_qqq_options_owner_decision_manifest_policy`、
  `build_qqq_options_owner_decision_manifest`、
  `resolve_qqq_options_owner_decision_manifest`。

两个 sealed record 均提供 `seal` / `canonical_bytes` / `canonical_sha256` /
`from_json_bytes`；module 不导入或调用真实 engine。

## 11. 验收测试

至少覆盖：

- 28-slot exact catalog、immutable group mapping 与 catalog hash golden；
- accounting/acceptance `ACC_*` 前缀不再参与 group 推断；
- group-level G1/G3/G4 deterministic materialization；
- group-level + override、G2/G5 group-level 均拒绝；
- PER_SLOT missing/duplicate/extra/unknown/cross-group 全部拒绝；
- G2 exactly-one typed value、schema/status/evidence/review/expiry negative；
- G5 rationale/impact exactly-one negative；
- dependency DAG、corporate-action hard stop、DQ/PIT NOT_EVALUATED 不升级；
- input permutation 不改变 replay/hash；
- duplicate JSON key、non-canonical bytes、unknown field、hash/identity tamper 拒绝；
- 默认 unsigned/unauthorized 路径输出 typed cash-preservation/no-order/no-fill；
- primary start 不是 `2021-02-22` 或 role 不是 `PRIMARY` 时拒绝；
- unit/property/golden/compatibility/architecture/contract/integration/reproducibility/Full 在 final tree PASS。

## 12. 非目标与禁止事项

- 不填写、推荐或暗示 DTE/moneyness/delta/spread/OI/volume/quote freshness/fee/slippage/latency/
  partial-fill/cancel/expiry/sizing/cash/acceptance 数值；
- 不替 Owner 选择 G1–G5，不代签 decision token；
- 不修改 2502 pack exact bytes；
- 不实现真实 DAILY engine，不运行策略回测，不生成收益/投资结论；
- 不执行 QuantConnect login/project/cloud backtest/API/CLI/HTTP/Object Store；
- 不下载、记录或导出 raw option rows；
- 不执行 paper/live/broker/production；
- 不修改 TRADING-2503 Atlas projection/page contract。

## 13. 阶段与状态

1. C0 registration boundary：task row + 本 requirement，ordinary push；
2. C1 serial contract freeze：catalog、G semantics、manifest schema、canonical identity；
3. C2 strict implementation：loader/builder/parser/resolver；
4. C3 focused/property/golden/compatibility；
5. C4 final-tree formal gates、ordinary push、cleanup；
6. C5 Owner handoff：2502 仍等待真实 typed Owner decision；本任务不自动进入 policy adoption。

## 14. Final contract freeze

- registration ordinary-pushed main=`2da20cf05ec6d31c2c4cb9d7c6ce797c9128f301`；
- `SINGLE_LANE --contract-change` START/LANE preflight=`PASS`；
- policy file SHA-256=
  `55fb29bb2e4347959920cd3f5d72cbc5fc94c2aac5794f301e1c41f9a31547de`；
- policy canonical SHA-256=
  `c872e9aee37cf2ea36b201d81c48c98603ca4daa96c53d900cdaa5997e13f0db`；
- slot catalog SHA-256=
  `a1492e27ea8599d453249e5d29280d6fcb882ea0376ce531b949eae7b7621ad6`；
- module LF SHA-256=
  `ab80c9e3b8bca03d9bc6c72eb568c7ddeb8420364e60a615bec7d936277a0b77`；
- focused failure-fix chain=`13 PASS / 8 FAIL -> 20 PASS / 1 FAIL -> 21 PASS`，最终同覆盖
  `21 PASS`；
- 2481–2504 QQQ adjacent=`658 PASS`；Ruff、strict mypy、compileall=`PASS`；
- DevEx=`1099 modules / 1262 tests / 856 writers / 0 violations`；
- task shadow=`971 / 466 / 505` byte-identical。

C0–C3 已完成，C4 final-tree compatibility/formal gates 只验证本节冻结的 tracked bytes；门禁后不再修改
tracked 内容。任务状态为 `BASELINE_DONE`，表示 canonical decision-input contract 已建立，不表示 Owner 已
提供 2502 decisions、policy 已 adopted、engine 已激活或 backtest 已运行。后继 blocker=
`TRADING_2502_TYPED_OWNER_DECISIONS_NOT_YET_PROVIDED`。
