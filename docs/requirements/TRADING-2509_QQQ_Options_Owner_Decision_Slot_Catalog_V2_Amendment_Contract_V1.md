# TRADING-2509：QQQ Options Owner Decision Slot Catalog V2 Amendment Contract V1

最后更新：2026-08-12

稳定任务 ID：`TRADING-2509_QQQ_OPTIONS_OWNER_DECISION_SLOT_CATALOG_V2_AMENDMENT_CONTRACT_V1`

优先级：`P0`

状态：`IN_PROGRESS`

## 1. 目标

把 Project Owner 对 TRADING-2502 decision pack 的结构选择和 11 项 catalog amendment disposition 封存为可重放、可迁移、fail-closed 的 versioned successor contract。该任务只建立 Owner attestation、slot catalog v2、v1→v2 migration 与 evidence admission 边界；不提供任何 DTE、moneyness、delta、spread、OI、volume、quote freshness、fee、slippage、latency、partial-fill、sizing、cash 或 acceptance 数值，不实现或激活 DAILY engine/backtest。

## 2. 冻结输入

- exact reviewed main：`e08bca3d22c1174e3dc31c14e2a4416ea809c440`；
- 2502 pack LF SHA-256：`afdcb44f44032fee958d4f6b1e8e4b56c1edb2faefa44026e16aff7153968588`；
- authority set SHA-256：`1702d50c135204f1d92405cfaf4da7c3a06dae0bb09f2095d68ea388390e687c`；
- 2504 manifest policy file SHA-256：`55fb29bb2e4347959920cd3f5d72cbc5fc94c2aac5794f301e1c41f9a31547de`；
- 2504 manifest policy canonical SHA-256：`c872e9aee37cf2ea36b201d81c48c98603ca4daa96c53d900cdaa5997e13f0db`；
- v1 slot catalog SHA-256：`a1492e27ea8599d453249e5d29280d6fcb882ea0376ce531b949eae7b7621ad6`；
- Owner decision token：`owner_decision:TRADING-2502:2026-08-11:review_qqq_options_backtest_policy_decision_pack_v1`；
- independent reviewer：`project_owner`。

Owner 选择为：selection group=`G3`；execution/accounting/lifecycle/acceptance=`PER_SLOT`；20 个 per-slot override 使用 Owner 消息中的 exact G1/G3/G4 映射；`owner_supplied_policy_values=NONE`、`not_applicable_rationales=NONE`。11 项现有 amendment disposition 全部为 `ACCEPTED_FOR_VERSIONED_SUCCESSOR`，successor catalog version=`2.0.0`。

## 3. 继承与禁止边界

1. 继承 TRADING-2481–2507 的 shared envelope、DQ/PIT、signal package、adapter、selection、execution、accounting、decision manifest 与 adoption contract authority，不复制或重定义 v1 public records。
2. v1 policy/catalog/record bytes 保持 immutable；v2 必须使用新 schema/version 和显式 migration receipt。
3. 2021-02-22 仍为 primary research window；DQ/PIT `UNKNOWN`/`NOT_EVALUATED` 不得转换为 PASS。
4. Owner token只批准结构选择与 versioned successor，不批准 G2 policy values、G5 N/A rationales、engine、selection、orders、fills、backtest、外部平台动作或投资解释。
5. 默认输出必须保持 `POLICY_BLOCKED_CASH_PRESERVATION`、`selection_authorized=false`、`orders=0`、`fills=0`。
6. 禁止 QuantConnect/cloud/API/CLI/HTTP/Object Store/raw options export/paper/live/broker/production 动作。

## 4. 实现分解

### R1：Owner attestation evidence

- 以 2507 canonical `OwnerDecisionAttestationRecord` 的 `seal/from_json_bytes/canonical_bytes/canonical_sha256` 封存原始 Owner 选择；
- tracked evidence 必须绑定 token、decision date、reviewer、exact main、2502/2504/v1 catalog hashes、28 slots、11 amendment dispositions；
- 任何 slot 缺失/重复、group/override 冲突、非空 G2/G5、hash 漂移或未批准 amendment 都 fail closed。

### R2：slot catalog v2 与 migration

- 建立 immutable `2.0.0` successor catalog 和 v1→v2 deterministic migration；
- 落实已批准的 11 项 amendment：拆分生命周期/identity/settlement composite，增加 quote observation identity、terminal valuation→result inclusion dependency、position state transition、execution observation source、cash carry benchmark、metric benchmark identity、research multiplicity control；
- slot dependency 使用显式 DAG，输入排列不改变 canonical identity；迁移不得填充 Owner policy values。

Web Pro 另指出 `EXE_CANCEL_REJECT_NO_FILL` 与 `ACC_DQ_PIT_REPRO` 仍有 composite 风险。它们不是 2507 typed 11-disposition inventory 的一部分，因此本任务必须将其作为“reviewed successor finding”单独记录并验证边界，不得伪称已由本次 Owner token完成 typed amendment acceptance；若需要改变 v2 public inventory，必须先形成可追踪的 reviewed contract disposition。

### R3：typed evidence admission

- policy evidence reference 必须绑定 relative path、schema version、file SHA-256、content SHA-256、requested/evaluated range、as-of/session identity 与 DQ status；
- G2 后继 schema 必须按 slot 使用 discriminated typed payload，禁止 generic scalar map；
- 当前没有 G2 值，因此所有数值 policy slot 保持 unresolved/policy-blocked。

### R4：验证与收口

- unit/property/golden 覆盖 attestation replay、28-slot exact-once、11 amendments、migration determinism、DAG cycle/unknown dependency、evidence mismatch、forged authorization、cash-preservation；
- 更新 system flow、architecture fragments、task registry/generated/compatibility authority；
- final latest-main tree 完成 Architecture→Contract→Integration→Reproducibility→exclusive Full；
- ordinary non-force push，验证 local main=origin/main，并清理 task branch/worktree。

## 5. 验收标准

1. Owner token 可从 tracked canonical evidence exact replay，hash、slot 和 amendment binding 全部一致。
2. v2 catalog 与 migration receipt deterministic、sealed、fail closed，v1 bytes 未改。
3. 11 项已批准 amendment 有 typed successor 表达；两项新增 review finding 不被误报为已批准。
4. 没有 Owner policy values、无 heuristic threshold、无 engine/backtest/external action。
5. adoption result 最高只能为 contract-only，cash-preservation、zero-order/zero-fill 不变量保持。
6. focused、compatibility 与 final formal gates 全部 PASS，canonical task 状态与 supporting requirement 同步。

## 6. 后继边界

TRADING-2509 完成后仍不能开始正式日级回测。下一自然步骤是独立的 primary-window evidence admission/calibration wave：针对 2021-02-22 起始窗口补齐 reviewed G2 policy evidence 和 Owner policy values；只有后继 executable-policy task 在所有 DQ/PIT、lineage、acceptance 条件满足后，才可另行评审是否解除 engine blocker。
