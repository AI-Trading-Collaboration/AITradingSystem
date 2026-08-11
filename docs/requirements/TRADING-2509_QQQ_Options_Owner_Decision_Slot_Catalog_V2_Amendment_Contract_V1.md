# TRADING-2509：QQQ Options Owner Decision Slot Catalog V2 Amendment Contract V1

最后更新：2026-08-12

稳定任务 ID：`TRADING-2509_QQQ_OPTIONS_OWNER_DECISION_SLOT_CATALOG_V2_AMENDMENT_CONTRACT_V1`

优先级：`P0`

状态：`BASELINE_DONE`

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

## 7. 实现进展（2026-08-12）

- registration ordinary-pushed main：`1d7de7ff08e7253985760eb7e2257f117679b32c`；
- tracked attestation raw/canonical SHA-256：
  `8345a55a73df022ef70cb57d6d8df4d6c498cafb091647ef8e27c835cde6fccc`；
- tracked attestation content SHA-256：
  `2777768003bb81bdeadc72929edeae9db6f1a1d970b25ebf9e050adafa30b57c`；
- v2 policy candidate file SHA-256：
  `d4f7fb3ffb196ce65000ec24fc302c44395a9d3c4dad3e2e5554683639f9ca79`；
- v2 policy candidate canonical SHA-256：
  `9ac542b464ba4417d67fb626dc820d2e7e331c3c154951590fdf7a409ab67272`；
- catalog inventory：37 slots=`24 unchanged + 8 split successors + 5 added axes`；
- focused failure-fix：首轮 `23 passed / 1 failed`，唯一根因为 unknown split source 负例被
  unchanged-inventory 检查先拦截；调整 fail-closed 诊断优先级后，同覆盖 `24 passed`。
- frozen authority 保护复核：一次中间编辑误触 2502 exact-byte requirement，原覆盖产生
  `5 passed / 19 failed`；未降低校验，已完整撤回该编辑并复核 2502 LF SHA-256 恢复为
  `afdcb44f44032fee958d4f6b1e8e4b56c1edb2faefa44026e16aff7153968588`，随后同一
  24-test 覆盖重新 `24 passed`。该轮只作为 failure-fix 证据，不作为正式门禁证据；
- adjacent contract replay：2500 daily retry、2502 decision pack、2507 adoption 与 2509 v2
  合并覆盖 `93 passed`；Ruff、mypy、compileall 均 PASS；
- DevEx current tree：`1108 modules / 1269 tests / 856 writers / 0 violations`；canonical task
  registry generate/validate 与 task shadow validate 均 PASS；
- compatibility/deprecation failure-fix 使用完全相同的
  `python -m pytest -n 16 --dist loadfile tests/test_arch_004_refactor_policy.py tests/test_arch_004g_deprecation.py`
  覆盖：首轮 `210 passed / 1 failed in 306.22s`（inventory id stale）；更新 exact test
  constant 后第二轮 `210 passed / 1 failed in 309.11s`（frozen inventory YAML stale）；仅同步
  canonical inventory id/module/test exact 值后第三轮 `211 passed in 318.74s`。两轮 FAIL
  保留为 focused failure-fix 证据，不作为正式门禁证据。
- 首次 final-tree Full：`8741 passed / 22 failed / 3 skipped / 644 warnings`，artifact=
  `outputs/validation_runtime/full_20260811T161724Z/test_runtime_summary.json`。2509 domain
  tests 无失败；22 个 node 归并为三条 authority/consumer 根因：2502/2507 status update 的
  compatibility projection cells 未保留 requirement link，导致 canonical `requirement_refs=[]`；
  `docs/system_flow.md` 变化后 DEVX-006D source seal/shadow 尚未重建；2509 reviewed successor 尚未
  进入 page-effectiveness exact coverage，local canonical page validation 因而 FAIL。
- failure-fix 不降低任何 gate：以 append-only canonical task events 恢复 2502/2507 requirement
  binding；page-effectiveness coverage 从 27 精确提升至 28，并仅披露“Owner structure + versioned
  successor contract 已批准、G2 values/engine 仍 blocked”；刷新 DEVX-006D、fragmented
  compatibility authority 与 canonical page。修复后 Full 必须使用上述失败 artifact 作为
  `failure_fix_rerun` parent。
- failure-fix 首轮 50-test 原因文件覆盖：`31 passed / 19 failed`；DEVX-006D failures 已清零，
  18 个 Atlas node 同根因收敛为新纳入的 2509 task 自身也缺 requirement ref，另 1 个为旧
  27-task canonical page sidecar。已追加 2509 requirement-binding event；canonical page 必须在
  28-task manifest 下完整重建并保留既有 human review facts 后再以完全相同覆盖复跑。
- 追加 2509 requirement-binding event、重建 registry/DevEx/compatibility authority 后，相同
  50-test 覆盖为 `49 passed / 1 failed in 104.46s`；唯一失败严格收敛为 ignored canonical page
  仍携带旧 27-task sidecar，代码、合同、registry 与 DEVX-006D 均无 node failure。
- 使用现有 canonical writer 完整重建 11 个页面 artifacts；原样保留既有三条独立 acceptance
  facts（`ENGINEERING_VALIDATION=PASS`、`OWNER_VISUAL_REVIEW=PASS`、
  `READER_COMPREHENSION_REVIEW=PASS`）及其 reviewer/time/decision/evidence，不重新签署、不串轨。
  重建时 `index.html` SHA-256=`93f169ff08bc768aab36ca65b1c45e537ce2b83883c70decfedcf7cd123e1970`，
  `page_effectiveness.json` SHA-256=`fc490e72c54e250755e8d771f990ff41735e2e49690838732fa351311e2d36a0`；
  完全相同 50-test 覆盖最终 `50 passed in 108.67s`。

当前实现候选只达到 `VALID_VERSIONED_SUCCESSOR_CONTRACT_ONLY`。新增轴均为
`OWNER_ACTION_UNRESOLVED`，不存在 implicit G1/G2；policy evidence inventory 为空，DQ/PIT、engine、selection、
orders/fills、external action 与 investment interpretation 均保持 fail closed。正式验证与 final exact-main
hash 在收口后补录。

`BASELINE_DONE` 只表示 versioned successor contract 已建立；后继仍由 Project Owner 提供 reviewed G2
per-slot policy values、primary-window canonical evidence 与验收策略。上述输入缺失前，不得建立 executable
policy、解除 cash-preservation 或启动正式日级回测。
