# TRADING-2510：QQQ Options Primary Window Policy Calibration Evidence V1

最后更新：2026-08-12

稳定任务 ID：`TRADING-2510_QQQ_OPTIONS_PRIMARY_WINDOW_POLICY_CALIBRATION_EVIDENCE_V1`

优先级：`P0`

状态：`BASELINE_DONE`

mode：`SINGLE_LANE`

production effect：`none`

broker action：`none`

## 1. 目标

把 TRADING-2502/2509 中已由 Project Owner 选择为 G3
`EVIDENCE_CALIBRATION_REQUIRED` 的 policy slots 转换为可重放、typed、export-safe、fail-closed
的 primary-window calibration evidence intake 与 readiness report。该任务只建立证据合同、证据请求清单、
admission/replay 与 Owner review readiness；不选择或建议投资阈值，不把 G3 转换为 G2，不建立 executable
policy，也不激活 selection、engine、backtest 或外部平台动作。

## 2. 冻结基线与继承

- registration base / latest main：`22d59f0e8b2067ab34d2b70ff201533f566549da`；
- 2509 v2 policy file SHA-256：
  `d4f7fb3ffb196ce65000ec24fc302c44395a9d3c4dad3e2e5554683639f9ca79`；
- 2509 v2 policy canonical SHA-256：
  `9ac542b464ba4417d67fb626dc820d2e7e331c3c154951590fdf7a409ab67272`；
- Owner attestation raw/canonical SHA-256：
  `8345a55a73df022ef70cb57d6d8df4d6c498cafb091647ef8e27c835cde6fccc`；
- 2509 catalog inventory：37 slots=`24 unchanged + 8 split successors + 5 added axes`；
- primary research start 固定为 `2021-02-22`；`2022-12-01` 不是默认值；
- 继承 2481 shared records/envelope、2482 DQ/PIT identity、2499 DAILY chronology、2500 reviewed
  Free-tier DAILY capability facts、2504 manifest、2507 admission 与 2509 v2 migration authority，不复制
  shared record 或弱化任何 fail-closed gate。

## 3. Owner 授权的 exact calibration scope

G3 evidence scope 只包含当前 attestation 中 18 个 G3 slots：

- selection：`SEL_DELTA_SOURCE_RANGE`、`SEL_DTE_WINDOW`、`SEL_MONEYNESS_RANGE`、
  `SEL_OPEN_INTEREST_FLOOR`、`SEL_QUOTE_FRESHNESS`、`SEL_RANK_PRIORITY`、
  `SEL_SPREAD_LIMIT`、`SEL_VOLUME_FLOOR`；
- execution：`EXE_MARKETABLE_LIMIT`、`EXE_QUOTE_DISPOSITION`；
- accounting/acceptance：`ACC_CASH_RESERVATION`、`ACC_DQ_PIT_REPRO`、`ACC_FEE_SCHEDULE`、
  `ACC_RESULT_INCLUSION`、`ACC_SAMPLE_COVERAGE`、`ACC_SIZING_EXPOSURE`；
- lifecycle：`LIFE_EXPIRY_EXIT_GUARD`、`LIFE_TERMINAL_VALUATION`。

G1 slots 继续 unresolved/blocked；G4 slots 只能作为明确标注的 isolation/sensitivity，不得成为 reality
baseline；2509 新增的 5 个 `OWNER_ACTION_UNRESOLVED` axes 不在本次 calibration admission scope；G5 inventory
为空。上述集合必须从 tracked attestation + v2 migration 机械派生，不能由调用者自报或在 config 中静默扩张。

## 4. Evidence admission 合同

每个 admitted calibration evidence item 必须至少绑定：

- slot id、evidence class、relative path、schema version、file/content SHA-256；
- requested/evaluated range、primary role、as-of session、reviewed XNYS session inventory；
- provider/dataset/source checksum identity 与 export-safe derived aggregate 声明；
- canonical DQ report/receipt path、scope、as-of、checksum 与 15-check PASS identity；
- no raw option rows、no raw export、no external action、no investment interpretation；
- deterministic metric-definition identity；输入排列不得改变 bundle/report identity。

Admission 必须读取并校验 canonical evidence bytes，不能信任调用者构造的 PASS declaration。DQ
`FAIL`/`UNKNOWN`/`NOT_EVALUATED`、scope/range/as-of/hash mismatch、symlink/path escape、duplicate slot、G1/G4/新增轴
伪装成 G3、pre-window primary evidence、raw-row exposure 或缺失 provenance 全部 fail closed。

## 5. 输出与状态机

任务实现 deterministic sealed：

1. calibration evidence requirement catalog；
2. evidence bundle admission receipt；
3. per-slot coverage/readiness report；
4. Owner review handoff manifest。

没有真实 primary-window calibration evidence 时，canonical report 必须为
`EVIDENCE_NOT_PROVIDED_POLICY_BLOCKED`；有部分证据时只能为
`PARTIAL_EVIDENCE_POLICY_BLOCKED`；全部 18 slots 的 evidence admission PASS 也最多达到
`READY_FOR_OWNER_POLICY_REVIEW_NOT_EXECUTABLE`。任何状态都保持：

- `owner_policy_value_count=0`；
- `executable_policy_authorized=false`；
- `engine_status=POLICY_BLOCKED_CASH_PRESERVATION`；
- `selection_authorized=false`；
- `orders=0`、`fills=0`；
- `external_action_authorized=false`；
- `investment_interpretation_allowed=false`；
- `production_effect=none`、`broker_action=none`。

## 6. 实现与验证计划

### S0：登记与 authority audit

- canonical task row + requirement；
- exact main、2502/2507/2509 hashes、18-slot G3 inventory 与 no-threshold audit；
- governed START/LANE preflight。

### S1：serial contract wave

- task-owned policy、typed models、canonical seal/replay、strict loader；
- evidence item/bundle/readiness report 的 discriminated schema；
- v2 migration/attestation mechanical scope derivation。

### S2：fail-closed coverage

- unit/property/golden：permutation identity、duplicate/unknown slot、G1/G4/added-axis injection、
  forged PASS、DQ semantic FAIL/UNKNOWN、scope/range/as-of/hash mismatch、pre-window primary、raw-row flag、
  symlink/path escape、partial/complete readiness 与 cash-preservation；
- 不使用真实 raw options rows，不执行 QuantConnect/cloud/API/CLI/HTTP/Object Store 动作。

### S3：共享 wiring 与收口

- 更新 system flow、architecture fragments、task registry/generated/compatibility authority；
- Atlas 只披露 2510 evidence-contract/readiness，不把 calibration engineering 绿色解释成策略有效；
- focused/adjacent/compatibility 后，在 final tree 串行完成
  Architecture→Contract→Integration→Reproducibility→exclusive Full；
- ordinary non-force push、SHA verify、task branch/worktree cleanup。

## 7. 验收标准

1. exact 18-slot G3 scope 从 canonical attestation/v2 migration 派生，非授权 slot 无法 admission。
2. evidence bytes、DQ/PIT、primary window、session/source/checksum identity 全部严格重放。
3. empty/partial/complete evidence 使用 typed 状态，complete 也不越权形成 policy value 或 executable policy。
4. 无新增投资阈值、无 raw export、无 engine/backtest/external/production/broker action。
5. focused、compatibility、generated authority 与 final five-tier gates PASS。

### 7.1 BASELINE_DONE 实现事实

- task-owned policy 固定 primary start、18-slot G3 scope、exact 15-check DQ contract 与全部 no-effect
  safety；production evidence inventory 为空；
- `primary_window_policy_calibration` 从 2509 migration/attestation 机械派生 scope，读取 evidence 与 2481
  `DQReportRecord` canonical bytes，并输出 catalog、bundle receipt、readiness 与 Owner handoff 四类 sealed
  records；
- empty/partial/complete 状态分别为 `EVIDENCE_NOT_PROVIDED_POLICY_BLOCKED`、
  `PARTIAL_EVIDENCE_POLICY_BLOCKED`、`READY_FOR_OWNER_POLICY_REVIEW_NOT_EXECUTABLE`；complete 仍不生成
  policy values 或 executable policy；
- negative coverage 包含 forged PASS、DQ FAIL/NOT_EVALUATED/UNKNOWN、scope/range/as-of/source/hash、
  duplicate/non-G3、pre-window primary、raw rows、path traversal/symlink 与 permutation replay；
- focused 2510 tests `28 passed`，core+page focused `63 passed`，2481/2482/2485–2509 adjacent
  mechanics `276 passed`；没有 QuantConnect/cloud/API/CLI/HTTP/raw/paper/live/broker/production 动作；
- final formal artifacts 必须来自包含本状态写回与全部 generated authority 的 exact final tree；artifact locator
  只在 terminal handoff 回传，不在 Full 后修改 tracked bytes。

## 8. 后继与 blocker

本任务完成后仍需 Project Owner 审阅 calibration evidence，并对拟采用的每个 slot 另行提供 G2 typed policy
value、rationale、evidence refs、effective/review/expiry metadata。只有独立 executable-policy serial contract
wave 完成并通过 DQ/PIT 与 acceptance gates 后，才可评审是否解除 DAILY engine blocker。

当前 blocker：`PRIMARY_WINDOW_DERIVED_CALIBRATION_EVIDENCE_NOT_PROVIDED`。
