# DATA-GOV-002C3P：Price Non-Market-Session Typed Attribution Contract Wave

最后更新：2026-07-27

稳定任务 ID：
`DATA-GOV-002C3P_PRICE_NON_MARKET_SESSION_TYPED_ATTRIBUTION_CONTRACT_WAVE`

Owner decision：
`owner_decision:DATA-GOV-002C3P:2026-07-27:approve_price_non_market_session_contract_wave_v1`

状态：`BASELINE_DONE`

## 1. 决策原文

Decision：`APPROVE_FOR_CONTRACT_WAVE`

Conditions：

- `primary_market_prices` 的适用资产受 reviewed US equity calendar 约束；
- row attribution 必须绑定 exact source artifact/checksum；
- `source_ordinal` 只承诺在同一 source snapshot 内稳定；
- `canonical_row_digest` 的字段集合、规范化和版本必须在 C3 明确定义；
- calendar 或 special-closure policy 漂移必须重新评审；
- 任一归因维度不完整时保持 `GLOBAL_OR_UNKNOWN_SCOPE`；
- C3 通过前不得改变 capability policy、consumer 或生产行为。

该决定只授权最小 serial runtime contract wave，不直接授权 capability policy
cutover、consumer migration、production 或 broker action。

## 2. 当前问题

`prices_non_market_session_date` 当前 runtime issue 只包含：

- `rows`：requested window 内 distinct non-session date 数；
- `sample`：前 10 个 distinct non-session dates；
- `affected_instruments`：触发行 ticker 的去重集合。

它没有绑定完整日期集、requested window、source role、source artifact/checksum、
defective fields 或全部 row identities。当前 capability classifier 仍只按
`affected_instruments` 与 consumer required tickers 是否不相交来执行既有 pilot。

同一 `_check_price_market_calendar_dates` emitter 同时可能由 primary prices 和
secondary self-check 调用。Owner decision 只批准 `primary_market_prices`；secondary
source 不得因复用相同 issue code 自动获得 typed attribution 或隔离 authority。

## 3. 最小合同范围

本任务仅为 exact site
`dq_issue_site_312625a26da21428b763 / prices_non_market_session_date /
_check_price_market_calendar_dates`增加 optional typed attribution。

### 3.1 Source artifact binding

完整 attribution 必须记录：

- `source_role=primary_market_prices`；
- exact captured source path；
- captured source bytes 的 SHA-256；
- requested window start/end；
- US equity calendar source label；
- reviewed special-closure policy id、version 与 SHA-256。

缺少或不匹配任一 binding 时不得生成 complete attribution。

### 3.2 Row identity contract

`source_ordinal`定义为 exact captured CSV snapshot 中零基、按文件解析顺序的物理行序号。
它只在相同 source artifact SHA-256 内有意义，不承诺跨下载或跨快照稳定。

`canonical_row_digest`采用
`price_non_market_session_row_digest.v1`：

- 字段顺序固定为
  `date,ticker,open,high,low,close,adj_close,volume`；
- `date`规范为 ISO `YYYY-MM-DD`；
- `ticker`执行 trim 后保留大小写；
- 缺失值规范为 JSON `null`；
- bool、integer、finite float 与 string 使用显式 type-tagged canonical values；
- 非 finite float 不允许进入 complete attribution；
- 使用 UTF-8、sorted-key、无多余空白的 canonical JSON bytes；
- digest 为 lowercase SHA-256。

相同内容的重复 source rows 允许具有相同 digest，但必须通过不同
`source_ordinal`保留独立身份。

### 3.3 Six-dimensional scope

- affected price tickers：全部触发行中的 normalized non-empty ticker 去重集；
- affected rate series：空；
- affected source roles：仅 `primary_market_prices`；
- affected dates：requested window 内 exact distinct trigger-date set；
- affected fields：仅 `date`；
- affected rows：全部触发行的 ordinal、digest、date 与 ticker。

日期集合不是连续区间，`rows`旧字段继续表示 distinct date count。

### 3.4 Fail-closed

以下任一情况使 typed attribution incomplete：

- source role 不是已批准的 `primary_market_prices`；
- source path/checksum 或 requested window 缺失；
- trigger date 为空、不可解析或超出 window；
- 任一触发行 ticker 空白；
- ordinal 缺失、重复或与 snapshot 行序不一致；
- canonical digest 无法按 v1 生成；
- 未包含任一 trigger date 的全部 source rows；
- calendar source 或 special-closure policy binding 缺失/漂移。

Incomplete 时：

- typed attribution 不得标记 complete；
- `affected_instruments`必须置空，使既有 classifier 保持 global；
- 不从 message、sample 或 `rows`反向推断 scope。

## 4. 明确不在本任务

- 不修改 capability policy YAML；
- 不新增 allowed issue code；
- 不迁移 daily、periodic、research 或 strategy consumer；
- 不让 classifier 使用 date/window/row 维度扩大隔离；
- 不修改 cached market data；
- 不改变研究窗口、评分、仓位、策略结论或 backtest；
- 不执行 production 或 broker action。

既有 instrument-only pilot 对 complete primary attribution 可保持兼容；任何不完整或
非 primary attribution 必须更保守地保持 global。后续若要让 policy 消费新增维度，
必须另建 owner-reviewed adoption task。

## 5. 实施步骤

1. 新增 immutable owner-decision authority；
2. 定义 typed source binding、row identity 与 issue attribution contract；
3. 在 canonical price validation path 传递 exact snapshot checksum、source role 和
   reviewed calendar binding；
4. 只为 complete primary price issue填充 typed attribution；
5. 更新 Markdown DQ report，使完整/不完整 attribution可审计；
6. 证明 secondary/unapproved source、blank ticker、缺失 binding、calendar drift、
   duplicate row 与 source reorder 均 fail closed；
7. 保持 capability policy、consumer 和 production boundary不变；
8. 更新 system flow、task register和generated governance；
9. 执行 focused、Ruff、strict mypy、Architecture、Contract、Report、
   Reproducibility、Integration 与唯一自然边界 Full；
10. 通过后转`BASELINE_DONE`，任何 dimensional classifier adoption另行评审。

## 6. 验收标准

- owner decision exact bytes与 C2P pack id均被绑定；
- optional typed contract向后兼容所有其他 DQ issues；
- primary complete case具有六维 scope、source artifact SHA与calendar policy SHA；
- `rows`仍为distinct dates，typed affected rows包含全部source rows；
- digest v1确定性、field-complete、type-tagged且对source reorder可识别；
- duplicate identical rows由ordinal区分；
- blank ticker、missing checksum/window、unapproved source或policy drift不得隔离；
- current capability policy bytes和consumer bytes不变；
- runtime DQ behavior只增加owner-approved typed evidence和fail-closed tightening；
- `production_effect=none`、`broker_action=none`；
- focused/formal/Full与post-Full gates全部PASS。

## 7. 生命周期

- governed mode：`SINGLE_LANE`；
- contract wave：smallest reviewed serial wave；
- frozen base：preflight后记录；
- 不创建额外 worktree、clone或stash；
- base-drift只读证据使用
  `D:\Work\AITradingSystem\outputs\architecture\data_gov_002c3p_change_manifest.v1.json`
  与
  `D:\Work\AITradingSystem\outputs\architecture\data_gov_002c3p_integration_revalidation_plan.v1.json`；
  owner为本任务coordinator，purpose为绑定frozen-base/lane-head/latest-main真实delta，
  exit condition为最终candidate已通过formal/Full、plan id与decision写入本要求且
  local/remote closeout完成；届时先审计唯一证据，再按canonical retention规则保留或清理；
- 不读取、hash、复制、修改或提交
  `docs/research/growth_tilt_owner_diagnosis_pack.md`；
- 任务分支、validation evidence与cleanup按ARCH-005流程管理。

## 8. 进度

- 2026-07-27：project owner以price source-owner身份给出
  `APPROVE_FOR_CONTRACT_WAVE`及七项conditions。登记本最小serial contract wave，
  当前仅建立任务和实施边界；尚未修改runtime schema、classifier、policy、consumer或
  production行为。
- 2026-07-27：governed `SINGLE_LANE/coordinator/LANE`在frozen
  base=`e1edaf7ab2f68c531196c004cbd70612c3b12f3d`通过。实现decision authority、
  `data_quality_issue_attribution.v1`、source/calendar/row contracts、digest v1、
  primary complete与secondary/incomplete fail-closed接入，以及DQ Markdown完整归因段。
  C2P pack按owner实际审阅bytes冻结，decision绑定pack SHA=
  `c6b9bd2ee23f3aee4c65e21f1bf7673ec41e72af7239cd5e4a3877ebc8962af7`；旧pack不随
  C3 runtime重建。
- 2026-07-27：focused contract/C2P lifecycle/current DQ tests首轮=`31 passed`；
  扩展DQ/capability/inventory回归首轮=`95 passed / 1 failed`，唯一失败为预期的C1
  content-derived inventory freshness，已用canonical generator刷新，未绕过门禁。
  Ruff与strict mypy通过；当前转formal validation准备。Capability policy、classifier、
  receipt consumer bytes、production与broker边界保持不变。
- 2026-07-28：formal Architecture=`755 passed`、Contract=`275 passed`、
  Report=`57 passed`、Reproducibility=`23 passed`、Integration=`995 passed`。
  首个可审计Full=`7591 passed / 6 failed / 3 skipped`；其中Wave14/Wave15两项
  `CARRIER_PUSH_DRIFT`明确记录frozen lane HEAD=`e1edaf7...`、最新
  `origin/main=b2a819b...`，进入ARCH-005 base-drift integration plan；一个
  external-request-cache `PermissionError`发生于另一任务并发Full期间，待最终候选独占
  重跑。其余三项均为C1 inventory刷新导致既有
  `rate_issue_attribution_review_pack_v1`派生freshness漂移。该pack继续保持全部decision
  pending、无新增authority；本任务扩充claims，仅以canonical generator刷新其JSON、
  validation与Markdown，不改变rate contract、policy、consumer或生产行为。
- 2026-07-28：base-drift plan
  `integration-revalidation-9e4adceaa506509d7fe8`
  （SHA-256=`9e4adceaa506509d7fe85741e731cdd206c7524c2bee81b359cdb6286d69df64`）
  判定`RECONCILIATION_REQUIRED`且无blocker/contract conflict；仅
  `docs/task_register.md`与`tests/test_arch_004_refactor_policy.py`存在domain overlap，
  四个architecture/task-registry生成物按coordinator refresh处理。使用该exact plan id
  的INTEGRATION preflight通过，candidate从latest local main
  `b2a819b105bcc9c30a0952e64796ab3493f267d2`建立；S1/S2历史权威保留，C3成为当前
  append-only hash authority。集成focused=`38 passed`，Ruff、strict mypy、C1、
  frozen price pack、rate pack、DevEx、task registry与deprecation checks均PASS。
- 2026-07-28：首个latest-main candidate=`0e922155b09e2a4d32151e57095ffb56c5d0892c`
  开始formal时，local/remote main又前进到
  `f625b8bffd004ee0beafde00962eec2299cd3e27`（TRADING-2463 S3 pack）。
  Architecture实际=`756 passed / 2 failed`，两项均为预期
  `CARRIER_PUSH_DRIFT`，无C3功能或contract失败。刷新计划为
  `integration-revalidation-5959fa017e29523a4860`
  （SHA-256=`5959fa017e29523a4860af843e4aaa546b3b50dc33996ff4eeca6b5b5c132989`），
  仍为无blocker/contract conflict的`RECONCILIATION_REQUIRED`；在exact S3 main
  上重建最终candidate，S3转不可变历史权威，C3保持唯一当前hash authority。
- 2026-07-28：S3-base candidate=`89cdfaf1db2f2ff783ba4f19a8559bfc89e38eb5`
  已通过Architecture=`761 passed`、Contract=`275 passed`、Report=`57 passed`、
  Reproducibility=`23 passed`与Integration=`995 passed`。启动Full前发现TRADING-2463
  S4 session已有唯一Full在运行，本任务等待其Full、post-Full Architecture/Contract、
  lease release和local/remote main同步完成，未并发第二个Full。S4最终main=
  `ae5de3cce5606445b340d25713e1e55ba1c2ce3d`；刷新计划为
  `integration-revalidation-3e80a767b883e3d422b3`
  （SHA-256=`3e80a767b883e3d422b3ed3c3ba4edcb83028128556f2a08d67db5089d15df88`），
  仍无blocker/contract conflict。最终C3候选改从该exact main重建，S4转不可变历史
  权威，C3继续作为唯一当前hash authority。
- 2026-07-28：exact S4-base C3 candidate=
  `af37b804f48c2aff3f7a262107343e56f8146ce5`在无并发pytest/Full、clean
  governed worktree和active integration lease下完成最终门禁：
  focused=`44 passed`、Ruff=`PASS`、strict mypy=`PASS`、
  Architecture=`764 passed`、Contract=`275 passed`、Report=`57 passed`、
  Reproducibility=`23 passed`、Integration=`995 passed`；唯一自然边界独占
  Full=`7606 passed / 3 skipped / 642 warnings`，runtime artifact=
  `outputs/validation_runtime/full_20260727T173947Z/test_runtime_summary.json`。
  post-Full final tracked-state Architecture=`764 passed`
  （`outputs/validation_runtime/architecture-fitness_20260727T180732Z/test_runtime_summary.json`）、
  Contract=`275 passed`
  （`outputs/validation_runtime/contract-validation_20260727T180907Z/test_runtime_summary.json`）。
  C3 typed contract基线转`BASELINE_DONE`；后续dimensional classifier adoption、
  capability policy或consumer迁移继续要求独立owner评审，当前
  `production_effect=none`、`broker_action=none`。
- 2026-07-28：local/remote closeout已完成，`main=origin/main=
  917cf6d80285de57ab336b39e277dc315aae7aba`。临时分支唯一实现审计确认
  decision、contract、runtime与focused test核心blob在旧候选和最终main逐文件相同；
  共享文档/生成物由最终candidate supersede，无唯一未保留实现或证据，也无本任务
  worktree。批准的本地分支删除allowlist恰为：
  `codex/data-gov-002c3p-price-attribution-contract`、
  `codex/data-gov-002c3p-integration`、
  `codex/data-gov-002c3p-integration-final`、
  `codex/data-gov-002c3p-integration-s4`及
  `codex/data-gov-002c3p-cleanup-closeout`；不得扩展到其他branch/worktree。
  Canonical retained evidence为main commit、本文记录的旧commit/plan id，以及
  `outputs/architecture/data_gov_002c3p_change_manifest.v1.json`
  （raw SHA-256=`6765f25bdd3cfc142449c0a86ce2b5b57a5a36eec5ae5e5fa38d66ae697b6db1`）
  与`outputs/architecture/data_gov_002c3p_integration_revalidation_plan.v1.json`
  （raw SHA-256=`cff1171c1fb08a28e64dda1b1866a78f43f63f4f104a4346d118b65a50f7af09`）。
  分支删除只移除local refs，不删除commits/artifacts；短期可由本文commit SHA或Git
  reflog恢复，远端main为长期canonical recovery boundary。
- 2026-07-28：OPS-071最终候选Full发现 frozen price review pack 与当前受约束
  source bytes 不一致。Project owner要求修复阻塞并完成当日恢复，作为
  `owner_decision:OPS-071:2026-07-28:implement_governed_same_as_of_recovery_v1`
  的最小authority refresh：使用canonical builder重建pack，得到
  `dq_price_issue_attribution_review_0731caba2f2b6280dda3385b`、raw SHA-256
  `e1f3841dc27a9bee78c79fe07250acfe006941ed252235c23c13b3b8017a3449`；
  decision version升至`1.0.1`并绑定新bytes。Calendar function AST与special-closure
  policy语义未变化；AST hash canonicalization显式保留empty fields并排除新版本空
  `type_params`字段，使Python 3.11/3.14均得到
  `23ab933d7013e15b73d912aa09258adc4c7ba252a36330190d66735d1b70f01c`。
  七项owner conditions、typed attribution语义、DQ policy、consumer和production
  边界均未改变。
