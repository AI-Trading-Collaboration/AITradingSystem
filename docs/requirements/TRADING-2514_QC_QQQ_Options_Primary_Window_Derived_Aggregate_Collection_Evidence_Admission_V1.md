# TRADING-2514：QC QQQ Options Primary Window Derived Aggregate Collection / Evidence Admission V1

最后更新：2026-08-12

稳定任务 ID：`TRADING-2514_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_COLLECTION_EVIDENCE_ADMISSION_V1`

优先级：`P0`

状态：`BASELINE_DONE`

mode：`SINGLE_LANE`

production effect：`none`

broker action：`none`

## 1. 目标

在 TRADING-2513 已 ordinary-push exact 1202-session proposal package 后，建立一个严格离线的
authorization lifecycle 与 evidence admission 合同。该合同负责验证未来 Project Owner 签署的
exact 2513 token、single-use/expiry 状态、外部动作 ledger、Owner 手工下载的 QuantConnect Results
JSON，以及向 TRADING-2482 / TRADING-2510 / TRADING-2511 交付的 DQ/PIT facts。

本任务在没有真实 Owner token 和真实 Results JSON 时仍可完成合同、loader、canonical seal、typed
failure、测试与页面披露；但不得把未签署 token、调用者自报 PASS、任意 JSON bytes 或 synthetic fixture
当作真实平台证据。当前不登录 QuantConnect、不修改项目、不运行 Cloud backtest、不调用 API/CLI/HTTP/
Object Store、不下载或导出 raw option rows、不购买数据、不提交订单、不生成投资解释，也不激活
selection、engine、paper/live/broker/production。

## 2. 冻结 authority

- registration base：`f6505359ab6697c4c54bc42807026f34685d97a8`；
- 2513 ordinary-pushed exact main：`f6505359ab6697c4c54bc42807026f34685d97a8`；
- 2513 proposal content SHA-256：
  `f48732afc0d69656fbe5c62b1965296feccda30caa3279c80b9d1c20ce272240`；
- 2513 run-scope content SHA-256：
  `80c11d7073dcc86f1297a34b3497fe705069619d6f1f51927ab9b673172db15e`；
- 2513 project code LF SHA-256：
  `d7f96fbb14e03a1f248b0a14b3ebdaa1bbeeada2d15f87fb3277b98b9c6641a6`；
- 2513 proposal policy file/canonical SHA-256：
  `dc64eae45a3581089af1223c8bc6da005c0962d17906ad447cf72f8a9a5fbbaf` /
  `4c80425fae656c573ca74d44e5d738bc78307619c0471f2c852446430fefdbc6`；
- 2512 collector policy file/canonical SHA-256：
  `48511cc64cab07b091787e2b0cb23354424248da66e7dba8866cd9ce9a766a8f` /
  `3ebdd8a4dd89aad4584fbe8bffeeabb30d9b7bd2c28cd394c0fbc346939e999f`；
- 2512 transport-map SHA-256：
  `60c970b71d3c47337fb76452d1384f2463079ef5026239e875e78b8c37d3eab5`；
- 继承 2481 shared records/envelope、2482 DQ/PIT、2484 QC adapter、2499 DAILY chronology、
  2500 DAILY capability、2509 v2 catalog、2510 evidence admission、2511 generator、2512 collector
  与 2513 proposal；不得复制或重定义；
- 2502/2504/2507 仍无 Owner-supplied executable policy values，engine 固定
  `POLICY_BLOCKED_CASH_PRESERVATION`。

## 3. 授权合同

未来 token 必须 exact-bind：

- token id：
  `owner_decision:TRADING-2513:2026-08-12:authorize_single_zero_order_primary_window_derived_aggregate_collection_v1`；
- ordinary-pushed main、repository code、proposal/scope/code/policy/transport hashes；
- target project id `34808569`；PRIMARY range `2021-02-22..2025-12-02`；
  exact session count `1202`；
- maximum project mutations `1`、maximum cloud backtests `1`、orders/fills `0`；
- collector `codex_capability_coordinator`、independent reviewer `project_owner`；
- expiry、single-use 与 invalidates-after-evidence 条款；
- exact allowed/prohibited action inventories。

合同必须拒绝：缺失/重复/过期 token、错误 hash/range/project/session/cap/reviewer/action、先消费后复用、
授权外动作、未记录外部动作、第二次 mutation/run、任何 order/fill/raw row/network automation/purchase/
paper/live/broker/production 放宽。Owner 未签署时只能产生 typed
`OWNER_AUTHORIZATION_NOT_PROVIDED`，不得自造 approval。

## 4. Evidence admission 与 DQ/PIT

后续真实 evidence 只允许 Project Owner 在 UI 中手工 `Download Results` 的 JSON；必须先由 2512 strict
parser 完整解析并验证 schema、project/backtest id、requested/evaluated range、1202 session coverage、
10-series aggregate inventory、orders/fills 为零、transport/source identity 与 artifact checksum。不得信任
调用者自报 declaration、截图结论、任意 bytes 或 report 文件名。

admission 产物至少包含：

- authorization token identity 与 consumption receipt；
- external action ledger（每项 action、时间、project/backtest id、结果与 scope verdict）；
- source artifact SHA-256/byte count，不包含 raw option rows；
- 2512 parser facts 与 2511 derived-evidence handoff facts；
- option-event DQ 与 local cached DQ 分轴状态；未实际评估项保持 `NOT_EVALUATED` 或 `FAIL`；
- 2482 canonical DQ report/receipt、PIT chronology 与 prior-session identity；
- typed aggregate decision：在真实 evidence 和 canonical DQ/PIT 全部通过前保持
  `EVIDENCE_NOT_ADMITTED_POLICY_BLOCKED`。

UNKNOWN、NOT_EVALUATED、scope mismatch、as-of mismatch、checksum mismatch、semantic FAIL 或伪造 PASS
都不得产生 admission PASS。即使 evidence admission 成功，也不提供 G2 policy values、不证明策略有效，
不自动授权 selection、engine 或投资解释。

## 5. 实现计划

### S0：registration boundary

- canonical task row + 本 supporting requirement；
- task shadow/DevEx/current authority 重建；
- focused registration validation、ordinary push 与 exact base release。

### S1：离线 authorization lifecycle

- task-owned policy、typed Owner token、consumption receipt、external action ledger；
- strict canonical bytes/SHA/from-json/replay；
- expiry/single-use/cap/action/hash binding 与 cash-preservation default。

### S2：evidence admission

- 复用 2512 Results parser，不复制 transport 或 aggregate schema；
- strict artifact identity、scope/session/project/backtest/zero-order validation；
- 复用 2482 DQ/PIT envelope 与 2510/2511 admission/readiness authority；
- deterministic evidence receipt 与 fail-closed aggregate verdict。

### S3：验证与 shared wiring

- unit/property/golden 与 forged/semantic FAIL/UNKNOWN/mismatch/replay negatives；
- system flow、architecture fragment、task registry/generated/compatibility authority；
- Atlas 披露“admission 合同已实现，但 Owner token/run/evidence/DQ 尚未发生”；
- focused/adjacent/compatibility 与 final-tree formal gates；
- ordinary non-force push、SHA verify、branch/worktree cleanup。

## 6. 验收标准

1. 未提供 exact Owner token 时，authorization、external action 与 evidence admission 全部 fail closed。
2. token 的 hash/range/project/session/cap/expiry/single-use/reviewer/action 任一漂移都产生 typed failure。
3. Results JSON 必须经 2512 strict parser，从真实 facts 派生 evidence receipt；调用者自报 PASS 无效。
4. report semantic FAIL/UNKNOWN、scope/as-of/checksum mismatch、missing/extra series/session、order/fill/raw-row
   事实均阻止 admission。
5. 只复用 2482/2510/2511 canonical DQ/PIT/admission authority；未评估项不得伪造成 PASS。
6. 输入 key/order 排列不改变 canonical semantic identity；artifact byte checksum 仍可区分不同原始 bytes。
7. 不引入 investment-facing 数值阈值，不提供 G2 values，不激活 selection/engine/backtest。
8. 页面不得把合同完成解释为 Owner 已授权、Cloud run 已执行、DQ PASS、策略有效或可下单。
9. external action、production effect、broker action 均为 `none`。
10. focused、generated/compatibility、Atlas 与适用 formal gates PASS。

## 7. 当前 blocker / exit condition

当前 blocker：`OWNER_DECISION_NOT_PROVIDED_FOR_EXACT_TRADING_2513_PROPOSAL`。

本任务工程 exit condition 是离线 authorization/evidence admission 合同 ordinary-push，并向后续真实采集提供
可执行但默认拒绝的严格入口。实际 QuantConnect action 只有在 Project Owner 另行签署 exact 2513 token 后才可
执行；真实 evidence 到达后还需独立 reviewer 与 DQ/PIT 复核。没有 token 或 evidence 不阻止本任务完成离线
工程，但会继续阻止真实 run、evidence admission PASS、selection 与 engine。

## 8. 进度记录

- 2026-08-12：从 exact main `f6505359ab6697c4c54bc42807026f34685d97a8` 启动 registration；
  当前仅登记 requirement/task authority，未执行任何外部动作。
- 2026-08-12：离线 policy、strict Owner token admission、single-use consumption receipt、四阶段 external
  action ledger、2512 Results replay、canonical 2481 DQ report validation、2511 source-bundle handoff 与 sealed
  evidence receipt 已实现；Owner token 仍未提供，external action 仍为 `none`。
- 2026-08-12：task-owned focused 首轮 `15 passed / 2 failed`（测试 fixture 使用 shared taxonomy 不接受的
  `UNKNOWN`，并调用不存在的 shared helper）；第二轮 `17 passed / 1 failed`（fixture chronology 无效）；
  第三轮 `17 passed / 1 failed`（fixture requested/evaluated range 无效）；修正 fixture 后相同并行覆盖
  `18 passed in 58.69s`。这些失败均保留为 failure-fix 证据，未降低 shared record/DQ/PIT 合同。
- 2026-08-12：task + Atlas/page/historical 共享 focused 首轮 `52 passed / 1 failed`，唯一失败为 ignored
  canonical page 的旧 32-task sidecar 被新 33-task exact contract fail closed；完整 writer 重建并显式保留
  ENGINEERING、OWNER_VISUAL、READER_COMPREHENSION 三条既有 PASS facts 后，相同 `-n 16 --dist loadfile`
  覆盖 `53 passed in 108.12s`。
- 2026-08-12：ARCH-004E 重建为 `1113 modules / 1274 tests / 856 direct writers / 0 violations`；
  ARCH-004G frozen inventory、DEVX-006D `2886 entries / 192 fragments`、compatibility authority 与 Atlas
  33-task disclosure 均进入 final candidate。正式五级门禁、ordinary push 与 cleanup 仍需在 final tracked
  bytes 上完成；本状态不代表 Owner token、Cloud run、真实 evidence 或 DQ/PIT PASS 已发生。
- 2026-08-12：2510–2514 + Atlas + task-source + report-flow 邻接首轮
  `162 passed / 19 failed`；19 项全部由 BASELINE_DONE task update notes 未保留 supporting requirement link、
  使 canonical `requirement_refs=[]` 后触发 Atlas fail-closed 级联。追加受治理的 requirement-ref restore event，
  不修改历史 event、不绕过 requirement binding；相同 181-test `-n 16 --dist loadfile` 覆盖重跑
  `181 passed in 104.58s`。首轮保留为 failure-fix parent，不作正式门禁证据。
- 2026-08-12：final candidate `7d80ccddd61b3fc2d6a991f11e28eefffeff14a9` 的 Architecture、Contract、
  Integration、Reproducibility 分别 `865/276/995/24 PASS`；exclusive Full
  `8873 passed / 1 failed / 3 skipped`，唯一失败为 Windows spawned-process cache coordination
  waiter 在 winner atomic publish 期间读取共享 pointer 的低概率 `FileNotFoundError`，与 2514 admission
  业务语义无关。依 OPS-075 的“不同异常另建任务”约束登记
  `OPS-076_WINDOWS_REVALIDATION_WAITER_PROBE_ARBITER_ATOMICITY`，采用最小 durable critical-section
  修复；后续必须从修复后 final tree 重跑完整五级，Full 以 `failure_fix_rerun` 绑定
  `full_20260812T134447Z`，不得复用旧 PASS tiers。
- 2026-08-12：OPS-076 durable fix 后 coordination=`42 PASS`；2514/OPS-076/task-registry 邻接
  failure-fix=`79 passed / 1 exact-count failed` → 相同覆盖 `80 PASS`；compatibility/deprecation
  final=`211 PASS`。所有 tracked 进度在重建 authority 与正式门禁前封存；后续 final tree 不得复用
  修复前四级 PASS，必须完整重跑 Architecture→Contract→Integration→Reproducibility→exclusive Full。
