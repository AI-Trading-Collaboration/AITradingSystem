# TRADING-2513：QC QQQ Options Primary Window Derived Aggregate Run Proposal / Owner Decision Pack V1

最后更新：2026-08-12

稳定任务 ID：`TRADING-2513_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_RUN_PROPOSAL_OWNER_DECISION_PACK_V1`

优先级：`P0`

状态：`IN_PROGRESS`

mode：`SINGLE_LANE`

production effect：`none`

broker action：`none`

## 1. 目标

在 TRADING-2512 离线 collector contract 已完成后，生成并 ordinary-push 一份可由 Project Owner
逐字段审阅的 exact run proposal package。package 固定 run scope、project id、完整 XNYS session inventory、
deterministic QuantConnect `main.py`、2512 policy/transport/code hashes、单次授权上限、allowed/prohibited actions
与 evidence collection checklist。

本任务只实现离线 proposal/package builder、strict loader、canonical seals、Owner decision request 与
fail-closed tests；不登录 QuantConnect，不创建或修改项目，不运行 Cloud backtest，不调用 API/CLI/HTTP/Object
Store，不下载或导出 raw option rows，不购买数据，不提交订单，不生成投资解释，不激活 selection、engine、
paper/live/broker/production。

## 2. 冻结 authority

- registration base：`1533952659413f0d890b64720e0e8a06a32fbdd4`；
- 2512 policy file SHA-256：
  `48511cc64cab07b091787e2b0cb23354424248da66e7dba8866cd9ce9a766a8f`；
- 2512 policy canonical SHA-256：
  `3ebdd8a4dd89aad4584fbe8bffeeabb30d9b7bd2c28cd394c0fbc346939e999f`；
- 2512 transport-map canonical SHA-256：
  `60c970b71d3c47337fb76452d1384f2463079ef5026239e875e78b8c37d3eab5`；
- 2512 module SHA-256：
  `1c20aca322baa18c7b673167a290b5d38a20a6384f1c3a79d4af642dd32fbe2a`；
- 继承 2481 shared records/envelope、2482 DQ/PIT、2484 QC adapter、2499 DAILY chronology、
  2500 DAILY capability、2509 v2 catalog、2510 admission/readiness、2511 generator 与 2512 collector；
  不复制或重定义这些 authority；
- 2502/2504/2507 仍无 Owner-supplied executable policy values，engine 固定
  `POLICY_BLOCKED_CASH_PRESERVATION`。

## 3. Reviewed proposal scope

- target project id：`34808569`；
- requested/evaluated range：`2021-02-22..2025-12-02`；
- primary research role：`PRIMARY`；exchange calendar：`XNYS`；
- exact session count：`1202`；首尾 session：`2021-02-22` / `2025-12-02`；
- maximum project mutations：`1`；maximum cloud backtests：`1`；
- maximum orders：`0`；maximum fills：`0`；
- result carrier：Owner manual `Download Results` JSON only；
- authorization baseline：`NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS`；
- decision baseline：`OWNER_AUTHORIZATION_REQUIRED`。

选择 `2025-12-02` 作为首次全窗采集终点，是因为该日已有 reviewed QC account/project/options capability
evidence，可避免在 proposal 阶段假设 2026 的实际平台数据可用性。它不是投资阈值、研究默认起点、最短样本
边界或永久 end；未来 range expansion 必须另行 reviewed proposal。项目唯一 primary start 仍为
`2021-02-22`。

## 4. Package layout 与严格绑定

canonical package 计划位于：
`inputs/research/qqq_options/trading_2513_primary_window_derived_aggregate_run_proposal_v1/`。

exact inventory：

1. `run_scope.json`：2512 sealed run scope 与 1202-session inventory；
2. `proposal.json`：2512 sealed proposal，状态固定未授权；
3. `main.py`：2512 deterministic renderer 的 exact LF bytes；
4. `owner_decision_request.md`：人类可读授权上下文、allowed/prohibited actions 与 token template；
5. `package_manifest.json`：上述四项的 path/SHA-256/byte count 与 upstream authority cross-binding。

strict loader 必须拒绝 symlink、路径逃逸、inventory 漂移、noncanonical JSON、duplicate keys、scope/proposal/code/
policy/repository/range/session/hash mismatch、额外或缺失文件、授权状态被提前改写，以及任意 order/fill/raw/log/
Object Store/API/CLI/HTTP/paper/live/broker/production 放宽。

## 5. Owner decision 边界

本任务不会代签 Owner token。最终 decision request 必须提供可直接复核的：

- ordinary-pushed exact main SHA；
- package manifest file/content SHA-256；
- proposal/run-scope content SHA-256；
- project code LF SHA-256/byte count；
- 2512 policy file/canonical/transport hashes；
- exact range/session count/project id；
- single-use / expiry / invalidates-after-evidence 条款；
- collector 与 independent reviewer；
- allowed/prohibited action exact inventories。

即使 Owner 后续授权，也只允许一个 bounded zero-order collection lifecycle；实际平台动作、evidence admission 与
independent review 属于后继任务。授权 token 不得复用 2480、2492、2498、2500，也不得由本 task 自动生成。

## 6. 实现计划

### S0：registration boundary

- canonical task row + supporting requirement；
- task shadow/DevEx/current authority 重建；
- focused registration validation、ordinary push 与 exact base release。

### S1：proposal package

- task-owned package policy、builder、strict loader 与 public sealed manifest；
- deterministic run scope/proposal/project code/decision request package；
- canonical bytes/SHA/from-json/replay 与 exact inventory verification。

### S2：fail-closed coverage

- unit/property/golden：key permutation semantic replay、byte checksum distinction、session exactness；
- missing/extra/symlink/path/hash/canonical/range/project/code/policy/action/status drift negatives；
- no authorization token、no platform action、no order/fill/raw/policy value/engine activation invariants。

### S3：shared wiring 与收口

- system flow、architecture fragment、task registry/generated/compatibility authority；
- Atlas 披露“exact proposal 已完成但 Owner 未授权、run/evidence/DQ 仍缺失”；
- focused/adjacent/compatibility 与 final-tree formal gates；
- ordinary non-force push、SHA verify、branch/worktree cleanup。

## 7. 验收标准

1. ordinary-pushed package exact、canonical、可重放，并绑定 2512 frozen authority 与 1202-session PRIMARY scope。
2. `main.py` hash/bytes 与 2512 renderer 完全一致；无 threshold、order、raw/log/Object Store/network 行为。
3. Owner decision request 完整列出单次授权上限、allowed/prohibited actions、expiry/reviewer/evidence lifecycle。
4. 未提供真实 Owner token 时，任何 authorization/evidence/run admission 均 fail closed。
5. 页面不得把 proposal 完成解释为 QC run、DQ PASS、policy reviewed、策略有效或 engine 可执行。
6. external action、production effect、broker action 均为 `none`。
7. focused、generated/compatibility、Atlas 与适用 formal gates PASS。

## 8. 当前 blocker / exit condition

当前 blocker：`OWNER_DECISION_NOT_PROVIDED_FOR_EXACT_TRADING_2513_PROPOSAL`。

本任务 exit condition 是 exact proposal package ordinary-push 后，向 Project Owner 提供其所有 canonical hashes 与
可复核 token template。真实 Owner 授权、平台 run 与 evidence collection 必须登记为独立后继，不能在本任务中
自动执行。
