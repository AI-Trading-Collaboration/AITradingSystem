# TRADING-2492：QQQ Options Bounded QuantConnect Free Cloud Pilot V1

最后更新：2026-08-05

稳定任务 ID：`TRADING-2492_QC_QQQ_OPTIONS_BOUNDED_FREE_CLOUD_PILOT_V1`

优先级：`P0`

状态：`IN_PROGRESS`

mode：`SINGLE_LANE`

production effect：`none`

broker action：`none`

外部平台 Owner token：`owner_decision:TRADING-2492:2026-08-05:authorize_single_bounded_qc_free_cloud_pilot_v1`

当前工程阶段：`TRACKED_SINGLE_USE_AUTHORIZATION_ADMISSION_PRE_RUN`

## 1. 目标与当前可执行边界

本任务最终目标是在独立 Owner 授权后，执行一次 preregistered、严格限界的 QuantConnect Free Cloud
QQQ options smoke pilot，验证真实平台上的 minute subscription、事件时序、单张 long-premium 选约、
next-independent-minute execution、现金核算、manual evidence collection 与 local reconciliation。

2026-08-05 Owner 已 exact-bind proposal policy/authority hash，签发一次性 bounded pilot 授权。该授权在
tracked admission、project source identity、focused/formal validation 与 pre-run audit 完成前不产生平台动作；
API、CLI、HTTP、Object Store、raw options download、paper/live/broker/production 继续禁止。此前已完成的
strictly offline、deterministic、fail-closed 基线包括：

1. pilot policy 与 inherited authority exact binding；
2. Owner 未授权时的 canonical preregistration/readiness record；
3. 未 reviewed 数值和日期不得被 caller 自报 READY 的 typed blocker；
4. 真实执行后必须交付的 evidence inventory、review roles 和最终 disposition contract；
5. unit/property/golden/tamper coverage、architecture fragments 与 system flow。

该离线阶段完成不等于 cloud pilot 已运行，也不满足本任务完整退出条件。task status 保持
`BLOCKED_OWNER_INPUT`，直到 Owner 以 reviewed tracked policy/token 明确授权平台动作与 pilot scope。

## 2. 非目标与禁止事项

未获得独立 Owner authorization 前，本任务不：

- 登录 QuantConnect、创建或修改 project、调用 API/CLI/HTTP/Object Store 或运行 cloud backtest；
- 读取、下载、复制、重建或导出 raw option chain、minute quote、OpenInterest、Greeks 或 provider rows；
- 自行把 pilot 日期、窗口长度、order cap、resource cap、latency、slippage、fee、partial-fill、
  spread、OI、volume、DTE、moneyness、delta、position cap 或 reconciliation tolerance 标记为
  Owner-reviewed/active；Owner 已允许工程线选择一组待审建议值，但该建议只能进入独立
  `PROPOSED_OWNER_REVIEW_REQUIRED` authority，不能改写本文件冻结的 blocked baseline；
- 激活 2485 selection、2486 execution、2487 accounting、2488 lifecycle、2489 collection 或 2490
  reconciliation 的 tracked unauthorized/policy-blocked defaults；
- 使用 synthetic fixture、2491 PASS、caller supplied token、任意 receipt bytes 或 checklist 字段伪造
  platform readiness；
- 宣称策略有效、回测有效、范围可扩、Free tier 足够、付费升级必要或可以 paper/live/production；
- 做 short、assignment、exercise、underlying delivery、roll、multi-leg、LEAPS 或 Wheel；
- 改写 TRADING-2481–2491 shared schema、DQ/PIT、adapter、selector、execution、accounting、lifecycle、
  evidence、reconciliation 或 golden authority。

所有离线输出必须固定：`external_action_executed=false`、`cloud_run_authorized=false`、
`investment_interpretation_allowed=false`、`range_expansion_allowed=false`、`production_effect=none`、
`broker_action=none`。

## 3. Inherited exact authority

2492 policy 必须 exact-bind 并在 loader 中重算下列 predecessor policy/module authority：

- 2480 capability/license/evidence admission；
- 2481 shared sealed records 与 safety envelope；
- 2482 DQ/PIT/cache/evidence identity；
- 2483 daily signal/run package 与 primary research window；
- 2484 QuantConnect project adapter descriptor；
- 2485 deterministic selection；
- 2486 minute execution reality model；
- 2487 cash/premium/settlement accounting；
- 2488 lifecycle/expiry/corporate-action safety；
- 2489 manual evidence bundle；
- 2490 local ingest/reconciliation；
- 2491 cross-layer synthetic harness、golden 与 blocked cloud checklist。

任何 missing、extra authority id、duplicate、symlink、path escape、hash drift 或非 regular file 必须 fail
closed。实现只调用 predecessor 的 canonical/seal/from-json authority，不复制 shared record。

## 4. Research window 与 pilot scope governance

项目 primary research default 继续是 `2021-02-22` 起始的 validated QQQ/SGOV/TQQQ window；
`2022-12-01` 绝不是新 run 默认。

2492 cloud smoke 是 `BOUNDED_PLATFORM_SMOKE_NOT_RESEARCH_CONCLUSION`，不是 primary backtest。真实 pilot 的：

- `requested_start`、`requested_end`；
- `maximum_order_count`、`maximum_contract_quantity`；
- compute/time/resource budget；
- any numeric reality-model/tolerance value

在当前 tracked policy 中全部保持 `UNKNOWN_REQUIRES_OWNER_REVIEW`。未来 Owner-reviewed scope 必须记录 rationale、
intended effect、validation evidence、review/expiry condition，并说明 requested/evaluated range。窗口必须位于
primary validated range 内；若要使用其他历史起点，只能有 reviewed sensitivity/proxy/stress role 和 DQ caveat。

“最多一张合约”“低订单”“极短窗口”只作为定性上界要求；没有 Owner-reviewed exact integer/date/resource
values 时不得被实现转成自选数值。

## 5. Offline preregistration contract

task-owned contract 计划包括：

- policy/authority/scope-field/evidence-role/readiness-item/safety models；
- sealed `QQQOptionsBoundedCloudPilotPreregistration`；
- sealed per-item readiness result 与 aggregate readiness report；
- typed `QQQOptionsBoundedCloudPilotContractError`；
- strict policy loader、blocked preregistration builder、readiness evaluator 与 canonical replay API。

所有 sealed records 使用 strict、extra-forbid、frozen model，提供 `seal`、`canonical_bytes`、
`canonical_sha256` 与 `from_json_bytes`。拒绝 noncanonical JSON、caller hash、NaN/Inf、naive/future timestamp、
unsorted/duplicate item、unknown scope、path/hash/schema/lineage mismatch 与 input permutation drift。

当前 builder 不接受任意 caller token 来激活执行。tracked policy 未经 Owner-reviewed serial change 前，输出必须：

- status=`BLOCKED_OWNER_AUTHORIZATION_AND_SCOPE`；
- owner token=`NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS`；
- every real-platform readiness item=`NOT_EVALUATED` 或 `BLOCKED`；
- `pilot_authorized=false`、`external_action_executed=false`；
- typed cash-preservation/no-order/no-fill disposition；
- blocking reasons 至少包括 `OWNER_AUTHORIZATION_NOT_GRANTED` 与
  `OWNER_REVIEWED_PILOT_SCOPE_NOT_GRANTED`。

2491 cloud checklist 只能作为继承的 blocked authority；synthetic PASS 不能转换为 2492 READY。

## 6. 真实 pilot 的前置条件与 evidence inventory

只有后续 Owner-reviewed tracked authority 同时满足以下条件，真实平台动作才可能单独执行：

1. exact Owner authorization id/token、授权人、reviewer、scope、expiry 与 revocation condition；
2. 2480 confirmed capability receipt，并 canonical replay 为
   `CAPABILITY_CONFIRMED_FOR_BOUNDED_PILOT`；
3. Owner-reviewed exact requested range、order/contract/resource bounds；
4. 2485–2490 所需 policy 或明确保持 blocked 的 no-order smoke design；
5. 2491 checklist 每项由真实 evidence 事实派生，不信任 caller declaration；
6. 2484 exact project/code/subscription/engine identity；
7. 2489 strict manual bundle collection 与 two-person attestation；
8. 2490 explained local reconciliation；
9. no same-bar、no daily-close、no short、no assignment/exercise/underlying delivery；
10. external platform secrets、account/broker identifiers 与 raw rows 不进入 tracked evidence。

真实 run evidence inventory 必须包括 requested/evaluated ranges、code/project/engine identity、capability
receipt、resource telemetry、orders/fills/cash/lifecycle、2491 checklist facts、2489 complete bundle、2490
reconciliation、DQ/PIT axes、license/export classification、two-person review 与 immutable checksums。

## 7. Final disposition contract

在真实 evidence 完整并经 independent review 前，不得生成最终 disposition。最终只能是：

- `BOUNDED_PILOT_ACCEPTED_FOR_RANGE_EXPANSION`；
- `PILOT_REQUIRES_PAID_TIER`；
- `PILOT_NO_GO_LICENSE_OR_EVIDENCE`。

任何 UNKNOWN、missing evidence、unexplained reconciliation、license/export uncertainty 或 resource telemetry
缺失都不能产生第一个结论。该 disposition 仅交给 TRADING-2493 Owner stage gate；不授权 range expansion、
investment conclusion、paper/live/production 或 broker action。

## 8. Acceptance criteria

离线 preregistration stage：

- exact authority hash replay、policy canonical load 与 symlink/path/tamper negatives PASS；
- unknown Owner token/scope/numeric fields 强制 blocked；caller-forged READY/token/receipt/checklist 被拒绝；
- 2021-02-22 primary boundary 保留，2022-12-01-not-default 与 unreviewed alternate-window negatives PASS；
- evidence inventory、review roles、typed blockers、cash/no-order/no-fill、permutation/canonical replay PASS；
- system flow、module/flow fragments、task register、generated/task shadow/current hashes 同步；
- focused、adjacent、compatibility/deprecation 与 final-tree formal gates PASS；
- 外部 QuantConnect/cloud/API/CLI/HTTP/raw export/paper/live/broker/production 动作全部为 none。

完整任务退出：

- 另行获得 exact Owner authorization 和 reviewed scope；
- 真实 bounded pilot、manual bundle、reconciliation 与 independent review 完成；
- 生成上述三个 disposition 之一并交接 TRADING-2493。

## 9. 当前 blocker 与退出条件

blocker：`OWNER_AUTHORIZATION_AND_REVIEWED_PILOT_SCOPE_NOT_GRANTED`。

next owner：project owner + pilot coordinator + independent reviewer。

解除 blocker 所需输入：明确授权是否允许登录/创建或修改 project/运行一次 bounded cloud backtest/下载
export-safe manual evidence，并提供 reviewed exact dates、order/contract/resource bounds、授权 expiry 与 reviewer。

在这些输入到达前，本任务只推进离线工程 baseline，状态不得改为 `BASELINE_DONE` 或任何 pilot success。

## 10. 2026-08-03 离线工程进度

- governed `SINGLE_LANE` START/LANE preflight 从 exact main
  `55858bedfa898f076eae496675aa2d669a5a6eed` PASS；`contract_change=true` 仅声明 2492 task-owned
  policy/public contract，未修改 predecessor shared authority；
- policy exact-bind 2480–2491 共 25 项 module/policy/golden authority；scope/evidence/readiness inventory
  分别为 12/10/12；
- default preregistration/readiness 固定 `BLOCKED_OWNER_AUTHORIZATION_AND_SCOPE`、cash preservation、
  order/fill=0、external action=false；caller 无激活入口；
- focused 首轮同覆盖 `python -m pytest -n 16 --dist loadfile` 为 `24 passed`；Ruff、mypy、compileall PASS；
- DevEx 为 `1081 modules / 1248 tests / 856 writers / 0 violations`；task shadow
  `961/456/505` byte-identical；
- architecture fragments/system flow 已同步；compatibility/current-source authority 与 final-tree formal gates
  在同一 frozen candidate 收口；
- QuantConnect 登录/project/cloud/API/CLI/HTTP/Object Store/raw export/paper/live/broker/production 动作均
  未执行，Owner token 与 reviewed pilot scope blocker 不变。
- 2026-08-03：TRADING-2480 read-only evidence serial contract wave 只刷新 2480/2489/2490/2491 exact
  authority bindings；policy LF SHA-256=`60ed5237fc37e4d44737fe295f4d341a58d318ecad59f8cdf753a0486609f66e`，
  authority set SHA-256=`34d960e7f90c5270495bf4dbbf010a6b67354a43713c00fabdbaa098e72515df`，
  default preregistration/readiness SHA-256 分别为
  `3b2f38fc2672dc2915a0e1b48a9df195bd76295c75cbfe9f82e708581bb49233` /
  `c75c4008fec682c8a227f3fd37bce6e44a705308f9233dc74072cde9ca1c3bd6`。2480 receipt 仍 blocked，
  12 个 scope fields 仍 `UNKNOWN_REQUIRES_OWNER_REVIEW`，2492 不因 2480 登录只读授权而激活。

## 11. 2026-08-05 Owner-review proposal wave

Owner 已明确允许 engineering coordinator 选择一组建议参数，以减少逐项人工挑数；该指示只授权
strictly offline proposal engineering，不是 QuantConnect 登录、project mutation、cloud run、artifact
download 或 order-path authorization。原 2492 blocked policy、preregistration 和 readiness contract 保持
byte-for-byte authority，不因 proposal 存在而变为 READY。

本 wave 使用独立 task-owned policy/API：

- `config/research/qc_qqq_options_bounded_cloud_pilot_owner_review_proposal_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/bounded_cloud_pilot_owner_review.py`；
- `tests/test_qc_qqq_options_bounded_cloud_pilot_owner_review.py`。

建议值冻结为一次技术 smoke，而不是策略结论：

- requested range=`2025-12-02..2025-12-02`，因为 2480 accepted review 已在该 XNYS session
  观察到 Free / Community B-MICRO / QQQ minute option chain；项目 primary research default 仍从
  `2021-02-22` 开始，`2022-12-01` 仍绝非默认；
- dedicated project mutation/create cap=`1`、cloud backtest cap=`1`、runtime cap=`300s`、processed data
  points cap=`250000`、order cap=`1`、long-premium quantity cap=`1`；
- technical direction=`LONG_CALL`；selection=`DTE 7/14/21`、absolute delta=`0.30/0.40/0.55`、
  max absolute moneyness deviation=`0.05`、quote age=`60s`、relative spread=`0.20`、OI floor=`10`、
  volume floor=`0`，并使用 deterministic DTE→delta→spread→OI→volume→SID rank；
- next-independent-minute execution：submission/fill latency 分别=`60000/60000ms`、marketable-limit
  buffer=`$0.01/share`、reality slippage=`$0.01/share`、fee=`$0.65/contract`、quote age=`60000ms`、
  cancel timeout=`60000ms`；zero-slippage 只保留 isolation sensitivity；
- initial cash=`$100000`、premium budget=`$2000`、fee buffer=`$1/contract`、T+1 cash settlement、FIFO、
  fee included in cost basis、cash quantum=`$0.01`、`ROUND_HALF_EVEN`；
- pre-expiry guard=`2 XNYS sessions`，任何 unexpected exercise/assignment/corporate action、DQ/PIT
  FAIL/NOT_EVALUATED、stale/missing/crossed quote 或 scope violation 都必须 no-order/cancel/invalid-run；
- monetary/price reconciliation tolerance=`$0.01`，timestamp tolerance=`60s`；只允许 2489
  export-safe aggregate/manual evidence，raw option rows、API/CLI/HTTP/Object Store 与 secrets 继续禁止。

这些数值是带 rationale、intended effect、validation plan、review/expiry/revocation condition 的临时
pilot proposal。proposal loader 必须 exact-bind 2480 accepted review、2480 evidence、原 2492 blocked
module/policy；还必须显式验证 2480 `bounded_pilot_preparation_allowed=false` 与 prior admission
`CAPABILITY_OR_LICENSE_BLOCKED`，因此不能把 accepted discovery evidence 冒充 pilot admission。

owner-review pack 只能输出 `OWNER_REVIEW_REQUIRED_NO_EXTERNAL_ACTION`、cash preservation、order/fill=0、
external action=false。任何 caller token、配置 tamper、authority drift、日期/primary-window 混淆、阈值越界、
zero-slippage baseline、same-bar/daily-close、short/multi-leg/assignment 或 reviewer=collector 都 fail closed。
完成本 wave 后 task 仍为 `BLOCKED_OWNER_INPUT`；下一步只能由 Owner 对 final proposal policy SHA-256
签发新的 exact authorization，随后才可另行执行一次已审 scope 的 cloud pilot。

实现进度：governed `SINGLE_LANE` START/LANE 从 exact main
`d1c45decf8d41fb0ef47b0db8f9868263f2e7c45` PASS，`contract_change=true` 只覆盖 2492 task-owned
proposal API。policy LF SHA-256=`9b3e50731663871e01626f0360c717ecdd14278c63f81e74ed79c4c2fd4041de`，
authority-set SHA-256=`69578c198823b95ba16b5f6c2780c3a7e24104babe2c6cc1fed8cd740c446bea`。
focused 首轮在完全相同的 `-n 16 --dist loadfile` 53-test 覆盖下为 `52 passed / 1 failed`，唯一失败是
missing-policy negative 收到 Windows `FileNotFoundError` 文本而非 typed regular-file message；helper 先验证
regular file 后，同覆盖 failure-fix 为 `53 passed in 3.95s`。Ruff、mypy、compileall PASS；DevEx
`1088 modules / 1252 tests / 856 writers / 0 violations`，task shadow=`961/456/505` byte-identical。
兼容层/current hashes 与 final-tree formal gates 尚需在冻结候选上完成；外部平台动作持续为 none。

## 12. 2026-08-05 single-use platform action authorization

Owner 签发：

`owner_decision:TRADING-2492:2026-08-05:authorize_single_bounded_qc_free_cloud_pilot_v1`

授权 exact-bind：

- proposal policy SHA-256=`9b3e50731663871e01626f0360c717ecdd14278c63f81e74ed79c4c2fd4041de`；
- proposal authority-set SHA-256=`69578c198823b95ba16b5f6c2780c3a7e24104babe2c6cc1fed8cd740c446bea`；
- requested range=`2025-12-02..2025-12-02`；
- maximum project mutations/cloud backtests/orders/contracts=`1/1/1/1`；
- collector=`codex_pilot_coordinator`，independent reviewer=`project_owner`；
- expiry=`2026-08-12T00:00:00Z`，single-use=true，evidence collection 完成后立即失效；
- allowed=`quantconnect_login,dedicated_project_create_or_modify,cloud_backtest,export_safe_manual_evidence_collection`；
- prohibited=`api,cli,http,object_store,raw_options_data_download,paper,live,broker,production`。

该 token 只允许一次 cloud backtest 内的 simulated long-premium order；不产生真实券商订单。授权 overlay
不得改写 2480 prior admission、2481–2491 shared schema/policy 或原 2492 blocked/proposal authority；必须如实
记录 prior admission 仍为 `CAPABILITY_OR_LICENSE_BLOCKED`，并以 Owner exact decision 作为本次单次 smoke
例外的唯一授权来源。任何 hash/scope/expiry/reviewer/project-source drift 均在平台动作前 fail closed。

本 wave 复用 clean checkout `D:\Work\AITradingSystem_ops073_integration`，owner task 为 TRADING-2492，
用途是隔离当前 OPS-074 dirty main checkout；exit condition 为 task candidate ordinary-push 后 checkout clean、
任务分支删除并释放 `main`。canonical evidence 必须先进入 governed location 并通过 hash/audit，才允许清理。

截至本段写入，QuantConnect project mutation/cloud run/evidence download 均尚未执行；下一步是构建并验证
task-owned authorization admission、canonical project file 与 pre-run record。

Pre-run admission 实现进度：governed START/LANE 从 exact main
`5dc32d240a9fe440e3d7b8fe6a5651a0461849f9` PASS；authorization policy file SHA-256=
`2934ec3e43a9fb7db7357fa6d0fdc518098724eaed3ce14f46c93b7adf3747a7`，canonical semantic SHA-256=
`cc61e318ea2cd1bce32c93bdc51a2b0a135d20d33ac2a0849918c8c20c8d3823`。focused 首轮同一
74-test `-n16/loadfile` 覆盖为 `73 passed / 1 failed`，唯一失败是正确 fail-closed 日期错误文本与测试 regex
不一致；只更正断言后同覆盖 `74 passed`。Ruff、strict mypy、compileall PASS；canonical `main.py`
本地 compile 且小于 32768 bytes。此时 external action 仍为 none。
