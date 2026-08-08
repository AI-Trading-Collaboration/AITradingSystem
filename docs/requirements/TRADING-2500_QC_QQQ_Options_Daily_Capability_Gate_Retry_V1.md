# TRADING-2500：QQQ Options Daily Capability Gate Retry V1

最后更新：2026-08-08

稳定任务 ID：`TRADING-2500_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_V1`

优先级：`P0`

状态：`BASELINE_DONE`

mode：`SINGLE_LANE`

exact base：`72095ff0620c8fe7c7f2e059ca95e4b26eee5a3b`

production effect：`none`

broker action：`none`

## 1. 背景与目标

TRADING-2498 已 ordinary-push 日级零订单 capability proposal。Owner 随后签发 exact
single-use token；capability coordinator 在已有 QuantConnect project `34808569` 保存了审查脚本，
并触发唯一一次 `Backtest Project`。Cloud Build 成功，但平台在算法运行前要求通过信用卡完成
account verification，因此没有 backtest id、没有 processed data points、没有订单/成交或 raw rows。

2498 token 已按 `authorization_single_use=true` 与 evidence-collection invalidation 失效，不能因
Owner 的自然语言“重试”而复用。Owner 已在 QuantConnect UI 完成 account verification，本任务只为
同范围的一次重试生成新的 deterministic proposal；exact token 前不执行外部 retry。

Owner 随后签发
`owner_decision:TRADING-2500:2026-08-08:authorize_single_zero_order_verified_account_qc_daily_capability_retry_v1`。
capability coordinator 已在 project `34808569` 完成 read-only code precheck 与授权内唯一一次
zero-order Cloud backtest；single-use token 随 evidence collection 立即失效。当前阶段只把
`GO_FOR_DAILY_ENGINEERING_ONLY` 记录为 candidate status。Independent reviewer 随后对 ordinary-pushed
exact evidence 签署 tracked attestation；strict terminal review 重新加载 canonical evidence 并从事实派生
所有确认项，最终只在 `DAILY_ENGINEERING_ONLY` scope 内授权登记 2499。

## 2. 冻结继承与 predecessor evidence

本任务继承 2481/2482/2484/2489/2490、2493/2497 与 2498 全部 authority，不重定义 shared
schema、DQ/PIT、adapter、manual evidence 或 terminal gate taxonomy。

2498 predecessor identity：

- repository code SHA：`676d6b1429ee1ef60fbfc4de1d62f9d6ee9184ce`；
- proposal file/content SHA-256：`6b226751453bc2d73e0e5ec14be6975124e3a0948435ff7282658a3c2fe3e5dc` /
  `98566866892b081ad1011e7388348c780e506018e94d568f83b1fcef888a7f95`；
- policy file/canonical SHA-256：`0036996a1d4e9928f2f4b537a3e4158ada2efd15dc24cf6b0918467a1f647812` /
  `1ec345fdf36a101023eacaff6ca78450bd54b45290758438f0ae4a56b2ff63f9`；
- saved QuantConnect script LF SHA-256：
  `1da0d834d5509aabd7fb3baeeff9b8b3f56eed3d9ba095679f84fda926843139`；
- blocked attempt build id：`7edc98-0a3a57`；
- blocked screenshot SHA-256：
  `c09620fa797936ab66cc0f757d3a46cca080bdcd67171474815fe3ac53ad2912`；
- predecessor disposition：`NO_GO_CAPABILITY_OR_ENTITLEMENT_ACCOUNT_VERIFICATION_REQUIRED`；
- no backtest id、orders、fills、processed data points、result artifact 或 raw rows。

独立 reviewer 尚未对 2498 terminal evidence 签署最终 tracked attestation；2500 proposal 必须保留
该 missing-review 事实，不能把 Owner 的 account-verification 完成声明伪造成 predecessor PASS。

## 3. Retry scope

新 proposal 必须冻结：

- target project：`34808569`；
- project mutation：`0`；运行前只读复制并 LF-normalize 当前 editor bytes，必须精确匹配上述
  script SHA-256，否则停止；
- requested range：`2021-02-22..2021-02-26`；
- expected sessions：`2021-02-22`、`2021-02-23`、`2021-02-24`、`2021-02-25`、
  `2021-02-26`；
- QQQ Equity `Resolution.DAILY` + `DataNormalizationMode.RAW`；
- QQQ Equity Options `Resolution.DAILY`；
- maximum cloud backtests：`1`；
- maximum orders/fills：`0/0`；
- raw option rows logged/exported：`false`；
- account verification：点击 Backtest 前只读确认不再显示 verification gate；不授权继续绑定信用卡、
  purchase、subscription 或 upgrade；
- token：single-use，evidence collection 后立即失效。

Allowed actions 仅限：已登录会话下的 read-only account/project/code verification、一次 zero-order Cloud
backtest、export-safe aggregate evidence collection。Prohibited actions 继续包括 project mutation、第二
次 backtest、API、CLI、HTTP、Object Store、raw options download/rows、purchase/subscription、range
expansion、investment interpretation、paper/live/broker/production。

## 4. Evidence 与 gate

复用 2498 的 per-session aggregate 字段与三态 terminal taxonomy。完整 evidence 至少包括：

- account verified UI observation；
- exact project id 与 LF-normalized code SHA-256；
- engine version、build id、algorithm id、backtest id；
- requested/evaluated range 与五 session aggregate；
- processed data points、elapsed runtime、orders/fills/fees/portfolio invested；
- result artifact bytes/SHA-256；
- no raw rows、no scope violation；
- independent reviewer、timestamp、exceptions 与 missing evidence。

只有五个 session 全部满足 2498 的 chain/contract/two-sided quote/positive OI/finite Greeks/finite IV
条件，且 identity/evidence 完整、orders/fills/raw rows/scope violation 均为零，才允许
`GO_FOR_DAILY_ENGINEERING_ONLY`。再次 account gate、run/build failure、required field 缺失或 prohibited
action 得到 `NO_GO_CAPABILITY_OR_ENTITLEMENT`；证据不全得到 `UNKNOWN_EVIDENCE_INCOMPLETE`。

本次 export-safe evidence 固定为
`inputs/external_validation/qc_qqq_options_daily_capability_gate_retry_evidence_20260808.json`：

- evidence file/content SHA-256：
  `829cd5de1d7691d98bfbf3554d27fabcda64598f3e26ce4747beddaf03f1c3b0` /
  `c19c2601e35fe6ee0495a041c1ddeafc52aa275a18856585b36ba2e6435fc609`；
- result artifact `Jumping Blue Pig.json`：16,776 bytes，SHA-256=
  `3e3b41b529294ac31c9559a6d46a7c8ad777063304adde72a72437d240751a09`；
- project code LF SHA-256=`1da0d834d5509aabd7fb3baeeff9b8b3f56eed3d9ba095679f84fda926843139`，
  与授权完全一致；project mutation count=`0`；
- build/backtest identity=`cd73fe-0a3a57` / `077252aa78ce2e0a7c3b9b4c38a554f7`，
  engine=`LEAN Engine v2.5.0.0.17989`，result=`Completed`；
- requested/evaluated range 均为 `2021-02-22..2021-02-26`，五 session inventory 完整；
- processed data points=`63,982`，orders/fills/fees=`0/0/0.00`，portfolio invested=`false`，
  raw rows logged/exported=`false`；
- account tier=`FREE`、compute UI=`Free Node`，未观察到 verification gate；没有第二次 run 或 prohibited
  action。

上述 evidence 只证明 Free Cloud 对目标日级数据合同的 capability candidate，不能推出历史全窗稳定性、
策略收益、license/download 权利、投资结论或 production readiness。

## 5. Sequencing 与验收

1. S0：登记本任务与 requirement；
2. S1：governed START/LANE preflight；
3. S2：生成 task-owned strict retry policy/proposal 与 negative tests；
4. S3：同步 architecture/system-flow/generated authority，formal final-tree validation；
5. S4：ordinary non-force push exact proposal hashes；
6. S5：Owner 签发新的 hash-bound single-use token；
7. S6：read-only account/code precheck 后执行唯一一次 retry；
8. S7：independent review 与 terminal gate；reviewed GO 后登记 2499。已完成。

本任务不以 2498 的自然语言方向、失效 token 或 Owner account-verification 操作替代 S5 exact token。

## 6. Workspace lifecycle

隔离 worktree：`D:\Work\AITradingSystem_trading2500_qc_daily_retry`。

用途：在不触碰当前 `D:\Work\AITradingSystem` 的 TRADING-2496 dirty checkout 前提下，完成 2500
registration/proposal/validation/integration。退出条件：candidate ordinary-push 并确认 canonical evidence
可恢复后 remove worktree、delete merged branch、`git worktree prune`；若 governed gate 阻断，保留并在
本 requirement 记录 exact blocker、unique bytes 与清理条件。

Task-shadow generator 需要 `arch_005_bootstrap_handoff.yaml` 冻结的四个 historical runtime summary。
隔离 worktree 初始不含 Git-ignored runtime evidence，因此只允许从
`D:\Work\AITradingSystem\outputs\validation_runtime` 读取 exact expected path，并在 source SHA-256
匹配 handoff 后复制到本 worktree 同相对路径。hydrated files 不是 2500 新验证证据、不进入 Git，随
worktree 一并清理；source canonical evidence 保持原位不修改。

Full 还会消费 TRADING-2464 的 frozen DQ gate
`outputs/validation_runtime/trading_2464_o1_dq_20260729T183000Z/o1_dq_gate.json`。隔离 worktree 缺失时，
只允许从上述 canonical main checkout 按 exact relative path hydration；source/destination 必须同为
4,057 bytes、SHA-256=`ca02b4310f99d664bb8d987debd4900f4367935b3938663c7a633400d988a1ca`。
该 ignored file 只用于完整回归 fixture，不是 2500 新 evidence；source 保留、destination 随 worktree
清理，恢复方式是从 source canonical path 再次按 hash 复制。

若 frozen lane 与 local main 在 integration boundary 发生 drift，2500 只在
`D:\Work\AITradingSystem_trading2500_qc_daily_retry\outputs\validation_runtime\trading2500_integration\`
创建 ignored `change_manifest.json` 与 `integration_revalidation_plan.json`。两者仅用于从 exact
base/lane-head/latest-main 重建并校验 `integration_revalidation_plan.v1`，不进入 Git；integration preflight
与 final candidate 完成后删除，所需 plan id/status 进入 terminal handoff，且不替代 canonical formal evidence。

外部 run 完成后的 evidence-review worktree：
`D:\Work\AITradingSystem_trading2500_evidence_review`，branch=
`codex/trading-2500-daily-capability-evidence-review`，exact base=
`ab22067ab9f57cc11144ae4eef899cb21f639181`。用途是封存 export-safe evidence、strict loader、tests、
task/system-flow/architecture authority 与 independent-review handoff。退出条件为 evidence candidate
完成 final-tree validation、ordinary non-force push、remote SHA verify，且 canonical evidence 可由 Git
恢复；随后审计并 remove worktree、delete merged branch、`git worktree prune`。下载的 result artifact
保留在 Owner 本机 `G:\Download\Jumping Blue Pig.json`，不复制进 repository；tracked evidence 只保存其
byte count、top-level key inventory 与 SHA-256，不含 raw option rows。

Terminal-review 复用同一绝对路径，新 branch=
`codex/trading-2500-daily-capability-terminal-review`，exact base=
`0cafcc6423364f04177ea86b6cc16badb862a42e`。用途仅为 sealed Owner review、2500 terminal status、2499
registration 与 final authority；不启动 2499 实现。退出条件为 final-tree formal PASS、ordinary non-force
push、local/origin SHA verify 后 remove worktree、delete branch、`git worktree prune`。本阶段不执行外部
QuantConnect action。

## 7. Current progress

2026-08-08：READ_ONLY preflight 在 exact local/main/origin
`72095ff0620c8fe7c7f2e059ca95e4b26eee5a3b` PASS；当前主 checkout 被 2496 owner-visual-acceptance
tracked changes 占用，因此未读取其 diff、未切换/覆盖。2500 从 exact local main 创建独立 worktree；
START/LANE preflight PASS，contract change=false。strict retry policy/loader 与 focused tests 已完成：首轮
pytest `43 passed in 4.05s`，仅 Ruff 报一处 unused import 且 formatter 要求两个文件；最小格式修复后以
同一 `-n 16 --dist loadfile` 覆盖重跑为 `43 passed in 4.01s`，Ruff/mypy/format 均 PASS。

Task-shadow 所需四份 ignored historical runtime summary 已在 source/destination SHA-256 与 handoff
逐一一致后完成 hydration；registry generate/validate 为 `965/460/505` 且 byte-identical，DevEx validate
为 `1094 modules / 1258 tests / 856 direct writers / 0 violations`。兼容层首轮固定覆盖为
`199 passed / 3 failed in 137.35s`：仅 frozen deprecation inventory id/count、latest task-shadow fragment
count 与 2498 EOF current-source authority stale；无 2500 semantic node failure。该首轮作为
failure-fix parent 保留，后续仅以 append-only 2500 current authority 与 refreshed inventory 修复，不改
2498 historical payload/hash。

Compatibility/deprecation failure-fix 第二轮以完全相同的 203-test `-n 16 --dist loadfile` 覆盖得到
`202 passed / 1 failed in 219.65s`，唯一失败为 final test wiring 后
`inputs/architecture/arch_004e_test_manifest.yaml` current hash stale；全量 source replay 确认无第二项差集。
只刷新该 2500 EOF source hash 后，第三轮同覆盖为 `203 passed in 187.70s`。最初 199/3 与第二轮
202/1 均保留为 focused failure-fix evidence，不作为 formal promotion evidence；历史 prefix 始终以
`3,044,567 bytes` / SHA-256=`ac1df7903b9c7f9204303aa427d2968ee6c0ccd76a44d61a76024ff680e0467e`
重放并保持 byte-identical。

Status freeze 前 final same-coverage compatibility/deprecation=`203 passed in 188.97s`；2480–2500
capability/owner/license/authority adjacent=`254 passed in 10.01s`。proposal engineering scope 现标记
`BASELINE_DONE`，完整外部 retry 仍由新的 ordinary-pushed exact Owner token 阻断；这不是 capability GO，
也不允许提前登记/启动 2499。

截至本记录，external retry、project mutation、Cloud run、API/CLI/HTTP/Object Store/download/purchase/
paper/live/broker/production 均为 none。

Implementation authority commit=`c880bb9e55dbcf5c641756e80fdd2f9d00eaa0e2`。由该 exact commit
重放得到 proposal file/content SHA-256=`d5ecad8167e2abef7e5a8d6427604da5b6f59d4be50607228097191eba74239e` /
`77570e7ff88e1c567c29d10dcfc534cef07628cab58ceb894da79c6075f013b9`，policy file/canonical
SHA-256=`851ee0fb3c2a14b25263b37115ece581869fee08dffac95e272960108c46bb19` /
`540107c9dce0fa08a8f461f8c733a1c1c5b413405bb2caf4a6a46501575f9e9d`，authority-set
SHA-256=`52f8246d8192f4fbf40c3aa415aee56bdbb5eb937f4778daa30fda42f06ad3a2`。tracked proposal
保持 `NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS`，不因生成 proposal 自动授权外部动作。

2026-08-08：Owner exact token 与 proposal authority 完全匹配。read-only precheck 确认已登录 FREE
organization、project `34808569`、`Free Node`，editor code LF bytes=`6,148` 且 SHA-256 精确匹配；没有
project mutation。唯一一次 `Backtest Project` 完成并生成 build `cd73fe-0a3a57`、backtest
`077252aa78ce2e0a7c3b9b4c38a554f7`；token 随 evidence collection 失效，未执行第二次 run。
五个预期 session 的 contract/two-sided quote/finite Greeks/finite IV 分别完整覆盖，positive OI 均为正；
result artifact 与 UI facts 共同确认 63,982 data points、0 orders、0 fills、0 fees、未投资、无 raw rows。

2026-08-08：evidence-review focused failure-fix 链保持同一
`python -m pytest -n 16 --dist loadfile tests/test_qc_qqq_options_daily_capability_gate_retry.py`
覆盖。首轮 `21 passed / 2 failed`，根因是测试把 strict Python `date/datetime/tuple` payload 直接写成
JSON；修正为 canonical model bytes 后第二轮 `22 passed / 1 failed`，唯一失败是临时 project fixture
遗漏 2497 predecessor deep-chain files；只补完整 predecessor fixture 后第三轮 `23 passed in 4.35s`。
随后只修 Ruff import ordering，同覆盖最终 `23 passed in 4.39s` 且 Ruff PASS。前两轮只作为 focused
failure-fix 记录，不作为 formal promotion evidence。final-tree authority refresh 后同覆盖重跑出现
`22 passed / 1 failed in 4.31s`：唯一失败是 proposal hash tamper test 仍先调用 strict `seal()` 构造
已被 `Literal` 禁止的非法对象，导致在外部伪造 evidence bytes 写入前 fail closed。修复边界仅为该负向
测试改成直接篡改 canonical JSON bytes 并由 loader 拒绝；完全相同覆盖随后 `23 passed in 4.30s`。
该 22/1 轮同样只保留为 focused failure-fix evidence，不是 formal promotion evidence。当前 candidate status=
`GO_FOR_DAILY_ENGINEERING_ONLY`，但 independent review=`PENDING_PROJECT_OWNER_REVIEW`、
successor registration authorized=`false`；final GO 与 2499 仍等待 ordinary-pushed exact evidence 的
Owner attestation。

2026-08-08：compatibility/deprecation 固定覆盖始终为
`python -m pytest -n 16 --dist loadfile tests/test_arch_004_refactor_policy.py tests/test_arch_004g_deprecation.py`
的完整 205 tests。一次 5 秒 wrapper timeout 在 collection 阶段停止且 runner audit 为零，没有 node
结果。首个 terminal=`122 passed / 83 failed in 275.38s`，全部为新 phase 前多一个换行导致 suffix marker
错位的级联；byte-level relocation 先证明移除新增 block 后与 pushed base `3,066,628 bytes` / SHA-256
`6824df74142c45e9265f44e1b9f773979604853a578b410dcb6cd3ae291dea97` 完全一致，再把同一 block 移到 EOF。
第二个 terminal=`122 passed / 83 failed in 229.43s`，精确差集仅
`tests/test_trading2452_architecture_contract.py` 的 historical successor 特例缺 additional-supersession
声明；补 exact path 后第三个 terminal=`204 passed / 1 failed in 225.06s`，仅旧 2500 proposal phase
自测尚未消费新 successor paths。最小修复后定向 old/new/Wave14 三节点=`3 passed`，第四次同覆盖最终
`205 passed in 228.31s`。所有失败轮仅作为 focused failure-fix evidence，不是 formal promotion evidence；
historical prefix/hash 与历史 payload 从未改写。

2026-08-08：final-tree source refresh 后同一 205-test 覆盖重跑为
`204 passed / 1 failed in 228.67s`。唯一失败是 evidence-review phase 已把
`compatibility_regression` 收口为 `PASS_205_TESTS_N16_LOADFILE`，而同一 current-authority test 仍精确
期待 `PENDING_FINAL_TREE`；source hash、historical prefix 与其他 204 节点全部通过。最小修复仅同步该
validation expectation，随后必须用相同 `-n 16 --dist loadfile` 完整覆盖重跑；该 204/1 轮只作为
failure-fix parent，不是 formal promotion evidence。

同步 expectation 的首次定向预检 `1 failed in 7.40s`，原因是重复键文本使 patch 命中一处早期历史
测试、未命中 TRADING-2500 EOF 节点；在任何完整重跑前已原样恢复早期测试，并用 2500 唯一上下文
更新正确节点。compatibility baseline historical payload 未修改；该定向失败同样不是 promotion evidence。

2026-08-08：Owner/independent reviewer 签署 exact attestation
`owner_attestation:TRADING-2500:2026-08-08:accept_qc_daily_capability_retry_evidence_v1`。新增 strict
`daily_capability_gate_retry_review` loader；它重新解析原 evidence schema/seal/canonical bytes，并从真实
project/code/range/session/quote/Greeks/IV/OI/order/fill/raw-row/prohibited-action facts 派生 review record，
不信任调用者构造的 PASS。attestation canonical file/content SHA-256=
`2c5ed5b80a101e0fc8a0285fabb941722189f3d034837df560292b1a031d132a` /
`46690e117b7e89367bd37dcf1b17c28d6b097a7426a2bc3666337a52a621aded`；focused review+evidence=
`55 passed in 5.88s`，Ruff PASS。

Terminal decision=`GO_FOR_DAILY_ENGINEERING_ONLY`，successor scope=`DAILY_ENGINEERING_ONLY`，2499 已登记。
该结论不解除 2493 broader NO-GO，不证明完整历史 coverage/license/download/投资有效性，不激活 2485
selection 或 2486 execution policy，也不授权进一步 external action。Atlas TRADING-2501 registration window
将在 2500 ordinary push 后先行；2499 START/LANE 与实现等待包含 2499/2501 两行登记的 exact latest main。

Task-shadow 以本 worktree `PYTHONPATH=src;.` authority 重建并 validate 为 `966/461/505`、两代
byte-identical；DevEx generate/validate 为 `1095 modules / 1259 tests / 856 direct writers / 0 violations`。
首次完整 compatibility/deprecation 206-test terminal=`204 passed / 2 failed in 285.06s`：仅新 module/test
造成 frozen deprecation inventory id/count stale，以及旧 2500 phase successor skip 未承认 terminal-review
已接管 architecture fitness；无 review/DQ/2499 semantic failure。刷新精确 inventory 三字段、提升 append-only
current authority 后，定向复核=`6 passed`，再以完全相同
`python -m pytest -n 16 --dist loadfile tests/test_arch_004_refactor_policy.py tests/test_arch_004g_deprecation.py`
覆盖得到 `206 passed in 290.35s`。首轮 204/2 与此前 targeted 3/1 均只作为 failure-fix 证据，不作 formal
promotion evidence；historical prefix 持续为 `3,076,157 bytes` / SHA-256=
`6dbe0d4a66bb6b00f6b68d830b84a2c0edd55c6bc25237d9ac575bb997896ae5`，byte-identical。

首次 final-tree 五级中 Architecture/Contract/Integration/Reproducibility 分别
`853/276/995/24` PASS；exclusive Full=`8589 passed / 9 failed / 6 skipped`。九个失败全部在
`tests/research_strategies/test_o1_relative_opportunity_event_attempt_ledger.py` 的 fixture 构建阶段，
同源缺失上述 ignored TRADING-2464 DQ gate，2500 review/compatibility/contract 无 node failure。
该 Full artifact=`full_20260808T043407Z/test_runtime_summary.json`，作为 failure-fix parent 保留；
修复边界仅为已记录的 canonical ignored-artifact hydration 与本说明，不改变 shared contract、DQ/PIT、
review decision 或投资语义。修复后先以相同 `-n 16 --dist loadfile` 运行该完整 test file，再从最终
tracked tree 重跑五级，Full provenance 使用 `failure_fix_rerun` 并绑定该 parent。
