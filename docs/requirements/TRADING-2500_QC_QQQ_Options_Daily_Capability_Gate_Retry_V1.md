# TRADING-2500：QQQ Options Daily Capability Gate Retry V1

最后更新：2026-08-08

稳定任务 ID：`TRADING-2500_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_V1`

优先级：`P0`

状态：`IN_PROGRESS`

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

## 5. Sequencing 与验收

1. S0：登记本任务与 requirement；
2. S1：governed START/LANE preflight；
3. S2：生成 task-owned strict retry policy/proposal 与 negative tests；
4. S3：同步 architecture/system-flow/generated authority，formal final-tree validation；
5. S4：ordinary non-force push exact proposal hashes；
6. S5：Owner 签发新的 hash-bound single-use token；
7. S6：read-only account/code precheck 后执行唯一一次 retry；
8. S7：independent review 与 terminal gate；仅 GO 后登记 2499。

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

若 frozen lane 与 local main 在 integration boundary 发生 drift，2500 只在
`D:\Work\AITradingSystem_trading2500_qc_daily_retry\outputs\validation_runtime\trading2500_integration\`
创建 ignored `change_manifest.json` 与 `integration_revalidation_plan.json`。两者仅用于从 exact
base/lane-head/latest-main 重建并校验 `integration_revalidation_plan.v1`，不进入 Git；integration preflight
与 final candidate 完成后删除，所需 plan id/status 进入 terminal handoff，且不替代 canonical formal evidence。

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
