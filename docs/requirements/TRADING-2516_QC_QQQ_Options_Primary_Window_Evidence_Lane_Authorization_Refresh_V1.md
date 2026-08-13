# TRADING-2516：QQQ Options 主窗口证据车道授权刷新 V1

## 1. 状态与目的

- task id：`TRADING-2516_QC_QQQ_OPTIONS_PRIMARY_WINDOW_EVIDENCE_LANE_AUTHORIZATION_REFRESH_V1`；
- priority：`P0`；
- status：`IN_PROGRESS`；
- governed mode：`SINGLE_LANE`；
- contract change：`true`；
- registration base：`65b2bc1c88bf98132b7f6d58359ae3f18cea85f9`；
- production effect：`none`；broker action：`none`。

Project Owner 已持续要求推进 QQQ Options 工程链路，因此本任务把 TRADING-2515 的唯一数据证据车道选择落实为
`QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE`。该选择只授权本地合同与授权请求刷新，不等于
QuantConnect 外部动作授权，不等于 DQ/PIT PASS，不提供 G2 policy values，也不激活 selection、engine、
backtest 或投资解释。

TRADING-2513/2514 已冻结 `2026-08-12` decision token 与 `2026-08-19T00:00:00Z` expiry。当前继续要求
Project Owner 使用旧日期会形成倒签风险；本任务必须以 versioned successor 生成新的 hash-bound 授权请求，
不得覆写或改写 2513/2514 历史 authority。

## 2. 继承 authority

本任务严格继承且不得重定义：

- 2481 shared records/envelope 与 public enum；
- 2482 DQ/PIT、cache/evidence identity 与 15 required checks；
- 2484 QuantConnect project adapter contract；
- 2499 DAILY primary chronology 与 `2021-02-22` primary start；
- 2500 `GO_FOR_DAILY_ENGINEERING_ONLY`，不扩张为完整外部回测授权；
- 2509 slot catalog v2；2510 calibration admission；2511 derived evidence generator；
- 2512 export-safe collector；2513 exact proposal package；2514 evidence-admission receipts；
- 2515 `KEEP_CLOSED + PREREGISTRATION_ONLY`，直到真实 evidence 与 DQ/PIT 通过后由后继 reviewed decision 改变。

冻结的 collection scope 保持：

- target project id：`34808569`；
- requested/evaluated range：`2021-02-22..2025-12-02`；
- primary role / calendar：`PRIMARY` / `XNYS`；
- expected session count：`1202`；
- maximum project mutations / cloud backtests：`1 / 1`；
- maximum orders / fills：`0 / 0`；
- result carrier：Owner manual `Download Results` JSON only。

## 3. 实现范围

### S0：registration boundary

- 创建 canonical task row 与本 supporting requirement；
- 重建 task shadow、DevEx 与 current compatibility authority；
- rerun governed `START/LANE` preflight 后才能进入实现。

### S1：versioned authorization refresh contract

- 新建 task-owned successor policy 与 typed authorization request；
- exact-bind latest ordinary-pushed main、2513 proposal package、2514 admission authority、range/session/project/
  action limits 与 upstream hashes；
- decision date 使用真实新决策日期，expiry 不超过 168 小时；
- 保持 single-use 与 evidence collection 后失效；
- strict loader 拒绝旧 token、倒签、过期、重复、hash/range/session/project/action 漂移、额外文件、symlink、
  duplicate keys 与 noncanonical bytes；
- 只生成待 Owner 审阅的 unsigned request；不得代签 token。

### S2：2514-compatible pre-admission candidate

- 生成可由后继 2514-compatible admission adapter 消费的 sealed pre-admission candidate；
- 不修改 2513/2514 历史 policy/package/receipt schema 的既有语义；
- 缺失真实 Owner token 时继续输出 typed `OWNER_AUTHORIZATION_NOT_PROVIDED`；
- 未发生真实 collection 时 evidence/DQ/PIT 保持 `NOT_EVALUATED` 或 policy-blocked，不能伪造 PASS。

2516 只验证 fresh Owner decision candidate，固定 `authorization_consumed=false` 与
`external_action_performed=false`。真实 token admission、single-use consumption ledger、Cloud action 与 Results/DQ
admission 必须进入后继任务，不能由本任务自动执行。

### S3：consumer disclosure 与 closeout

- Atlas/readiness 只披露“QQQ Options evidence lane 已选、fresh authorization request 已准备、Owner token/run/
  evidence/DQ/PIT 尚未发生”；
- 更新 `docs/system_flow.md`、canonical task registry/generated/compatibility authority；
- focused/property/golden/negative tests 后，在 final tree 串行运行 Architecture → Contract → Integration →
  Reproducibility → exclusive Full；
- ordinary non-force push、SHA verify、branch/worktree cleanup。

## 4. 明确禁止

本任务不授权并且不得执行：

- QuantConnect login、project create/modify、cloud backtest；
- API、CLI、HTTP、Object Store；
- raw options data download、raw option row logging/export；
- purchase/subscription、第二次 cloud backtest、range expansion；
- order/fill、paper/live/broker/production；
- selection、engine activation、G2 policy values、投资解释或策略结论。

## 5. 验收标准

1. QQQ Options evidence lane selection 与 external authorization 分轴记录；不得把前者解释为后者。
2. 2513/2514 tracked authority bytes 不被覆写；successor exact-bind 两者 hashes 与 latest main。
3. 新 token request 使用真实日期、expiry `<=168h`、single-use、evidence 后失效；旧/倒签/过期 token fail closed。
4. range/session/project/action caps 与 2513 保持完全一致，orders/fills 固定为零。
5. authorization、run、evidence、DQ/PIT 任一未发生时均不能产生 PASS 或 engine unblock。
6. unit/property/golden/negative coverage 包含 tamper、duplicate key、hash/scope/as-of/expiry mismatch、token reuse。
7. external action、production effect、broker action 均为 `none`；final governed gates、ordinary push 与 cleanup 完成。

## 6. Path claims

Task-owned：

- `docs/requirements/TRADING-2516_QC_QQQ_Options_Primary_Window_Evidence_Lane_Authorization_Refresh_V1.md`；
- 2516 successor policy/module/tests/package paths（实现阶段精确声明）。

Coordinator-owned shared paths：

- canonical task registry/index 与 generated task shadows；
- `docs/system_flow.md`；
- architecture fragments、DevEx seal、compatibility/current-authority sources；
- Atlas task coverage/disclosure（只做事实同步，不改变页面 acceptance/freshness 语义）。

## 7. Progress

- 2026-08-13：latest main、runner、lease、task-id availability 与 2481–2515 authority 已 READ_ONLY 审计；
  `TRADING-2516..2519` 均未占用。
- 2026-08-13：发现 2514 expected token 固定为 2026-08-12；选择 versioned successor，禁止倒签或改写历史。
- 2026-08-13：task policy file/canonical SHA-256=`4aa2983a6cb6c0ac02d03d18a807ea3bdf553770ac545130011911bf83caca77` /
  `acd849fd8189256d4908cc162eb0c9bfe4162c669760577f21d6c960919b4882`；module file SHA-256=
  `b21c80a485f034874b604e6f181485ed7fb8e7a73debdfcc2749f831c65e2763`。
- 2026-08-13：unsigned request SHA-256=`d351ed7c54eb0531a29bdd5d27e5e518a1870ef89aefd53041ad91fd6c45457e`；
  package manifest file/canonical SHA-256=`7373474ee0279f70dcc678f6325935c82e96b90e5e46da82613bb8fcb106d924`，
  content SHA-256=`0978dceaefb1acec33e2da2681075128c880d19ce4b01a194a7b38961f943381`。
- 2026-08-13：task-only focused=`16 passed in 46.33s`。task + Atlas 同覆盖首轮=`31 passed / 20 failed`；
  19 项为 loader 漏保留 2515 的级联，1 项为旧 34-task ignored canonical page。补回 2515、完整 writer 重建
  35-task package并保留 ENGINEERING/OWNER_VISUAL/READER_COMPREHENSION 三条 PASS 后，相同覆盖=
  `51 passed in 78.27s`。
- 2026-08-13：2510–2516 + Atlas 相邻覆盖首轮=`194 passed / 1 failed`，唯一失败为 generated authority
  重建后 ignored canonical page source identity 正确 stale；final generated bytes 上完整 writer 重建后，相同覆盖=
  `195 passed in 80.47s`。
- 2026-08-13：compatibility/deprecation 原样 `-n 16 --dist loadfile` 覆盖首轮=`210 passed / 1 failed`
  （ARCH-004G current inventory identity stale）；第二轮仍=`210 passed / 1 failed`（测试常量已更新、frozen YAML
  尚未同步）；仅同步 exact inventory id 与真实 `1115 modules / 1276 tests / 856 writers` 后，第三轮=
  `211 passed in 295.63s`。历史 prefix、exact-byte/hash 验证与 removal gate 未放宽。
- 2026-08-13：正式 Architecture 首轮=`864 passed / 1 failed`，artifact=
  `outputs/validation_runtime/architecture-fitness_20260812T191928Z/test_runtime_summary.json`；唯一失败为 ARCH-005
  self-hosted registry current count 仍固定 986，实际新增 2516 后为 987。仅提升 exact current-authority count；
  failure-fix Architecture 必须绑定该 parent 重跑。
- 2026-08-13：首棵 formal tree 的 Architecture failure-fix=`865 passed`、Contract=`276 passed`、
  Integration=`995 passed / 642 warnings`、Reproducibility=`24 passed`；Full parent=
  `outputs/validation_runtime/full_20260812T193651Z/test_runtime_summary.json`，terminal=
  `8921 passed / 3 failed / 3 skipped / 643 warnings`。三个失败节点均属于
  `tests/test_devx_006d_report_catalog_flow_authority.py`，根因为 `docs/system_flow.md` 已包含 2516 流程，
  而 DEVX-006D exact seal 仍固定旧的 `2201903 bytes / 967 entries`。
- 2026-08-13：failure-fix 仅把 system-flow seal 提升到真实 `2203556 bytes / 971 entries / SHA-256
  8762483da0c9743e6aba360a03edbce0130f9c21806eca72bcfc8b6ffd3aad2b`，以现有 canonical writer
  重建 lossless shadow、DevEx 与 current compatibility authority；未放宽 byte-identical、coverage、prefix、
  hash 或 inactive-shadow 约束。DEVX focused 首轮=`14 passed / 1 failed`（仅 aggregate count 仍为 2890），
  同步真实 aggregate count 2894 后同覆盖=`15 passed`；compatibility/deprecation 同覆盖=`211 passed in
  279.69s`。最终 promotion 必须从修复后的 final tree 重跑五级，Full 使用该失败父运行。
- 2026-08-13：修复后 final-tree Architecture/Contract/Integration/Reproducibility 分别为 `865 / 276 /
  995 / 24 passed`。首次 failure-fix Full 在启动后被 Windows 主机重启中止（process status `0x40010004`，
  `LastBootUpTime=2026-08-13T05:33:36.5+09:00`），没有 runtime artifact 或 pytest node failure，不作为证据。
- 2026-08-13：同树 replacement Full=`8922 passed / 2 failed / 3 skipped / 644 warnings`，artifact=
  `outputs/validation_runtime/full_20260813T013110Z/test_runtime_summary.json`。两项失败分别为第二处
  DEVX-006D compatibility consumer aggregate count 漏同步，以及 ignored canonical Atlas page 在 requirement
  进度记录改变后以 `TASK_COVERAGE_OR_STATUS_DRIFT` 正确 fail closed。仅同步第二处 exact count，并使用完整
  canonical writer 原位重建 11 个 page artifacts；index.html SHA-256 仍为
  `6f1303384c67fbc22a2939a2a599ff0ff044e9a058c828e5d8b2cdb5904122f8`，既有
  ENGINEERING/OWNER_VISUAL/READER_COMPREHENSION 三条 PASS 的 reviewer/time/decision/evidence 原样保留。
  两个失败域的组合 focused 同覆盖=`29 passed in 23.21s`。最终五级必须以该 Full 为 failure-fix parent。
- 当前真实状态：Owner refresh token=`NOT_PROVIDED`；external action=`none`；collection/evidence/DQ/PIT=`not occurred`；
  engine=`POLICY_BLOCKED_CASH_PRESERVATION`。
