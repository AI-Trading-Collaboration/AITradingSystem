# TRADING-2549：Evidence-First Research Portfolio And Reader Entry Reset V1

最后更新：2026-08-31

- stable task id：`TRADING-2549_EVIDENCE_FIRST_RESEARCH_PORTFOLIO_AND_READER_ENTRY_RESET_V1`
- priority：`P0`
- status：`DONE`
- governed mode：`SINGLE_LANE`
- task class：`MANDATORY_CORRECTNESS + DIRECT_EXPERIMENT_ENABLER`
- production effect：`none`
- broker action：`none`

## 1. Owner 决定与问题

Project Owner 确认项目存在系统性重心偏移：工程、治理、页面和 task successor 的推进速度显著快于
策略研究 verdict。Owner 指示按 evidence-first 方案调整后续任务，并要求策略研究页面从普通研究读者
视角重新审阅和压缩；完成后先观察后续任务是否真正改善，不把本轮变成新的长期治理主线。

当前根因不是 DQ/PIT、审计或外部动作边界过严，而是缺少比 task backlog 更高一级的研究调度规则：
完成工程 blocker 后没有机械切换到 empirical experiment，工程/page PASS 又与 research verdict 共用
一套视觉进度语言。现有 Atlas 2524--2527 已实现 why-first、状态对象化、progressive disclosure 和
自动 accessibility 检查，但默认 reader mainline 仍包含七个展开区、五个 canonical questions、三轴验收、
change、flow 和大量 QQQ Options/task 细节；这是“重新排序细节”，不是足够简洁的研究读者入口。

本任务只做一次短、可退出的 research-priority correctness wave：建立 evidence-first research portfolio
authority，选择下一 primary evidence question，并把 Atlas L0 压缩为研究问题、当前 verdict、证据阶梯、
下一实验和停止条件。它不运行数据下载、回测、QuantConnect、provider、paper/live/production/broker，
也不产生投资结论。

## 2. 权威顺序与任务准入

后续研究调度权威顺序冻结为：

1. Project Owner 选择的 `primary_evidence_question`；
2. reviewed research charter / portfolio policy；
3. 已预注册 experiment 与唯一 typed blocker；
4. canonical task register；
5. presentation、Atlas、便利性与非阻塞优化。

新 P0 必须且只能归入以下一种 admission class：

- `EMPIRICAL_EVIDENCE`：直接产生 `RETAIN / REJECT / INSUFFICIENT`；
- `DIRECT_EXPERIMENT_ENABLER`：解除一个已登记主实验的唯一 typed blocker，并在完成时机械交还实验；
- `MANDATORY_CORRECTNESS`：修复 DQ/PIT、研究窗口、会计、执行、安全或结果完整性错误；
- `PUBLISHING_OR_CONVENIENCE` 默认不得成为 P0，除非同时满足前三类之一。

每个新 P0 必须声明 `research_question_id`、`decision_enabled`、`evidence_type`、
`blocked_experiment`、`stop_condition` 和 `successor_condition`。`blocked_experiment` 已解除时，下一研究
P0 必须回到 empirical experiment；继续创建不必要的 contract/projection/successor task 为
`RESEARCH_PRIORITY_DRIFT`。

同一研究假设使用一个 umbrella task 和 supporting requirement 分阶段推进；contract、DQ、execution、
result admission、reader projection 与 closeout 不再默认拆成独立 successor。失败后只有新 PIT 数据、
新经济机制、会改变结论的实现错误或真正新的 prospective evidence 才允许重开；换 threshold、删除不利
时期、新增后验 benchmark、改 gate taxonomy 或继续增加模型复杂度不足以重开。

## 3. 当前 Research Portfolio 冻结

项目最终目标：在严格 PIT、合理成本、可重放条件下，高效证伪或保留可能有研究价值的策略，为人工投资
判断提供可解释证据。

当前唯一 primary evidence question：

`SIGNAL_VALUE_FIRST_LAYER_COMPOSER_V2`

问题：冻结的 `first_layer_composer_v2` 五态信号，在固定资本、固定时钟、固定成本和预注册 benchmark 下，
是否提供可保留的增量价值？

证据阶梯：

- engineering reproducibility：`READY`；
- primary-window DQ/PIT：`READY`；
- exact 1202-session signal package：`READY`；
- signal value：`UNRESOLVED`；
- same-signal implementation value：`NOT_RUN`；
- robustness / prospective evidence：`NOT_ESTABLISHED`；
- production：`NOT_ELIGIBLE`。

下一实验固定为单一 `FROZEN_SIGNAL_VALUE_CONFIRMATION` umbrella task。它不得修改信号、搜索参数或使用
option data；输出只允许 `RETAIN / REJECT / INSUFFICIENT`。只有 `RETAIN` 才允许把已冻结的
TRADING-2548 paired implementation comparison 提升为下一 primary question；`REJECT` 关闭 option
implementation 的 P0 优先级，`INSUFFICIENT` 只允许补明确缺失的 future evidence。

`2021-02-22..2025-12-02` 继续是项目 primary window，但必须标为已反复接触的 historical
development/confirmation evidence；算法 walk-forward 不得冒充 researcher-pristine OOS。真正 pristine 的
确认只来自本 policy 冻结后的 prospective sessions。

## 4. Reader Entry V2

Atlas 首页 L0 只回答五件事，并由同一 evidence-first portfolio authority 投影：

1. `研究问题`：现在到底在检验什么；
2. `当前 verdict`：已经证明什么、尚未证明什么；
3. `证据阶梯`：engineering/data/signal/implementation/robustness/production 分层状态；
4. `下一实验`：为什么它是信息增益最高的下一步；
5. `停止条件`：什么结果会关闭或转向当前主线。

L0 禁止出现 task ID、contract ID、slot count、raw enum、SHA、receipt、manifest、完整 ledger、transport axis、
三轴页面验收矩阵或全量 canonical question list。紧凑 freshness/safety strip 可以默认可见，但不能占据首屏
主要注意力。

L1 只通过一个默认关闭的“研究细节”入口到达 historical context、canonical questions、snapshot change、
计算、flow、QQQ projection 与 result ledger。Audit 通过独立默认关闭入口到达 exact identifiers、hashes、
receipts、manifests、sidecars、source coverage 和 compatibility evidence。关键风险、禁止推断和
`production_effect=none / broker_action=none` 不得只藏在关闭层。

现有 2524--2527 的 source binding、状态对象化、无 nested disclosure、keyboard/accessibility contract 与
human acceptance `PENDING_REVIEW` 全部保留；本任务不得把自动验证升级为 Owner visual 或 reader
comprehension PASS。页面默认内容显著变短后，2526-B/2527-B 仍需绑定新的 exact HTML identity 重新验收。

## 5. 实施步骤

### S0：registration boundary

- 建立本 supporting requirement 和 canonical task row；
- 不修改 AGENTS、policy、renderer、HTML 或 generated authority；
- registration transaction 只服务 task/source-of-truth 建立，随后释放。

### S1：evidence-first steering serial contract

- 新增 strict portfolio policy、loader 与 focused negative/golden tests；
- 冻结 primary question、evidence ladder、P0 admission classes、必填字段、phase switch、reopen 和
  no-automatic-successor 规则；
- 根 `AGENTS.md` 是既有冻结策略契约的 exact authority，本任务不改变其 bytes；evidence-first steering
  由独立 reviewed policy、canonical task/requirement 和下一 empirical handoff 承载，避免批量重签历史投资契约；
- 不建立第二套 task registry、锁、scheduler 或 publication queue。

### S2：Reader Entry V2 serial consumer wave

- 更新 reader projection contract，使 L0 只有 compact safety/freshness 与五问 reader entry；
- renderer 从 portfolio contract 投影五问，旧 reader modules 进入一个默认关闭 L1，audit 保持独立；
- 更新 source binding、page effectiveness、Atlas tests、accessibility validator 与 canonical HTML；
- 更新 `docs/system_flow.md`、artifact/report/architecture/compatibility generated authority；
- 不改变研究事实、DQ/PIT、window、option policy、backtest result 或 external authority。

### S3：next empirical handoff

- 登记一个 umbrella `FROZEN_SIGNAL_VALUE_CONFIRMATION` 任务，初始状态只允许 `READY`/`PROPOSED`，不在
  本 task 中运行回测；
- 2524--2527 保留历史实现和未完成人工验收事实，并把后续 exact-HTML review 指向 Reader Entry V2；
- 页面和 task status 不冒充 signal verdict。

### S4：validation、publication 与观察

- focused pytest-xdist、Ruff、strict mypy、py_compile；
- Architecture、Contract、Integration、Reproducibility 与 Full 在最终候选自然边界运行；
- local-main ff-only、ordinary non-force push 和 SHA 复核；
- 进入观察期：后续新 P0 由本 policy 审查，Owner 根据实际协作感受决定是否需要校准，不自动增加治理层。

## 6. Acceptance criteria

1. Primary evidence question 与证据阶梯有单一 reviewed source；
2. 新 P0 admission、必填字段、phase-switch、reopen 与 successor 规则可机械验证；
3. 页面 L0 只含五问 reader entry 和紧凑安全/时效边界；
4. task/hash/receipt/manifest/完整 ledger 不进入 L0；
5. L1 只有一个默认关闭入口，audit 独立且默认关闭；
6. current verdict 明确为 signal value `UNRESOLVED`，不得把工程或 option capability PASS 升级；
7. TRADING-2548 保持 conditional successor，不被本任务授权执行；
8. 2526-B/2527-B 对新 HTML 保持 `PENDING_REVIEW`；
9. no data download/backtest/QuantConnect/provider/paper/live/production/broker action；
10. 本任务完成后，下一研究主任务是 empirical signal-value confirmation，而不是新的治理 successor。

## 7. Path ownership 与临时工作区

Task-owned paths：

- 本 supporting requirement；
- evidence-first portfolio policy/loader/tests；
- reader projection contract、portfolio reader projection 与对应 focused tests；
- renderer/page policy/accessibility 的 Reader Entry V2 变更。

Coordinator-owned paths：

- canonical task fragments/index/views；
- `docs/system_flow.md`、artifact/report registry；
- architecture/report-flow/compatibility generated authority；
- formal validation artifacts、final HTML/manifest/sidecars。

临时 Git worktree：`D:\Work\AITradingSystem_trading2549_evidence_first`；owner=`TRADING-2549`；
purpose=registration、single-lane implementation、validation 与 final candidate；exit condition=validated candidate
进入 local/origin main、required evidence 进入 canonical location、tracked/untracked/ignored unique-content audit
完成且无活跃进程依赖后，以 `git worktree remove` 清理。若本轮未能安全收口，必须在本 requirement 和
canonical task 中记录保留原因、风险、next owner 与具体退出条件。

临时浏览器产物：`D:\Work\AITradingSystem_trading2549_evidence_first\output\playwright\trading-2549-reader-entry-v2`；
purpose=对最终 exact-commit 静态页面执行 desktop/mobile 默认态、单一 L1 展开与截图检查；exit condition=
关键视觉尺寸和检查结论记入本 requirement、截图不存在唯一未记录 evidence 且无浏览器进程依赖后删除。

known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 不读取、不 hash、不 diff、不 stage、
不修改。

## 8. 当前边界与开放问题

- 本任务不运行 empirical research；
- 不新增投资解释阈值；
- 不改变 2021-02-22 primary research start；
- 不替 Owner 发明 2527 human pilot sample/threshold/reviewer/signature policy；
- `FROZEN_SIGNAL_VALUE_CONFIRMATION` 的 primary metric、benchmark 和 stop rule 必须在未来独立任务中于
  result-blind 状态冻结；
- `production_effect=none`、`broker_action=none`。

## 9. 进度记录

- 2026-08-31：Owner 确认采用 evidence-first 任务调度方案，并要求同步调整策略研究页面后观察实际改善。
  READ_ONLY governed preflight PASS；local main=origin/main=`ab2c7077ec38d92d40d2b9143a595b7508885949`。
  既有 2524--2527 实现已审计：why-first 和 accessibility 基础可复用，但默认七个 reader sections 与超长
  desktop/mobile 页面仍未满足普通策略研究读者的注意力预算。本轮选择一个短 `SINGLE_LANE` serial
  correction，不并行创建第二套页面或治理系统。
- 2026-08-31：S0 registration boundary 完成并形成独立注册 commit `c12b2ce06`；随后
  `SINGLE_LANE/LANE --contract-change` preflight PASS。S1 已新增 strict
  `evidence_first_research_portfolio.v1` policy/loader/tests，并把 evidence-first P0 admission、phase switch、
  no-automatic-successor 与 reopen 规则冻结在独立 reviewed policy。S2 已将 `reader_projection_contract` 升级为 Entry V2，
  Atlas live/page policy 前移到本任务，renderer 默认只投影主问题、`UNRESOLVED`、七级证据阶梯、下一实验、
  stop condition 和禁止推断；既有 why-first、五问、change、acceptance、flow、QQQ projection 与 ledger 统一
  下沉到单一默认关闭 L1，audit 继续独立关闭。首轮 focused pytest 暴露术语大小写与页面 successor 分类
  漂移，两项均已通过显式 policy/contract 修复，没有加入绕行或弱化 validator。
- 2026-08-31：S3 已登记 `TRADING-2550_FROZEN_SIGNAL_VALUE_CONFIRMATION_V1`，状态=`PROPOSED`、
  evidence type=`EMPIRICAL_EVIDENCE`。它只固定下一经验研究的 umbrella 边界，仍需在
  outcome-blind 状态冻结 comparator/metric/capital/clock/cost/reducer 并取得适用授权；本轮没有运行实验。
  Atlas live policy 将 current mainline 前移到该 proposed handoff，但 Reader Entry 的 signal-value verdict 仍为
  `UNRESOLVED`，没有把任务登记误写成研究进展或投资结论。
- 2026-08-31：task implementation 在 frozen lane commit `809dc37321` 完成。期间 local/origin `main`
  前进到 `a2ddfac116`；`integration-revalidation-d3f3a217daa3c6e156de` 对 exact Git delta 判定
  `RECONCILIATION_REQUIRED`、blocker count=`0`，只报告 `docs/system_flow.md`、task register/index 与
  S5 count test 四个协调者重叠。最终候选从最新 `main` 重建：task-only bytes 原样吸收，system flow 按
  当前语义人工协调，generated task views/index 丢弃旧 lane bytes 后由 canonical writer 重建。
  `TRADING-2550` 与 `TRADING-2549` 均通过 task-matched publication transaction 在最新基线重放；最终
  task count=`1042`、active=`508`、completed=`534`。2549 转为 `DONE`，2550 保持 `PROPOSED`；这只表示
  steering/reader reset 已实现，不表示 signal value 已判定，也不签署 Owner visual 或 reader comprehension。
- 2026-08-31：首次 report-flow rebuild 对 `docs/system_flow.md` 的旧 exact byte/SHA seal 正常 fail closed；
  没有跳过 validator。最终 publication transaction 切换为
  `trading-2549-final-publication-20260831-v10`，显式把 report-flow policy seal 与 regression expectation
  纳入 coordinator scope 后继续重建。v8 focused validation 随后又暴露 terminal task notes 丢失 requirement
  link，以及 report-flow aggregate 从 3107 增至 3108 的 exact expectation 漂移；v9 通过 canonical writer
  保留 requirement binding，并更新该机械计数，没有放宽 source 或页面验证。v9 focused 结果为
  `70 passed / 1 failed`；唯一失败是 task event 使用 UTC 日期字符串，而 Atlas research-state contract 按
  `Asia/Tokyo` project date 断言 2026-08-31。v10 追加等价 JST terminal event，显式保存时区语义，不改变
  研究事实或页面 verdict。
- 2026-08-31：v10 clean candidate `a04c90683` 的集成 preflight 按设计阻断：2549 已过早转为 terminal
  `DONE`，且所提供 plan 仍绑定 frozen lane `809dc37321`，而不是 clean latest-main candidate。实现 bytes 与
  失败 transaction evidence 均保留；同一 worktree 从 `a2ddfac116` 重组 final candidate，先以
  `VALIDATING` task state 和 candidate-bound plan 通过 INTEGRATION，再在正式发布 transaction 的
  `TASK_SOURCE_PRE_WRITE` 阶段归档为 `DONE`。不创建新的治理 successor，不绕过 terminal-state 或 plan gate。
- 2026-08-31：clean pre-integration candidate `7da367eb1` 从最新 `main=a2ddfac116` 构建；
  `integration-revalidation-971bf03dd535d86f01b1` 重算结果为
  `READY_FOR_SINGLE_INTEGRATION_CANDIDATE`、blocker=`0`、overlap=`0`。在 2549 保持 `VALIDATING` 时，
  `SINGLE_LANE / INTEGRATION / contract-change` preflight 对 exact candidate、task、claims、plan、lease 与
  `trading-2549-final-publication-20260831-v11` 返回 `PASS`；随后才由 canonical writer 转为 `DONE`。
  `TRADING-2550` 继续为 `PROPOSED`，没有执行数据下载、backtest、QuantConnect 或任何外部/生产/broker 动作。
- 2026-08-31：对 candidate `0cebccbb0` 的 exact-commit HTML 完成本地 Playwright 自动检查：desktop
  `1440×900` 默认态 `scrollHeight=1945`、mobile `390×844` 默认态 `scrollHeight=3111`，两者均满足
  `scrollWidth=clientWidth`、无横向溢出；默认态唯一 `h1`，研究细节按钮 `aria-expanded=false`，点击后变为
  `true` 并展示既有研究材料，再次点击可关闭；console error/warning=`0/0`。相较旧页约 14,407px desktop、
  29,970px mobile 的默认长度，当前入口显著缩短。截图人工观察未发现文字截断、卡片溢出或移动端按钮不可达，
  但这仍只是自动化/实施者检查，Owner visual 与 reader comprehension 必须保持 `PENDING_REVIEW`。
- 2026-08-31：v11 Architecture formal tier 使用 16 workers 运行 `881` tests，结果为
  `879 passed / 2 failed`；immutable failure artifact 为
  `outputs/validation_runtime/architecture-fitness_20260831T001527Z/test_runtime_summary.json`。两项失败均为
  exact generated expectation 随动：新增一个 contract module 与一个 test 后 deprecation inventory 从
  `1182/1339` 变为 `1183/1340` 并产生新 inventory id；system-flow 新增一个 block 后 DEVX-011 聚合计数从
  `3107` 变为 `3108`。generated inventory 与 report-flow authority 本身均已重建且 PASS；v12 只更新对应
  exact regression expectations，并要求原 16-worker Architecture tier 完整重跑，不改生命周期、writer allowance、
  研究结论或安全边界。
- 2026-08-31：v12 前置正式层级全部 PASS：Architecture=`881 passed`、Contract=`278 passed`、
  Integration=`995 passed`、Reproducibility=`24 passed`。随后 Full 在 16 workers 下得到
  `9692 passed / 340 failed / 6 skipped`，immutable failure artifact 为
  `outputs/validation_runtime/full_20260831T003810Z/test_runtime_summary.json`，SHA-256=
  `17556bee6a1d66536b5599e48152eb63815afb3ef02d513c699e5fc8c756579f`。失败收敛为三类：新增
  `AGENTS.md` 段落使历史冻结策略 authority `PROJECT_ENGINEERING_RULES` 哈希级联失配；Atlas task source
  数量从 78 增至 80 后两条 exact expectation 未刷新；隔离 worktree 缺少历史本地证据。v13 采用最小正确
  修正：撤回根 `AGENTS.md` bytes 变化，保留独立 evidence-first policy/loader/tests；刷新 80 的页面期望；
  不批量重签历史投资契约，也不把 Full 失败降格为局部 PASS。
- 2026-08-31：为 parent-bound failure-fix Full 建立最小 validation snapshot，只从主 checkout 同字节复制
  `outputs/validation_runtime/trading_2464_o1_dq_20260729T183000Z/o1_dq_gate.json`、
  `outputs/research_trends/operational_forecast/trading_2542i_real_v3/real_materialization_receipt.json` 和
  `.../manifest_replay_receipt.json`，以及 1,205 文件、3,478,266 bytes 的 exact frozen signal package 目录
  `outputs/qqq_options/signal_packages/trading_2542i_operational_forecast_real_v3/` 到当前 task worktree 的相同
  repository-relative 路径。owner=`TRADING-2549`；
  purpose=让隔离 worktree 重放既有只读测试依赖；这些 bytes 不构成本任务新研究证据。exit condition=parent-bound
  Full 与 closeout 完成、确认无进程依赖后删除复制文件/目录及由此产生的空目录；canonical source 仍保留在主
  checkout，因此 snapshot 删除后可由原来源恢复。
- 2026-08-31：v13 根因代表集先以 16 workers 得到 `6 passed`；随后使用 pytest last-failed cache 对 v12
  仍记录的其余失败集合完整并行复验，结果为 `334 passed`。因此上一轮 340 failures 已全部被原失败 nodeid
  集合覆盖并转绿；未使用串行测试、未删除失败证据、未放宽历史 authority hash 或运行任何 empirical action。
