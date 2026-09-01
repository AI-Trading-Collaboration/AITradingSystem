# TRADING-2550：Frozen Signal Value Confirmation V1

最后更新：2026-09-01

- stable task id：`TRADING-2550_FROZEN_SIGNAL_VALUE_CONFIRMATION_V1`
- priority：`P0`
- status：`BASELINE_DONE`（唯一 bounded empirical confirmation 已完成并准入；frozen verdict=`RETAIN`）
- task class / evidence type：`EMPIRICAL_EVIDENCE`
- research question id：`SIGNAL_VALUE_FIRST_LAYER_COMPOSER_V2`
- production effect：`none`
- broker action：`none`

## 1. 任务目的

这是 evidence-first portfolio 选定的下一项 umbrella research task。它只回答一个问题：冻结的第一层五态
组合信号，在固定资本、固定时钟、固定成本和结果不可见时预先登记的比较基准下，是否提供值得保留的增量
价值。它不得在同一任务中修改信号、搜索参数、删除不利时期或事后添加 benchmark。

Owner 已按 exact file/canonical SHA 批准并冻结 outcome-blind `draft.1`。批准通过独立 immutable
freeze admission 接纳，不改写获批草案 bytes/status；它仍不授权读取市场结果、DQ、收益计算、confirmation、
backtest、QuantConnect、外部提供方、cache、paper/live/production/broker，也不生成投资结论。

2026-09-01 的后续 Owner 决定另行激活一次 bounded `DATA_RESEARCH` run：只读访问 allowlisted
既有 QQQ adjusted-close，`manifest replay / canonical DQ / local confirmation / independent replay`
各最多一次；download、cache mutation、QuantConnect、option/provider、paper/live/production/broker
以及 orders/fills/positions 继续为零。该授权不改写 preregistration 或 freeze admission，而由独立
`frozen_signal_value_confirmation_run_authorization_v1` 绑定和消费。

## 2. P0 admission fields

- `research_question_id`：`SIGNAL_VALUE_FIRST_LAYER_COMPOSER_V2`；
- `decision_enabled`：在同资本、同成本的预注册比较下形成一次 `RETAIN / REJECT / INSUFFICIENT` signal-value verdict；
- `evidence_type`：`EMPIRICAL_EVIDENCE`；
- `blocked_experiment`：`none`；本任务就是当前 portfolio 的 primary experiment，不得再以页面、投影或便利性
  successor 代替；
- `stop_condition`：`RETAIN` 才进入期权实现 paired comparison；`REJECT` 关闭该实现路线的 P0；
  `INSUFFICIENT` 只补 verdict 明确指出的 prospective evidence；
- `successor_condition`：只允许上述三个 verdict 机械选择 conditional successor，不得自动创建新的治理任务。

## 3. 启动前必须冻结

1. exact 1,202-session signal package 与输入身份；
2. primary comparator、primary metric、成本、资本、时钟、缺失值和失败传播规则；
3. 结果不可见的 `RETAIN / REJECT / INSUFFICIENT` reducer；
4. historical development/confirmation 与 prospective evidence 的用途边界；
5. 一次性研究运行 manifest、资源上限、零生产/零 broker 边界和 terminal artifact；
6. 结果准入与独立重放路径。

这些内容应留在同一个 supporting requirement 和 umbrella task 的阶段记录中；除非出现真正的 shared contract
wave 或独立外部授权边界，不再为 contract、DQ、execution、result admission、reader projection 和 closeout
自动拆分 successor。

### 3.1 `draft.1` 的研究对象

本草案不重建趋势判断，也不使用 option data。唯一信号仍是 exact 1,202-session
`first_layer_composer_v2:trend_state` package：

- `constructive / risk_on -> 100% QQQ`；
- `defensive / neutral / risk_off -> ZERO_RETURN_CASH`；
- 不允许 leverage、short、options、参数搜索或结果出现后修改映射；
- 使用 package 已冻结的 `NEXT_VALID_US_EQUITY_SESSION` 一日 lag，不从本地重新解释 signal date。

这是一条信号价值 measurement implementation，不是新的 allocation authority，更不是面向期权的第二条预测链。

### 3.2 primary comparator 与共同会计

唯一 primary comparator 提议为 `EXPOSURE_MATCHED_STATIC_QQQ_ZERO_RETURN_CASH`。在加载价格结果前，仅从冻结
signal plan 计算：

`target_qqq_weight = LONG exposure return intervals / 1201`。

Comparator 在首个可评估 close 按该固定比例买入 QQQ，剩余为零收益现金，持有固定 shares 到末日 close 清算；
不根据价格结果拟合，也不允许运行后追加 comparator。Candidate 与 comparator 均使用：

- initial capital=`USD 100,000`；
- adjusted-close、effective-session close 到下一 XNYS session close；
- fractional shares、无负现金、无 fill-forward、无缺失价格插值；
- 相同 `FIXED_TRADED_NOTIONAL_BPS` cost formula；
- initial allocation、target change 与 terminal liquidation 均按 one-way `5 bps` 计费。

QQQ buy-and-hold 只作 market context；calendar-year 与 pre-2023/post-2022 slice 只披露 concentration，不参与
primary verdict。这样 primary test 比较的是同计划平均 QQQ exposure 下的 timing increment，而不是把更少持仓
误写成 signal alpha。

### 3.3 primary metric、falsification 与三态 reducer

- primary metric：`candidate net total return - exposure-matched comparator net total return`，单位为 percentage
  points；
- RETAIN return threshold：严格 `> 0`；
- mandatory falsification guard：`candidate max-drawdown magnitude - comparator max-drawdown magnitude <= 0`；
- 任一 identity、DQ/PIT、manifest replay、1202/1201 coverage、missing/duplicate/unknown/imputation、accounting/
  cost reconciliation、metric、independent replay 不满足，先输出 `INSUFFICIENT`，不得落入性能比较；
- 两个结果条件同时满足才 `RETAIN`；任一 return `<= 0` 或 drawdown regression `> 0` 则 `REJECT`；
- diagnostics 不得覆盖 primary verdict。

Stop action 仍保持 portfolio 已冻结的语义：`RETAIN` 仅开启期权 paired-comparison 的 Owner review；`REJECT`
关闭该 options implementation P0 路线且不参数救援；`INSUFFICIENT` 只补 verdict 明确指出的 prospective evidence。

### 3.4 historical / prospective 与未来运行 envelope

`2021-02-22..2025-12-02` 明确标记为 `REUSED_DEVELOPMENT_CONFIRMATION`，不得称为 researcher-pristine OOS。
Prospective 起点只能是 final policy approval 后首个 XNYS session，且只能确认冻结规则。

草案包含 `SPECIFICATION_ONLY_NOT_AUTHORIZED` 的一次性 future envelope：未来若另获 exact Owner 授权，最多
允许 manifest replay=1、canonical DQ=1、local signal-value confirmation=1、independent replay=1；download、
cache mutation、QuantConnect、option backtest、provider、orders/fills/positions 均为 0。该 proposed maxima 不构成
当前授权；当前 safety 中 data read、DQ、confirmation、backtest 与所有 external action 均为 false/0。

### 3.5 exact draft identity

- policy：`frozen_signal_value_confirmation_preregistration_v1@1.0.0-draft.1`；
- file SHA-256：`507ab3dd3610971c0962fa093cec0c7f09e1b816f694b7dd946c4b9703013dfa`；
- canonical SHA-256：`7d12dd62127cb02676d4e18510c06fddc9e2a0afa03ec2f0e758ba6143bed88c`；
- authority-set SHA-256：`45d508d563b46b0929d80687155213d265399a4f105da69f31810780a34c754f`；
- current decision state：原草案 bytes 保持 `OWNER_REVIEW_REQUIRED`，独立 admission 记录
  `OWNER_EXACT_PREREGISTRATION_FROZEN_NO_EMPIRICAL_RUN_AUTHORITY`；
- execution activation：`false`。

### 3.6 exact freeze admission

- admission：`frozen_signal_value_confirmation_preregistration_freeze_admission_v1@1.0.0`；
- Owner decision：
  `owner_decision:TRADING-2550:2026-09-01:freeze_signal_value_confirmation_preregistration_v1`；
- authorization state：`EXACT_PREAUTHORIZED`；
- exact-freeze surface：signal identity、candidate、exposure-matched comparator、USD 100,000 common
  capital、adjusted-close calendar、5 bps one-way cost、primary estimand、zero thresholds、drawdown
  non-regression 与 reducer precedence；
- predecessor file/canonical/authority-set SHA 必须继续精确重放，草案不因接纳而被改写；
- signal-value verdict 仍为 `UNRESOLVED`，empirical confirmation completed=`false`；
- next legal action 仅为未来另行取得 exact bounded-run authorization；本次批准不隐式创建 successor，
  不允许读取/下载市场数据、DQ、confirmation/backtest、QuantConnect/provider/cache 或任何交易动作。

### 3.7 exact bounded run authorization

- authorization：`frozen_signal_value_confirmation_run_authorization_v1@1.0.0`；
- Owner decision：
  `owner_decision:TRADING-2550:2026-09-01:authorize_bounded_signal_value_confirmation_v1`；
- authorization state：`EXACT_PREAUTHORIZED`；
- 允许：只读既有 allowlisted QQQ adjusted-close、manifest replay=`1`、canonical DQ=`1`、local
  signal-value confirmation=`1`、independent replay=`1`；
- 禁止：data download、cache mutation、QuantConnect、option data/backtest、provider、paper/live/
  production/broker、orders/fills/positions；
- 只接纳 aggregate terminal result；raw market/option payload 不进入结果 artifact；
- 任一 exact identity、DQ/PIT、coverage、accounting/cost 或 replay gate 失败，直接按冻结 reducer
  输出 `INSUFFICIENT`，不得重试、替换输入或扩大授权。

### 3.8 bounded result 与准入

- execution manifest file/canonical SHA：`6051ad6c48b8d402c646945e4f33442f11d0435cb0228c125384776afd7c30d3` /
  `5dd18a206ad80c983db8584431074888b6bcc9fe9130d76a366377388770b32e`；
- actual counters：manifest replay=`1`、canonical DQ=`1`、local confirmation=`1`、independent
  replay=`1`；download/cache mutation/QuantConnect/options/provider/orders/fills/positions=`0`；
- canonical DQ：`PASS`，errors=`0`、warnings=`0`，requested/evaluated 均为
  `2021-02-22..2025-12-02`；
- candidate net total return=`45.27935887187362%`，exposure-matched comparator=`31.533381915138015%`，
  primary estimand=`+13.745976956735603 percentage points`；
- candidate max-drawdown magnitude=`9.647781253983167%`，comparator=`13.077171432379409%`，
  delta=`-3.4293901783962415 percentage points`；
- independent replay=`PASS`，全部 reconciliation difference 小于 `1e-8`；
- frozen reducer verdict=`RETAIN`，只允许打开 conditional options paired-comparison 的 Owner review；
  它不证明 options implementation value、robustness、pristine OOS 或 production eligibility。

### 3.9 Atlas reader-entry 可读性澄清

2026-09-01 Project Owner 要求先试行 Web Pro 对 Atlas L0 的可读性建议。本波仅调整 reader
projection，不改变任何冻结研究结论、阈值、证据身份或执行权限：

- 页首必须声明“本页仅展示当前策略研究主线，不代表整个系统总体进展”，避免新读者把单页当作全系统
  仪表盘；
- H1 使用“这个信号，值得继续研究吗？”的普通语言入口；冻结的 exact primary question 紧随其后完整
  展示，不修改 portfolio authority，也不以简化标题替代审计问题；
- 在保留七级证据阶梯的同时，以三个彼此独立的摘要分别说明 evidence/engineering readiness、当前
  research conclusion 与 production/trading eligibility；上游 `READY` 不得被解释为下游自动晋级；
- terminal verdict 继续精确投影为 `RETAIN`：仅表示冻结比较下保留 signal value，不能改写为策略整体
  已验证、robustness 已建立或 production-ready；
- 首屏 verdict banner 用普通语言解释“为什么继续研究”以及仍未证明什么；精确数值、reducer 与 evidence
  explanation 继续保留在七级阶梯和审计层，不从 authority 删除；
- 下一动作必须反映最新 governed successor 状态：conditional paired-comparison Owner review 与
  fixture-only Wave A 已完成；当前等待的是 Project Owner 另行冻结 underlying comparator fee semantics
  并批准 Wave B exact package/manifest，Wave C 单次 QuantConnect run 仍需再次独立授权；
- 默认折叠的 L1 研究详情必须与 L0 从同一 projected evidence-first portfolio 读取 terminal verdict、
  next action 与禁止推论；不得在 L0 显示 `RETAIN` 时继续硬编码“尚无经验 verdict”、`UNRESOLVED`
  或“等待 signal-value confirmation”的旧状态；
- L0 继续隐藏 task id、commit SHA、manifest id 等审计标识；详细身份只留在既有 L2 审计层。

本波不读取或下载市场数据，不运行 DQ、confirmation/backtest，不调用 QuantConnect/options/provider/cache，
也不产生 paper/live/production/broker、orders/fills/positions 动作。

## 4. Acceptance criteria

1. 预注册发生在任何 outcome 可见之前；
2. primary window 起点保持 `2021-02-22`，requested/evaluated range 显式记录；
3. signal package、comparator、capital、clock、cost、metric 与 reducer exact-bind；
4. 输出严格为 `RETAIN / REJECT / INSUFFICIENT` 之一，并解释允许的下一动作；
5. 工程 PASS、数据 PASS、页面 PASS 或既有期权 `+4.48%` 均不能代签 signal-value verdict；
6. 任一 DQ/PIT、identity、manifest、resource 或 replay gate 失败时 fail closed；
7. 运行需在未来单独通过 governed preflight 和适用的 R1/R2/R3 授权边界；
8. `production_effect=none`、`broker_action=none`。
9. Atlas L0 明示页面范围、三个独立进展面与阶段不可自动晋级；页面明确说明 Owner review 与 fixture-only
   Wave A 已完成，而 Wave B exact package/manifest 与 Wave C bounded QuantConnect run 尚未授权、未开始；
   浏览器与自动化测试共同验证该 reader projection。
10. Atlas L0 与默认折叠的 L1 研究详情从同一 terminal portfolio projection 读取 `RETAIN` 与下一动作，
    展开详情后不得出现旧的 `UNRESOLVED` 或等待 signal-value confirmation 文案。

## 5. 当前 blocker 与 next owner

- 已解除 blocker：`draft.1` 的 comparator、`5 bps` cost、zero return threshold、zero drawdown-regression
  guard 与三态 reducer 已获 Owner exact file/canonical SHA freeze；
- 当前已完成项：bounded manifest、canonical DQ、local confirmation、independent replay 与 aggregate
  result 均完成，frozen verdict=`RETAIN`；
- 已解除收口依赖：TRADING-2551 已把 evidence-first reader contract 扩展到 terminal
  `RETAIN / REJECT / INSUFFICIENT`，无需硬编码页面或绕过共享 contract；
- 已完成后继：TRADING-2552 conditional paired-comparison Owner review 与 TRADING-2553 fixture-only
  Wave A 已进入 latest `main`；Wave A 仅证明本地 ledger/admission/replay 工程边界，不是 platform evidence；
- next owner：Project Owner 仅可另行冻结 underlying comparator fee semantics 并批准 Wave B exact
  package/manifest；Wave C 单次 bounded QuantConnect run 仍需再次独立授权。当前没有新的真实数据读取、
  DQ、QuantConnect/options/provider/backtest 或交易权限；
- exit condition：result admission 与 consumer projection 精确重放 aggregate result，任务保持
  `BASELINE_DONE` 且不自动创建或执行 successor；任何新 options manifest/DQ/backtest 仍需另行授权。

## 6. 进度记录

- 2026-08-31：由 TRADING-2549 S3 建立为 `PROPOSED` empirical handoff。未运行实验，未读取新市场数据，
  未调用外部平台，未创建订单或仓位。
- 2026-08-31：Owner 授权继续推进预注册设计。`SINGLE_LANE` START/LANE preflight 从 exact
  `main=ab478cd5fe8dcb811780e91e0b221a914252af71` PASS；形成 strict `1.0.0-draft.1` policy、authority
  byte/semantic replay、non-executable action rejector 和 focused negative/golden tests。草案把同一冻结五态信号
  映射为 fully-funded QQQ/zero-return cash，并以 result-blind exposure-matched static QQQ/cash 作为 primary；
  没有读取 signal payload、市场结果或 option data，没有运行 DQ、confirmation、backtest、QuantConnect/provider，
  orders/fills/positions=`0/0/0`。focused pytest-xdist=`9 passed`；当前转为 `BLOCKED_OWNER_INPUT`，等待 exact
  draft identity 复核，不自动触发经验运行。
- 2026-09-01：Owner 按完整 file SHA
  `507ab3dd3610971c0962fa093cec0c7f09e1b816f694b7dd946c4b9703013dfa` 与 canonical SHA
  `7d12dd62127cb02676d4e18510c06fddc9e2a0afa03ec2f0e758ba6143bed88c` 精确冻结全部所列规则。
  独立 freeze admission 保留原草案 bytes/status，机械重放 signal/candidate/comparator/accounting/cost/
  estimand/threshold/drawdown/reducer surface，并把经验 verdict 保持为 `UNRESOLVED`。本次只限
  non-executable `DATA_RESEARCH`；没有读取/下载市场数据，没有运行 DQ、confirmation/backtest 或调用
  QuantConnect/provider/cache，paper/live/production/broker 与 orders/fills/positions 仍为 false/0。
- 2026-09-01：Owner 另行授权 exact bounded empirical confirmation，只读既有 allowlisted QQQ
  adjusted-close，四项执行上限为 `manifest replay=1 / canonical DQ=1 / local confirmation=1 /
  independent replay=1`；download、cache mutation、QuantConnect、options/provider、paper/live/
  production/broker 与 orders/fills/positions 为零。任务重新进入 `IN_PROGRESS`，先冻结 run
  authorization/manifest 与 fail-closed executor，再消费唯一运行配额。
- 2026-09-01：唯一 bounded dispatch 完成。manifest replay、canonical DQ、local confirmation、
  independent replay 各一次且全部 PASS；frozen reducer 机械输出 `RETAIN`。Candidate 对 exposure-matched
  comparator 的 net total-return difference 为 `+13.745976956735603 percentage points`，max-drawdown
  magnitude delta 为 `-3.4293901783962415 percentage points`。未下载/修改缓存，未调用 QuantConnect、
  option/provider、paper/live/production/broker，orders/fills/positions=`0/0/0`。现有 reader contract
  无法表示 terminal verdict，按 no-silent-workaround 规则先执行最小 serial contract wave。
- 2026-09-01：TRADING-2551 terminal-verdict contract 已以 `main=b704784e...` 发布；旧 lane 到最新
  `main` 的 drift plan `integration-revalidation-ac8786f156b0dffad7ae` 已验证并由 coordinator 精确复核。
  集成只保留 task-owned policy/code/aggregate evidence，`docs/system_flow.md` 手工协调，task/Atlas 与
  architecture/report-flow/compatibility authority 从最终树重建。临时 manifest/plan 仅存放于
  `outputs/architecture/`，closeout 后删除；不重跑 confirmation，也不扩大任何外部或交易权限。
- 2026-09-01：首个集成候选 `296e04d0...` 的 formal Architecture tier 在 882 项中仅发现
  `tests/test_arch_004g_deprecation.py` 仍冻结新增 executor 前的 inventory identity/count；其余 881 项
  PASS。该失败不涉及 empirical result、DQ 或 replay。失败 publication transaction 已留存并释放，
  后续候选只更新确定性 architecture ratchet 并重建关联 authority，不重跑 confirmation。
- 2026-09-01：failure-fix 候选 `afd4b2a4...` 的 Architecture/Contract/Integration/Reproducibility 分别
  `882/278/995/24 PASS`；Full 为 `10084 passed / 3 skipped / 6 failed`。6 项同根因：集成曾直接把
  preregistration SHA 锁定的 `evidence_first_research_portfolio_v1.yaml` 从 `UNRESOLVED` 改为
  `RETAIN`，破坏 immutable authority identity。v4 修复恢复该冻结文件原字节，只允许 Atlas 经
  result-admission、evidence SHA、aggregate counter/DQ/replay/safety 全部校验后构造只读 terminal
  projection；prior failed Full 作为 `failure_fix_rerun` parent 绑定，不重跑 confirmation。
- 2026-09-01：Owner 要求先试行 Web Pro 的 reader-entry 建议；任务为该 reader-projection-only wave
  暂时恢复 `IN_PROGRESS`。实现范围固定为页首 scope 声明、三条独立进展摘要、阶段不可自动晋级提示与
  下一动作授权状态；terminal verdict 仍为 `RETAIN`，七级证据阶梯与 L2 审计链保持不变。本波没有新
  research run、external action 或 production/broker 权限。
- 2026-09-01：真实浏览器展开“研究细节”后发现既有 L1 仍硬编码 `UNRESOLVED`，与 L0 的 terminal
  `RETAIN` 冲突。修复把 L1 当前决定、当前工作、最大阻塞、下一步与禁止推论改为从同一 projected
  evidence-first portfolio 派生；历史期权 baseline 只保留为明确标注的背景，不再冒充当前主线结论。
- 2026-09-01：canonical writer 因已登记但尚未接入 Atlas classification 的 `TRADING-2552` 按设计
  fail closed，且本波 task index 尚未进入 exact commit。为完成可读性试改的浏览器核验，隔离预览复用
  上一次已验证 snapshot/diff payload，只重跑当前 renderer、terminal reader projection 与 accessibility
  validator；它不能证明 latest task coverage、exact-commit freshness 或 page-effectiveness acceptance，
  不得替代正式目录。退出条件是 TRADING-2552 完成分类、最终候选提交后由 canonical writer 重建并验证。
- 2026-09-02：`TRADING-2552` 与 `TRADING-2553` 已先后进入 latest local/origin `main`；reader-entry
  lane 保留在 frozen base `9e2e3f04a0092c5fe1477b88842135aa01834654`，以 governed
  `integration_revalidation_plan.v1` 协调到最新主线。integration worktree 固定为
  `D:\Work\AITradingSystem_trading2550_reader_entry_integration`，owner=`TRADING-2550`，purpose=从
  latest `main` 协调 reader renderer/tests/system-flow/task state、重建 canonical Atlas 与 formal
  authority，并形成唯一发布候选；exit condition=候选已进入 local/origin `main`、正式证据已保全、无进程
  依赖且 tracked/untracked/ignored 审计无唯一内容。删除 allowlist 仅限该精确路径，满足退出条件后使用
  `git worktree remove` 并 `git worktree prune`；tracked bytes 可由 Git 恢复，未保全的 ignored bytes
  清理后不可恢复。临时 manifest/plan 只位于 `outputs/architecture/`，收口后删除。
- 2026-09-02：latest-main reconciliation 保持 frozen signal verdict=`RETAIN` 与全部研究数值不变，只把
  L0/L1 当前进展更新为 TRADING-2552 review complete、TRADING-2553 fixture-only Wave A complete，且
  Wave B fee semantics/exact manifest 与 Wave C single bounded QuantConnect run 均未授权、未开始。该协调
  不读取真实数据，不运行 DQ、confirmation/backtest 或外部平台，不产生 production/broker/trading effect。
- 2026-09-02：final publication v2 在 Atlas exact-source generator 前按计划以 `FAILED` 释放；原因是
  renderer、live projection、tests 与 `docs/system_flow.md` 必须先进入 task-branch exact source commit，不能
  把 dirty bytes 归到旧 `HEAD=d72350d8...`。随后 source-stage v1 仅声明 canonical task-source generator，
  并重放当前 `IN_PROGRESS` projection。该重放复用了先前 change id，append-only registry 因而保留一条
  语义等价的 `IN_PROGRESS -> IN_PROGRESS` 事件；它没有改变 task projection、研究结论或执行权限，后续
  final closeout 使用新的唯一 change id，不删除或改写该审计事件。
