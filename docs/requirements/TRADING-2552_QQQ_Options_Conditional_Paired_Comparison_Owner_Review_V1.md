# TRADING-2552：QQQ Options Conditional Paired Comparison Owner Review V1

最后更新：2026-09-01

- stable task id：`TRADING-2552_QQQ_OPTIONS_CONDITIONAL_PAIRED_COMPARISON_OWNER_REVIEW_V1`
- priority：`P0`
- status：`BASELINE_DONE`
- task class：`NON_EXECUTABLE_OWNER_REVIEW`
- production effect：`none`
- broker action：`none`

## 1. Owner 决定与任务目的

TRADING-2550 已按冻结 preregistration 完成唯一一次 bounded signal-value confirmation，结果为
`RETAIN`，并把唯一下一合法动作机械确定为
`OWNER_REVIEW_FOR_CONDITIONAL_OPTIONS_PAIRED_COMPARISON_ONLY`。Project Owner 随后回复“批准”。本任务
把该回复精确解释为：允许打开并完成 conditional options paired-comparison 的本地、只读、不可执行复核，
形成可审计的现状判断、复用边界、工程缺口和后续授权分层。

本决定不等于批准 exporter、run manifest、real DQ、QuantConnect save/build/backtest/retry、provider、
Object Store、public share、raw option export、paper/live/production/broker，也不允许任何新的
orders/fills/positions。

Owner decision：
`owner_decision:TRADING-2552:2026-09-01:open_conditional_paired_comparison_owner_review_v1`

## 2. 精确继承的权威

### 2.1 已保留的趋势信号价值

- result admission：`frozen_signal_value_confirmation_result_admission_v1@1.0.0`；
- file SHA-256：`38221eb705893b61da27c06ee623d1e237084bfc38222fb60b8a5d69b88d7127`；
- verdict：`RETAIN`；
- window：`2021-02-22..2025-12-02`，XNYS `1202` sessions / `1201` return intervals；
- candidate 对 exposure-matched comparator 的 net-return difference：
  `+13.745976956735603 percentage points`；
- max-drawdown magnitude delta：`-3.4293901783962415 percentage points`；
- 该结果只证明冻结趋势信号值得保留，不能证明期权实现有价值，也不能重设期权参数。

### 2.2 已冻结的 paired comparison 合同

- contract：`qc_qqq_options_paired_comparison_contract_v1@1.0.0-draft.1`；
- contract file/canonical SHA-256：
  `8c748634f6869eb4d4e9dfb14493acd072d146074ce7e86462eec0adae15714a` /
  `6f77cf17af6e435799a2e86e1fb6a81936368e053b2367efb3a8e2be13412267`；
- freeze-admission file SHA-256：
  `fbedb47e5f2a748dc75669faabee9641ba7e0596de4ad8c340ed7ebcbd4c5c76`；
- primary comparator：`SAME_SIGNAL_FULLY_FUNDED_QQQ_CASH_ACCOUNT`；
- primary view：`COMMON_CAPITAL_ACCOUNT_VIEW`，两边 initial capital 均为 `USD 100,000`；
- primary estimand：
  `optionized_net_return - underlying_implementation_net_return`；
- secondary view：`CAPITAL_AT_RISK_TIME_VIEW`；
- diagnostics：`SGOV_CARRY_COMPARATOR` 与 `QQQ_BUY_AND_HOLD`；
- 16-axis falsification、2021..2025 calendar partitions、export-safe fields 与全部 safety 关闭项保持
  exact-frozen，不在本任务修改。

### 2.3 可复用的既有 option implementation

- 唯一方向源仍为 `first_layer_composer_v2:trend_state`；
- mapping 仍为 `risk_on/constructive -> LONG_CALL`、其余三态 `-> FLAT`；
- `LONG_PUT` 不进入 baseline；
- TRADING-2542I 的 37-slot selection/execution/accounting/lifecycle policy 保持 immutable；
- exact signal package 仍为 `1202/1202` sessions、`83` transitions；
- 既有 backtest `f2879a3cee7ec4e0b68b4f943aafd1f8` 只保留为
  `CAPABILITY_AND_DIAGNOSTIC_EVIDENCE_ONLY`，不能选择 comparator、normalization、window 或参数。

## 3. 复核结论

### 3.1 不需要重新设计趋势预测链

paired comparison 使用 TRADING-2550 已保留的 exact direction signal；期权数据只在 QuantConnect 内用于
选约、成交模拟、持仓生命周期与收益计量，不回流趋势 feature、label、state 或 mapping。因此当前没有
“根据期权重新适配趋势信息源”的数据缺口，也不需要新增 FMP、Cboe、Fed、BLS 或 BEA 输入。

### 3.2 旧 QC 运行不能回答新问题

TRADING-2542I 的 QC `main.py` 已实现 optionized account，但其
`UNDERLYING_IMPLEMENTATION` 仍是
`NORMALIZED_ONE_SHARE_QQQ_QUOTE_LEDGER / NONE_NORMALIZED_RETURN_ONLY`：

- 只复合一股 QQQ 的 quote return，没有 USD 100,000 cash/share ledger；
- 没有完整 comparator cash、shares、P&L、fee、peak equity、drawdown 与 chronology reconciliation；
- terminal aggregate 缺少 TRADING-2548 冻结的大部分 identity/event/account/risk/comparator fields；
- 无法形成 common-capital primary estimand，也无法对 16 个 falsification axes 作完整结论。

因此既有 `4.48%` option net profit 和旧 `underlying_comparator_return` 不能补算、重解释或升级为 paired
comparison。正确路径是修改未来 successor QC research code，在同一次平台 run 内维护 fully-funded virtual
QQQ/cash ledger；本地仍不对期权重新定价。

### 3.3 当前阻塞类型

当前 blocker 是 `ENGINEERING_AND_EXACT_RUN_AUTHORITY_GAP`，不是市场数据缺口：

1. successor QC code 尚未实现 frozen fully-funded comparator ledger；
2. export-safe terminal aggregate 尚未覆盖 frozen mandatory field inventory；
3. local result admission 与 independent aggregate replay 尚未实现；
4. exact project/code/package/action-maxima manifest 尚未生成；
5. 没有任何新的 QuantConnect save/build/backtest/retry authority。

## 4. 建议的后续分层

### Wave A：non-executable implementation contract

在后续独立授权后，允许实现并测试：

- QC code generator/adapter：复用 37-slot option side，替换旧 one-share comparator 为 fully-funded
  virtual QQQ/cash account；
- export-safe aggregate collector：只输出冻结字段和 counts，不输出 raw option rows、完整 chain、SID、
  contract quote history 或本地 repricing input；
- strict local admission/reducer：机械执行 16-axis `INVALID > FAIL > INSUFFICIENT > PASS`；
- synthetic/golden/negative tests；只使用 fixture，不访问真实市场结果或 QuantConnect。

Wave A 不生成真实 run manifest，不运行 DQ/backtest，不修改 frozen signal、mapping、37-slot policy、
comparator contract 或 threshold。

### Wave B：exact bounded-run manifest

只有 Wave A final-tree validation PASS 后，才另行请求 Owner 批准生成 exact manifest。manifest 必须冻结：

- existing research clone 或另一明确 project target；
- exact repository commit、QC `main.py` LF SHA、signal package 与 contract identities；
- requested/evaluated `2021-02-22..2025-12-02`、XNYS `1202` sessions；
- save/build/backtest/retry maxima、runtime/data-point ceilings 与平台自动 build 的计数语义；
- aggregate-only export、zero provider/Object Store/public share 与 QC 外 zero orders/fills/positions；
- dispatch 前 manifest replay 和 terminal stop condition。

### Wave C：单次 QuantConnect DATA_RESEARCH paired backtest

必须再取得与 exact manifest 绑定的独立授权后才可 dispatch。有效结果不因 underperformance 自动重试或改参；
platform identity、mandatory field、event/accounting/risk reconciliation 或 export safety 任一不足时按冻结
precedence 输出 `INSUFFICIENT`/`INVALID`，不得本地替代。

## 5. 本任务验收标准

1. 明确记录 Owner 只批准打开 review，而非批准 successor implementation 或 external run；
2. exact-bind TRADING-2550 `RETAIN` admission 与 TRADING-2548 frozen contract；
3. 证明现有趋势数据和 1202-session signal package 可继续复用；
4. 精确指出旧 one-share comparator 与 fully-funded primary estimand 的语义差异；
5. 把后续工作拆成 implementation、manifest、single run 三个独立授权波；
6. 不修改 frozen signal、37-slot policy、paired contract 或 historical aggregate；
7. data read/download/cache mutation、DQ、QuantConnect、provider、raw option export、paper/live/
   production/broker 与 orders/fills/positions 均为 `0/false/none`。

## 6. Path ownership 与生命周期

本任务原始 review bytes 只拥有本 supporting requirement 和 canonical task event；review 不改变任何 CLI、
data flow、DQ gate 或 backtest behavior。正式 Full 验证发现新增 task 必须进入 Atlas successor policy，否则
current-state projection 会按设计 fail closed。因此 failure-fix 额外拥有 `config/atlas/live_snapshot.yaml`、
`config/atlas/page_effectiveness.yaml` 与对应 `tests/atlas/` 断言，并按 report-output 维护规则同步
`docs/system_flow.md`。这只是 read-only report projection 分类，不改变策略、数据、DQ 或交易链。known-unrelated exclusion
`docs/research/growth_tilt_owner_diagnosis_pack.md` 不读取、不 hash、不 diff、不 stage、不修改。

正式验证期间共享主 checkout 被另一任务切换分支。为避免干扰该任务，failure-fix 收口使用独立临时 Git
worktree `D:\Work\AITradingSystem_trading2552_owner_review`，owner=`TRADING-2552`，purpose=重建
architecture manifest/compatibility authority、运行 final validation 并形成唯一候选。exit condition=候选已
fast-forward 到 local/origin `main`、canonical evidence 已保全、无进程依赖且 tracked/untracked/ignored 审计
无唯一内容；满足后用 `git worktree remove` 清理并 `git worktree prune`。删除 allowlist 仅限该精确路径，
合入前可通过 Git 历史恢复，清理后临时未提交内容不可恢复。

## 7. 当前 terminal

- review decision：`READY_FOR_SEPARATELY_AUTHORIZED_NON_EXECUTABLE_IMPLEMENTATION_WAVE`；
- current paired outcome：`INSUFFICIENT_PLATFORM_EVIDENCE`；
- next owner：Project Owner 可另行授权 Wave A；
- successor task implicitly authorized：`false`；
- exporter/manifest/DQ/QuantConnect/backtest authorized：`false`；
- production effect：`none`；
- broker action：`none`；
- terminal：`OWNER_REVIEW_COMPLETE_SEPARATE_WAVE_A_AUTHORITY_REQUIRED`。

## 8. 进度记录

- 2026-09-01：Owner 批准打开 conditional paired-comparison review。READ_ONLY governed preflight=
  `PASS`，local/main/origin main=`942b3e61d70f72251d23ffa79553e389897c2c0e`，active lease=`0`，
  governed worktree audit=`PASS`；本轮只登记 review，不访问 QuantConnect 或市场数据。
- 2026-09-01：复核已完成。TRADING-2550 result admission、TRADING-2548 contract/freeze-admission 与
  TRADING-2542I execution policy 的精确身份均保持不变；聚焦 pytest-xdist=`76 passed`。结论为
  `READY_FOR_SEPARATELY_AUTHORIZED_NON_EXECUTABLE_IMPLEMENTATION_WAVE`：现有趋势信号与数据足够，
  当前仅缺 fully-funded comparator、aggregate exporter、local admission/replay、exact manifest 和独立
  QuantConnect run authority。task-owned review bytes 已完成，等待 canonical task 状态与 formal
  publication 收口；external/data/trading counters 继续为零。
- 2026-09-01：parent Full `full_20260901T023827Z` 得到 `10062 passed / 27 failed / 6 skipped`。
  其中 14 项来自隔离 worktree 未携带既有 ignored retained receipts/signal-package evidence，13 项来自
  TRADING-2552 尚未在 Atlas successor policy 分类；两类均不涉及研究数值或外部运行失败。failure-fix
  将精确复制既有留存 evidence、把本 review 投影为 `OWNER_REVIEW_COMPLETE_SEPARATE_WAVE_A_AUTHORITY_REQUIRED`，
  重建 Atlas/report-flow/architecture/compatibility authorities，并以该 Full 作为 immutable parent 执行
  唯一 `failure_fix_rerun`。QuantConnect、市场数据、provider、DQ、backtest 与交易 counters 继续为零。
- 2026-09-01：Atlas successor 分类聚焦回归=`47 passed / 1 skipped`，既有 O1 DQ receipt 与 2542I exact
  package 环境回归=`19 passed`；canonical Atlas writer 在 exact commit `a43b7c731…` 上重建为
  `CURRENT`，coverage=`82`、validation=`PASS`。随后 report-flow builder 正确以
  `RCF_SOURCE_SEAL_DRIFT` 拒绝旧 `docs/system_flow.md` seal：reviewed 新 identity 为
  byte_count=`2328913`、file/LF SHA-256=
  `f777cef82e7deede672c81fa33402ca2c443e67dd6c2d0a4d988642dff0342a5`、git blob=
  `25365d7f36088d46a818bbb4961a2da3f08bf161`、blank-line block count=`1178`。该 gate 不允许 builder
  自动接纳文档漂移；后继 transaction 将显式声明 report-flow policy/test 路径、更新这组 reviewed seal
  与相邻 total-entry assertions，再完整重建 authority。此修复不改变研究结论或任何外部权限。
- 2026-09-01：report-flow recovery source commit `ed298705f…` 已精确接纳上述 seal；report-flow build=
  `PASS`（3111 entries / 192 fragments），compatibility build=`PASS`，Atlas exact writer 也在该 commit 上
  `PASS`。v6 因预声明路径漏列相邻 compatibility assertion 而在 mutation 前释放；v7 的 generator 本身
  全部成功，但 `GENERATED_REBUILD_POST` 正确拒绝 acquire 后新增的 source commit（声明 lane head 仍为
  `d36b4b372…`）。两次均无 candidate/formal/main/push/外部动作；后继 transaction 从包含 source seal、
  assertions 与全部 generated bytes 的新 exact HEAD 重放，不修改旧 transaction 或放宽 lane identity。
