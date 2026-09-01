# TRADING-2552：QQQ Options Conditional Paired Comparison Owner Review V1

最后更新：2026-09-01

- stable task id：`TRADING-2552_QQQ_OPTIONS_CONDITIONAL_PAIRED_COMPARISON_OWNER_REVIEW_V1`
- priority：`P0`
- status：`IN_PROGRESS`
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

本任务只拥有本 supporting requirement 和 canonical task event。它不创建临时 worktree/clone/cache，不修改
`docs/system_flow.md`，因为此次 review 没有改变任何 CLI、data flow、DQ gate、backtest behavior 或 report
consumer。known-unrelated exclusion
`docs/research/growth_tilt_owner_diagnosis_pack.md` 不读取、不 hash、不 diff、不 stage、不修改。

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
