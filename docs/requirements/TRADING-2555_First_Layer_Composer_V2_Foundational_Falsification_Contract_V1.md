# TRADING-2555：First-Layer Composer V2 基础有效性证伪合同 V1

最后更新：2026-09-03

稳定任务 ID：`TRADING-2555_FIRST_LAYER_COMPOSER_V2_FOUNDATIONAL_FALSIFICATION_CONTRACT_V1`

状态：`IN_PROGRESS`（F0 实现已完成，登记/Atlas failure-fix 与正式发布尚未收口）

Owner 指令：2026-09-03，参考 Web Pro 对 frozen signal-value confirmation 的复核建议，继续推进基础有效性分析与研究。

## 1. 问题与目标

TRADING-2550 已在 exact `2021-02-22..2025-12-02`、1202 个 XNYS session 的冻结窗口内，证明二元映射
`constructive/risk_on -> QQQ`、其余状态 `-> zero-return cash` 相对静态 exposure-matched QQQ/cash comparator 的
净收益差为 `+13.745976956735603` 个百分点，并通过 DQ、manifest replay 与 independent replay。该结论足以保留
`NARROW_SIGNAL_VALUE_RETAINED`，但不能证明五状态模型、期权实施、生产可用性或未来可迁移 alpha。

本任务是 result-blind serial contract wave，只冻结后继基础证伪研究必须使用的输入身份、统计口径、诊断轴、状态
reducer、授权边界和停止条件。除上述已知冻结 aggregate 外，本任务不得读取、计算或选择任何新 empirical
diagnostic 结果。后继实证任务必须从本合同 ordinary-pushed exact main 建立，不得回写或调参本合同。

## 2. 已知信息与非盲边界

- 已知且不可作为新选择依据：TRADING-2550 的 RETAIN、收益/回撤/成本 aggregate、年度 LONG_CALL 占比和 universal
  first-layer route 的历史 `DEFENSIVE_REGRESSION_DUE_TO_FALSE_ADD_RISK`；
- 本任务仍保持结果盲：尚未计算的年度收益归因、连续 episode 归因、leave-one-calendar-year-out、moving-block
  bootstrap、成本/SGOV carry sensitivity、state-transition attribution；
- 任何诊断失败都不得触发参数搜索、样本边界移动、比较器替换、特征增删、权重/阈值修改或“补救”回测；
- 当前结论边界固定为 `NARROW_SIGNAL_VALUE_RETAINED` 与 `UNIVERSAL_DEFENSIVE_USE_REJECTED` 并存。

## 3. 冻结研究对象

### 3.1 Primary identity

- primary requested/evaluated window：`2021-02-22..2025-12-02`；
- calendar：XNYS；expected sessions：1202；expected return intervals：1201；
- signal package、feature policy、threshold policy、composer、one-session decision lag 和成熟 label 规则必须 exact-bind
  TRADING-2550 immutable manifest；
- candidate：`constructive/risk_on -> 100% QQQ`，其他状态 `-> 100% cash`；禁止杠杆、做空、期权、盘中成交；
- primary cash return：0；primary transaction cost：每次单向 traded notional `5 bps`；
- comparator：与 candidate 冻结 LONG_CALL session fraction 完全相同的静态 QQQ/cash exposure-matched comparator；
- adjusted-close source bytes、DQ report、provider/download/checksum、runtime Git identity、manifest identity 和 replay
  identity 缺一项即不可进入实证计算。

### 3.2 Diagnostics only

- transaction-cost sensitivity：单向 `10/15/20 bps`；`5 bps` 始终是唯一 primary；
- cash-carry sensitivity：FLAT 日使用 exact SGOV adjusted-close return；只作 diagnostic，不替换 zero-return primary；
- 所有 sensitivity 必须重算 candidate 与 comparator，不得只惩罚其中一方；
- 不允许 2021-02-22 之前的数据进入 primary、bootstrap 或敏感性结论。

## 4. 冻结诊断轴

后继实证包必须同时产出以下轴，缺失任一项时 reducer 至少为 `INSUFFICIENT`：

1. `POLICY_CONSUMPTION_INVENTORY`：逐项列出声明字段是否被 runtime 读取；特别核验
   `negative_score_quantile`、`min_predicted_share`、`max_predicted_share`、`missed_upside_penalty`、
   `net_of_cost_penalty` 与 `score_weights.tqqq_penalty`。只报告 drift，不在旧模型中补接字段；
2. `CALENDAR_YEAR_ATTRIBUTION`：2021 partial、2022、2023、2024、2025 partial 的 candidate/comparator return、paired
   excess、MDD、turnover、cost、LONG_CALL fraction；明确 partial-year 标记；
3. `CONTIGUOUS_EPISODE_ATTRIBUTION`：按冻结二元持仓的连续 LONG_CALL/FLAT run 切分，保存起止 session、session
   count、gross/net candidate contribution、comparator contribution、paired excess、transition-in/out state；
4. `LEAVE_ONE_CALENDAR_YEAR_OUT`：依次删除左端 session 所属的一个 calendar-year interval，按原时间顺序拼接其余
   paired return 序列并重算 candidate/comparator compounded return、paired excess 与 MDD；不重训、不重估阈值；
5. `PAIRED_MOVING_BLOCK_BOOTSTRAP`：对同一 session 的 candidate-minus-comparator daily net-return pair 做 circular
   moving-block bootstrap；block length 固定为 `21` 和 `63` sessions，seed=`2555`，每个 block length
   `10000` 次；报告 compounded paired-excess 的 2.5%、50%、97.5% percentile interval 以及 `P(excess<=0)`；
6. `COST_SENSITIVITY`：5/10/15/20 bps 下的双方净结果和 paired excess；另报告基于完整重算得到的离散
   break-even bracket，不以一阶近似替代；
7. `SGOV_CARRY_SENSITIVITY`：primary 信号/比较器 exposure 不变，仅把各自 FLAT interval 按 SGOV exact return
   计息，报告相对 zero-return primary 的差异；
8. `STATE_TRANSITION_ATTRIBUTION`：冻结五状态及二元 exposure 的状态计数、转移计数、转移后 1/5/20-session
   QQQ return（尾部 horizon 不成熟则显式 missing），不得把 non-probabilistic confidence 当概率；
9. `SELECTION_HISTORY_INVENTORY`：列出 first-layer v1/v2、universal rejection、narrow TRADING-2550 confirmation 与
   本任务之间的已知选择路径，结论必须标记 `REUSED_DEVELOPMENT_CONFIRMATION`，不得称 pristine OOS；
10. `SOURCE_REVISION_DIFF`：若当前 adjusted-close bytes 与 TRADING-2550 sealed bytes 不同，必须同时报告旧/新 hash
    和信号/收益/state-transition diff；不能静默使用修订数据覆盖冻结复现。

`21/63` block length 对应约一个交易月/季度，`10000` 次为本 V1 的预注册诊断预算；它们不是可搜索参数。任何替代
bootstrap、seed、replicate count 或 interval 定义必须另建 V2 合同，并说明原因，不能在看到结果后修改。

## 5. Reducer 与结论边界

precedence 固定为 `INVALID > FAIL > INSUFFICIENT > PASS`：

- `INVALID`：DQ/PIT、session/range、hash、manifest、runtime identity、one-session lag、comparator identity、replay、
  schema 或独立复算任一失败；
- `FAIL`：有效完整 primary 5 bps evidence 的 paired excess `<= 0`，或 21/63 两个 bootstrap 中任一 97.5% 上界
  `<= 0`；这触发停止基础信号价值推进，不得调参救援；
- `INSUFFICIENT`：evidence 有效但任一必需诊断轴不完整；或 primary paired excess `> 0` 但任一 bootstrap 2.5%
  下界 `<= 0`；或任一 leave-one-calendar-year-out paired excess `<= 0`；
- `PASS`：所有必需轴完整有效、primary paired excess `> 0`、两个 bootstrap 的 2.5% 下界均 `> 0`，且五个
  leave-one-calendar-year-out paired excess 均 `> 0`。

episode concentration、年度集中度、成本 break-even 和 SGOV carry 只作强度/脆弱性解释，不额外设置见结果后阈值。
`PASS` 也只支持 `FOUNDATIONAL_NARROW_SIGNAL_VALUE_SUPPORTED`，不授权五状态模型、期权、paper/live、production 或
broker。`INSUFFICIENT` 不等于失败，但保持 Wave B/C HOLD。

## 6. 分阶段工作与后继顺序

### F0：本任务合同波

- 建立 versioned preregistration policy、strict loader/schema 和 deterministic contract tests；
- 静态完成 policy-consumption inventory 的“声明字段/预期消费者”合同，不读取 empirical outcome；
- 冻结 output schema、aggregate reducer、hash/manifest identity 和 fixture-only negative tests；
- 不运行 DQ、市场数据读取、回测、bootstrap 或任何外部平台动作。

### F1：独立后继基础证伪

- 仅在 F0 ordinary-pushed exact main 后分配新的 canonical task ID；
- 先运行 `aits validate-data`/同一路径 DQ 并 exact replay TRADING-2550 manifest；
- 在本地 bounded research sandbox 一次性生成全部冻结轴、aggregate-only conclusion 和 independent replay；
- 禁止参数、阈值、模型、比较器、窗口或 reducer 修改。

### F2：Prospective OOS lane

- 与 F1 分离登记；从合同 freeze 后第一个 XNYS session 起 append-only；
- 保存 feature/signal/fit hashes 和 next-session decision；20-session label 仅在成熟后加入；禁止 backfill；
- 不授权 paper trading、broker、orders、fills 或 positions。

### Options gate

- F1 未得到 `PASS` 前，TRADING-2553 后的 Wave B fee semantics/package/manifest 保持 HOLD；
- 本任务和 F1 均不授权 Wave C QuantConnect run；前 90 天默认 `WAVE_C_NOT_AUTHORIZED`。

## 7. 验收标准

- canonical task row 与本 requirement exact-bind；
- result-blind V1 policy 明确 primary/sensitivity、10 个诊断轴、reducer 和 stop condition；
- schema/loader 拒绝缺失轴、额外字段、错误窗口/session、错误 block/seed/replicate、错误 cost grid、hash 漂移和
  conclusion 越权；
- fixtures 证明 precedence `INVALID > FAIL > INSUFFICIENT > PASS`，并证明 PASS 不会授权 options/production；
- policy-consumption inventory 能区分 `DECLARED_AND_CONSUMED`、`DECLARED_NOT_CONSUMED`、`CODE_ONLY`、
  `NOT_APPLICABLE`，不改变既有 runtime；
- focused parallel pytest、适用 architecture/contract validation、governed audit 与 ordinary publication PASS；
- `docs/system_flow.md` 仅当实际新增 CLI、业务 artifact flow 或 consumer-visible runtime path 时更新；本 F0
  contract-only wave 预期 `system_flow_change=none`。

## 8. 权限与生命周期

- governed mode：`SINGLE_LANE`；branch：`codex/trading-2555-foundational-falsification-contract`；
- 复用当前 repository checkout，不创建额外 worktree；因此无新增 temporary workspace；
- external action、market-data read、DQ run、backtest、bootstrap、QuantConnect/provider/cache mutation、orders、fills、
  positions、paper/live、production、broker 均为 `none/false/0`；
- 本任务只允许 tracked contract/loader/tests/docs/task authority 变更；默认 ordinary push 仍须通过最终 publication
  fence、formal validation、local-main fast-forward 和 remote ancestry 门禁。

## 9. 进度记录

- 2026-09-03：完成 result-blind V1 policy、strict loader、authority byte/semantic replay、静态
  policy-consumption inventory、四态 reducer 和 synthetic/negative contract tests；未读取新的 empirical
  diagnostic 结果，未运行 DQ、市场数据、回测、bootstrap 或外部平台动作。
- V1 policy file SHA-256=`54dc349be1ec5670f9e02fc74e9467b668b2311a7dadbdc22680c8c605a824ad`；canonical
  SHA-256=`ea6b51baf7d8bdfec2454fb037131a199736e6cacb1eecdc35e01701f5357818`；authority-set
  SHA-256=`a07e63c9f3ba035d94cfdbf18bc096b69380e4baf1b003540390b66d4ec44fe3`。
- focused parallel pytest：本任务与 TRADING-2550 adjacent contract/execution 共 `64 passed`；Ruff、strict mypy、
  compileall 与 canonical task-source validation PASS。最终 formal validation 和 ordinary publication 仍是收口门禁。
- 第一轮 Architecture=`877 passed / 6 failed`；失败 artifact 保留在
  `outputs/validation_runtime/architecture-fitness_20260902T180417Z/test_runtime_summary.json`（SHA-256
  `92412ea4637d11969208b79dd48035f20e5ba27892545885e842607b534ec067`）。六项均来自 task count 1051→1052、
  DevEx manifests 与 compatibility current-hash authority 尚未刷新；没有信号/统计合同测试失败，不得把本轮记为
  PASS。
- formal failure 后 local `main` 被并行 TRADING-2554 从 `12f1e645...` 推进到 `accae71a...`。保留冻结 lane，不
  rebase/重建；base-drift manifest/plan 固定保存在
  `outputs/architecture/trading_2555_foundational_falsification_contract/`，在 latest-main candidate 合入后完成
  failure-fix。该目录保留到 ordinary publication 与 closeout receipt 均 PASS，之后仅保留可审计 plan/receipt，
  不保留临时 worktree 或 clone。
- reviewed reconciliation plan=`integration-revalidation-8139d0e83333bcf3692f`；only overlap 为
  `docs/task_register.md` 的 coordinator reconciliation 与 `arch_005_task_registry_index.yaml` 的 coordinator
  refresh。latest-main candidate 已用 canonical writer 合并 TRADING-2554/2555，task count=`1053`，官方 DevEx 与
  compatibility generators 均 validate PASS；第一轮六项失败加本任务/2550 adjacent 的 focused replacement=
  `92 passed`。
- 登记前置候选 `c538ca0f79815ca1b76aa090d597c6b5a559cd74` 的 Architecture=`883 passed`、
  Contract=`278 passed`、Integration=`995 passed`、Reproducibility=`24 passed`；首次 Full=
  `10119 passed / 14 failed / 3 skipped`。失败 artifact 为
  `outputs/validation_runtime/full_20260902T185116Z/test_runtime_summary.json`（SHA-256
  `9bf085614ab876202974a3da99172027b63cf3c4604c77c730d1c0f4070bd6e1`）；14 项全部由新增
  TRADING-2555 未进入 Atlas successor classification 派生。按 fail-closed 原因修复 Atlas task coverage、
  current mainline 与 system-flow 投影后，必须绑定该 failed parent 执行 `failure_fix_rerun`，不得把首次
  Full 记为 PASS。
- Atlas failure-fix 后的第二次 Full=`10130 passed / 3 failed / 3 skipped`；artifact 为
  `outputs/validation_runtime/full_20260902T201401Z/test_runtime_summary.json`（SHA-256
  `28726d6e1ab38f5963639a1f41a9a096bd06ecfa9247f495733ac81c74ef0a57`）。Atlas 失败已清零，剩余三项
  全部是 `docs/system_flow.md` 更新后 DEVX-006D report-flow source seal、lossless shadow 与固定测试哈希未刷新；
  下一次 `failure_fix_rerun` 必须绑定该第二次失败 artifact，并运行官方 `report-flow-authority` generator。
