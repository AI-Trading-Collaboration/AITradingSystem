# TRADING-2542H：QQQ Options Frozen-Signal Implementation Retest Contract V1

最后更新：2026-08-28

稳定任务 ID：
`TRADING-2542H_QQQ_OPTIONS_FROZEN_SIGNAL_IMPLEMENTATION_RETEST_CONTRACT_V1`

优先级：`P0`

状态：`IN_PROGRESS`

Owner 决策：
`owner_decision:TRADING-2542H:2026-08-28:adopt_quantconnect_frozen_signal_implementation_retest_path_v1`

production effect：`none`

broker action：`none`

## 1. 问题与纠偏目标

项目接入 QQQ options 的原始目标，是把既有趋势/配置决策作为唯一方向信号，在 QuantConnect
内部选择期权合约、模拟成交和生命周期，并比较同一信号的 underlying implementation 与
optionized implementation。它不是再生产一条由 option activity、VIX、宏观日历或供应商数据驱动的
趋势判断链路。

TRADING-2542E/F/G 的 bytes 与历史结论保持 immutable，但其 mandatory veto source 路径不得再作为
baseline options retest 的前置条件：

- TRADING-2542E 保留为既有 growth action-value exact-freeze 与零订单 review 历史证据；
- TRADING-2542F 保留为 result-blind layered architecture 历史证据；
- TRADING-2542G 保留为 optional overlay/source-readiness 研究，不再阻塞 baseline retest；
- FMP、Cboe VIX、Fed、BLS、BEA 仅可作为后续独立、result-blind optional overlay，缺失时不得改变
  baseline frozen signal，也不得使 baseline run 伪装为 DQ FAIL。

本任务建立最小 serial consumer-contract wave，明确 local/QC 的职责、baseline required 与 optional
overlay 的分层、信号映射待决项、严格停止状态和后续外部运行授权边界。本波不运行真实 DQ/backtest，
不访问 QuantConnect，不读取或导出 raw option rows。

## 2. 冻结继承与已证明事实

### 2.1 继承 authority

- TRADING-2478：既有完成日信号 -> QQQ option universe -> deterministic selection -> execution/fill ->
  lifecycle -> result evidence 的原始目标架构；
- TRADING-2483：immutable daily signal/run package、`2021-02-22` primary start、lag=1、DQ/PIT 与
  raw-options-export prohibition；
- TRADING-2485..2488：deterministic selector、execution reality、cash accounting 与 lifecycle mechanics；
  这些 mechanics 可复用，但当前 numeric/rank policy 仍是 `OWNER_REVIEW_REQUIRED_BASELINE`；
- TRADING-2499：DAILY primary backtest contract、strict chronology 与 cash-preservation blocker；
- TRADING-2502/2509：selection/execution/accounting/lifecycle decision inventory，不把未签署 slot
  伪装成 reviewed policy；
- TRADING-2541 exact-date recovery V3 export-safe terminal evidence：目标窗口 1202/1202 sessions，
  `chain_presence_status=PASS_WITH_EXACT_DATE_PROVIDER_HISTORY_RECOVERY`、
  `data_quality_status=PASS_FOR_RESEARCH_TRANSPORT_COMPLETENESS`、
  `point_in_time_status=PASS_FOR_EXACT_SOURCE_AND_AVAILABILITY_DATE`、unresolved=0。该证据只证明
  QuantConnect 数据可用性，不证明策略收益、选约、成交或生命周期正确。

### 2.2 不可冒充的事实

- 当前仓库没有可直接发往 QC 的 retained exact frozen signal package；
- 2483 仍固定 `etf_signal_mapping_status=UNKNOWN_REQUIRES_OWNER_REVIEW`、
  `etf_signal_mapping_allowed=false`；
- DTE、moneyness/delta、quote freshness、spread、OI、volume、rank、allocation、fee、slippage、
  lifecycle 和 result acceptance 尚无完整 executable Owner authority；
- 因此本波可以冻结结构和 blocker taxonomy，但不得把实际 backtest 标为 ready 或自行选择数值。

## 3. 目标数据流与职责边界

```text
existing governed strategy facts
  -> exact frozen signal artifact + identity
  -> reviewed direction/action mapping
  -> immutable QC run manifest
  -> QuantConnect option chain + result-blind deterministic selector
  -> QuantConnect execution/fill + fee/slippage model
  -> QuantConnect lifecycle/mark + portfolio/P&L
  -> export-safe run/result evidence
  -> local exact replay, DQ/PIT admission and paired comparator review
```

Local authority：

- 生成并冻结 existing strategy signal package、source/cutoff/session/code/policy identity；
- 冻结方向映射、selector/execution/accounting/lifecycle policy 与 run maxima；
- 重放 manifest 和 export-safe result evidence；
- 验证同一 signal identity、requested/evaluated range、DQ/PIT、event counts、equity/return/drawdown、
  terminal status 与 paired comparator；
- 不下载 raw options payload，不在本地重新定价期权，不用本地合成 P&L 取代 QC ledger。

QuantConnect authority：

- 提供 QQQ option chain、合约标识与 selection-time observations；
- 按 reviewed deterministic policy 选券；
- 执行 quote/fill、fee/slippage、position/lifecycle/mark、cash/equity/P&L；
- 输出可回流的 export-safe aggregate 与 identity evidence；
- 不生成新的方向 alpha，不调用 broker，不产生 paper/live/production action。

## 4. Baseline 与 optional overlay 分层

### 4.1 Baseline required

基线只允许下列依赖：

1. exact frozen strategy signal identity；
2. reviewed signal-to-`LONG_CALL/LONG_PUT/FLAT` mapping；
3. QuantConnect QQQ option chain 与 field/source identity；
4. deterministic, result-blind selection policy；
5. execution/fill/fee/slippage policy；
6. lifecycle/mark/accounting policy；
7. exact run identity、DQ/PIT、requested/evaluated range 与 export-safe result contract；
8. same-signal underlying comparator 与 optionized implementation comparator。

Option IV/Greeks/OI/volume/quote 只能用于 reviewed eligibility、rank、execution 或 risk control，不能修改
direction signal。缺失不得按 false、zero、market-clear 或跨日 fallback 处理。

### 4.2 Optional overlay

FMP SPY/QQQ、Cboe VIX、Fed/BLS/BEA schedule 与 TRADING-2542G source artifacts 统一标为
`OPTIONAL_RESULT_BLIND_RISK_OVERLAY`：

- 不属于 baseline required；
- 不能成为 frozen direction signal 的 producer；
- 不能改变 baseline run 的可运行性或 baseline DQ terminal；
- 后续如启用，必须独立版本、独立 manifest、独立结果 lane，并与 baseline 做同 signal 的配对比较；
- 不得用 overlay result 反向调参 baseline。

## 5. Signal mapping 与策略参数门禁

本任务只冻结 mapping contract 的完整字段，不代替 Owner 选择具体映射。后继 exact-freeze 至少必须绑定：

- exact source signal artifact/path/file SHA/canonical SHA/repository SHA；
- source signal enum 与每个值到 `LONG_CALL/LONG_PUT/FLAT` 的一一映射；
- defensive/SGOV/neutral 状态究竟映射 `FLAT` 还是 `LONG_PUT`；不得默认；
- effective session、lag 与 calendar；
- unknown/missing source signal 的 typed terminal；
- mapping 不得引用 option chain、option returns、selected contract 或 result bucket。

selection/execution/accounting/lifecycle 至少必须完成 TRADING-2509 slot inventory 中与本 baseline 相关的
全部 exact decision。任何 numeric/rank/default engine value 未冻结时，terminal 固定为
`OWNER_EXACT_POLICY_FREEZE_REQUIRED_NO_BACKTEST`，而不是采用 QuantConnect 默认值。

## 6. Paired comparator 与结果证据

每次后续 run 必须复用同一 frozen signal identity，形成：

- `UNDERLYING_IMPLEMENTATION`：既有 QQQ/SGOV 或 reviewed underlying comparator；
- `OPTIONIZED_IMPLEMENTATION`：QQQ single-underlying、long-premium、single-leg implementation；
- 可选 `OPTIONAL_OVERLAY_IMPLEMENTATION`：只有独立授权和 manifest 时存在。

export-safe result 至少包含：run/project/backtest id、repository/code/policy/manifest SHA、LEAN version、
requested/evaluated dates、session inventory、DQ/PIT、signal/mapping identity、selection/no-contract、
intent/submit/fill/reject/cancel、lifecycle disposition、fee/slippage、cash/equity/return/drawdown、terminal 与
comparator id。raw option rows、完整 chain、contract-level quote history 不回流本仓库。

本地 validator 只做 identity、chronology、count、aggregate consistency 与 result admission；没有 raw ledger
或平台证据时必须 `INSUFFICIENT_PLATFORM_EVIDENCE`，不得自行重算出替代收益。

## 7. Typed stop taxonomy

- `MISSING_FROZEN_SIGNAL_IDENTITY`；
- `UNREVIEWED_SIGNAL_MAPPING`；
- `OPTION_ALPHA_LEAKAGE`；
- `UNFROZEN_SELECTION_POLICY`；
- `UNFROZEN_EXECUTION_POLICY`；
- `UNFROZEN_ACCOUNTING_POLICY`；
- `UNFROZEN_LIFECYCLE_POLICY`；
- `FIELD_SOURCE_UNPROVEN`；
- `MULTIPLIER_UNPROVEN`；
- `MARK_POLICY_UNSPECIFIED`；
- `RUN_IDENTITY_UNSPECIFIED`；
- `MANIFEST_REPLAY_NOT_PASS`；
- `INSUFFICIENT_PLATFORM_EVIDENCE`；
- `IMMUTABLE_LINEAGE_MUTATION`；
- `EXTERNAL_RUN_NOT_AUTHORIZED`。

precedence 固定为 `INVALID > FAIL > INSUFFICIENT > PASS`；missing/unknown/not-evaluated 永不升级为 PASS。

## 8. 分阶段实施与验收

### S0：scope correction contract

- 登记 TRADING-2542H 与本 requirement；
- 新增 strict non-executable policy/loader/tests；
- 绑定 2478/2483/2499、2541 V3 evidence 与 2542E/F/G immutable lineage；
- baseline/optional-overlay 分层与 typed blockers 可机械重放；
- 更新 `docs/system_flow.md`；
- 不冻结方向映射数值或任何 investment heuristic。

### S1：Owner exact policy freeze（后续独立 serial wave）

- 绑定 exact signal package 和 mapping；
- 完成 relevant TRADING-2509 slots；
- 形成 executable-research-only manifest draft；
- 在所有 exact policy 通过前保持 zero run/order/fill/position。

### S2：bounded QuantConnect research backtest（需单独授权）

- manifest replay PASS 后才可在现有 research clone 发起一次有上限的 backtest；
- QuantConnect 内计算期权收益，本地只接纳 export-safe evidence；
- `paper/live/broker/production=0`；
- 任何 project mutation、run maxima、retry 与证据回流范围必须写入独立授权 manifest。

### S3：paired research review

- 同 frozen signal 比较 underlying 与 optionized implementation；
- baseline 与 optional overlay 分 lane；
- 仅在完整 DQ/PIT、policy、engine、result evidence PASS 后评估研究结论；
- 不自动 promotion 或生成投资建议。

## 9. 本波文件与生命周期

Task-owned：

- 本 supporting requirement；
- `config/research/qc_qqq_options_frozen_signal_implementation_retest_contract_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/frozen_signal_implementation_retest_contract.py`；
- `tests/test_qqq_options_frozen_signal_implementation_retest_contract.py`；
- 对应 architecture module/flow fragments。

Coordinator-owned：

- canonical task source/index/views；
- `docs/system_flow.md`；
- architecture/report-flow/compatibility generated authority 与 formal artifacts。

当前 frozen base：`8b677b452f6ec5810ccfaecbdf61664ac0a1f2c3`。

计划 branch：`codex/trading-2542h-qqq-options-frozen-signal-retest-contract`；复用主 checkout，不创建额外
worktree/clone/cache。退出条件是 final candidate 验证、local main fast-forward、普通 push 与 exact SHA
复核完成；task branch 仅在无 unique tracked/untracked/ignored bytes、无活动进程依赖后删除。

known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 不读取、不 hash、不 diff、
不 stage、不修改。

## 10. 当前安全状态

- `scope=non-executable DATA_RESEARCH`；
- `static_contract_authorized=true`；
- `signal_mapping_frozen=false`；
- `selection/execution/accounting/lifecycle_policy_frozen=false`；
- `qc_backtest_authorized=false`；
- `real_dq_authorized=false`；
- `raw_option_payload_download_or_export=false`；
- `orders/fills/positions=0`；
- `paper/live/production/broker=false/none`；
- 当前 terminal=`OWNER_EXACT_POLICY_FREEZE_REQUIRED_NO_BACKTEST`。

## 11. 进度记录

- 2026-08-28：Owner 确认采用“local 冻结既有信号与 run/result admission、QuantConnect 内完成
  option selection/execution/lifecycle/P&L”的路径并指示继续推进。READ_ONLY governed preflight PASS；
  local main=origin/main=`8b677b452f6ec5810ccfaecbdf61664ac0a1f2c3`、active lease=0、worktree audit PASS。
  本波只建立 non-executable serial scope-correction contract，不发起真实 DQ/backtest，不访问 QC/broker。
- 2026-08-28：canonical task registration=`PASS`，SINGLE_LANE START/LANE preflight=`PASS`；registration-only
  publication transaction 在 task event 写入后因其不具备完整 final-publication path scope，以 fail-closed
  terminal 释放，未冒充正式发布证据。contract file/canonical SHA-256=
  `d86e294f2be3ad8b8f601953ecd5e2b90d71317bca6dd197096de0db196dc80a`/
  `133eb368414d9f93429818a8a098df4d9bd0eed994ef58ca3b7615f31363d496`；strict load、Ruff、strict
  mypy、py_compile PASS，新增合同及 2483/2499/2542F 邻接测试=`91 passed`。当前 terminal 仍为
  `OWNER_EXACT_POLICY_FREEZE_REQUIRED_NO_BACKTEST`；provider/QC/real DQ/backtest/orders/fills/positions/
  production/broker=`0`。
