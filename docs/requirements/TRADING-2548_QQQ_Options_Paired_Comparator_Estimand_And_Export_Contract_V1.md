# TRADING-2548：QQQ Options Paired Comparator Estimand And Export Contract V1

最后更新：2026-08-30

稳定任务 ID：
`TRADING-2548_QQQ_OPTIONS_PAIRED_COMPARATOR_ESTIMAND_AND_EXPORT_CONTRACT_V1`

优先级：`P0`

状态：`BLOCKED_OWNER_INPUT`

Owner 指令：Project Owner 在完成 exact commit
`d5ff6dd9f8b84274bfc945ad8bd86fcecb92a8ed` 的网页版 ChatGPT Pro 规划审阅后指示
“好的 基于这个结论推进吧”。本任务据此只推进第一波 serial non-executable paired-comparison
contract：冻结比较问题、primary comparator、normalization、export-safe schema、calendar
diagnostics 与 typed stop matrix；不修改既有五态方向信号或 37-slot option implementation
baseline，不访问 QuantConnect，不运行真实 DQ/backtest。

Owner decision：
`owner_decision:TRADING-2548:2026-08-30:adopt_paired_comparator_contract_wave_v1`

production effect：`none`

broker action：`none`

## 1. 问题与目标

TRADING-2542I 已完成一次 frozen-signal QQQ option baseline backtest，并只接纳 export-safe
aggregate。该结果证明 bounded QuantConnect implementation path 可以运行，但没有准入足以证明
same-signal paired comparator outcome 或风险可比性的 evidence。

当前 optionized baseline 是 `$100,000` cash account、每次最多使用 pre-trade NAV 的 `2%`
购买 premium、最多 `1 contract`；既有 `UNDERLYING_IMPLEMENTATION` 却只是
`NORMALIZED_ONE_SHARE_QQQ_QUOTE_LEDGER / NONE_NORMALIZED_RETURN_ONLY`。两者不共享同一 capital
estimand，因此不能从 `4.48%` 净收益推断 optionized implementation 优于或劣于 underlying。

本任务建立一个结果盲、不可执行、可机械重放的 successor contract，回答：

> 在完全不改 frozen signal 与 frozen option implementation policy 的前提下，未来单一 paired
> QuantConnect run 至少需要什么 comparator、normalization 和 export-safe evidence，才能区分方向
> signal effect 与 option implementation effect？

本任务不回答该经验问题，只冻结 future evidence contract。

## 2. 冻结继承与当前事实

### 2.1 immutable predecessor

- `first_layer_composer_v2` 五态仍是唯一 direction fact；
- mapping 固定为 `risk_on/constructive -> LONG_CALL`、
  `neutral/defensive/risk_off -> FLAT`；
- `LONG_PUT` 不属于 baseline；
- TRADING-2542I whole-draft 37-slot selection/execution/accounting/lifecycle policy 保持 immutable；
- primary requested/evaluated window 固定为 `2021-02-22..2025-12-02`、XNYS `1202` sessions；
- exact signal/package/DQ/PIT/manifest replay 已 `PASS`；
- backtest `f2879a3cee7ec4e0b68b4f943aafd1f8` aggregate 仅为
  `PASS_EXPORT_SAFE_AGGREGATE_ONLY`，不得用于 threshold、comparator、normalization 或 window
  selection；
- 当前无新 QuantConnect save/build/backtest/retry authority。

### 2.2 existing-result disposition

既有单次结果只允许标记为：

- `CAPABILITY_AND_DIAGNOSTIC_EVIDENCE_ONLY`；
- `FROZEN_BASELINE_SINGLE_RUN_AGGREGATE`；
- `PAIRED_COMPARATOR_OUTCOME=INSUFFICIENT_PLATFORM_EVIDENCE`。

它可以证明 frozen package 曾完成一次 bounded QC research run，并产生
orders/entries/exits/cancels=`116/58/58/0`、end equity=`104479.60`、net profit=`4.48%`、
fees=`75.40`。它不能证明 paired outcome、same-capital 或 risk-normalized superiority、signal alpha、
robustness、subperiod consistency、投资价值或 production readiness。

## 3. Primary estimand 与 comparator hierarchy

### 3.1 primary comparator

唯一 primary comparator ID 保持 `UNDERLYING_IMPLEMENTATION`，successor method 冻结为：

`SAME_SIGNAL_FULLY_FUNDED_QQQ_CASH_ACCOUNT`

语义：

- initial cash=`USD 100,000`；
- 与 optionized side 使用相同 exact signal/package identity、mapping、effective session 与 event clock；
- `LONG_CALL` 状态持有 unlevered QQQ，`FLAT` 状态持有 zero-return cash；
- QQQ entry 使用对应合法 event 的 ask，exit 使用 bid；
- 只在 QuantConnect research simulation 内维护 comparator ledger，不提交 comparator order；
- option `NO_ELIGIBLE_CONTRACT` 不取消 underlying signal exposure；该差异属于 option availability cost；
- option expiry/guard/re-entry 不强迫 underlying comparator roll，因为这是 option-specific lifecycle；
- negative cash、margin、leverage、short QQQ 和 fill-forward 均禁止。

### 3.2 primary normalization

primary view=`COMMON_CAPITAL_ACCOUNT_VIEW`：

- 两边 initial capital 均为 `USD 100,000`；
- headline estimand=
  `optionized_net_return - underlying_implementation_net_return`；
- 两边都必须输出完整 start/end equity、cash、P&L、fee、drawdown 与 chronology reconciliation；
- valid negative outcome 是可证伪研究结果，不触发参数修改或 retry。

### 3.3 mandatory secondary view

secondary view=`CAPITAL_AT_RISK_TIME_VIEW`：

- option side=`entry premium debit * holding time`；
- underlying side=`deployed QQQ capital * holding time`；
- 只解释 leverage/capital-utilization trade-off；
- 不得把 primary `FAIL` 翻成 `PASS`；
- 与 primary 方向冲突时，terminal interpretation 固定为
  `MIXED_IMPLEMENTATION_TRADEOFF`。

### 3.4 named diagnostics 与 multiplicity

- `SGOV_CARRY_COMPARATOR`：仅衡量 zero-return idle cash 的 opportunity cost，不并入 primary ledger；
- `QQQ_BUY_AND_HOLD`：仅提供 market-context，不参加 primary PASS/FAIL；
- 既有 one-share ledger 降级为 `EVENT_CLOCK_AND_QUOTE_PATH_DIAGNOSTIC`；
- notional、entry/average/max delta、realized exposure、time-in-market、max premium-at-risk、
  capital utilization、no-contract exposure gap 均为 diagnostics；
- comparator 上限固定为 `1 primary + 2 named diagnostics`，运行后不得追加 benchmark；
- 无 authoritative continuous delta sampling evidence 时，realized delta terminal 必须为
  `INSUFFICIENT_PLATFORM_EVIDENCE`，不得本地补算。

## 4. Future export-safe paired evidence contract

未来 exporter 至少必须提供以下 aggregate/identity，且不得回流 raw option rows、完整 chain、contract-level
quote history 或本地 option repricing input。

### 4.1 identity

- run/project/backtest ID；
- repository exact commit、QC code SHA、policy file/canonical SHA、freeze-admission SHA；
- comparator-contract SHA、signal package/index/source SHA、run-manifest SHA；
- LEAN/platform version、build identities；
- requested/evaluated dates、calendar、session count。

### 4.2 DQ/PIT/signal

- canonical DQ receipt identity/status、PIT status、manifest replay status；
- expected/observed signal sessions、missing/duplicate/unknown counts；
- expected/observed transition count、mapping identity。

### 4.3 option event counts

- selection attempts、eligible selections、`NO_ELIGIBLE_CONTRACT`；
- entry intent/submit/fill/reject/timeout/cancel；
- exit fill 与 reason：`FLAT`、pre-expiry guard、terminal；
- fresh re-entry、invalid lifecycle、missing terminal mark counts；
- event reconciliation 必须解释 83 signal transitions 与 order/entry/exit counts 的关系。

### 4.4 account aggregates

optionized 与 underlying 两边分别输出：

- start/end equity、net P&L USD、net return；
- fees、contract-defined spread/slippage cost；
- min/ending cash、peak equity、max drawdown；
- time in market。

### 4.5 risk/exposure aggregates

- gross premium debit、max entry premium-at-risk、average/max premium utilization；
- premium-at-risk × holding-time；
- QQQ deployed-capital × holding-time；
- entry delta-notional、average/max notional；
- delta observation/missing counts；
- time-in-market sessions/minutes。

### 4.6 comparator evidence

- comparator ID/version/hash、signal identity match；
- LONG/FLAT episode count；
- option/comparator effective-event alignment and mismatch counts；
- entry/exit quote availability；
- primary comparator start/end equity、net return、drawdown；
- named diagnostic results only when preregistered。

## 5. Calendar diagnostics 与结果解释上限

只允许把同一次 primary run 机械分成：

- primary-window ∩ calendar 2021；
- calendar 2022；
- calendar 2023；
- calendar 2024；
- primary-window ∩ calendar 2025。

它们不是独立 backtest，不 refit、不重选 policy、不改变 threshold、不删除 no-contract/no-fill period；
zero-event year 保留 `ZERO_EVENT_COUNT`。不得在看到结果后新增事件窗口、regime window 或 sensitivity。

最高 interpretation=`RESEARCH_IMPLEMENTATION_COMPARISON_ONLY`。即使 future paired run 完整 PASS，
仍不证明 signal alpha、robustness、investability、production readiness 或 broker eligibility。

## 6. Typed falsification 与 stop matrix

precedence 固定为 `INVALID > FAIL > INSUFFICIENT > PASS`；missing/unknown/not-evaluated 永不升级为 PASS。

Mandatory axes：

1. frozen signal identity；
2. 1202-session coverage；
3. DQ/PIT/manifest；
4. frozen 37-slot policy；
5. option-alpha isolation；
6. comparator contract；
7. capital normalization；
8. event alignment；
9. accounting；
10. risk fields；
11. export safety；
12. platform identity；
13. calendar subperiod completeness；
14. multiplicity；
15. primary implementation estimand；
16. external authorization。

每个 axis 必须冻结 `PASS/FAIL/INSUFFICIENT/INVALID` 的机械条件和 stop action。至少包含：

- primary comparator 或 normalization 未 exact-freeze：
  `COMPARATOR_ESTIMAND_UNFROZEN_NO_BACKTEST`；
- 使用既有 `4.48%` 选择 comparator/normalization/window/baseline：
  `RESULT_LEAKAGE_AFTER_BASELINE / INVALID`；
- comparator outcome 缺平台 evidence：`INSUFFICIENT_PLATFORM_EVIDENCE`；
- frozen signal/policy hash 漂移、post-dispatch comparator change、raw export、local repricing、
  unauthorized external action：`INVALID`；
- valid empirical underperformance：`FAIL`，停止并报告，不修改 baseline 或重跑。

## 7. 实施阶段

### S0：registration boundary

- canonical task row 与本 supporting requirement；
- focused task-source validation；
- registration bytes 形成正式 implementation lane 的祖先；
- 不触发 generated report/strategy/backtest 或外部动作。

### S1：serial comparator contract

- 新增 strict non-executable YAML policy、loader 与 canonical serialization；
- 绑定 predecessor exact identities、current result disposition、primary/secondary/diagnostic roles；
- 冻结 export schema、calendar diagnostics、falsification matrix 与 safety；
- extra/missing comparator、axis、field、window、unknown status、executable flag 或 external authority
  均 fail closed。

### S2：consumer-safe documentation

- 更新 `docs/system_flow.md`，只加入 frozen signal/result admission 到 comparator-contract 的只读边；
- 更新 architecture module/flow authority；
- 不实现 QC exporter、local result admission、run manifest 或 empirical consumer。

### S3：validation 与收口

- focused golden/negative tests、Ruff、strict mypy、py_compile；
- Architecture、Contract、Integration、Reproducibility 与 Full 按自然 integration boundary 执行；
- final candidate 发布到 local/origin main；
- 任务转为 `BLOCKED_OWNER_INPUT`，等待 Owner exact-freeze contract file/canonical SHA；
- exact freeze 前 terminal=`OWNER_PAIRED_COMPARATOR_CONTRACT_EXACT_FREEZE_REQUIRED_NO_BACKTEST`。

## 8. Acceptance criteria

1. frozen signal、mapping、37-slot policy 与 historical run evidence bytes 不变；
2. current paired outcome 固定为 `INSUFFICIENT_PLATFORM_EVIDENCE`；
3. exactly one primary comparator 与 primary estimand；
4. exactly one mandatory secondary view，不能改变 primary disposition；
5. exactly two named diagnostics；
6. export-safe identity/event/account/risk/comparator schema 完整且不含 raw option payload；
7. calendar 2021..2025 partitions exact-once；
8. 16 mandatory axes 的四态机械规则 exact-once；
9. no parameter search、baseline mutation、QC/run manifest/external action；
10. `investment_conclusion_generated=false`、`production_effect=none`、`broker_action=none`。

## 9. Path ownership 与生命周期

Task-owned：

- 本 supporting requirement；
- `config/research/qc_qqq_options_paired_comparison_contract_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/paired_comparison_contract.py`；
- `tests/test_qqq_options_paired_comparison_contract.py`；
- 对应 architecture module/flow fragments。

Coordinator-owned：

- canonical task fragment/index/views；
- `docs/system_flow.md`；
- architecture、report-flow、compatibility generated authority；
- formal validation artifacts。

计划 branch：`codex/trading-2548-qqq-options-paired-comparator-contract`。复用主 checkout，不创建额外
worktree/clone/cache。registration commits 作为 implementation lane 祖先保留；最终只发布一次完整候选。

known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 不读取、不 hash、不 diff、
不 stage、不修改。

## 10. 当前安全状态

- `scope=NON_EXECUTABLE_DATA_RESEARCH`；
- `option_specific_alpha_allowed=false`；
- `five_state_mapping_frozen=true`；
- `frozen_37_slot_baseline_mutation_allowed=false`；
- `paired_comparator_outcome=INSUFFICIENT_PLATFORM_EVIDENCE`；
- `comparator_contract_exact_frozen=false`；
- `qc_exporter_implementation_authorized=false`；
- `local_result_admission_implementation_authorized=false`；
- `run_manifest_generation_authorized=false`；
- `real_dq_authorized=false`；
- `new_qc_action_authorized=false`；
- `provider_query_or_purchase=false`；
- `raw_option_payload_download_or_export=false`；
- `paper/live/production/broker=false/none`；
- `orders/fills/positions outside QC simulation=0`；
- terminal=`OWNER_PAIRED_COMPARATOR_CONTRACT_EXACT_FREEZE_REQUIRED_NO_BACKTEST`。

## 11. 进度记录

- 2026-08-30：Project Owner 指示按 Web Pro exact-commit 审阅结论推进。READ_ONLY governed
  preflight PASS：local/main/origin main=
  `d5ff6dd9f8b84274bfc945ad8bd86fcecb92a8ed`、active lease=0、worktree audit PASS。选择
  `SINGLE_LANE` serial contract wave；本轮不访问 QuantConnect、不运行真实 DQ/backtest，不修改
  frozen signal 或 37-slot baseline，production/broker action=none。
- 2026-08-30：registration transaction `trading-2548-registration-20260830-v1` 在
  `TASK_SOURCE_PRE_WRITE` 写入 canonical task row 后，task-source focused=`9 passed`、canonical
  registry validate=`PASS`（1035 tasks / 507 active / 528 completed）。该 registration-only transaction
  按 `FAILED` 释放并释放 lease，因为它不声明最终 implementation/generated/formal publication scope；
  登记 bytes 作为正式 task branch 的祖先保留，不冒充 final publication evidence。SINGLE_LANE
  `START` preflight 随后在 exact base `d5ff6dd9...` 上 PASS。
- 2026-08-30：S1 strict contract、loader、16-axis matrix 与 23 项 golden/negative tests 已完成；focused
  pytest-xdist=`23 passed`，Ruff=`PASS`，strict mypy=`PASS`，py_compile=`PASS`。draft 当前 file SHA-256=
  `8c748634f6869eb4d4e9dfb14493acd072d146074ce7e86462eec0adae15714a`、canonical SHA-256=
  `6f77cf17af6e435799a2e86e1fb6a81936368e053b2367efb3a8e2be13412267`。这些 SHA 仍是 lane draft
  身份，只有 final integrated tree 发布后才提交 Owner exact freeze；本轮无 QuantConnect、DQ、provider、
  raw option 或 backtest action。
- 2026-08-30：integration revalidation plan
  `integration-revalidation-ce0014943e91bcd4911c`=`READY_FOR_SINGLE_INTEGRATION_CANDIDATE`，publication
  transaction `trading-2548-paired-comparator-contract-20260830-v1` 的 governed `INTEGRATION` preflight=
  `PASS`。同一 transaction 已把 canonical task 状态推进到 `BLOCKED_OWNER_INPUT`，并按固定顺序完成
  task-source、architecture manifests、report-flow 与 compatibility authority rebuild；generator validation=
  `PASS`，相邻 focused pytest-xdist=`62 passed`。最终 candidate/formal tiers/Full/main/push 继续由该 transaction
  收口；不新增 QuantConnect 或其他外部动作。
- 2026-08-30：v1 candidate `06247d1f7fd81450ca43066ce9a8c9c2246fdc45` 的 Integration=
  `995 passed`、Reproducibility=`24 passed`；Architecture 与 Contract 各只有同一个失败：新增 module/test 后
  `arch_004g_deprecation_inventory` 的 frozen ID/count 断言仍为旧值。v1 失败证据保留并释放 lease，没有运行
  Full、main/push 或外部动作。v2 transaction 显式扩展 `tests/test_arch_004g_deprecation.py` shared claim，
  只把 inventory ID 与 module/test count 同步到 generator 已证明的 `1179/1337`；未改变任何 deprecation
  lifecycle 或 writer allowance。重建后 generators=`PASS`，对应 focused=`47 passed`。
- 2026-08-30：v2 candidate `0ab075e7729828bdb757c13526df9b36bdccb39c` 的 Architecture=
  `878 passed`、Contract=`278 passed`、Integration=`995 passed`、Reproducibility=`24 passed`；Full=
  `9971 passed / 14 failed / 3 skipped`。14 项失败全部由同一根因触发：新增 canonical task
  TRADING-2548 尚未进入 Atlas successor classification，页面因此按设计 fail closed 为
  `UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED`。v2 Full 证据已 immutable 保存并以 FAILED 释放 transaction，
  未发布 main、未 push、未运行外部动作。
- 2026-08-30：failure-fix v3 只试图扩展 Atlas source classification 与 reader projection；聚焦回归发现
  cited-query coverage count test 也必须同步，但该文件未列入 v3 精确路径声明，因此 transaction 在 candidate
  commit 前按 FAILED 释放，没有越权修改。v4 已扩展该测试路径并绑定 parent Full summary
  `outputs/validation_runtime/full_20260830T113951Z/test_runtime_summary.json`；本波仍不改变 paired contract、
  frozen signal、37-slot baseline、existing aggregate 或 safety authority，只允许一次 `failure_fix_rerun` Full。
