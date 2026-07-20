# 策略研究重启 R0～R2 真实结果收口

最后更新：2026-07-20

状态：`CONTINUE_EVIDENCE_CLOSURE`

结论边界：research-only / validation-only；`production_effect=none`、
`broker_action=none`、`candidate_expansion_allowed=false`。

## 1. 结论

R0～R2 工程与决策链已经完成，但策略证据没有通过继续扩展门槛。当前正确动作不是启动新一轮
参数搜索，而是继续闭合独立证据，并维持候选扩展暂停：

- R0 preflight 为 `PASS`，13/13 hard checks 通过，DQ 为 `PASS_WITH_WARNINGS`
  （0 error、2 warning）；
- R1 walk-forward 真实重跑 20 candidates × 2 windows × train/test，共 80/80 fold 完整；
  40 个 test fold 中 20 个 `reject`、20 个 `review_required`，每个候选均有一个负向窗口；
- source top-N 来自 full-period leaderboard，且两个 walk-forward window 与 locked holdout 重叠，
  因此只能作为 `2022-12-01 legacy_comparison` 诊断，不能声称无偏 OOS 或
  `2021-02-22 primary_validated` 结论；
- R1 robustness 的 9/9 一阶邻居已齐全，两个专用 stress bucket 完整且未出现负向 gate；
  但 `event_risk_high` 只有 15 行，低于 reviewed pilot floor 20，整体仍为
  `REVIEW_REQUIRED_INCOMPLETE_EVIDENCE`；
- forward ledger 有 16 条 append-only events；1d/5d/10d 成熟 15/14/9 条，20d/60d
  均为 0；另有 5 个缺失 daily archive，不能在本批补造；
- R2 技术验证为 `PASS`，业务决策为 `CONTINUE_EVIDENCE_CLOSURE`，并显式暂停新候选扩展。

## 2. 为什么这样设计

R0 先冻结窗口角色、source bytes、DQ/PIT、成本、execution lag、holdout、hypothesis 和 safety，
避免在看到结果后重新解释“默认窗口”。项目级 AI-cycle 仍从 `2022-12-01` 开始；
QQQ/SGOV/TQQQ 专项 primary validated window 从 `2021-02-22` 开始；2022 窗口在专项研究中
只承担 legacy comparison 角色。

R1 不再把 full-period aggregate 切片冒充 OOS，而是逐 fold 调用真实 evaluator，并保留
signal → next-session execution → return chronology、成本、false-risk-off 与 gate 输入。
robustness 也使用 daily path 上的独立 stress dates 和 per-regime comparator；不足样本保持
`REVIEW_REQUIRED`，不能降低 floor 来强行 PASS。

R2 按 fail-closed 顺序决策：R0 失败先 HOLD；证据 contract 不完整先继续闭合；只有 contract
完整后才判断负面/legacy-only 是否暂停；再之后才考虑 forward maturity 和 owner review。
因此本次 robustness 样本不足优先得到 `CONTINUE_EVIDENCE_CLOSURE`，同时 OOS 负面结果使
`candidate_expansion_allowed=false`。

## 3. 输入、输出与计算链

|阶段|主要输入|核心计算|主要输出|
|---|---|---|---|
|R0|window policies、source sweep、prices/rates/manifest、cost/execution policies|DQ、fingerprint、window-role、holdout、cost/lag、hypothesis/safety hard checks|`outputs/research_ops/strategy_restart/strategy_research_restart_preflight.json/md`|
|R1 OOS|R0 PASS、source top-20、2 windows|1-day purge + 1-day embargo；每 fold 真实 evaluator；cost/lag/chronology/false-signal/gate|`reports/etf_portfolio/dynamic_v3_rescue/walk_forward_r1/r1-wf_d6b1ac52eeafe099/`|
|R1 robustness|R0 PASS、source candidate、neighbor grid、daily paths|real/derived neighbor、high-drawdown、fast-recovery、per-regime dynamic-vs-static comparator|`reports/etf_portfolio/dynamic_v3_rescue/robustness_r1/r1-robustness_d6b1cd3e5521f1bd/`|
|R1 forward|append-only ledger、current caches、controlled policy|DQ、1/5/10/20/60d maturity、daily continuity、append-only integrity|`outputs/forward_evidence/maturity_tracker_r1/`|
|R2|全部通过 validator 的 R0/R1 artifacts|ordered decision rules、reason codes、next actions、source/output commitments|`outputs/research_ops/strategy_restart/r2_decision/`|

所有 validator 都重读 live source。Walk-forward validator 从完整 evaluator payload 重算 80 个
summary；robustness validator从 source daily path 重算 stress/regime comparators；R2 validator
重跑四类上游 validator、DQ/forward maturity 计算和 Markdown。

## 4. 性能优化结果

最初 R1 使用 `ThreadPoolExecutor`，CPU-bound evaluator 因 GIL 基本只占用一个核心，运行
207.54 秒仍未完成第一个 20-candidate phase。该无成品试跑已安全中止并清除不完整目录。

改为 4 个 persistent process workers 后，每个 worker 只初始化一次 runtime，并按 phase 缓存
fixed robustness reports：

|测量|工作量|耗时|
|---|---:|---:|
|串行基准|top-1，4 folds|67.95s|
|4-process 基准|top-4，16 folds|72.46s|
|吞吐提升|等工作量折算|约 3.75×|
|全量 R1 OOS|top-20，80 folds|226.02s|
|全量 OOS content validation|80 folds|5.34s|
|R1 robustness|9 neighbors + stress/regime|94.09s|
|R2 build / validation|完整证据链|7.32s / 7.09s|

4 worker 运行期间 CPU 负载均衡，RSS 稳定在约 325 MB/worker，没有长尾 worker 或内存失控。

## 5. 后续触发条件

1. `event_risk_high` 独立样本达到至少 20 行后，按同一 policy 重跑 robustness；不得提前改 floor。
2. owner 需要决定 5 个历史 archive gap 的治理方式；本批不回填、不补造。后续 daily evidence
   仍只能由统一 daily scheduler 或 runbook 受控人工路径产生。
3. 20d/60d 等 forward outcome 继续 append-only 成熟；target date 到达不等于 outcome 已绑定。
4. 若未来重启候选比较，必须另建不使用 full-period top-N 且与 locked holdout 分离的 selection
   protocol；当前 source 不能支持无偏 OOS claim。
5. simple selector 保持 `KILL`，GBDT 保持 `PIVOT_DESIGN_ONLY`，regret state machine 保持
   `WATCHLIST`；没有新的 owner 明确任务不得改变。

## 6. 验证与性能收口

最终 tracked state 已通过以下并行验证：

|验证层|结果|pytest / wall|
|---|---:|---:|
|focused integration|187 passed|30.22s|
|post-closeout focused|51 passed|18.10s|
|fast-unit|300 passed|43.90s / 45.07s|
|architecture-fitness|419 passed|34.41s / 34.92s|
|contract-validation|265 passed|150.84s / 151.35s|
|report-validation|55 passed|21.45s / 21.95s|
|reproducibility|23 passed|9.26s / 9.95s|
|full|6403 passed、2 skipped|978.80s / 979.69s|

full runtime artifact 为
`outputs/validation_runtime/full_20260720T031553Z/test_runtime_summary.json`。与此前
961.89s full 基线相比，墙钟增加约 17.80s（约 1.9%）；slowest tail 仍来自既有
smoothed/forward 周期链，新增 R0～R2 测试未进入 slowest 50，未观察到本批性能回退。

初次 fast-unit 暴露 report selection policy 与 deprecation inventory freshness 两个问题；
初次 architecture 暴露历史 compatibility source、callback matrix 与 manifest freshness 三类问题。
这些问题均按 canonical generator/source 规则修复，随后相应正式 tier PASS；没有以串行 pytest 或
临时 workaround 覆盖失败。
