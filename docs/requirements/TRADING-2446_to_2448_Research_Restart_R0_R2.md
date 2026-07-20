# TRADING-2446～2448：策略研究重启 R0～R2

最后更新：2026-07-20

## 背景与目标

ARCH-005 S0/S1 已完成，策略研究可以在不等待 ARCH-004 G2.5 或 ARCH-005 S2～S6
的前提下恢复。重启必须先消除研究窗口、数据快照、成本、holdout 和结论角色的语义歧义，
再补齐高风险证据债，最后形成基于真实结果的继续、暂停或扩展决策。

本批由三个连续阶段组成：

|任务|阶段|目标|初始状态|
|---|---|---|---|
|TRADING-2446|R0 Research Restart Contract|冻结窗口角色、研究 lane、PIT/DQ、成本、holdout、假设和安全边界，并生成可验证 preflight artifact|DONE|
|TRADING-2447|R1 Evidence Closure|补齐 TRADING-096/097 的逐 fold OOS、专用 stress、per-regime comparator，并刷新 TRADING-777 append-only forward maturity|BASELINE_DONE_EVIDENCE_INCOMPLETE|
|TRADING-2448|R2 Evidence Decision|消费 R0/R1 artifacts，形成继续证据成熟、暂停候选扩展或进入下一轮受控研究的明确决策|DONE|

完成工程实现不等于候选策略通过。若真实结果为负面、证据不足或仅 legacy-window 有效，
R2 必须保留负面结论并暂停扩展，不得为了关闭任务而升级策略状态。

## R0：研究重启契约

### 双窗口语义

两个窗口必须同时存在，但承担不同结论角色：

1. 项目级 `ai_after_chatgpt` 结论窗口：anchor=`2022-11-30`，start=`2022-12-01`。
   它回答“ChatGPT 发布后的 AI-cycle 表现如何”。
2. QQQ/SGOV/TQQQ 专项研究的 `primary_validated` 窗口：start=`2021-02-22`。
   它回答“在更长 exact-three-asset 数据上，候选是否稳健”。
3. `2022-12-01` 在 QQQ/SGOV/TQQQ 多窗口研究中同时是
   `legacy_comparison`；不得作为新 multi-window 主排行榜或 owner primary evidence 的唯一依据。
4. `2020-05-28` 仅为带 SGOV secondary-source gap caveat 的 sensitivity；
   `2020-05-26` 仅为 requested-inception metadata。

报告不得再使用无层级限定的 `default window`。必须同时输出 `window_id`、`role`、
`requested_start`、`actual_start`、`conclusion_scope` 和 caveats。

### Preflight 输入与输出

输入：

- `config/research/primary_research_window_policy.yaml`；
- `config/research/research_window_registry.yaml`；
- 本批新增的 versioned restart/evidence policy；
- TRADING-096/097 source sweep manifest、normalized config 和 candidate artifacts；
- primary/secondary prices、rates、download manifest 和同源 `aits validate-data` 质量门；
- controlled strategy cost/holdout policy 与 append-only forward ledger。

输出必须包含：

- policy/source/artifact SHA256；
- DQ 状态、价格与 rates 实际日期范围、PIT/source lineage；
- hypothesis、candidate family、primary lane、falsification criteria 和 kill criteria；
- cost/lag/purge/embargo/holdout snapshot；
- 双窗口语义检查和禁止混用检查；
- `research_execution_unblocked`，仅所有 hard checks 通过时为 true；
- `production_effect=none`、`promotion_gate_allowed=false`、
  `paper_shadow_change_allowed=false`、`broker_action=none`。

## R1：证据闭合

### TRADING-096 Walk-forward OOS

- 继续锁定 source sweep、candidate、real-evaluation report id、路径和 checksum。
- 对 source top-N candidate 按每个 train/test fold 重新执行真实 evaluator；不得复制 full-period
  metrics，也不得仅切片 full-period 聚合结果冒充逐 fold evaluator。
- split 必须应用 policy-governed purge/embargo，并输出 requested/effective/actual range、
  signal/execution/return chronology、cost、lag、false-risk-off、robustness、DQ 和 gate 输入。
- train evidence 用于 fold 内 ranking；test evidence只用于 OOS 评价。locked holdout 不得反向参与选择。
- validator 必须从 source/policy 重算或验证 content/checksum/Markdown，篡改状态、路径、window、
  metrics 或 gate 都必须 fail closed。
- `PASS` 只表示 evidence contract 和配置中的 pilot window gate 通过，不是 promotion evidence。

### TRADING-097 Robustness

- 补齐缺失的真实一阶参数 neighbor；若 source sweep 未包含该 grid point，允许在 robustness
  artifact 内生成 lineage-locked derived-neighbor evaluation，但不得改写 source sweep。
- `high_drawdown` 和 `fast_recovery` 必须来自 real daily path 的专用日期集合，禁止复用
  full-period aggregate analysis。
- per-regime comparator 必须同时输出 dynamic/static row count、gross/net return、drawdown、
  turnover、false-risk-off、relative gap、coverage 和 reviewed pilot gate。
- neighbor/stress/regime 任一 completeness 不足时最高为 `REVIEW_REQUIRED`；不得伪造
  robustness `PASS` 或 `LOW_RISK`。

### TRADING-777 Forward maturity

- 只读消费 append-only ledger，先执行同源 data-quality gate，再按 1d/5d/10d/20d/60d
  刷新 maturity。
- 本批不补造缺失 daily event、不回写历史 decision input，不把 target date 到达等同于
  outcome 已绑定。
- 必须披露 ledger event count、matured/pending count、missing archive、append-only integrity、
  latest market date 和 remaining blockers。

## R2：决策规则

R2 只消费通过 validator 的 R0/R1 artifacts：

|条件|决策|
|---|---|
|R0 hard check 失败或 DQ fail|`HOLD_RESEARCH_RESTART`|
|096/097 evidence contract 未闭合|`CONTINUE_EVIDENCE_CLOSURE`|
|证据闭合但 OOS/robustness 为负面或仅 legacy comparison 有效|`PAUSE_CANDIDATE_EXPANSION`|
|OOS/robustness 可继续但 forward evidence 未成熟|`CONTINUE_FORWARD_MATURATION`|
|全部 research-only gate 通过|`READY_FOR_OWNER_CONTROLLED_NEXT_RESEARCH_REVIEW`|

任何决策都不自动启动 simple selector、GBDT tuning、regret expansion、paper-shadow、promotion、
production weight 或 broker/order。simple selector 继续 `KILL`，GBDT 继续 design-only `PIVOT`，
regret state machine 继续 `WATCHLIST`，除非后续 owner 新任务明确改变。

## 实施顺序

1. 登记本任务与 versioned policy，建立 R0 builder/CLI/validator/tests。
2. 运行真实 R0；只有 `research_execution_unblocked=true` 才执行 R1。
3. 升级 TRADING-096/097 evidence schema、builder、validator 和 focused tests。
4. 运行真实 096/097，并刷新 TRADING-777 maturity tracker。
5. 构建 R2 decision artifact、Markdown closeout 和 validator。
6. 更新 task register、研究文档、artifact catalog、report registry、system flow 和 manifests。
7. 执行 focused、architecture、contract、report/reproducibility 及风险相称的 full parallel validation。

## 验收标准

- R0 artifact/Markdown/validator 均 PASS，且 DQ/PIT/window/cost/holdout/hypothesis/safety 可审计。
- 双窗口语义无歧义，2021 primary validated 与 2022 AI-cycle/legacy comparison 不混用。
- TRADING-096 逐 fold evaluator、purge/embargo、cost/lag/window gate 和 source hash 完整。
- TRADING-097 real neighbor、dedicated stress、per-regime comparator 和 source hash 完整。
- TRADING-777 以当前真实 ledger/data 重新生成，append-only 和 maturity 结果可复算。
- R2 decision 与 R0/R1真实结果一致，负面结果不会被升级。
- 所有产物固定 `production_effect=none`；无 shadow enrollment、promotion、production 或 broker action。
- required tests、validators、manifests 和文档一致性通过，工作区归属清晰。

## 进展记录

- 2026-07-20：owner 明确要求继续推进 R0、R1、R2；复合任务新增并进入 `IN_PROGRESS`。
  ARCH-004 仍停在 G2.5 前，ARCH-005 S2 未启动；本批为独立 research-only lane。
- 2026-07-20：R0 真实 preflight/validator `PASS`，13/13 hard checks 通过；项目级
  `2022-12-01` 与 QQQ/SGOV/TQQQ `2021-02-22` 双窗口角色已机器冻结。
- 2026-07-20：R1 真实 walk-forward 完成 20 candidates × 2 windows × train/test = 80/80
  fold，validator `PASS`；40 test folds 为 20 reject + 20 review-required，且 source selection
  与 locked holdout 污染明确披露。Robustness 9/9 neighbor 与 2/2 stress buckets 完整，
  `event_risk_high=15<20` 使 per-regime evidence 保持 incomplete；validator `PASS`。
- 2026-07-20：TRADING-777 刷新到 16 ledger events，append-only PASS；1d/5d/10d matured
  15/14/9，20d/60d 为 0，missing daily archive=5。R2 build/validator `PASS`，决策为
  `CONTINUE_EVIDENCE_CLOSURE`，candidate expansion/new parameter search 均 false。复合任务转
  `BASELINE_DONE`，剩余缺口为真实样本成熟、archive gap owner治理和未来无污染 selection protocol。
- 2026-07-20：最终验证通过：focused `187 passed`、fast-unit `300 passed`、
  architecture-fitness `419 passed`、contract-validation `265 passed`、report-validation
  `55 passed`、reproducibility `23 passed`；正式 full 为 `6403 passed, 2 skipped`，pytest
  978.80s、wall 979.69s。相对既有 full 961.89s 基线增加约 1.9%，无本批新增异常长尾；
  full runtime artifact 为
  `outputs/validation_runtime/full_20260720T031553Z/test_runtime_summary.json`。

真实结果与完整链路见
`docs/research/strategy_research_restart_r0_r2_closeout_2026-07-20.md`。
