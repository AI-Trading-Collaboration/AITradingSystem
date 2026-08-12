# TRADING-2515：Strategy Research Reopen Readiness Decision V1

最后更新：2026-08-13

稳定任务 ID：`TRADING-2515_STRATEGY_RESEARCH_REOPEN_READINESS_DECISION_V1`

优先级：`P0`

状态：`BASELINE_DONE`

mode：`SINGLE_LANE`

registration base：`c8da1bed30887b9a1bdc1a17b7699b026da93538`

`production_effect=none`；`broker_action=none`；`external_action=none`。

## 1. 目标

在任何新一轮策略研究、候选搜索、经验回测或投资结论之前，冻结一个可重放、可审计、
fail-closed 的 strategy-research reopen readiness 决策合同。该合同只回答：

1. 当前是否允许重新进入经验研究；
2. 当前最多允许推进到哪个治理阶段；
3. 哪些证据缺口仍阻止 reopen；
4. 后继数据证据车道是否已经被 Owner 明确选择和授权；
5. 哪些既有 preregistration、DQ/PIT 与 external-action 边界必须继续继承。

当前 baseline 必须同时表达：

- `reopen_decision=KEEP_CLOSED`；
- `permitted_stage=PREREGISTRATION_ONLY`；
- `empirical_research_authorized=false`；
- `candidate_search_authorized=false`；
- `backtest_authorized=false`；
- `holdout_access_authorized=false`；
- `investment_conclusion_authorized=false`。

`PREREGISTRATION_ONLY` 只允许合同设计、readiness 盘点与全新假设预注册，不表示策略研究、
参数搜索、模型训练、回测或投资判断已经重开。

## 2. 必须继承的 authority

本任务只聚合并验证既有 authority，不得复制、重定义或降低其约束：

- `TRADING-2449_DYNAMIC_V3_CLEAN_SELECTION_PREREGISTRATION_GATE`：legacy source 仍受
  contaminated-source 与 holdout-overlap fail-closed 规则约束；
- `TRADING-2451_DYNAMIC_V3_CLEAN_SELECTION_S1_PREREGISTRATION`：clean S1 package 的最高
  语义仅为 `ELIGIBLE_FOR_OWNER_AUTHORIZED_CLEAN_RUN`，历史市场结果已被看见，不能声称
  investigator-blind 或 unbiased OOS；
- `TRADING-2463_DECISION_TARGET_REDESIGN_PREREGISTRATION`：O1 capability audit 必须在
  canonical DQ strict PASS 与独立 Owner 授权后另立任务；
- `TRADING-2465_POST_O1_ROUTE_DECISION_AND_BLIND_REENTRY_PREREGISTRATION` 及其 reviewed
  successor：blind calendar re-entry 不得被当前 readiness 合同提前触发；
- `TRADING-2510` / `TRADING-2511`：QQQ Options primary-window calibration 与 derived
  evidence 仍以 canonical DQ/PIT、2021-02-22 primary start 和真实 evidence inventory 为准；
- `TRADING-2512` / `TRADING-2513` / `TRADING-2514`：collector、exact Owner token、
  single-use ledger 与 evidence admission 合同必须保持；当前 2513 token、collection、真实
  evidence 与 production DQ/PIT PASS 均未发生。

Primary Research Window 继续固定从 `2021-02-22` 开始。`2022-12-01` 不得成为新研究默认。

## 3. Readiness 决策合同

### 3.1 决策与阶段

v1 只允许以下 typed 状态：

- `KEEP_CLOSED`：经验研究不得重开；
- `PREREGISTRATION_ONLY`：仅允许治理合同与全新假设预注册；
- `SINGLE_DATA_EVIDENCE_LANE_ONLY`：仅允许一个已审阅数据证据车道，不允许同时跑多个重数据路线；
- `READY_FOR_OWNER_REOPEN_REVIEW`：证据已达到 Owner review 输入完整度，但仍不是 reopen 授权。

本合同不得产生 `REOPEN_AUTHORIZED`、`STRATEGY_PASS`、`MODEL_READY`、`PROMOTION_READY` 或
等价状态。任何真实 reopen 必须由后继任务和独立 Owner exact decision 处理。

### 3.2 单一数据证据车道

后继阶段同一时间只允许选择一条重数据路线：

1. `QLD_CANONICAL_FULL_CACHE_DQ`；或
2. `QQQ_OPTIONS_PRIMARY_WINDOW_EVIDENCE`。

QQQ Options 路线在 2513 exact Owner token 缺失时必须保持不可执行。v1 可以根据当前事实推荐
QLD DQ 作为下一调查路线，但 recommendation 不等于 Owner selection、cache mutation 授权或
周期性操作授权。

### 3.3 必须 fail closed 的事实

- authority path/hash/schema/status 任一不一致；
- required source 为 `UNKNOWN`、`FAIL`、`NOT_EVALUATED` 或缺失，却被调用方声明为 PASS；
- primary research start 不是 `2021-02-22`；
- 同时选择两个数据证据车道；
- 通过 legacy results、full-period leaderboard 或 locked holdout 反向构造 preregistration；
- 把 engineering PASS、页面 PASS、capability GO 或 policy contract 完成解释为 strategy PASS；
- 把 2514 admission 合同完成解释为 Owner token、cloud run、evidence 或 DQ/PIT PASS 已发生；
- 任何 candidate/search/backtest/holdout/paper/live/broker/production 标志为 true。

## 4. 实现范围

### S0：registration boundary

- canonical task row 与本 supporting requirement；
- task shadow/DevEx/current authority 重建；
- focused registration validation、ordinary non-force push 与 exact base release。

### S1：serial readiness contract wave

- task-owned reviewed-baseline policy；
- typed sealed readiness source、blocking reason、lane recommendation 与 decision record；
- strict canonical bytes/SHA/from-json/replay；
- current authority file identity 与 frozen semantic-fact validation；
- 默认 `KEEP_CLOSED + PREREGISTRATION_ONLY`。

### S2：consumer-safe wiring

- system flow 与 architecture fragment；
- Atlas/page coverage 只披露 readiness，不提升 strategy research 状态；
- task registry/generated/compatibility authority 同步；
- 不接入 central empirical runner、backtest CLI 或 external platform action。

### S3：验证与收口

- unit/property/golden；
- hash drift、forged PASS、UNKNOWN/FAIL、双车道、research-window drift、unauthorized
  empirical action 与 replay negatives；
- focused/adjacent/compatibility 与 final-tree formal gates；
- ordinary non-force push、SHA verify、branch/worktree cleanup。

## 5. Task-owned 与 coordinator paths

预期 task-owned：

- `config/research/strategy_research_reopen_readiness_decision_v1.yaml`；
- `src/ai_trading_system/strategy_research_reopen_readiness_decision.py`；
- `tests/test_strategy_research_reopen_readiness_decision.py`；
- `docs/requirements/TRADING-2515_Strategy_Research_Reopen_Readiness_Decision_V1.md`；
- 对应 architecture fragment。

Coordinator 负责：

- canonical task registry 与 generated views；
- `docs/system_flow.md`；
- Atlas coverage consumer；
- compatibility/report-catalog/generated authority；
- formal validation、main integration、ordinary push 与 cleanup。

## 6. 明确禁止

- 不运行 dynamic_v3、O1、QQQ/SGOV/TQQQ 或 QQQ Options 的真实候选搜索、参数搜索、经验回测；
- 不读取 prospective/locked holdout 结果以设计新假设；
- 不自行填写投资阈值、acceptance threshold、position sizing 或 promotion policy；
- 不执行 QuantConnect/cloud/API/CLI/HTTP/Object Store/raw download；
- 不执行 cache refresh、QLD full-cache build、paper/live/broker/production；
- 不生成投资建议或声称策略有效。

## 7. 当前 blocker 与 exit condition

当前 blocker：

- 统一 readiness contract 尚未实现；
- 后继单一数据证据车道尚未由 Owner 明确选择；
- 2513 exact Owner token、QQQ Options primary-window collection/evidence/DQ-PIT PASS 均未发生；
- O1 blind-calendar trigger 与独立 capability-audit 条件尚未满足。

exit condition：2515 final-tree 合同、tests、shared authority 与 formal gates 完成并普通 push；随后
只能从该 exact main 另行登记 strategy-governance preregistration lane 和一条数据证据 lane。

## 8. 进度记录

- 2026-08-13：从 exact main `c8da1bed30887b9a1bdc1a17b7699b026da93538` 完成只读
  authority 盘点；2515 尚未登记，runner/lease 为 0，external action 为 none。
- 2026-08-13：registration boundary 已普通 push 为
  `b5c3b8d69c4c48dd76141394d7db794a74dd0802`。随后从该 exact base 建立 serial contract
  task branch；START/LANE preflight 均 PASS，未读取 cache 或运行任何经验研究。
- 2026-08-13：完成 reviewed baseline policy、9-source exact authority inventory、39 项 YAML
  semantic fact、7 项 requirement semantic snippet、typed source/reason taxonomy、canonical seal/replay
  与 fail-closed builder。当前 policy file SHA-256 为
  `6f4688c245b512cef315128d721f76012725f37b7e232e197f107b1b4d27e223`，canonical
  SHA-256 为 `ccde97a297ff9334dc8b93a937ecf43efadaac959d3e01dadc1a2980d438a637`，
  authority-set SHA-256 为 `16d18eb1ad3c1052eac533979374cff2d1f118a0a021008533d5aed10c2b9269`。
- 2026-08-13：task-focused 首轮为 `32 passed / 1 failed`，唯一失败为 test exact inventory
  把 39 误写成 45；更正后同一 `-n 16 --dist loadfile` 覆盖为 `33 passed`。consumer 组合首轮
  为 `66 passed / 2 failed`（旧 renderer 文案与 33-task canonical page），完整 writer 重建后第二轮
  为 `67 passed / 1 failed`（旧 exact count 33），第三轮同一 68-test 覆盖为 `68 passed`。
  这些首轮/第二轮只作为 failure-fix evidence，不作为 formal promotion evidence。
- 2026-08-13：Atlas consumer 只新增 `KEEP_CLOSED + PREREGISTRATION_ONLY`、单一 evidence-lane
  未选择与 QLD recommendation-only 披露；既有 ENGINEERING/OWNER_VISUAL/READER_COMPREHENSION
  三条 typed PASS 事实由完整 writer 原样保留，没有重置、伪签或串轨。当前仍未执行 candidate
  search、backtest、holdout、cache、QuantConnect 或其他 external action。
