# TRADING-2449：Dynamic v3 无污染选择预注册资格门

最后更新：2026-07-20

状态：`BASELINE_DONE`（S0 实现闭环；current real source artifact 缺失；真实 clean run 未授权）

## 背景与目标

TRADING-2446～2448 的 R1 已完成真实逐 fold evaluator，但 source top-N 来自
full-period leaderboard，两个 walk-forward window 又与 locked holdout 重叠。因此当前证据只能
支持 `2022-12-01 legacy_comparison` 诊断，不能支持无偏 OOS 结论；R2 正确输出
`CONTINUE_EVIDENCE_CLOSURE`，并保持 `candidate_expansion_allowed=false`。

本任务不补跑参数搜索，也不把既有受污染结果重新命名为 clean evidence。S0 只在现有
TRADING-106 fold-local selection/evaluation 之前增加 source eligibility 与 preregistration gate，
证明未来输入是否在结果不可见时冻结、是否独立于 full-period 排名、以及 selection/test window
是否与 locked holdout 隔离。

## 安全边界

- 复用 canonical `ResearchPreregistration`、`ResearchEvaluationContext`、`CampaignSpec` 和
  TRADING-106 fold-local evaluator，不建立第二套评价逻辑。
- 当前真实 R1 source 必须得到 `BLOCKED_CONTAMINATED_LEGACY_SOURCE`；不得被本任务洗白。
- S0 不运行 backtest、不生成或扩展候选、不改变阈值、score、position、official weights、
  paper-shadow、promotion、production 或 broker/order。
- 固定 `clean_run_unblocked=false`、`unbiased_oos_claim_allowed=false`、
  `candidate_expansion_allowed=false`、`new_parameter_search_allowed=false`、
  `production_effect=none`、`broker_action=none`。
- 真实 S1 只有 research owner 提供新的、结果不可见时冻结的 preregistration 后才能另行登记和启动。

## 输入、输出与计算逻辑

### 输入

1. R2 validated decision artifact 及其 source commitments；
2. 当前 R1 walk-forward source manifest、selection source 与 locked holdout 定义；
3. `config/research/strategy_research_restart_policy.yaml`、research window registry、primary window
   policy 与受控 cost/execution policy references；
4. canonical preregistration contract 与 TRADING-106 campaign/fold-local selection contract；
5. 新增的 versioned clean-selection policy，只定义资格与禁止条件，不创建候选参数。

### 输出

- schema：`dynamic_v3_clean_selection_preregistration_gate.v1`；
- JSON/Markdown：`outputs/research_ops/strategy_restart/clean_selection_gate/`；
- 输出 source/policy/preregistration checksum、结果可见性、candidate-universe origin、selection rule、
  window/holdout overlap、metric/policy references、failed check 与下一责任方；
- validator 必须重读 live source 并逐项重算，不能只信任 artifact 自报状态。

### 判定顺序

1. source 或 policy checksum 不一致：`BLOCKED_SOURCE_DRIFT`；
2. source origin 为 `full_period_source_leaderboard_top_n`，或候选由结果可见后的排名产生：
   `BLOCKED_CONTAMINATED_LEGACY_SOURCE`；
3. preregistration 缺少冻结时间、candidate universe、selection rule、window catalog、metric ids 或
   policy refs：`BLOCKED_INCOMPLETE_PREREGISTRATION`；
4. `result_visibility != NONE` 或 freeze time 不早于任何被选择结果：
   `BLOCKED_RESULT_VISIBILITY`；
5. train/test 与 locked holdout 任一重叠：`BLOCKED_HOLDOUT_OVERLAP`；
6. 只有全部资格检查通过时，未来 artifact 才可为 `ELIGIBLE_FOR_OWNER_AUTHORIZED_CLEAN_RUN`；
   该状态仍不自动运行 evaluator，也不允许访问 locked holdout。

## 实施拆解

|步骤|内容|依赖|验收|
|---|---|---|---|
|S0-A|登记任务并冻结 policy/schema/输入输出/安全边界|R2 closeout|无新候选、阈值或运行副作用|
|S0-B|实现 eligibility builder 与 content-derived validator|S0-A|当前 R1 明确 blocked；tamper/source drift/overlap fail closed|
|S0-C|接入 research-restart CLI、report registry、artifact catalog、system flow 与 focused tests|S0-B|CLI/JSON/Markdown/validator 一致，`production_effect=none`|
|S1（未授权）|由 owner 提交新的 preregistration 并执行 clean fold-local run|S0 PASS + owner input|必须另建任务，不在本批自动进入|

## 验收标准

- 当前真实 R1 source 输出 `BLOCKED_CONTAMINATED_LEGACY_SOURCE`，且不得改变 R2 决策；
- full-period top-N、结果可见后冻结、candidate-universe checksum 漂移、holdout overlap、缺失字段、
  artifact/Markdown 篡改全部 fail closed；
- 合成的 contract-only clean fixture 仅证明资格门可达，不执行 backtest、不生成候选；
- CLI help/path/exit code、report registry、文档、module/test manifest 和 architecture contract 同步；
- focused pytest 默认 `-n 16 --dist loadfile`，并按风险运行 architecture/contract；是否运行 full 由
  自然集成边界与 formal trigger policy 决定；
- `candidate_expansion_allowed=false`、`new_parameter_search_allowed=false`、
  `production_effect=none`、`broker_action=none` 全链固定。

## 状态记录

- 2026-07-20：R0～R2 closeout 后只读审计确认，`event_risk_high=15<20`、20d/60d
  maturity=0 与 5 个 archive gap 都不能由工程补造；唯一无需外部数据且不改变策略结论的可执行项是
  clean-selection S0 资格门。任务登记并进入 `IN_PROGRESS`；真实 S1 仍需 owner 预注册。
- 2026-07-20：S0 policy、builder、content-derived validator、JSON/Markdown、research-ops CLI、report
  registry 与 tamper contracts 完成；正数 train-only top-N 不被误伤，只有 full-period/result-visible
  selection 才按污染阻断。Focused integration 81 passed，contract-validation 265 passed / 149.33s。
  当前工作区不存在先前 run-specific R1/R2 artifact bundle，因此不能在不补跑 backtest 的条件下生成
  真实 current-source gate artifact；本批不得补造。若 final formal gates PASS，先以
  `BASELINE_DONE` 保留该真实证据缺口；真实 clean S1 仍需 owner 新预注册和另建任务。
- 2026-07-20：formal closeout PASS：architecture=446 passed / 35.11s，contract=265 passed /
  149.33s，full=6456 passed / 2 skipped / 643 warnings / 969.84s；新增 gate tests 未进入
  slowest 50。任务转 `BASELINE_DONE`，继续保留 current real R1/R2 bundle 缺口，不用 synthetic
  fixture 代替真实 evidence；candidate expansion/new search/evaluator/backtest/production/broker 继续关闭。
