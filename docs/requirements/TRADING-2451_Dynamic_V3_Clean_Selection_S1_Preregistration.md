# TRADING-2451：Dynamic v3 clean-selection S1 预注册冻结包

最后更新：2026-07-21

状态：`DONE_PREREGISTRATION_OWNER_CLEAN_RUN_AUTHORIZATION_PENDING`

稳定任务 ID：`TRADING-2451_DYNAMIC_V3_CLEAN_SELECTION_S1_PREREGISTRATION`

## 背景与目标

TRADING-2449 S0 已证明旧 R1 的 `full_period_source_leaderboard_top_n` 与既有
locked holdout 重叠，真实 gate 因而必须保持
`BLOCKED_CONTAMINATED_LEGACY_SOURCE`。Project owner 于 2026-07-21 指示继续推进
TRADING-2449 S1；本任务独立登记新的、结果不可见时冻结的 clean-selection 输入，
不改写旧 artifact、旧 gate 或 R2 决策。

本阶段只冻结候选全集、fold-local selection rule、窗口目录、指标、成本/执行、
falsification/kill criteria、canonical preregistration/context/campaign 和 content checksum。
它不运行 evaluator、backtest、robustness、候选搜索或 prospective holdout。

## 证据口径

- 本 S1 只能建立 `protocol-clean`：新的 fold-local train ranking/test evaluation 结果在
  freeze 时不可见，且旧 leaderboard/candidate metrics 不得进入候选全集或 selection 输入。
- 历史市场结果以及旧 R1 摘要已经可见，因此历史 fold 只能作为
  `historical_seen_protocol_replay`，不能包装成 investigator-blind 市场样本。
- 真正 outcome-blind 的结论必须依赖 freeze 后的 prospective holdout；该 holdout 在本任务中
  固定 `OWNER_AUTHORIZATION_REQUIRED` 且不读取。
- 即使预注册资格通过，仍固定 `unbiased_oos_claim_allowed=false`；只有独立 owner 授权后，
  后续 TRADING-106 clean evaluator 才能运行。

## 冻结设计

### 候选全集

- 来源仅限 tracked
  `config/etf_portfolio/dynamic_v3_rescue/parameter_sweep_real_smoke.yaml` 和既有
  `medium_real.max_candidates=300` 口径。
- 按 YAML `parameter_space` 轴顺序做 deterministic Cartesian product，冻结前 300 个参数包；
  不读取 sweep result、leaderboard、candidate report、real-evaluation report 或旧 top-N。
- 这是对既有 300-candidate governed space 的重新绑定，不增加参数轴、取值或候选数量；
  `candidate_expansion_allowed=false`、`new_parameter_search_allowed=false` 保持不变。

### Fold-local selection

- 每个历史 development fold 在 train 段独立评价全部 300 个冻结参数包；test 指标不得参与
  eligibility、score、ranking、tie-break 或缺位回填。
- 复用现有 reviewed hard gate 与 weighted score 语义，并在 selection rule 中逐字段冻结
  normalization、penalty、排序和 tie-break；按 train score 降序、candidate id 升序选 top 20。
- train 全部 reject 时该 fold 直接 `INCOMPLETE_NO_ELIGIBLE_CANDIDATE`；不得使用 reject 占位、
  不得从旧 leaderboard 回填。
- 固定 1 trading day purge、1 trading day embargo、1 trading day signal-to-execution lag、
  commission 0 bps、slippage 2 bps。

### 窗口与 holdout

- primary research window 保持 QQQ/SGOV/TQQQ `exact_three_asset_validated`，requested start
  为 2021-02-22；报告同时声明 project `ai_after_chatgpt` anchor=2022-11-30、start=2022-12-01。
- 历史 protocol replay 使用四个 expanding train / six-month test fold，最大 test end
  为 2024-12-31；effective dates 由 XNYS calendar 应用 purge/embargo 后生成。
- 2021-02-22 至 freeze date 的市场结果统一标记 `prior_market_outcome_visibility=KNOWN`。
- prospective holdout 从 2026-07-22 开始，至少覆盖至 2027-07-21；本阶段禁止读取，
  也不得以历史 replay 结果替代。

## Heuristic/Threshold Governance

- 300 candidates、top 20、score weights/normalization/penalties、execution cost、lag、
  purge/embargo、fold/holdout dates 与 campaign evidence budget 的唯一 canonical 定义位于
  owned `selection_rule.yaml` / `window_catalog.yaml`；module 从 policy 读取并重算 package，不再逐项
  镜像投资解释性 numeric literals。
- module 中邻接 TRADING-2451 说明的 `FROZEN_SELECTION_RULE_SHA256` 与
  `FROZEN_WINDOW_CATALOG_SHA256` 是本次 owner-frozen policy 的完整内容锚点。任一 threshold、date、
  cost、metadata 或 review condition 发生 byte-level drift，构包/验证均 fail closed，必须新建并审核
  preregistration 后才能更新指纹。
- `PACKAGE_ID_DIGEST_LENGTH`、`CANDIDATE_UNIVERSE_ID_DIGEST_LENGTH` 与
  `CANDIDATE_ID_DIGEST_LENGTH` 仅控制 schema identifier 长度，不参与投资判断。

## 实施拆解

|步骤|内容|依赖|验收|
|---|---|---|---|
|S1-A|登记任务并冻结 requirement、候选/selection/window policy|TRADING-2449 S0 DONE + owner 继续指示|无旧 result/leaderboard 输入，无参数扩展|
|S1-B|生成 canonical candidate universe、research context、preregistration、campaign 与 manifest|S1-A|所有 path/checksum/id 可重算，`result_visibility=NONE`|
|S1-C|实现 content-derived package validator 与 focused tests|S1-B|source drift、result-source 注入、candidate/order/tamper、window/holdout overlap、owner/run flag 篡改均 fail closed|
|S1-D|输出资格状态并等待独立 clean-run 授权|S1-C|最高只到 `ELIGIBLE_FOR_OWNER_AUTHORIZED_CLEAN_RUN`；不执行 evaluator|
|后续（未授权）|TRADING-106 clean fold-local evaluator|S1-D + owner 独立授权 + runtime DQ gate|不属于本任务|

## 安全边界

- `research_only=true`、`manual_review_required=true`、`production_effect=none`、
  `broker_action=none`。
- `candidate_expansion_allowed=false`、`new_parameter_search_allowed=false`、
  `evaluator_execution_allowed=false`、`locked_holdout_access_allowed=false`。
- `paper_shadow_change_allowed=false`、`production_weight_change_allowed=false`、
  `unbiased_oos_claim_allowed=false`。
- 本任务不得运行 TRADING-106/107、参数 sweep、backtest、promotion、shadow enrollment、
  official weights 或 broker/order 路径。

## 验收标准

- 候选全集必须从 tracked parameter config 机械重算为恰好 300 个唯一参数包；任何源 checksum、
  axis order、candidate id、数量或内容漂移均 FAIL。
- package 不得引用 `leaderboard.json`、`candidate_results.jsonl`、candidate report、旧 top-N 或
  real-evaluation 输出作为 selection 来源。
- canonical `ResearchPreregistration`、`ResearchEvaluationContext` 和 `CampaignSpec` 全部可解析，
  id/checksum/互相引用一致；owner clean-run authorization 与 holdout authorization 均为 false。
- 四个历史 fold 在应用 purge/embargo 后互不使用 test 结果做选择，且不与 prospective holdout
  重叠；historical seen 与 prospective outcome-blind 口径必须同时显式。
- validator 从 live tracked source 重算 package，不信任 manifest 自报；缺失、tamper、source drift、
  forbidden result source、窗口重叠或安全字段放宽必须 fail closed。
- focused pytest 使用 `-n 16 --dist loadfile`；本阶段不运行 Full/architecture/contract，正式共享
  manifests/system-flow 集成由后续 coordinator change 处理。

## 状态记录

- 2026-07-21：project owner 指示继续推进 TRADING-2449 S1；独立登记为 TRADING-2451 并进入
  `IN_PROGRESS`。当前授权仅覆盖预注册冻结与资格验证，不构成 TRADING-106 evaluator、historical
  replay 或 prospective holdout 运行授权。
- 2026-07-21：S1-B/S1-C/S1-D owned scope 已实现；content-derived package
  `dynamic-v3-clean-s1_cf88e2fc1cee51406b6b` 通过校验，资格为
  `ELIGIBLE_FOR_OWNER_AUTHORIZED_CLEAN_RUN`。focused pytest 为 `11 passed`；Ruff、Black check 与
  owned diff check 均通过。任务仍保持 `IN_PROGRESS`，等待 coordinator 集成 shared
  docs/manifests 与正式 gate；未运行 evaluator、backtest、search、prospective holdout，未产生
  production/broker effect。
- 2026-07-21：完成 Heuristic/Threshold Governance 自审；campaign budget 从 module 移入 frozen
  selection policy，validator 改为 canonical policy derivation + frozen SHA-256 fail-closed，不再镜像
  300/top20、score/cost/window numeric values；新增 top_n、score weight、slippage、window date
  policy-tamper coverage，四类变更均验证为 fail closed。最终owned validator=`PASS/0`，focused=
  `11 passed`，Ruff/Black/diff-check均PASS。当前等待coordinator完成system flow、generated manifests、
  compatibility与architecture/contract/full；TRADING-106仍未授权。
- 2026-07-21：coordinator完成shared integration与正式门槛。generated state=`878 tasks / 429 active /
  449 completed / 992 modules / 1143 test files / 0 direct-writer violations`；architecture/contract=
  `446/265 passed`，Full=`6498 passed / 2 skipped / 642 warnings / 940.47s`。首次architecture run因新增
  module/test带来的deprecation inventory总数与reference counts陈旧而`445 passed / 1 failed`，完整刷新
  frozen inventory后复验PASS；未采用workaround。任务归档DONE仅表示预注册冻结包完成，不表示clean run、
  历史评估或prospective OOS结论完成。`clean_run_authorized=false`、`unbiased_oos_claim_allowed=false`、
  `production_effect=none`继续成立。
