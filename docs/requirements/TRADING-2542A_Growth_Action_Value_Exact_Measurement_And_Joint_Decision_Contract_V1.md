# TRADING-2542A：Growth Action Value Exact Measurement And Joint Decision Contract V1

最后更新：2026-08-23

稳定任务 ID：`TRADING-2542A_GROWTH_ACTION_VALUE_EXACT_MEASUREMENT_AND_JOINT_DECISION_CONTRACT_V1`

优先级：`P0`

状态：`IN_PROGRESS`

mode：`SINGLE_LANE`（serial contract wave）

Owner 决定：
`owner_decision:TRADING-2542A:2026-08-23:adopt_gpt_pro_review_and_request_measurement_complete_v2_draft_v1`

`production_effect=none`；`broker_action=none`；`external_action=none`。

## 1. 决策与问题定义

Project Owner 已采纳对 exact commit
`b70fe3963988241b187bc0d30bbc422eed2b2160` 的 ChatGPT Web Pro 审阅结论。审阅会话为
`https://chatgpt.com/c/6a8a90ac-2e40-83e8-9ce6-6fc1cfb4dfdd`。UI 与回答自报
`GPT-5.6 Pro`，但没有 backend route attestation，因此模型证据状态为
`UI_PRO_AND_MODEL_SELF_REPORT_GPT_5_6_PRO_ROUTE_UNVERIFIED`；该限制不改变 Owner 对审阅内容的采纳。

采纳结论是 `REQUEST_NEW_VERSION_BEFORE_ANY_FREEZE`，不是批准或冻结
`strategy_growth_action_value_threshold_exact_value_sheet_v1.yaml`。V1 必须保持 immutable、
`DRAFT_FOR_OWNER_REVIEW`、`threshold_bundle_frozen=false`。八轴 disposition 为：七轴
`REJECT_AND_REQUEST_NEW_VERSION`，`CANONICAL_DQ_PIT` 为
`INSUFFICIENT_EVIDENCE_TO_APPROVE`。根因不是多数经济阈值数值本身，而是公式、测量语义、样本构造、
UNKNOWN/INVALID 处理和 joint terminal rule 尚未形成可复算合同。

本任务执行最小串行 contract wave，建立 V2 草案及独立 comparator contract。V2 完成后仍需 Owner
逐项审阅；本任务不冻结阈值，不运行 DQ/provider/cache/backtest/primary-window empirical evaluation，
也不产生投资结论。

## 2. V2 必须封死的合同

### 2.1 公共序列与年化

- candidate、baseline、comparator 和 QQQ factor 必须使用相同 exchange-session 日历与 common-session
  daily total-return 序列；缺日、重复 session、非有限值、收益小于等于 `-1` 均 `INVALID`；
- annualized geometric return 固定为
  `(product(1 + daily_return) ** (252 / common_session_count)) - 1`；所有 return delta 都是两条独立
  年化 return 的差，不允许先平均 daily spread 再年化；
- 所有小数阈值按 exact decimal 比较，不引入未声明 rounding tolerance。

### 2.2 八轴 V2 变更

1. `NON_BETA_ACTION_VALUE`：经济 floor 保留 `0.0100`；paired common-session daily total returns，
   circular moving-block bootstrap，block=`20` sessions，resamples=`10000`，seed=`2542`，one-sided
   confidence 从 `0.90` 提升为 `0.95`；lower bound 使用 nearest-rank 5th percentile 且必须严格
   `>0`。bootstrap replicate 对 candidate/comparator 使用同一组 circular block indices。
2. `NET_OF_COST_RETURN`：floor 保留 `0.0075`，cost reconciliation tolerance 保留 `0.0001`；明确
   `transaction_cost_model_v1` 的 daily modeled cost 从 candidate gross return 扣除，再按公共年化公式计算；
   gross、net、cost 三者 identity 超出 tolerance 为 `INVALID`，低于 return floor 为 `FAIL`。
3. `ACTUAL_PATH_DRAWDOWN_REGRESSION`：上限保留 `0.0200`；每个 slice 从首个 common session 的
   NAV=`1` 独立重置，按 actual candidate path 与 comparator total return path 计算 peak-to-trough absolute
   max drawdown；`abs(MDD_candidate)-abs(MDD_comparator)` 必须在 full window 和全部预声明 slices 分别通过。
4. `FALSE_RISK_OFF_COST`：上限保留 `0.0025`，但新增 minimum independent qualifying event
   count=`10`；anchor 是 baseline defensive veto 从 inactive 变 active 的 session，forward window 是随后
   `20` 个完整 exchange sessions，缺少完整 forward window 的 right-censored anchor 不进入数值统计；
   anchor 后 `20` sessions 内的新 activation 与前一事件合并，保留最早 anchor。qualifying event 要求
   QQQ-SGOV forward compounded total-return spread `>=0.0300` 且 QQQ forward path max drawdown
   `>=-0.0500`。event missed-return cost 按各路径相对 QQQ 的实际 underweight 与随后 QQQ-SGOV daily
   compounded excess return逐日复合计算；axis value 是 candidate event cost 减 baseline event cost 的算术均值。
5. `CANONICAL_DQ_PIT`：继续要求 DATA_RESEARCH canonical gate exact `PASS`，但 V1 的 120s、20%、
   OI 10、volume 1 只能作为 owner-intent draft，不能直接成为 repository authority。独立 serial DQ contract
   必须定义 quote-age clock、spread denominator、contract/session aggregation、missing/UNKNOWN、exact-date、
   PIT 和 terminal ordering，且获得独立审阅后才能执行。
6. `SAMPLE_AND_WINDOW_DEPENDENCE`：full-window independent action count 保留 `30`，per-slice 从 `3`
   提升为 `5`，gap 保留 `20` sessions，single-regime contribution share 保留 `0.50`。episode anchor 是
   growth action 从 inactive 变 active；20-session gap 内 action 合并并归属最早 anchor 所在 slice；跨 slice
   episode 不重复计数。贡献分母为各 episode signed net non-beta value 绝对值之和，slice numerator 为该
   slice episodes 对应绝对值之和；分母 `<=0` 为 `INSUFFICIENT`。
7. `ACTUAL_PATH_TURNOVER`：annualized one-way turnover 上限保留 `1.00`，cost-drag share 上限保留
   `0.25`。session turnover=`sum(abs(fill_notional))/opening_NAV`，不乘 `0.5`，同 session opposite fills
   不 net；annualized turnover=`sum(session_turnover)*252/common_session_count`。cost drag share=
   annualized modeled cost drag / annualized gross non-beta edge；分母 `<=0` 时不得 PASS，定为 `FAIL`。
8. `LEVERAGE_BETA_ATTRIBUTION`：beta increment 上限保留 `0.0200`，exposure-match tolerance 保留
   `0.0100`；beta 为带 intercept 的非年化 daily OLS slope，QQQ total return 为 factor，candidate 与
   comparator 分别对同一 common sessions 回归后相减，minimum common sessions=`252`。QLD/TQQQ、
   options position 或 borrowed leverage 仍直接 `INVALID`。

### 2.3 Comparator 与 joint terminal

`exposure_matched_no_signal` 必须是独立 versioned contract：只持有 QQQ/SGOV，不读取 growth signal，
使用 candidate 实际 QQQ exposure 的预声明 exposure target 进行 session-level matching，固定 rebalance
timing、calendar、total-return source、cost treatment、missing-session 和 mismatch calculation。它不得由
结果反推参数，也不得使用 leverage ETF、options 或 borrowed leverage。

八轴 joint terminal 固定为：任一 `INVALID` -> `GLOBAL_INVALID`；否则任一 `FAIL` ->
`GLOBAL_FAIL`；否则任一 `INSUFFICIENT` -> `GLOBAL_INSUFFICIENT`；仅八轴全部 `PASS` ->
`GLOBAL_PASS`。禁止 weighted compensation、majority vote 或 7-of-8。

## 3. 实施步骤与路径声明

### S0：canonical registration

- 本 requirement 与 canonical task event；
- 更新 parent TRADING-2542，记录 Owner 已采纳 Pro review、V1 未冻结、由 2542A 负责 V2。

### S1：serial measurement contract wave

- `config/research/strategy_growth_action_value_threshold_exact_value_sheet_v2.yaml`；
- `config/research/exposure_matched_no_signal_comparator_contract_v1.yaml`；
- `src/ai_trading_system/strategy_growth_action_value_measurement_contract.py`；
- `tests/test_strategy_growth_action_value_measurement_contract.py`；
- coordinator 更新 `docs/system_flow.md`、本 requirement、canonical task registry 与 generated views。

S1 只实现 strict schema、exact identity、cross-file binding、canonical seal/replay、公式和 terminal
contract 的 pure/offline validation。不得读取市场 cache、provider、Cloud 或 empirical result。

### S2：validation 与 Owner handoff

- focused pytest 默认 `-n 16 --dist loadfile`，并执行适用的 Architecture、Contract、Integration、
  Reproducibility 与 Full final-tree validation；
- V2 保持 `DRAFT_FOR_OWNER_REVIEW`，所有 axis 为 `PENDING_OWNER_APPROVAL`；
- task 完成后 parent TRADING-2542 继续 `BLOCKED_OWNER_INPUT`，下一步是 Owner 逐项审阅 V2；
- V2 获得逐项批准后，才可另行执行 S2B freeze；DQ 轴还必须先完成独立 serial DQ contract wave。

## 4. 验收标准

- V1 bytes 不变且明确记录为 rejected/not frozen；
- V2 完整覆盖八轴、comparator、公式、unit、common-session、sample、missing/UNKNOWN/INVALID 和 joint
  terminal，0 silent omission；
- bootstrap、event、episode、turnover、beta 的边界均有 positive/negative deterministic tests；
- strict YAML/JSON duplicate key、unknown field、identity drift、wrong comparator、wrong window、hidden
  leverage 和 action request 均 fail closed；
- DQ numeric intent 不被冒充为 reviewed canonical DQ authority；
- `threshold_bundle_frozen=false`，所有 DQ/empirical/cache/backtest/external/investment/trading flags=false；
- 适用 validation PASS，task source、system flow 与实现一致。

## 5. 生命周期

- exact base：`b70fe3963988241b187bc0d30bbc422eed2b2160`；
- task branch：`codex/trading-2542a-measurement-contract-v1`；
- workspace：复用 `D:\Work\AITradingSystem` 当前受审计 checkout，不创建 worktree、clone 或 external cache；
- purpose：仅完成 V2/comparator/measurement/joint-decision contract 和 tests；
- exit condition：V2 draft、测试、task/source/system-flow 同步并通过 final-tree validation，普通推送到 main，
  checkout 回到 clean main，删除已合并 task branch；
- recovery：合入前由 task branch/commit 恢复，合入后由 local/remote main 恢复；已登记的
  `docs/research/growth_tilt_owner_diagnosis_pack.md` 无关改动不得读取、hash、stage 或修改。

## 6. 进度记录

- 2026-08-23：Project Owner 采纳 GPT Pro 的 `REQUEST_NEW_VERSION_BEFORE_ANY_FREEZE` 结论，授权 Codex
  基于该结果继续；2542A 进入 registration/preflight。
- 2026-08-23：SINGLE_LANE START/LANE preflight PASS；已建立 V2 exact sheet、独立
  `exposure_matched_no_signal` comparator、strict typed loader、canonical replay 与 executable formula
  helpers。focused measurement-contract tests=`33 passed`；仍未运行 DQ/provider/cache/backtest/empirical
  action，V2 保持 draft/unfrozen。
- 2026-08-23：V2 file SHA-256=`bbb2e0ade108213269c3c9524b465836518457d932a6344887e6d8afb89ae620`，
  canonical SHA-256=`b978e952c4767756025fc01b17f8694004e720a5bb44aa5dde893628a4d9c199`；
  comparator file SHA-256=`4ced9407b8b8bca7b973c34016868fbf3151017bcc0b8ab67db449a0fed3b850`，
  canonical SHA-256=`f429d9ffc12b227bf9fad6eed3340ca833fdb44dc179c5f29dfc8f0318d9e1cf`。
  新旧 preregistration/decision-pack/V1/V2 联合 focused=`107 passed`，Ruff 与 strict mypy=`PASS`。
- 2026-08-23：正式 Reproducibility=`24 passed`、Integration=`995 passed / 643 warnings`、
  Contract=`276 passed`。首轮 Architecture=`863 passed / 2 failed`：新增 canonical task 后 explicit
  task-count ratchet 仍为 1016，且新增 module/test 尚未刷新 ARCH-004E generated manifests；两项均为
  freshness failure，不涉及测量公式。已把 ratchet 更新为 1017，并用 canonical generator 刷新 module、
  test、aggregate 与 fitness artifacts；对应 focused=`22 passed`，等待完整 Architecture 重跑。
- 2026-08-23：第二轮 Architecture=`864 passed / 1 failed`；唯一失败是新增 module/test 使 frozen
  deprecation inventory repository counts 从 `1143/1305` 变为 `1144/1306`，9 个 legacy surface 的
  reachability/count 与 direct writers=`856` 均未改变。已刷新 deterministic inventory id 为
  `arch_004g_deprecation_inventory_8771b87d09a116537d0c` 并再次生成 ARCH-004E artifacts；
  deprecation/devex focused=`24 passed`，compatibility/deprecation 联合回归=`211 passed`。
- 2026-08-23：第三轮完整 Architecture parallel rerun=`865 passed`，runtime artifact=
  `outputs/validation_runtime/architecture-fitness_20260823T074138Z/test_runtime_summary.json`。
  当前正式门为 Architecture=`865`、Contract=`276`、Integration=`995`、Reproducibility=`24`，均 PASS；
  下一步只运行独占 Full final-tree validation。
- 2026-08-23：首次独占 Full final-tree validation=`9376 passed / 28 failed / 3 skipped / 644 warnings`，
  runtime artifact=`outputs/validation_runtime/full_20260823T074719Z/test_runtime_summary.json`。28 个失败归并为
  三个治理 freshness 根因：`docs/system_flow.md` 的 DEVX-006D/006C sealed source identity 尚未随本任务更新；
  parent TRADING-2542 的一次 append-only projection 曾只保留 successor requirement、遗漏原 requirement；
  Atlas page-effectiveness disclosure 与 ignored local canonical page 仍指向旧 successor identity。新 V2
  measurement-contract tests 在该 Full 中无失败。
- 2026-08-23：已用 append-only parent task event 恢复 TRADING-2542 的原 requirement 与 successor
  requirement 双重 binding；已把 Atlas disclosure 更新为 `V1 rejected / V2 draft / Owner + independent DQ
  review required`，并把 `docs/system_flow.md` 的 DEVX-006D sealed identity 更新为 byte_count=`2255594`、
  SHA-256=`df18386e9b5338ab9fbc87220f9c6563f01db1a905604cbcb16190e89a4bd007`、git blob=
  `b027f38589f0ddaf689a165e4e8e1a6405cc9bc1`、entry_count=`1068`。待按 006D→006C 顺序重建权威、
  重建 final-commit-bound ignored Atlas canonical page 后，使用首次 Full artifact 作为 parent 运行一次
  `failure_fix_rerun`；仍不运行 DQ/provider/cache/backtest/empirical/external/trading action。
