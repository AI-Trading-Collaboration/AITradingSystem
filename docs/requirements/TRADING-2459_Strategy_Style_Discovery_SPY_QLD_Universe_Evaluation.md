# TRADING-2459：Strategy Style Discovery 的 SPY/QLD 宇宙评估

最后更新：2026-07-26

状态：`BASELINE_DONE_ROLE_LIMITED_2X_IMPLEMENTATION_APPROVED`

稳定任务 ID：`TRADING-2459_STRATEGY_STYLE_DISCOVERY_SPY_QLD_UNIVERSE`

Owner 决策：

- `owner_decision:TRADING-2459:2026-07-25:spy_reference_now_qld_research_before_action_universe`；
- `owner_decision:TRADING-2459:2026-07-25:approve_scoped_five_asset_dq_to_avoid_qld_block`；
- `owner_decision:TRADING-2459:2026-07-25:approve_qld_role_limited_2x_implementation_instrument`。

## 目标

本任务落实两项不同性质的决定：

1. `SPY` 立即进入 Strategy Style Discovery 的强制 reference / benchmark /
   regime-control universe，但不进入当前 QQQ/SGOV/TQQQ 权重动作空间。
2. `QLD` 先进入 research-only instrument evaluation，回答它在相同
   QQQ-equivalent exposure 下，是否相对现有 QQQ/TQQQ/SGOV 实现提供可复核的成本、
   外部再平衡、暴露漂移或风险收益增量；评估结论形成后，Owner 已批准它进入
   role-limited 2x execution / implementation universe。

当前正式 primary action assets、official weights、paper-shadow、production 和 broker
均不改变。角色批准只允许执行层在满足上游条件后考虑 QLD，不是立即生成 QLD 权重。

## 为什么不能直接做“加入 QLD 后谁的回测收益更高”

QLD 是 Nasdaq-100 每日 2x 暴露工具，不是独立 Alpha 来源。在现有 long-only fractional
weight 空间中，QQQ/TQQQ/SGOV 已可表达 2x QQQ-equivalent exposure；增加 QLD 不会扩大
理论可达暴露区间，但可能改变：

- 维持约 2x 暴露所需的外部再平衡；
- 交易成本和持仓漂移；
- 每日杠杆产品真实路径、费用和融资影响；
- drawdown、worst-window 和恢复路径。

若直接把 QLD 加入候选搜索，结果会混入新增选择自由度，无法识别收益来自工具价值还是事后
选择。因此本任务只做 instrument implementation comparison，不运行策略搜索、不修改信号、
threshold、candidate generator 或 allocator。

## 预注册范围

### 日期与可见性

- active primary research start：`2021-02-22`；
- historical-seen diagnostic end：`2026-07-21`；
- 原计划的 prospective untouched start：`2026-07-22`；
- 2026-07-25 审计共享 `prices_daily.csv` 文件尾部时，终端已显示
  `2026-07-22` 至 `2026-07-24` 行；因此本任务不得再把该区间称为 untouched/OOS；
  正式计算必须在加载后立即截断到 `2026-07-21`，且后续值不得进入指标、结论或参数修改；
- `2022-12-01` 只作为 legacy / AI-cycle comparison 标签，不是新的主起点；
- `2020-05-28` 只可作带 SGOV 来源 caveat 的 sensitivity，本任务 v1 不使用；
- 当前结果只能是 historical-seen instrument diagnostic，不能声称 unbiased OOS。

### 宇宙角色

- reference / benchmark / regime control：`SPY`, `QQQ`；
- current primary action comparator：`SGOV`, `QQQ`, `TQQQ`；
- role-limited 2x execution / implementation instrument：`QLD`；
- QLD 不得作为 signal input、candidate ranking key 或 primary research-window 迁移理由。

### 固定 2x 实现

在相同日期、相同 adjusted-close return、相同成本政策下比较：

1. `qld_100`：100% QLD；
2. `qqq_50_tqqq_50`：50% QQQ + 50% TQQQ；
3. `sgov_33_tqqq_67`：1/3 SGOV + 2/3 TQQQ。

后两者的目标 QQQ-equivalent exposure 均为 2x。不得根据结果改变组合或新增第四种实现。

每种实现固定评估：

- buy-and-hold；
- weekly target-weight rebalance；
- monthly target-weight rebalance；
- daily target-weight rebalance。

再平衡日由交易日序列机械决定，不读取未来收益。交易成本使用
`config/research/transaction_cost_model.yaml` 的同一 as-run 版本；基金自身费用、融资和每日
reset 的实际影响由真实 adjusted-close 路径承载，不另外补造。

初始建仓统一计 `1.0` one-way traded notional；之后每次收盘再平衡的 one-way turnover
定义为 `0.5 * sum(abs(target_weight - drifted_weight))`，成本为该 turnover 乘成本政策的总
bps，并从当日 NAV 扣除。`buy_and_hold` 除初始建仓外不再平衡。年化按 `252` sessions，
Sharpe 使用 `0` risk-free rate并明确标注为 non-excess-return Sharpe。每个切片独立从
`NAV=1` 和目标权重开始并计初始成本。

### 必须报告的指标

- requested / evaluated range 与共同交易日数；
- total return、CAGR、annualized volatility、max drawdown、Sharpe、Calmar；
- worst 1d / 5d / 20d loss；
- external turnover 与 cost drag；
- QLD、TQQQ 相对 2x / 3x QQQ daily target 的 residual bias、MAE、RMSE；
- calendar-year 和 `2022-12-01` 前后切片；
- SPY / QQQ buy-and-hold benchmark；
- 数据质量、provider、endpoint、参数、下载时间、row count、SHA-256 和 source cache
  commitments。

## 数据质量与审计

1. 原 QQQ/SGOV/TQQQ/SPY cached source 必须先走 `aits validate-data` 同一 canonical
   validation code path并停止于失败。
2. QLD 必须从已治理的 FMP price provider 单独抓取到隔离 research input package；不得静默写入
   canonical `data/raw/prices_daily.csv`。
3. 合并后的五资产 research panel 必须以 expected tickers
   `SPY/QQQ/SGOV/TQQQ/QLD` 再走 canonical data-quality code path。
4. 任一 missing session、duplicate key、non-finite/non-positive price、source hash drift、
   date-range drift 或 prospective row 都必须 fail closed。
5. 评估 artifact 必须绑定输入、政策和实现源码 hash；独立 validator 从 source 重建全部指标和
   中文 Markdown。

### 2026-07-25 实际门禁结果

- 已在 canonical checkout 执行
  `aits validate-data --as-of 2026-07-24`，状态为 `FAIL`；
- canonical receipt：
  `outputs/data_quality/executions/dq_execution_5951b5b0db13728e99d6de0bace226f543d3756a95e193cfed49c9ea24c382cb/receipt.json`；
- 唯一 blocker code：`prices_non_market_session_date`，共 31 行；
- 逐 ticker 诊断确认 31 行全部属于 `^VIX`，`SPY/QQQ/SGOV/TQQQ` 没有该类行；
- 该 blocker 已由
  `DATA-GOV-001_D0B2B_CANONICAL_DAILY_ACCEPTANCE_REMEDIATION` 治理，当前退出条件是下一合法
  provider-ready XNYS date 通过唯一 canonical daily-run；
- 按 no-silent-workaround 和 required data-quality gate，真实 QLD 下载与指标计算已在门禁失败后
  停止。除非 canonical strict PASS，或 Owner 明确接受有记录的 scoped research-only panel
  workaround，本任务不得生成 QLD 评估结论。

### Owner 批准的 scoped research-only 例外

Owner 随后明确要求“不想阻塞 QLD 的评估”，形成：
`owner_decision:TRADING-2459:2026-07-25:approve_scoped_five_asset_dq_to_avoid_qld_block`。

- 原因：全局唯一 blocker 的 31 行全部属于本评估不消费的 `^VIX`；
- 行为影响：QLD 评估改为只构建并校验 `SPY/QQQ/SGOV/TQQQ/QLD` 与同期 rates 的隔离
  source snapshot；
- 风险：scoped PASS 不证明 canonical full cache PASS，不覆盖 `^VIX`，不能被其他 consumer、
  promotion、paper-shadow、production 或 broker 路径复用；
- 验证：实际五资产 panel 仍调用同一 `validate_data_cache` 实现并 fail closed，绑定 source/
  panel/rates/DQ report/request-cache SHA-256；source package 构建时和每次 experiment
  重建时都直接调用同一质量实现，content-derived validator 重建全部指标和 Markdown；
- 退出条件：`DATA-GOV-001_D0B2B` 取得下一合法 canonical strict PASS 后，用相同冻结政策重跑并
  替代或确认 scoped 结论；
- canonical cache 不允许因本例外被修改，报告必须同时披露全局 DQ=`FAIL` 与 scoped DQ 状态。

实际 scoped DQ=`PASS_WITH_WARNINGS`，0 error / 1 warning。唯一 warning
`prices_adjustment_ratio_jump` 对应 QLD 官方 2:1 forward split：

- 2021-05-25，ProShares 官方公告；split 日 raw close return 约 `-49.87%`，adjusted-close
  return 为 `+0.2556%`；
- 2025-11-20，ProShares 官方公告；split 日 raw close return 约 `-52.36%`，adjusted-close
  return 为 `-4.7097%`。

因此 warning 被 scoped policy 解释为已核验 corporate action，而不是收益路径异常；不借此修改
全局 `config/data_quality.yaml`，报告继续保留 `PASS_WITH_WARNINGS`。

## 结论映射

- `QLD_ELIGIBLE_FOR_OWNER_ACTION_UNIVERSE_REVIEW`：数据与重建验证 PASS，且 QLD 在至少一个
  预注册 cadence 上相对两种 current-universe 实现处于 Pareto non-dominated frontier，并提供
  至少一项明确的外部 turnover、cost-adjusted return、drawdown 或 worst-window改善。机械
  判定要求其在同一 cadence 至少一个预注册 Pareto objective 上严格优于两种 comparator。
- `QLD_NO_INCREMENTAL_IMPLEMENTATION_VALUE`：QLD 在全部预注册 cadence 上均被现有实现
  Pareto dominated，且没有独立操作优势。
- `QLD_MIXED_EVIDENCE_KEEP_RESEARCH_ONLY`：不满足上述两类，或优势只集中于单一切片/
  单一指标。
- `BLOCKED_DATA_QUALITY_OR_COVERAGE`：数据、共同窗口或 lineage 不合格。

Pareto 比较使用原始有限数值，不设结果可见后的容差或加权 composite score；仅允许
`1e-12` 浮点比较 epsilon，理由是数值稳定而非投资门槛。

评估本身不自动修改 action universe。Owner 后续决定已把 QLD 加入角色受限的
implementation universe，但没有把它加入 primary action asset / signal / style / free candidate
universe，也没有授权自动执行。

## 2026-07-25 实际评估结论

机械结论为 `QLD_ELIGIBLE_FOR_OWNER_ACTION_UNIVERSE_REVIEW`；这表示 QLD 有可复核的增量
实现价值，不表示它在所有 cadence / slice 全面胜出。Owner 已在读取该结论后批准受限
implementation 角色；机械状态保留不改写，以区分“研究结果”与“Owner disposition”。

- 实际范围：`2021-02-22..2026-07-21`，五资产共同 1,359 个价格 sessions；
- scoped DQ：`PASS_WITH_WARNINGS`，0 error / 1 个已由 ProShares 官方 split 公告解释的
  warning；global canonical full-cache DQ 仍为 `FAIL`；
- full-primary buy-and-hold：QLD total return `198.21%`，高于
  QQQ/TQQQ 50/50 的 `172.97%` 和 SGOV/TQQQ 1/3–2/3 的 `152.33%`；
- weekly / monthly / daily 下，QQQ/TQQQ 50/50 的 total return 分别比 QLD 高约
  `4.15 / 3.52 / 8.67` percentage points，但需要 `4.21 / 2.53 / 7.96` 倍 one-way
  turnover；QLD 只有统一初始建仓 turnover=`1.0`；
- QLD cost drag 为 `0.119` percentage point；QQQ/TQQQ 在 weekly / monthly / daily
  分别约 `0.510 / 0.306 / 0.978` percentage point；
- QLD full-primary max drawdown 为 `-63.66%`，说明它仍是高风险 2x 工具，加入 universe
  不能解释为风险降低；
- QLD 对 QQQ 的 realized daily beta=`1.9990`、correlation=`0.99970`；TQQQ 对 QQQ 的
  realized beta=`2.9701`。QLD 的直接 2x 暴露更贴近目标，但仍有约 `-5.07%` 的
  arithmetic annualized daily-target residual bias；
- calendar-year / AI-cycle slices 为 mixed：QLD 的收益优势并非稳定存在，不能用 full-primary
  单一收益排名把它写成新的 Alpha。

Owner disposition：QLD 被批准进入**角色受限的 2x execution / implementation universe**，
但不得作为独立 strategy style、signal input 或自由 candidate dimension。只有当独立趋势模型
先确认 Nasdaq-100 处于可信上升趋势、组合层先形成接近 2x 的 QQQ-equivalent target，且风险门
通过时，执行层才可在 QLD 与现有 QQQ/TQQQ/SGOV 实现之间按治理后的 turnover、cost、
tracking 和风险规则选择；不得按本次历史收益动态挑选工具。

该决定尚未定义“可信上升趋势”、“接近 2x”的数值容差、risk-gate binding、instrument selector、
forward-shadow 验收和去杠杆/退出规则。因此当前只完成角色登记；automatic selection、
official target weights、paper-shadow、production 和 broker 继续关闭。

## 阶段与验收

|阶段|内容|退出条件|
|---|---|---|
|S0|任务登记、Owner 决策、预注册政策|结果读取前完成|
|S1|SPY reference contract 与 QLD isolated source package|Owner 已批准 scoped DQ；五资产 source/DQ/lineage PASS 后退出|
|S2|固定实现和 cadence 的 historical-seen evaluation|双构建 byte-identical|
|S3|独立 content-derived validator 与中文 owner pack|tamper tests PASS|
|S4|formal validation、任务状态和 system flow 收口|QLD scoped、report、reproducibility、contract 与 architecture tiers PASS；仓库 Full 的非本任务失败单列披露|
|S5|Owner role disposition 与治理配置|role-limited implementation role进入policy/decision protocol；signal/candidate/automatic execution/weights边界测试PASS|

## 验证与非阻塞边界

- Owner disposition v2 固定时间双构建：primary、summary、Markdown、envelope、run-ledger
  全部 byte-identical，content-derived validation=`0 errors`；
- v2 primary SHA-256：
  `c8fe91eaddbdae3eba15786211bc34313874520e39a253f489814d11e10daec3`；
- v2 owner-role / runner / architecture focused：`31 passed`；
- report-validation：`57 passed`；
- reproducibility：`23 passed`；
- contract-validation：`275 passed`，runtime artifact
  `outputs/validation_runtime/contract-validation_20260725T101417Z/test_runtime_summary.json`；
- architecture-fitness：`619 passed`，runtime artifact
  `outputs/validation_runtime/architecture-fitness_20260725T102719Z/test_runtime_summary.json`；
- Full：`7220 passed / 4 skipped / 3 failed`，runtime artifact
  `outputs/validation_runtime/full_20260725T103158Z/test_runtime_summary.json`。

Full 的三项失败可在未修改的既有模块中独立并行复现，不依赖 QLD 输入或本任务代码：

1. `tests/test_trading2453_constraint_hit_diagnosis.py` 对 HHI 使用严格浮点相等，实际值与
   期望值仅相差约 `1.3e-17`；
2. `tests/test_artifact_validation_session.py` 的 Windows embedded-NUL 路径在
   compatibility / hardened 两个 lane 中都错误复用 validation cache。

这些发现不改变 QLD scoped DQ、指标、Pareto mapping 或 Owner 建议，也不阻塞本任务转为
research baseline；它们不是本任务获准修复的范围。由于 Full 未全绿，本工作区不执行
commit / push，待独立 owner 授权修复或基线处置后再完成工程交付收口。

2026-07-26 clean-main集成更新：上述严格浮点相等与Windows embedded-NUL cache失败均已由
后续reviewed main修复，main最新formal Full=`7268 passed / 3 skipped / 643 warnings`。
旧worktree的37项在途内容已冻结为本地取证快照`95a26bcac`，并从
main=`3e58b2c6d`重放到`codex/trading-2458-2460-integration`。当前阻塞已从“既有Full失败”
解除，剩余退出条件是重新生成当前共享authority、执行本集成的focused/formal/required Full、
commit/push并完成旧worktree证据审计和清理；QLD安全与角色边界不变。

2026-07-26 formal closeout：focused=`100 passed`、report-validation=`57 passed`、
reproducibility=`23 passed`、contract-validation=`275 passed`、
architecture-fitness=`648 passed`、integration=`995 passed`、required
Full=`7281 passed / 4 skipped / 643 warnings`。Full runtime artifact=
`outputs/validation_runtime/full_20260725T185736Z/test_runtime_summary.json`。
工程集成commit=`0f585879650f3433008bbbfbbaf52f47dba1ae15`已进入并推送
`main`/`origin/main`；QLD仍仅为role-limited implementation instrument，未生成官方权重，
未开放automatic execution、paper-shadow、production或broker。

## 临时工作区生命周期

- owning task：`TRADING-2459_STRATEGY_STYLE_DISCOVERY_SPY_QLD_UNIVERSE`；
- path：`D:\Work\AITradingSystem-TRADING-2459-style-discovery`；
- purpose：隔离当前 shared checkout 中 OPS-070/DEVX-001 的未提交修改；
- exit condition：本任务已验证、必要证据进入受治理位置、commit/push 完成或尝试被明确放弃；
- cleanup：退出前检查 tracked/untracked/ignored 内容，确认无唯一证据和活动进程后使用
  `git worktree remove`，随后 `git worktree prune`。
- 2026-07-26 closeout：37项在途内容已在clean-main重放、验证并随上述commit推送；8组formal
  evidence完成逐文件SHA-256迁移。旧worktree经tracked/untracked/ignored与活动进程审计后按
  exact allowlist删除，目录及Git registration均已消失；实现可由`main`/remote恢复，本地取证
  checkpoint=`95a26bcac`保留旧在途字节的恢复边界。

## 安全边界

- ARCH-004 feature-freeze exception 已以本任务 Owner 决策登记；实现必须复用
  `research_framework` experiment/plugin，不得新增 god-module CLI、第二套 runner 或 task-shaped
  report control plane；
- `research_only=true`；
- `historical_seen_only=true`；
- `prospective_source_rows_observed=true`，且该 visibility 事件已记录；
- `prospective_values_used_in_evaluation=false`；
- evaluator 在任何日期截断之前检查 source panel；panel 中只要存在
  `2026-07-21` 之后的行即 fail closed，不允许静默过滤；
- `candidate_search_executed=false`；
- `strategy_logic_changed=false`；
- `official_action_universe_changed=false`；
- `official_primary_action_universe_changed=false`；
- `role_limited_implementation_universe_changed=true`；
- `qld_role_limited_2x_implementation_approved=true`；
- `qld_automatic_execution_allowed=false`；
- `paper_shadow_changed=false`；
- `production_effect=none`；
- `broker_action=none`。
