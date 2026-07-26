# TRADING-2460：Decision Target Capability Audit 第一批 Label Foundation

最后更新：2026-07-26

状态：`BASELINE_DONE_REAL_LABEL_FOUNDATION_READY`

稳定任务 ID：`TRADING-2460_DECISION_TARGET_CAPABILITY_AUDIT_LABEL_FOUNDATION`

Owner 决策：
`owner_decision:TRADING-2460:2026-07-25:approve_decision_target_capability_audit_batch1_label_foundation`

## 目标

本批建立 Strategy Style Discovery 的 decision-target label 基础，回答后续模型**应该预测什么**，
而不是寻找新策略、权重或 candidate。第一批只允许：

1. 冻结目标、窗口、horizon、return basis、数据角色和安全边界；
2. 从受治理 adjusted-close total-return source 构建可重建 label package；
3. 由独立 content-derived validator 重建全部 label、summary、commitment 和中文 Markdown。

本批不得训练模型、读取 feature-family score、搜索参数、运行 strategy candidate backtest、生成
target weights、改变 action universe、访问 prospective untouched、进入 paper-shadow、promotion、
production 或 broker。

## 决策目标

### Primary target

`QQQ_MINUS_SGOV`：

```text
QQQ forward total return - SGOV forward total return
```

它表示在相同起点和 horizon 下承担 Nasdaq-100 风险相对持有可交易防御资产 SGOV 的 gross
机会差；它不是 alpha、risk-adjusted utility 或自动 risk-on 信号。

### Diagnostic decomposition

- `SPY_MINUS_SGOV`：broad equity risk premium diagnostic；
- `QQQ_MINUS_SPY`：Nasdaq / growth leadership diagnostic。

必须逐行满足：

```text
QQQ_MINUS_SGOV == SPY_MINUS_SGOV + QQQ_MINUS_SPY
```

允许数值稳定 epsilon，但不得用 epsilon 改写投资判断。SPY 保持 reference / regime-control 角色，
不因本诊断成为 action-weight asset。

### Fixed horizons

- `1d`
- `5d`
- `10d`
- `20d`

每个 horizon 表示从 decision-date adjusted close 到后续第 h 个共同交易 session adjusted close 的
复合 total return。不得根据结果选择或删除 horizon；新增 60d/120d 必须新版本和 Owner review。

## Label contract

机器可读 row 至少包含：

- `decision_date`
- `horizon_id`
- `horizon_sessions`
- `label_start_date`
- `label_end_date`
- `label_available_on_session`
- `qqq_forward_total_return`
- `spy_forward_total_return`
- `sgov_forward_total_return`
- `qqq_minus_sgov`
- `spy_minus_sgov`
- `qqq_minus_spy`
- QQQ / SPY / SGOV 各自的 `future_max_drawdown`
- QQQ / SPY / SGOV 各自 horizon 内的 `worst_1d_return`

收益使用相同 source panel 的 adjusted close。`label_start_date` 是 decision date 后第一个共同
session，`label_end_date` 是第 h 个共同 session；`label_available_on_session` 等于 label end
session，表示该 session close publication 之前标签不可使用。

Future max drawdown 从 decision-date close 的 wealth=`1.0` 开始，沿后续 h sessions adjusted-close
路径计算；worst 1d return 只在该 forward interval 内计算。风险字段保持独立诊断，不得和 excess
return 合成为本批 score。

## 日期、数据角色与可见性

- active primary research start：`2021-02-22`；
- 本批是 `HISTORICAL_SEEN_LABEL_FOUNDATION`，不得声称 unbiased OOS；
- requested source end 由运行时 `as_of` 明确记录；
- 各 horizon 的实际最后 decision date 由 source end 和 label maturity 机械决定；
- source 中如存在 `as_of` 后价格、非共同 session、duplicate key、non-finite/non-positive
  adjusted close 或 ticker 缺失，必须 fail closed；
- protocol freeze 后的未来 observation 另立 batch，不在本批访问。

V1 历史 package 因 31 条 `^VIX` non-session 行在全局 DQ `FAIL` 处 fail closed；TRADING-2459
的 QLD scoped exception 仍不可复用。2026-07-26 Owner 通过
`owner_decision:DATA-GOV-002:2026-07-26:approve_long_term_capability_receipt_engineering_v1`
批准长期 consumer capability receipt 工程方案与本任务首个 pilot。V2 每次仍必须运行、保存并披露
full canonical DQ；只有 reviewed policy 预先冻结 price-only `QQQ/SPY/SGOV`、空 rate scope、
window、fields 与
consumer identity，所有 global ERROR 都有 structured `affected_instruments`、被 policy 明确允许且
与 required scope 完全不相交，同时 exact projection 使用同一 `validate_data_cache` 得到 strict
`PASS` 时，才允许生成 capability receipt 与 label panel。

V2 capability PASS 不得声称 canonical full-cache PASS，不授权 daily operation、其他 consumer、
D0B2B、model、strategy、paper-shadow、production 或 broker。任何 manifest/publication/schema/
parse、unknown-scope、required-scope 或 scoped warning/error 仍输出 `BLOCKED_DATA_QUALITY_OR_SOURCE`。

## Purge / embargo readiness contract

本批不训练模型，但必须输出未来 split 可消费的 label interval：

```text
[label_start_date, label_end_date]
```

后续 purged walk-forward 必须：

- 删除任何 label interval 与 test interval 相交的 train row；
- 只有 `label_available_on_session` 已不晚于 fold train cutoff 的 row 才可训练；
- embargo 的 session 数和适用边界由下一批 reviewed split policy 明确定义；
- 本批不得预设一个未治理的固定 embargo 数值或生成模型 fold。

## Source package 与审计

输入 package 必须记录：

- provider、endpoint、request parameters；
- download/capture timestamp；
- ticker、requested/evaluated date range、row count；
- source path、size、SHA-256；
- DQ policy、DQ report、receipt 或 scoped exception commitment；
- project policy、implementation source 和 requirement commitment。

正式 artifact 至少包括：

- primary label package JSON；
- row-level JSONL 或等价 content-addressed table；
- compact summary JSON；
- artifact envelope / run ledger；
- 中文 research report；
-独立 validation result。

Validator 必须从冻结 source package 重建 rows、summary、Markdown 和 commitment，拒绝：

- source/path/hash/row-count drift；
- decision/label date tamper；
- return、excess return、drawdown、worst-return tamper；
- target identity、horizon、decomposition、summary tamper；
- safety / DQ / window / role tamper；
- prospective row 或 `as_of` 后 row。

## 阶段与退出条件

|阶段|内容|退出条件|
|---|---|---|
|S0|任务、需求、Owner 决策和架构例外登记|范围与安全边界在实现前完成|
|S1|policy manifest 与 label schema|target/horizon/date/return/purge-readiness 合同可解析并有治理元数据|
|S2|source loader、DQ gate、label builder|真实 DQ fail closed；fixture package 可生成完整四 horizon rows|
|S3|content-derived validator 与报告|双构建 byte-identical；source/label/safety tamper tests PASS|
|S4|report registry、artifact catalog、system flow 与正式验证|focused、report/reproducibility、contract/architecture 和所需 Full 按风险通过|

## 安全边界

- `research_only=true`
- `historical_seen_only=true`
- `model_training_executed=false`
- `feature_family_scoring_executed=false`
- `candidate_search_executed=false`
- `parameter_search_executed=false`
- `strategy_backtest_executed=false`
- `target_weights_generated=false`
- `action_universe_changed=false`
- `prospective_accessed=false`
- `paper_shadow_changed=false`
- `promotion_allowed=false`
- `production_effect=none`
- `broker_action=none`

## 进展记录

- 2026-07-26：Owner批准DATA-GOV-002长期capability receipt工程线与TRADING-2460首个pilot；
  新增v2 label policy、reviewed `decision_target_label_core` capability policy、typed/content-derived
  receipt、immutable source capture、structured affected-instrument attribution、content-bound
  requested-window authority、source projection/verifier与tamper tests。V1历史BLOCKED证据不改写。
- 2026-07-26：首个真实 pilot 证明 label builder 只消费价格；初次将 DGS3MO 纳入 required
  scope 时 scoped DQ 以 `rates_empty` 正确 fail closed，随后按真实 transitive dependency
  closure 将 v2 policy 修正为 price-only QQQ/SPY/SGOV、空 rate scope。
- 2026-07-26：真实 `2021-02-22..2026-07-24` v2 build 得到 capability receipt
  `dq_capability_e7f233ca6e0c41ce9506df46f067e56348004a19f953cf74626d5c9936ccb059`。
  Full canonical DQ=`FAIL`，唯一 ERROR=`prices_non_market_session_date`、affected=`^VIX`；
  scoped QQQ/SPY/SGOV DQ=`PASS`，global PASS claim=false。Label status=
  `LABEL_FOUNDATION_READY`，common sessions=`1362`、rows=`5412`，四个 horizon 分别为
  1d=`1361`、5d=`1357`、10d=`1352`、20d=`1342`，content-derived validator 0 errors。
  DATA-GOV-002 Phase A 已完成 formal validation 收口：architecture=`654 passed`、
  report=`57 passed`、reproducibility=`23 passed`、contract=`275 passed`、
  integration=`995 passed`、Full failure-fix rerun=`7292 passed / 3 skipped / 643 warnings`；
  Full artifact=`outputs/validation_runtime/full_20260726T022008Z/test_runtime_summary.json`。
  下一步由 strategy research owner 预注册 Batch 2 split/model ladder/metrics。
- 2026-07-26：clean-main formal closeout通过：focused=`100 passed`、
  report-validation=`57 passed`、reproducibility=`23 passed`、
  contract-validation=`275 passed`、architecture-fitness=`648 passed`、
  integration=`995 passed`、Full=`7281 passed / 4 skipped / 643 warnings`；
  Full artifact=`outputs/validation_runtime/full_20260725T185736Z/test_runtime_summary.json`。
  这只完成工程交付；canonical DQ未修复，真实labels仍为
  `BLOCKED_DATA_QUALITY_OR_SOURCE`。
- 2026-07-26：既有Full三项无关失败已在后续reviewed main修复；本任务与2458/2459的37项
  在途内容已冻结为本地取证快照`95a26bcac`，并从main=`3e58b2c6d`重放到
  `codex/trading-2458-2460-integration`。当前等待共享authority重建和required Full；真实
  label READY仍独立受`prices_non_market_session_date` canonical DQ blocker约束。
- 2026-07-25：Owner 同意先做第一批。任务进入 `IN_PROGRESS`；先冻结合同与实现最小 label
  foundation。正式数据 run 仍受 canonical DQ blocker 约束，不复用 QLD scoped exception。
- 2026-07-25：完成 v1 policy、typed ExperimentSpec、canonical-DQ-first source package、
  QQQ/SPY/SGOV 四 horizon label builder、未来路径左尾诊断、purge/maturity metadata、
  content-derived validator、中文报告、registry/catalog/system-flow 和架构清单。
- 2026-07-25：clean fixture 双构建与 tamper tests PASS；focused `6 passed`，report-validation
  `57 passed`，reproducibility `23 passed`，contract-validation 修复 deprecation inventory
  当前态 ratchet 后 `275 passed`，architecture-fitness 刷新 DevEx manifest 后 `619 passed`。
  Ruff、compileall、task-shadow 和 DevEx validation 均 PASS。
- 2026-07-25：真实 `2021-02-22..2026-07-24` canonical full-cache gate 读取 prices、rates、
  download manifest 和 Marketstack secondary，结果 `FAIL`、1 error、0 warnings，唯一 blocker
  为 `prices_non_market_session_date`；因此 `panel_materialized=false`，正式 report status 为
  `BLOCKED_DATA_QUALITY_OR_SOURCE`。同一冻结输入的 primary/summary/Markdown/envelope/ledger
  五项产物双构建 byte-identical，content-derived validator 0 errors。
- 2026-07-25：本批以 `BASELINE_DONE` 收口，表示标签合同和实现已完成，不表示真实 labels
  READY。下一步由 DATA-GOV-001_D0B2B 修复 canonical VIX session 数据并取得 strict PASS，
  然后按同一 v1 policy 重建 source package；不允许复用 QLD scoped exception。由于同一
  worktree 还包含 TRADING-2459/2458 的未提交变更且既有 Full 已记录 3 个无关失败，本批不另行
  commit/push，也不把已知失败改写为 PASS。
