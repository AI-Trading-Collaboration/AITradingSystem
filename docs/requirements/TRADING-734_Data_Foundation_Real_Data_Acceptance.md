# TRADING-734 Data Foundation Real-Data Acceptance

## 状态

- 状态：VALIDATING
- 日期：2026-06-21
- 范围：validation-only / observe-only real-data acceptance for TRADING-726～733
- Owner：系统实现 + 项目 owner 后续复核

## 背景

Owner 要求在 TRADING-726～733 data foundation baseline 上开启 TRADING-734，做 representative
universe 和 representative date windows 的真实数据验收。仓库中已存在一个归档完成的
`TRADING-734_DAILY_INCREMENTAL_REFACTOR_CLI_BOUNDARY`；本任务按 owner 当前要求使用
`TRADING-734_DATA_FOUNDATION_REAL_DATA_ACCEPTANCE` 作为完整稳定 ID，以免审计时混淆。

## 安全边界

- validation-only / observe-only。
- `production_effect=none`。
- `broker_action=none`。
- `promotion_gate_allowed=false`。
- `paper_shadow_change_allowed=false`。
- `production_weight_change_allowed=false`。
- 不修改 production、paper-shadow、official weights。
- 不触发 broker、order、live trading、paper-shadow activation。
- 不放宽 PIT、data-quality、lineage gate。
- 真实 source 不足时必须 fail closed、标记 diagnostic-only，或标记 blocked_until_qualified。

## Representative Universe

- `SPY`
- `QQQ`
- `SMH`
- `MSFT`
- `GOOGL`
- `NVDA`
- `AMD`
- `TSM`
- `cash`

## Representative Date Windows

- 正常趋势窗口。
- 回撤窗口。
- 高波动窗口。
- 财报 / AI / 半导体事件窗口。
- 最近 forward-like decision date。
- 一个预期会被 PIT gate fail-closed 的日期。

具体日期由 `config/data/data_foundation_acceptance.yaml` 固定，报告必须披露实际 requested
date range 和 `ai_after_chatgpt` market regime。

## 验收范围

- PIT feature store：真实/本地可追溯 snapshot、manifest、available_time、source_manifest、
  config_hash、input_hash、current_view_only 标记、lookahead_violation_count 和 feature family
  risk classification。
- Asset master / universe：asset_id 稳定性、ticker history、tradability calendar、corporate
  action source、universe as-of query、missing / uncertain / diagnostic-only asset records。
- Cost / liquidity：代表调仓 action 的 estimated cost、turnover 单调性、cash yield / financing
  cost 版本、liquidity cap / spread proxy、gross vs net return demo。
- Regime / event / cluster labels：as_of_label 与 post_hoc_analysis_label 区分、future event
  leakage 风险、cluster label as-of 数据来源、label coverage 和 unknown label count。
- Experiment run registry：至少登记 benchmark、strategy diagnostic、reverse/oracle diagnostic
  三类 run，且每个 run 包含 dataset version、feature snapshot id、universe version、cost model
  version、label version、config hash、code version、artifact paths。
- Execution / cache / checkpoint：小型 batch、cache hit/miss、checkpoint resume、duplicate run
  dedupe、failed-run reason 分类。
- Forward evidence：dry-run capture、无 broker、daily archive、feature snapshot link、
  baseline / benchmark / candidate outputs link、future outcome append-only。
- Case library：至少一个正向用例、一个逆向构造用例、一个 oracle diagnostic case；
  oracle case `promotion_gate_allowed=false`；case 可被 strategy-pair diagnostics 查询。

## Acceptance Report 必须输出

- `pit_feature_store_status`
- `asset_master_status`
- `cost_liquidity_status`
- `label_store_status`
- `run_registry_status`
- `execution_cache_status`
- `forward_evidence_status`
- `case_library_status`
- `source_qualification_summary`
- `promotion_grade_ready_count`
- `diagnostic_only_count`
- `blocked_until_qualified_count`
- `lookahead_violation_count`
- `production_effect=none`

## 开放问题

- 本地缓存中可能存在 fixture、shadow observe 或 diagnostic artifacts；它们可以用于验收
  contract 和 fail-closed 行为，但不能被升级为 promotion-grade evidence。
- 若缺少 SEC / fundamental / valuation 的完整 as-reported source manifest，报告必须把相关
  feature family 标记为 diagnostic-only 或 blocked_until_qualified。
- 最近 forward-like date 只能做 dry-run capture；future outcome 需要后续真实时间流逝后 append。

## 进度记录

- 2026-06-21：任务登记进入 IN_PROGRESS；固定 validation-only / observe-only 安全边界和
  source qualification / fail-closed 验收要求。
- 2026-06-21：完成 acceptance runner、CLI、config/schema、report registry、artifact catalog、
  system flow 和 focused tests，进入 VALIDATING。默认 `aits data foundation-acceptance run`
  输出 `BLOCKED_UNTIL_QUALIFIED_DATA`，`promotion_grade_ready_count=0`、
  `diagnostic_only_count=2`、`blocked_until_qualified_count=3`、
  `lookahead_violation_count=0`，并保持 `production_effect=none`、`broker_action=none`、
  `promotion_gate_allowed=false`、`paper_shadow_change_allowed=false`、
  `production_weight_change_allowed=false`。该结果表示当前本地源可用于 contract / diagnostic
  acceptance，但仍不能作为 promotion-grade evidence。验证通过 compileall、Black check、Ruff、
  focused pytest、fast-unit、contract-validation 和 report-validation。
