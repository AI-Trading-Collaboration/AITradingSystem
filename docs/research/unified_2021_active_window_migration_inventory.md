# 统一 2021 活跃研究窗口迁移清单

最后更新：2026-07-21

关联任务：`TRADING-2452_UNIFIED_2021_PRIMARY_WINDOW_AND_CLEAN_RUN`

## 结论

`2021-02-22` 是后续活跃策略研究、主回测和主要结论的唯一默认起点。
`2022-12-01` 不再是 active default、primary conclusion boundary、required comparator 或
minimum allowed start。它只可保留在不可变历史证据、旧 schema/compatibility snapshot 和对旧结论的
说明中，且这些内容不得被新 run 当作 primary evidence。

本次初始扫描在排除 outputs 与 completed task view 后仍发现 630 个文本引用：

|区域|引用文件数|迁移处理|
|---|---:|---|
|`AGENTS.md`|1|迁移 active project rule|
|`config/`|90|逐项区分 active default、legacy fixture 与 immutable compatibility；active 必须迁移|
|`src/`|59|迁移 runtime default、minimum-date guard、CLI default/help 和 active report wording|
|`tests/`|71|随 owned runtime contract 更新；历史 fixture 仅在明确 legacy case 中保留|
|`docs/`|322|active architecture/runbook/system-flow 迁移；历史报告与状态记录保留|
|`inputs/`|87|immutable evidence/compatibility snapshot 默认保留；新 package 使用新 id/hash|

文件数不是替换次数，也不是允许机械全局替换的依据。

## 分类规则

### `MIGRATE_ACTIVE`

满足任一条件即必须迁移：

- 决定新命令未传日期时的默认起点；
- 阻止 `2021-02-22` 作为合法开始日期；
- 报告把 `2022-12-01` 描述为默认、主结论或强制比较边界；
- data-quality、scheduler、backtest、research campaign 或 selector 以旧日期裁剪 active input；
- tests 对上述 active 行为作旧值断言。

首批 source-of-truth 包括：

- `AGENTS.md`；
- `config/market_regimes.yaml`；
- `config/research/primary_research_window_policy.yaml`；
- `config/research/research_window_registry.yaml`；
- `config/research/dynamic_walk_forward_policy.yaml`；
- `config/data_quality.yaml` 及 active ETF data-quality policy；
- active research/execution policies 的 `default_backtest_start`；
- runtime shared date constants、minimum-date validators 和 CLI defaults。

### `VERSION_AND_REBUILD`

冻结内容不得原地改写，必须新建版本并重新生成 consumer evidence：

- TRADING-2451 preregistration package；
- ARCH-004 semantic glossary 与 compatibility baseline 中冻结的旧 active contract；
- checksum/source-hash 绑定的 generated manifests；
- 任何把旧 policy bytes 纳入 lineage 的 artifact。

旧版本继续可读，但 active resolver 必须选新版本，且报告说明 supersession。

### `RETAIN_HISTORICAL`

以下引用保留原值：

- 已生成报告中的实际 requested/actual range；
- requirement/status progress 中对历史运行的事实记录；
- legacy comparison fixtures、negative tests 与 immutable source commitments；
- task completed archive 与历史 compatibility snapshot。

保留不等于 active allowlist。新代码不得从 retained historical payload 推导 active default。

## 第二轮 active-path 审计与处置

首版迁移后再次按“会不会影响新 run”扫描，而不是只搜索统一字段名。该轮发现
`default_start`、`default_start_date`、CLI 单日默认、缺省 requested-range、report fallback 和
worker 内部日期过滤仍可能把新 run 截到 2022。已直接迁移以下 active 类别：

- ETF 通用 backtest、controlled strategy、AI leadership、event/regime/risk/exposure、
  liquidity/rates、first-layer、expanded allocation、indicator registry 与 research protocols；
- controlled benchmark 同时新增 `unified_primary_2021_full`，原
  `ai_after_chatgpt_full` 仅作为明确 historical comparison 保留；
- decision snapshot、trace、Reader Brief、governance summary、daily lineage card、CLI date-range、
  research campaign 与 executable-research 缺省 range；
- `current_subscription_qualification` 和 `controlled_strategy_batch` 的真实数据过滤及 active
  summary fallback；
- targeted weight search 新增 `weight_search_targeted_v2.yaml`，active resolver 切换到 v2；v1
  原文件保留历史复现，其 `2022-12-01` minimum 是机器 allowlist 中唯一该字段例外。
- paper-shadow 初始化与 historical backfill 分别新增 `paper_shadow_account_v2.yaml` 和
  `paper_shadow_backfill_v2.yaml`，active runtime/CLI resolver 统一切到 v2；原 v1 bytes 不改，
  只用于既有 artifact replay。v2 起点为`2021-02-22`，backfill 的 account source 也指向 v2。

明确保留的源码/配置必须满足可审计历史角色：`ai_after_chatgpt` comparison、2022 bear/high-rate
stress、旧 B2 / growth-tilt 实际 requested range、validation sample、paper-shadow/backfill v1、
asset membership/lifecycle 或 immutable artifact commitment。保留项不得供应 active default；
`AI_REGIME_START` 兼容 alias 只指向 `AI_CYCLE_COMPARISON_START`。

新 guard 扫描 `default_backtest_start`、`default_decision_start`、
`default_evaluation_start`、`default_start`、`default_start_date` 和
`minimum_requested_start_date`，并拒绝 `ai_after_chatgpt + 2021` 混合语义。对 requested-window、
protocol、controlled primary/historical segments 和 active targeted v2 另作结构化断言。

## `2024-12-31` 截止点处置

旧 dynamic walk-forward 把 2025 定义为 locked holdout，因此 TRADING-2451 历史 folds 截止
`2024-12-31`。2025 outcome 现已可见，旧 R1 也已披露 holdout contamination，所以该截止点不能再
提供 outcome-blind 性。

新口径为：

- 六个完整半年 test folds 覆盖至 `2025-12-31`；
- `2026-01-02` 至新 preregistration freeze 前最后完整交易日只作 recent-known diagnostic；
- `2026-07-22..2027-07-21` 保持 prospective untouched holdout，本轮不访问。

## 迁移退出门槛

1. active config/runtime/CLI/report contracts 不再以 `2022-12-01` 为默认或下限；
2. active-vs-historical guard 能阻止新引用重新进入 active scope；
3. 新 TRADING-2452 package 与 validator PASS，旧 TRADING-2451 bytes 不变；
4. historical evaluator 先过 runtime DQ，且不读取 prospective holdout；
5. system flow、architecture glossary、compatibility/deprecation/manifests 与 task views 同步；
6. focused、architecture、contract 和风险相称的 Full validation PASS；
7. 所有输出保持 `production_effect=none`、`broker_action=none`。
