# TRADING-1506～1525 Expanded QQQ / SGOV / TQQQ Allocation Search

最后更新：2026-06-27

## 状态

`VALIDATING`

## 背景

TRADING-1486～1505 已把原 dynamic strategy 主线收口为 closeout / disposition
review。当前最稳健的 `limited_adjustment` actual-path 策略主要在两个固定
QQQ/SGOV 档位之间低频切换，无法回答 QQQ / SGOV / TQQQ 三资产组合空间是否
存在更好的静态或动态候选。

本批任务重新打开 expanded allocation research，但只作为 research-only evidence。
TQQQ 可进入研究宇宙，不得自动进入 promotion universe、paper-shadow、production
或 broker 路径。

## 安全边界

- 所有输出默认为 `research_only`、`candidate_only`、`actual_path_required`。
- dynamic promotion 固定 `BLOCKED`。
- target-path metrics 只能作为 diagnostic。
- TQQQ 使用真实 adjusted close，不允许用 `3 * QQQ daily return` 合成。
- 含 TQQQ 结论必须披露 QQQ-equivalent exposure 和 TQQQ 风险归因。
- actual-path leaderboard 才能用于候选排序。
- owner review 前不得进入 paper-shadow、production 或 broker action。

## 阶段拆解

|任务|阶段|状态|验收标准|
|---|---|---|---|
|TRADING-1506|Expanded universe scope / policy skeleton|DONE|新增 scope 文档与 `config/research/expanded_allocation_universe.yaml`。|
|TRADING-1507|TQQQ data quality blocking review|DONE|新增 formal TQQQ review；默认只允许 research-only。|
|TRADING-1508|Static simplex grid generator|DONE|生成 `static_simplex_grid_*` index、metrics、frontier、risk bucket summary。|
|TRADING-1509|Static frontier review|DONE|生成 tracked review 文档和 YAML matrix。|
|TRADING-1510|Risk bucket representatives|DONE|按 QQQ-equivalent exposure 输出代表组合。|
|TRADING-1511|State portfolio candidates|DONE|从 frontier / bucket reps 生成 state-to-portfolio candidates。|
|TRADING-1512|Monotonic risk classifier|DONE|为每个动态候选记录 exposure ordering 和 violations。|
|TRADING-1513|Expanded actual-path rebacktest|DONE|输出 actual-path leaderboard、target diagnostic、gap、TQQQ risk、promotion readiness。|
|TRADING-1514|TQQQ risk attribution|DONE|生成 TQQQ risk attribution docs/YAML。|
|TRADING-1515|Same-risk baseline comparison|DONE|按 exposure/volatility/drawdown bucket 比较 dynamic vs static baseline。|
|TRADING-1516|Candidate survival matrix|DONE|输出 allowed verdicts，promotion 继续 blocked。|
|TRADING-1517|Overfit guardrail|DONE|记录 searched count、rank、split metrics、stability score。|
|TRADING-1518|Walk-forward validation|DONE|生成 walk-forward review artifact；不足时 fail closed 为 blocker。|
|TRADING-1519|Net-of-cost review|DONE|披露 cost / turnover 影响。|
|TRADING-1520|Stress risk review|DONE|披露 TQQQ 和高 exposure stress risk。|
|TRADING-1521|Promotion guardrail|DONE|新增测试防止 expanded evidence 解禁 promotion。|
|TRADING-1522|Registry and catalog|DONE|更新 report registry、artifact catalog、system flow、task register。|
|TRADING-1523|Owner review pack|DONE|回答 owner 六个核心问题，默认 `promotion_status=BLOCKED`。|
|TRADING-1524|Validation|VALIDATING|运行 focused parallel pytest、Ruff、compileall、diff checks。|
|TRADING-1525|Commit summary|PENDING|验证通过后本地提交并按上游状态决定是否 push。|

## 验收标准

- Expanded QQQ / SGOV / TQQQ universe 被正式建模。
- TQQQ 数据质量 warning 被升级为 formal review，promotion universe 仍 blocked。
- 静态三资产 simplex frontier 可由 CLI 生成。
- 动态 state-to-portfolio candidate 不再局限于 65/35 和 45/55。
- actual-path research rebacktest 输出可审计 leaderboard。
- 含 TQQQ 策略披露 TQQQ risk attribution。
- 动态候选与同风险静态 frontier 比较。
- dynamic promotion、paper-shadow、production、broker action 全部继续禁止。

## 进展记录

- 2026-06-27：新增并进入 `IN_PROGRESS`。本批承接 dynamic strategy closeout 后的 expanded universe research；目标是建立 QQQ / SGOV / TQQQ static frontier、state-to-portfolio dynamic candidates、actual-path-only rebacktest、TQQQ formal review、same-risk comparison 和 owner review pack。所有输出固定 research-only，dynamic promotion 继续 `BLOCKED`。
- 2026-06-27：实现完成并进入 `VALIDATING`。新增 expanded universe module/CLI、policy config、scope/TQQQ review、static simplex grid/frontier、risk bucket representatives、state portfolio candidates、actual-path rebacktest、TQQQ attribution、same-risk comparison、survival/walk-forward/net-cost/stress reviews、owner pack、registry/catalog/system-flow updates 和 focused tests。真实 run 使用 `ai_after_chatgpt`、default start `2022-12-01`；static grid size=231、frontier rows=509、actual-path strategy count=11、surviving candidate count=0、data_quality_status=`PASS_WITH_WARNINGS`；dynamic promotion 继续 `BLOCKED`，paper-shadow/production/broker 全部禁止。
