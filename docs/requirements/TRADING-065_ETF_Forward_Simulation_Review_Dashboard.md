# TRADING-065 ETF Forward Simulation Review Dashboard

状态：VALIDATING

最后更新：2026-06-01

## 背景

TRADING-064 已完成 ETF calibration experiment pack、candidate selection、shadow
enrollment、weekly review 和 final experiment validation gate。TRADING-065 在此
基础上把历史实验候选推进到 forward shadow observation：只观察候选在真实后续
市场数据中的表现，不改变 production ETF weights，不触发 broker action，也不
自动 promotion。

## 范围

- 加固 `data/simulation/etf_shadow_candidates.json` 的 schema、生命周期状态和
  safety fields。
- 新增 `aits etf forward ...` 命令组，覆盖 daily update、dashboard、rolling
  metrics、weekly review、watchlist 和 final validation gate。
- 把 forward dashboard / weekly review / watchlist / validation 纳入 report
  registry、Reader Brief 和 system flow。
- 所有产物固定 `observe_only=true`、`production_effect=none`、
  `broker_action=none`、`manual_review_required=true`。

## 非目标

- 不接入 broker execution、真实账户交易或 options execution。
- 不自动替换 baseline，不自动把 candidate promotion 到 production。
- 不让 forward return 或 evaluation-only 字段进入 decision-time 输入。

## 阶段拆解

|任务|状态|验收要点|
|---|---|---|
|TRADING-065A Shadow Candidate State Store Hardening|DONE|shadow state schema、safety validation、deterministic write、legacy migration|
|TRADING-065B Daily Forward Performance Updater|DONE|active candidate forward update、baseline/benchmark comparison、evaluation-only fields|
|TRADING-065C Candidate vs Baseline vs Benchmark Dashboard|DONE|JSON/Markdown dashboard、safety banner、no-active/insufficient-data handling|
|TRADING-065D Rolling Window Review Metrics|DONE|5D/20D/60D rolling metrics and null reasons|
|TRADING-065E Candidate Lifecycle Status Rules|DONE|config-driven status transitions; no production promotion|
|TRADING-065F Weekly Review Markdown / HTML Report|DONE|readable weekly forward review with allowed actions only|
|TRADING-065G Reader Brief Forward Simulation Section|DONE|daily Reader Brief summary and dashboard link|
|TRADING-065H Alert / Watchlist Summary|DONE|local watchlist JSON/Markdown, no external alert or broker action|
|TRADING-065I Final Forward Simulation Validation Gate|DONE|fail-closed validation for schema, reports, safety, and no-lookahead separation|

## 验收标准

- `aits etf forward validate` 输出 `PASS`。
- 全量 pytest、ruff、compileall 和 `git diff --check` 通过。
- Runtime artifacts 保持在 ignored runtime 目录。
- Documentation、artifact catalog、system flow、report registry 和 task register 同步。
- `production_effect=none`、`broker_action=none`、`manual_review_required=true`、
  `production_promotion_allowed=false` 在所有 forward 输出可见。

## 进展记录

- 2026-06-01: 新增并进入 IN_PROGRESS。目标是实现 ETF shadow candidates 的真实
  forward observation dashboard/workflow；本阶段只允许 observation 和 manual review，
  不允许 production promotion 或 broker action。
- 2026-06-01: TRADING-065A~I baseline implementation 完成并进入 VALIDATING。新增
  `config/etf_portfolio/forward_simulation.yaml`、hardened shadow state schema、
  `aits etf forward update/dashboard/weekly-review/watchlist/validate`、Reader Brief
  `ETF Forward Simulation` 区块、report registry / scheduled daily ops integration、
  artifact catalog 和 system flow 更新。验证通过：`python -m pytest tests -q`
  （1747 passed）、`python -m ruff check config src tests scripts docs`、`python -m
  compileall -q src tests scripts`、`git diff --check`、`python -m ai_trading_system.cli
  etf forward validate`（PASS）。所有 forward 输出保持 `observe_only=true`、
  `production_effect=none`、`broker_action=none`、`manual_review_required=true`，
  不写 production weights、不触发 broker action、不允许 automatic production promotion。
