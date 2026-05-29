# OPS-059: Unified Scheduled Task Orchestration Review

## 背景

当前系统已经具备 daily scoring、Reader Brief、SEC PIT shadow observe / monitor、research governance、backtest、parameter governance、documentation contract 和 report registry 产物。原有 `aits ops daily-plan` / `aits ops daily-run` 仍以早期 dashboard 与反馈复盘链路为中心，缺少一份统一、可审计的调度计划，容易在新增报告链时遗漏 Reader Brief 上游、SEC PIT shadow monitor 或 weekly weight/backtest governance。

OPS-059 的目标是把 daily / weekly / biweekly / monthly / ad hoc research 任务统一登记，并让每日生产调度只执行交易日 daily chain。非 daily governance/research 任务必须显式登记，但不得静默进入每日执行。

## 范围

- 新增 `config/scheduled_tasks.yaml` 作为调度链路配置源。
- 更新 `aits ops daily-plan`，使交易日计划顺序由 `config/scheduled_tasks.yaml` 校验。
- 更新 `aits ops daily-run`，按同一顺序执行并在 Reader Brief 前补齐 report index、documentation contract、research governance summary 和 SEC PIT shadow monitor。
- 新增调度 runbook，说明 daily、weekly、biweekly、monthly、ad hoc research cadence 与 closed-market 行为。
- 更新 `docs/system_flow.md` 与 README 中的每日链路说明。
- 增加测试，覆盖命令顺序、非 daily 任务隔离、closed-market 跳过和 safety invariants。

## Daily Trading-Day Chain

`daily_trading_day` 的默认顺序为：

1. `download-data`
2. `validate-data`
3. `pit-snapshots fetch-fmp-forward --continue-on-failure`
4. `pit-snapshots build-manifest`
5. `pit-snapshots validate`
6. `fundamentals download-sec-companyfacts`
7. `fundamentals extract-sec-metrics`
8. `fundamentals merge-tsm-ir-sec-metrics`
9. `fundamentals validate-sec-metrics`
10. `valuation fetch-fmp`
11. `score-daily`
12. `reports dashboard`
13. `sec-pit shadow-observe --latest`
14. `sec-pit shadow-monitor --latest`
15. `reports score-change-attribution --latest`
16. `reports market-panel --latest`
17. `reports index --latest`
18. `docs report-contract --latest`
19. `reports research-governance-summary --latest`
20. `reports reader-brief --latest`
21. `reports validate-reader-brief --latest`
22. `ops health`
23. `security scan-secrets`

## Non-Daily Cadences

Weekly 只登记 backtest、backtest robustness、parameter replay、parameter candidates、parameter governance、weight candidate evaluation、weight promotion gate 和 research governance summary review。

Biweekly 只登记 investment review、feedback loop review、shadow lane review、SEC PIT observe-only review、manual thesis review 和 manual risk review。

Monthly 只登记 documentation contract audit、artifact catalog review、report registry audit、data source coverage review、PIT coverage review 和 long-window backtest review。

Ad hoc research 只登记 SEC PIT historical backfill、SEC PIT cognitive evaluation、SEC PIT baseline comparison、SEC PIT diagnostics、SEC PIT candidate review、large parameter search 和 cache-only replay-window。

这些 cadence 当前不由 `aits ops daily-run` 自动执行。

## Safety Boundary

Reader Brief、governance、shadow monitor、documentation/report registry 任务必须保持：

- `production_effect=none`
- 不写 production weights
- 不写 active shadow weights，除非未来有显式 owner 批准任务
- 不触发 broker action
- 不触发 trading action

`config/scheduled_tasks.yaml` 中的 reader/governance/shadow/report scope 任务由 loader safety 校验统一检查；测试覆盖默认配置不得出现 safety violation。

## Closed-Market 行为

休市日计划仍保留数据与健康复核链路：`validate-data`、PIT fetch/build/validate、SEC companyfacts/metrics、valuation、`ops health --non-trading-day` 和 secret scan。

休市日默认跳过 `score-daily`、dashboard、SEC PIT shadow observe / monitor、score change attribution、market panel、report index、documentation contract、research governance summary、Reader Brief 和 Reader Brief quality。原因是这些 Reader Brief scoring artifacts 依赖新的 signal-date decision snapshot，休市日不得补造当日投资结论。

## 验收标准

- `aits ops daily-plan` 列出所有 required daily commands，顺序与 `config/scheduled_tasks.yaml` 一致。
- `aits ops daily-run` 在 `score-daily` 后执行 dashboard、SEC PIT shadow observe / monitor、score change attribution、market panel、report index、documentation contract、research governance summary、Reader Brief 和 Reader Brief quality。
- report index 和 documentation contract 在 Reader Brief 前执行。
- research governance summary 在 Reader Brief 前执行。
- SEC PIT shadow monitor 在 research governance summary 前执行。
- weekly / biweekly / monthly / ad hoc research tasks 已登记，但不进入 daily plan。
- closed-market mode 跳过 score 和 Reader Brief scoring artifacts，并保留 health / PIT / SEC / valuation。
- reader/governance/shadow monitor/report tasks 的 `production_effect` 保持 `none`，且不写 production weights、active shadow weights 或 broker/trading action。

## 进展记录

- 2026-05-29: 新增需求记录，开始 OPS-059 实现。
- 2026-05-29: 新增 `config/scheduled_tasks.yaml` 和 loader，完成 daily / weekly / biweekly / monthly / ad hoc research cadence 登记。
- 2026-05-29: 更新 `aits ops daily-plan` / `aits ops daily-run`，daily chain 由配置校验，Reader Brief 上游链进入交易日执行，旧 parameter/feedback/investment review 不再 daily-run 自动执行。
- 2026-05-29: 更新 direct dispatcher、`reports index --latest` 和 `docs report-contract --latest` 支持，补齐 OPS-059 专项测试。

## 状态

DONE。后续新增或重命名 daily report、governance、shadow monitor、backtest、parameter、documentation 或 report registry 命令时，必须同步更新 `config/scheduled_tasks.yaml`、本需求文档或新的后续任务、`docs/system_flow.md` 和相关测试。
