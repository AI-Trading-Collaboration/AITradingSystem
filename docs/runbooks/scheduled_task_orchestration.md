# 统一计划任务编排 Runbook

## 目标

`config/scheduled_tasks.yaml` 是 OPS-059 后的统一调度计划源，登记 daily / weekly / biweekly / monthly / ad hoc research 任务。`aits ops daily-plan` 和 `aits ops daily-run` 只执行 `daily_trading_day` 链路，并在生成计划时校验顺序与配置一致。TRADING-099 允许 daily 链路包含 Dynamic v3 rescue lightweight `schedule observe` gate；它只做 due/skip/block 审计和只读检查，不执行参数搜索或 promotion pack。

本 runbook 用于调度审查，不替代各报告自己的审计产物。所有 report / governance / shadow monitor 任务默认 `production_effect=none`，不写 production weights，不写 active shadow weights，不调用 broker，不触发交易。

## Daily Trading-Day Chain

交易日 `aits ops daily-run` 的受控顺序如下：

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
12. `forward-evidence capture-dry-run-daily --as-of {as_of}`
13. `reports dashboard`
14. `sec-pit shadow-observe --latest`
15. `sec-pit shadow-monitor --latest`
16. `reports score-change-attribution --latest`
17. `reports market-panel --latest`
18. `data freshness --latest`
19. `data recover-freshness --latest`
20. `portfolio track-candidate --latest`
21. `portfolio review-tracking --latest --show-window-progress`
22. `reports portfolio-tracking-review --latest`
23. `etf forward update --latest`
24. `etf forward dashboard --latest`
25. `etf forward watchlist --latest`
26. `reports artifact-lineage --latest`
27. `reports validate-artifact-lineage --latest`
28. `reports index --latest`
29. `docs report-contract --latest`
30. `reports research-governance-summary --latest`
31. `reports reader-brief --latest`
32. `reports quality-gate --latest`
33. `reports validate-reader-brief --latest`
34. `etf dynamic-v3-rescue schedule observe --as-of {as_of}`
35. `ops health`
36. `security scan-secrets`

`validate-data` 是 cached market / macro data 的必需质量门禁。任何 downstream scoring、technical features、backtest 或 daily report 不得绕过该门禁。`forward-evidence capture-dry-run-daily` 只在 `score-daily` 后写 dry-run archive 和 append-only ledger，固定 `production_effect=none`，不得触发 broker/order、paper-shadow、official weight 或 production mutation。Portfolio tracking review 的 `needs_more_data` 是 VALIDATING 下的正常窗口状态，不得作为 scheduler 失败或 production approval。Dynamic v3 rescue `schedule observe` 只允许检查 weekly due 条件、latest pointer validation、stale 状态和可选 observe-only shadow monitor；不得自动运行 `sweep run-profile`、real sweep、candidate attribution、walk-forward、overfit 或 `promotion pack`。

## 验证 Daily Plan

只读检查：

```powershell
aits ops daily-plan --as-of 2026-05-06
```

真实执行：

```powershell
aits ops daily-run --as-of 2026-05-06
```

计划和执行器都应显示同一顺序。若配置与代码步骤不一致，`daily-plan` / `daily-run` 应 fail closed，而不是继续用隐式顺序运行。

两者还会在原计划Markdown旁写入 `daily_operations_shadow.v1` JSON sidecar，保存source config hash、market-session activated WorkflowSpec、DUE resolution、non-executing RunLedger和exact parity。该sidecar是additive审计证据，不执行命令、不启用non-daily dispatch、不改变原Markdown bytes/path。

## Closed-Market Mode

周末或 NYSE 常规整日休市日：

- 仍运行 `validate-data`、PIT fetch/build/validate、SEC companyfacts/metrics、valuation、Dynamic v3 rescue `schedule observe`（输出 closed-market skip audit）、`ops health --non-trading-day` 和 secret scan。
- `official_policy_sources` 以 `config/scheduled_tasks.yaml` 的 `activation_condition=closed_market_only` 在 `validate-data` 后运行；交易日不激活。配置计划、legacy daily plan和canonical shadow plan必须解析为相同步骤顺序。
- 跳过 `score-daily`、forward evidence dry-run archive、dashboard、SEC PIT shadow observe / monitor、score change attribution、market panel、market data freshness review、freshness recovery、portfolio candidate tracking、portfolio tracking review、report index、documentation contract、research governance summary、Reader Brief 和 Reader Brief quality。
- 不生成新的日报评分、decision snapshot、Reader Brief scoring artifacts、prediction ledger 行或执行动作。

## Weekly Cadence

Weekly 任务在 `config/scheduled_tasks.yaml` 中登记，但不由 daily-run 自动执行：

- backtest
- backtest robustness
- parameter replay
- parameter candidates
- parameter governance
- weight candidate evaluation
- weight promotion gate
- research governance summary review
- Dynamic v3 rescue artifact validation / stale review / governance validate / research index / observe-only shadow monitor

Weekly 输出应继续声明 `ai_after_chatgpt` market regime；默认回测结论窗口从 2022-12-01 开始，除非报告明确说明为何使用更早历史。

## Biweekly Cadence

Biweekly 任务只作为人工或后续 scheduler 接入口登记：

- investment review
- feedback loop review
- shadow lane review
- SEC PIT observe-only review
- manual thesis review
- manual risk review

这些任务不得因为存在于配置中而进入 daily-run。

## Monthly Cadence

Monthly audit 任务只登记，不自动 daily-run：

- documentation contract audit
- artifact catalog review
- report registry audit
- data source coverage review
- PIT coverage review
- long-window backtest review

Monthly 任务适合用于检查文档覆盖、report registry freshness、数据源覆盖和长窗口回测解释是否仍然可审计。

## Ad Hoc Research Chain

以下任务标记为 manual / ad hoc research：

- SEC PIT historical backfill
- SEC PIT cognitive evaluation
- SEC PIT baseline comparison
- SEC PIT diagnostics
- SEC PIT candidate review
- large parameter search
- cache-only replay-window
- Dynamic v3 rescue data audit / profile validation / small_real sweep / injection audit / candidate attribution / walk-forward selection / overfit / promotion pack

这些任务可能成本高、耗时长或需要 owner 明确选择窗口。不得由 daily-run 自动触发。

## Safety Checklist

调度审查时必须检查：

- reader/governance/shadow monitor/report tasks 的 `production_effect` 是否为 `none`。
- 是否存在 production weight write。
- 是否存在 active shadow weight write。
- 是否存在 broker action 或 trading action。
- `reports index` 和 `docs report-contract` 是否在 Reader Brief 之前。
- `research-governance-summary` 是否在 Reader Brief 之前。
- `sec-pit shadow-monitor` 是否在 research governance summary 之前。
- daily Dynamic v3 rescue 任务是否仅为 `schedule observe`，且所有 heavy research 命令仍在 weekly / ad hoc cadence。
- weekly / biweekly / monthly / ad hoc research tasks 是否没有进入 daily plan。

## Windows Task Scheduler

OPS-059 不自动创建或修改 Windows Task Scheduler 任务。现有模板若要接入，应只调用：

```powershell
aits ops daily-run
```

不要把 weekly / monthly / ad hoc research 命令直接塞进 daily trigger；Dynamic v3 rescue 只允许 daily trigger 调用 `schedule observe` gate。若未来需要本地模板更新，先审查 `config/scheduled_tasks.yaml`、本 runbook 和任务登记，再生成独立模板变更。

## 测试

OPS-059 的基础回归：

```powershell
python -m pytest tests/test_scheduled_tasks.py tests/test_ops_daily.py tests/test_cli_direct.py -q
```

测试覆盖 daily 命令顺序、Reader Brief 链执行顺序、closed-market skips、非 daily 任务隔离、direct dispatcher 支持和 safety invariants。
