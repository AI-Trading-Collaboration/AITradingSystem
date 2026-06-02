# AITradingSystem Operations Runbook

最后更新：2026-06-02

本文是 AITradingSystem 周期性任务的总入口。执行任何 daily、weekly、biweekly、monthly、governance、scheduler validation 或 artifact catalog consistency 任务前，先读取本文，再进入对应的细分 runbook、配置或报告。

本文不替代 `config/scheduled_tasks.yaml`、`docs/runbooks/scheduled_task_orchestration.md`、`docs/runbook_daily_ops.md`、`docs/artifact_catalog.md` 或 `docs/system_flow.md`。其中：

- `config/scheduled_tasks.yaml` 是调度 cadence 和命令登记的配置源。
- `docs/runbooks/scheduled_task_orchestration.md` 说明 daily chain、非 daily cadence 隔离和 scheduler safety checklist。
- `docs/runbook_daily_ops.md` 说明人工交接、失败排查和运行归档。
- `docs/artifact_catalog.md` 说明关键 artifact 的生成者、上游、下游、production effect 和常见误解。
- `docs/system_flow.md` 仍是数据输入到结论输出路径的源-of-truth 图。

## Operating Principles

- 每日 scheduler trigger 是统一外部入口。Windows Task Scheduler、cron、GitHub Actions 或云调度器默认只应调用 `aits ops daily-run`，不要把 weekly / biweekly / monthly / governance 命令直接散落成多个未审计系统任务。
- 更长周期任务可以由同一个每日 scheduler 入口根据日期和条件触发，但必须通过受控编排实现：检查交易日、cadence due 状态、上游 daily artifacts、数据质量、production safety 和人工审批条件。
- 当前 baseline 中，`aits ops daily-run` 只执行 `daily_trading_day` 链路；weekly / biweekly / monthly / ad hoc research 任务已在 `config/scheduled_tasks.yaml` 登记，但不会自动进入 daily-run。自动 due-cadence 执行实现前，非 daily 任务由 operator 按本文手动运行或由后续受控 scheduler 执行。
- 所有依赖 cached market / macro data 的评分、回测、技术特征、报告或治理任务必须先通过 `aits validate-data` 或同一路径的数据质量门禁，并在下游输出中可见。
- 默认市场 regime 为 `ai_after_chatgpt`，anchor event 为 ChatGPT public launch on 2022-11-30，默认回测结论窗口从 2022-12-01 开始。任何回测、weekly review 或 monthly governance 报告都必须写明实际日期范围。
- Reader Brief、dashboard、report index、documentation contract、research governance、SEC PIT shadow monitor、weight feedback 和 scheduler validation 默认 `production_effect=none`。它们不得写 production weights、active shadow weights、broker action 或 trading action。

## Unified Daily Trigger

标准外部调度入口：

```powershell
aits ops daily-run
```

人工预检入口：

```powershell
aits ops daily-plan --fail-on-missing-env
```

指定评估日时使用：

```powershell
aits ops daily-run --as-of YYYY-MM-DD
```

`daily-run` 面向最新已完成的 U.S. equity market trading day。历史严格复现使用 `aits ops replay-day --mode cache-only --as-of YYYY-MM-DD`，不要用生产 daily scheduler 入口补造历史 PIT 视图。

## Daily Tasks

交易日 daily chain 至少覆盖以下必需任务；完整顺序以 `config/scheduled_tasks.yaml` 的 `daily_trading_day` 为准。

|类别|任务|命令或检查|阻断规则|
|---|---|---|---|
|Daily PIT|抓取 forward-only PIT snapshots|`aits pit-snapshots fetch-fmp-forward --as-of {as_of} --continue-on-failure`|失败快照不得作为可用 PIT 输入；失败必须进入报告或 `ops health`。|
|Daily PIT|刷新 PIT manifest|`aits pit-snapshots build-manifest --as-of {as_of}`|manifest 缺失、checksum 或 schema 异常时下游不得静默继续。|
|Daily PIT|验证 PIT manifest|`aits pit-snapshots validate --as-of {as_of}`|严重错误必须 fail closed。|
|Daily scoring|生成每日评分和决策 artifact|`aits score-daily --as-of {as_of}`|必须先通过 `validate-data`；失败时停止 Reader Brief、dashboard/latest 和下游报告。|
|Dashboard/latest checks|生成 evidence dashboard|`aits reports dashboard --as-of {as_of}`|只读，不替代 `daily_score` 和 trace audit。|
|Dashboard/latest checks|刷新 report index|`aits reports index --latest`|只读扫描既有报告；`STALE` / `MISSING` 需要进入人工复核。|
|Dashboard/latest checks|刷新 documentation contract|`aits docs report-contract --latest`|用于检查 registry 与 artifact catalog 文档覆盖。|
|Dashboard/latest checks|刷新 research governance summary|`aits reports research-governance-summary --latest`|必须在 Reader Brief 前完成；缺失时 Reader Brief 只能披露受限上下文。|
|Shadow candidate tracking|生成 market data freshness readiness|`aits data freshness --latest`|只读检查 tracking readiness；stale/missing freshness 不得静默进入 tracking review。|
|Shadow candidate tracking|执行受控 freshness recovery|`aits data recover-freshness --latest`|失败时显式阻断；不得伪造价格、manifest 或 tracking days。|
|Shadow candidate tracking|滚动 active candidate tracking|`aits portfolio track-candidate --latest`|只写 shadow tracking artifact；不启用 candidate、不修改 production。|
|Shadow candidate tracking|生成 tracking window review|`aits portfolio review-tracking --latest --show-window-progress`|`tracking_days<5` 必须保持 `needs_more_data`；这是 VALIDATING 正常状态，不是 scheduler 失败。|
|Shadow candidate tracking|生成 tracking review report alias|`aits reports portfolio-tracking-review --latest`|只读读取 review artifact，供 Dashboard / Reader Brief 展示。|
|Reader Brief|生成读者入口|`aits reports reader-brief --latest`|只读消费既有 artifact，不重跑 scoring、backtest、SEC PIT、shadow、weight 或 docs 上游。|
|Reader Brief|校验读者入口质量|`aits reports validate-reader-brief --latest`|质量失败或上下文受限必须在后续输出中可见。|

Daily chain 还包括 `download-data`、`validate-data`、SEC companyfacts / metrics、valuation fetch、SEC PIT shadow observe / monitor、score change attribution、market panel、`ops health` 和 secret scan。休市日模式下，系统不得生成新的 score、decision snapshot、Reader Brief scoring artifacts、tracking review artifacts、prediction ledger 行或执行动作。

## Periodic Task Register

以下任务登记为周期性运营职责。除非后续受控 scheduler 明确实现 due-cadence dispatch，否则它们不会由 `aits ops daily-run` 自动执行。

|Cadence|登记任务|默认入口|Due 条件|验收重点|
|---|---|---|---|---|
|Weekly|Weekly review|`aits etf weekly-review generate --as-of {as_of}`、`aits etf weekly-review validate`、`aits backtest --regime ai_after_chatgpt`、`aits backtest --regime ai_after_chatgpt --robustness-report`、`aits feedback build-parameter-replay --as-of {as_of}`、`aits feedback build-parameter-candidates --as-of {as_of}`、`aits feedback evaluate-parameter-governance --as-of {as_of}`、`aits reports research-governance-summary --latest`|每周最后一个已完成美股交易日之后，且 daily chain PASS 或明确披露限制。|ETF weekly review 只读 existing artifacts，并固定 `observe_only=true`、`candidate_only=true`、`production_effect=none`、`broker_action=none`、`manual_review_required=true`；报告声明 regime/date range、source artifacts、missing data 和 validation status；backtest/robustness/parameter governance 不得直接改变 production。|
|Weekly|Portfolio decision journal|`aits etf decision-journal add/update/list/remove`、`aits etf decision-journal report --as-of {as_of}`、`aits etf decision-journal analytics --as-of {as_of}`、`aits etf decision-journal propose-state-updates --as-of {as_of}`、`aits etf decision-journal validate`|TRADING-068 weekly review 已生成且 reviewer 已完成人工判断后；也可在下一次 weekly review 前刷新 report。|Journal entries 必须链接 source weekly review 和 action item；report/proposal/validation 固定 `observe_only=true`、`candidate_only=true`、`production_effect=none`、`broker_action=none`、`manual_review_required=true`；proposal 不修改 shadow registry、production weights 或 broker state。|
|Weekly|ETF parameter review evidence|`aits etf parameter-review aggregate --as-of {as_of}`|TRADING-065 forward dashboard、TRADING-068 weekly review 和 TRADING-069 decision journal report 已存在后；缺少 forward dashboard 或 forward candidate rows 时输出 `needs_more_data`。|Aggregation 只读 latest evidence sources，保留 source paths、missing source warnings 和 safety banner；固定 `observe_only=true`、`candidate_only=true`、`production_effect=none`、`broker_action=none`、`manual_review_required=true`；不得写 baseline config、production weights、shadow registry 或 broker state。|
|Weekly|Shadow parameter backtest review|`aits parameters shadow-backtest --latest`、`aits parameters validate-shadow-backtest --latest`、`aits reports shadow-parameter-backtest --latest`、`aits reports parameter-promotion --latest`|手动参数复核前，或每周最后一个已完成美股交易日之后，且 `aits validate-data` 可通过或报告明确 `INSUFFICIENT_DATA` / `FAILED`。|固定 `production_effect=none`、`manual_review_required=true`、`auto_promotion=false`；不得修改 `config/parameters/production/current.yaml`、production weights/gates、broker 或交易动作；报告声明 `ai_after_chatgpt` regime、实际 date range 和 data quality status。|
|Biweekly|Biweekly weight feedback|`python scripts/run_weight_candidate_evaluation.py --date {as_of}`、`python scripts/run_weight_promotion_gate.py --date {as_of}`、`aits feedback loop-review --as-of {as_of}`、`python scripts/run_shadow_vs_production_multi_day_review.py --date {as_of}`|每两周，且已有足够新 daily / weekly artifacts；样本不足时保持 `INSUFFICIENT_DATA` 或 manual review。|只读或 manual-review-only；不得写 production profile、approved overlay、broker order 或 trading action。|
|Monthly|Monthly governance|`aits docs report-contract --as-of {as_of}`、`aits reports index --as-of {as_of}`、`aits data-sources validate --as-of {as_of}`、`aits backtest-pit-coverage --as-of {as_of}`、`aits backtest --regime ai_after_chatgpt --to {as_of}`|每月最后一个已完成美股交易日之后，且 latest daily artifacts 可追溯。|检查文档、registry、数据源覆盖、PIT coverage 和长窗口回测解释是否仍可审计。|
|Governance|Scheduler validation|`aits ops daily-plan --as-of {as_of} --fail-on-missing-env`；调度配置或代码变更时运行 `python -m pytest tests/test_scheduled_tasks.py tests/test_ops_daily.py tests/test_cli_direct.py -q`|scheduler 配置、模板、daily chain、closed-market 行为或 cadence 登记变更时；也可每周人工抽检。|daily order 与 `config/scheduled_tasks.yaml` 一致；非 daily cadence 不泄漏进 daily plan；safety invariants 保持。|
|Governance|Artifact catalog consistency|`aits docs report-contract --latest`，并人工核对 `docs/artifact_catalog.md`、`config/report_registry.yaml` 和 latest report outputs|新增、重命名或删除 report/artifact 后；至少 monthly governance 时复核。|artifact 生成者、上游、下游、production effect、常见误解和 Reader Brief/report registry 可见性一致。|

## Date And Condition Dispatch

未来若把 weekly / biweekly / monthly / governance 任务接入每日 scheduler trigger，必须遵守以下 dispatch 条件：

1. 每日 trigger 仍只暴露一个外部入口，例如 `aits ops daily-run` 或后续明确命名的统一 scheduler entry。
2. due 判断必须基于明确日期规则、交易日历和已完成 daily artifacts，而不是系统启动时间的隐式副作用。
3. 非 daily 任务运行前必须确认当日 daily data gate 和必需上游 artifact 状态；缺失时输出 `SKIPPED`、`LIMITED`、`INSUFFICIENT_DATA` 或 fail-closed 诊断，不补造结论。
4. 任务输出必须写明 cadence、as_of、date range、source artifacts、data quality status 和 `production_effect`。
5. 任一会影响投资解释的阈值、promotion gate、readiness rule 或样本 floor 必须来自配置/政策 manifest，或按 AGENTS 规则登记为可审计 pilot baseline。

## Operator Checklist

执行任何周期任务前：

1. 读取本文和对应细分 runbook。
2. 确认任务 cadence、`as_of`、date range、market regime 和是否交易日。
3. 确认 `aits validate-data` 或同一路径门禁已通过，并记录对应质量报告。
4. 确认任务的 `production_effect`、weight write、broker action 和 trading action 边界。
5. 如果任务会新增或修改 CLI、配置、artifact、report schema、数据流、评分、回测或治理解释，同步更新 `docs/system_flow.md`、`docs/artifact_catalog.md`、任务登记和相关 requirements/runbook。
6. 如果遇到 blocker，不要静默绕过；按 AGENTS 的 no-silent-workaround 流程记录原因、影响、风险、验证覆盖和退出条件。
