# AITradingSystem Operations Runbook

最后更新：2026-06-03

本文是 AITradingSystem 周期性任务的总入口。执行任何 daily、weekly、biweekly、monthly、governance、scheduler validation 或 artifact catalog consistency 任务前，先读取本文，再进入对应的细分 runbook、配置或报告。

本文不替代 `config/scheduled_tasks.yaml`、`config/etf_portfolio/operations_schedule.yaml`、`docs/runbooks/scheduled_task_orchestration.md`、`docs/runbook_daily_ops.md`、`docs/artifact_catalog.md` 或 `docs/system_flow.md`。其中：

- `config/scheduled_tasks.yaml` 是调度 cadence 和命令登记的配置源。
- `config/etf_portfolio/operations_schedule.yaml` 是 TRADING-074 ETF portfolio operations schedule source config，用于描述 ETF research daily / weekly / biweekly / monthly / manual-review command plan、dependencies、expected outputs 和 safety boundary；它不是外部 scheduler entry。
- `docs/runbooks/scheduled_task_orchestration.md` 说明 daily chain、非 daily cadence 隔离和 scheduler safety checklist。
- `docs/runbook_daily_ops.md` 说明人工交接、失败排查和运行归档。
- `docs/artifact_catalog.md` 说明关键 artifact 的生成者、上游、下游、production effect 和常见误解。
- `docs/system_flow.md` 仍是数据输入到结论输出路径的源-of-truth 图。

## Operating Principles

- 每日 scheduler trigger 是统一外部入口。Windows Task Scheduler、cron、GitHub Actions 或云调度器默认只应调用 `aits ops daily-run`，不要把 weekly / biweekly / monthly / governance 命令直接散落成多个未审计系统任务。
- 更长周期任务可以由同一个每日 scheduler 入口根据日期和条件触发，但必须通过受控编排实现：检查交易日、cadence due 状态、上游 daily artifacts、数据质量、production safety 和人工审批条件。
- 当前 baseline 中，`aits ops daily-run` 只执行 `daily_trading_day` 链路；weekly / biweekly / monthly / ad hoc research 任务已在 `config/scheduled_tasks.yaml` 登记，但不会自动进入 daily-run。自动 due-cadence 执行实现前，非 daily 任务由 operator 按本文手动运行或由后续受控 scheduler 执行。
- TRADING-074A/B/C/D 的 ETF operations schedule spec、daily / weekly / biweekly / monthly graph 只用于规划和校验 ETF portfolio research workflow；weekly / biweekly / monthly graph 会显式披露跨 cadence 上游为 external dependencies，并标记人工复核 checkpoint；monthly graph 保证 bounded historical weight search 只在 slower cadence 计划中出现；在 TRADING-074H/K dry-run 和 validation gate 完成前，它们不执行命令、不自动 dispatch cadence、不改变 `aits ops daily-run` 的统一入口。
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
|Weekly|ETF parameter review evidence|`aits etf parameter-review aggregate --as-of {as_of}`、`aits etf parameter-review report --as-of {as_of}`、`aits etf parameter-review validate`|TRADING-065 forward dashboard、TRADING-068 weekly review 和 TRADING-069 decision journal report 已存在后；缺少 forward dashboard 或 forward candidate rows 时输出 `needs_more_data`。|Aggregation/report 只读 latest evidence sources，保留 source paths、missing source warnings、proposal scorecard 和 safety banner；validation gate 确认 proposal-only workflow、Reader Brief visibility、source links 和 unsafe action blockers；Reader Brief 只读最新 report 摘要；固定 `observe_only=true`、`candidate_only=true`、`production_effect=none`、`broker_action=none`、`manual_review_required=true`；不得写 baseline config、production weights、shadow registry 或 broker state。|
|Weekly / Manual|AI confirmation forward attribution review|`aits etf ai-attribution build --as-of {as_of}`、`aits etf ai-attribution report --as-of {as_of}`、`aits etf ai-attribution validate`|AI confirmation reports 和 ETF price cache 已存在后；用于复核 `AIConfirmationScore` 与 component scores 是否对 forward ETF / semiconductor / satellite outcomes 具备 attribution value；样本不足时输出 `needs_more_data`。|Build/report 必须先通过 ETF price quality gate；dataset row 固定 `evaluation_only=true`，forward returns 只能用于 attribution/evaluation；report 必须披露 `ai_after_chatgpt` regime、requested date range、source report paths、sample size、data quality status、bucket/component/regime/event/redundancy analysis、evidence scorecard 和 manual review recommendation；validation gate 确认 workflow modules、Reader Brief/report registry visibility、evaluation-only separation、forbidden production/trading output keys 和 safety fields；Reader Brief 只读 latest attribution report，不运行上游；固定 `observe_only=true`、`candidate_only=true`、`production_effect=none`、`broker_action=none`、`manual_review_required=true`；不得写 production weights、不得自动 promotion、不得触发 broker action。|
|Weekly / Manual|Satellite replacement forward attribution review|`aits etf satellite-attribution build --as-of {as_of}`、`aits etf satellite-attribution report --as-of {as_of}`、`aits etf satellite-attribution validate`|Satellite replacement reports、可选 AI confirmation reports 和 ETF/satellite price cache 已存在后；用于复核 eligible satellite stocks 是否跑赢 benchmark ETF、fallback_to_etf 是否保护 downside、SatelliteCandidateScore 是否有 ranking power，以及 role/group/risk/AI interaction attribution；样本不足时输出 `needs_more_data`。|Build/report 必须先通过 ETF price quality gate；dataset row 固定 `evaluation_only=true`，forward returns 只能用于 attribution/evaluation；report 必须披露 `ai_after_chatgpt` regime、requested date range、source report paths、sample size、data quality status、eligibility bucket、stock-vs-ETF、fallback、score、risk、role/group、AI interaction analysis、evidence scorecard 和 manual review recommendation；validation gate 确认 workflow modules、Reader Brief/report registry visibility、evaluation-only separation、forbidden production/trading output keys 和 safety fields；Reader Brief 只读 latest attribution report，不运行上游；固定 `observe_only=true`、`candidate_only=true`、`production_effect=none`、`broker_action=none`、`manual_review_required=true`；不得写 production weights、不得自动 promotion、不得触发 broker action。|
|Weekly / Manual|ETF weight calibration dual-track setup|`aits etf weight-calibration search --search etf_initial_weight_search_v1`、`aits etf weight-calibration register-candidates --latest --top N`、`aits etf weight-calibration enroll-forward --latest --top N`、`aits etf weight-calibration aggregate-evidence --as-of {as_of}`、`aits etf weight-calibration overfit-diagnostics`、`aits etf weight-calibration generate-proposals`、`aits etf weight-calibration report --latest`、`aits etf weight-calibration validate`|需要重新生成 historical candidate initial weights、把已登记 safe candidates 显式放入 forward observation、比较 historical expectation vs forward evidence、生成 overfit risk bands、生成 evidence-linked proposal-only recommendations、汇总 dual-track manual review package，或确认 TRADING-071 workflow safe/complete 时。|Search 必须先通过 ETF price quality gate，并披露 `ai_after_chatgpt` regime、requested date range、baseline/benchmark comparison、robustness metrics 和 safety fields；register/enroll/aggregate/diagnostics/proposals/report/validate 只写 ignored `data/etf_portfolio/weight_calibration/` 和 `reports/etf_portfolio/weight_calibration/` runtime artifacts，blocked/rejected/needs_more_data candidates 不得 enroll；缺少 forward row 或 forward days 不足时 evidence status 必须是 `needs_more_forward_data`，不得补造结论；overfit diagnostics 只给 low/medium/high/critical risk band；proposal types 只允许 `continue_forward_observation`、`reject_weight_set`、`defer_until_more_forward_data`、`propose_extended_shadow` 和 `propose_manual_baseline_review`，不得 apply/promote/enable broker；report 只汇总 source links、proposal scorecard 和 manual review package；validation gate 必须确认 config bounded、workflow modules available、Reader Brief/report registry visibility、unsafe proposal type blocking、evidence-linked proposals 和 safety fields，失败时 fail closed；Reader Brief 只读 latest `etf_weight_dual_track_calibration_report` 摘要，不运行上游；固定 `observe_only=true`、`candidate_only=true`、`production_effect=none`、`broker_action=none`、`manual_review_required=true`；不得写 `target_weights.csv`、baseline config、shared experiment shadow registry 或 broker state。|
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
