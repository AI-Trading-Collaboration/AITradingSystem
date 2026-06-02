# TRADING-073 Satellite Replacement Forward Attribution Review

最后更新：2026-06-03

## 背景

TRADING-067 已建立 satellite stock replacement policy、candidate-only replacement plan
和 validation gate。TRADING-073 的目标不是扩大 satellite replacement 对 production
weights 的影响，而是验证这些 replacement / fallback 决策是否相对 ETF-first exposure
具备可解释的 forward attribution evidence。

本阶段固定为 attribution-only：

```text
observe_only=true
candidate_only=true
production_effect=none
broker_action=none
manual_review_required=true
```

不得自动修改 production weights，不得自动 promotion candidate，不得触发 broker action。

## 市场区间

默认解释区间使用 `ai_after_chatgpt` regime：

- anchor event: ChatGPT public launch on 2022-11-30;
- default attribution start: 2022-12-01;
- pre-2022 data 仅可用于 warm-up、压力测试或 regime comparison，不能作为 AI-cycle
  结论默认窗口。

所有 attribution report 必须披露 selected market regime 和 requested date range。

## 目标问题

- `eligible` satellite stocks 是否在 forward window 中跑赢 benchmark ETF？
- `fallback_to_etf` 是否避免了弱股票 underperformance，或错过了 upside？
- `SatelliteCandidateScore` 是否能区分强弱 satellite stocks？
- satellite replacement 是否在 drawdown、volatility、event risk 后仍有足够收益补偿？
- 哪些 role / group 的 satellite replacement 更有证据？
- `AIConfirmationScore` 是否改善 satellite selection 的解释力？

输出是 attribution review report，不是交易决策。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|TRADING-073A dataset builder|DONE|`aits etf satellite-attribution build --as-of YYYY-MM-DD` 生成 evaluation-only dataset，包含 decision date、eligibility date、replacement plan date、forward window、ticker、benchmark ETF、role/group、eligibility status、score/components、fallback flag、replacement weight、forward outcomes、regime、event flag、sample availability 和 safety fields。|
|TRADING-073B eligibility bucket analysis|DONE|按 eligible/watch/fallback_to_etf/blocked/insufficient_data bucket 计算 stock/benchmark/replacement forward return、outperformance hit rate、drawdown、volatility、event ratio 和 sample warning。|
|TRADING-073C stock-vs-benchmark attribution|DONE|按 ticker 与 benchmark ETF 输出 mean/median alpha、hit rate、risk deltas、best/worst window、eligibility success rate 和 fallback miss rate。|
|TRADING-073D fallback attribution|DONE|评估 fallback saved loss、fallback missed gain、saved drawdown、missed upside 和 fallback reason breakdown。|
|TRADING-073E score attribution|DONE|按 SatelliteCandidateScore buckets 输出 ranking power、forward alpha、risk deltas、replacement impact 和 sample warning。|
|TRADING-073F risk attribution|DONE|输出 replacement drawdown/volatility delta、single-name risk contribution、event-window drawdown、risk-adjusted alpha、fallback saved drawdown 和 eligible added drawdown。|
|TRADING-073G role/group attribution|DONE|按 role/group 输出 alpha、hit rate、risk deltas、eligibility success、fallback saved/missed rates、best/worst role 和 warning。|
|TRADING-073H AI interaction attribution|DONE|把 satellite outcomes 与 AIConfirmationScore、SemiconductorBreadthScore、MegaCapAIScore、EventRiskScore high/low buckets 连接，输出 interaction metrics 和 missing/insufficient warning。|
|TRADING-073I evidence scorecard|DONE|汇总 eligible outperformance、fallback protection、score ranking、risk-adjusted evidence、role/group evidence、AI interaction、sample quality 和 data coverage，输出 overall status 与 manual review recommendation。|
|TRADING-073J report generator|DONE|`aits etf satellite-attribution report --as-of YYYY-MM-DD` 生成 JSON/Markdown report，包含 safety banner、metadata、coverage、B-I analysis、source links 和 manual review recommendation。|
|TRADING-073K Reader Brief integration|DONE|Reader Brief 新增 `Satellite Attribution Review` 区块，只读展示 overall status、eligible/fallback/role/risk evidence、safety 和 detail report。|
|TRADING-073L validation gate|DONE|`aits etf satellite-attribution validate` 检查 A-K、evaluation-only separation、安全字段、Reader Brief/report registry visibility，并输出 JSON/Markdown gate。|

## 数据与输出约束

Forward return 字段只可用于 attribution/evaluation。每条 dataset row 必须包含：

```text
decision_date
eligibility_date
replacement_plan_date
forward_window
evaluation_as_of_date
evaluation_only=true
```

允许输出：

```text
satellite_attribution_dataset
eligibility_bucket_analysis
stock_vs_etf_attribution
fallback_attribution
score_attribution
risk_attribution
role_group_attribution
AI_interaction_attribution
evidence_scorecard
manual_review_recommendation
```

禁止输出：

```text
production_weight_update
candidate_auto_promotion
broker_order
baseline_config_mutation
live_satellite_allocation
```

## Pilot heuristic policy

以下数值只用于 attribution report 的 warning、status 和 summary，不得用于
production weights、candidate promotion 或 broker action。它们是 TRADING-073 baseline
实现的临时 pilot constants，退出条件是积累足够真实 forward samples 后迁移到 reviewed
policy manifest 或重新校准：

|constant|value|用途|退出条件|
|---|---:|---|---|
|`MIN_SATELLITE_ATTRIBUTION_SAMPLE_COUNT`|5|bucket/stock/role/AI interaction 样本不足 warning floor|真实 satellite forward attribution 样本足够后由 owner review 校准|
|`SATELLITE_MEANINGFUL_ALPHA_THRESHOLD`|0.005|判断 50bp 以上 stock-vs-ETF / replacement-vs-ETF lift 是否具备报告层意义|以真实 satellite forward evidence 分布重新估计|
|`SATELLITE_HIGH_RISK_VOLATILITY_DELTA`|0.05|报告层 high-volatility failure / risk note 的 volatility delta pilot boundary|以真实 stock-vs-ETF volatility distribution 重新校准|
|`SATELLITE_DRAWDOWN_SEVERITY_THRESHOLD`|0.02|fallback saved drawdown / added drawdown severity 分界|以真实 satellite forward drawdown evidence 重新估计|
|`SATELLITE_AI_HIGH_THRESHOLD`|65.0|AI interaction attribution 中 high bucket 的报告层分界|以真实 AI-confirmation / satellite interaction evidence 重新校准|
|`SATELLITE_AI_LOW_THRESHOLD`|45.0|AI interaction attribution 中 low bucket 的报告层分界|以真实 AI-confirmation / satellite interaction evidence 重新校准|

这些阈值只能降低结论置信度或提示人工复核，不得提高投资动作权限。

## 验证计划

最低验证命令：

```bash
python -m pytest tests -q
python -m ruff check config src tests scripts docs
python -m compileall -q src tests scripts
git diff --check
python -m ai_trading_system.cli etf satellite-attribution validate
```

TRADING-073 不得标记完成，除非 Satellite attribution validation gate 与全量测试通过。

## 进展记录

- 2026-06-03: 新增需求文档并进入 `IN_PROGRESS`，原因：owner 要求继续下一项
  TRADING-073 计划，验证 satellite replacement 是否相对 ETF-first exposure 具备
  forward attribution / explanatory value；本阶段保持 observe-only / candidate-only /
  manual-review-only。
- 2026-06-03: TRADING-073A-L baseline workflow 完成，原因：新增 evaluation-only
  satellite attribution dataset、eligibility bucket / stock-vs-ETF / fallback / score /
  risk / role/group / AI interaction analysis、evidence scorecard、JSON/Markdown
  report、Reader Brief `Satellite Attribution Review`、report registry、artifact catalog、
  system flow、operations runbook、README integration 和 fail-closed validation gate；
  验证通过全量 pytest（1947 passed）、ruff、compileall、diff check 和
  `aits etf satellite-attribution validate`（PASS）。父任务进入 `VALIDATING`，下一步
  等待真实 forward samples 与 owner manual review。
