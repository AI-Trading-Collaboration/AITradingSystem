# TRADING-923 to 932 Forward-Aging Observation Launch

## 背景

TRADING-911 to 922 已把 QQQ/TQQQ/SGOV 数据修复链路推进到
`OWNER_APPROVE_FORWARD_AGING`。本批不扩展策略池，目标是正式启动
`equal_risk_qqq_sgov` 的 research-only forward-aging observation，并把数据
修复、SGOV proxy、Marketstack warning、Reader Brief、scheduler dry-run 和
paper-shadow blocker 状态做成可复现、可审计、可长期运行的链路。

默认研究 regime 仍为 `ai_after_chatgpt`，anchor date 为 2022-11-30，
默认 backtest start 为 2022-12-01。pre-2022 历史只允许用于 warm-up、
stress 或 regime comparison，不能作为当前 AI-cycle 结论窗口。

## 安全边界

- `production_effect=none`
- `broker_action=none`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `manual_review_required=true`
- Forward-aging observation 只写 research-only 观察记录，不生成交易建议、
  broker action、paper-shadow activation 或 production config mutation。
- Reader Brief 只能展示极简 observation 状态，不得输出买入、卖出、应调仓、
  目标实盘仓位或真实交易建议。

## 阶段拆解

|任务|阶段|当前状态|验收标准|
|---|---|---|---|
|TRADING-923|data repair reproducibility proof|DATA_REPAIR_REPRODUCIBLE|证明 TQQQ 可从 0 行重建到目标行数，SGOV adj_close proxy、repair manifest、provider-backed manifest 和 no fixture/manual-copy fallback 均可审计。|
|TRADING-924|Marketstack SSL failure triage|MARKETSTACK_FAIL_CLOSED_ACCEPTED|记录 Marketstack endpoint/symbol/date range；不关闭 SSL；不绕过下载安全检查；明确 retry/fallback/second-source 保留策略。|
|TRADING-925|SGOV total-return proxy quality review|SGOV_PROXY_ACCEPTABLE|回答 SGOV adj_close 是否包含分红调整、price-only vs adj_close 差异、carry 低估、是否需要更明确 total-return source，以及是否允许带 warning forward-aging。|
|TRADING-926|first research-only observation write|FIRST_OBSERVATION_WRITTEN|正式写入或识别已存在第一条 observation；`broker_action=none`，不进入 paper-shadow/production，并记录 definition hash、data quality 和 warnings。|
|TRADING-927|idempotency and duplicate guard|FORWARD_IDEMPOTENCY_GUARD_PASS|同一 decision_date 重复运行返回 `OBSERVATION_ALREADY_EXISTS`，且 target weights、signal inputs 和 policy definition hash 不变。|
|TRADING-928|scoreboard after first observation|FORWARD_SCOREBOARD_INSUFFICIENT|第一条 observation 后 scoreboard 保持 `FORWARD_SCOREBOARD_PENDING` 或 `FORWARD_SCOREBOARD_INSUFFICIENT`，不得输出 ready、paper-shadow 或 production allowed。|
|TRADING-929|Reader Brief minimal forward-aging summary|DAILY_FORWARD_SUMMARY_SAFE|Reader Brief 显示 primary/challenger/latest observation/data quality/matured counts/safety fields，不显示交易建议。|
|TRADING-930|scheduler dry-run|FORWARD_AGING_SCHEDULER_OBSERVATION_ALREADY_EXISTS|美股交易日 dry-run 计划一次 observation；非交易日 skip；数据缺失 fail closed；可接日报前置流程；不接 broker。|
|TRADING-931|paper-shadow blocker status report|PAPER_SHADOW_BLOCKED|继续显示 `paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`、120d matured remaining 和 manual review required。|
|TRADING-932|owner launch pack|FORWARD_AGING_OWNER_LAUNCH_PACK_READY|回答第一条 observation、数据质量、Marketstack/SGOV warning、scheduler/Reader Brief allowed，以及 paper-shadow/production blocked 状态。|

## 实施顺序

1. 工程与数据可复现：TRADING-923、924、925。
2. 正式写入第一条 observation：TRADING-926、927、928。
3. 日报与调度 dry-run：TRADING-929、930、931、932。

## 进展记录

- 2026-06-24: 新增需求拆解并进入 `IN_PROGRESS`。本批延续
  TRADING-911～922 owner approval，主线是启动长期 research-only
  forward-aging observation，而不是继续扩展策略。
- 2026-06-24: 实现增量 builder/CLI/report registry/test 草案：
  `data-repair-reproducibility-proof`、`marketstack-ssl-failure-triage`、
  `sgov-total-return-proxy-quality-review`、`first-forward-aging-observation-write`、
  `forward-aging-idempotency-and-duplicate-guard`、
  `forward-aging-scheduler-dry-run`、`paper-shadow-blocker-status-report`、
  `forward-aging-owner-launch-pack`；`daily-reader-forward-aging-summary`
  增补 `data_quality_status`。所有新增输出保持 research-only 和 no broker /
  no production 边界。
- 2026-06-24: 真实执行 923～932 launch 链路。`aits validate-data --as-of
  2026-06-22` 为 `PASS_WITH_WARNINGS` / 0 errors；第一条正式 observation 写入
  `decision_date=2026-06-22`，`observation_count=5`，并显式替换此前
  `MARKET_DATA_MISSING` / 0 observation 的失败占位 artifact。Scoreboard 为
  `FORWARD_SCOREBOARD_INSUFFICIENT`，matured 20d/60d/120d 均为 0；scheduler
  dry-run 识别同日 observation 已存在；owner launch pack ready，但
  paper-shadow、production 和 broker 仍全部 blocked/none。
