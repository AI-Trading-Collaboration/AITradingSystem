# TRADING-072 AI Confirmation Forward Attribution Review

最后更新：2026-06-02

## 背景

TRADING-066 已建立 `AIConfirmationScore`、component scores、candidate-only overlay
和 validation gate。TRADING-072 的目标不是扩大 AI confirmation 对权重的影响，
而是验证它是否对未来 ETF / semiconductor / satellite 表现具备可解释的增量证据。

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
- default attribution/backtest start: 2022-12-01;
- pre-2022 data 仅可用于 warm-up、压力测试或 regime comparison，不能作为 AI-cycle
  结论默认窗口。

所有 attribution report 必须披露 selected market regime 和 requested date range。

## 目标问题

- `AIConfirmationScore` 高位时，QQQ / SMH / SOXX forward return 是否更好？
- `SemiconductorBreadthScore` 高位时，SMH / SOXX 是否相对 QQQ 更强？
- `MegaCapAIScore` 高位时，QQQ 是否相对 SPY 更强或回撤更小？
- `EventRiskScore` 高位时，forward drawdown 或 volatility 是否更高？
- `AIConfirmationScore` 是否只是已有 ETF momentum / relative strength 的重复代理？

输出是 attribution review report，不是交易决策。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|TRADING-072A dataset builder|DONE|`aits etf ai-attribution build --as-of YYYY-MM-DD` 生成 evaluation-only dataset，包含 score date、evaluation date、forward window、component scores、regime、forward returns、drawdown、volatility、sample availability 和 safety fields。|
|TRADING-072B score bucket analysis|DONE|按 0-30 / 30-45 / 45-65 / 65-80 / 80-100 bucket 计算多窗口 forward return、excess、hit rate、drawdown、volatility 和 sample warning。|
|TRADING-072C component attribution|DONE|对 SemiconductorBreadthScore、MegaCapAIScore、AISemiconductorRelativeStrengthScore、EventRiskScore、DataCoverageScore 计算 bucket metrics、rank correlation、directional hit rate、drawdown relationship 和 sample warning。|
|TRADING-072D regime attribution|DONE|按 regime 和 score bucket 输出 sample、forward/excess return、drawdown、volatility、hit rate 和 warning，缺失 regime 归入 `unknown`。|
|TRADING-072E event risk attribution|DONE|按 low/medium/high/critical event-risk bucket 计算 drawdown、volatility、negative-return hit rate、relative return、false positive/negative rate 和 sample count。|
|TRADING-072F redundancy diagnostics|DONE|比较 AI score 与 QQQ/SMH momentum、SMH/QQQ relative strength、QQQ/SPY relative strength、baseline signal/regime proxy，输出 correlation、rank correlation、incremental lift、residual summary 和 redundancy band。|
|TRADING-072G evidence scorecard|DONE|汇总 forward evidence、semiconductor relative evidence、mega-cap evidence、event-risk evidence、regime stability、redundancy penalty、sample quality 和 data coverage，输出 overall status 与 manual review recommendation。|
|TRADING-072H report generator|DONE|`aits etf ai-attribution report --as-of YYYY-MM-DD` 生成 JSON/Markdown report，包含 safety banner、metadata、coverage、bucket/component/regime/event/redundancy/scorecard/source links。|
|TRADING-072I Reader Brief integration|DONE|Reader Brief 新增 `AI Attribution Review` 区块，只读展示 status、best/weak evidence、redundancy、manual review 和 detail report。|
|TRADING-072J validation gate|DONE|`aits etf ai-attribution validate` 检查 A-I、evaluation-only separation、安全字段、Reader Brief/report registry visibility，并输出 JSON/Markdown gate。|

## 数据与输出约束

Forward return 字段只可用于 attribution/evaluation。每条 dataset row 必须包含：

```text
score_date
forward_window
evaluation_as_of_date
evaluation_only=true
```

允许输出：

```text
attribution_dataset
bucket_analysis
component_attribution
regime_conditional_attribution
event_risk_attribution
redundancy_diagnostics
evidence_scorecard
manual_review_recommendation
```

禁止输出：

```text
production_weight_update
candidate_auto_promotion
broker_order
baseline_config_mutation
```

## Pilot heuristic policy

以下数值只用于 attribution report 的 warning、status 和 summary，不得用于
production weights、candidate promotion 或 broker action。它们是 TRADING-072 baseline
实现的临时 pilot constants，退出条件是积累足够真实 forward samples 后迁移到 reviewed
policy manifest 或重新校准：

|constant|value|用途|退出条件|
|---|---:|---|---|
|`MIN_ATTRIBUTION_SAMPLE_COUNT`|5|bucket/component/regime/event-risk 样本不足 warning floor|真实 forward attribution 样本足够后由 owner review 校准|
|`MEANINGFUL_LIFT_THRESHOLD`|0.005|判断 50bp 以上 bucket lift 是否具备报告层意义|以真实 ETF forward evidence 分布重新估计|
|`EVENT_RISK_DRAWDOWN_SEVERITY_THRESHOLD`|0.02|event-risk false positive/negative 的 drawdown severity 分界|以真实 event-risk 样本重新估计|
|`REDUNDANCY_MEDIUM_CORRELATION`|0.50|AI score 与 momentum/relative-strength overlap medium band|以真实样本相关性稳定性重新校准|
|`REDUNDANCY_HIGH_CORRELATION`|0.75|AI score 与 momentum/relative-strength overlap high band|以真实样本相关性稳定性重新校准|

这些阈值只能降低结论置信度或提示人工复核，不得提高投资动作权限。

## 验证计划

最低验证命令：

```bash
python -m pytest tests -q
python -m ruff check config src tests scripts docs
python -m compileall -q src tests scripts
git diff --check
python -m ai_trading_system.cli etf ai-attribution validate
```

TRADING-072 不得标记完成，除非 AI attribution validation gate 与全量测试通过。

## 进展记录

- 2026-06-02: 新增需求文档并进入 `IN_PROGRESS`，原因：owner 提供 TRADING-072
  开发计划，要求验证 AI confirmation 是否对未来 ETF / semiconductor / satellite
  表现具备 attribution 和 forward explanatory value；本阶段保持 observe-only /
  candidate-only / manual-review-only。
- 2026-06-02: TRADING-072A-J baseline workflow 完成并进入 `VALIDATING`，原因：
  新增 AI attribution dataset、bucket/component/regime/event/redundancy analysis、
  evidence scorecard、JSON/Markdown report、Reader Brief `AI Attribution Review`、
  report registry/artifact catalog/system flow/runbook/README integration 和
  fail-closed validation gate；验证通过 `python -m pytest tests -q`
  （1938 passed）、`python -m ruff check config src tests scripts docs`、
  `python -m compileall -q src tests scripts`、`git diff --check` 和
  `python -m ai_trading_system.cli etf ai-attribution validate`（PASS）。
  父任务保持 `VALIDATING`，直到真实 forward samples 和 owner manual review 支持
  下一阶段 AI confirmation overlay 解释或 policy review。
