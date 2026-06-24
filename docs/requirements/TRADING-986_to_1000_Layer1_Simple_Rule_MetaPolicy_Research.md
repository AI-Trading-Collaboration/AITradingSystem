# TRADING-986 to 1000 Layer-1 Simple Rule Meta-Policy Research

最后更新：2026-06-25

## 背景

TRADING-976～985 已完成 Layer-1 meta-policy readiness gate，当前结论为：

```text
master status = LAYER1_SIMPLE_RULE_RESEARCH_READY
owner recommendation = START_LAYER1_SIMPLE_RULE_RESEARCH
```

本批开始第一层 simple-rule selector 的 research-only 历史研究。研究只允许在：

```text
equal_risk_qqq_sgov
100_qqq
```

之间选择或加权。`qqq_50_sgov_50` / `qqq_60_sgov_40` 只做 reference comparison。
QQQ-plus growth、TQQQ-heavy、tail-risk fallback、LEAPS、Wheel 和 Options 继续排除。

全局安全边界固定：

```text
paper_shadow_allowed=false
production_allowed=false
broker_action=none
manual_review_required=true
```

## 阶段拆解

|任务|阶段|当前状态|验收标准|
|---|---|---|---|
|TRADING-986|selector registry|VALIDATING|新增 `config/research/layer1_simple_rule_selector_registry.yaml` 和 `layer1_simple_rule_selector_registry_review.json/md`；registry 覆盖 required selectors、feature inputs、decision rules、switching constraints 和 safety flags。|
|TRADING-987|trend rule selector backtest|VALIDATING|`layer1_trend_rule_selector_backtest.json/md` 输出 trend 200DMA、100/200DMA、distance-to-200DMA selector 的 gross/net return、drawdown、Sharpe、Calmar、turnover、switch count、holding period、cost drag、regret 和 relative metrics。|
|TRADING-988|volatility rule selector backtest|VALIDATING|`layer1_volatility_rule_selector_backtest.json/md` 输出 20d/60d realized vol percentile 和 volatility expansion selector rows，并披露 false defensive / false risk-on counts。|
|TRADING-989|drawdown rule selector backtest|VALIDATING|`layer1_drawdown_rule_selector_backtest.json/md` 输出 drawdown guard metrics、drawdown reduction、missed rebound cost、late risk-on/off counts。|
|TRADING-990|combined simple-rule selector search|VALIDATING|`layer1_combined_simple_rule_selector_search.json/md` 搜索 trend、vol、drawdown、vote 和 soft-blend simple rules，不使用 ML、future outcome feature 或 unbounded parameter search。|
|TRADING-991|cost and latency stress|VALIDATING|`layer1_selector_cost_latency_stress.json/md` 覆盖 zero/low/medium/high cost、one/two-day lag、weekly/monthly/threshold rebalance scenarios。|
|TRADING-992|period split validation|VALIDATING|`layer1_selector_period_split_validation.json/md` 覆盖指定 periods/regimes，并输出 rank、relative metrics 和 period commentary。|
|TRADING-993|drawdown episode review|VALIDATING|`layer1_selector_drawdown_episode_review.json/md` 覆盖 2018Q4、2020 COVID、2022 bear、2023 recovery、2024 AI rally、largest QQQ drawdown 和 largest regret episode。|
|TRADING-994|regret attribution|VALIDATING|`layer1_selector_regret_attribution.json/md` 拆解 selected wrong component、late switch、over defensive/risk-on、chop、latency 和 cost regret。|
|TRADING-995|selector vs component ranking|VALIDATING|`layer1_selector_vs_component_baseline_ranking.json/md` 统一比较 always/static/reference/simple-rule selector，并输出 dominance/recommendation。|
|TRADING-996|overfit and sensitivity review|VALIDATING|`layer1_selector_overfit_sensitivity_review.json/md` 输出 perturbation score distribution、rank stability、metric degradation、fragile parameters 和 overfit risk。|
|TRADING-997|minimum holding period review|VALIDATING|`layer1_selector_minimum_holding_period_review.json/md` 比较 5/10/20/40/60 trading days 并给出 recommended minimum holding period。|
|TRADING-998|forward-aging watchlist gate|VALIDATING|`layer1_selector_forward_aging_watchlist_gate.json/md` 判断是否存在 research-only watchlist candidate，保持 paper/production/broker false/none。|
|TRADING-999|owner decision pack|VALIDATING|`layer1_selector_owner_decision_pack.json/md` 回答 owner 10 个问题并输出 recommendation。|
|TRADING-1000|master review|VALIDATING|`layer1_simple_rule_selector_master_review.json/md` 汇总 registry、ranking、watchlist gate 和下一阶段最小任务。|

## 新增 CLI

```bash
aits research strategies layer1-simple-rule-selector-registry-review
aits research strategies layer1-trend-rule-selector-backtest
aits research strategies layer1-volatility-rule-selector-backtest
aits research strategies layer1-drawdown-rule-selector-backtest
aits research strategies layer1-combined-simple-rule-selector-search
aits research strategies layer1-selector-cost-latency-stress
aits research strategies layer1-selector-period-split-validation
aits research strategies layer1-selector-drawdown-episode-review
aits research strategies layer1-selector-regret-attribution
aits research strategies layer1-selector-vs-component-baseline-ranking
aits research strategies layer1-selector-overfit-sensitivity-review
aits research strategies layer1-selector-minimum-holding-period-review
aits research strategies layer1-selector-forward-aging-watchlist-gate
aits research strategies layer1-selector-owner-decision-pack
aits research strategies layer1-simple-rule-selector-master-review
```

## Guardrails

- 所有 cached-data dependent 命令必须走同源 Layer-2 data-quality gate。
- 所有 selector feature 必须来自 decision-time price/return history，不得读取 future outcome、best label 或 oracle fields。
- Reference-only / inactive / excluded components 不得作为 selector output。
- ML selector、reinforcement learning、options、LEAPS、Wheel、tail-risk fallback 和 QQQ-plus growth selectable 继续禁止。
- Report Registry、Artifact Catalog 和 System Flow 必须同步更新。
- Reader Brief 不自动接入本批任务。

## 进展记录

- 2026-06-24: 新增需求文档并进入 `IN_PROGRESS`。实现范围为 TRADING-986～1000 全批 research-only historical study，不进入 paper-shadow、production 或 broker。
- 2026-06-24: 实现完成并转入 `VALIDATING`。新增 selector registry、15 个 CLI/report artifacts、report registry、artifact catalog、system flow 和 focused tests；真实 master smoke 为 `LAYER1_SELECTOR_RESEARCH_ONLY`，data quality=`PASS_WITH_WARNINGS`，actual range=`2022-12-01`～`2026-06-23`，watchlist 仍需 owner review；paper-shadow、production、broker 全部保持 false/none。
