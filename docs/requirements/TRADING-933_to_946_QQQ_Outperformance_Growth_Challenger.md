# TRADING-933 to 946 QQQ Outperformance Growth Challenger

最后更新：2026-06-25

## 背景

TRADING-923 to 932 已启动 `equal_risk_qqq_sgov` 的 research-only
forward-aging observation。该线定位为 defensive primary，不应该被增长型研究线
替代。本批新增独立的 QQQ-plus growth challenger 研究线，用来回答：

在 `QQQ` / `TQQQ` / `SGOV` 宇宙内，是否存在长期收益超过 `100_qqq`，同时
回撤、波动、换手、路径依赖和 regime concentration 仍可接受的候选策略。

默认研究 regime 仍为 `ai_after_chatgpt`，anchor date 为 2022-11-30，
默认 backtest start 为 2022-12-01。pre-2022 历史只允许用于 warm-up、
stress 或 regime comparison，不能作为 AI-cycle 主结论窗口。

## 安全边界

- `production_effect=none`
- `broker_action=none`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `manual_review_required=true`
- `uses_options=false`
- `uses_margin=false`
- `max_tqqq_weight<=0.40`
- `max_effective_qqq_exposure<=1.80`

本批只生成 research-only artifacts 和 owner decision pack，不创建
paper-shadow、production weights、broker/order、真实调仓建议，也不恢复
TQQQ-heavy、LEAPS 或 Wheel 主线。

## 阶段拆解

|任务|阶段|当前状态|验收标准|
|---|---|---|---|
|TRADING-933|objective contract|VALIDATING|新增 `aits research strategies qqq-outperformance-objective-contract`；定义 `100_qqq` primary benchmark、secondary benchmarks、收益/Calmar/Sharpe/drawdown/turnover/regime 门槛，并输出 `QQQ_OUTPERFORMANCE_CONTRACT_*` 状态。|
|TRADING-934|growth candidate registry|VALIDATING|新增 `config/research/qqq_plus_growth_candidate_registry.yaml` 和 registry review artifact；候选类型、约束、安全字段、生产边界可审计，不污染 simple baseline registry。|
|TRADING-935|controlled TQQQ overlay search|VALIDATING|搜索少量 TQQQ overlay 候选，输出 annual_return_vs_qqq、drawdown、effective exposure、TQQQ contribution、leverage drag、path dependency、Calmar edge。|
|TRADING-936|trend-gated leverage search|VALIDATING|只用 t 或更早可见的 QQQ 100DMA/200DMA、drawdown、realized volatility percentile 信号；禁止 future return/drawdown、tail-risk label 和 fallback_triggered。|
|TRADING-937|vol-targeted growth search|VALIDATING|按 target vol、realized vol window、max TQQQ、min SGOV、rebalance policy 搜索；收益超过 QQQ 但风险恶化时只能标 challenger。|
|TRADING-938|drawdown-guarded growth search|VALIDATING|研究上涨期增强、回撤期降仓；回答是否避开 2022 大回撤、是否错过 2023/2024 反弹和是否过度切换。|
|TRADING-939|outperformance ranking report|VALIDATING|统一比较 benchmarks、simple baseline challenger 和 935～938 候选，输出 top lists、dominated/non-dominated、growth watchlist。|
|TRADING-940|period split validation|VALIDATING|覆盖 2012-2015、2016-2019、2020-2021、2022、2023、2024、2025-to-latest、pre/post-2020、AI rally、rate-hike bear；只在 2023/2024 赢时标 `REGIME_CONCENTRATED`。|
|TRADING-941|drawdown replay|VALIDATING|覆盖 2018Q4、2020 COVID crash、2022 rate-hike bear、2023 recovery、2024 AI rally、largest QQQ/TQQQ drawdown；回答 TQQQ、risk-off、risk-on 和风险形态问题。|
|TRADING-942|growth edge significance review|VALIDATING|输出 annual return/Calmar/Sharpe edge、drawdown/turnover/TQQQ path dependency/complexity penalty 和 net_growth_edge_score，状态为 `GROWTH_EDGE_*`。|
|TRADING-943|forward-aging watchlist|VALIDATING|最多 1～2 个 growth challenger 进入 forward-aging watchlist，`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。|
|TRADING-944|risk budget review|VALIDATING|输出 effective QQQ beta、effective leverage、TQQQ/SGOV weight、vol、max drawdown、tail loss frequency 和 risk contribution，回答是否只是 beta > 1。|
|TRADING-945|role allocation review|VALIDATING|明确 `equal_risk_qqq_sgov=defensive core`、growth candidate=growth challenger、`100_qqq=risk reference`、`qqq_60_sgov_40=static comparator`。|
|TRADING-946|owner decision pack|VALIDATING|回答 10 个 owner 问题；确认 defensive primary 保留、是否新增 1 个 growth challenger、TQQQ-heavy 暂停、LEAPS/Wheel blocked、paper_shadow/production false。|

## 实施顺序

1. 目标与候选池：TRADING-933、934。
2. 增长候选搜索：TRADING-935、936、937、938。
3. 是否真的优于 QQQ：TRADING-939、940、941、942、944。
4. Watchlist 与 owner 决策：TRADING-943、945、946。

## 进展记录

- 2026-06-24: 新增需求拆解并进入 `IN_PROGRESS`。本批从
  `equal_risk_qqq_sgov` defensive forward-aging 旁路新增 growth challenger
  研究线，目标是验证是否存在风险后仍有意义的 QQQ outperformance，而不是
  替换 defensive primary。
- 2026-06-24: 实现完成并转入 `VALIDATING`。新增 QQQ-plus growth
  registry/config、14 个 `aits research strategies ...` CLI/artifacts、
  report registry、artifact catalog、system flow、owner decision pack 和
  focused tests。验证通过 focused parallel pytest 2 passed、report/task/
  documentation parallel pytest 27 passed、Ruff、compileall、`git diff --check`
  和真实 CLI smoke；实际 data-dependent smoke 返回
  `CONTROLLED_TQQQ_OVERLAY_SEARCH_READY` 且 `data_quality_status=PASS_WITH_WARNINGS`。
