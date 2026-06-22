# TRADING-879～887 Simple Baseline Real Run Decision Pack

## 背景

TRADING-865～878 已建立 QQQ / TQQQ / SGOV simple baseline portfolio control
research CLI 和 artifacts。本批不继续扩大搜索空间，目标是用真实缓存运行结果收敛候选，
解释 dominance 与风险来源，并输出 owner 人工决策包和观察名单。

默认解释窗口仍使用 `ai_after_chatgpt` regime：anchor event 为 2022-11-30
ChatGPT 公开发布，默认回测开始日为 2022-12-01。2018、2020、2022、2023、
2024 只用于历史关键回撤/恢复段复盘或 regime 对比；若结论依赖 pre-2022 数据，必须
明确标注用途。

## 安全边界

本批所有输出固定为 research-only / observe-only：

- `production_effect=none`
- `broker_action=none`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `manual_review_required=true`
- `paper_shadow_allowed=false`
- `production_allowed=false`

不得 merge main，不得修改 production config，不得创建 paper-shadow，不得生成 broker
action 或订单建议。TRADING-887 只建立 watchlist，且每行必须保持
`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。

## 任务拆解

|任务|范围|状态|
|---|---|---|
|TRADING-879|确认 `codex/simple-baseline-portfolio-control` 已 push，并输出 PR 前检查摘要|VALIDATING|
|TRADING-880|真实运行 TRADING-865～878 全部 CLI，并生成 real run summary JSON/Markdown|VALIDATING|
|TRADING-881|从真实运行结果提取 top 5～10 候选，并按风险/收益/简单性/TQQQ 分组|VALIDATING|
|TRADING-882|解释 dominated 策略被支配的原因和支配者|VALIDATING|
|TRADING-883|复盘主要候选在 2018Q4、2020 COVID、2022 rate-hike、2023 recovery、2024 AI rally 的回撤段|VALIDATING|
|TRADING-884|拆解收益来源，区分 QQQ beta、TQQQ exposure、SGOV carry、rebalance/filter/cost 贡献|VALIDATING|
|TRADING-885|仅对 top candidates 做参数扰动和 rank stability review|VALIDATING|
|TRADING-886|生成 owner decision pack，回答是否收敛、是否 forward aging、TQQQ/option blocked 状态|VALIDATING|
|TRADING-887|建立 paper-shadow watchlist，但不激活 paper-shadow 或 production|VALIDATING|

## 输出

- `outputs/research_strategies/simple_baselines/simple_baseline_pr_readiness_summary.json/md`
- `outputs/research_strategies/simple_baselines/simple_baseline_real_run_summary.json/md`
- `outputs/research_strategies/simple_baselines/simple_baseline_top_candidate_extraction_report.json/md`
- `outputs/research_strategies/simple_baselines/simple_baseline_dominance_explanation.json/md`
- `outputs/research_strategies/simple_baselines/simple_baseline_drawdown_episode_replay.json/md`
- `outputs/research_strategies/simple_baselines/simple_baseline_exposure_decomposition_report.json/md`
- `outputs/research_strategies/simple_baselines/simple_baseline_parameter_robustness_top_candidates.json/md`
- `outputs/research_strategies/simple_baselines/simple_baseline_owner_decision_pack.json/md`
- `outputs/research_strategies/simple_baselines/simple_baseline_paper_shadow_watchlist.json/md`

## 验收标准

- TRADING-879 确认当前 branch、`c57cb082`、clean status 和 push 结果；不得 merge main。
- TRADING-880 逐条记录 865～878 CLI command、exit code、status、artifact path、data quality
  status、关键 summary 和 blocker/warning。
- TRADING-881 每个候选输出 `strategy_id`、`annual_return`、`max_drawdown`、`sharpe`、
  `calmar`、`turnover`、`tqqq_weight`、`sgov_weight`、`dominance_status`、`pit_status`、
  `regime_status`、`cost_status`、`owner_comment`。
- TRADING-882 对每个 dominated strategy 解释收益、回撤、Sharpe / Calmar、换手、TQQQ
  暴露、regime concentration 和被哪个更简单策略支配。
- TRADING-883 关键回撤段输出 weight path、max drawdown、recovery days、是否切 SGOV、
  是否过度防御、是否错过反弹，以及相对 100% QQQ 和 best static baseline 的表现。
- TRADING-884 输出收益来源拆解，回答候选是真有仓位控制能力，还是只是更高/更低 QQQ
  exposure。
- TRADING-885 仅对 top candidates 做 100DMA / 200DMA、vol threshold、drawdown threshold、
  TQQQ max weight 和 rebalance frequency 扰动，输出 robustness score、fragile parameters、
  rank stability 和 performance degradation。
- TRADING-886 必须回答 owner 提出的 8 个决策问题，并保持 tail-risk fallback、LEAPS/Wheel
  blocked 状态除非 owner 明确解除。
- TRADING-887 watchlist 必须固定 `paper_shadow_allowed=false`、`production_allowed=false`、
  `broker_action=none`。

## 进展记录

- 2026-06-23: 新增本批任务文档。TRADING-879 已开始执行，branch/commit/clean status
  检查通过并已 push 到 `origin/codex/simple-baseline-portfolio-control`；后续仍需生成 PR
  readiness summary artifact，并继续真实运行 865～878 CLI。
- 2026-06-23: 真实运行完成并转入 VALIDATING。初次 865～878 数据依赖 CLI 因主价格缓存
  缺 `TQQQ` fail-closed；随后通过既有 FMP price repair / manifest 路径执行
  `aits data repair-backtest-inputs --date 2026-06-18 --price-only --symbols TQQQ
  --price-provider fmp`，补入 TQQQ 1008 行，重跑 `validate-data` 为
  `PASS_WITH_WARNINGS` / 0 errors。865～878 全部真实 CLI 生成 artifacts；
  ranking=`BASELINE_RANKING_READY`，top recommended=`equal_risk_qqq_sgov`，
  readiness=`PAPER_SHADOW_REVIEWABLE_LATER` 但仍固定 paper-shadow/production/broker
  false/none，master=`PAUSE_TQQQ_HEAVY`，options=`OPTIONS_RESEARCH_BLOCKED`。
  879～887 指定 JSON/Markdown 均已生成；watchlist 只观察不激活。
