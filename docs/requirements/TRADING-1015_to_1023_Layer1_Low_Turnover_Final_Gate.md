# TRADING-1015 to 1023 Layer-1 Low-Turnover Final Gate

最后更新：2026-06-25

## 背景

TRADING-1009～1014 已把 Layer-1 selector 的低换手研究收敛到
`soft_blend_200dma_three_state`，但 owner pack 因 `switch_count_controlled`
定义不清而保持 `KEEP_SELECTOR_DRY_RUN_ONLY`。本批只做 1015～1023，不扩大
selector family，核心问题是：

```text
soft_blend_200dma_three_state 是否能在严格换手约束下仍保持优势。
```

如果不能通过最终门禁，Layer-1 selector 继续保持 research-only / dry-run
only / not forward-aging，主线继续回到 `equal_risk_qqq_sgov` forward-aging 与
第二层候选库观察。

## 阶段拆解

|任务|阶段|当前状态|验收标准|
|---|---|---|---|
|TRADING-1015|switch-count threshold contract|VALIDATING|在 reviewed research-only policy/config 中定义 `switch_count_controlled`：`max_switches_per_year <= 2`、`max_switches_per_3y <= 6`、`max_turnover_per_year <= 1.0`、`min_avg_holding_period >= 60 trading days`、`allowed_exception_cases`；所有后续报告输出 contract pass/fail 与失败原因。|
|TRADING-1016|soft-blend constrained search|VALIDATING|只围绕 `soft_blend_200dma_three_state` 搜索 risk-on/neutral/risk-off blend、buffer 和 confirmation_days 小网格，输出 net return after cost、drawdown、Calmar、Sharpe、switch_count、turnover、avg_holding_period、missed_rebound_cost 和 late_risk_off_cost。|
|TRADING-1017|monthly-only selector review|VALIDATING|比较 daily signal/monthly execution、monthly signal/monthly execution、threshold signal/monthly execution，判断降频到 monthly 是否自然解决 turnover noise。|
|TRADING-1018|no-flip-zone hysteresis review|VALIDATING|评估 200DMA ±3% no-flip zone，输出 switch_count reduction、late_risk_off_cost、missed_rebound_cost 和 chop reduction。|
|TRADING-1019|switch-quality attribution|VALIDATING|对低换手候选逐次输出 `switch_date`、`from_state`、`to_state`、20d/60d outcome、`switch_benefit_vs_not_switch`、`turnover_cost`、`net_switch_value`。|
|TRADING-1020|low-turnover finalist ranking|VALIDATING|统一排序 original trend、soft blend、monthly soft blend、hysteresis soft blend、confirmed soft blend、min-holding soft blend，输出指定维度和 `LOW_TURNOVER_FINALIST_FOUND` / `LOW_TURNOVER_NO_ACCEPTABLE_SELECTOR` / `LOW_TURNOVER_INCONCLUSIVE`。|
|TRADING-1021|selector vs simple components final gate|VALIDATING|比较 `always_equal_risk`、`always_100_qqq`、`qqq_50_sgov_50`、`qqq_60_sgov_40` 与 best low-turnover selector；若 selector 只优于 equal risk、不优于 100 QQQ 且 turnover 更高，则不得进入 forward-aging。|
|TRADING-1022|selector forward-aging watchlist final review|VALIDATING|只有 1020/1021 同时通过时，才允许 research-only forward-aging watchlist review；始终保持 `paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`，否则输出 `KEEP_SELECTOR_DRY_RUN_ONLY`。|
|TRADING-1023|pause-or-continue owner pack|VALIDATING|生成 owner 决策包，回答是否继续 Layer-1 selector、是否只保留 dry-run、是否有低换手候选进入 forward-aging、是否需要更长历史、是否继续禁止 ML selector、是否继续以 equal risk forward-aging 为主线。|

## 新增 CLI

```bash
aits research strategies layer1-selector-switch-count-threshold-contract
aits research strategies layer1-selector-soft-blend-constrained-search
aits research strategies layer1-selector-monthly-only-review
aits research strategies layer1-selector-hysteresis-review
aits research strategies layer1-selector-switch-quality-attribution
aits research strategies layer1-selector-low-turnover-finalist-ranking
aits research strategies layer1-selector-vs-simple-components-final-gate
aits research strategies layer1-selector-forward-aging-watchlist-final-review
aits research strategies layer1-selector-pause-or-continue-owner-pack
```

## Guardrails

- 本批只围绕 `soft_blend_200dma_three_state` 及其月度、迟滞、确认、最小持有
  低换手变体，不扩大策略族。
- Formal selectable components 仍只允许 `equal_risk_qqq_sgov` 与 `100_qqq`。
- `qqq_50_sgov_50` / `qqq_60_sgov_40` 只能作为 reference comparison。
- QQQ-plus growth、TQQQ-heavy、tail-risk fallback、LEAPS、Wheel、Options、ML
  selector 和 reinforcement learning 继续排除。
- 所有 cached-data dependent 命令必须走同源 `validate-data` 质量门禁路径，并在
  输出中披露 data quality。
- 所有 outputs 固定 `paper_shadow_allowed=false`、`production_allowed=false`、
  `broker_action=none`、`manual_review_required=true`。
- 任何 forward-aging 结论只允许 research-only watchlist review，不写正式
  observation，不进入 paper-shadow、production 或 broker。

## 进展记录

- 2026-06-25: 新增需求文档并进入 `IN_PROGRESS`。实现范围限定为 TRADING-1015～1023，
  不扩展 selector family，优先明确 `switch_count_controlled` contract，再做低换手
  final gate 和 owner pack。
- 2026-06-25: 实现完成并转入 `VALIDATING`。真实 CLI 输出：1015=`SWITCH_COUNT_CONTRACT_READY`，
  contract=`max_switches_per_year=2` / `max_switches_per_3y=6` /
  `max_turnover_per_year=1.0` / `min_avg_holding_period=60`；1016=243 rows，
  `SOFT_BLEND_CONSTRAINED_SEARCH_REVIEWED`，`switch_count_controlled_count=0`；
  1017=`MONTHLY_ONLY_SELECTOR_REVIEWED`，monthly execution 未自然解决 turnover noise；
  1018=`HYSTERESIS_REVIEWED`；1019=`SWITCH_QUALITY_ATTRIBUTION_READY`，candidate
  `min_holding_soft_blend` 的 8 次 switch 中 3 次 net positive、5 次 noise/negative；
  1020=`LOW_TURNOVER_FINALIST_FOUND`，best=`min_holding_soft_blend`；1021=`SELECTOR_FINAL_GATE_FAIL_KEEP_DRY_RUN`，
  原因是 selector 优于 equal risk 但不优于 100 QQQ 且 turnover 更高；1022=`KEEP_SELECTOR_DRY_RUN_ONLY`；
  1023 recommendation=`KEEP_SELECTOR_DRY_RUN_ONLY_AND_CONTINUE_EQUAL_RISK_FORWARD_AGING`。
  data quality=`PASS_WITH_WARNINGS`，actual range=`2022-12-01`～`2026-06-24`，所有 safety fields 仍 false/none。
