# TRADING-1009 to 1014 Layer-1 Low-Turnover Selector Refinement

最后更新：2026-06-25

## 背景

TRADING-1001～1008 的真实结果显示 `trend_200dma_selector` 有成本后 edge，但 owner watchlist review 因 `TOO_MUCH_TURNOVER` 保持 `KEEP_SELECTOR_RESEARCH_ONLY`。

本批任务不扩大到任意 simple-rule selector 搜索，研究路线收敛为：围绕 `trend_200dma_selector` 做低换手改造，先解释和降低 turnover，再判断是否有版本可进入 research-only forward-aging review。

## 阶段拆解

|任务|阶段|当前状态|验收标准|
|---|---|---|---|
|TRADING-1009|turnover source diagnosis|VALIDATING|新增 `layer1_selector_turnover_source_diagnosis.json/md`，逐笔输出 `switch_date`、`from_component`、`to_component`、`market_state`、`subsequent_20d_outcome`、`switch_was_helpful`、`switch_was_noise`、`turnover_cost`，回答切换年份、200DMA 附近横跳、缺少 buffer、minimum holding、trend confirmation 和噪音切换贡献。|
|TRADING-1010|buffered 200DMA variants|VALIDATING|新增 `layer1_selector_buffered_200dma_variants.json/md`，比较 buffer=1%/2%/3%/5% 与 confirmation_days=3/5/10/20 的 200DMA 变体，输出 turnover reduction、net return after cost、max drawdown、missed rebound cost、late risk-off cost。|
|TRADING-1011|minimum holding and cooldown review|VALIDATING|新增 `layer1_selector_min_holding_cooldown_review.json/md`，比较 minimum_holding_period=20/40/60d、cooldown_after_switch=5/10/20d、max_switches_per_year=3/4/6，判断是否降低 `TOO_MUCH_TURNOVER` 且不明显损害 Calmar / drawdown control。|
|TRADING-1012|soft blend review|VALIDATING|新增 `layer1_selector_soft_blend_review.json/md`，测试 hard switch 替代方案，输出 `blend_weight_path`、turnover、net return after cost、drawdown、missed upside、regret vs best component。|
|TRADING-1013|low-turnover selector ranking|VALIDATING|新增 `layer1_selector_low_turnover_ranking.json/md`，统一比较 original、buffered、confirmed、minimum holding、cooldown、soft blend、always baselines，输出 top_by_net_return、top_by_calmar、top_by_low_turnover、top_by_regret_reduction、dominated_variants、recommended_low_turnover_candidate。|
|TRADING-1014|low-turnover owner decision pack|VALIDATING|新增 `layer1_selector_low_turnover_owner_decision_pack.json/md` 和 `docs/research/layer1_selector_low_turnover_owner_decision_pack.md`，判断是否存在 `LOW_TURNOVER_SELECTOR_REVIEWABLE` 候选；否则保持 `KEEP_SELECTOR_DRY_RUN_ONLY` / `NO_SELECTOR_EDGE` / `BLOCKED`。|

## 新增 CLI

```bash
aits research strategies layer1-selector-turnover-source-diagnosis
aits research strategies layer1-selector-buffered-200dma-variants
aits research strategies layer1-selector-min-holding-cooldown-review
aits research strategies layer1-selector-soft-blend-review
aits research strategies layer1-selector-low-turnover-ranking
aits research strategies layer1-selector-low-turnover-owner-decision-pack
```

## Pilot 参数治理

本批引入的 buffer、confirmation、minimum holding、cooldown、max switches 和 soft blend 权重均为 research-only pilot baseline，不是 calibrated production policy：

- buffer grid: 1%、2%、3%、5%；
- confirmation_days grid: 3、5、10、20；
- minimum_holding_period grid: 20、40、60 trading days；
- cooldown_after_switch grid: 5、10、20 trading days；
- max_switches_per_year grid: 3、4、6；
- soft blend weights: strong risk-on 80% 100_qqq / 20% equal_risk，normal 50% / 50%，risk-off 20% / 80%。

退出条件：若 owner 要将任一低换手参数作为正式 observation / paper-shadow / production gate，必须迁移到 reviewed policy/config manifest，并补充样本外或 forward-aging evidence。

## Guardrails

- Formal selectable components 仍只允许 `equal_risk_qqq_sgov` 与 `100_qqq`。
- `qqq_50_sgov_50` / `qqq_60_sgov_40` 只能作为 reference-only。
- QQQ-plus growth、TQQQ-heavy、tail-risk fallback、LEAPS、Wheel 和 Options 继续排除。
- 所有 cached-data dependent 命令必须走同源 `validate-data` 质量门禁路径，并在输出中披露 data quality。
- 本批只做 research-only low-turnover review，不写正式 forward-aging observation，不进入 paper-shadow、production 或 broker。
- 所有 outputs 固定 `paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`、`manual_review_required=true`。

## 进展记录

- 2026-06-25: 新增需求文档并进入 `IN_PROGRESS`。实现范围为 TRADING-1009～1014 turnover source diagnosis、buffer / confirmation variants、minimum holding / cooldown review、soft blend review、low-turnover ranking 和 owner decision pack；研究路线从任意 simple-rule selector 收敛为解决 `trend_200dma_selector` 的 `TOO_MUCH_TURNOVER`。
- 2026-06-25: 实现完成并转入 `VALIDATING`。真实 CLI 输出：1009=`TURNOVER_NOISE_DOMINANT`，switch_count=6，noise_switch_count=4，near_200dma_switch_share=0.5，buffer / confirmation / minimum-holding 缺口均为 likely；1010 生成 8 个 buffer/confirmation variants；1011 生成 27 个 holding/cooldown/max-switch variants；1012 soft blend 生成全量 `blend_weight_path`；1013 推荐 `soft_blend_200dma_three_state` 作为低换手候选但 `always_100_qqq` 仍为 top_by_net_return / top_by_low_turnover；1014=`KEEP_SELECTOR_DRY_RUN_ONLY`，原因是 recommended candidate turnover 从 6.0 降到 3.6 但 switch_count_controlled 未通过。所有 safety fields 仍 false/none。
- 2026-06-25: 验证通过并继续保持 `VALIDATING`，等待 owner 复核是否接受 `KEEP_SELECTOR_DRY_RUN_ONLY` 和继续解决 switch-count blocker。验证包括真实 1009～1014 CLI、focused Layer-1 pytest、Layer-2 regression pytest、task/report/documentation pytest、Ruff、compileall 和 `git diff --check`。
