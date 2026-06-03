# TRADING-076 Strategy Evidence Dashboard

最后更新：2026-06-03

## 状态

- 父任务：TRADING-076
- 当前状态：BASELINE_DONE
- 优先级：P0
- 下一责任方：系统实现
- 安全边界：`observe_only=true`、`candidate_only=true`、`production_effect=none`、`broker_action=none`、`manual_review_required=true`

## 背景

TRADING-075 已完成 ETF data quality / staleness governance baseline。当前系统已经有 ETF baseline、historical weight calibration、forward simulation、weekly review、decision journal、parameter review、AI confirmation / attribution、satellite replacement / attribution、operations health、data-quality governance、Reader Brief、report registry 和 artifact catalog。

TRADING-076 的目标是把这些分散报告汇总为单一研究证据视图，回答：

```text
Across all system modules, what evidence currently supports or rejects each strategy component?
```

本阶段是 evidence aggregation/dashboard stage，不新增交易信号，不修改 production allocation，不自动 candidate promotion，不触发 broker action。

## 非目标

- 不实现 broker execution。
- 不写 production weights。
- 不自动 baseline replacement。
- 不自动 candidate promotion。
- 不新增 strategy signal generation。
- 不新增 ML model。
- 不接入 paid vendor。
- 不把 LLM/news/EDGAR 变成 production weighting。

## 阶段拆解

|子任务|状态|验收标准|
|---|---|---|
|TRADING-076A Evidence Dashboard Schema|VALIDATING|Dashboard schema 与 evidence card schema 存在；safety fields mandatory；missing source link、invalid status、unsafe production effect fail closed；JSON serialization stable。|
|TRADING-076B Evidence Source Registry|VALIDATING|新增 `config/etf_portfolio/evidence_dashboard.yaml`；sources/categories/freshness/quality/manual priority/safety 可加载可校验；required source 必须有 report_id。|
|TRADING-076C Evidence Aggregator|VALIDATING|聚合 report registry/index 中的 ETF baseline、weight calibration、forward、AI、satellite、weekly/journal/parameter/data-quality/ops/validation gate evidence；missing/stale/blocked evidence 显式保留 source links。|
|TRADING-076D Strategy Component Evidence Cards|VALIDATING|生成 ETF baseline、weight calibration、forward、AI confirmation/attribution、satellite replacement/attribution、parameter review、decision journal、data quality、operations health cards。|
|TRADING-076E Candidate Evidence Ranking|VALIDATING|按 forward performance、drawdown、turnover、stability、attribution、journal、data quality、freshness 和 sample size 生成 candidate ranking；数据质量 blocked 或样本不足不得高排。|
|TRADING-076F Evidence Conflict and Data Quality Overlay|VALIDATING|检测 backtest-forward、AI-data-quality、satellite-attribution、journal-proposal、ops-validation 等冲突，并输出 manual review action。|
|TRADING-076G Manual Review Priority Queue|VALIDATING|汇总 critical data blockers、stale gates、evidence conflicts、high-risk proposals、underperformance/redundancy/journal deferred items；allowed actions 仅限人工复核/继续观察/请求更多数据/拒绝/新实验。|
|TRADING-076H Strategy Evidence Dashboard Report|VALIDATING|生成 JSON / Markdown dashboard，包含 safety banner、metadata、overall status、cards、ranking、conflicts、data-quality overlay、validation gate summary、manual queue、source links 和 next steps。|
|TRADING-076I Reader Brief Strategy Evidence Section|VALIDATING|Reader Brief 只读展示 dashboard overall status、strongest/weakest evidence、blocking issues、manual review priority count、data quality status 和详细报告链接；缺失时 graceful degradation。|
|TRADING-076J Strategy Evidence Dashboard Validation Gate|VALIDATING|`aits etf evidence-dashboard validate` fail-closed 校验 schema、registry、aggregator、cards、ranking、conflict overlay、manual queue、report generator、Reader Brief integration、traceability 和 safety。|

## 设计决策

1. Dashboard 作为 ETF research evidence aggregator，不复用根级 `src/ai_trading_system/evidence_dashboard.py` 的 daily-score trace drill-down 语义；新增 ETF 专用模块和 report id，避免两个 dashboard 混淆。
2. Source registry 只描述 existing report artifacts 和 freshness/quality expectations，不运行上游命令。
3. Aggregator 默认读取 `config/report_registry.yaml` / report index 指向的 latest artifacts，并保留 missing、stale、blocked、optional-missing 状态。
4. Evidence card 结论必须带 source report path、source metric、freshness、data quality、validation 和 sample-size context。无法定位 source link 的 card 不合法。
5. Ranking 是 evidence-quality ranking，不是 return-only ranking；blocked data quality、stale required source 和 insufficient sample 会降低或阻断 ranking。
6. Manual review queue 只产生人工复核动作，不产生 `place_order`、`promote_to_production`、`change_production_weights` 等 action。
7. Reader Brief 只读 latest `etf_strategy_evidence_dashboard` report，不运行 evidence-dashboard CLI，不补造结论。

## 验收命令

最终运行：

```powershell
python -m pytest tests -q
python -m ruff check config src tests scripts docs
python -m compileall -q src tests scripts
git diff --check
python -m ai_trading_system.cli etf evidence-dashboard validate
```

## 进展记录

- 2026-06-03: 新增任务文档并进入 IN_PROGRESS，原因：owner 提供 TRADING-076 开发计划，要求把各 ETF strategy research modules 的支持/阻断/ stale / data-quality evidence 汇总成可审计研究 dashboard，并输出 owner manual review priorities。
- 2026-06-03: 完成 A-J baseline implementation，新增 source registry、strategy evidence dashboard module、CLI aggregate/report/validate、report registry entries、Reader Brief section、focused tests 和文档更新；进入 full validation。
- 2026-06-03: 从 IN_PROGRESS 改为 BASELINE_DONE，原因：全量 pytest（2057 passed）、ruff、compileall、diff check 和 `aits etf evidence-dashboard validate` 均通过；实际 `aits etf evidence-dashboard report --as-of 2026-06-03` 生成 dashboard，current local `overall_status=blocked` 由缺失/陈旧 source evidence 显式暴露，留给 owner review / artifact refresh，不自动绕过。
