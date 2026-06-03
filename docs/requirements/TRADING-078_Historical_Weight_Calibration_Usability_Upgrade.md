# TRADING-078 Historical Weight Calibration Usability Upgrade

最后更新：2026-06-03

## 状态

- 父任务：TRADING-078
- 当前状态：IN_PROGRESS
- 优先级：P0
- 下一责任方：系统实现
- 安全边界：`observe_only=true`、`candidate_only=true`、`production_effect=none`、`broker_action=none`、`manual_review_required=true`

## 背景

TRADING-071 已实现 dual-track ETF weight calibration，TRADING-076 / TRADING-077 已把 evidence dashboard 和 baseline review playbook 接入治理层。TRADING-078 的目标是把既有 historical calibration 能力整理成可操作的候选权重选择流程，回答：

```text
Based on historical data, what are the best candidate initial ETF allocation weights
that are robust enough to enter forward shadow observation?
```

本阶段只生成 historical candidate / comparison / recommendation / shadow enrollment suggestion artifacts，不自动替换 production weights，不触发 broker，不自动 promotion。

## 非目标

- 不实现 automatic production weight replacement。
- 不实现 broker execution、real account trading 或 automatic candidate promotion。
- 不引入 unbounded optimizer、ML optimizer replacement、paid data provider dependency、options strategy 或 LLM/news/EDGAR production weighting。
- 不把最高历史收益候选直接解释为 production baseline。

## 阶段拆解

|子任务|状态|验收标准|
|---|---|---|
|TRADING-078A Historical Data Range Presets|DONE|`config/etf_portfolio/weight_calibration_presets.yaml` 存在并可验证；presets 支持 rolling end-date policy、minimum coverage、benchmark set 和 safety fields。|
|TRADING-078B Weight Search Result Top-N Export|DONE|`aits etf weight-calibration export-top --latest/--run-id --top N` 输出 JSON / CSV / Markdown top-N candidates，保留 blockers、warnings、overfit risk、forward readiness 和 safety。|
|TRADING-078C Candidate Weight Comparison Table|READY|生成 candidate + benchmark comparison table，包含 current baseline、QQQ、SPY、SMH、static references、top-N candidates 和 deterministic ordering。|
|TRADING-078D Regime Robustness Heatmap Data|READY|生成 candidate/regime matrix JSON / CSV / Markdown，披露 sample count、confidence warning、weak regimes 和 safety。|
|TRADING-078E Overfit Risk Explanation|READY|为候选输出 human-readable overfit reasons、supporting metrics、blocking metrics 和 manual review note。|
|TRADING-078F Weight Candidate Shadow Enrollment Workflow|READY|`enroll-top` / `enroll` 只允许 shadow-ready candidates，保留 source links，不修改 production state。|
|TRADING-078G Initial Weight Recommendation Report|READY|生成 JSON / Markdown recommendation report，汇总 safety、run metadata、preset、top-N、comparison、regime robustness、overfit、forward readiness、shadow recommendation 和 next steps。|
|TRADING-078H Reader Brief Weight Candidate Section|READY|Reader Brief 只读 latest recommendation report，显示 top candidate、suggested action、overfit risk、blocked count、safety 和 detail link。|
|TRADING-078I Historical Calibration Usability Validation Gate|READY|`aits etf weight-calibration usability-validate` fail-closed 校验 A-H workflow、bounded search、安全字段和无 production mutation。|

## 设计决策

1. TRADING-078 是 TRADING-071 的 usability / workflow layer，复用既有 bounded search、candidate registry、forward enrollment、overfit diagnostics、dual-track report 和 validation gate，不新增优化模型。
2. Historical presets 默认使用 `ai_after_chatgpt` 研究语义，`ai_cycle_recent` 从 2022-12-01 开始；pre-2022 preset 只能用于 comparison / stress，而不能覆盖 AI-cycle 默认结论窗口。
3. Top-N、comparison、robustness、overfit 和 recommendation artifacts 必须全部固定 candidate-only safety fields，并披露 benchmark context 与 data range。
4. Shadow enrollment 仍通过 TRADING-071 enrollment ledger，不写 `target_weights.csv`、baseline config、shared experiment shadow registry 或 broker state。
5. Reader Brief 只读 latest recommendation report；缺失时显示 `MISSING`，不运行 `weight-calibration` 上游命令。

## 验收命令

最终运行：

```powershell
python -m pytest tests -q
python -m ruff check config src tests scripts docs
python -m compileall -q src tests scripts
git diff --check
python -m ai_trading_system.cli etf weight-calibration usability-validate
```

## 进展记录

- 2026-06-03: 新增任务文档并进入 IN_PROGRESS，原因：owner 提供 TRADING-078 计划，要求把 TRADING-071 historical calibration 结果整理为可复核、可比较、可 shadow enrollment 的 candidate-only workflow；本阶段固定 observe-only / candidate-only / manual-review-only，不应用权重、不触发 broker。
- 2026-06-03: TRADING-078A 完成。新增 `config/etf_portfolio/weight_calibration_presets.yaml`、preset schema/loader/resolver、`search --preset` CLI 接入和专项测试；search payload 记录 `historical_range_preset` 与 resolved requested date range，安全字段保持 mandatory。
- 2026-06-03: TRADING-078B 完成。新增 Top-N export schema/builder/writer/Markdown renderer、`aits etf weight-calibration export-top --latest/--run-id --top N` CLI 和专项测试；导出 JSON / CSV / Markdown，保留 overfit risk、forward readiness、blockers/warnings、benchmark context 和 safety fields。
