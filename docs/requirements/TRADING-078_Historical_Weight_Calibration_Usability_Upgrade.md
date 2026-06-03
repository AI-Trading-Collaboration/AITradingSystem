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
|TRADING-078C Candidate Weight Comparison Table|DONE|生成 candidate + benchmark comparison table，包含 current baseline、QQQ、SPY、SMH、static references、top-N candidates 和 deterministic ordering。|
|TRADING-078D Regime Robustness Heatmap Data|DONE|生成 candidate/regime matrix JSON / CSV / Markdown，披露 sample count、confidence warning、weak regimes 和 safety。|
|TRADING-078E Overfit Risk Explanation|DONE|`overfit-explain --latest/--run-id --top N` 输出 human-readable overfit reasons、supporting metrics、blocking metrics 和 manual review note。|
|TRADING-078F Weight Candidate Shadow Enrollment Workflow|DONE|`enroll-top` / `enroll` 只允许 shadow-ready candidates，保留 source links，不修改 production state。|
|TRADING-078G Initial Weight Recommendation Report|DONE|`recommendation --latest/--run-id --top N` 生成 JSON / Markdown recommendation report，汇总 safety、run metadata、preset、top-N、comparison、regime robustness、overfit、forward readiness、shadow recommendation 和 next steps。|
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
- 2026-06-03: TRADING-078C 完成。新增 candidate comparison table schema/builder/writer/Markdown renderer、`aits etf weight-calibration comparison --latest/--run-id --top N` CLI 和专项测试；comparison rows 包含 current baseline、Buy & Hold QQQ/SPY/SMH、static references、Top-N candidates、metric null reasons、overfit/readiness 和 safety fields。
- 2026-06-03: TRADING-078D 完成。新增 regime robustness heatmap schema/builder/writer/Markdown renderer、`aits etf weight-calibration regime-robustness --latest/--run-id --top N` CLI 和专项测试；candidate/regime matrix 覆盖 required regimes，缺失 slice 显式输出 `MISSING` / `REGIME_SLICE_MISSING`，并保留 sample count、constraint hit rate 和 safety fields。
- 2026-06-03: TRADING-078E 进入 IN_PROGRESS，原因：在既有 TRADING-071G overfit diagnostics 基础上补充可读解释层，让高收益但脆弱、极端或依赖单一时期的候选权重可被人工复核。
- 2026-06-03: TRADING-078E 完成。新增 overfit explanation schema/builder/writer/Markdown renderer、`aits etf weight-calibration overfit-explain --latest/--run-id --top N` CLI 和专项测试；输出 top overfit reasons、supporting metrics、blocking metrics、manual review note、readiness/blockers/warnings 和 safety fields。
- 2026-06-03: TRADING-078F 进入 IN_PROGRESS，原因：在既有 candidate registry / forward enrollment ledger 上补充 Top-N / weight_set_id 驱动的 shadow enrollment workflow，要求只允许 `shadow_ready` 候选并保留 source links。
- 2026-06-03: TRADING-078F 完成。新增 `enroll_top_weight_candidates_forward`、`aits etf weight-calibration enroll-top --latest/--run-id --top N`、`aits etf weight-calibration enroll --latest/--run-id --weight-set <id>`、source link preserving enrollment results 和专项测试；blocked / non-shadow-ready candidates fail closed，ledger 保持 production_effect=none / broker_action=none。
- 2026-06-03: TRADING-078G 进入 IN_PROGRESS，原因：需要把 A-F 产物汇总成 candidate-only initial weight recommendation report，给人工 review 提供 top candidates、benchmark comparison、regime robustness、overfit explanation 和 shadow enrollment recommendation。
- 2026-06-03: TRADING-078G 完成。新增 recommendation report schema/builder/writer/Markdown renderer、`aits etf weight-calibration recommendation --latest/--run-id --top N` CLI 和专项测试；报告汇总 safety、run metadata、preset/date range、search constraints、Top-N、comparison、regime robustness、overfit explanations、forward readiness、shadow enrollment recommendation、source artifacts 和 next steps。
