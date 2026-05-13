# 市场反馈优化闭环初版设计

状态：VALIDATING

最后更新：2026-05-13

关联任务：`CALIBRATION-003`、`FEEDBACK-002`、`EXPERIMENT-001`、`SHADOW-002`、`SHADOW-003`、`GOV-003`、`LOOP-001`

## 背景

2026-05-11 收盘后数据和 2026-05-12 盘中回调的复盘暴露了一个结构问题：系统能通过估值、风险预算和置信度 gate 看到“脆弱性较高、不要主动加仓”，但长期输出 40% 限制仓位也会降低结论分辨率。当前 `if/else` 或硬阈值若没有历史依据、样本约束和后续市场反馈，就不具备稳定迭代能力。

本任务目标是建立独立的市场反馈优化系统：任何信息进入判断后都要能被记录、观察、复盘和校准；权重、gate、thesis、风险事件、估值拥挤等信息的贡献应来自历史积累和后续市场表现，而不是一次性的主观规则。

## 目标

- 建立独立于 `score-daily` 的反馈优化编排层，消费既有 `decision_snapshot`、`prediction_ledger`、outcome、因果链、学习队列、候选规则、shadow maturity 和 calibration overlay。
- 默认聚焦 `ai_after_chatgpt` 市场阶段，as-if 回放默认起点为 2022-12-01；更早历史只用于 warm-up、压力测试或阶段对比，不能作为默认 AI 周期结论。
- 用固定报告回答：样本是否足够、错误判断如何归因、候选规则是否需要 replay/shadow、是否存在可审计 overlay、下一步是否允许进入权重诊断。
- 保持 `production_effect=none`：初版不直接改变正式评分、仓位 gate、日报结论、回测仓位或 rule card。
- 把周/月执行频次纳入运行手册，与现有 daily-run、投资复盘、反馈闭环复核兼容。

## 非目标

- 初版不自动拟合新权重，不自动改 `config/scoring_rules.yaml`。
- 初版不把单次错误复盘转成生产规则。
- 初版不把样本不足的回测或 shadow 结果写成 promotion 通过。
- 初版不替代 `aits validate-data`，也不绕过 outcome 生成命令中的数据质量门禁。

## 设计原则

- 只有数据质量失败、未来函数、来源不可信、thesis 被证伪或 owner 明确禁止的事项应作为硬 gate；其他信息优先参数化、可分桶、可回放和可校准。
- 后验 outcome 只能成为未来 prior，不能改写 signal-time 的证据、因果链或输入快照。
- 权重或规则调整必须经过候选、as-if replay、forward shadow、owner approval 和回滚条件登记。
- 判断错误和判断正确都要进入复盘；只复盘失败样本会导致过度调参。
- 报告必须给出时间范围内的结论，不能把跨阶段历史混成单一结论。

## 初版架构

```text
daily-run / score-daily
  -> decision_snapshot + prediction_ledger + trace
  -> feedback calibrate / calibrate-predictions
  -> build-causal-chain
  -> build-learning-queue
  -> build-rule-experiments
  -> run-shadow / shadow-maturity
  -> optimize-market-feedback
  -> candidate diagnostics / overlay governance（后续阶段）
```

初版新增命令：

```text
aits feedback optimize-market-feedback --as-of YYYY-MM-DD
```

主要输入：

- `outputs/reports/data_quality_YYYY-MM-DD.md`
- `data/processed/decision_outcomes.csv`
- `data/processed/prediction_outcomes.csv`
- `data/processed/decision_causal_chains.json`
- `data/processed/decision_learning_queue.json`
- `data/processed/rule_experiments.json`
- `outputs/reports/shadow_maturity_YYYY-MM-DD.md`
- `data/processed/approved_calibration_overlay.json`
- `outputs/current_effective_weights.json`

主要输出：

- `outputs/reports/market_feedback_optimization_YYYY-MM-DD.md`

## 执行频次

|频次|建议命令|目的|
|---|---|---|
|每周|`aits feedback optimize-market-feedback --as-of YYYY-MM-DD`|检查 outcome、学习队列、候选规则、shadow 和 overlay readiness。|
|每周上游|`aits feedback calibrate`、`aits feedback calibrate-predictions`、`aits feedback build-causal-chain`、`aits feedback build-learning-queue`、`aits feedback build-rule-experiments`|先生成市场反馈优化报告依赖的可审计产物。|
|每月|`aits feedback optimize-market-feedback --as-of YYYY-MM-DD --replay-start 2022-12-01 --replay-end YYYY-MM-DD`|固定 AI regime 窗口复核样本成熟度、错误归因和候选规则是否可以进入 replay/shadow/owner review。|

该流程不放进 `daily-run` 阻断链。日常生产仍先由 `daily-run` 生成正式日报和快照；反馈优化在盘后或周末读取这些产物做复核。

## Readiness 口径

|Readiness|含义|允许动作|
|---|---|---|
|`INSUFFICIENT_REPORTING_SAMPLE`|decision 或 prediction outcome 未达到 reporting floor|只展示缺口，不启动后续流程。|
|`INSUFFICIENT_DECISION_PILOT_SAMPLE`|decision outcome 未达到 pilot floor|继续收集，不启动学习队列和候选整理。|
|`INSUFFICIENT_FORWARD_SHADOW_PILOT_SAMPLE`|prediction/shadow outcome 未达到 pilot floor|继续收集，不启动 shadow 复核。|
|`PILOT_DIAGNOSTIC_REVIEW`|样本达到 pilot floor 但未达到 diagnostic floor|允许跑 causal chain、learning queue、rule experiment 候选整理和 pilot 复盘；不得输出正式调权结论。|
|`READY_FOR_REPLAY_OR_SHADOW_REVIEW`|有候选规则待 replay 或 forward shadow|先跑 as-if replay 或 shadow，不改 production。|
|`READY_FOR_APPROVED_OVERLAY_AUDIT`|存在已批准 overlay|审计命中上下文、有效期、回滚条件和解释。|
|`READY_FOR_WEIGHT_DIAGNOSTIC_REVIEW`|样本达到 diagnostic floor 且无未完成 replay/shadow|可以设计候选 weight diagnostics，但仍保持 candidate-only。|

样本政策不再使用硬编码 `30`。初版新增 `config/feedback_sample_policy.yaml`，把门槛拆成四层：

|层级|Decision outcome|Prediction/shadow outcome|含义|
|---|---:|---:|---|
|Reporting floor|1|1|只允许展示覆盖状态和缺口。|
|Pilot floor|5|2|当前样本少时允许启动 pilot 复盘、因果链、学习队列和候选整理。|
|Diagnostic floor|30|30|允许输出 weight diagnostics 和候选 overlay 设计，但仍不能自动上线。|
|Promotion floor|60|30|进入生产变更复核的最低样本下限；仍必须通过 replay、forward shadow、owner approval 和回滚条件。|

当前放宽只影响 pilot 流程启动，不改变 production 结论。若后续周度报告显示候选生成过快、噪声过高或反复误报，可直接提高 `pilot_floor`、提高 `review_after_reports` 或把周度执行改为双周/月度。

## 与既有流程兼容

- `score-daily` 继续输出正式结论；本系统只读产物，不改变 daily score。
- `feedback loop-review` 继续做周期复核总览；`optimize-market-feedback` 负责更明确地判断样本、候选、overlay 和执行频次。
- `investment-review` 可引用该报告，但不得把 readiness 当作交易指令。
- calibration overlay 仍由 `apply-calibration-overlay` 和后续 governance 控制；本系统不能直接写 production rule。
- 若后续接入自动调度，先按周/月执行；不在每日生产链路里 fail closed。

## 阶段拆解

|阶段|状态|验收|
|---|---|---|
|1. 文档、任务登记和只读编排报告|VALIDATING|新增需求文档、任务登记、系统流图、runbook 频次、CLI 命令和单测；命令可在现有产物上跑通并输出中文报告。|
|2. 样本政策配置化和 pilot 放宽|VALIDATING|新增 `config/feedback_sample_policy.yaml`，区分 reporting/pilot/diagnostic/promotion floor；当前少量样本可启动后续 pilot 流程，但报告保留不能晋级 production 的限制。|
|3. Weight diagnostics 候选|PROPOSED|按模块、gate、horizon 和市场阶段输出信息权重诊断；样本不足时只给 limitation，不给调权建议。|
|4. As-if replay 实验入口|PROPOSED|对候选规则/权重生成 replay spec，比较 baseline vs challenger 的 missed upside、avoided drawdown、turnover 和超额收益。|
|5. Forward shadow 与 promotion 连接|PROPOSED|候选通过 replay 后进入 `prediction_ledger` shadow；成熟度报告达到门槛才允许 owner review。|
|6. Overlay / rule card 晋级|PROPOSED|通过 owner approval 后生成 approved overlay 或 rule card，具备有效期、回滚条件和审计引用。|

## 验收标准

- 新命令在缺少 overlay 或 shadow 样本时仍能跑通，但必须输出 `PASS_WITH_LIMITATIONS` 和原因。
- 报告声明市场阶段、复核窗口、as-if 回放窗口、生产影响、样本政策版本和四层样本门槛。
- 周/月执行频次进入 runbook。
- `docs/system_flow.md` 展示新命令、输入和输出。
- 单测覆盖报告构建和 CLI 写出。
- 初版不得改变 `score-daily`、`position_gate`、`prediction_ledger` production 行、回测仓位或日报结论。

## 状态记录

- 2026-05-13：新增任务并进入实现，原因：owner 要求把市场反馈驱动的权重/判断迭代机制作为独立系统设计、登记、实现并跑通。
- 2026-05-13：阶段 1 完成并进入 VALIDATING。新增 `aits feedback optimize-market-feedback`，可读取既有 outcome、learning queue、rule experiments、overlay 和 effective weights 产物，生成 `market_feedback_optimization_YYYY-MM-DD.md`；目标测试和 lint 通过。后续等待真实周/月运行观察，并推进 weight diagnostics 与 as-if replay 实验入口。
- 2026-05-13：根据 owner 对 `30` 样本门槛的质疑，新增样本政策配置并放宽 pilot floor：decision outcome 5、prediction/shadow outcome 2。该放宽只允许启动复盘和候选整理，不允许生产晋级；若后续发现节奏过快，优先调整 `config/feedback_sample_policy.yaml`。
- 2026-05-13：按当前缓存覆盖的最新日期 2026-05-12 跑通后续 pilot 流程：`feedback calibrate`、`feedback calibrate-predictions`、`feedback build-causal-chain`、`feedback build-learning-queue`、`feedback build-rule-experiments`、`feedback shadow-maturity` 和 `feedback optimize-market-feedback` 均完成；优化报告 readiness 为 `PILOT_DIAGNOSTIC_REVIEW`。
- 2026-05-13：尝试刷新 2026-05-11 outcome 时，`feedback calibrate --as-of 2026-05-11` 被数据质量门禁阻断，原因是当前价格缓存已包含 2026-05-12，严格 2026-05-11 校准不能读取未来价格。未做临时 CSV 过滤或绕过门禁；后续若要在历史 as-of 上刷新 feedback outcome，应补 replay-scoped feedback calibration 输入。
