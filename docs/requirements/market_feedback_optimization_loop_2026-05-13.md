# 市场反馈优化闭环初版设计

状态：VALIDATING

最后更新：2026-05-16

关联任务：`CALIBRATION-003`、`CALIBRATION-004`、`CALIBRATION-011`、`CALIBRATION-012`、`CALIBRATION-014`、`CALIBRATION-015`、`CALIBRATION-016`、`CALIBRATION-017`、`FEEDBACK-002`、`EXPERIMENT-001`、`SHADOW-002`、`SHADOW-003`、`GOV-003`、`LOOP-001`

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
aits feedback build-parameter-replay --as-of YYYY-MM-DD
aits feedback build-parameter-candidates --as-of YYYY-MM-DD
aits feedback evaluate-parameter-governance --as-of YYYY-MM-DD
aits feedback run-parameter-shadow --as-of YYYY-MM-DD
aits feedback optimize-market-feedback --as-of YYYY-MM-DD
aits feedback search-shadow-parameters --from YYYY-MM-DD --to YYYY-MM-DD
aits feedback evaluate-shadow-parameter-promotion --search-output-dir outputs/parameter_search/<run_id>
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
- `config/weights/shadow_weight_profiles.yaml`
- `config/weights/shadow_position_gate_profiles.yaml`
- `config/weights/shadow_parameter_search_space.yaml`
- `config/weights/shadow_parameter_objective.yaml`
- `config/weights/shadow_parameter_promotion_contract.yaml`
- `data/processed/shadow_weight_profile_observations.csv`
- `config/parameter_governance.yaml`
- `outputs/reports/parameter_governance_YYYY-MM-DD.json`

主要输出：

- `outputs/reports/parameter_governance_YYYY-MM-DD.md/json`
- `outputs/reports/parameter_shadow_predictions_YYYY-MM-DD.md`
- `outputs/reports/shadow_weight_profiles_YYYY-MM-DD.md`
- `outputs/reports/shadow_weight_performance_YYYY-MM-DD.md/.csv`
- `outputs/parameter_search/<run_id>/{manifest.json,trials.csv,pareto_front.csv,best_profiles.yaml,search_report.md}`
- `outputs/parameter_search/<run_id>/shadow_parameter_promotion_<run_id>.md/json`
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
|4. 参数 as-if replay 收益变化入口|VALIDATING|读取已运行的 backtest robustness summary，把模块权重扰动、再平衡和成本压力等参数候选接入 feedback 报告，比较 baseline vs challenger 的收益、回撤、换手和限制；第一阶段不自动拟合新权重。|
|5. 参数候选台账与 trial registry|VALIDATING|从参数 replay summary 生成 candidate-only 参数候选台账，记录 trial、来源 replay、指标、material 标记、治理状态和下一步；不自动生成 approved overlay。|
|6. Effective weights 单一入口|VALIDATING|`score-daily` 和 `backtest` 通过同一 resolver 使用 `weight_profile_current.yaml` 与 approved overlay 产出 effective weights；报告、CSV、trace 和回测 daily row 写入 profile/overlay/权重审计；approved overlay 未知 signal fail closed。|
|7. 多目标 candidate gate|VALIDATING|parameter replay/candidate ledger 消费 OOS、same-turnover random、signal-family baseline、coverage/data credibility 证据；baseline-only、OOS 弱化、随机基线未过、coverage 不足和风险恶化不得仅凭 total return 进入 owner review。|
|7B. 参数治理 manifest 与 owner 暂缺输入边界|VALIDATING|新增 `config/parameter_governance.yaml` 和 `aits feedback evaluate-parameter-governance`，把参数面、source level、owner quantitative input 状态、证据要求和 action 建议显式化；报告接入 `optimize-market-feedback` 和交易日 `daily-run` dashboard 前置流程；仍不写 production 参数。|
|8. Forward shadow 与 promotion 连接|VALIDATING|严格候选通过 replay 后进入 `prediction_ledger` shadow；`flow_validation` 可先写 validation-only parameter shadow 行来验证后续接线；成熟度报告达到门槛才允许 owner review。|
|9. Overlay / rule card 晋级|PROPOSED|通过 owner approval 后生成 approved overlay 或 rule card，具备有效期、回滚条件和审计引用。|
|9B. Shadow weight profile 长期观察|IN_PROGRESS|维护若干套隔离测试权重参数，初始值参考生产 `weight_profile_current.yaml`，每日从 production decision snapshot 计算 shadow score / model band / gated band，并可选写入隔离 prediction ledger 进入 outcome 观察；不得替换生产权重。|
|9C. 当前样本 validation 门槛|VALIDATING|把 diagnostic/validation 门槛放宽到当前已积累 outcome 样本可启动后续验证，但 production promotion floor 不降低；shadow maturity 区分 validation review 和 promotion/governance review，低样本 validation 只能进入 `READY_FOR_VALIDATION_REVIEW`。|
|9D. Shadow weight 表现优选|VALIDATING|按 gate 后仓位把 shadow 权重与主线转成可比的 position-weighted return、drawdown、turnover 和成本；当前三套 shadow profile 与主线 gate 后仓位一致，未找到正向 excess profile；不得自动替换生产权重。|
|9E. Shadow gate 参数实验|VALIDATING|新增隔离 hard gate / confidence / risk budget cap profile，允许与 shadow weight profile 组合观察 gate 后仓位和表现差异；profile 只覆盖 validation ledger，不修改生产 `scoring_rules.yaml`、`portfolio.yaml`、approved overlay 或日报仓位 gate。|
|9F. Shadow 参数搜索器|VALIDATING|新增可复现搜索入口，按指定区间枚举或采样 shadow weight/gate 组合，输出 trial registry、Pareto front 和 best profile YAML；结论只代表当前回测区间 in-sample 最优候选，不能直接生产替换。|
|9G. Shadow 参数验证收紧|VALIDATING|搜索报告输出 weight-only / gate-only / combined 的 factorial attribution；默认 objective 要求验证级样本和正 excess；短样本结果只能作为 diagnostic-leading，不得写成 eligible best 或 production 候选。|
|9H. Cap-level attribution 与仓位解释|VALIDATING|在搜索报告中拆出单个 gate cap 的边际贡献，并按日期披露 production/candidate 最终仓位、binding gate 和 return impact；仍只作为 validation 解释。|
|9I. Shadow 参数 promotion contract|VALIDATING|新增独立 contract 与 CLI，把 search ranking 和生产晋级拆开；缺 eligible best、forward shadow、owner approval 或 rollback 时不得进入 production。|
|9J. Objective regularization 与 lineage|VALIDATING|objective 增加 gate relaxation、weight distance、changed dimension penalty 和生产邻近性限制；search manifest 记录价格、快照、权重、resolver 和 git commit lineage。|
|10. Coverage / placeholder / source veto|VALIDATING|robustness summary 汇总模块覆盖率、placeholder 占比、数据来源可信度和关键模块缺失；parameter replay/candidate ledger 把这些字段作为 `BLOCKED_BY_DATA` 或降级原因，而不只作为报告说明。|
|11. Overlay target weights 与冲突治理|VALIDATING|approved overlay 支持 `target_weights` 模式、priority、mutual exclusion group 和冲突审计；approved overlay 的未知 signal、非法权重或同组优先级冲突必须 fail closed。|
|12. Benchmark 扩展与反过拟合证据|VALIDATING|robustness 增加 same-exposure random、vol-targeted/fixed exposure、no-gate、alpha-only/risk-state-only 等 benchmark，并把关键 benchmark 结果接入 candidate 证据摘要。|
|13. 统计证据诊断|VALIDATING|参数复测报告输出 paired block bootstrap confidence interval，并披露明确标注为 proxy 的 Deflated Sharpe / PBO 诊断；candidate 晋级不能把缺失统计证据误写成通过。正式 CSCV/PBO 仍保留为后续更严格统计层。|
|14. 有效独立样本与 purging/embargo 口径|VALIDATING|样本成熟度报告和 candidate evidence 披露 horizon、embargo、时间跨度和有效独立窗口估计；少量重叠样本不能直接支持 production promotion。|
|15. Alpha / risk / hard gate 分层迁移|VALIDATING|先以 robustness benchmark 和审计字段验证 alpha、risk budget、hard/soft gate 的分层解释；默认不改变 production scoring，除非后续 owner approval 明确允许分层模型替代线性加权。|

## 验收标准

- 新命令在缺少 overlay 或 shadow 样本时仍能跑通，但必须输出 `PASS_WITH_LIMITATIONS` 和原因。
- 报告声明市场阶段、复核窗口、as-if 回放窗口、生产影响、样本政策版本和四层样本门槛。
- 周/月执行频次进入 runbook。
- `docs/system_flow.md` 展示新命令、输入和输出。
- 单测覆盖报告构建和 CLI 写出。
- 初版不得改变 `score-daily`、`position_gate`、`prediction_ledger` production 行、回测仓位或日报结论。
- 本轮 effective weights 接入后，base profile 与当前 `scoring_rules.yaml` 权重等价；未命中 approved overlay 时 production 数值应保持不变，但输出必须披露 resolver 版本与审计字段。
- 参数候选晋级必须优先阻断 benchmark-only、OOS/random/coverage 证据不足和风险恶化场景；`READY_FOR_FORWARD_SHADOW` 只能表示可进入候选 shadow，不表示 owner approval 或 production effect。
- 高级证据阶段新增的阈值、窗口、priority 和冲突策略必须进入 policy/config 或具名常量并写明解释；不得在 scoring/backtest/candidate 路径引入未记录的投资解释数字。

## 状态记录

- 2026-05-13：新增任务并进入实现，原因：owner 要求把市场反馈驱动的权重/判断迭代机制作为独立系统设计、登记、实现并跑通。
- 2026-05-13：阶段 1 完成并进入 VALIDATING。新增 `aits feedback optimize-market-feedback`，可读取既有 outcome、learning queue、rule experiments、overlay 和 effective weights 产物，生成 `market_feedback_optimization_YYYY-MM-DD.md`；目标测试和 lint 通过。后续等待真实周/月运行观察，并推进 weight diagnostics 与 as-if replay 实验入口。
- 2026-05-13：根据 owner 对 `30` 样本门槛的质疑，新增样本政策配置并放宽 pilot floor：decision outcome 5、prediction/shadow outcome 2。该放宽只允许启动复盘和候选整理，不允许生产晋级；若后续发现节奏过快，优先调整 `config/feedback_sample_policy.yaml`。
- 2026-05-13：按当前缓存覆盖的最新日期 2026-05-12 跑通后续 pilot 流程：`feedback calibrate`、`feedback calibrate-predictions`、`feedback build-causal-chain`、`feedback build-learning-queue`、`feedback build-rule-experiments`、`feedback shadow-maturity` 和 `feedback optimize-market-feedback` 均完成；优化报告 readiness 为 `PILOT_DIAGNOSTIC_REVIEW`。
- 2026-05-13：尝试刷新 2026-05-11 outcome 时，`feedback calibrate --as-of 2026-05-11` 被数据质量门禁阻断，原因是当前价格缓存已包含 2026-05-12，严格 2026-05-11 校准不能读取未来价格。未做临时 CSV 过滤或绕过门禁；后续若要在历史 as-of 上刷新 feedback outcome，应补 replay-scoped feedback calibration 输入。
- 2026-05-13：owner 要求继续推进完整闭环；本轮把缺口收敛为参数复测收益变化入口，先消费现有 `backtest_robustness_*.json` 的真实回测场景，生成 feedback 层参数 replay 报告，再由 `optimize-market-feedback` 汇总其连接状态。该阶段仍保持 `production_effect=none`，不自动生成 approved overlay。
- 2026-05-13：参数 replay 基础版完成并进入 VALIDATING。新增 `aits feedback build-parameter-replay`，输出 `outputs/reports/parameter_replay_YYYY-MM-DD.md/json`；`optimize-market-feedback` 新增 Parameter replay 产物状态、场景数和 material delta 汇总；系统流图、README、runbook 和测试已同步。真实 smoke 使用现有最新 robustness summary 生成 2026-05-13 参数 replay 报告，报告正确披露该来源 summary 仍缺 module weight perturbation 场景。
- 2026-05-13：owner 要求继续推进到当前数据可测试跑通、并能持续迭代的回测调参闭环；本轮新增参数候选台账与 trial registry 阶段，目标是让 `parameter_replay` 的场景进入结构化 candidate ledger，再由 `optimize-market-feedback` 汇总候选状态。
- 2026-05-13：参数候选台账基础版完成并进入 VALIDATING。新增 `aits feedback build-parameter-candidates`，输出 `data/processed/parameter_candidates.json` 和 `outputs/reports/parameter_candidates_YYYY-MM-DD.md`；当前数据烟测已跑通 `parameter replay -> candidate ledger -> optimize-market-feedback`，报告汇总 3 个 trial / 3 个 candidate。因当前 replay 未提供 materiality policy 阈值，候选保持 `NEEDS_MATERIALITY_POLICY`，不能进入 owner approval 或 production。
- 2026-05-13：继续排查 materiality policy 阻塞。当前实现生成新的 `backtest_robustness` summary 时会写入 `policy`，但现有最新 summary 是旧产物且缺少该字段；本轮优先确认是否可通过重跑当前数据生成带 policy 的正式回测产物来解除阻塞，再决定是否需要补兼容读取逻辑。
- 2026-05-13：materiality policy 阻塞已解除并进入 VALIDATING。全量当前数据 robustness 重跑超过 15 分钟未完成且未写出新 summary，已停止悬挂进程；`build-parameter-replay` 现在对旧 summary 缺 policy 的情况读取当前 `config/backtest_validation_policy.yaml` 并在报告中披露 limitation。当前闭环 smoke 已跑通，`Needs materiality policy=0`，3 个 trial / 3 个 candidate 中 1 个正向 owner review、1 个负向 risk review、1 个 observe-only；仍不改变 production。
- 2026-05-13：owner 要求继续处理所有阻塞点，确保从头开始完整跑通。任务切回 IN_PROGRESS；本轮排查范围收敛到 `aits backtest --robustness-report --to 2026-05-12 --quality-as-of 2026-05-13` 长时间运行且不写出新 robustness summary 的问题。
- 2026-05-13：从头闭环阻塞已解除并进入 VALIDATING。`backtest --robustness-report` 中成本压力、起点后移、样本内/样本外和模块权重扰动改为复用基础回测已审计的每日 component scores、gate caps、raw target exposure、下一交易日收益和成本假设生成 as-if 指标，避免重复 full daily scoring；默认 `ai_after_chatgpt` 全窗口命令已在当前数据上完成并写出 `backtest_robustness_2022-12-01_2026-05-12.json`，包含 as-run policy、41 个 robustness 场景、`remaining_gaps=[]`。随后 `build-parameter-replay` 为 PASS（17 个参数场景，material delta=3），`build-parameter-candidates` 为 PASS（17 trial / 17 candidate，owner review=1，risk review=2，needs policy=0），`optimize-market-feedback` 已读取默认产物。
- 2026-05-14：owner 明确不要长期维护“跑一次后复用基础行派生指标”的第二套计算逻辑。任务切回 IN_PROGRESS；本轮目标改为把全窗口回测中昂贵的 point-in-time feature/report 准备拆成可缓存上下文，权重扰动、成本压力和窗口切分等调参场景仍调用同一套评分与回测执行路径，随后重新跑通 `backtest robustness -> parameter replay -> parameter candidates -> optimize-market-feedback`。
- 2026-05-14：单一评分/回测引擎方案已完成并进入 VALIDATING。`run_daily_score_backtest` 支持复用 prepared PIT context，robustness 的成本压力、窗口切分和模块权重扰动场景不再维护独立派生评分逻辑；固定 exposure、再平衡、信号族和随机策略保留为执行/信号基线。当前数据从头跑通：`backtest --robustness-report --to 2026-05-12 --quality-as-of 2026-05-13` 431 秒完成，summary 含 41 个场景、12 个 module weight perturbation、policy present、`remaining_gaps=[]`；`build-parameter-replay` 为 PASS（17 场景、material delta=3）、`build-parameter-candidates` 为 PASS（17 trial / 17 candidate、owner review=1、risk review=2、needs policy=0）、`optimize-market-feedback` 为 `PASS_WITH_LIMITATIONS` / `PILOT_DIAGNOSTIC_REVIEW`。验证通过目标测试、全量 pytest、ruff 和 diff check。
- 2026-05-14：外部方案复核确认当前主要缺口是 `weight_profile_current.yaml`/approved overlay 尚未成为 `score-daily` 与 `backtest` 的单一权重入口，以及 parameter candidate 仍由 total return materiality 主导。本轮状态切回 IN_PROGRESS，实施 effective weights resolver、approved overlay fail-closed 和多目标 candidate veto；不引入黑箱优化器，不自动生成 production overlay。
- 2026-05-14：阶段 6/7 基础实现完成并进入 VALIDATING。`score-daily` 与 `backtest` 已通过同一 effective weight resolver 使用 `weight_profile_current.yaml` 和 approved overlay；日报、scores CSV、backtest daily CSV、回测报告、robustness summary 和 trace bundle 记录 `weight_profile_version`、matched overlays、effective weights 与审计原因；approved overlay 中未知 signal 现在 fail closed。`parameter_replay` 输出 `robustness_evidence`，`parameter_candidates` 使用 data quality/data credibility、OOS、same-turnover random、signal-family baseline 和 drawdown veto，正向 total return 不再直接进入 owner review，而是进入 `READY_FOR_FORWARD_SHADOW` 或被阻断/降级。验证通过 `ruff check src tests`、`git diff --check`、目标 pytest 83 passed 和全量 pytest 516 passed。
- 2026-05-14：owner 要求继续推进其余方向并尽可能完整实现。本轮新增 `CALIBRATION-004`，状态切回 IN_PROGRESS；实现范围扩展为 coverage/placeholder veto、overlay `target_weights`/priority/conflict、benchmark 扩展、统计证据、有效独立样本和 alpha/risk/gate 分层迁移。生产效果继续保持 `none`，大功能完成后必须复测完整链路。
- 2026-05-14：阶段 10-15 baseline 完成并进入 VALIDATING。robustness summary/report 新增 coverage/source veto、same-exposure random、vol-targeted exposure、no-gate、alpha-only/risk-state-only/gate-modules 架构基线和 paired block bootstrap CI；parameter replay 新增 Deflated Sharpe / PBO proxy 诊断但明确不是正式 CSCV 统计；approved overlay 支持 `target_weights`、priority、mutual exclusion group 和 fail-closed 冲突治理；parameter replay/candidates 消费 coverage、有效独立窗口、score-architecture baseline 和 bootstrap CI。当前真实链路已重跑：`aits backtest --robustness-report --to 2026-05-12 --quality-as-of 2026-05-13` 410 秒完成；`build-parameter-replay` 为 PASS（66 场景、material delta=2）；`build-parameter-candidates` 为 PASS_WITH_LIMITATIONS（66 trial / 16 candidate / forward shadow ready=0 / blocked=16）；`optimize-market-feedback` 为 PASS_WITH_LIMITATIONS / `PILOT_DIAGNOSTIC_REVIEW`。阻断原因集中在 data credibility / component coverage、random baseline 和 architecture baseline，符合 candidate-only 边界。
- 2026-05-15：owner 暂时无法提供可量化参数输入；新增参数治理 manifest 与只读报告阶段，`evaluate-parameter-governance` 会把 candidate ledger 映射为 `KEEP_CURRENT`、`COLLECT_MORE_EVIDENCE`、`PREPARE_FORWARD_SHADOW`、`OWNER_DECISION_REQUIRED`、`BLOCKED_BY_DATA` 或 `BLOCKED_BY_POLICY`，并由 `optimize-market-feedback` 汇总 action 分布。交易日 `daily-run` 在 market feedback/dashboard 前生成该报告；生产参数、approved overlay 和 rule card 仍不自动改写。
- 2026-05-15：owner 要求为验证流程系统逻辑继续跑完全流程，数据阻塞时允许用既有数据和 pilot 限制放宽推进。当前发现 `score-daily` 已在 decision snapshot 写入 `weight_calibration`，但没有同步写默认 `outputs/current_context.json` 与 `outputs/current_effective_weights.json`，导致独立 `apply-calibration-overlay` 默认路径和 dashboard effective weights 状态断开；本轮新增 `CALIBRATION-006` 修复该接线缺口，生产影响仍为只读审计输出，不改变 scoring、position gate 或 approved overlay。
- 2026-05-15：CALIBRATION-006 进入 VALIDATING。代码已让默认 `score-daily` 写出 `outputs/current_context.json` 和 `outputs/current_effective_weights.json`；独立 `apply-calibration-overlay` 在没有 approved overlay 时允许缺 current context 并输出 base/effective weights 相等的审计结果，若存在 approved overlay 但缺 context 仍 fail closed。全量 pytest 530 passed，ruff 和 diff check 通过。真实验证使用既有 2026-05-14 数据完成：outcome/因果链/学习队列/shadow maturity 已刷新，`backtest --robustness-report --to 2026-05-14 --quality-as-of 2026-05-14` 写出最新 summary，随后 `build-parameter-replay` PASS（66 场景、material delta=2）、`build-parameter-candidates` PASS_WITH_LIMITATIONS（16 candidate，0 ready，16 blocked）、`evaluate-parameter-governance` PASS_WITH_LIMITATIONS、`optimize-market-feedback` PASS_WITH_LIMITATIONS、dashboard/ops health/secret scan PASS。Approved overlay 仍为 NOT_CONNECTED，因为当前没有 owner-approved overlay；这是治理边界，不是流程接线阻塞。
- 2026-05-15：owner 进一步要求尽快调整限制先跑全流程，确认是否还有后续接线问题。本轮新增 `CALIBRATION-007`，实现显式 `flow_validation` candidate gate 模式：严格 veto 仍写入审计字段，但允许 validation-only 参数候选进入 forward shadow 接线；该模式只能写 `production_effect=none` 的验证 prediction，不批准 overlay、不改变 production scoring、position gate 或正式权重。
- 2026-05-15：CALIBRATION-007 进入 VALIDATING。代码新增 `build-parameter-candidates --candidate-gate-mode flow_validation` 和 `feedback run-parameter-shadow`；目标测试 18 passed、全量 pytest 534 passed、ruff、diff check 和 secret scan 通过。真实验证使用 2026-05-14 既有数据完成：flow validation candidate ledger 为 PASS_WITH_LIMITATIONS（66 trial / 16 candidate / 16 ready / 16 override / blocked=0），parameter governance 为 PASS_WITH_LIMITATIONS（PREPARE_FORWARD_SHADOW=2、COLLECT_MORE_EVIDENCE=3），单独 `prediction_ledger_flow_validation.csv` 写入 16 条 validation-only parameter shadow prediction；`calibrate-predictions` 输出 160 行 outcome（available=13、pending=127、missing=20），`shadow-maturity` 为 PASS_WITH_LIMITATIONS，`optimize-market-feedback` 为 PASS_WITH_LIMITATIONS / `PILOT_DIAGNOSTIC_REVIEW`。生产 `prediction_ledger.csv`、scoring、position gate 和 approved overlay 未被修改。
- 2026-05-15：owner 要求后续调参流程也尝试跑通，并先维护若干套隔离测试权重参数。新增 `CALIBRATION-008` 与阶段 9B；目标是把 shadow weight profiles 作为长期观察对象，每套 profile 可独立计算 shadow score、与主线评分对比并进入独立 outcome 观察，但暂不定义替换生产参数的条件。
- 2026-05-15：CALIBRATION-008 进入 VALIDATING。新增 `config/weights/shadow_weight_profiles.yaml`，包含 alpha tilt、risk/macro tilt 和 guardrail tilt 三套隔离测试权重；新增 `aits feedback run-shadow-weight-profiles`，从 production decision snapshot 计算 shadow score、模型仓位和 gate 后观察仓位，默认写独立 observation ledger，可选写隔离 prediction ledger。真实 2026-05-14 验证：主线评分 67.79，`shadow_alpha_tilt_v1` 71.46（+3.67），`shadow_risk_macro_tilt_v1` 63.52（-4.27），`shadow_guardrail_tilt_v1` 66.18（-1.61）；三者 gate 后观察仓位均受 valuation gate 限制为 40%-40%。隔离 prediction ledger 写入 3 行，prediction outcome 15 行（available=0、pending=15、missing=0），shadow maturity 为 PASS_WITH_LIMITATIONS。目标测试 23 passed、全量 pytest 537 passed、ruff、diff check 和 secret scan 通过；生产权重、approved overlay、正式 prediction ledger、日报结论和仓位 gate 未改变。
- 2026-05-16：owner 要求把限制放宽到当前已积累数据刚好能启动后续流程。新增 `CALIBRATION-009` 与阶段 9C；本轮只允许 validation/diagnostic 放宽，production promotion floor、approved overlay、正式 prediction ledger 和日报仓位 gate 不得降低或自动改写。
- 2026-05-16：CALIBRATION-009 进入 VALIDATING。`config/feedback_sample_policy.yaml` 升级为 `feedback_sample_policy_v2`，decision/prediction diagnostic floor 下调为 14/13，promotion floor 仍为 60/30；`aits feedback shadow-maturity` 新增 `--review-mode validation`，默认使用 prediction pilot floor 并把达标状态写为 `READY_FOR_VALIDATION_REVIEW`，避免把当前少量样本误写成 production governance review。真实验证使用 2026-05-04 至 2026-05-14 可追溯 decision snapshots 回填隔离 shadow-weight ledger，生成 33 条 shadow prediction；2026-04-30 旧 snapshot 因 trace 指向 pytest 临时目录未写 prediction ledger。随后 `calibrate-predictions --horizons 1,5` 生成 66 行 outcome（available=36、pending=18、missing=12），validation maturity PASS，`optimize-market-feedback` 在隔离 shadow-weight outcome 上达到 `READY_FOR_WEIGHT_DIAGNOSTIC_REVIEW`。全量 pytest 538 passed，ruff 和 diff check 通过；生产 prediction ledger、approved overlay、日报结论和仓位 gate 未改变。
- 2026-05-16：owner 明确初期目标是通过调整权重实现一个更好表现的配置。新增 `CALIBRATION-010` 与阶段 9D；本轮先补 position-weighted evaluator，把 shadow score 差异转化为与主线可比的收益、回撤、换手和成本差异，再决定哪套权重值得继续扩展观察或迭代。
- 2026-05-16：CALIBRATION-010 进入 VALIDATING。新增 `aits feedback evaluate-shadow-weight-performance`，读取 shadow weight observation ledger 和价格缓存，按 production/shadow gate 后仓位计算 position-weighted return、最大回撤、换手和成本；observation ledger 增补 production/shadow 模型目标仓位与 gate 后目标仓位字段。真实 2026-05-14 验证使用 2026-05-04 至 2026-05-14 样本、SMH、1D horizon 和 5bps 单边成本，三套 profile 均为 production total return 4.74%、shadow total return 4.74%、excess 0.00%、turnover 1.20。当前没有正向 excess 的 shadow weight profile，直接原因是估值/风险等 hard gate 将三套 shadow 的最终仓位压到与主线一致；生产权重、approved overlay、正式 prediction ledger、日报结论和仓位 gate 未改变。
- 2026-05-16：owner 明确 hard gate、confidence cap、risk budget cap 等可配置参数都可以纳入观察，只要做好 shadow 隔离。新增 `CALIBRATION-011` 与阶段 9E；本轮实现范围限定为 validation-only shadow gate profile 与现有 shadow weight profile 的组合观察，不改生产 gate 触发阈值或正式仓位结论。
- 2026-05-16：CALIBRATION-011 进入 VALIDATING。新增 `config/weights/shadow_position_gate_profiles.yaml`，包含 relaxed valuation、balanced caps 和 defensive caps 三套隔离 gate profile；`aits feedback run-shadow-weight-profiles` 默认可组合 shadow weight 与 shadow gate profile，并在 observation ledger 记录 weight/gate profile、gate overrides 和 gate cap sources。真实 2026-05-04 至 2026-05-14 验证写入 9 个组合/日；`evaluate-shadow-weight-performance --as-of 2026-05-14 --since 2026-05-04 --horizon-days 1` 为 PASS，return-leading profile 为 `shadow_alpha_tilt_v1__shadow_gate_relaxed_valuation_v1`，available=8、pending=1、missing=2，shadow total return 8.31%、production total return 4.74%、excess 3.58%、shadow MDD -1.57%、production MDD -1.09%、beat rate 75.00%。该结果仍是 validation-only，样本少且未满足 production promotion；生产配置、正式 prediction ledger、日报结论和仓位 gate 未改变。
- 2026-05-16：owner 要求第一版可频繁调用的最优参数搜索器，并用当前 2026-05-04 至 2026-05-14 输入寻找最优权重策略。新增 `CALIBRATION-012` 与阶段 9F；第一版限定为枚举式 validation-only 搜索，输出 top-N、Pareto front 和 best profile YAML，不写 production 配置。
- 2026-05-16：CALIBRATION-012 进入 VALIDATING。新增 `config/weights/shadow_parameter_search_space.yaml`、`config/weights/shadow_parameter_objective.yaml` 和 `aits feedback search-shadow-parameters`；真实 2026-05-04 至 2026-05-14 初版搜索为 PASS，共 204 个 weight candidates、4 个 gate candidates、816 个 trials、582 个 Pareto front trials。当前目标函数下最优 trial 为 `source_current__shadow_gate_relaxed_valuation_v1`，available=8、pending=1、missing=2，shadow total return 8.31%、production total return 4.74%、excess 3.58%、shadow MDD -1.57%、production MDD -1.09%、turnover 0.60、beat rate 75.00%；最优结果主要来自 relaxed valuation gate，多个权重候选并列，说明当前短样本中 gate cap 仍主导最终仓位。输出位于 `outputs/parameter_search/current_20260504_20260514_v1/`；生产权重、approved overlay、正式 prediction ledger、日报结论和仓位 gate 未改变。
- 2026-05-16：根据 owner 反馈，修正 CALIBRATION-012 第一版语义：`search-shadow-parameters` 不能只在几套预设 gate profile 中选优，必须在配置化数值搜索空间内拟合参数。`shadow_parameter_search_space.yaml` 已新增 `gate_grid`，默认关闭预设 shadow gate profile 参与最优解选择，改为枚举 weight grid + gate cap grid；报告和 manifest 记录 `exhaustive_grid_with_optional_manifest_seeds`、weight/gate grid 启用状态和配置 checksum。该方式仍是搜索空间内的 validation-only in-sample 最优，不是无限连续空间或 production approval。
- 2026-05-16：CALIBRATION-012 修正版真实搜索 PASS，run id 为 `current_20260504_20260514_grid_v2`。搜索空间生成 204 个 weight candidates、253 个 gate candidates、51,612 个 trials、45,288 个 Pareto front trials；当前最优 trial 为 `grid_weight_0118__grid_gate_0217`，target weights 为 trend 25%、fundamentals 35%、macro_liquidity 10%、risk_sentiment 20%、valuation 5%、policy_geopolitics 5%，gate cap overrides 为 valuation 0.70、risk_budget 0.70、thesis 0.70、confidence 0.90、data_confidence 0.80。样本 available=8、pending=1、missing=2，shadow total return 9.18%、production total return 4.74%、excess 4.45%、shadow MDD -1.70%、production MDD -1.09%、turnover 0.75、beat rate 75.00%。Top trials 存在大量并列，说明短样本和离散仓位/gate 仍会形成平台区间；生产配置、approved overlay、正式 prediction ledger、日报结论和仓位 gate 未改变。
- 2026-05-16：新增 CALIBRATION-013 shadow / production 边界复核。`validation_shadow` 被定义为 validation-only source level；`run-parameter-shadow` 默认输出从正式 `prediction_ledger.csv` 改为隔离的 `prediction_ledger_flow_validation.csv`，显式路径仍可覆盖。该修复只收紧 shadow 默认隔离，不改变正式 `score-daily`、approved overlay、生产权重或仓位 gate。
- 2026-05-16：新增 CALIBRATION-014 shadow 参数验证收紧。当前实现目标是补 factorial attribution，收紧默认 objective 到 prediction diagnostic floor，短样本只输出 diagnostic-leading trial，并在 hard overlay 未接入下游执行层前对 `approved_hard` hard effect fail closed。
- 2026-05-16：CALIBRATION-014 进入 VALIDATING。`search-shadow-parameters` 报告和 manifest 已输出 factorial attribution；默认 objective 现要求 `min_available_samples=13` 且 `require_positive_excess=true`。完整当前样本搜索 `current_20260504_20260514_validation_v3` 评估 51,612 trials，结果为 `PASS_WITH_LIMITATIONS`，没有 eligible best trial；诊断领先项为 `grid_weight_0118__grid_gate_0217`，factorial attribution 显示 `weight_only` excess delta 0.00%、`gate_only` 约 4.29%、`combined` 约 4.45%，primary driver 为 `gate`。
- 2026-05-16：新增 CALIBRATION-015、CALIBRATION-016 和 CALIBRATION-017。原因：最新评估指出下一步应继续拆解 gate 主导效应、把 search ranking 与 promotion contract 分层，并强化 objective risk-awareness、生产邻近性和 lineage。实现已让 `search-shadow-parameters` 输出 cap-level attribution、最终仓位变化解释、source weight / price / decision snapshot checksum、resolver version、git commit sha 和 dirty worktree 标记；新增 `config/weights/shadow_parameter_promotion_contract.yaml` 与 `aits feedback evaluate-shadow-parameter-promotion`，默认 contract 仍要求 eligible best、30 个 available、正 excess、回撤/换手约束、cap review、forward shadow、owner approval、rollback condition，并保持 `approved_hard_allowed=false`。目标测试已通过，生产权重、正式 gate、approved overlay、正式 prediction ledger 和日报结论未改变。
- 2026-05-16：CALIBRATION-015/016/017 当前样本 smoke 通过。`current_20260504_20260514_cap_promotion_v3` 为 `PASS_WITH_LIMITATIONS`，51,612 trials，无 eligible best；diagnostic-leading 为 `source_current__grid_gate_0217`，excess 4.29%，factorial primary driver 为 `gate`。Cap-level attribution 显示 primary gate cap 为 `valuation`，valuation cap-only excess delta 约 2.42%、thesis 约 0.76%、其他 cap 约 0%；position change 表展示 2026-05-04 至 2026-05-14 每日 production/candidate 最终仓位和 return impact。`evaluate-shadow-parameter-promotion` 对同一 bundle 输出 `NOT_PROMOTABLE`，因为没有 eligible best、available=8 低于 contract floor 30，且缺 forward shadow outcome。
