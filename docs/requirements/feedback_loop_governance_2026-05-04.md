# 反馈闭环与治理能力改进需求

状态：IN_PROGRESS

最后更新：2026-05-04

关联任务：`EVIDENCE-001`、`CAUSE-001`、`LEARNING-001`、`EXPERIMENT-001`、`LOOP-001`、`FEEDBACK-001`、`FEEDBACK-002`、`UNIVERSE-001`、`DATA-002`、`SCENARIO-001`、`CATALYST-001`、`PORTFOLIO-002`、`EXEC-001`、`COST-001`、`PROXY-001`、`GOV-001`、`OPS-001`、`ALERT-001`、`TEST-001`、`STORAGE-001`、`UI-001`、`SECURITY-001`、`DOC-001`

## 背景

本轮评估来自 2026-05-04 的反馈闭环强化讨论。现有任务登记表已经覆盖评分、`position_gate`、评分/置信度拆分、风险事件 schema、watch/active 区分、source policy、估值可信度、回测防过拟合、thesis 状态机、产业链节点状态和 LLM 证据分类边界。

新增任务不应继续堆叠短期评分因子，而应补足第二层能力：

- 记录系统当时为什么判断。
- 观察判断后的真实结果。
- 防止观察池、基准和数据源把回测带偏。
- 把规则变更、执行纪律、系统不变量和运行健康纳入治理。
- 将报告从单次结论扩展为可复盘、可校准、可审计的运营链路。

总体判断：本轮建议没有需要直接 `DROPPED` 的项；但优先级和独立性不同。`FEEDBACK-001`、`FEEDBACK-002`、`UNIVERSE-001`、`DATA-002`、`SCENARIO-001`、`CATALYST-001`、`GOV-001`、`TEST-001` 应进入近期 P1/P0 队列。`PORTFOLIO-002`、`EXEC-001`、`PROXY-001` 有明确投资解释价值，也应登记，但需要和 `SCORE-003`、`REPORT-002`、`BACKTEST-001` 协同。`COST-001`、`OPS-001`、`ALERT-001`、`UI-001`、`STORAGE-001`、`SECURITY-001`、`DOC-001` 更偏长期治理、运营或产品边界，暂列 P2/P3。

2026-05-04 追加判断：要把“基于新市场信息进行回测和决策因果链条认知迭代”落成工程闭环，现有任务还缺 5 个承接层。`EVIDENCE-001` 负责把新市场信息先变成可审计证据账本；`CAUSE-001` 负责把证据、模块变化、gate、决策快照和结果连接成因果链；`LEARNING-001` 负责把判断失误或成功样本归因到数据、规则、thesis、执行或不可预测冲击；`EXPERIMENT-001` 负责候选规则的历史重放和前向 shadow 验证；`LOOP-001` 负责周期性闭环复核报告。它们不是替代 `FEEDBACK-001/002` 和 `GOV-001`，而是把这些部件串成持续迭代流程。

## 价值评估摘要

|建议|处理结论|原因|
|---|---|---|
|`FEEDBACK-001` 决策快照|新增 P1|没有结构化快照，就无法做结果归因、评分校准和规则复盘。|
|`FEEDBACK-002` 结果观察与评分校准|新增 P1|直接验证分数、置信度、gate、thesis 状态和风险事件是否有预测或风控价值。|
|`UNIVERSE-001` point-in-time 观察池生命周期|新增 P0|防止扩展 universe 后产生幸存者偏差，影响回测正确性。|
|`DATA-002` 跨供应商校验与数据源健康评分|新增 P1，依赖 owner 数据源选择|单一供应商错误会直接污染评分、回测和报告；不能用临时公共源假装完成。|
|`SCENARIO-001` 情景压力测试库|新增 P1|补足历史回测不能覆盖的 AI 产业链尾部风险和非重复冲击。|
|`CATALYST-001` 未来催化剂日历|新增 P1|报告需要说明未来哪些事件会改变判断，并支持事件前/后复核纪律。|
|`PORTFOLIO-002` 组合暴露分解|新增 P1|与 `SCORE-003` 重叠但不是重复；第一版可作为解释层，后续再进入风险预算。|
|`EXEC-001` 再平衡与执行纪律规则|新增 P1|当前系统已输出仓位建议，需要防止把小幅变化误当成交易指令。|
|`COST-001` 真实交易摩擦模型|新增 P2|当前回测已有基础交易成本和滑点；更细成本模型有价值但不应挤占 P1 正确性任务。|
|`PROXY-001` AI proxy / benchmark 治理|新增 P1|避免把半导体 ETF beta 误解释成完整 AI 产业链判断。|
|`GOV-001` 规则变更生命周期与 rule card|新增 P1|支持系统自我优化，但防止无验证、无审计地修改生产规则。|
|`OPS-001` 运行监控与失败告警|新增 P2|日常自动化后很重要；当前还不是投资解释的第一阻塞。|
|`ALERT-001` 投资与数据告警系统|新增 P2|适合在报告链路和触发条件稳定后接入，避免早期重复刷屏。|
|`TEST-001` 系统级不变量测试|新增 P1|保护 source policy、point-in-time、gate 优先级、LLM 隔离等核心原则。|
|`STORAGE-001` DuckDB / Parquet 评估|新增 P3|CSV 对当前规模仍可接受；应在 schema 演进、查询性能或历史快照压力出现时升级。|
|`UI-001` 证据下钻型仪表盘|新增 P2|有价值，但应等 evidence bundle、报告结构和结论等级稳定后推进。|
|`SECURITY-001` 密钥、日志和供应商权限治理|新增 P2|数据源扩展后必要；当前已有 FMP key 不入报告的基础约束。|
|`DOC-001` 系统使用边界与结论等级|新增 P2|减少误用风险，但应与 `SCORE-002` 置信度和 `REPORT-002` 解释输出协同。|
|`EVIDENCE-001` 新市场信息证据账本|新增 P1|新信息必须先结构化、分级、去重和归档，才能进入 risk event、thesis、valuation、catalyst 或报告解释。|
|`CAUSE-001` 决策因果链条 ledger|新增 P1|把证据、模块变化、gate、快照、outcome 和规则候选串起来，支持事后判断“为什么对/错”。|
|`LEARNING-001` 错误归因与学习队列|新增 P1|校准报告告诉哪里表现差，归因队列负责区分数据问题、规则问题、thesis 问题、执行问题和不可预测冲击。|
|`EXPERIMENT-001` 候选规则实验与 shadow replay|新增 P1|规则改动必须先以候选版本重放历史并前向 shadow 观察，不能由单次复盘直接改生产。|
|`LOOP-001` 闭环复核编排与周期报告|新增 P1|把 evidence、snapshot、outcome、calibration、rule card 和待办状态汇总成固定复核节奏。|

## EVIDENCE-001

标题：新市场信息证据账本与影响分类

价值判断：值得优先做。新市场信息不能直接跳到评分或规则改动，必须先进入结构化证据账本，记录来源、时间、影响对象、证据等级、是否新增信息、是否需要人工确认，以及可能影响的 risk event、thesis、valuation、catalyst 或产业链节点。

分步开发：

1. 设计 `market_evidence` schema，覆盖 `evidence_id`、source、source_type、published_at、captured_at、ticker、industry_chain_node、topic、evidence_grade、novelty、impact_horizon、direction、confidence、manual_review_required、linked_risk_event、linked_thesis、linked_valuation_snapshot、linked_catalyst。
2. 建立人工录入或 CSV/YAML 导入基础版，并复用 `SOURCE-001` 的来源等级和 `REPORT-001` 的 evidence 引用。
3. 对重复信息、低可信来源、来源冲突和过期信息输出校验警告。
4. 允许 `LLM-001` 后续只作为证据分类辅助写入待复核队列，不允许直接改评分。

验收标准：

- 新市场信息进入系统前都有结构化证据记录、来源等级、采集时间和去重键。
- 报告能区分已确认证据、待人工复核证据和不能进入自动评分的低可信证据。
- 证据可追溯到 risk event、thesis、valuation、catalyst、industry_chain_node 或日报 claim。
- 实现时同步更新 `docs/system_flow.md` 并补充证据 schema、去重、来源等级和 LLM 隔离测试。

## CAUSE-001

标题：决策因果链条 ledger

价值判断：值得优先做。`REPORT-001` 能从报告 claim 反查 evidence，`FEEDBACK-001` 能保存当日决策快照；但认知迭代还需要把“哪个证据导致哪个模块变化、哪个 gate 生效、最终仓位如何变化、后续结果如何、是否形成规则候选”串成稳定链条。

分步开发：

1. 设计 `decision_causal_chain` schema，覆盖 `chain_id`、signal_date、market_regime、linked_evidence_ids、affected_modules、score_delta、confidence_delta、triggered_gates、position_delta、linked_decision_snapshot、linked_outcome_windows、review_status、linked_rule_candidate。
2. 在 `score-daily` 或日报解释链路中输出本期主要因果链条，第一版可以从模块变化、gate 和 evidence 引用生成。
3. 在 `FEEDBACK-002` outcome 生成后回填对应观察窗口结果。
4. 将因果链条纳入 `trace lookup` 或等价查询入口。

当前实现状态：

- 2026-05-04 基础版已完成：新增 `decision_causal_chains` 模块和 `aits feedback build-causal-chain`，从历史 `decision_snapshot`、trace bundle evidence 引用和 `decision_outcomes.csv` 构建 `data/processed/decision_causal_chains.json`。
- `signal_time_context` 固定保存 signal_date 当时可见的 evidence、模块分/置信度变化、触发 gate、仓位变化、质量状态和 snapshot 引用；`post_signal_observations` 只追加 outcome 窗口，避免未来 outcome 改写当时因果解释。
- 新增 `aits feedback lookup-chain` 作为等价查询入口，按 `chain_id` 展示市场阶段、质量状态、决策快照、evidence、受影响模块、触发 gate 和 outcome 窗口。
- 仍预留 `linked_rule_candidate`，后续由 `LEARNING-001` 和 `EXPERIMENT-001` 在错误归因和候选规则验证完成后回填。

验收标准：

- 每个核心仓位变化或结论降级都能对应至少一个 `chain_id`。
- 因果链能连接 evidence、module score/confidence 变化、gate、decision snapshot、outcome 和后续规则复核。
- 不允许用未来 outcome 改写 signal_date 当时的 causal chain，只能追加后验观察字段。
- 实现时同步更新 `docs/system_flow.md` 并补充因果链可见性和未来函数防护测试。

## LEARNING-001

标题：结果归因、错误分类与学习队列

价值判断：值得做。`FEEDBACK-002` 能量化表现，但“认知迭代”需要把表现好坏归因到可行动类别：数据质量、来源问题、规则过度敏感、规则过度迟钝、thesis 错误、执行纪律、观察池偏差、基准误选或不可预测外生冲击。

建议分类：

- `data_issue`：数据缺失、延迟、字段漂移、来源冲突或质量门未覆盖。
- `rule_issue`：评分、gate、阈值、置信度或执行规则需要复核。
- `thesis_issue`：核心假设错误、验证指标不足或证伪条件过弱。
- `execution_issue`：建议正确但调仓纪律或成本假设影响结果。
- `universe_or_proxy_issue`：观察池、AI proxy 或基准选择导致误读。
- `exogenous_unforecastable`：无法合理提前识别的外部冲击。
- `sample_limited`：样本不足，不足以形成规则结论。

当前实现状态：

- 2026-05-04 基础版已完成：新增 `decision_learning_queue` 模块、`aits feedback build-learning-queue` 和 `aits feedback lookup-learning`，从 `decision_causal_chains.json` 生成 `data/processed/decision_learning_queue.json` 与中文学习队列报告。
- 每个复核项记录 `review_id`、关联 `chain_id`、signal_date、market regime、decision snapshot、evidence、触发 gate、受影响模块、outcome summary、成功/失败方向、归因分类、owner、next step、是否需要规则候选和治理边界。
- 失败样本优先按数据质量、低置信模块、已触发 gate、缺失 evidence 和外生冲击顺序归因；成功样本也进入复核队列，用于后续规则有效性观察。
- `sample_limited` 项明确不生成规则候选；即便 `rule_candidate_required=true`，也只能进入 `EXPERIMENT-001` 和 `GOV-001`，不得自动修改 production 规则。

验收标准：

- 校准报告和重大偏差样本能生成 review queue，记录归因分类、证据、owner、下一步和是否需要规则候选。
- 成功样本也能归因，避免系统只从失败样本学习。
- 样本不足必须标记为 `sample_limited`，不能强行生成规则修改建议。
- 实现时同步更新 `docs/system_flow.md` 并补充归因分类和学习队列测试。

## EXPERIMENT-001

标题：候选规则实验、历史重放与前向 shadow 验证

价值判断：值得优先做。`GOV-001` 管规则生命周期，但还需要规则进入 production 前的实验路径。候选规则应先在 `ai_after_chatgpt` 窗口和指定压力窗口重放，再前向 shadow 一段时间，比较 production 和 candidate 的差异。

分步开发：

1. 定义 `rule_candidate` schema，记录候选规则、触发原因、关联 causal chain、预期改善、风险、验证窗口、回滚条件。
2. 建立历史 replay 输出，比较 production vs candidate 在收益、回撤、换手、置信度、触发 gate、结论等级和主要失败样本上的差异。
3. 建立 forward shadow 记录，不影响正式日报仓位，只记录候选规则如果上线会如何改变判断。
4. 通过 `GOV-001` rule card 批准后才允许进入 production。

当前实现状态：

- 2026-05-04：进入基础实现；第一阶段先从 `decision_learning_queue` 中 `rule_candidate_required=true` 的复核项生成 rule experiment ledger，记录候选原因、关联 causal chain、replay 计划、forward shadow 计划、样本限制、风险和 production 隔离边界。
- 第一阶段不执行真实历史重放、不改变 scoring / position_gate / thesis / daily report；所有候选规则默认 `production_effect=none`，等待后续 replay runner、shadow observation 和 `GOV-001` rule card 批准。
- 2026-05-04 基础版已完成：新增 `rule_experiments` 模块、`aits feedback build-rule-experiments`、`aits feedback lookup-rule-experiment`、`data/processed/rule_experiments.json` 和 `outputs/reports/rule_experiments_YYYY-MM-DD.md`；`feedback loop-review` 已读取 rule experiment ledger 并显示候选规则、未运行 replay 和待前向 shadow 状态。
- 当前完成态为 `BASELINE_DONE`：候选规则登记、验证计划、隔离边界和查询入口已具备；完整 `DONE` 仍需要真实 replay runner、forward shadow 观测记录和 `GOV-001` rule card 审批。

验收标准：

- 每个候选规则都有历史重放结果、前向 shadow 计划、样本限制说明和回滚条件。
- 报告显示 candidate 相对 production 的收益、回撤、换手、错误归因和规则复杂度变化。
- 候选规则未批准前不得影响正式评分、仓位或日报结论。
- 实现时同步更新 `docs/system_flow.md` 并补充 candidate isolation 和 replay 测试。

## LOOP-001

标题：闭环复核编排与周期报告

价值判断：值得做。单个任务分别解决证据、快照、outcome、规则和报告，但还需要固定节奏的闭环报告，明确本期新信息、哪些判断已到观察窗口、哪些规则需要复核、哪些任务被阻塞、哪些结论仍不能升级。

建议复核节奏：

- 每日：新证据、数据质量、催化剂、risk/thesis/valuation 状态和当日 decision snapshot。
- 每周：已成熟的 5D/20D outcome、主要偏差样本、执行纪律和新 rule candidate。
- 每月或每个财报季后：60D/120D outcome、calibration、backtest robustness、scenario stress 和 rule card 复核。

验收标准：

- 生成闭环复核报告，覆盖 new evidence、decision snapshots、due outcomes、calibration findings、causal chains、learning queue、rule candidates、blocked owner inputs 和 task register 状态变化。
- 报告声明市场阶段，默认以 `ai_after_chatgpt` 作为主要结论窗口。
- 报告明确哪些结论可执行、哪些仅研究、哪些因数据或样本不足降级。
- 实现时同步更新 `docs/system_flow.md` 并补充周期报告测试。

当前实现状态：

- 2026-05-04 基础版已完成：新增 `feedback_loop_review` 模块和 `aits feedback loop-review`，生成 `outputs/reports/feedback_loop_review_YYYY-MM-DD.md`。
- 报告覆盖 market evidence、decision snapshots、decision_outcomes、decision_causal_chains、decision_learning_queue、rule candidate 接入状态、blocked tasks 和 task register 状态统计。
- 报告默认声明 `ai_after_chatgpt` 市场阶段，并明确“无自动交易动作”“学习队列规则候选需复核”“样本不足或未验证结论仅研究用途”。
- `rule_candidates` 目前显示 `NOT_CONNECTED`，等待 `EXPERIMENT-001/GOV-001` 接入。

## FEEDBACK-001

标题：决策快照与结果观察窗口基础

价值判断：值得优先做。现有 `scores_daily.csv`、日报 evidence bundle 和回测明细已经具备部分审计能力，但还没有把每一次当日判断固化为面向后续复盘的 `decision_snapshot`。没有这个快照，后续很难系统性回答当时哪些 gate 生效、置信度如何、主要解释因子是什么，以及后续结果是否验证了判断。

分步开发：

1. 设计 `decision_snapshot` schema，覆盖 signal_date、market_regime、requested_range、overall score、module scores、confidence、model position range、final position range、triggered gates、thesis state、risk events、valuation state、major explanation factors、config version、data quality refs、trace bundle refs。
2. 在 `aits score-daily` 成功通过数据质量门禁并生成日报后追加或重写当日快照。
3. 将快照与 `REPORT-001` evidence bundle、`SCORE-002` 置信度拆分和 `REPORT-002` 变化原因树兼容。
4. 为重复运行同一 signal_date 设计幂等策略，避免同一天多份冲突快照。

验收标准：

- 每次评分都会生成机器可读 `decision_snapshot`，且包含数据质量状态、配置版本和 evidence 引用。
- 快照能复原当日总分、模块分、置信度、仓位区间、触发 gate、thesis、风险事件、估值状态和主要解释因子。
- 报告或 trace 反查能定位对应快照。
- 实现时同步更新 `docs/system_flow.md` 并补充快照 schema、幂等写入和报告引用测试。

## FEEDBACK-002

标题：结果归因与评分校准报告

价值判断：值得做，但依赖 `FEEDBACK-001`。该任务不是普通回测，而是对真实系统判断做后验观察，用来检验 80-100 分是否真的优于 65-80 分、低置信高分是否容易失误、估值或风险 gate 是否降低回撤、thesis warning 是否有提前量。

观察窗口：

- 1D、5D、20D、60D、120D。
- AI proxy return、SPY return、QQQ return、SMH/SOXX return、excess return、max drawdown、realized volatility、hit/miss。

分步开发：

1. 从历史 `decision_snapshot` 生成 `decision_outcome`，按 signal_date 之后的交易窗口计算收益、回撤和超额收益。
2. 建立 score bucket、confidence bucket、gate state、thesis state、risk level、valuation state 分组统计。
3. 输出机器可读 outcome CSV 和中文 Markdown calibration report。
4. 将校准结果用于规则复核，而不是自动改生产规则；规则变更必须走 `GOV-001`。

验收标准：

- 能按总分、置信度、gate、thesis、风险等级和估值状态分桶统计未来收益、回撤、波动、胜率和超额收益。
- 报告声明市场阶段、样本数量、观察窗口、数据质量状态和样本不足限制。
- 不把样本不足或重叠窗口误解释为稳定结论。
- 实现时同步更新 `docs/system_flow.md` 并补充 outcome 计算和分桶报告测试。

## UNIVERSE-001

标题：Point-in-time 观察池生命周期

价值判断：应列为 P0。当前 `config/watchlist.yaml` 适合记录现时观察池和能力圈，但扩展回测时必须知道某个 ticker 在历史上何时被纳入、移除、为什么纳入、当时是否已在能力圈、是否已有产业链节点映射和 thesis。否则长期回测会倾向于只包含后来幸存的 AI 龙头。

分步开发：

1. 设计 `watchlist_lifecycle` schema，记录 `ticker`、`added_at`、`removed_at`、`reason`、`active_from`、`active_until`、`competence_status`、`node_mapping_valid_from`、`thesis_required_from`、来源和复核人。
2. 更新 `aits watchlist validate`，校验当前 `watchlist.yaml` 与 lifecycle 的一致性。
3. 在回测中按 signal_date 只允许使用当时已进入观察池且映射有效的 ticker。
4. 对历史扩展 universe 输出覆盖和限制说明。

验收标准：

- 每个进入评分、回测或报告的 ticker 都能追溯观察池生命周期。
- 回测不能使用 signal_date 尚未进入观察池或尚未有效映射的 ticker。
- 报告能区分主动移除、并购/退市、失去相关性、能力圈不足和数据源不足。
- 实现时同步更新 `docs/system_flow.md` 并补充防幸存者偏差测试。

## DATA-002

标题：跨供应商数据校验与数据源健康评分

价值判断：值得做，但真实跨供应商校验依赖 owner 选择可长期使用的数据源和权限。当前 `aits validate-data` 已覆盖 schema、完整性、新鲜度、重复键和异常值；下一层需要识别供应商之间价格、估值、财报、宏观数据的口径冲突、字段漂移、延迟更新和 ticker alias 风险。

2026-05-04 owner 决策：初版先走低成本框架方案，不直接订阅 Intrinio `US Fundamentals` 等高价年付来源。第一阶段使用 SEC EDGAR、FRED、现有 FMP/Yahoo 和低成本或试用来源搭建 adapter、数据源目录、provider health score、差异报告和未覆盖声明；后续按实际缺口订阅 Intrinio、Sharadar、Databento 或企业级来源。该阶段的完成态只能是 `BASELINE_DONE`，不能把价格、估值、财报、宏观的完整生产级 cross-provider reconciliation 标成 `DONE`。

分步开发：

1. 定义 provider health score，覆盖 freshness、schema drift、row count anomaly、checksum change、alias risk、cross-provider deviation、permission status。
2. 先为已有 provider 输出单源健康报告，并把无第二来源的域标记为 reconciliation 未覆盖。
3. 接入一个低成本或试用第二来源作为框架验证源，优先验证价格、估值、财报和宏观数据的差异报告、调查项和降级路径，不把低成本源自动视为生产裁判。
4. 对价格、估值、财报和宏观数据分别定义容忍区间和调查门槛。
5. 把 provider health 引入数据质量报告和日报限制说明；严重问题应停止下游使用或降级结论。

当前实现状态：

- 2026-05-04 低成本基础版已完成：新增 `aits data-sources health`，读取 `config/data_sources.yaml` 和 `data/raw/download_manifest.csv`，输出 provider health score、cache path 状态、latest manifest `downloaded_at`/`row_count`/`checksum`、checksum drift、新鲜度和问题表。
- 报告按 `market_prices`、`macro_rates`、`fundamentals`、`valuation` 输出 qualified source 覆盖状态；少于两个 qualified source 的领域标记为 `NOT_COVERED`，不把单源通过伪装成 cross-provider reconciliation 通过。
- 2026-05-05 修正健康检查语义：active 来源的 manifest checksum mismatch 继续 fail closed；inactive/diagnostic-only 来源的历史 manifest checksum 漂移降为调查警告，避免旧 Yahoo 记录在 FMP/Cboe 主缓存接管后误阻断 provider health。
- 当前完成态为 `BASELINE_DONE`：框架、审计字段和失败路径已具备，但完整 `DONE` 仍依赖 owner 提供长期可用第二来源、API key、商业授权/再分发限制和生产口径策略。

验收标准：

- 报告显示 provider、endpoint、请求参数、下载时间、row count、checksum、新鲜度、字段漂移、alias 风险和跨来源差异。
- 跨供应商冲突进入调查项，不被自动平滑。
- 没有合格第二来源时，系统明确标记 cross-provider reconciliation 未覆盖，而不是伪造通过；低成本初版只能标 `BASELINE_DONE`。
- 实现时同步更新 `docs/system_flow.md` 并补充数据源健康测试。

## SCENARIO-001

标题：AI 产业链情景压力测试库

价值判断：值得做。`BACKTEST-001` 回答历史规则表现，情景压力测试回答当前仓位和 thesis 在指定冲击下的脆弱点。两者互补。

建议首批场景：

- 云厂商 CapEx 下修。
- GPU 毛利率压缩。
- HBM 供应过剩。
- 先进封装瓶颈缓解或恶化。
- 出口管制升级。
- 台海或地缘风险升级。
- 美债利率快速上行。
- AI 龙头估值压缩。
- 数据中心电力或能源限制。
- AI 应用需求低于预期。

验收标准：

- 每个场景能映射到产业链节点、ticker、thesis、风险事件和仓位 gate。
- 报告输出当前组合在该场景下的主要脆弱点、受影响 ticker、观察条件和人工复核要求。
- 场景不伪装成概率预测；必须区分历史压力、假设冲击和真实 active 风险事件。
- 实现时同步更新 `docs/system_flow.md` 并补充场景映射测试。

当前实现状态：

- 2026-05-04：进入基础实现；第一阶段建立 `scenario_library` 配置和校验报告，覆盖首批 AI 产业链压力场景、产业链节点、ticker、risk event、position gate 影响、观察条件和人工复核要求。
- 第一阶段只做审计和解释层，不把情景当作概率预测，不直接修改生产评分、`position_gate`、日报仓位或回测仓位；正式仓位影响仍需经过规则治理和人工复核。
- 完整 `DONE` 仍需要把情景结果接入日报/回测审计的当前组合暴露、可量化冲击参数和历史压力样本。
- 2026-05-04 基础版已完成：新增 `config/scenario_library.yaml`、`scenario_library` 模块、`aits scenarios validate`、`aits scenarios lookup` 和 `outputs/reports/scenario_library_YYYY-MM-DD.md`。默认情景库覆盖云 CapEx 下修、GPU 毛利率压缩、HBM 供应过剩、先进封装瓶颈、出口管制、台海地缘、利率重定价、AI 龙头估值压缩、电力/数据中心容量限制和 AI 应用需求低于预期。
- 当前完成态为 `BASELINE_DONE`：schema 校验、重复 id、节点/ticker/risk event/gate 引用、`not_probability_forecast=true`、复核到期、报告和查询入口已具备；完整 `DONE` 仍需要日报/回测的当前组合暴露、量化冲击参数和历史压力样本接入。

## CATALYST-001

标题：未来催化剂日历与事件前模式

价值判断：值得做。当前系统偏重已发生风险和当日评分，未来催化剂日历能让日报说明未来 5/20/60 天哪些事件可能改变判断，并支持事件前不主动加仓、事件后 thesis 复核等纪律。

候选事件类型：

- 财报日和业绩指引。
- CPI、FOMC、非农等宏观事件。
- 大型科技公司 CapEx 说明。
- 芯片新品发布和重要行业会议。
- 监管听证、出口管制评论期或执行日。

验收标准：

- 日报显示未来 5/20/60 天重要催化剂、关联 ticker/节点、影响范围、来源和可信度。
- 重大事件前可触发 `pre_event_review`、禁止主动加仓或降低结论等级。
- 事件后自动要求复核相关 thesis、risk event 或 valuation state。
- 实现时同步更新 `docs/system_flow.md` 并补充日历 schema 和报告测试。

当前实现状态：

- 2026-05-04：进入基础实现；第一阶段建立手工/审计 `catalyst_calendar` 配置、来源/采集时间/复核人字段、校验报告、未来 5/20/60 天分桶报告和查询入口。
- 第一阶段不编造真实事件日期，不自动接入第三方日历源，不直接改变生产评分、`position_gate` 或日报仓位；重大事件前限制只作为报告和人工复核提示。
- 完整 `DONE` 仍需要 owner 提供可信事件数据源或正式手工维护流程，并把日历摘要接入日报和事件后 thesis/risk/valuation 复核队列。
- 2026-05-04 基础版已完成：新增 `config/catalyst_calendar.yaml`、`catalyst_calendar` 模块、`aits catalysts validate`、`aits catalysts upcoming`、`aits catalysts lookup` 和 `outputs/reports/catalyst_calendar_YYYY-MM-DD.md`。报告按 5/20/60 天窗口输出 upcoming catalyst、事件前动作、事件后复核目标、来源和治理边界。
- 当前完成态为 `BASELINE_DONE`：schema、重复 id、来源/采集/复核元数据、ticker/节点/risk event 引用、高重要性事件前后复核要求、报告和查询入口已具备；完整 `DONE` 仍需要真实事件数据源或 owner 手工维护流程，并接入日报摘要和事件后复核队列。

## PORTFOLIO-002

标题：组合暴露分解

价值判断：值得做，但应定位为 `SCORE-003` 的解释层和输入准备，而不是另起一套仓位优化器。AI 仓位风险可能集中在单票、节点、地区、客户链、估值因子、ETF beta 或相关性簇。

2026-05-04 生产就绪复盘追加边界：日报不能把 `config/watchlist.yaml`、模型建议仓位或 AI 产业链评分伪装成真实账户持仓。第一阶段必须只读取用户/owner 提供的持仓文件；没有真实持仓输入时，组合暴露章节应明确 `NOT_CONNECTED`，并维持只读解释层。

验收标准：

- 日报显示当前 AI 仓位的 ticker、产业链节点、地区、客户链、因子、ETF beta、相关性簇和集中度。
- 报告能解释“AI 仓位 60%”背后的主要集中风险，而不是只显示总百分比。
- 第一版不直接改变仓位；进入仓位约束前必须和 `SCORE-003`、`GOV-001` 协同验证。
- 实现时同步更新 `docs/system_flow.md` 并补充暴露计算测试。

当前实现状态：

- 2026-05-04：进入基础实现；第一阶段目标是持仓 CSV schema、暴露计算、中文报告、CLI 和日报只读章节。
- 第一阶段不改变 `score-daily` 评分、`position_gate`、执行建议或回测仓位；`production_effect=none`。
- 2026-05-04 基础版已完成：新增 `portfolio_exposure` 模块、`aits portfolio exposure`、`docs/examples/portfolio_positions/current_positions_template.csv`、`outputs/reports/portfolio_exposure_YYYY-MM-DD.md` 和日报“组合暴露”章节。
- 基础版只读取真实持仓 CSV；缺少文件时输出 `NOT_CONNECTED`，存在但 schema/数值错误时停止。它会按 ticker、产业链节点、地区、客户链、因子、相关性簇和 ETF beta 覆盖率分解 AI 名义暴露，但不会使用观察池、模型建议仓位或 AI 产业链评分替代真实持仓。
- 当前完成态为 `BASELINE_DONE`：解释层和审计边界已具备；完整 `DONE` 仍需要 owner 提供真实账户持仓文件，并在 `SCORE-003` 中评估是否把单票、节点、相关性、FX、流动性和税费纳入风险预算。

## EXEC-001

标题：再平衡与执行纪律规则

价值判断：值得做。系统不是自动交易，但只要输出仓位建议，就需要区分“模型建议区间变化”和“是否值得执行调仓”。否则用户容易把每天的小幅变化当成交易指令。

建议规则：

- 再平衡阈值。
- 最小调仓幅度。
- 冷却期。
- 禁止追涨条件。
- 事件前不新增仓位。
- 连续信号确认。
- 高波动期降低交易频率。

报告动作语言应保持 advisory 边界，默认使用：

- 维持。
- 小幅加仓。
- 禁止主动加仓。
- 减仓到目标区间。
- 等待人工复核。
- 观察，不形成交易结论。

除非项目 owner 明确改变产品边界，否则日报不应默认输出“买入/卖出”式交易指令。

验收标准：

- 日报区分仓位建议变化和执行建议。
- 小幅仓位变化、低置信度、高波动或重大事件前状态可触发维持不动、等待确认或人工复核。
- 执行动作必须使用固定、可配置、可回测的 advisory action taxonomy，避免同一状态在不同报告中写成不同交易含义。
- 规则必须可配置、可回测、可审计，不能在报告层临时写死。
- 实现时同步更新 `docs/system_flow.md` 并补充执行纪律测试。

当前实现状态：

- 2026-05-04：进入基础实现；第一阶段建立 `execution_policy` 配置、固定 advisory action taxonomy、校验报告、查询入口，并把日报接入执行建议章节。
- 第一阶段只改变报告解释层：执行建议声明 `production_effect=none`，不改变 scoring、`position_gate`、决策快照或回测持仓计算。
- 完整 `DONE` 仍需要把冷却期、连续信号确认、成本、事件窗口和回测成交假设接入统一执行模拟；当前目标完成态预计为 `BASELINE_DONE`。
- 2026-05-04 基础版已完成：新增 `config/execution_policy.yaml`、`execution_policy` 模块、`aits execution validate`、`aits execution lookup` 和 `outputs/reports/execution_policy_YYYY-MM-DD.md`。`score-daily` 在数据质量门禁通过后校验执行政策，并在日报输出“执行建议”章节。
- 当前完成态为 `BASELINE_DONE`：固定动作词表、最小再平衡阈值、低置信度人工复核、禁止主动加仓 gate、校验报告、查询入口和日报 advisory language 已具备；完整 `DONE` 仍需要把冷却期、连续信号确认、成本模型、事件窗口和回测执行模拟统一接入。

## COST-001

标题：更真实的交易摩擦模型

价值判断：有价值但列 P2。当前回测已支持基础单边交易成本和线性滑点，短期不应因成本模型扩展挤占 point-in-time、反馈闭环和数据源健康任务。后续真实资金规模化时，应支持 spread、滑点、冲击、税费、汇率、融资利率、ETF 延迟和最小交易单位。

验收标准：

- 回测支持 bid-ask spread、滑点、市场冲击、汇率、税费、融资利率、ETF 申赎或成交延迟、最小交易单位。
- 报告显示成本敏感性，并说明哪些假设适用于研究、模拟或真实执行。
- 所有成本假设写入 run manifest 和 evidence bundle。

当前实现状态：

- 2026-05-04：进入基础实现；第一阶段先扩展回测成本假设拆分，不接入券商成交回报或逐笔盘口。
- 2026-05-04 基础版已完成：`aits backtest` 支持 commission、bid-ask spread、linear slippage、market impact、tax、FX、annual financing carry 和 ETF delay 假设；`backtest_daily.csv` 保存每类成本扣减，回测报告输出成本假设和执行成本摘要，trace run manifest 写入 `cost_assumptions`。
- 当前完成态为 `BASELINE_DONE`：成本假设可审计、可复现；完整 `DONE` 仍需要真实成交样本、券商费率/税费规则、容量约束、最小交易单位和成交质量验证。

## PROXY-001

标题：AI proxy 与 benchmark 治理

价值判断：值得做。`SMH` 是重要半导体 proxy，但不是完整 AI 产业链；回测和校准报告需要区分半导体 beta、纳指 beta 和 AI 主题 beta。

验收标准：

- 形成 `benchmark_policy`，说明何时使用 SPY、QQQ、SMH、SOXX 或自定义 AI basket。
- 自定义 AI basket 必须具备 point-in-time lifecycle，不能事后挑选赢家。
- 回测、校准和报告能说明收益来自半导体 beta、纳指 beta、AI 主题 beta 还是系统动态仓位。
- 实现时同步更新 `docs/system_flow.md` 并补充基准治理测试。

当前实现状态：

- 2026-05-04：进入基础实现；第一阶段建立 `benchmark_policy` 配置和校验报告，明确 SPY、QQQ、SMH、SOXX 的解释角色、适用场景和限制。
- 第一阶段把回测和决策校准报告接入基准政策摘要，避免把 `SMH` 误写成完整 AI 产业链表现；自定义 AI basket 先要求登记 point-in-time lifecycle、权重方法和数据来源，暂不生成正式 basket return。
- 完整 `DONE` 仍需要可复算的自定义 AI basket 历史收益、成员 lifecycle 与权重快照接入回测/校准。
- 2026-05-04 基础版已完成：新增 `config/benchmark_policy.yaml`、`benchmark_policy` 模块、`aits feedback validate-benchmark-policy`、`aits feedback lookup-benchmark-policy` 和 `outputs/reports/benchmark_policy_YYYY-MM-DD.md`。回测与决策校准报告会输出 benchmark policy 状态、选中 proxy/benchmark 的角色和解释限制。
- 当前完成态为 `BASELINE_DONE`：SPY/QQQ/SMH/SOXX 的基准治理、schema 校验、重复 ticker/id 检查、source path 检查、本次选择口径校验、报告和查询入口已具备；完整 `DONE` 仍需要 custom AI basket 的 point-in-time 成员 lifecycle、权重快照和可复算 basket return。

## GOV-001

标题：规则变更生命周期与 rule card

价值判断：值得优先做。反馈闭环可以提出规则改进，但不能自动无审计地改生产规则。核心评分、`position_gate`、risk event、valuation gate、thesis 状态机都需要规则版本、适用范围、上线原因、验证报告、已知失败模式和回滚方式。

验收标准：

- 核心规则具备 production / candidate / retired 生命周期。
- 每次规则变更能追溯 rule card、验证报告、批准记录和回滚方式。
- `score-daily`、回测、decision snapshot 和 evidence bundle 能记录 rule version。
- 实现时同步更新 `docs/system_flow.md` 并补充规则版本测试。

当前实现状态：

- 2026-05-04：进入基础实现；第一阶段先建立 rule card registry，把当前已在 production 路径中使用的 scoring、position gate、risk event、valuation gate、thesis state、data quality 与 feedback learning 规则登记为 baseline rule cards。
- 第一阶段目标是 rule card 校验、报告和查询，不把 baseline 记录伪装成完整 owner 批准流程；未来规则变更仍必须先经过 `EXPERIMENT-001` replay/shadow，再进入 `GOV-001` 审批。
- 2026-05-04 基础版已完成：新增 `config/rule_cards.yaml`、`rule_governance` 模块、`aits feedback validate-rule-cards`、`aits feedback lookup-rule-card` 和 `outputs/reports/rule_governance_YYYY-MM-DD.md`。默认 rule cards 覆盖 scoring、position_gate、source_policy、risk_events、thesis_state、data_quality 和 feedback_loop。
- 当前完成态为 `BASELINE_DONE`：rule card registry、schema 校验、重复 id 检查、来源路径检查、复核到期警告、报告和查询入口已具备；完整 `DONE` 仍需要正式 owner approval/promotion/retirement 流程，以及 `score-daily`、回测、decision snapshot/evidence bundle 写入 rule version。
- 2026-05-04：`GOV-002` 已完成基础实现：`score-daily` 和 `backtest` 会校验 rule card registry，并把适用于本次命令的 production rule versions 写入 trace run manifest；decision snapshot 保存同一 manifest。该 manifest 表示本次运行使用的当前规则登记，不是历史时点审批证明；正式 approval/promotion/retirement 仍留在 `GOV-001` 后续 owner 流程。

## OPS-001

标题：运行监控与 pipeline health report

价值判断：有价值但列 P2。当前重点仍是数据质量和投资解释；自动日常运行稳定后，应补运行状态、耗时、输入版本、质量门禁状态、API 错误、缺失数据和报告生成状态。

验收标准：

- 每次运行记录成功/失败、耗时、输入文件版本、质量门禁状态、API 错误、缺失数据和报告生成状态。
- 失败时输出明确排查入口和相关日志引用。
- 不把运行成功误写成投资结论有效；投资结论仍受数据质量和覆盖限制约束。

当前实现状态：

- 2026-05-04：进入基础实现；第一阶段提供只读 `pipeline health` 报告，检查关键输入/输出文件存在性、mtime、大小和排查入口。
- 第一阶段不接入后台调度器或日志采集系统，也不把运行成功解释为投资结论有效；投资结论仍由数据质量、结论使用等级和报告覆盖决定。
- 完整 `DONE` 仍需要每次命令运行的结构化 run log、耗时、异常栈、API 错误和自动化调度状态；当前目标完成态预计为 `BASELINE_DONE`。
- 2026-05-04 基础版已完成：新增 `pipeline_health` 模块、`aits ops health` 和 `outputs/reports/pipeline_health_YYYY-MM-DD.md`，检查价格缓存、利率缓存、数据质量报告、特征缓存、评分缓存和日报 artifact。
- 当前完成态为 `BASELINE_DONE`：关键 artifact 存在性、mtime、文件大小、问题清单和排查入口已具备；完整 `DONE` 仍需要结构化 run log、耗时、异常栈、API 错误和调度状态采集。

## ALERT-001

标题：投资与数据告警系统

价值判断：有价值但应在触发规则稳定后推进。告警必须有等级、来源、触发条件、解除条件和引用，避免重复刷屏。

候选告警：

- 数据源断更。
- 估值快照过期。
- risk event 从 watch 升级 active。
- L2/L3 风险触发。
- thesis 从 active 变 warning/challenged。
- 仓位上限下降超过阈值。
- 未来 5 天有重大财报或政策事件。

验收标准：

- 告警有 data/system 和 investment/risk 两类。
- 每条告警有等级、来源、触发条件、解除条件、对应 claim/evidence 引用和去重策略。
- 报告显示告警摘要，通知渠道接入必须可关闭、可审计。

当前实现计划：

- 2026-05-04：进入基础实现；第一阶段新增只读 `alert` 记录和报告，来源包括数据质量/特征警告、低可信模块、估值快照健康、L2/L3 或可触发仓位闸门的 risk event、thesis 复核警告、仓位上限大幅下降和未来 5 天 high/critical catalyst。
- 第一阶段输出单独 Markdown 告警报告，并在 `score-daily` 日报中加入告警摘要；告警记录必须声明 `production_effect=none`，不能直接改变评分、`position_gate`、回测仓位或执行建议。
- 第一阶段不接入邮件、IM、桌面推送或后台调度；通知渠道、静默时间、确认/解除流和重复抑制持久化留待后续。

当前实现状态：

- 2026-05-04 基础版已完成：新增 `alerts` 模块、`outputs/reports/alerts_YYYY-MM-DD.md` 和 `score-daily` 日报“告警摘要”；每条告警包含 category、severity、source、trigger_condition、clear_condition、claim_refs、evidence_refs、dedupe_key 和 `production_effect=none`。
- 第一阶段覆盖 data/system 告警和 investment/risk 告警：数据质量、特征警告、低可信模块、估值快照低可信/过期、估值拥挤、可触发仓位闸门的 L2/L3 risk event、thesis 复核警告、非日常 position gate、仓位上限大幅下降和未来 5 天 high/critical catalyst。
- 为降低噪声，`score_model` 与静态 `portfolio_limits` 这类日常约束不作为告警触发；它们仍保留在日报仓位解释和 gate 明细中。
- 当前完成态为 `BASELINE_DONE`：真实 2026-05-04 日报可生成告警报告，状态为 `ACTIVE_WARNINGS`。完整 `DONE` 仍需要通知渠道、确认/解除状态、持久化去重、调度状态和 owner 告警策略。

## TEST-001

标题：系统级不变量测试

价值判断：值得优先做。该系统的核心风险不是单个函数 bug，而是后续改动破坏关键投资原则。必须用测试保护 source policy、point-in-time、gate 优先级、LLM 隔离和 watch/active 区分。

首批不变量：

- `watch` 风险事件不应直接触发重仓位扣减。
- `public_convenience` 来源不能自动评分。
- LLM 输出不能直接影响仓位。
- `position_gate` 必须取最严格上限。
- 回测不能使用未来估值快照。
- SEC 指标必须满足 `filed_date <= signal_date`。
- 估值采集日之后才可见。
- 缺少数据质量通过状态时，评分、回测和报告不能输出完整结论。

验收标准：

- 每条核心投资原则都有系统级测试。
- 规则改动不能破坏 source policy、point-in-time、gate 优先级、LLM 隔离、watch/active 区分。
- 新增关键规则时必须同步补不变量测试或说明不适用原因。

## STORAGE-001

标题：从 CSV 逐步评估 DuckDB / Parquet

价值判断：有价值但列 P3。CSV 对当前本地 MVP 足够透明；只有当 schema 演进、join 性能、历史快照、字段类型约束或审计查询压力出现时，才应推进内部存储升级。

验收标准：

- 不破坏现有 CSV 输入输出和用户可审计路径。
- 内部可用 typed schema 存储，报告和回测仍可追溯 checksum、数据版本和来源。
- 迁移计划包含兼容期、回滚方式和数据验证。

2026-05-06 当前决策：暂缓升级。全量校验、短区间真实缓存回测和报告生成未暴露 CSV 层面的 schema、join 性能、历史快照、类型约束或审计查询瓶颈；当前 CSV 格式对本地 owner 复核更透明。若后续出现可量化瓶颈，再重新打开本任务并先补迁移计划、兼容期、回滚方式和数据验证。

## UI-001

标题：证据下钻型仪表盘

价值判断：有价值但列 P2。仪表盘不应只展示曲线，而应围绕结论下钻、gate、thesis、risk event、数据源和 evidence bundle 设计。应等报告结构和 evidence 引用稳定后推进。

验收标准：

- 仪表盘可从结论下钻到模块分、gate、risk event、thesis、数据源、原始证据和质量报告。
- 支持本期/上期差异、仓位限制原因和未来观察条件。
- 支持三类读者模式：快速读者只看结论卡、动作建议、最大风险和改变判断条件；投资复核者查看变化原因树、产业链节点、thesis 状态、risk gate 和仓位上限来源；系统审计者查看 claim/evidence/dataset/quality refs、trace lookup、数据质量门禁和 source policy。
- 不替代 Markdown 报告的审计责任；关键结论仍需可导出和可追溯。

2026-05-04 第一阶段实现范围：主要使用者是项目 owner 本人，优先关注报告结论与实际输入数据的联系以及论证逻辑，视觉风格保持简约。第一版先生成本地静态 evidence-first HTML，不引入后台服务、实时刷新或新投资计算；详细拆解见 `docs/requirements/ui_evidence_dashboard_2026-05-04.md`。

## SECURITY-001

标题：密钥、日志和供应商权限治理

价值判断：有价值。当前 FMP API key 已要求从环境变量读取且不写入报告；随着数据源扩展，需要 secret hygiene、日志脱敏、供应商权限和内容使用限制治理。

验收标准：

- API key 不进入报告、日志、manifest、trace bundle 或错误输出。
- 支持 secret scan 或等价检查。
- 供应商权限、缓存限制、再分发限制和 LLM 处理限制记录在 data source catalog。
- 新数据源接入必须通过安全与权限检查。

当前实现状态：

- 2026-05-04：进入基础实现；第一阶段提供本地 secret hygiene 扫描报告，检查配置、文档、报告、manifest、trace bundle 中的疑似 API key、token、secret 或 bearer credential。
- 第一阶段不接入企业级密钥管理、pre-commit hook 或供应商权限审批流；只提供可审计的本地扫描和问题清单。
- 完整 `DONE` 仍需要供应商权限治理、日志脱敏策略、pre-commit/CI 集成和 owner 审批流程；当前目标完成态预计为 `BASELINE_DONE`。
- 2026-05-04 基础版已完成：新增 `secret_hygiene` 模块、`aits security scan-secrets` 和 `outputs/reports/secret_hygiene_YYYY-MM-DD.md`，疑似密钥只输出脱敏片段。
- 当前完成态为 `BASELINE_DONE`：本地扫描、脱敏报告和 CLI 已具备；完整 `DONE` 仍需要供应商权限治理、日志脱敏策略、pre-commit/CI 集成和 owner 审批流程。

## DOC-001

标题：系统使用边界与结论等级

价值判断：有价值但应与 `SCORE-002` 和 `REPORT-002` 协同。系统是投资决策支持，不是自动交易；报告需要将结论等级标准化，避免用户把所有分数当成同等可靠信号。

建议等级：

- `trend_only`：当前范围下只用于趋势判断和投研辅助，不触发交易。
- `actionable`：可作为仓位复核依据；当前系统范围不默认输出该成功态，除非 owner 未来重新提高到仓位复核/交易执行范围。
- `review_required`：必须人工复核。
- `research_only`：仅研究观察，不应用于仓位。
- `data_limited`：数据不足，结论降级。
- `backtest_limited`：回测输入覆盖不足。

结论等级只回答“这个结论能用于什么范围”。当前 owner 决策是趋势判断/投研辅助，不触发交易；报告还需要独立的投资姿态标签，回答“当前 AI 产业链处于什么状态”。建议第一版投资姿态标签：

- 积极进攻。
- 中高配。
- 中高配但受限。
- 中性观察。
- 防守降仓。
- 人工复核。

两层状态必须分开输出，避免把市场状态、分数、仓位和结论可靠性混为一个答案。

验收标准：

- 每份日报和回测报告都有结论等级。
- 每份日报有独立投资姿态标签，并能解释该标签与结论等级、评分、置信度和仓位 gate 的关系。
- 低置信度、数据不足、来源不足、回测覆盖不足、事件前状态会自动降级。
- 报告清楚说明降级原因、解除条件和可追溯证据。

当前实现状态：

- 2026-05-04：进入基础实现；第一阶段在日报和回测报告中输出结论使用等级，把 `actionable`、`review_required`、`research_only`、`data_limited`、`backtest_limited` 与投资姿态标签分开。
- 第一阶段只改变报告解释边界，不改变 scoring、`position_gate`、execution policy、回测仓位或交易成本计算。
- 完整 `DONE` 仍需要把状态 taxonomy 配置化、接入未来事件窗口和更完整的 owner review policy；当前目标完成态预计为 `BASELINE_DONE`。
- 2026-05-04 基础版已完成：新增 `conclusion_boundary` taxonomy，日报和回测报告均输出“结论使用等级”章节，说明结论等级、投资姿态标签、降级原因、解除条件和证据引用。
- 当前完成态为 `BASELINE_DONE`：`review_required`、`data_limited`、`backtest_limited` 等降级路径已具备；完整 `DONE` 仍需要 taxonomy 配置化、未来 catalyst/event window 降级和正式 owner review policy。
- 2026-05-04：根据 owner 最新范围，结论等级新增 `trend_only`，`score-daily` 和回测以 `trend_judgment` 范围运行；数据质量全绿时也只能显示趋势判断，不自动升级为仓位复核或交易执行。

## 状态记录

- 2026-05-04：创建需求文档，完成价值评估并登记任务。当前仅完成需求评估和任务拆解，尚未实现。
- 2026-05-04：`EVIDENCE-001` 已完成基础实现：新增 `market_evidence` schema、YAML/CSV 导入、`aits evidence import-csv/validate`、来源等级校验、重复检查、LLM 待复核隔离和 public_convenience 不可自动评分测试。
- 2026-05-04：`FEEDBACK-001` 已完成基础实现：`aits score-daily` 在数据质量门禁通过后写入 deterministic `decision_snapshot_YYYY-MM-DD.json`，快照包含 market regime、评分、置信度、仓位、position gates、质量状态、人工复核、估值/风险状态、trace bundle 引用和配置路径。
- 2026-05-04：`FEEDBACK-002` 已完成基础实现：新增 `aits feedback calibrate`，先复用数据质量门禁，再从历史 `decision_snapshot` 和 `prices_daily.csv` 生成 `decision_outcomes.csv` 与中文校准报告；支持 1D/5D/20D/60D/120D 窗口、AI proxy/benchmark return、超额收益、最大回撤、实现波动、hit/miss，并按总分、置信度、gate、thesis、风险等级和估值状态分桶；报告明确样本不足、窗口重叠和不得自动修改生产规则。
- 2026-05-04：`CAUSE-001` 已完成基础实现：新增 `decision_causal_chains` ledger、`aits feedback build-causal-chain` 和 `aits feedback lookup-chain`，把 evidence、模块 score/confidence 变化、position gate、decision snapshot、quality 和后验 outcome 串成可查询链路；测试覆盖未来 outcome 只能进入 `post_signal_observations`，不得改写 `signal_time_context`。
- 2026-05-04：`LEARNING-001` 已完成基础实现：新增 `decision_learning_queue`、`aits feedback build-learning-queue` 和 `aits feedback lookup-learning`，从 causal chain 生成失败/成功样本复核队列，记录归因分类、evidence、owner、next step 和规则候选需求；测试覆盖 `rule_issue`、`data_issue`、`sample_limited` 和样本不足不生成规则候选。
- 2026-05-04：`LOOP-001` 已完成基础实现：新增 `aits feedback loop-review` 和闭环复核报告，汇总 market evidence、decision snapshots、decision_outcomes、causal chains、learning queue、rule candidate 接入状态、blocked tasks 和 task register 状态；报告声明 `ai_after_chatgpt` 和执行/复核/研究用途边界。
- 2026-05-04：`EXPERIMENT-001` 进入实现；第一阶段目标是 rule experiment ledger 和 candidate isolation，不伪造已完成历史 replay 或 production rule approval。
- 2026-05-04：`EXPERIMENT-001` 达到 `BASELINE_DONE`：候选规则实验台账、CLI、中文报告、loop-review 接入和 candidate isolation 测试完成；`python -m ruff check src tests`、`python -m pytest -q` 通过。真实 replay runner、forward shadow 观测和 GOV-001 rule card 批准仍待后续实现。
- 2026-05-04：`GOV-001` 进入实现；第一阶段目标是 rule card registry、校验报告和查询入口，先补齐现有 production 规则的版本与审计登记。
- 2026-05-04：`GOV-001` 达到 `BASELINE_DONE`：rule card registry、治理校验 CLI、查询 CLI、中文报告、系统流图和测试完成；`python -m ruff check src tests`、`python -m pytest -q` 通过。正式 owner approval/promotion/retirement 流程和 rule version 注入仍待后续实现。
- 2026-05-04：根据最终报告呈现形式讨论，补充 `EXEC-001` 的 advisory action taxonomy、`DOC-001` 的“双层状态”设计和 `UI-001` 的三类读者模式；完整报告结构由 `docs/requirements/report_decision_chain_presentation_2026-05-04.md` 与 `REPORT-003/004` 承接。
- 2026-05-04：`DOC-001` 达到 `BASELINE_DONE`：日报和回测报告输出结论使用等级，低置信度、来源不足、人工复核问题和回测覆盖不足会降级；投资姿态标签与结论等级分开解释。
- 2026-05-04：补充 `trend_only` 范围边界，原因：owner 明确当前系统只做趋势判断，不需要实际触发交易。
- 2026-05-04：`OPS-001` 达到 `BASELINE_DONE`：新增 pipeline health 只读报告和 CLI，检查关键 artifact 并输出排查入口；报告明确运行健康不等于投资结论有效。
- 2026-05-04：`SECURITY-001` 达到 `BASELINE_DONE`：新增 secret hygiene 扫描 CLI 和脱敏报告；企业级密钥管理、pre-commit/CI 和供应商权限审批仍待后续。
- 2026-05-04：`DATA-002` 已完成低成本基础版：新增 `aits data-sources health` 和 `outputs/reports/data_sources_health_YYYY-MM-DD.md`，覆盖 provider health score、cache/manifest/row count/checksum/freshness 检查、manifest checksum mismatch 失败，以及 qualified source 不足时的 reconciliation `NOT_COVERED` 声明；owner 已验证 SEC User-Agent、FMP、FRED、Tiingo EOD 和 EODHD Fundamentals 初版权限可访问，EODHD EOD 价格未订阅且价格核验使用 Tiingo；完整 `DONE` 仍依赖生产级第二来源、商业授权/再分发限制和长期口径策略。
- 2026-05-04：`UI-001` 进入第一阶段实现；owner 明确第一版主要服务个人复核，优先连接报告结论、论证链、trace evidence 和实际输入数据，风格保持简约。
- 2026-05-04：`UI-001` 达到 `BASELINE_DONE`：新增 `aits reports dashboard` 静态 HTML 输出，按快速读者、投资复核者和系统审计者分层展示结论、论证链、gate、thesis/risk/valuation 状态、claim/evidence/dataset/quality refs、输入路径、checksum 和 trace lookup；完整 `DONE` 仍需要更完整交互服务、跨日报对比和 reader mode 配置。
- 2026-05-04：`UNIVERSE-001` 已完成基础实现：新增 `config/watchlist_lifecycle.yaml`、`aits watchlist validate-lifecycle` 和回测 signal_date lifecycle 过滤，测试覆盖尚未进入观察池的 ticker 不参与历史市场特征。
- 2026-05-04：`TEST-001` 已推进为完成状态：系统级不变量测试覆盖 watch 风险事件不自动评分、低证据等级/公开便利源隔离、LLM evidence 隔离、watchlist point-in-time 过滤、decision snapshot 写入、评分置信度和估值 PIT 可信度。
