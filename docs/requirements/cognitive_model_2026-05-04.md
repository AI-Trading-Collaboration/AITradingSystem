# AI 产业链认知模型升级需求

状态：BASELINE_DONE

最后更新：2026-05-04

关联任务：`COGNITION-001`、`EVIDENCE-001`、`CHAIN-001`、`THESIS-001`、`SCORE-002`、`SCORE-003`、`REPORT-002`、`FEEDBACK-001`、`FEEDBACK-002`、`CAUSE-001`、`LEARNING-001`、`EXPERIMENT-001`、`GOV-001`、`LOOP-001`、`LLM-001`、`DOC-001`

## 背景

本需求来自 2026-05-04 关于系统长期目标的讨论。目标不是把系统升级成自动交易黑箱，也不是让 LLM 直接做加仓、减仓或改权重决策；目标是把现有 AI 产业链仓位管理系统升级为一个可审计、可复盘、可校准的投资认知模型。

正式目标表述：

```text
构建一个面向 AI 产业链仓位管理的可审计认知模型。

系统基于市场价格、宏观流动性、基本面、估值、政策/地缘风险、
产业链事件、交易 thesis 和历史复盘结果，持续更新对 AI 产业链状态、
风险环境、假设有效性、判断置信度和仓位边界的认知。

系统不直接自动交易，不允许未经验证的 LLM 或新闻结论直接改变仓位。
所有规则迭代必须经过 point-in-time 回测、稳健性测试、shadow mode
和人工批准。
```

默认研究和报告结论仍以 `ai_after_chatgpt` 市场阶段为主，即默认从 2022-12-01 之后的 AI 交易周期得出主要结论。更早历史可以用于 warm-up、压力测试和 regime 对比，但不得被写成当前 AI 周期的默认结论窗口。

## 设计边界

允许系统自动更新：

- `belief_state` 中的市场状态、产业链节点状态、风险状态、thesis 状态摘要和置信度。
- 数据源健康状态、证据覆盖缺口、报告降级原因和人工复核优先级。
- 历史判断的 outcome、校准统计、错误归因和候选规则建议。

允许系统自动提出但不能直接上线：

- scoring rule 调整建议。
- `position_gate` 调整建议。
- 估值拥挤 gate 或风险预算规则候选。
- thesis 状态机迁移规则候选。

禁止系统无审批自动改变：

- 生产评分权重。
- 生产 `position_gate` 上限。
- 正式仓位建议规则。
- thesis `invalidated` 状态。
- 交易执行建议。
- 任何会绕过数据质量门禁、point-in-time 约束或人工批准的规则。

LLM 的角色限制：

- 可以做信息抽取、来源摘要、节点映射、矛盾提示和待复核证据分类。
- 不得直接输出买卖、加仓、减仓或仓位上限。
- 未经人工确认或明确来源等级允许前，不得把 LLM 输出接入自动评分或仓位层。

## 四层模型

### 第一层：市场感知模型

回答：

```text
市场现在发生了什么？
```

输入包括价格趋势、相对强弱、VIX、利率、美元、SEC / IR 基本面、估值快照、风险事件、催化剂、新闻/公告证据和交易结果。

本层职责是标准化信息、记录来源、保存证据和暴露数据质量限制，不直接做买卖判断。

### 第二层：产业链认知模型

回答：

```text
这些信息改变了 AI 产业链的哪个部分？
```

候选节点包括云 CapEx、GPU/ASIC、HBM、先进封装、晶圆代工、设备材料、电力/数据中心和 AI 应用需求。

输出应区分：

- 哪个节点热度上升。
- 哪个节点健康度下降。
- 哪个节点数据不足。
- 哪个节点风险集中。
- 哪个节点可能已经被市场提前定价。

该层应与 `CHAIN-001` 的 `node_heat`、`node_health`、覆盖率和集中度协同。

### 第三层：投资假设模型

回答：

```text
原来的 thesis 还成立吗？
```

该层应依赖 `THESIS-001` 的状态机：

```text
draft -> active -> warning -> challenged -> invalidated -> closed
```

状态迁移必须保留证据、来源、触发条件和人工复核状态。短期价格波动、估值拥挤、基本面恶化、风险事件升级和 thesis 证伪必须分开表达，不能都压缩成一个总分变化。

### 第四层：自我校准模型

回答：

```text
系统过去的判断质量怎么样？
```

该层依赖 `decision_snapshot`、`decision_outcomes`、`decision_causal_chain`、校准报告、错误归因和候选规则实验。它可以提出规则改进草案，但不得直接修改 production 规则。

## COGNITION-001

标题：只读 `belief_state` 认知状态层

价值判断：值得优先做。当前系统已经具备数据质量门禁、市场特征、评分、`position_gate`、thesis、risk event、估值、trace bundle 和回测审计，但中间判断仍主要散落在各模块和报告段落里。`belief_state` 的作用是把系统“当前相信什么、依据什么、置信度如何、哪些风险限制仓位、哪些条件会改变判断”结构化保存下来。

第一版定位：

- 只读解释层。
- 不改变生产评分、仓位、gate 或交易建议。
- 不接受 LLM 自由文本直接写入正式结论。
- 作为 `score-daily` 报告、`decision_snapshot`、后续校准和规则实验的统一中间状态。

建议输出文件：

```text
data/processed/belief_state/belief_state_YYYY-MM-DD.json
data/processed/belief_state_history.csv
outputs/reports/daily_score_YYYY-MM-DD.md#认知状态
```

第一版 schema 应覆盖：

```yaml
schema_version: 1
as_of: 2026-05-04
market_regime:
  regime_id: ai_after_chatgpt
  requested_start: 2022-12-01
  actual_range: 2022-12-01..2026-05-04
data_quality:
  status: PASS
  quality_report_ref: outputs/reports/data_quality_YYYY-MM-DD.md
  limiting_issues: []
market_state:
  trend_belief: constructive
  liquidity_belief: neutral_to_tight
  risk_sentiment_belief: fragile
  confidence:
    data_quality_confidence: high
    evidence_strength: medium
    regime_fit_confidence: medium
ai_chain:
  gpu_asic:
    belief: demand_strong_but_crowded
    node_heat: high
    node_health: medium_high
    evidence_strength: high
    confidence: high
valuation:
  belief: crowded
  action_bias: limit_new_adds
risk:
  policy_geo:
    belief: watch
    confidence: medium
thesis:
  active_count: 4
  warning_count: 1
  challenged_count: 0
  invalidated_count: 0
position_boundary:
  model_range: 60-80
  final_range: 60-65
  limiting_factors:
    - valuation_crowding
    - confidence_cap
    - concentration_risk
evidence_refs:
  - claim_id: daily_score.overall
  - quality_id: data_quality.current
```

置信度必须拆分为至少以下维度，而不是只输出单个 `high` / `medium` / `low`：

- `data_quality_confidence`：数据门禁、覆盖率、来源可信度。
- `evidence_strength`：支持当前判断的证据强度。
- `regime_fit_confidence`：当前样本是否适配 `ai_after_chatgpt` 或指定 regime。
- `model_calibration_confidence`：历史校准对该类判断是否有足够样本。
- `human_review_status`：是否需要人工复核、是否已确认。

分步开发：

1. 设计 `belief_state` schema 和历史索引，明确字段、枚举、来源引用和兼容策略。
2. 在 `aits score-daily` 通过数据质量门禁后生成只读 `belief_state`，并在日报中输出中文认知状态摘要。
3. 将 `belief_state` 引用写入 `decision_snapshot` 和 evidence bundle，支持 trace 反查。
4. 在 `FEEDBACK-002`、`CAUSE-001` 和 `LEARNING-001` 中使用历史 `belief_state` 做校准和错误归因。
5. 经足够 shadow 观察和 `GOV-001` 批准后，再评估是否允许部分 `belief_state` 字段影响置信度或报告降级，不直接影响生产仓位。

验收标准：

- 每次 `score-daily` 通过质量门禁后可生成机器可读 `belief_state`。
- `belief_state` 能结构化表达市场状态、产业链节点状态、估值状态、风险状态、thesis 状态、仓位边界和限制因素。
- `belief_state` 中每个核心判断都有数据质量、证据或人工复核引用。
- 日报以中文显示认知状态摘要，并明确它是解释层还是已进入 production 规则。
- 第一版不得改变正式评分、`position_gate`、回测仓位或交易建议。
- 实现时同步更新 `docs/system_flow.md` 并补充 schema、报告输出、trace 引用和“belief_state 不直接改仓位”的测试。

## 与既有任务的关系

|任务|关系|
|---|---|
|`EVIDENCE-001`|提供新市场信息的证据账本，是 `belief_state` 的增量输入之一。|
|`CHAIN-001`|提供产业链节点热度、健康度、覆盖率和集中度，是 `belief_state.ai_chain` 的主要来源。|
|`THESIS-001`|提供 thesis 状态机，是 `belief_state.thesis` 的主要来源。|
|`SCORE-002`|提供评分和判断置信度拆分，需与 `belief_state.confidence` 对齐。|
|`SCORE-003`|后续风险预算可以读取稳定后的认知状态，但必须经过验证和治理。|
|`REPORT-002`|报告变化原因树应引用 `belief_state` 的限制因素和改变判断条件。|
|`FEEDBACK-001`|`decision_snapshot` 应保存或引用当日 `belief_state`。|
|`FEEDBACK-002`|校准报告按历史 `belief_state` 分桶复盘判断质量。|
|`CAUSE-001`|因果链把证据、`belief_state` 变化、gate、snapshot 和 outcome 串起来。|
|`LEARNING-001`|错误归因用历史 `belief_state` 判断错误类型。|
|`EXPERIMENT-001`|候选规则只能在 shadow/replay 中观察其对 `belief_state` 和仓位边界的影响。|
|`GOV-001`|任何让 `belief_state` 影响 production 规则的变更必须走 rule card 和人工批准。|
|`LOOP-001`|周期复核报告汇总 `belief_state` 变化、校准结果和任务状态。|
|`LLM-001`|LLM 只能辅助生成待复核证据，不能直接写入正式 `belief_state` 的可执行判断。|
|`DOC-001`|结论等级应读取 `belief_state` 中的数据限制、置信度和人工复核状态。|

## 阶段路线

### 阶段 1：可审计认知状态

目标：

```text
系统不仅输出分数，还输出只读 belief_state。
```

交付：

- `belief_state/belief_state_YYYY-MM-DD.json`
- `belief_state_history.csv`
- 日报中的认知状态摘要
- `decision_snapshot` 中的 `belief_state_ref`

状态边界：不改变生产仓位。

### 阶段 2：结果反馈与校准

目标：

```text
系统知道自己过去相信什么，以及后来发生了什么。
```

交付：

- `decision_snapshots.csv`
- `decision_outcomes.csv`
- `score_calibration_report.md`
- `risk_gate_feedback_report.md`
- `thesis_feedback_report.md`

依赖：`FEEDBACK-001`、`FEEDBACK-002`、`CAUSE-001`。

### 阶段 3：候选规则生成与 shadow 验证

目标：

```text
系统可以提出改进建议，但不能自动上线。
```

交付：

- `improvement_proposals.yaml`
- `candidate_rules/`
- `shadow_mode_report.md`
- production vs candidate 对比报告

依赖：`EXPERIMENT-001`、`GOV-001`。

### 阶段 4：有限自适应

目标：

```text
系统在严格边界内自动调整解释层、置信度层和复核优先级。
```

可自动调整：

- 数据源健康评分。
- 结论置信度。
- 报告提示等级。
- watchlist 或 risk event 人工复核优先级。

仍禁止自动调整：

- 生产仓位规则。
- 核心评分权重。
- `position_gate` 上限。
- thesis `invalidated` 状态。
- 正式交易建议。

## 开放问题

- 是否还需要在 `score-daily` 内部输出之外补独立 `aits cognition build-belief-state` 子命令。
- `belief_state_history.csv` 第一版仅保存摘要和完整 JSON 路径；后续是否需要展开更多字段供校准查询。
- 哪些 `belief_state` 字段必须人工确认后才能进入日报顶部结论。
- 产业链节点健康度、估值拥挤和 thesis 状态之间的冲突如何排序。
- `model_calibration_confidence` 的样本量阈值如何定义。

## 状态记录

- 2026-05-04：创建需求文档，正式定义认知模型边界、`belief_state` 第一阶段任务、阶段路线和与既有任务的关系。当前仅完成需求评估和任务拆解，尚未实现。
- 2026-05-04：`COGNITION-001` 已完成首版实现：`score-daily` 通过质量门禁后写入 `data/processed/belief_state/belief_state_YYYY-MM-DD.json` 和 `belief_state_history.csv`，日报输出中文认知状态摘要，decision snapshot 写入 `belief_state_ref`，evidence bundle 增加 `belief_state` dataset/claim 引用，并补充了“belief_state 不直接改仓位”的测试。第一版仍是只读解释层，`model_calibration_confidence` 暂为 `not_assessed`，不改变正式评分、`position_gate`、回测仓位或交易建议。
