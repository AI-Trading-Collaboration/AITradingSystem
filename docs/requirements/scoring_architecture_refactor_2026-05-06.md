# 评分架构解耦与风险事件生命周期改进计划

状态：BASELINE_DONE

最后更新：2026-05-06

关联任务：`SCORE-004`、`SCORE-005`、`RISK-009`、`REPORT-007`、`CALIBRATION-001`、`SCORE-001`、`SCORE-002`、`SCORE-003`、`RISK-003`、`RISK-004`、`RISK-005`、`BACKTEST-001`、`FEEDBACK-002`

## 背景

本计划来自 2026-05-06 对当前评分系统的架构复核。复核结论是：当前系统的方向正确，已经不是简单把每条新闻风险线性相加；它采用“结构化信号线性评分 + 数据质量、风险事件、估值、thesis、组合和宏观预算非线性门控”的可解释结构。

但当前生产路径仍更接近第一版可解释决策系统。后续优化应集中在解耦 `alpha/conviction`、`risk state`、`execution/portfolio constraint`，并防止同一证据在模块评分和仓位闸门中被静默重复惩罚。

本计划初始登记后续设计和任务；第一阶段只允许做审计字段和报告结构，不改变正式评分、回测仓位或日报执行建议。后续任何进入 production scoring 或 position gate 行为的改动，都必须经过 replay、forward shadow、rule card 和 owner approval。

2026-05-06 第一阶段已完成：日报、decision snapshot 和 trace 增加
`Base Signal / Risk Caps` 与 `score_architecture` 审计输出；`PositionGate`
增加 `gate_class`、`target_effect` 和 `execution_effect` 字段；风险事件发生记录
schema 增加 `lifecycle_state`、`dedup_group`、`primary_channel`、`used_in_alpha`、
`used_in_gate`、`decay_half_life_days`、`expiry_time` 和 `resolution_reason`。本阶段
只改变审计字段和报告结构，不改变正式评分、position gate 最严格上限、回测仓位或执行建议。

## 价值评估摘要

|建议|处理结论|原因|
|---|---|---|
|拆分方向信号和风险约束|新增 `SCORE-004`，P1|当前已有 `position_gate`，但线性核中仍同时包含方向信号和风险状态；需要显式角色分类，避免“趋势强 + 尾部风险高”被总分中和。|
|归一化、置信度和覆盖率|部分已覆盖，纳入 `SCORE-004` 后续|`SCORE-002` 已输出 confidence，`CALIBRATION-001` 已建立权重 overlay 基础设施；下一步需要统一 factor semantic、score direction、horizon 和 confidence 使用边界。|
|消息面风险非线性处理|已覆盖基础版，新增 `RISK-009`|`RISK-003/004/005/007/008` 已保证 LLM/news 不直接评分；缺口是事件生命周期、衰减、过期和 dedupe group。|
|防止双重计分|新增 `SCORE-004` 与 `RISK-009`|当前 source policy 和 trace 有基础引用，但还没有生产级 `primary_channel / used_in_alpha / used_in_gate / dedup_group` 审计规则。|
|硬闸门与软闸门|新增 `SCORE-005`，P1|当前主要使用 `min(cap)`；需要区分 hard block、soft cap、速度限制、确认要求和只读解释，避免过度保守或跳变。|
|时间衰减和事件生命周期|新增 `RISK-009`，P1|风险事件已有 active/watch/resolved 和人工复核声明，但还没有按事件类型配置半衰期、过期、复核到期和 stale 风险释放。|
|regime-aware 权重|已有 `CALIBRATION-001` 承接|权重 overlay 已支持 context matching；后续可在该任务阶段 2-7 中把 `market_regime`、risk-on/risk-off 和高波动期作为 overlay context。无需重复新增任务。|
|非线性仓位映射和执行平滑|新增 `SCORE-005`，P1|当前仓位区间是分段映射，执行纪律已有最小调仓阈值；缺口是 soft gate、非线性目标、turnover limit、cooldown 和 rebalance band 的统一回测。|
|报告拆分 base signal 与 risk-adjusted conclusion|新增 `REPORT-007`，P1|`REPORT-003` 已有结论卡，但还未固定输出 base signal、raw position、risk caps、final position 的结构化块。|
|回测、消融和稳定性|大部分已覆盖|`BACKTEST-001` 已完成稳健性、权重扰动、趋势基线、随机同换手率和样本外验证；`FEEDBACK-002/SHADOW-003/REPORT-005` 继续承接真实 PIT outcome。暂不新增任务。|

## SCORE-004：Alpha、风险状态和约束通道解耦

价值判断：值得做，P1。现有系统已经把最终仓位闸门独立出来，但线性评分模块仍混合了方向信号和风险状态。下一步应先做审计型解耦，再决定是否改变 production 权重或仓位。

目标结构：

```text
alpha_conviction
  -> base_signal_score
  -> raw_position

risk_state
  -> risk_caps / confirmation requirements / confidence limits

execution_portfolio_constraints
  -> turnover, rebalance band, portfolio concentration, advisory action
  -> final_position
```

第一阶段建议只做结构化输出和审计，不改变生产评分：

1. 为每个评分模块和信号配置 `semantic_role`：`alpha`、`risk_state`、`valuation_alpha`、`valuation_risk`、`execution_constraint`、`report_only`。
2. 为每个模块输出统一字段：`normalized_score`、`confidence`、`coverage`、`horizon`、`source_type`、`primary_channel`。
3. 日报、decision snapshot 和 trace 中同时保存 `base_signal_score` 与 `risk_adjusted_score_context`。
4. 对同一 `evidence_id`、`risk_id`、`dedup_group` 或 `source_id` 检查是否同时进入 alpha 和 gate；如果允许，报告必须显式解释双通道原因。

验收标准：

- 报告能区分“看好但受限”和“不看好”。
- 同一事件不会在评分和 gate 中静默重复扣分。
- `confidence` 只能降低方向信号权重或结论使用等级，不能把低置信风险解释为低风险。
- 第一阶段不改变 production 仓位；任何改变权重或 gate 的候选规则必须进入 replay/shadow 和 rule governance。

## SCORE-005：软硬闸门、非线性仓位映射与执行平滑

价值判断：值得做，P1。当前 `position_gate` 采用最严格上限，对 P0 风险正确，但所有限制都用 `min(cap)` 会让系统偏保守，并可能让仓位边界跳变。

建议新增 gate taxonomy：

|类型|用途|允许影响|
|---|---|---|
|`hard_block`|数据质量失败、thesis invalidated、重大风险事件、组合硬超限|强制上限、禁止加仓、人工复核|
|`hard_cap`|高置信 L2/L3、极端估值拥挤、明确组合集中|降低最大仓位|
|`soft_cap`|估值偏贵、波动率升高、宏观偏紧、短期情绪拥挤|降低目标仓位或加仓速度|
|`confirmation_required`|低置信但潜在重要风险、事件前窗口、数据覆盖不足|等待确认、扩大观察区间|
|`report_only`|解释性风险、未确认 LLM 线索、弱来源证据|不改变评分或仓位|

分步开发：

1. 扩展 gate 输出 schema，先记录 `gate_strength`、`gate_class`、`target_effect` 和 `execution_effect`，不改变现有最终 cap。
2. 增加非线性仓位映射候选，例如分段 S 型或配置化 score-to-position curve，并作为 challenger shadow。
3. 将 `execution_policy` 的 cooldown、最小调仓、连续确认、turnover limit 和 rebalance band 纳入回测比较。
4. 经 `BACKTEST-001`、`SHADOW-003` 和 `GOV-003` 通过后，才允许 soft gate 或非线性映射进入 production。

验收标准：

- P0 硬约束仍 fail closed，不被 soft gate 或 confidence overlay 放松。
- soft gate 不直接伪装成风险事件或 thesis 证伪。
- 非线性映射和执行平滑能在回测中降低无意义换手，且不扩大 tail drawdown。
- 报告能说明最终仓位变化来自 hard cap、soft cap、执行平滑还是基础分变化。

## RISK-009：风险事件生命周期、衰减和去重

价值判断：值得做，P1。当前风险事件流程已经强调人工复核和 LLM 隔离，但事件长期有效性、时效衰减和多条新闻指向同一风险的归并仍需要加强。

建议事件状态：

```text
none
extracted
pending_review
confirmed_low
confirmed_medium
confirmed_high
confirmed_thesis_break
resolved
expired
rejected
```

建议字段：

```text
dedup_group
primary_channel
used_in_alpha
used_in_gate
event_created_at
last_confirmed_at
severity
confidence
decay_half_life_days
expiry_time
review_owner
next_review_due
resolution_reason
```

分步开发：

1. 扩展 risk event occurrence 或派生状态报告，支持 `dedup_group`、生命周期字段和过期规则。
2. 对不同事件类型配置默认有效期：地缘/政策较长，单日新闻情绪较短，财报到下一次披露，监管政策到落地或反转。
3. 在日报和回测中按 signal_date 只读取当时未过期、已复核、来源合格的事件状态。
4. 对重复新闻、同源候选和 LLM 预审重复项输出人工复核问题，而不是重复生成风险扣分。

验收标准：

- 旧风险不会无限期压制仓位；过期或 resolved 必须有审计原因。
- 短期噪音不会直接触发高等级 gate。
- 同一风险的多个新闻来源能被归并或至少在报告中标记 `dedup_group`。
- 回测按 point-in-time 事件生命周期切片，不读取未来 resolved/expired 信息。

## REPORT-007：基础信号与风险调整后结论拆分

价值判断：值得做，P1。当前日报已输出评分、置信度、模型仓位、最终仓位和 gate 摘要，但用户仍可能看不清“基础结论变弱”与“基础结论仍强但风险约束压低仓位”的差异。

建议日报新增固定结构：

```text
Base Signal
  score
  direction / posture
  confidence
  supporting modules
  pressure modules

Raw Position
  score_mapped_position
  confidence_adjusted_position

Risk Caps
  event_risk_cap
  macro_budget_cap
  valuation_cap
  thesis_cap
  data_quality_cap
  portfolio_cap

Final Position
  final_risk_asset_ai_position
  total_asset_ai_position
  execution_action
```

验收标准：

- 报告能一眼看出是 `base_signal_weak`、`risk_limited_bullish`、`data_limited` 还是 `manual_review_required`。
- 每个 cap 都有来源、触发状态、上限和证据引用。
- 该结构复用现有 `REPORT-001/002/003`、`SCORE-001/002/003` 和 `execution_policy`，不新增独立评分逻辑。

## 与现有任务的关系

- `SCORE-001` 已完成硬闸门基础；`SCORE-004/005` 不能削弱现有最严格上限规则，只能在验证后分层表达。
- `SCORE-002` 已完成评分和置信度拆分；后续要把 confidence 使用边界固定为“降低信号权重或结论等级，不释放未确认风险”。
- `SCORE-003` 已完成风险预算基础；`SCORE-005` 继续承接更细的软硬约束和执行平滑。
- `RISK-003/004/005/007/008` 已完成风险事件与 LLM 预审的保守边界；`RISK-009` 只补生命周期、衰减和去重。
- `CALIBRATION-001` 已承接 regime-aware 权重 overlay；本计划不新增平行权重系统。
- `BACKTEST-001` 已覆盖多数消融和稳健性实验；后续重点是用这些实验验证 `SCORE-004/005/RISK-009` 的候选改动。

## 状态记录

- 2026-05-06：新增本计划。原因：owner 提供了对当前评分系统的外部复核建议；评估后确认大方向合理，现有任务已覆盖大部分基础能力，但仍需登记 alpha/risk 解耦、防双重计分、事件生命周期、软硬闸门、非线性仓位映射和报告拆分的后续计划。本次仅登记需求，不改变生产评分、仓位闸门、回测或日报输出。
- 2026-05-06：第一阶段达到 `BASELINE_DONE`。原因：已完成审计型
  `score_architecture` 输出、日报 `Base Signal / Risk Caps` 固定区块、trace claim、
  decision snapshot 字段、position gate taxonomy 字段，以及风险事件 lifecycle/dedup/expiry
  schema 与过期事件不进入自动评分/仓位闸门的基础防护；验证覆盖日报文本、trace/snapshot、
  风险事件过期和任务边界。本阶段仍不引入 soft gate 生产行为、非线性仓位映射、replay
  candidate 或 owner promotion。
