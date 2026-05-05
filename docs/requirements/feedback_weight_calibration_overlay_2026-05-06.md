# 回测权重校准 overlay 基础设施计划

状态：BASELINE_DONE

最后更新：2026-05-06

关联任务：`CALIBRATION-001`、`FEEDBACK-002`、`EXPERIMENT-001`、`SHADOW-001`、`SHADOW-002`、`SHADOW-003`、`BACKTEST-003`、`GOV-003`、`REPORT-005`

## 背景

本计划来自 2026-05-06 关于“反馈闭环 x 回测权重校准机制”的评估和 owner 确认。

目标不是让每日回测自动改 production 权重，而是在现有反馈闭环、prediction ledger、forward shadow 和 rule governance 之上增加一层可审计、可过期、可回滚的 calibration overlay：

```text
daily decision / backtest outcome
  -> weight diagnostics
  -> weight update candidate
  -> policy replay
  -> forward shadow
  -> owner approval
  -> approved calibration overlay
  -> production decision computes effective_weights
```

当前仓库已经具备 `decision_snapshot`、`decision_outcomes.csv`、`decision_causal_chains.json`、`decision_learning_queue.json`、`rule_experiments.json`、`prediction_ledger.csv`、`feedback run-shadow`、`shadow-maturity` 和 rule card promotion 基础版。因此本计划必须复用现有治理链路，不能再平行建设一套不可追溯的规则晋级系统。

## 设计原则

- 回测和校准报告只能生成 diagnostics、candidate、replay spec 或 shadow 记录，不能直接改 `config/scoring_rules.yaml`、`position_gate`、thesis、日报结论或正式仓位区间。
- production 只读取 `approved_soft` 或 `approved_hard` overlay；candidate、shadow-only 或 expired overlay 不得影响正式输出。
- 第一阶段只实现 `approved_soft` 能力：`effective_weights`、`confidence_delta`、`position_multiplier`、`required_confirmation` 和解释。`approved_soft` 不得 hard block，不得直接提高到 full position。
- `approved_hard` 只作为 schema 预留。后续如需影响 hard gate 或 position cap，必须有更高样本门槛、forward shadow 证据和 owner explicit approval。
- 所有影响 production 的 overlay 必须有 approval、valid_from、expires_at 或 no_expiry_reason、rollback_condition 和 replay/shadow evidence ref。
- 信心调整采用当前项目的 0-100 score point 语义，不使用 0-1 小数语义，避免和 `DailyConfidenceAssessment.score` 混淆。
- 权重模块名必须对齐当前 production scoring modules：`trend`、`fundamentals`、`macro_liquidity`、`risk_sentiment`、`valuation`、`policy_geopolitics`。
- 后验 outcome 只能成为未来 prior，不得改写 signal_date 当时 evidence、context 或 causal chain。

## 阶段拆解

|阶段|目标|验收|
|---|---|---|
|1|Schema 与纯逻辑基础设施|BASELINE_DONE：新增 `config/weights/weight_profile_current.yaml`、weight profile loader、approved overlay loader、context matching、effective weight 计算和 `aits feedback apply-calibration-overlay`；未接入 `score-daily` production 行为|
|2|Production report 审计区块|`score-daily` 读取 profile/overlay，报告新增 `Historical Calibration`；无命中时明确 `No approved overlays matched`；第一版可只审计，不改变评分|
|3|Soft overlay 接入评分|`approved_soft` 可改变 effective module weights、confidence adjustment 和 required confirmation；不得改变 hard gate；decision snapshot 和 trace 记录 applied overlay|
|4|Weight diagnostics 与候选|`feedback calibrate` 或独立命令输出 `information_weight_diagnostics`、`context_signal_reliability` 和 `weight_update_candidates`；样本不足输出 `insufficient_sample`|
|5|Policy replay|对单个 candidate 重放 baseline vs challenger，输出 changed decisions、avoided losers、missed winners、net excess return delta、drawdown 和 turnover|
|6|Promotion workflow|复用现有 shadow/prediction/rule governance，把通过 replay/shadow/owner approval 的 candidate 晋升为 approved overlay，并保留 rollback condition|
|7|长期验证|按 1D/5D/20D/60D outcome 累积真实 PIT 样本；周/月报展示 production vs challenger 和 overlay maturity|

## 第一阶段范围

本次基础设施只做阶段 1：

- 新增当前基础权重配置。
- 新增 calibration overlay 数据模型和 loader。
- 支持 approved overlay 过滤、过期过滤、上下文匹配、权重倍率、置信度 delta、position multiplier 和 required confirmation 聚合。
- 新增 CLI 入口用于给定 context 计算 `effective_weights`。
- 新增单元测试覆盖基础不变量。

本阶段明确不做：

- 不接入 `score-daily` 正式评分。
- 不生成 weight candidate。
- 不执行 policy replay。
- 不做 owner promotion 写入。
- 不改变 `position_gate`、日报结论、prediction ledger 或回测策略结果。

## 后续运行计划

基础设施完成后，每日运行纪律应为：

1. 先运行数据质量门禁和 production 日报。
2. 记录 decision snapshot、prediction ledger 和候选机会。
3. 对成熟 outcome 运行 calibration 和 prediction outcome 报告。
4. 对候选规则或权重调整运行 forward shadow，保持 `production_effect=none`。
5. 每周检查 learning queue、shadow maturity 和样本不足原因。
6. 每月或样本足够后运行 policy replay。
7. 只有 replay、forward shadow 和 owner approval 均满足时，才写入 approved overlay。

## 开放问题

- `decision_candidates.csv` 的候选机会粒度需要后续确认：按 aggregate AI portfolio、按 ticker，还是按 ticker + signal family。
- diagnostics 的因果识别方法需要谨慎设计，不能仅按失败样本降低权重；优先考虑 ablation replay 和 counterfactual replay。
- 多个 overlay 冲突时的优先级、叠加上限和解释排序需要在阶段 3 前固定。
- `approved_hard` 是否需要单独 rule card 类型，待 soft overlay 稳定后再决定。

## 验收标准

- 非法 base weight sum 会失败。
- 未批准、过期或 status 不合格的 overlay 不会参与 production-effective 计算。
- approved overlay 缺 approval、rollback condition 或 expiry/no-expiry reason 会失败。
- `approved_soft` 不能包含 hard block / hard position cap。
- context match 使用全部字段匹配；list 字段用交集逻辑；boolean 字段必须相等；缺失字段默认不匹配。
- 无 overlay 命中时 effective weights 等于 base weights。
- 命中 overlay 时输出 matched overlays、base weights、effective weights、confidence_delta、position_multiplier、required_confirmations 和 why applied / why not applied。

## 状态记录

- 2026-05-06：新增并进入实现。原因：owner 同意先搭建基础系统，再通过每日数据收集、回测和 forward shadow 等待样本成熟；第一阶段只实现不改变 production 的 schema、loader、matching 和 CLI 骨架。
- 2026-05-06：从 IN_PROGRESS 改为 BASELINE_DONE。原因：已完成第一阶段权重 profile、approved overlay schema/loader、context matching、effective weights 计算、standalone CLI、系统流图和单元测试；`score-daily`、回测仓位、`position_gate` 和 prediction ledger 尚未接入，后续仍按阶段 2-7 推进。
