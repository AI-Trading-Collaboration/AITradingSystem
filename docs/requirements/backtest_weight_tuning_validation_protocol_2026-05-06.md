# 回测调权防过拟合验证协议

状态：BASELINE_DONE

最后更新：2026-05-06

关联任务：`CALIBRATION-002`、`BACKTEST-004`、`CALIBRATION-001`、`BACKTEST-001`、`BACKTEST-003`、`SHADOW-003`、`GOV-003`、`SCORE-005`、`RISK-009`

## 背景

本计划来自 2026-05-06 关于“回测参数调整和权重优化注意事项”的复核。结论是：这些建议需要进入后续计划，但不应直接合并进当前已完成基础版的 `CALIBRATION-001`。`CALIBRATION-001` 解决的是 approved overlay 的可审计基础设施；本计划解决的是权重候选在生成、筛选和晋级前如何避免 data snooping、selection bias 和回测过拟合。

现有系统已经具备以下基础：

- `BACKTEST-001`：成本压力、起点后移、固定仓位、再平衡频率、趋势基线、模块权重扰动、同换手率随机策略和时间顺序样本外验证。
- `BACKTEST-003`：模型晋级门槛，把回测稳健性、PIT 覆盖、shadow outcome 和 rule governance 串起来。
- `SHADOW-003` / `GOV-003`：前向 shadow 样本成熟度和受控 promotion/retirement 基础版。
- `CALIBRATION-001`：approved calibration overlay schema、context matching 和 effective weights 计算基础版。

缺口是：如果后续开始通过回测调权，仍需要把研究协议、调参试验次数、nested walk-forward、purging/embargo、PBO/DSR、稳定区域、先验收缩和 gate/news/LLM 的独立归因固定下来。否则即使 production overlay 有审批，也可能把历史噪音包装成“已验证权重”。

## 外部建议评估

|建议|判断|承接方式|
|---|---|---|
|先锁定研究协议再调权|需要新增|`CALIBRATION-002` 固定 protocol manifest、数据版本、成本、执行、切分和验收指标|
|参数分层，不要全局一起调|需要新增|`CALIBRATION-002` 规定先信号权重、再 gate、最后执行参数的顺序|
|nested walk-forward、purging 和 embargo|需要新增|`CALIBRATION-002` 扩展当前普通 OOS holdout|
|不要只优化 Sharpe 或总收益|需要新增|`CALIBRATION-002` 定义多目标 objective 和硬性验收门槛|
|记录 trial 次数|需要新增|`CALIBRATION-002` 要求 experiment registry 和 number_of_trials|
|选稳定区域，不选最优点|需要新增|`CALIBRATION-002` 输出 neighborhood stability 和 weight_stability_score|
|先验权重加收缩|需要新增|`CALIBRATION-002` 将 fitted weights 作为候选，不直接替代 prior|
|消融测试|部分已覆盖|`BACKTEST-001` 已有趋势基线、权重扰动和随机基线；`CALIBRATION-002` 补 no-factor/no-gate 的调权后验收|
|单独评估 news / LLM|需要新增|`BACKTEST-004` 评估事件抽取、严重度、人工复核、gate 触发后的收益和损失|
|regime-aware 但不要太细|部分已覆盖|`CALIBRATION-001` 支持 context matching；`CALIBRATION-002` 限制 regime adjustment 复杂度|
|gate 价值用避免损失衡量|需要新增|`BACKTEST-004` 输出 avoided_drawdown、missed_upside、false_alarm 和 late_trigger|
|成本和执行假设从第一天纳入|已有基础，需锁定|`COST-001`、`EXEC-001`、`BACKTEST-001` 已有基础；`CALIBRATION-002` 要求调权协议锁定这些假设|
|benchmark 不只和空仓比|部分已覆盖|`BACKTEST-001` 已覆盖多类基线；`CALIBRATION-002` 固定候选权重必须比较的 baseline set|
|上线门槛|已有基础，需增强|`BACKTEST-003` / `GOV-003` 承接 promotion；`CALIBRATION-002` 补 DSR/PBO/稳定性门槛|
|固定权重更新节奏|需要新增|`CALIBRATION-002` 要求月度或季度评估、权重变化上限和新旧权重平滑|

## CALIBRATION-002：调权防过拟合验证协议

价值判断：P1。该任务应在任何自动或半自动调权进入 `approved_soft` overlay 前完成。原因不是当前权重一定错误，而是调权过程会天然增加多重测试和 selection bias；如果没有审计协议，最终选择的权重很难区分真实稳健性和历史拟合。

### Protocol manifest

每次调权前必须冻结并记录：

```text
protocol_id
experiment_id
git_commit
feature_version
prompt_version
model_version
data_snapshot_hash
yaml_hash
cost_model_version
execution_assumption_version
market_regime
date_range
label_horizon
train_validation_test_scheme
purge_days
embargo_days
objective_version
benchmark_set
parameter_family_scope
parameter_search_space_hash
number_of_trials
approval_owner
```

没有 manifest 的调权结果只能作为临时探索，不得进入 shadow、promotion 或 approved overlay。

### 参数分层

调权顺序必须分层，避免把无效 alpha 伪装成 gate 或执行优势：

1. 固定 gate 和执行规则，只评估基础信号权重。
2. 固定信号权重，单独验证 gate 是否降低回撤和尾部损失。
3. 固定信号和 gate，再评估非线性仓位映射、rebalance band、turnover limit 和执行延迟。
4. 每一层通过后才能进入下一层；失败样本不得直接倒推修改上一层定义。

### Walk-forward 与泄漏防护

基础要求：

- 使用 nested walk-forward：内层只在训练区间调参，外层 test window 只看一次。
- 如 label 使用未来 5D、20D、60D 或 120D 收益/回撤，训练和测试之间必须加入 purging / embargo，避免标签窗口重叠。
- 每个 test window 的结果独立保存，最终看聚合 OOS 表现，而不是挑选单个漂亮区间。
- 对 `ai_after_chatgpt` regime 的主结论必须声明实际起止日期；如使用 2022-12-01 前历史，只能作为 warm-up、压力测试或 regime 对照。

### 目标函数和验收门槛

调权目标不得只最大化收益或 Sharpe。建议 objective 包含：

```text
median_oos_ir
- max_drawdown_penalty
- turnover_penalty
- tail_loss_penalty
- weight_instability_penalty
- complexity_penalty
```

硬性验收至少包括：

- 净表现优于 `buy_and_hold`、`manual_weight_signal`、`equal_weight_signal`、`trend_only_model`、`risk_parity_baseline`、`current_production_model` 和 `no_gate_model` 中的相关基准。
- 交易成本、延迟和换手压力测试后仍有效。
- 最差 regime、最差年份、最差月份和 crash window 不崩。
- 去掉任一主因子或主 gate 后系统不出现不可解释失控。
- 参数邻域扰动后表现不大幅恶化。
- DSR/PBO 或同等多重测试折扣后仍具备晋级价值。

### 稳定区域与先验收缩

候选权重不能直接采用回测最优点。每次调权必须输出：

```text
best_config_performance
neighborhood_average_performance
neighborhood_worst_performance
performance_sensitivity
weight_stability_score
prior_weights
fitted_weights
shrinkage_lambda
final_candidate_weights
```

默认使用：

```text
final_weight = (1 - lambda) * prior_weight + lambda * fitted_weight
```

`lambda` 只能由 OOS 强度、稳定性和样本质量提高，不能由 in-sample 最优收益提高。

### 更新节奏

- 权重候选最多按月或季度进入评估，不得每日被最新回测结果牵引。
- 单次 production 权重变化需要上限。
- 新旧权重应平滑过渡，例如先作为 `approved_soft` shadow 或低影响 overlay。
- 任何权重晋级必须继续走 `BACKTEST-003`、`SHADOW-003` 和 `GOV-003`。

## BACKTEST-004：Gate 与事件效果归因

价值判断：P1。风险 gate 和 news/LLM event 模块的价值不应只用最终收益衡量。一个有效 gate 可能牺牲部分上涨，但显著降低左尾损失；如果只看 CAGR 或 Sharpe，容易把真正有用的尾部保护误删，也可能把降低仓位误判为风险管理能力。

### Gate 归因指标

每个 gate 或 gate family 至少输出：

```text
gate_trigger_count
average_position_reduction
avoided_drawdown
missed_upside
net_effect
false_alarm_rate
late_trigger_rate
crash_period_return_delta
left_tail_loss_delta
time_under_water_delta
turnover_delta
```

`avoided_drawdown` 和 `missed_upside` 必须使用同一 execution assumption、成本模型、PIT 输入和 benchmark，不能混用不同假设。

### Event / LLM 评估指标

消息面和 LLM 模块应单独评估：

```text
event_precision
event_recall_if_label_available
severity_accuracy
manual_review_pass_rate
pending_to_confirmed_rate
confirmed_high_after_1d_max_adverse_move
confirmed_high_after_5d_max_adverse_move
confirmed_high_after_20d_max_adverse_move
false_positive
false_negative
review_latency
```

LLM 的 production 边界保持不变：

- 只做当时可见文本的信息抽取、事件归类、风险标签和人工复核候选。
- 不直接预测未来收益。
- 不直接决定仓位。
- 历史回测中的 LLM 输入必须保存原始文本、prompt、model_version、输出 JSON 和可见时间，避免使用后来历史结论污染过去事件判断。

## 参考文献

- Halbert White, 2000, [A Reality Check for Data Snooping](https://www.ssc.wisc.edu/~bhansen/718/White2000.pdf)。
- Campbell R. Harvey, Yan Liu, Heqing Zhu, 2014/2016, [... and the Cross-Section of Expected Returns](https://www.nber.org/papers/w20592)。
- David H. Bailey, Jonathan Borwein, Marcos Lopez de Prado, Qiji Jim Zhu, 2015, [The Probability of Backtest Overfitting](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253)。
- David H. Bailey, Marcos Lopez de Prado, 2014, [The Deflated Sharpe Ratio](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551)。

## 状态记录

- 2026-05-06：新增本计划。原因：外部复核指出，后续回测调权最主要的系统性风险不是初始权重不准，而是把重复试验后的历史噪音误判为稳健权重；现有 `BACKTEST-001` 和 `CALIBRATION-001` 需要一个专门的防过拟合验证协议连接起来。
- 2026-05-06：进入基础实现。范围限定为只读 protocol manifest 校验和 gate/event attribution 审计报告；不改变 production scoring、position gate、回测仓位、approved overlay 或 rule promotion。
- 2026-05-06：基础版完成。新增 `aits feedback validate-calibration-protocol`、`aits backtest-gate-attribution`、protocol 模板、系统流图、README 和测试；真实 2026-04-01 至 2026-05-05 回测产物的 gate/event attribution 报告为 `PASS_WITH_LIMITATIONS`，限制来自当前没有事件标签。完整 `DONE` 仍需真实 trial registry、DSR/PBO 结果、稳定区域、事件 severity/outcome 标签和 forward shadow 样本。
