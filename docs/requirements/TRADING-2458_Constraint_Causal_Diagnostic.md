# TRADING-2458：冻结 Constraint 证据的窄版因果诊断

最后更新：2026-07-25

状态：`IN_PROGRESS`

稳定任务 ID：`TRADING-2458_CONSTRAINT_CAUSAL_DIAGNOSTIC`

Owner 决策：
`owner_decision:TRADING-2458:2026-07-25:approve_narrow_constraint_causal_diagnostic_v1`

## 背景与授权上下文

TRADING-2452 的有效 historical-seen run
`trading2452-historical-seen_20260721T053621Z_144f31edee91` 在
`2021-02-22..2025-12-31` 六个完整半年 fold 上生成 1,800 个 `COMPLETE` train evaluations，
但全部被现行 constraint gate 拒绝。TRADING-2453 已机械证明原 artifact 的 hit count、rate、
delta、gate 与 reason 重算完全一致，没有计算实现缺陷；同时识别出
`max_constraint_hit_rate=0.65` 从 small-real / observe-only rationale 被消费成 fold train hard
eligibility 的 policy-role mismatch。

Owner 随后选择 Strategy A，关闭原 TRADING-2452 package。2026-07-25，Owner 又明确批准
Strategy C 的窄版诊断。本任务因此是一个新的只读研究诊断，不重开、改写或重跑 TRADING-2452，
也不是 Strategy B 的新 hard-gate policy。

## 要回答的问题

1. 四个既有 template 与七个冻结 candidate axis 中，哪些变化与 constraint-hit rate、delta 及
   gate reason 的变化存在可重复、跨 fold 一致的关联？
2. 现有 300-candidate grid 是否提供足够的 matched contrast 来识别单轴效应；若没有，具体缺少
   哪些配对、层级或变化，不能把相关性写成因果结论。
3. 全量拒绝主要表现为 template-level common mode、某个 axis 的稳定梯度、fold-specific regime
   暴露，还是现有 candidate generator 对该 gate 缺乏辨识度？
4. 后续最可证伪的动作应是关闭当前 candidate family/generator、另建新 hypothesis/generator，
   还是另行 author 一份 role-correct hard-gate policy；本任务本身不执行这些动作。

## 冻结输入

- TRADING-2452 package、有效 run、policy 与 `train_evaluations.jsonl` 的 exact bytes/hash；
- TRADING-2453 diagnosis 与其 content-derived validation；
- candidate universe 中既有七轴：
  `rescue_intensity`、`smooth_window_days`、`constraint_buffer_bps`、
  `turnover_penalty`、`risk_off_confirmation_days`、`rebalance_cooldown_days`、
  `drawdown_guard`；
- active primary research window 固定从 `2021-02-22` 开始；
- `2026-07-22` 起的 `PROSPECTIVE_UNTOUCHED` 数据保持零访问。

不得用 live config、当前 provider/cache 或后来生成的市场结果替换冻结输入。若任一 source/hash、
row count、candidate identity、fold 或 TRADING-2453 重算事实漂移，诊断必须 fail closed。

## 方法与解释边界

“因果诊断”在本任务中只允许表示对冻结设计矩阵做受控 contrast：

- 先在同 fold、同 template 且除目标 axis 外其余 axis 完全相同的候选间构造 matched pair；
- 只有 matched coverage、方向一致性和最小样本条件满足 reviewed policy 时，才可标记为
  `IDENTIFIABLE_ASSOCIATION`；不得写成已证明的投资因果；
- 无 exact pair、多个 axis 同时变化、effect direction 跨 fold 翻转或结果被 common-mode
  saturation 截断时，必须标记 `NOT_IDENTIFIABLE` 并披露原因；
- template/fold/axis aggregation 必须从冻结逐行事实重算，null 保持 null，不能转 0、平滑或补值；
- classification threshold、coverage floor 与 direction rule 必须进入 reviewed diagnostic policy，
  不能以未解释 numeric literal 留在代码或报告中。

## 分阶段计划

|阶段|内容|输出与退出条件|
|---|---|---|
|S0|冻结授权、输入、方法和安全边界|本需求、任务登记、source inventory 与 owner decision 固化；原 package 仍 closed|
|S1|实现 exact replay 与 matched-contrast engine|逐行重算沿用 TRADING-2453 合同；生成 template/fold/axis coverage、pair count、effect distribution 与 identifiability reason|
|S2|形成诊断 artifact 与独立 validator|机器可读 JSON + 中文 Markdown；validator 从冻结 source 重建所有聚合、classification、hash 和 safety 字段|
|S3|Owner decision pack|只给出 `RETIRE_CURRENT_FAMILY`、`AUTHOR_NEW_HYPOTHESIS_GENERATOR`、`AUTHOR_ROLE_CORRECT_GATE_POLICY` 或 `INSUFFICIENT_IDENTIFIABILITY` 的证据化建议；不自动执行|
|S4|共享集成与正式验证|task/docs/system-flow/manifests/compatibility 同步；focused、architecture、contract、reproducibility 与最终 Full 按风险通过|

## 预期实现边界

策略专属实现预计落在：

- `config/research/trading2458_constraint_causal_diagnostic.yaml`；
- `src/ai_trading_system/trading2458_constraint_causal_diagnostic.py`；
- `tests/test_trading2458_constraint_causal_diagnostic.py`；
- test fixtures 只使用 tracked frozen bytes 或由 exact source 机械裁剪、hash 绑定的最小样本。

共享 task register、system flow、architecture manifests 与 compatibility 由 coordinator 单写。

## 验收标准

- 1,800/1,800 冻结 evaluation 的 identity、gate facts 与 TRADING-2453 exact 一致；
- 每个 template/fold/axis 的 candidate coverage、matched pair、unmatched reason 和 effect summary
  可从输入重建且 double-build byte-identical；
- 置换 row order 不改变输出；source、policy、candidate axis、pair、aggregate、classification 或
  safety 字段 tamper 均被 validator 拒绝；
- 报告明确区分 common-mode rejection、可识别关联和不可识别；不把观察性 contrast 声称为
  已证明因果或预期收益；
- 不修改 constraint threshold、candidate universe、selection score、position、权重、promotion gate
  或当前 package 结论；
- 不执行 candidate expansion/search、backtest rerun、prospective access、paper-shadow、production
  或 broker/order。

全过程固定：
`research_only=true`、`manual_review_required=true`、
`current_package_reopened=false`、`strategy_gate_changed=false`、
`prospective_accessed=false`、`production_effect=none`、`broker_action=none`。

## 进展记录

- 2026-07-25：Owner 明确“策略线按照 C 继续诊断”。S0 启动；授权仅覆盖冻结证据的窄版
  per-template/per-axis diagnosis，不覆盖 B、新候选生成器、阈值修改、原 package 重跑、
  prospective、paper-shadow、promotion、production 或 broker。
