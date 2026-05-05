# PIT 回测与前向 shadow 评估计划

状态：BASELINE_DONE

最后更新：2026-05-05

关联任务：`PIT-001`、`SHADOW-001`、`BACKTEST-003`、`DATA-003`、`BACKTEST-002`、`BTINPUT-001`、`FEEDBACK-002`、`EXPERIMENT-001`、`GOV-001`

## 背景

项目已经完成 forward-only PIT 快照归档、回测 A/B/C 数据可信度、lag
sensitivity 和 PIT 覆盖持续验证。后续重点不再是补造历史 PIT，而是把
`available_time <= decision_time` 扩展成全系统规则，并让每日 production
判断与 challenger 模型都留下真实前向 prediction log。

本计划沿用现有 A/B/C 回测可信度体系，不引入 Bronze/Silver/Gold 等第二套评级。
历史回测仍用于研究线索和压力测试；模型晋级必须依赖 A/B 级数据、稳健性实验和真实
前向 shadow/outcome 样本。

## 设计原则

- 训练探索可以读取 C 级历史数据，但不得把 C 级结果作为模型晋级或投资结论证据。
- 进入模型选择、规则定型、晋级门槛或报告主结论的样本，必须通过 PIT 可见时间审计。
- 所有特征读取都必须能解释 `event_time`、`available_time` 和 `decision_time` 的关系。
- challenger/shadow 输出必须 append-only，且 `production_effect=none`，不得影响正式评分、仓位闸门或日报动作。
- 后验 outcome 只能追加到 prediction / decision 记录之后，不得改写当时的输入、特征、因果链或质量状态。

## PIT-001：全系统 feature availability policy

价值判断：P0。当前系统已经在估值、风险事件、SEC 基本面和观察池 lifecycle 上使用
PIT 切片，但后续如果增加更多特征族，必须有统一的可见时间目录和 fail-closed 规则，
否则容易在新模块中重新引入未来函数。

### 阶段拆解

|阶段|目标|验收|
|---|---|---|
|1|建立 feature availability catalog|BASELINE_DONE：`config/feature_availability.yaml` 已列出市场价格、宏观、观察池、SEC/TSM 基本面、估值、风险事件和市场证据的 `event_time`、`source_published_at`、`available_time` 与保守延迟|
|2|接入特征构建和回测输入审计|BASELINE_DONE：`build-features`、`score-daily`、`backtest` 已输出 PIT 特征可见时间报告，并把摘要写入 trace bundle|
|3|增加未来函数防护测试|BASELINE_DONE：已覆盖 catalog 缺失 source fail-closed、报告接入和 trace 引用；剩余数据源级未来公告/universe/宏观修订样本继续随新源接入补强|

### 验收标准

- 所有用于评分、回测、校准和日报核心结论的特征族都有明确 availability rule。
- 缺少 `available_time` 或只有 `event_time` 的新特征默认不得进入 A/B 级回测主结论。
- 回测报告和 trace bundle 能显示特征可见时间规则、实际 `feature_as_of` 和数据可信度等级。
- 实现时同步更新 `docs/system_flow.md` 并补充测试。

## SHADOW-001：在线 PIT prediction / shadow ledger

价值判断：P1。`decision_snapshot` 记录当前 production 判断，但模型迭代需要同时记录
production 和多个 challenger 在同一 `decision_time` 看到的数据、给出的信号和后续标签。
这会把未来几个月的真实在线日志变成最可信的 PIT 评估集。

### 最小字段

```text
prediction_id
run_id
model_version
rule_version_manifest
candidate_id
production_effect
feature_snapshot_id
data_snapshot_id
trace_bundle_ref
symbol / instrument_id
decision_time
market_regime
signal
score / probability
model_target_position
gated_target_position
execution_assumption
label_horizon
label_available_time
realized_return
max_drawdown_after_signal
slippage
fee
outcome_status
```

### 阶段拆解

|阶段|目标|验收|
|---|---|---|
|1|定义 append-only ledger schema 和写入策略|BASELINE_DONE：新增 `prediction_ledger.csv` schema，重复 `prediction_id` 停止，signal-time 字段与 outcome 字段分离|
|2|把 `score-daily` production 输出写入 prediction ledger|BASELINE_DONE：production 行引用 trace bundle、features、data quality、rule version 和 market regime|
|3|把 `EXPERIMENT-001` challenger/shadow 计划接入 ledger|待后续：当前 schema 和写入函数支持 `candidate_id` / `production_effect=none`，但自动 challenger 运行尚未接入|
|4|把 `feedback calibrate` 扩展为 prediction-level outcome|BASELINE_DONE：新增 `aits feedback calibrate-predictions`，按 candidate/model version/horizon 输出 outcome CSV 和 Markdown 报告|

### 验收标准

- 每个 production/challenger prediction 都能追溯到当时可见的数据快照、特征版本、规则版本和质量状态。
- outcome 追加后不能改写 signal-time 字段。
- 周报/月报能区分 production 真实判断和 challenger shadow 表现。
- 实现时同步更新 `docs/system_flow.md` 并补充 append-only、future outcome isolation 和 challenger isolation 测试。

## BACKTEST-003：模型晋级门槛与回测路线

价值判断：P1。`BACKTEST-001/002` 已解决稳健性和数据可信度标注，但还缺一个把回测、
PIT 覆盖、前向 shadow 和规则治理串起来的晋级门槛。没有晋级门槛，漂亮回测仍可能被误读为可上线证据。

### 晋级门槛

|层级|作用|允许结论|
|---|---|---|
|C 级历史探索|发现候选因子或规则|只能生成研究线索，不得晋级|
|B 级近似 PIT 回测|验证未来函数风险和滞后敏感性|可进入 challenger/shadow|
|A/B 级稳健性组合|通过成本、起点、权重扰动、随机同换手率和样本外验证|可进入前向 shadow 观察|
|真实 shadow/outcome|使用在线 PIT prediction ledger 的后验标签|决定是否提交 `GOV-001` 规则晋级|
|小资金或人工复核阶段|验证滑点、交易纪律和实际可执行性|只在 owner 明确需要交易闭环时启用|

### 阶段拆解

|阶段|目标|验收|
|---|---|---|
|1|新增模型评估摘要或扩展回测报告|BASELINE_DONE：`aits backtest --promotion-report` 输出模型晋级门槛 Markdown/JSON，并可嵌入主回测报告|
|2|定义 promotion gate|BASELINE_DONE：C 级回测标记 `NOT_PROMOTABLE`，缺少 robustness/lag/shadow outcome 标记 `MISSING`，完整基础证据后进入 `READY_FOR_GOV_REVIEW`|
|3|接入 rule governance|BASELINE_DONE：promotion gate 引用 rule card registry 校验状态；owner approval 仍由 `GOV-001` 管理|
|4|周期复盘接入|待后续：周报/月报读取 production vs challenger outcome 仍需真实 shadow 样本积累和 `REPORT-004` 后续接入|

### 验收标准

- 报告默认声明 `ai_after_chatgpt` regime 和实际日期范围。
- C 级输入不得输出无条件 Sharpe/CAGR 主结论，也不得生成 promotion 通过状态。
- 晋级状态必须引用具体 evidence、PIT 覆盖、robustness、shadow outcome 和 rule card。
- 实现时同步更新 `docs/system_flow.md` 并补充报告文本和治理隔离测试。

## 后续回测系统路线图

1. 先把 PIT 可见时间目录补齐，确保新特征族不会绕过 `available_time <= decision_time`。
2. 再实现 prediction/shadow ledger，让 production 和 challenger 每天留下真实前向样本。
3. 然后把现有 robustness、lag sensitivity、PIT 覆盖和 shadow outcome 汇总成模型晋级报告。
4. 最后用 `GOV-001` 管理规则 promotion/retirement，形成“探索 -> 近似 PIT 回测 -> 前向 shadow -> 晋级审批 -> 生产规则”的闭环。

## 状态记录

- 2026-05-05：新增本计划。原因：owner 认可“强 PIT 评估 + 弱 PIT 训练 + 在线真实 PIT 积累”的方向，需要把相关后续改动登记到 task register，并明确这些改动落地后的回测系统路线。
- 2026-05-05：从 READY 改为 BASELINE_DONE。原因：已落地 feature availability catalog/report、prediction ledger/outcome 校准和 backtest promotion gate；`ruff check src tests` 与目标测试通过。剩余缺口是数据源级 `available_time` 深校验、challenger 自动接入、真实 forward shadow 样本和 owner/rule approval。
