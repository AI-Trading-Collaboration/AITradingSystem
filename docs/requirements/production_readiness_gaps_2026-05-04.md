# 日报趋势判断就绪缺口登记

状态：DONE

最后更新：2026-05-06

关联任务：`PROD-001`、`PROD-002`、`PROD-003`、`PROD-004`、`PROD-005`、`PROD-006`、`TREND-001`、`RISK-005`、`RISK-006`、`RISK-007`、`RISK-008`、`DATA-001`、`DATA-002`、`DATA-003`、`BTINPUT-001`、`BACKTEST-001`、`BACKTEST-002`、`CHAIN-001`、`PORTFOLIO-002`、`SCORE-003`、`COST-001`、`GOV-001`、`GOV-002`、`GOV-003`、`SHADOW-002`、`SHADOW-003`、`REPORT-005`

## 背景

2026-05-04 日报生成成功，但结论使用等级为 `data_limited`，执行动作是 `observe_only`。项目 owner 已明确当前系统不需要实际触发交易，日报现阶段只作为 AI 产业链趋势判断和投研复核辅助。

因此，本文件不再把“生产交易依据”作为近期目标，而是记录哪些缺口会阻止日报成为可靠趋势判断输入。真实交易执行、券商税费、订单拆分和账户级买卖闭环保留为后续可选扩展，不作为当前优先级阻塞项。

## 趋势判断缺口与处理顺序

|顺序|缺口|承接任务|当前判断|
|---:|---|---|---|
|1|报告需要固定“趋势判断、不触发交易”的产品边界，避免把仓位区间或执行动作误读为交易触发|`TREND-001`|已完成：结论使用等级显示趋势判断范围，且不把成功态写成可交易或账户仓位复核。|
|2|风险事件发生记录缺少“今日已复核且未发现未记录重大事件”的可审计输入，空目录不能证明没有政策/地缘风险|`RISK-005`、`RISK-006`、`RISK-007`、`RISK-008`、`PROD-002`|工程底座已完成基础版；真实复核 owner、来源清单和运行纪律拆到 `PROD-002`，作为趋势判断生产前置运营任务。|
|3|估值和盈利预期数据仍受 point-in-time 样本、历史覆盖和 `eps_revision_90d_pct` 覆盖限制|`DATA-001`、`DATA-003`、`BTINPUT-001`、`BACKTEST-002`、`PROD-004`|工程底座已完成历史输入缺口诊断：`aits backtest-input-gaps` 可列出 signal_date 级估值/风险事件覆盖；历史 PIT 采购和历史补数暂不推进，系统继续保留限制声明；`DATA-003` 从当前日期开始积累 forward-only 自建快照，`BACKTEST-002` 负责把历史回测标注为 A/B/C 数据可信度，样本成熟度拆到 `PROD-004`。|
|4|产业链节点需要把价格热度和基本面/估值/风险/thesis 健康度分开，防止把上涨误读成基本面确认|`CHAIN-001`|节点热度与健康度基础版已完成：日报显示健康覆盖、支持项、风险/限制和数据缺口；回测每日明细和报告也可追踪历史节点状态；完整缺口只剩是否进入仓位约束的后续评估。|
|5|市场价格和宏观利率 cross-provider reconciliation 不足，少于两个 qualified source 的领域不能视为生产级核对完成|`DATA-002`、`PROD-003`|已由数据源健康报告暴露；第二 qualified source 候选、授权/成本和缓存审计字段拆到 `PROD-003`。|
|6|回测、校准和前向验证还不足以支撑趋势判断长期信任，需要抗过拟合、参数敏感性、样本外、baseline 对比和真实 outcome 样本|`BACKTEST-001`、`SHADOW-002`、`SHADOW-003`、`REPORT-005`、`PROD-006`|`BACKTEST-001` 已完成稳健性报告；真实 forward shadow/outcome 样本成熟度拆到 `PROD-006`，防止把历史回测或未成熟 shadow 结果误读为规则晋级证据。|
|7|账户持仓、执行成本和交易审批链不属于当前趋势判断目标|`PORTFOLIO-002`、`SCORE-003`、`COST-001`、`EXEC-001`|保留已完成的只读解释和 advisory 输出，但不作为当前趋势判断就绪的阻塞项；未来若重新要求实际调仓，再提升优先级。|
|8|规则治理仍停留在 baseline rule card registry，缺少正式 promotion / retirement / owner approval 与规则版本注入|`GOV-001`、`GOV-002`、`GOV-003`、`PROD-005`|工程底座已完成规则版本注入：日报、回测、decision snapshot 和 evidence bundle 记录当前 production rule versions；正式 owner approval 和规则生命周期批准拆到 `PROD-005`。|

## 后续子任务

`PROD-001` 已完成总控收口：报告边界已改成“趋势判断，不触发交易”，且阻止日报成为可靠趋势判断输入的主要缺口已拆成独立任务。后续不再用 `PROD-001` 承接多模块开发，改由以下子任务分别推进：

|任务|主题|阻塞类型|验收重点|
|---|---|---|---|
|`PROD-002`|风险事件每日复核运行纪律|owner 输入|有效复核声明、来源范围、过期处理和日报降级边界可审计。|
|`PROD-003`|市场价格与宏观利率第二 qualified source|owner 输入|`market_prices` 与 `macro_rates` 至少各有两个 qualified source，并能暴露跨源冲突。|
|`PROD-004`|forward-only PIT 估值样本成熟度|时间窗口|自建 PIT 样本覆盖达到可提升估值/预期可信度的条件，历史不足继续降级。|
|`PROD-005`|规则治理 owner approval|owner 输入|production rule baseline、promotion 和 retirement 具备批准记录与回滚条件。|
|`PROD-006`|forward shadow 与 outcome 成熟度|时间窗口|真实 prediction outcome 样本达到可评估窗口前，不把 shadow 或回测结果写成规则晋级证据。|

## 状态记录

- 2026-05-04：根据 2026-05-04 日报结论复盘新增本生产就绪缺口索引；首个可独立推进项为 `RISK-005`。
- 2026-05-04：`RISK-005` 完成基础版工程链路，新增复核声明 schema、CLI、校验报告、日报识别、历史切片和系统流图；该项不代替真实人工复核。
- 2026-05-04：`PORTFOLIO-002` 完成基础版工程链路，新增真实持仓 CSV schema、`aits portfolio exposure`、组合暴露报告、日报只读章节和系统流图；缺少真实持仓时保持 `NOT_CONNECTED`，不把观察池或模型仓位当作账户持仓。
- 2026-05-04：`SCORE-003` 完成基础版工程链路，新增 `risk_budget` gate、配置 schema、日报/回测输出和执行纪律约束；该项仍需真实持仓、参数验证和规则治理后才能视为生产级风险预算。
- 2026-05-04：`COST-001` 完成基础版工程链路，新增回测成本假设拆分、明细列、报告摘要和 trace manifest 成本参数；该项仍不替代真实成交质量验证。
- 2026-05-04：`BACKTEST-001` 完成基础版工程链路，新增回测稳健性报告，复用同一 point-in-time 输入运行成本压力、起点后移和买入持有基准对比；该项仍不替代完整防过拟合、样本外和真实历史输入覆盖验证。
- 2026-05-04：`GOV-002` 完成基础版工程链路，新增 rule version manifest，并写入日报 trace、回测 trace 和 decision snapshot；该项不替代正式 owner approval、promotion 或 retirement 流程。
- 2026-05-04：`BTINPUT-001` 完成基础版工程链路，新增 `aits backtest-input-gaps` 和历史输入缺口报告；该项只诊断缺口并链接补数模板，不自动生成历史估值、风险事件或无风险复核声明。
- 2026-05-04：owner 明确当前系统不需要实际触发交易，仅作趋势判断；本文件从“生产交易依据”口径改为“趋势判断就绪”口径，并把执行/账户闭环降为后续可选扩展。
- 2026-05-04：`TREND-001` 完成，日报与回测结论边界新增 `trend_only` / `trend_judgment`，当前范围下不会把成功态升级为仓位复核或交易执行。
- 2026-05-04：`CHAIN-001` 节点健康度基础版完成，`score-daily` 日报输出“产业链节点热度与健康度”，并明确估值拥挤和风险事件只是限制说明，不是基本面证伪。
- 2026-05-04：`CHAIN-001` 回测历史节点追踪完成基础版，`backtest_daily_*.csv` 写入 top 节点、热度、健康等级和节点数据缺口，回测报告输出“产业链节点历史状态摘要”；该层仍为只读审计解释，不改变评分、仓位闸门或回测仓位。
- 2026-05-04：owner 决策不购买或伪造历史 PIT estimates/archive；`DATA-001/BTINPUT-001` 保留 baseline 和缺口诊断，不继续推进历史 PIT 采购、历史估值或历史风险事件补数；新增 `DATA-003/BACKTEST-002`，分别承接 forward-only 自建快照归档和历史回测数据可信度/滞后敏感性。
- 2026-05-06：owner 决策将 `PROD-001` 总控任务改为 `DONE`，原因：该任务牵涉子模块较多，继续挂总任务会掩盖真实阻塞边界；后续拆为 `PROD-002` 至 `PROD-006`，分别承接风险复核运行纪律、第二合格数据源、PIT 样本成熟度、规则治理批准和 forward shadow/outcome 成熟度。
