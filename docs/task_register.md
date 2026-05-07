# 任务登记与优先级机制

本文是未完成任务、后续优化、阻塞项、基础版遗留问题和 owner 配合事项的统一登记表。已完成和基础版完成任务归档在 `docs/task_register_completed.md`；`docs/implementation_backlog.md` 继续负责长期模块路线图。

最后更新：2026-05-06

## 使用规则

- 所有非平凡 TODO、review 后续项、基础版遗留项、临时绕行方案和需要 owner 配合的数据源事项，都必须登记在本文。
- 不把任务只留在代码注释、聊天记录、临时 checklist 或个人记忆中。
- 后续讨论出的非平凡需求、bug 修复、评分调整、数据链路调整、报告行为调整，必须先在本文新增或更新任务，明确优先级、状态、下一责任方、阻塞项和验收标准，再进入实现。
- 只有不影响系统行为、投资解释、数据流、数据质量、评分、回测或报告输出的纯 housekeeping，才可以不先登记。
- 如果需求上下文太长，不适合塞进任务表格，应该在 `docs/requirements/` 或其他清晰命名的 `docs/` 子目录创建独立 Markdown 文档。任务表只保留简短摘要和文档链接；详细背景、设计决策、开放问题、验收标准、进展记录和状态迁移必须在该文档中维护。
- 如果任务需要拆成多个开发步骤，必须先创建或更新独立需求文档，记录阶段拆解、依赖关系、实施顺序、各阶段验收标准、开放问题和状态迁移；任务表必须引用该文档，而不是把完整计划塞进单个表格单元。
- 使用独立需求文档时，后续开发进展必须同步更新任务表的摘要/状态，以及被引用文档中的详细进展记录。
- 任务状态变化必须和触发变化的代码或文档改动一起更新。
- `BASELINE_DONE` 不等于完成。它表示已有可审计基础闭环，但仍存在数据源、验证、设计或覆盖缺口。
- 被 owner 输入、数据源、API 权限、样本时间窗口或成本限制阻塞的任务，必须明确 blocker 和解除条件。
- 每个任务必须有可验收的完成标准，避免长期停留在模糊“优化”状态。

## 优先级

|优先级|含义|处理原则|
|---|---|---|
|P0|影响正确性、数据质量门禁、未来函数、仓位硬约束或可能导致错误投资解释|优先于新功能；未解决前不能把相关输出当作完整交易结论|
|P1|显著改善评分、回测、数据源可信度、风险控制或审计解释能力|排在近期开发队列前列|
|P2|提升覆盖率、易用性、报告下钻、长期维护或开发效率|在 P0/P1 稳定后推进|
|P3|低风险清理、文档润色、非关键体验优化|有明确收益时再做|

## 状态

|状态|含义|
|---|---|
|PROPOSED|已记录，但需求、设计或验收标准还未充分明确|
|READY|需求和验收标准明确，可以实施|
|IN_PROGRESS|正在实现或验证|
|BLOCKED_OWNER_INPUT|需要项目 owner 提供数据源、权限、样本、业务判断或人工复核|
|BLOCKED_EXTERNAL|受外部 API、供应商、时间窗口、成本或依赖限制|
|BASELINE_DONE|基础版已完成，但仍有明确后续缺口|
|VALIDATING|实现已完成，等待真实数据、回测、审计报告或运行观察确认|
|DONE|验收标准已满足，且相关文档、测试和报告已更新|
|DEFERRED|明确暂缓，且暂缓原因已记录|
|DROPPED|明确不再需要，且原因已记录|

## 当前任务

本节只保留仍需推进、验证、owner 输入或外部条件解除的任务。已完成、基础版完成或不再需要的任务归档在 `docs/task_register_completed.md`。

|ID|领域|优先级|状态|下一责任方|阻塞或下一步|验收标准|备注|
|---|---|---|---|---|---|---|---|
|PROD-002|趋势判断/风险复核运行纪律|P1|BLOCKED_OWNER_INPUT|项目 owner + 系统验证|`RISK-005/RISK-006/RISK-007/RISK-008` 已有工程底座；下一步需要 owner 明确每日风险复核责任人、合格来源清单、跳过规则和复核声明运行纪律；详细索引见 `docs/requirements/production_readiness_gaps_2026-05-04.md` 与 `docs/requirements/risk_event_review_workflow_2026-05-04.md`|连续运行样本中，日报能区分“已复核且未发现未记录重大事件”“有待复核 backlog”“复核缺失导致 policy/geopolitics 降级”；有效复核声明必须包含 reviewer、source scope、reviewed_at、coverage window 和结论；缺失或过期声明时不得把空 occurrence 目录解释为无风险|2026-05-06: 从 `PROD-001` 拆出，原因：总控任务已完成缺口登记和边界收口，但风险事件真实复核是 owner 运营纪律，不应继续压在总任务里。|
|PROD-003|趋势判断/第二合格数据源准入|P1|BLOCKED_OWNER_INPUT|项目 owner + 系统实现|`DATA-002` 已完成 provider health 和 reconciliation 覆盖报告基础版；下一步需要 owner 确认 `market_prices` 与 `macro_rates` 的第二 qualified source、授权/成本边界和缓存审计字段|`aits data-sources health` 或等价质量报告显示 `market_prices` 与 `macro_rates` 至少各有两个 qualified source；报告记录 provider、endpoint、请求参数、下载时间、row count、checksum 和授权限制；跨源冲突进入调查项，严重冲突能停止下游或显式降级趋势判断结论|2026-05-06: 从 `PROD-001` 拆出，原因：市场价格和宏观利率 reconciliation 是趋势判断可信度前置条件，但供应商选择与授权需要独立 owner 决策。|
|PROD-004|趋势判断/PIT 估值样本成熟度|P1|BLOCKED_EXTERNAL|系统验证 + 时间窗口|`DATA-003/BACKTEST-002/BTINPUT-001` 已完成 forward-only PIT 快照、回测可信度标签和覆盖诊断；下一步等待自建 PIT 样本自然积累，并用覆盖报告判断何时可提升估值/预期结论可信度|`aits backtest-pit-coverage` 能给出 core universe 的可用样本起始日期、覆盖率和 A/B readiness；`score-daily` 对 `eps_revision_90d_pct`、valuation percentile 和 vendor/self-archived PIT 来源的限制声明与实际覆盖一致；历史不足时继续降级，不得用采集日视图伪装 signal_date 可见数据|2026-05-06: 从 `PROD-001` 拆出，原因：owner 已决定不购买或伪造历史 PIT；剩余工作是持续验证 forward-only 样本成熟度，而不是继续推进总控任务。|
|PROD-005|趋势判断/规则治理 owner approval|P1|BLOCKED_OWNER_INPUT|项目 owner + 系统验证|`GOV-001/GOV-002/GOV-003` 已有 rule card、rule version 注入和受控 promotion/retirement 基础版；下一步需要 owner 对 production rule baseline、promotion 条件和 retirement 条件形成批准记录|核心 production rule card 具备 owner approval、适用范围、验证证据、已知失败模式、rollback condition 和最后复核时间；未批准候选规则不得改变正式评分、仓位 gate、日报结论或回测 production 规则；日报、回测、decision snapshot 和 trace 能追溯本次运行使用的规则版本|2026-05-06: 从 `PROD-001` 拆出，原因：规则治理工程底座已完成，但真实批准流属于 owner 决策和长期治理，不应让 `PROD-001` 继续保持进行中。|
|PROD-006|趋势判断/forward shadow 与 outcome 成熟度|P1|BLOCKED_EXTERNAL|系统验证 + 时间窗口|`SHADOW-002/SHADOW-003/REPORT-005` 已有 challenger shadow runner、样本成熟度报告和 production vs challenger 复盘入口；下一步等待真实 prediction outcome 样本达到可评估窗口|shadow maturity 报告按 candidate、horizon 和 `ai_after_chatgpt` regime 输出样本数、pending/missing、收益、胜率、回撤和 benchmark excess；样本不足时 promotion gate 维持观察或缺失状态；达到门槛前，回测或 shadow 结果不得被写成 production 规则晋级结论|2026-05-06: 从 `PROD-001` 拆出，原因：趋势判断长期可信度需要真实前向样本，但该成熟度受交易日和 label horizon 限制，适合作为独立时间窗口任务。|
|OPS-003|运行架构/云端持续运行|P1|BASELINE_DONE|系统实现 + 项目 owner|第一阶段已完成 `aits ops daily-plan`；详细拆解见 `docs/requirements/cloud_operations_2026-05-06.md`。后续需要结构化 run log、真实 orchestrator，以及 owner 确认云厂商、VM 规格、调度时间、通知渠道、secret 管理和缓存/备份策略|每日运行入口必须明确下载、PIT 快照、数据质量门禁、日报、pipeline health、secret hygiene 的顺序和阻断关系；缺少关键凭据或输入时不得静默跳过；云端部署文档必须覆盖 systemd/cron、持久化目录、日志、告警、备份和恢复；运行报告必须显示数据质量状态和 output artifact 路径|2026-05-06: 新增任务，原因：当前持续运行依赖开发机开机；迁移到云 VM 可行但会影响运行链路、质量门禁和 PIT 样本连续性，必须先拆解和登记。2026-05-06: 从 IN_PROGRESS 改为 BASELINE_DONE，原因：新增每日运行计划命令、报告、系统流图、README 和测试；完整 DONE 仍需 run log、真实调度执行器、云 VM runbook、备份恢复和通知策略。|
|OPS-005|运行架构/日报前置数据依赖|P0|VALIDATING|系统验证|详细拆解见 `docs/requirements/daily_score_preflight_dependencies_2026-05-07.md`；daily plan 已新增 SEC companyfacts 刷新、SEC metrics 抽取/校验和 FMP 估值快照刷新；下一步用下一次真实日报验证新增前置链路|默认每日计划必须在 `score-daily` 前列出并阻断执行 `aits fundamentals download-sec-companyfacts`、`aits fundamentals extract-sec-metrics --as-of YYYY-MM-DD`、`aits fundamentals validate-sec-metrics --as-of YYYY-MM-DD` 和 `aits valuation fetch-fmp --as-of YYYY-MM-DD`；缺少 `SEC_USER_AGENT` 或 `FMP_API_KEY` 必须显示 `BLOCKED_ENV`；报告、README、系统流图和测试同步更新；下一次真实日报不应再因当日 SEC metrics 文件缺失或估值快照过期而失败|2026-05-07: 新增任务，原因：真实每日运行暴露日报依赖当日 SEC metrics，但现有每日编排没有生成该 CSV；同时审计发现估值快照刷新也不在日报前置链路中。2026-05-07: 从 IN_PROGRESS 改为 VALIDATING，原因：`aits ops daily-plan` 已补齐 SEC companyfacts、SEC metrics、SEC metrics validation 和 FMP valuation 前置步骤；验证通过 `ruff check src tests` 和完整 `pytest -q` 408 项。|

## 暂缓任务

本节保留已明确暂缓、但未来可能因数据规模、owner 决策或外部条件变化而重新打开的任务。

|ID|领域|优先级|状态|下一责任方|阻塞或下一步|验收标准|备注|
|---|---|---|---|---|---|---|---|
|STORAGE-001|数据工程|P3|DEFERRED|系统实现|已复核当前本地缓存、回测和报告规模，暂不引入 DuckDB / Parquet；详细拆解见 `docs/requirements/feedback_loop_governance_2026-05-04.md#storage-001`|重新打开前需证明 CSV 在 schema 演进、join 性能、历史快照、类型约束或审计查询上成为实际瓶颈；迁移仍必须保持 CSV 兼容期、checksum、数据版本、来源追溯和回滚验证|2026-05-06: 从 PROPOSED 改为 DEFERRED，原因：本轮全量校验、短区间真实缓存回测和报告生成未暴露存储层瓶颈；当前 CSV 更利于本地审计和可读性，避免无收益迁移。|

## 更新模板

新增任务时使用下面格式补充到“当前任务”表：

```text
ID:
领域:
优先级:
状态:
下一责任方:
阻塞或下一步:
验收标准:
备注:
```

状态更新时至少记录：

```text
YYYY-MM-DD: 从 <旧状态> 改为 <新状态>，原因：<触发条件、实现范围、验证结果或 blocker>。
```
