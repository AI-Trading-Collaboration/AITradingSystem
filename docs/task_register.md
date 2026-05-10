# 任务登记与优先级机制

本文是未完成任务、后续优化、阻塞项、基础版遗留问题和 owner 配合事项的统一登记表。已完成和基础版完成任务归档在 `docs/task_register_completed.md`；`docs/implementation_backlog.md` 继续负责长期模块路线图。

最后更新：2026-05-10

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
|OPS-003|运行架构/云端持续运行|P1|BASELINE_DONE|系统实现 + 项目 owner|本地基础闭环已完成 `aits ops daily-plan` 与 `aits ops daily-run`；详细拆解见 `docs/requirements/cloud_operations_2026-05-06.md`。后续仍需 owner 确认云厂商、VM 规格、调度时间、通知渠道、secret 管理和缓存/备份策略|每日运行入口必须明确下载、PIT 快照、数据质量门禁、日报、pipeline health、secret hygiene 的顺序和阻断关系；缺少关键凭据或输入时不得静默跳过；云端部署文档必须覆盖 systemd/cron、持久化目录、日志、告警、备份和恢复；运行报告必须显示数据质量状态和 output artifact 路径|2026-05-06: 新增任务，原因：当前持续运行依赖开发机开机；迁移到云 VM 可行但会影响运行链路、质量门禁和 PIT 样本连续性，必须先拆解和登记。2026-05-06: 从 IN_PROGRESS 改为 BASELINE_DONE，原因：新增每日运行计划命令、报告、系统流图、README 和测试；完整 DONE 仍需 run log、真实调度执行器、云 VM runbook、备份恢复和通知策略。2026-05-08: 从 BASELINE_DONE 改为 IN_PROGRESS，原因：每日自动化真实运行暴露 `daily-plan` 只生成计划、不执行前置依赖和日报评分，owner 同意以能让每日任务正常跑完为目标补齐执行入口。2026-05-08: 从 IN_PROGRESS 改为 BASELINE_DONE，原因：新增 `aits ops daily-run`、脱敏执行报告和关键 artifact 状态检查；自动化改为调用 daily-run；真实 2026-05-08 每日链路 9/9 步通过，验证 `ruff check src tests` 与 `pytest -q` 412 passed。|
|OPS-005|运行架构/日报前置数据依赖|P0|DONE|系统实现|详细拆解见 `docs/requirements/daily_score_preflight_dependencies_2026-05-07.md`；daily plan 已新增 SEC companyfacts 刷新、SEC metrics 抽取/校验和 FMP 估值快照刷新；2026-05-07 真实日报链路已验证不再因当日 SEC metrics 缺失停止；`ops health` 已显式检查 FMP PIT 抓取报告状态|默认每日计划必须在 `score-daily` 前列出并阻断执行 `aits fundamentals download-sec-companyfacts`、`aits fundamentals extract-sec-metrics --as-of YYYY-MM-DD`、`aits fundamentals validate-sec-metrics --as-of YYYY-MM-DD` 和 `aits valuation fetch-fmp --as-of YYYY-MM-DD`；缺少 `SEC_USER_AGENT` 或 `FMP_API_KEY` 必须显示 `BLOCKED_ENV`；报告、README、系统流图和测试同步更新；下一次真实日报不应再因当日 SEC metrics 文件缺失或估值快照过期而失败；PIT 抓取失败必须进入 FMP PIT 抓取报告和 pipeline health/alert|2026-05-07: 新增任务，原因：真实每日运行暴露日报依赖当日 SEC metrics，但现有每日编排没有生成该 CSV；同时审计发现估值快照刷新也不在日报前置链路中。2026-05-07: 从 IN_PROGRESS 改为 VALIDATING，原因：`aits ops daily-plan` 已补齐 SEC companyfacts、SEC metrics、SEC metrics validation 和 FMP valuation 前置步骤；验证通过 `ruff check src tests` 和完整 `pytest -q` 408 项。2026-05-07: 从 VALIDATING 改为 DONE，原因：真实日报链路通过，`score-daily` 输出 `PASS_WITH_LIMITATIONS`；同时修复运行观察发现的 PIT fetch 报告状态 health 漏报，验证通过 `ruff check src tests`、`pytest -q tests/test_pipeline_health.py`、`pytest -q tests/test_ops_daily.py tests/test_pipeline_health.py` 和 `aits ops health --as-of 2026-05-07`。|
|OPS-006|运行架构/休市日每日模式|P0|DONE|系统实现|详细拆解见 `docs/requirements/closed_market_daily_mode_2026-05-10.md`；已实现 `daily-plan/daily-run` 的显式休市日模式，避免周末或 NYSE 常规休市日硬跑当日市场下载和 `score-daily`|`aits ops daily-plan` 与 `aits ops daily-run` 必须识别非交易日并在报告中声明 market session、休市原因和上一交易日；当上一交易日价格缓存已覆盖时不得用休市日作为 `download-data --end`；休市日不得生成新的 `daily_score`、decision snapshot、执行动作或 score-derived alerts；仍可运行官方政策来源、PIT、SEC、valuation、ops health 和 secret scan，且 health 不得把缺少休市日日报当作失败；系统流图、README、测试和需求文档同步更新|2026-05-10: 新增并进入实现，原因：真实 2026-05-10 daily-run 在周日用 `download-data --end 2026-05-10` 失败，owner 决策将休市日运行策略正式记录并实现，避免把休市日缺行情误解释为评分失败或生成伪交易结论。2026-05-10: 从 IN_PROGRESS 改为 DONE，原因：新增交易日历模块、closed-market daily-plan/daily-run 步骤调整、`ops health --non-trading-day`、README/系统流图/需求文档和测试；验证 `ruff check src tests` 通过，`pytest -q` 424 passed，2026-05-10 daily-plan CLI smoke 输出 `READY_WITH_SKIPS`、`CLOSED_MARKET` 和上一交易日 `2026-05-08`。2026-05-10: 追加验证，原因：owner 要求清理后复验交易日/非交易日流程；备份并移出 2026-05-10 旧 daily ops 报告后真实 `daily-run --as-of 2026-05-10` 通过 `PASS_WITH_SKIPS`，交易日路径用干净临时目录 fake runner 验证 2026-05-08 9/9 步 PASS，并修复 PIT produced_paths 隔离路径问题；复验 `ruff check src tests` 通过、`pytest -q` 424 passed。2026-05-10: 补充归档输入回放，5/8 PIT 备份可用，但必须同时冻结 PIT manifest 可见窗口并隔离 5/9/5/10 valuation snapshots；按该边界执行缓存回放 `daily-run --as-of 2026-05-08`，`score_daily`、`ops health`、secret scan PASS，整体 `PASS_WITH_SKIPS`。|
|OPS-007|运行架构/历史交易日归档回放|P0|DONE|系统实现|详细设计见 `docs/requirements/historical_day_replay_infrastructure_2026-05-10.md`；单日 cache-only replay、批量 `replay-window`、production diff、production `daily-run` 脱敏 metadata sidecar、OpenAI 历史缓存策略、pre-run input checksum 和 production visibility cutoff 归档均已完成|必须提供 cache-only 历史交易日回放入口，冻结 as-of 可见输入窗口，隔离 replay 输出，不改写生产 artifacts，不调用 live provider；PIT/valuation/SEC/market/macro/risk/rule card 均需按 as-of 门禁 fail closed；输出 replay bundle、input freeze manifest、结构化脱敏 run log 和可选 production diff；5/8 回放应能自动排除 5/9/5/10 valuation 与未来 PIT manifest 行并通过 `score_daily`、`ops health`、secret scan；批量回放必须按交易日枚举并生成结构化窗口报告；production daily-run 必须归档 run id、git/config/rule hash、命令清单、env presence、pre-run input checksum、produced artifact checksum 和 visibility cutoff；OpenAI replay 不得调用 live API，只能 disabled 或 cache-only|2026-05-10: 新增 PROPOSED，原因：owner 确认历史交易日分析产出回放会成为模型调优重点基础设施；当前只能通过手工 backup/filter/restore 做归档回放，无法支撑高频、安全、可审计 replay。2026-05-10: 从 PROPOSED 改为 IN_PROGRESS，原因：owner 确认按设计实现；第一阶段聚焦单日 cache-only replay MVP，不覆盖批量回放和 production replay 元数据强化。2026-05-10: 从 IN_PROGRESS 改为 BASELINE_DONE，原因：新增 `aits ops replay-day`、input freeze manifest、隔离 replay bundle、PIT/valuation 可见窗口冻结、replay-scoped `score-daily`/`ops health`/secret scan；真实 2026-05-08 cache-only replay PASS，验证 `ruff check src tests` 通过、`pytest -q` 426 passed。2026-05-10: 从 BASELINE_DONE 改为 IN_PROGRESS，原因：owner 要求后续任务依次开发，先推进阶段 5 的批量窗口回放和 production diff，仍不进入 production daily-run 元数据强化阶段。2026-05-10: 阶段 5 完成并继续阶段 6，原因：`replay-window` 真实 inventory-only 验证 2026-05-07 到 2026-05-10 通过且跳过周末，`replay-day --compare-to-production` 真实 2026-05-08 完整回放通过并生成 diff；下一步补 production `daily-run` metadata sidecar。2026-05-10: 从 IN_PROGRESS 改为 BASELINE_DONE，原因：新增 `daily_ops_run_metadata_YYYY-MM-DD.json`，记录 run id、git/config/rule hash、命令清单、env presence、step result 和 produced artifact checksum；阶段 5/6 验证通过 `ruff check src tests`、`pytest -q` 430 passed、真实 5/8 replay diff PASS 和 5/7-5/10 window inventory PASS。2026-05-10: 从 BASELINE_DONE 改为 IN_PROGRESS，原因：owner 要求完成剩余重点任务并提交，范围收敛为 OpenAI 历史缓存策略、pre-run input checksum 和 production visibility cutoff 归档。2026-05-10: 从 IN_PROGRESS 改为 DONE，原因：新增 `--openai-replay-policy disabled|cache-only`、production metadata cutoff 推导、pre-run input artifact checksum；真实 5/8 OpenAI cache-only inventory replay PASS，验证 `ruff check src tests`、`pytest -q` 431 passed 和 CLI help 均通过。|
|DATA-012|数据质量/DTWEXBGS 新鲜度|P0|DONE|系统实现 + 项目 owner|详细拆解见 `docs/requirements/dtwexbgs_freshness_policy_2026-05-09.md`；已把 `DTWEXBGS` 从全体 FRED 宏观序列 7 日历日 freshness 改为 14 日历日 series 级阈值；后续 owner 可决定是否接入第二宏观来源或直接 Federal Reserve H.10 来源|`DTWEXBGS` 使用符合 Federal Reserve H.10 周度发布机制的 series 级 freshness 阈值，DGS2/DGS10 仍保持较严格默认阈值；`aits validate-data`、`score-daily` 和 `backtest` 继续 fail closed 于真正过期或缺失的宏观输入；数据源成本选项和接入边界记录在需求文档；系统流图、配置和测试同步更新|2026-05-09: 新增并进入实现，原因：真实每日 `daily-run` 已更新 FRED 缓存，但 FRED/Board `DTWEXBGS` 最新 observation 仍停在 2026-05-01，8 个日历日触发全局 7 天门禁；该滞后符合 H.10 周度发布时间特征，需用 series 级 freshness 表达，而不是绕过数据质量门禁。2026-05-09: 从 IN_PROGRESS 改为 DONE，原因：新增 rate series 级 `max_stale_calendar_days` override，`DTWEXBGS` 配置为 14 天，DGS2/DGS10 保持默认 7 天；`validate-data --as-of 2026-05-09` 已从 `FAIL` 变为 `PASS_WITH_WARNINGS` 且错误数 0；验证通过 `ruff check src tests`、目标测试 39 passed 和完整 pytest 415 passed。|
|DATA-013|PIT/原始快照不可变性|P0|DONE|系统实现|详细拆解见 `docs/requirements/pit_raw_payload_immutability_2026-05-09.md`；已修复 `valuation fetch-fmp` 同日多次运行覆盖 `fmp_analyst_estimates` raw JSON，导致 PIT manifest checksum mismatch 的问题|PIT manifest 指向的 raw payload 不得被后续日常步骤覆盖；同一 ticker 同一 captured date 多次运行必须写入不同 analyst-estimates raw 文件，文件名含下载时间或 checksum；`aits ops daily-run` 的 pipeline health 不再因同日 valuation rerun 产生 PIT checksum mismatch；测试覆盖同日多次写入不覆盖和 CLI raw path 变更|2026-05-09: 新增并进入实现，原因：验证 `DATA-012` 后真实 `daily-run` 已通过 score_daily，但 pipeline_health 失败，报 6 条 PIT raw payload checksum mismatch；根因是 valuation 前置步骤覆盖了 PIT manifest 之前记录的同日 analyst-estimates raw 文件。2026-05-09: 从 IN_PROGRESS 改为 DONE，原因：analyst-estimates raw 文件名新增 downloaded_at UTC token 与 checksum 前缀，loader 继续兼容旧 glob；真实 `aits ops daily-run` 9/9 步 PASS，pipeline health PASS，secret scan PASS；目标测试 53 passed，Ruff 通过。|
|SCORE-006|评分/仓位置信度约束|P0|DONE|系统实现|详细拆解见 `docs/requirements/confidence_position_gate_2026-05-08.md`；已把判断置信度上限并入 `position_gate` 最严格上限链路；默认 live `score-daily --as-of 2026-05-08` 额外验证在 10 分钟超时内未返回，已终止残留进程，未作为通过证据|`score-daily`、decision snapshot、belief_state、回测每日评分均使用一致的置信度仓位边界；置信度调整仓位基于评分模型原始仓位计算，并作为独立 gate 参与最终仓位约束；最终仓位不得高于置信度 gate 和其他风险 gate 的最严格上限；报告能说明估值、风险、thesis、数据质量和置信度谁是当前最严格限制；测试覆盖无更严格 gate 时置信度能压低最终仓位，以及估值极端过热时估值 gate 仍可比置信度更严格|2026-05-08: 新增并进入实现，原因：运行观察发现最近日报最终仓位持续 40%-40%，主因是估值 `EXTREME_OVERHEATED` gate；但报告同时显示“置信度调整后建议仓位”34%-34%、最终仓位 40%-40%，暴露置信度展示值未参与最终仓位约束，容易造成错误投资解释。2026-05-08: 从 IN_PROGRESS 改为 DONE，原因：`DailyConfidenceAssessment` 改为评分构建时固定计算，基于评分模型原始仓位生成 `confidence` position gate，并参与最终仓位最严格上限；日报文案、系统流图和测试同步更新；验证通过 `ruff check src tests`、`pytest -q tests/test_daily_scoring.py tests/test_position_model.py tests/test_alerts.py tests/test_backtest.py` 和完整 `pytest -q` 414 项。|

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
