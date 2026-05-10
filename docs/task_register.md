# 任务登记与优先级机制

本文是未完成任务、后续优化、阻塞项、基础版遗留问题和 owner 配合事项的统一登记表。已完成和基础版完成任务归档在 `docs/task_register_completed.md`；`docs/implementation_backlog.md` 继续负责长期模块路线图。

最后更新：2026-05-11

## 使用规则

- 所有非平凡 TODO、review 后续项、基础版遗留项、临时绕行方案和需要 owner 配合的数据源事项，都必须登记在本文。
- 不把任务只留在代码注释、聊天记录、临时 checklist 或个人记忆中。
- 后续讨论出的非平凡需求、bug 修复、评分调整、数据链路调整、报告行为调整，必须先在本文新增或更新任务，明确优先级、状态、下一责任方、阻塞项和验收标准，再进入实现。
- 只有不影响系统行为、投资解释、数据流、数据质量、评分、回测或报告输出的纯 housekeeping，才可以不先登记。
- 如果需求上下文太长，不适合塞进任务表格，应该在 `docs/requirements/` 或其他清晰命名的 `docs/` 子目录创建独立 Markdown 文档。任务表只保留简短摘要和文档链接；详细背景、设计决策、开放问题、验收标准、进展记录和状态迁移必须在该文档中维护。
- 如果任务需要拆成多个开发步骤，必须先创建或更新独立需求文档，记录阶段拆解、依赖关系、实施顺序、各阶段验收标准、开放问题和状态迁移；任务表必须引用该文档，而不是把完整计划塞进单个表格单元。
- 使用独立需求文档时，后续开发进展必须同步更新任务表的摘要/状态，以及被引用文档中的详细进展记录。
- 任务状态变化必须和触发变化的代码或文档改动一起更新。
- 任务改为 `DONE`、`BASELINE_DONE` 或 `DROPPED` 时，同一变更必须把整行移动到 `docs/task_register_completed.md`，并同步更新 completed 文档的 `最后更新` 日期。
- `docs/task_register.md` 的“当前任务”只保留仍需推进、验证、owner 输入或外部条件解除的任务；已归档状态不得继续留在当前任务表。
- 归档前必须检查任务 ID 是否已在 `docs/task_register_completed.md` 中存在；如果发现 ID 复用，先修正为唯一 ID，并在备注中记录原 ID 和修正原因。
- 提交前执行 `rg "^\|[^|]+\|[^|]+\|P[0-3]\|(DONE|BASELINE_DONE|DROPPED)\|" docs/task_register.md`；正常情况下不应有结果。若确实需要例外，必须在同一变更中说明原因。
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
|UNIV-001|标的池/模型参数化|P1|VALIDATING|系统验证|详细拆解见 `docs/requirements/ai_universe_and_model_parameterization_2026-05-11.md`；expanded universe 已通过真实非交易日 `daily-run`、交易日 strict `replay-day` 和包含周末的 `replay-window` 验证；下一步继续观察后续真实运行中的 AMZN R&D 披露口径 warning 和新增 ticker 基本面特征质量|新增代表 ticker 进入 `core_watchlist`、watchlist、lifecycle 和 SEC 映射；默认数据下载、特征、评分、回测和 dashboard 使用扩展后的核心观察池；配置测试、watchlist/lifecycle 校验和相关单测通过；文档列出 valuation gate、评分仓位映射、置信度折扣、宏观预算、风险预算、数据质量阈值和执行政策等后续可调参数候选；真实 daily/replay 验证不因新增 ticker 的价格、估值或基本面覆盖配置错误而阻断|2026-05-11: 新增并进入实现，原因：owner 明确系统尚未稳定进入生产，可以优先提升 AI 产业链代表性覆盖，不要求与早期 6 ticker 输出保持兼容；同时要求整理类似 valuation 40% cap 的硬阈值作为后续模型迭代重点。2026-05-11: 从 IN_PROGRESS 改为 VALIDATING，原因：核心观察池扩展到 17 个 ticker，watchlist/lifecycle/SEC 映射、风险事件映射、官方政策 ticker 匹配、AVGO/LRCX 拆股事件、系统流图和参数化清单均已更新；验证通过 CLI 校验、ruff 和全量 pytest。2026-05-11: 从 VALIDATING 改回 IN_PROGRESS，原因：expanded universe 真实非交易日 daily-run 已跑到 SEC metrics 并阻断于 ASML taxonomy 配置，SEC companyfacts 实际返回 `us-gaap + dei`，需要修正配置后复测。2026-05-11: 从 IN_PROGRESS 改为 VALIDATING，原因：ASML SEC taxonomy 修正为 `us-gaap + dei` 并补 `us-gaap/EUR` 指标映射；非交易日 `daily-run --as-of 2026-05-10` 为 PASS_WITH_SKIPS，交易日 `replay-day --as-of 2026-05-08 --full-universe` 为 PASS，`replay-window 2026-05-08..2026-05-10` 为 PASS_WITH_SKIPS（1 个交易日、2 个周末跳过）。|
|ARCH-001|架构/CLI 模块化拆分|P2|READY|系统实现|详细拆解见 `docs/requirements/system_design_review_2026-05-10.md`；路线已登记，下一批从低耦合命令组迁移到 `cli_commands/`，保留 `ai_trading_system.cli:app` 入口|形成可分阶段实施的 CLI 分包方案；每阶段有明确命令组、导入边界、测试范围和回滚方式；拆分后 `aits` 命令名、参数和退出码保持兼容；不得在未验证前改动评分、回测或日报语义|2026-05-10: 新增，原因：`cli.py` 已超过 400KB，review 认为长期维护风险上升，但本轮应先做运行闭环和报告收口，避免大规模结构迁移。2026-05-10: 路线已在系统设计 review 文档登记，本任务继续保留为下一批实际分包实施入口。|
|UI-002|仪表盘/每日决策展示|P2|VALIDATING|系统验证|详细拆解见 `docs/requirements/decision_dashboard_v2_2026-05-10.md`；`aits reports dashboard` 已接入 `ops daily-run` 的 `score_daily` 后置只读步骤，并接入 `ops replay-day` 的 cache-only 隔离输出；历史趋势区块已改为中文摘要 + 静态 SVG 折线图，标题改为“最近可用评分趋势”以避免把非连续历史误写成近 20 个交易日|Dashboard v2 仍只读取已生成 artifact，不重新计算结论；HTML 首页能在一屏内展示今日动作、仓位区间、置信度、Data Gate、最大限制、变化摘要、Top evidence/invalidators/next checks；可选读取 `alerts_YYYY-MM-DD.md` 和 `scores_daily.csv` 生成告警摘要与历史趋势；历史趋势展示应使用中文标签和可读静态图形，不用不可解释的字符块；同步输出 JSON payload 供测试和后续前端演进；页面、JSON 和文档均声明 `production_effect=none`、不替代 Markdown 日报和 trace bundle 审计源；`daily-run` 中 dashboard 生成失败不得改变评分结论或交易动作|2026-05-10: 新增并进入实现，原因：owner 认可静态只读 dashboard 路线，并要求继续推进数据展示层；范围限定为现有 evidence dashboard 的可审计增量，避免过早建设在线 Web App 或交易终端。2026-05-10: 从 IN_PROGRESS 改为 VALIDATING，原因：静态 HTML/JSON、alerts 聚合、history 摘要、CLI 和测试已完成；浏览器插件因 file URL 安全策略未能做内嵌视觉预览，需后续真实使用复核。2026-05-10: 从 VALIDATING 改回 IN_PROGRESS，原因：owner 要求把网页生成接入自动 daily-run 流程并跑最近交易日全流程。2026-05-10: 从 IN_PROGRESS 改为 VALIDATING，原因：自动触发已实现并通过测试；真实 `daily-run --as-of 2026-05-08` 先通过 download/PIT/SEC/valuation，随后阻断在 `score_daily` 的 `risk_event_prereview_request_in_future`，dashboard 后置步骤尚未执行到。2026-05-10: 最近交易日改用 strict replay 入口复测，`replay-day --as-of 2026-05-08 --mode cache-only --openai-replay-policy cache-only --compare-to-production` PASS，并在 replay bundle 生成 `evidence_dashboard_2026-05-08.html/json`。2026-05-11: 从 VALIDATING 改回 IN_PROGRESS，原因：owner 复核 dashboard 时发现历史趋势区块 `Score sparkline` 标签和字符块图形可读性差，需要修复展示但不改变结论计算。2026-05-11: 从 IN_PROGRESS 改为 VALIDATING，原因：历史趋势已改为中文 SVG 折线图，并重生成当前 2026-05-08 replay dashboard；全量测试、ruff 和 mypy 均通过。|
|VIS-001|数据可见性/PIT 复现契约|P0|VALIDATING|系统验证|详细拆解见 `docs/requirements/input_visibility_contract_2026-05-10.md`；运行入口级强边界已实现：历史 `as_of` 不再用 `daily-run` 调 live provider/OpenAI 或改写生产路径，严格复现走 `replay-day cache-only` 隔离 bundle；OpenAI prereview cache-only 已改为 row 级可见性过滤；`trade_theses`、`trades` 等手工输入已纳入 replay 隔离过滤视图，下一步观察更多真实交易记录 `recorded_at` 样本|`daily-run` 对历史 `as_of` 在执行任何子命令前 fail closed，并在执行报告/metadata 中说明应改用 `ops replay-day`；`replay-day` 不调用 live provider/OpenAI，按可见窗口过滤 PIT、估值、风险事件、OpenAI cache-only、交易 thesis 和交易记录输入，并生成 replay-scoped dashboard；所有相关文档说明 `event_time/source_published_at/provider_available_at/ingested_at/available_time/visibility_cutoff` 的分层语义；测试覆盖历史 daily-run 前置阻断、OpenAI cache-only 可见性过滤、手工输入过滤和最近交易日 cache-only replay 全链路|2026-05-10: 新增并进入实现，原因：最近交易日 `daily-run --as-of 2026-05-08` 在 OpenAI 预审处被 `risk_event_prereview_request_in_future` 阻断，暴露生产调度、历史复现和研究重建的输入可见性边界仍不够显式；owner 要求继续修复并用原日期复测。2026-05-10: 从 IN_PROGRESS 改为 VALIDATING，原因：daily-run 历史 as_of 预检查、metadata input visibility、replay dashboard、文档和测试已完成；原日期 daily-run 现在前置 `BLOCKED_VISIBILITY`，cache-only replay PASS。2026-05-10: 从 VALIDATING 改回 IN_PROGRESS，原因：owner 要求 replay 尽量复用历史 OpenAI 请求结果，但必须按复现时点可见性过滤缓存记录，避免未来 LLM 请求结果进入严格复现。2026-05-10: 从 IN_PROGRESS 改为 VALIDATING，原因：OpenAI cache-only replay 已按 `request_timestamp/cache_created_at/cache file created_at` 过滤；真实 2026-05-08 replay PASS，源 OpenAI 预审队列 5 条均因 2026-05-10 请求时间晚于 cutoff 被排除，dashboard/health/secret 全链路通过。2026-05-10: 调查 dashboard `final AI position=0` 时确认，直接原因是 `trade_theses` 全部创建/更新时间为 2026-05-10，严格回放 2026-05-08 时 thesis 校验报 24 个 future-input 错误并触发 `thesis` position gate 上限 0%；当前行为 fail closed，但后续应在 replay 输入冻结阶段先生成 thesis 隔离视图。2026-05-10: 从 VALIDATING 改回 IN_PROGRESS，原因：owner 明确要求这些手工数据也要做好隔离；本轮验收包含 replay path override、过滤 manifest 和原日期复测。2026-05-10: 从 IN_PROGRESS 改为 VALIDATING，原因：手工输入隔离视图已实现并通过全量测试；真实 2026-05-08 replay PASS，6 条 future thesis 被排除，dashboard 最终 AI 仓位从旧 run 的 0%-0% 恢复为估值 gate 约束下的 40%-40%。|
|DATA-016|数据质量/价格 reconciliation|P0|VALIDATING|系统验证|详细拆解见 `docs/requirements/confidence_lift_workplan_2026-05-10.md`；`NOW` 2025-12-17/18 拆股窗口 raw close 日期口径差异已按已知 corporate action 归因，下一步观察后续真实 `download-data` / `validate-data` 是否继续无未解释 raw close 冲突|`aits validate-data` 或独立 reconciliation 报告能列出主源/二源 ticker-date 差异、分类规则、证据字段、主/二源数值、可解释原因和不可解释冲突；能证明是指数 volume、已知拆股窗口日期口径或分红复权口径差异的必须降为有证据限制；无法解释的 raw close/OHLC/非正价格仍不得静默通过|2026-05-10: 新增并进入实现，原因：owner 指出该项理论上可把日报置信度从 61 提升到约 76；实现必须可审计，不允许静默忽略。2026-05-10: 从 IN_PROGRESS 改为 VALIDATING，原因：新增 INFO severity、指数 volume/拆股/Marketstack 自身坏点/adjusted close 口径差异 reconciliation CSV；真实 `validate-data --as-of 2026-05-10` 为 PASS，错误 0、警告 0、信息 11。2026-05-11: 从 VALIDATING 改回 IN_PROGRESS，原因：expanded universe 真实价格刷新后，`NOW` 官方 5-for-1 split 导致 Marketstack 在拆股窗口提前按 split-adjusted raw close 记录 2025-12-17，当前规则仍按未解释 raw close mismatch 阻断，需要补 corporate-action window 归因。2026-05-11: 从 IN_PROGRESS 改为 VALIDATING，原因：已补 `NOW` 官方 5-for-1 split 配置和 `known_split_raw_close_basis_difference` reconciliation 分类；`validate-data --as-of 2026-05-10 --full-universe` 与 replay 内 `data_quality_2026-05-08` 均为 PASS，错误 0、警告 0。|
|FUND-001|基本面/TSM IR 覆盖规则|P1|VALIDATING|系统验证|详细拆解见 `docs/requirements/confidence_lift_workplan_2026-05-10.md`；TSM IR 已合并进 2026-05-10 SEC-style metrics，且日报计划/`score-daily` 已接入合并路径；下一步观察每日流程持续运行结果|TSM 基本面覆盖由 TSMC IR 官方季度 Management Report 或等价可审计来源满足；缺 TSM IR 时报告明确缺官方 IR 覆盖；不再因 TSM 缺 SEC companyfacts 季度指标而误判为美国 SEC 指标缺口|2026-05-10: 新增并进入实现，原因：owner 明确 TSM 不应硬等同美国 SEC companyfacts 覆盖。2026-05-10: 从 IN_PROGRESS 改为 VALIDATING，原因：`merge-tsm-ir-sec-metrics` 后 SEC metrics 72 行且 validation PASS，`build-sec-features` PASS、警告 0。2026-05-10: 补充日报链路，`ops daily-plan` 在 `validate-sec-metrics` 前加入 TSM IR 合并，`score-daily` 在本地 TSM IR 缓存存在时自动合并后再校验。|
|VAL-001|估值/PIT 历史日回跑|P0|VALIDATING|系统验证|估值校验已改为历史 as-of 只使用可见快照，未来快照进入 warning 审计；下一步观察后续 replay/daily-run 是否继续保持无误用未来估值|历史 as-of 日报或 daily-run 只使用评估日可见估值快照；后续日期快照不得进入评分/复核，也不得导致历史日流程误失败；被排除快照必须以 warning/审计记录出现在估值校验报告；真实 schema/load 错误和当前可见快照错误仍 fail closed|2026-05-10: 新增并进入实现，原因：owner 要求测试最后一个交易日流程，真实 `ops daily-run --as-of 2026-05-08` 失败于 `valuation_snapshots`，暴露累计快照目录对历史日回跑的未来数据处理缺陷。2026-05-10: 从 IN_PROGRESS 改为 VALIDATING，原因：`valuation fetch-fmp --as-of 2026-05-08` 已从 FAIL 转为 PASS_WITH_WARNINGS，报告显式列出被排除的 2026-05-09/2026-05-10 快照。|
|PROD-002|趋势判断/风险复核运行纪律|P1|BLOCKED_OWNER_INPUT|项目 owner + 系统验证|`RISK-005/RISK-006/RISK-007/RISK-008` 已有工程底座；下一步需要 owner 明确每日风险复核责任人、合格来源清单、跳过规则和复核声明运行纪律；详细索引见 `docs/requirements/production_readiness_gaps_2026-05-04.md` 与 `docs/requirements/risk_event_review_workflow_2026-05-04.md`|连续运行样本中，日报能区分“已复核且未发现未记录重大事件”“有待复核 backlog”“复核缺失导致 policy/geopolitics 降级”；有效复核声明必须包含 reviewer、source scope、reviewed_at、coverage window 和结论；缺失或过期声明时不得把空 occurrence 目录解释为无风险|2026-05-06: 从 `PROD-001` 拆出，原因：总控任务已完成缺口登记和边界收口，但风险事件真实复核是 owner 运营纪律，不应继续压在总任务里。|
|RISK-013|风险事件/官方复核闭环|P1|BLOCKED_OWNER_INPUT|项目 owner + 系统验证|详细拆解见 `docs/requirements/confidence_lift_workplan_2026-05-10.md`；系统侧已检查官方来源权限、候选/队列状态和复核声明写入路径；下一步需要真实 reviewer 给出人工复核结论|报告能列出官方来源权限状态、OpenAI 预审队列、官方候选分类、已写入 occurrence/attestation 和缺口；只有存在真实 reviewer、source scope、reviewed_at、coverage window 和结论时，才允许用 `risk-events record-review-attestation` 写入人工复核声明|2026-05-10: 新增并进入实现，原因：owner 要求补风险事件复核闭环以解除 `policy_geopolitics` 的 `insufficient_data` 和 data confidence 上限；人工复核不得由系统伪造。2026-05-10: 从 IN_PROGRESS 改为 BLOCKED_OWNER_INPUT，原因：官方 API key 可见、官方来源抓取/triage/occurrence 校验已通过，但当前只有 LLM formal attestation，没有真实人工复核结论，不能写 manual_input 声明。|
|RISK-014|风险事件/PIT 历史日回跑|P0|VALIDATING|系统验证|风险事件 occurrence 校验已改为历史 as-of 只使用可见记录/声明，未来记录进入 warning 审计；下一步观察后续 replay/daily-run 是否继续保持无误用未来风险事件|历史 as-of 日报或 daily-run 只使用评估日可见的风险事件 occurrence、evidence 和 attestation；后续日期记录不得进入评分、仓位闸门或复核声明，也不得导致历史日流程误失败；被排除记录必须以 warning/审计记录出现在风险事件校验报告；当前可见记录自身错误仍 fail closed|2026-05-10: 新增并进入实现，原因：owner 要求测试最后一个交易日流程，估值阻断解除后，真实 `score-daily --as-of 2026-05-08` 失败于 2026-05-10 风险事件记录相对历史日的未来日期。2026-05-10: 从 IN_PROGRESS 改为 VALIDATING，原因：`score-daily --as-of 2026-05-08 --skip-risk-event-openai-precheck` 已通过，报告显式列出被排除的未来 occurrence/attestation。|
|THESIS-002|交易 thesis/初始主动 baseline|P1|BLOCKED_OWNER_INPUT|项目 owner + 系统验证|详细拆解见 `docs/requirements/confidence_lift_workplan_2026-05-10.md`；原 6 个 active baseline thesis 已写入并校验通过；扩展后的新增 ticker 仍为 `watch_only`，暂不要求主动交易 thesis；下一步需要 owner/人工复核 pending 业务指标|原 6 个主动 baseline ticker 均具备 active thesis、验证指标、证伪条件、复核频率、来源、适用边界和 owner 复核状态；校验通过；日报不再因缺少主动交易 thesis 将 `watch_only` 观察池误写成 thesis 失败|2026-05-10: 新增并进入实现，原因：owner 要求建立核心 ticker thesis，提高趋势判断到主动交易假设的可解释性。2026-05-10: 从 IN_PROGRESS 改为 BLOCKED_OWNER_INPUT，原因：MSFT/GOOG/TSM/INTC/AMD/NVDA thesis YAML 已写入，`thesis validate` PASS；`thesis review` 仍为 WATCH，因部分业务驱动指标需人工复核，系统未伪装为 confirmed。2026-05-11: owner 扩展核心观察池到 17 个代表性 ticker；本任务语义收窄为原 6 个主动 baseline thesis，新纳入 ticker 暂按 `watch_only` 处理，不新增 thesis gate 负担。|
|PROD-004|趋势判断/PIT 估值样本成熟度|P1|BLOCKED_EXTERNAL|系统验证 + 时间窗口|`DATA-003/BACKTEST-002/BTINPUT-001` 已完成 forward-only PIT 快照、回测可信度标签和覆盖诊断；下一步等待自建 PIT 样本自然积累，并用覆盖报告判断何时可提升估值/预期结论可信度|`aits backtest-pit-coverage` 能给出 core universe 的可用样本起始日期、覆盖率和 A/B readiness；`score-daily` 对 `eps_revision_90d_pct`、valuation percentile 和 vendor/self-archived PIT 来源的限制声明与实际覆盖一致；历史不足时继续降级，不得用采集日视图伪装 signal_date 可见数据|2026-05-06: 从 `PROD-001` 拆出，原因：owner 已决定不购买或伪造历史 PIT；剩余工作是持续验证 forward-only 样本成熟度，而不是继续推进总控任务。2026-05-10: owner 再次要求继续积累 forward-only PIT；本轮只刷新覆盖报告和运行状态，不回填历史 PIT。2026-05-10: 测试最后交易日流程时确认，若在 2026-05-10 对 2026-05-08 执行 live `pit-snapshots fetch-fmp-forward`，`available_time` 会晚于 as_of 并被 `ops health` 正确阻断；历史交易日应使用 `ops replay-day` 的 cache-only/隔离视图，而不是用 live daily-run 伪造 PIT。|
|PROD-005|趋势判断/规则治理 owner approval|P1|BLOCKED_OWNER_INPUT|项目 owner + 系统验证|`GOV-001/GOV-002/GOV-003` 已有 rule card、rule version 注入和受控 promotion/retirement 基础版；下一步需要 owner 对 production rule baseline、promotion 条件和 retirement 条件形成批准记录|核心 production rule card 具备 owner approval、适用范围、验证证据、已知失败模式、rollback condition 和最后复核时间；未批准候选规则不得改变正式评分、仓位 gate、日报结论或回测 production 规则；日报、回测、decision snapshot 和 trace 能追溯本次运行使用的规则版本|2026-05-06: 从 `PROD-001` 拆出，原因：规则治理工程底座已完成，但真实批准流属于 owner 决策和长期治理，不应让 `PROD-001` 继续保持进行中。|
|PROD-006|趋势判断/forward shadow 与 outcome 成熟度|P1|BLOCKED_EXTERNAL|系统验证 + 时间窗口|`SHADOW-002/SHADOW-003/REPORT-005` 已有 challenger shadow runner、样本成熟度报告和 production vs challenger 复盘入口；下一步等待真实 prediction outcome 样本达到可评估窗口|shadow maturity 报告按 candidate、horizon 和 `ai_after_chatgpt` regime 输出样本数、pending/missing、收益、胜率、回撤和 benchmark excess；样本不足时 promotion gate 维持观察或缺失状态；达到门槛前，回测或 shadow 结果不得被写成 production 规则晋级结论|2026-05-06: 从 `PROD-001` 拆出，原因：趋势判断长期可信度需要真实前向样本，但该成熟度受交易日和 label horizon 限制，适合作为独立时间窗口任务。2026-05-10: owner 要求继续积累 outcome 样本；本轮刷新成熟度/校准报告，样本不足仍保持外部时间窗口阻塞。|

## 暂缓任务

本节保留已明确暂缓、但未来可能因数据规模、owner 决策或外部条件变化而重新打开的任务。

|ID|领域|优先级|状态|下一责任方|阻塞或下一步|验收标准|备注|
|---|---|---|---|---|---|---|---|
|PROD-003|趋势判断/第二合格数据源准入|P1|DEFERRED|项目 owner + 系统验证|owner 2026-05-10 决策继续使用现有 FMP + Cboe VIX + Marketstack + FRED，暂无引入额外 macro/price qualified source 计划；系统继续用 provider health、FMP/Marketstack 价格 reconciliation、FRED series freshness 和数据质量门禁披露限制|重新打开前需 owner 明确新增宏观或价格 source、授权/成本、endpoint、缓存审计字段和长期口径；暂缓期间，趋势判断报告不得把 `macro_rates` 单一 FRED 来源写成跨源核验完成，价格仍按 FMP + Marketstack baseline 标识|2026-05-06: 从 `PROD-001` 拆出，原因：市场价格和宏观利率 reconciliation 是趋势判断可信度前置条件，但供应商选择与授权需要独立 owner 决策。2026-05-10: 从 BLOCKED_OWNER_INPUT 改为 DEFERRED，原因：owner 确认继续使用现有 FMP + Marketstack + FRED（含 Cboe VIX 专用源）且暂无引入第二个更可靠 macro/price qualified source 计划；缺口改为明确限制和报告披露，不再等待供应商选择。|
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
