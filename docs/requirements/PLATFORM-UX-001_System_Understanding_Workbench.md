# PLATFORM-UX-001 系统认知工作台

最后更新：2026-07-12

## 任务信息

- task id：`PLATFORM-UX-001_SYSTEM_UNDERSTANDING_WORKBENCH`
- priority：`P1`
- status：`PROPOSED`
- owner：project owner / architecture coordinator / reporting and UX owner
- dependency：`KNOWLEDGE-001_KNOWLEDGE_AND_INSIGHT_CORE`、`PUBLISHING-001_MULTI_CARRIER_PUBLISHING`；完整 ARCH-004G2.4 handoff PASS -> ARCH-005 S0/S1 PASS -> 新显式恢复指令 -> ARCH-004G2.5 parallel readiness -> ARCH-004G3 native reporting；native cut-in 依赖 ARCH-004H
- production effect：`none`

## 背景

系统已具备 `aits system status/doctor`、Evidence Dashboard、Reader Brief、learning path、calculation logic、artifact catalog、`aits explain`、ResearchEvaluationContext、ArtifactEnvelope、WorkflowSpec/RunLedger、ReportSpec、module/test manifests 和 task register，但这些能力仍分布在大量命令、报告、配置与文档中。

本任务不是重做这些事实源，而是建立一个只读的系统认知与解释入口，把使用者关心的“系统正在做什么、为什么这样做、结果如何、能否信任、哪里阻塞、后续如何改进”组织为可逐层下钻的统一视图。

根据 2026-07-12 最新 owner 讨论，本工作台正式定位为 **Publishing & Experience 的交互式客户端**。Data Foundation 生产事实，Knowledge & Insight Core 管理概念、解释、claim、evidence、limitation、版本与替代关系；本工作台只消费这两层的 canonical contracts 和 knowledge objects，不承担知识事实源、文档数据库或数据控制面职责。完整分层与多载体发布计划见 `docs/requirements/KNOWLEDGE-001_Knowledge_Insight_and_Multi_Carrier_Publishing.md`。

## 权威边界

- `aits system status` 继续作为 canonical first screen；不得创建第二套 system status/control plane；
- Knowledge & Insight Core 继续作为载体无关的解释与知识关系来源；工作台不得建立只在 UI 中存在的 claim、limitation、current/superseded 状态或 evidence mapping；
- 工作台只读取 canonical artifacts、contracts、run ledgers、decision traces、policies 和 task register，不在展示层重算 score、gate、backtest、promotion 或 owner decision；
- 每个结论必须保留 `as_of`、market regime、requested/effective/evaluation range、DQ/PIT/freshness、limitations、production effect 和 source refs；
- 缺 typed provenance 的区域必须标为 `LEGACY|LIMITED|BLOCKED`，不得由 UI 或 LLM 猜测；
- 默认本地、只读、无后台服务依赖；不新增 scheduler、账户连接、broker action 或自动修复。

## 核心用户问题

1. 系统现在在做什么：当前状态、due/running/blocked workflow、最近运行时间线和安全边界；
2. 为什么得到这个结果：component、policy、evidence、confidence、binding gate 和 decision trace；
3. 结果如何：结论、实际覆盖、outcome、与上期/上次运行的差异；
4. 是否可信：数据质量、PIT、freshness、样本、限制、验证状态和适用边界；
5. 哪里阻塞：按根因聚合 missing/stale/warning，同时保留完整明细；
6. 后续如何改进：区分数据缺口、研究假设、工程债、owner decision、所需证据和责任人；
7. 系统如何组成：component、workflow、config、CLI、artifact、decision、test 和 owner 的可追溯关系。

## 分阶段计划

### U0：Source inventory 与用户任务基线

- 消费 KNOWLEDGE-001 K0/K1 的知识资产 inventory 和 Knowledge Object contract，不建立第二份 inventory；
- 盘点并复用 `system status`、Evidence Dashboard、Owner Daily、Research Review、Audit Index、artifact lineage 和 explainers，记录工作台特有的 projection coverage；
- 定义 Owner、Operator、Researcher、Developer 的核心任务，但所有角色共享同一事实源；
- 记录当前完成典型理解任务所需的入口、步骤、时间和常见误读，作为可用性基线。

### U1：`SystemUnderstandingViewModel`

定义 versioned read model，作为 Knowledge Object 与 canonical runtime contracts 的只读 projection，至少覆盖：

- component / workflow / run / artifact；
- decision / evidence / dataset / quality；
- policy / gate / owner / lifecycle；
- blocker / warning / improvement task；
- source path、checksum、schema/version、as-of 和 lineage edges。

该 view model 只能投影已有事实，不得成为新的投资结论来源。

### U2：本地静态 MVP

- 扩展现有 canonical status/reporting surface，生成本地静态 HTML；
- 第一屏展示当前状态、主要原因、结果、可信度、关键 blocker、下一动作和安全边界；
- 支持从结论下钻到 evidence、dataset、quality、policy、artifact 和实现入口；
- 不新增新的 status 命令，不要求启动长期服务。

### U3：运行、决策与架构下钻

- workflow timeline 与失败传播；
- decision explorer 与 score-to-position/gate 解释；
- run/as-of/config/policy/artifact diff；
- architecture/lineage explorer 与 change impact；
- root-cause grouped blocker 和 task-register improvement navigation。

### U4：可选的带引用问答

- 仅在 deterministic view model 和 source coverage 足够后评估；
- 每个回答必须引用 canonical source；
- LLM 只负责检索和表达，不可写入 task、policy、weight、production state 或 broker/order；
- 无证据时必须回答 `LIMITED|BLOCKED`，不能补全推断。

## 验收标准

- 使用者能从单一入口回答“现在、为什么、结果、可信度、问题、改进”六类问题；
- 100% investment-facing claims 可下钻到 source artifact/trace/policy，并披露 as-of、regime、range、DQ/PIT/freshness 和 limitations；
- `SystemUnderstandingViewModel` versioned、deterministic、可 round-trip，source drift 和 missing provenance fail closed；
- blocker 聚合保留完整明细，不把 validation PASS 误写为 candidate/promotion/production ready；
- Owner/Operator/Researcher/Developer 只改变排序和层级，不产生不同事实；
- 首版为 read-only static HTML，`production_effect=none`、`broker_action=none`；
- 报告层不重算投资结论，不新增 scheduler，不自动执行修复；
- usability task tests、report-validation、architecture-fitness、contract-validation 和相关 focused tests PASS。

## 治理要求

- 初期不创建黑箱“系统健康综合分”或“改进优先级总分”；如后续需要，必须进入带 owner、version、rationale、validation 和 review condition 的 policy manifest；
- 直接读取 cached market/macro data 时必须经过 `aits validate-data` 或同源 code path；优先读取既有 DQ evidence；
- 实现新增 CLI、artifact/schema、report consumer 或运行链时，同步更新 system flow、artifact catalog、report registry、task register 和相关 runbook；
- 不能复用已归档 `UI-001` 的 task id；本任务是现有 evidence dashboard/learning/reporting 能力的统一产品化，而不是恢复旧任务状态。
- 本任务不替代 `KNOWLEDGE-001` 或 `PUBLISHING-001`；Obsidian、用户文档站、PDF 与本工作台应复用同一知识标识和 evidence refs，而不是分别维护文案真值。

## 状态记录

- 2026-07-12：根据 owner 关于降低系统理解和使用成本的讨论登记为 `PROPOSED`。当前只记录需求和边界，不在 ARCH-004G2 在途迁移中实现 UI；下一步是 owner/architecture/reporting 共同复核 U0/U1 范围及与 G3/H 的切入时点。
- 2026-07-12：根据最新三层架构讨论，将本任务明确收敛为 Publishing & Experience 的交互式客户端；其原有只读、deterministic、no-recompute 边界保留，知识对象、Obsidian 和多载体 publishing 由 KNOWLEDGE-001/PUBLISHING-001 承接。此前可能把工作台理解为完整知识系统的范围表述，以本次定位为准。
