# ARCH-004F2 Research Lifecycle 与研究策略执行链路

最后更新：2026-07-11

## 任务信息

- task id：`ARCH-004F2_RESEARCH_LIFECYCLE_AND_EXECUTION_CHAIN`
- parent：`ARCH-004`
- priority：`P0`
- status：`BASELINE_DONE`
- owner：research platform owner / architecture coordinator
- dependency：ARCH-004E `DONE`；full parallel `5,420 passed / 0 failed`
- production effect：`none`

## Owner Intent

为当前及后续研究策略建立一份可审计、可维护的权威执行链路文档。文档不能只描述理想架构，必须逐环节对应当前真实配置、代码、CLI、schema、artifact 和报告，并解释为什么这样设计、输入输出是什么、如何计算和传播状态、当前结果支持什么结论、哪里存在优化空间以及优化需要什么证据。

## 目标交付物

主文档：`docs/research/current_research_strategy_execution_chain.md`。

文档至少包含：

1. 研究问题与假设如何登记、预注册和版本化；
2. market regime、research window、requested/actual/effective/evaluation range 与 `as_of` 如何解析；
3. 数据源、缓存、provenance、DQ/PIT gate 和缺失/过期处理；
4. universe、feature、label、signal、candidate、baseline/control 的输入输出；
5. score、backtest/PIT replay、cost、risk、robustness、holdout 与 falsification 的计算边界；
6. evidence snapshot、multi-axis state、promotion/retirement decision 如何形成；
7. canonical artifact、envelope、run ledger、report plugin 与三层报告如何消费结论；
8. daily/weekly/biweekly/monthly/event-driven review 的触发与禁止行为；
9. 当前实际研究结果、coverage limitation、blocker 与不能推出的结论；
10. 优化空间清单：问题、触发证据、允许变化、必需验证、owner decision、退出条件。

每个环节必须使用统一模板：purpose、why、owner、inputs/provenance、calculation、outputs/schema、status/failure、quality/PIT/context gate、consumers、observability、current evidence、optimization boundary。

## 实施阶段

### F2.1 Current-state inventory 与 trace

- 盘点现有 research governance、strategy research、backtest/PIT replay、scoring/promotion、paper-shadow、reporting 和 periodic review 入口；
- 从至少一个现有 reference experiment 反向追踪 spec -> runner -> evidence/decision -> report；
- 区分 canonical、legacy façade、baseline-only、blocked/deferred 和尚未接线能力；
- 不把 registry/catalog 中“已登记”误写为 runtime 已执行。

### F2.2 Authoritative execution-chain document

- 输出完整 Mermaid flow 与逐阶段 contract 表；
- 每个声明链接到真实 source/config/artifact schema；
- 明确 2022-12-01 AI market regime 与 2021-02-22 validated primary research window 不是同一语义；
- 当前结果必须注明 artifact/as-of/coverage/evidence maturity。

### F2.3 Lifecycle 与 optimization boundary

- 固化 Observation -> EvidenceSnapshot -> ReviewDecision -> preregistered ChangeProposal -> validation -> OwnerDecision -> adoption/rejection；
- periodic review 只允许生成 observation/review/change proposal，不自动调参、改权重或 promotion；
- result-visible 后不得回写 selection rule/preregistration；
- 优化候选按数据、语义、模型、成本风险、执行、报告、DevEx 分类，并写明验证与退出条件。

### F2.4 Reference integration 与 validation

- 主文档纳入 system flow、task register 和相关 report/audit navigation；
- 与 ARCH-004D reference slice、ARCH-004B semantic context、ARCH-004C platform contracts 一致；
- architecture/docs/task consistency、focused、contract/full gates 按影响范围执行。

## 验收标准

- 文档覆盖 end-to-end 研究执行链，所有环节都有输入、输出、计算、状态、owner 和优化边界；
- 至少一条真实 reference trace 可从 spec 追到 artifact/envelope/run ledger/report；
- canonical 与 legacy/未实现能力明确分栏；
- 当前研究结论与 limitation 有 source artifact，不从计划或 PASS 状态推导投资有效性；
- optimization backlog 每项有触发、证据、验证、owner、production boundary 和退出条件；
- periodic review 与 auto-tuning 明确分离；
- 不修改策略逻辑、阈值、权重、promotion、paper-shadow、production 或 broker；
- 文档与 `docs/system_flow.md`、task register、architecture policy 一致，验证通过。

## 状态记录

- 2026-07-11：研究执行链路文档 baseline 完成并通过验证：focused docs/policy=`23 passed`、architecture-fitness=`80 passed`、contract-validation=`197 passed`，最近 full baseline=`5,420/0`。主文档现可作为当前设计/计算/结果/优化边界的权威人读说明；F2.5 generic lifecycle runtime migration 仍为 `NOT_STARTED`，不得把文档完成解释为 legacy 全域迁移完成。
- 2026-07-11：F2.1/F2.2 完成，F2.3 的 lifecycle/review/optimization boundary 文档基线完成，F2.4 进入 `VALIDATING`。主文档已按 `CANONICAL|REFERENCE|LEGACY|BLOCKED|PLANNED` 区分真实状态，逐环节记录输入输出/计算/状态/DQ-PIT/context/consumer，并给出 B0～B4 实际公式、B5/B6 blocker、growth-tilt closure reference trace、当前结论与 11 类优化方向；catalog/system flow 已接入，新增文档契约测试，等待 generated architecture/docs/contract gate。
- 2026-07-11：ARCH-004E exit gate 与推送完成后登记为 `IN_PROGRESS`。Owner 新增要求研究策略的具体执行链路必须有详细设计理由、逐环节输入输出/计算逻辑及优化空间；先执行 F2.1 真实链路盘点，F1/F3 保持可执行但本轮不并行修改共享控制面。
