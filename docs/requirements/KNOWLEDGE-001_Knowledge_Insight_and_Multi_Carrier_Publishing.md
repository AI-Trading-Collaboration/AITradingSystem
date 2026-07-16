# KNOWLEDGE-001 知识解释核心与多载体发布系统

最后更新：2026-07-12

## 任务信息

- parent task：`KNOWLEDGE-001_KNOWLEDGE_AND_INSIGHT_CORE`
- child tasks：`DOCS-PLATFORM-001_OBSIDIAN_AUTHORING_AND_GOVERNANCE`、`PUBLISHING-001_MULTI_CARRIER_PUBLISHING`、`PLATFORM-UX-001_SYSTEM_UNDERSTANDING_WORKBENCH`
- priority：Knowledge Core 为 `P1`；authoring/publishing adapters 为 `P2`；investment-facing workbench 保持 `P1`
- status：`PROPOSED`
- owner：project owner / knowledge owner / architecture coordinator / reporting and UX owner
- architecture parent：`ARCH-004`
- production effect：`none`

## 最新架构决策

“文档管理系统”不足以表达长期目标。后续正式采用以下逻辑分层：

```text
Data Foundation
  facts / snapshots / DQ / PIT / lineage
            |
            v
Knowledge & Insight Core
  concepts / explanations / claims / evidence / limitations / versions
            |
            v
Publishing & Experience
  Obsidian / docs site / Reader Brief / Dashboard / interactive web / PDF

Shared control planes
  contracts / operations / permissions / architecture fitness / lifecycle
```

Data Foundation 是事实生产者；Knowledge & Insight Core 是事实解释与研究知识组织层；Publishing & Experience 是多种阅读、探索和归档客户端。当前保持 monorepo 和统一 CI，只做职责、contract 与 owner 分离；只有出现稳定 API、独立团队/权限、跨项目复用或远程数据基础设施需求后，才重新评估物理 repository/service 拆分。

本计划是 `ARCH-004` 的知识与体验 addendum，不建立第二套 architecture、system status、artifact catalog、report registry 或 control plane。

## Knowledge Object

载体无关的知识对象至少需要表达：

- `knowledge_id`、type、title、summary、audience、owner；
- lifecycle status：`DRAFT|CURRENT|SUPERSEDED|ARCHIVED|BLOCKED`；
- `valid_from`、`valid_to`、`last_reviewed_at`、review cadence；
- concept、explanation、claim、limitation、common misread、next question；
- `dataset_id`、`snapshot_id`、`artifact_id`、`evidence_id`、`DataQualityEvidence` 引用；
- as-of、market regime、requested/effective/evaluation range、policy/config/code version；
- `related_to`、`derived_from`、`supports`、`contradicts`、`supersedes` 关系；
- canonical source、renderer targets 和 publication status。

机器事实继续来自 typed contracts/artifacts；Markdown 可以承载人工解释，但不能手写或覆盖 DQ PASS、run status、lineage、score、gate 或 promotion state。发布器只投影已有事实和知识对象，不在展示层重算投资结论。

## 载体定位

|载体|主要用途|事实源边界|
|---|---|---|
|Obsidian|研究人员编写、关联、复核与导航|Git Markdown client；不是独立数据库或 source of truth|
|用户文档站|新使用者学习、主题阅读、搜索和稳定导航|由同一 Knowledge Core 构建|
|Reader Brief / Dashboard|当前运行、决策和行动摘要|消费 canonical report/artifact contracts|
|交互式网页|比较股票、策略、指标、窗口和证据|消费 deterministic read model，不改源事实|
|PDF/冻结报告|某时点正式判断、审计与外部归档|绑定 snapshot、evidence 和 checksum，不可变|
|Git/CI|版本、review、质量门禁和发布记录|authoritative maintenance workflow|

## 分阶段任务

### K0：知识资产盘点与信息架构

- 盘点 user guide、learning path、calculation logic、system flow、runbook、requirements、research、Reader Brief、Dashboard、report registry 和 artifact catalog；
- 区分 USER_GUIDE、CONCEPT、CALCULATION、CURRENT_CONCLUSION、RESEARCH_HISTORY、OPERATION、REQUIREMENT、AUDIT_EVIDENCE；
- 明确首次使用者/阅读者、Owner、Operator、Researcher、Developer 的入口、任务式学习路径与典型问题；
- 建立 topic/MOC、current conclusion 与 history/supersession 导航，不把目录结构当作知识关系本身；
- 记录 duplicate basename、orphan、broken link、missing owner、stale current conclusion 和 audience gap。

### K1：Knowledge & Insight Core contract

- 定义 versioned Knowledge Object、关系与 lifecycle schema；
- 复用 `ArtifactEnvelope`、`DataQualityEvidence`、ResearchEvaluationContext、ReportSpec 和既有 evidence/trace contracts；
- 建立机器字段与人工解释的明确边界，以及 source/checksum/as-of drift 检查；
- 将 `DOCS-LEARN-001`、`DOC-001`、`REPORT-048/049/052` 的既有能力作为种子，不重做 `aits explain`、report registry 或 documentation contract；
- CURRENT 被替代时必须显式指向 successor，不能靠文件名或目录猜测当前结论。

### K2：Obsidian-compatible authoring 与文档治理

由 `DOCS-PLATFORM-001_OBSIDIAN_AUTHORING_AND_GOVERNANCE` 承接：

- Git + standard Markdown 保持权威；Obsidian 只作为编辑、backlink 和 graph 客户端；
- 默认使用标准 Markdown links，避免生产内容依赖 Obsidian-exclusive syntax/plugin；
- 定义 `.obsidian` 策略：本地 workspace 默认 ignore，只有经 review 的最小共享配置才可提交；
- 提供 MOC、metadata template、owner/review/supersedes 约定；
- 兼容当前文档 freshness validator；若迁移 YAML properties，必须先迁移 validator，不能静默失去 `最后更新` 检查；
- 对 broken links、orphan、duplicate basename、missing metadata 和 stale CURRENT 建立 CI gate 或分级报告。

### K3：Multi-carrier publishing

由 `PUBLISHING-001_MULTI_CARRIER_PUBLISHING` 承接：

- 从同一知识核心生成 Obsidian navigation、用户文档站、Reader Brief links、Dashboard drilldown 和冻结 PDF/report index；
- renderer contract 必须可确定性复算，输出保留 knowledge/source/evidence ids；
- 静态文档站优先于长期在线服务，先解决学习路径、搜索、主题和版本导航；
- 接管 `REPORT-052` 尚未实施的 generated docs index、归档与 freshness integration，不保留第二个平行 documentation roadmap；
- 不复制 `docs/system_flow.md` 为第二事实源；需要多视图时从受治理 fragment/contract 生成并校验。

### K4：Interactive Publishing & Experience

由 `PLATFORM-UX-001_SYSTEM_UNDERSTANDING_WORKBENCH` 承接：

- 它是 Publishing & Experience 的交互式客户端，不是 Knowledge Core、Data Foundation 或第二套 status/control plane；
- 复用 `aits system status`、Evidence Dashboard、typed reporting 和 Knowledge Object；
- 先交付只读 static HTML/read model，再评估交互比较、timeline、lineage、run diff 和带引用问答；
- 任意 claim 缺 source/as-of/regime/range/DQ/PIT/limitations 时显式 `LIMITED|BLOCKED`。

## 与既有计划的关系及冲突处置

最新 owner 讨论对未来路线具有优先级，但不篡改已完成任务的历史状态与当时验收证据：

1. **“文档管理系统”术语被取代**：后续上位概念统一为 Knowledge & Insight Core；文档只是载体之一。
2. **`ARCH-004F3` 不被推翻**：其 Owner Daily / Research Review / Audit Index 是 Publishing & Experience 中的三类 reporting surface，不是本计划所说的三层系统架构。
3. **`PLATFORM-UX-001` 重新定位**：由泛化的“统一理解系统”收窄为 Knowledge Core 的交互式 publishing client；其 view model 不成为事实源。
4. **`REPORT-052` 后续被本计划继承**：已完成 documentation contract 保留 `DONE`；尚未实施的 generated index、月度归档和 freshness integration 转入 K2/K3。
5. **历史“不继续扩展 report/dashboard”仅是阶段性优先级**：`TRADING-487_to_504` 和 `UI-002` 的当期边界仍有效，但不构成长期禁止网页、可视化或知识系统的决定。
6. **Obsidian 不成为权威数据库**：不启用双向同步来绕过 Git review，不将 wikilink/plugin metadata 设为唯一可读格式。
7. **`docs/system_flow.md` 继续是流程 source of truth**：在 generator/validator 切换完成前不手工拆出第二份权威流程图。
8. **逻辑分离先于物理拆分**：近期保持 monorepo；不得仅为组织目录而提前拆 repository/service。

## 验收标准

- 任一 CURRENT investment-facing knowledge object 都能追溯到 source artifact、evidence、as-of、regime/range、DQ/PIT、policy 和 limitations；
- 数据事实、人工解释和 presentation state 在 schema 与 owner 上可区分，网页/Markdown 不能覆盖机器事实；
- 同一知识对象可确定性发布到至少 Obsidian-compatible Markdown 与用户文档站，关键 id/引用一致；
- Obsidian workspace 不产生未治理的 repository noise，标准 Markdown reader 仍能完整阅读；
- current/superseded/history 关系明确，使用者不会把旧研究结论误读为当前结论；
- 首次使用者能够从任务式入口找到“系统是什么、如何阅读当前结论、如何追溯证据、如何安全运行”的路径；以 findability、time-to-answer 和旧结论误读测试验证，而不只检查目录存在；
- broken link、orphan、duplicate、missing owner、stale current knowledge 都可被检测并进入维护队列；
- `PLATFORM-UX-001` 的 investment-facing claim 100% 可下钻，且不重算 score/gate/backtest/promotion；
- static site、Dashboard、Reader Brief、PDF 等 renderer 共享 contract，不形成多套手工同步内容；
- 实现新增 module/CLI/schema/report 时同步更新 `docs/system_flow.md`、registry/catalog、runbook 和相关 tests；
- 所有输出保持 `production_effect=none`、`broker_action=none`，除非未来另有经 owner 审批的独立任务。

## 开放问题

- K0 首期优先服务 Owner、Operator 还是新研究者；
- 哪些 Markdown 属于 canonical human-authored explanation，哪些应完全由 artifacts 生成；
- 静态站点生成器和搜索方案的技术选择；
- 哪些交互图表具有足够使用频率，值得进入 K4，而不是留在静态报告；
- PDF 是否只覆盖正式 owner decision，还是也覆盖周期研究综述；
- 何时具备跨 repository/service 拆分条件。

这些开放问题影响具体实现选择，但不改变三层边界、单向事实流和 Git/contract 权威原则。

## 状态记录

- 2026-07-12：根据 owner 关于 Obsidian、使用者阅读、独立长期维护、数据/知识分离和网页可视化的连续讨论登记为 `PROPOSED`。本次只冻结架构、task breakdown、冲突处置和验收边界，不创建 vault、网站、服务、scheduler 或新运行时事实源。
