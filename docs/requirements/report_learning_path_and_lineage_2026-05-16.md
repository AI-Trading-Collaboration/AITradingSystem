# 报告学习链路与产物目录

最后更新：2026-05-16

## 背景

当前系统已经有完整审计链路：`docs/system_flow.md` 记录从数据输入到评分、仓位 gate、日报、trace、decision snapshot、prediction ledger、shadow 参数搜索和反馈校准的工程事实。但该文档体量较大，更适合审计和维护，不适合作为使用者第一次理解系统的入口。

本任务目标不是增加新的投资规则，而是把已有审计信息重组为学习链路，让使用者能回答：

- 这份报告由哪个命令生成；
- 使用了哪些关键输入；
- 输出了哪些 artifact；
- 分数如何变成最终仓位；
- 输入数据如何一步步计算成输出数据；
- 每个计算步骤的原因、直觉和设计取舍；
- 哪个 gate 真正限制了最终仓位；
- shadow / validation-only 结果为什么不影响 production；
- 遇到字段或文件时应该去哪里复核。

## 范围

本任务只改变文档和报告解释层，不改变 production scoring、position gate、effective weight resolver、backtest exposure、prediction ledger 写入语义或 approved overlay。

## 阶段拆解

### 阶段 1：学习入口与产物目录

状态：VALIDATING

交付：

- 新增 `docs/learning_path.md`，按使用者学习顺序解释数据门禁、特征、评分、仓位 gate、日报、trace、ledger、shadow 和 feedback。
- 新增 `docs/artifact_catalog.md`，按 artifact 说明生成者、上游输入、关键字段、下游使用、production_effect 和常见误解。
- 在 README 和 `docs/system_flow.md` 中增加学习入口引用。

验收：

- 使用者能从一个文档入口知道每日链路该按什么顺序读。
- 关键生产、审计和 validation-only 产物不再只散落在系统流图中。

### 阶段 2：日报学习卡片

状态：VALIDATING

交付：

- 日报增加 `Data Lineage Card`，展示生成命令、关键输入、关键输出、trace 和 production_effect。
- 日报把现有 `Base Signal / Risk Caps` 扩展为 `Score-to-Position Funnel`，按顺序解释 component score、effective weights、overall score、score band、confidence、macro risk budget、position gates 和 final position。
- 日报增加 `Binding Gate Ladder`，明确每个 gate 的 cap、触发状态、证据来源、对最终仓位的影响和 binding gate。
- 日报增加 `如何复核今天的结果` 五问，指导使用者从 data gate、binding gate、关键证据和 invalidator 入手复核。

验收：

- 日报第一屏附近能说明它从哪里来、到哪里去。
- 不读完整系统流图也能理解 score 如何映射到 final position。
- 最严格 gate 和非 production 边界清晰可见。

### 阶段 3：字段和 shadow 解释

状态：VALIDATING

交付：

- 新增字段解释字典，优先覆盖 `scores_daily.csv`、decision snapshot、trace、prediction ledger 和 parameter search trial。
- 参数搜索报告顶部新增 Trial Card，复用已实现的 factorial attribution、cap-level attribution、position change attribution 和 promotion contract 结论。
- 统一 `production_effect` 标签说明，降低 validation-only 被误读为 production 建议的风险。

验收：

- 关键字段有可追溯定义、上游来源、下游用途和常见误解。
- Top trial 的“改了什么、为什么领先、为什么不能上线”在报告顶部可见。

### 阶段 4：零金融背景计算逻辑

状态：VALIDATING

交付：

- 新增 `docs/calculation_logic.md`，面向无金融背景读者解释价格、收益率、移动平均、相对强弱、VIX、利率、美元、基本面、估值、风险事件、confidence、gate 和 position。
- 按输入、输出、计算逻辑、原因、设计思路和常见误解解释数据质量门禁、feature building、signal normalization、component score、effective weights、overall score、model position、confidence、macro risk asset budget、position gates、日报/snapshot/trace/ledger 和 shadow 参数搜索。
- 在 README、`docs/learning_path.md`、`docs/artifact_catalog.md` 和 `docs/system_flow.md` 增加入口链接。

验收：

- 使用者不读代码也能理解输入数据如何计算成输出数据。
- 文档明确 score、confidence、gate 和 final position 的分层原因。
- 文档说明 shadow / parameter search 的计算结果为什么仍是 validation-only。

### 阶段 5：解释命令和 dashboard 分层

状态：READY

交付：

- 基于字段字典和 trace bundle 增加 `aits explain` 的只读解释入口。
- Evidence dashboard 进一步分为快速读者、系统理解、审计排障三层读者模式。
- 评估是否逐步把 `docs/system_flow.md` 拆成 `docs/flows/*` 子流程文档。

验收：

- 命令行可以回答字段、日期、gate 和 artifact 的来源解释。
- dashboard 的读者模式不改变 Markdown 日报和 trace bundle 的审计责任。

## 开放问题

- `aits explain` 是否先做静态文档查询，还是直接解析 trace bundle，需要等字段字典稳定后决定。
- `system_flow.md` 拆分前需要避免总览和子流程形成双源维护。

## 进展记录

- 2026-05-16：新增任务和拆解文档，开始阶段 1 和阶段 2。范围限定为学习文档和报告解释层，不改变 production scoring、position gate、approved overlay、正式 prediction ledger 或回测仓位。
- 2026-05-16：阶段 1-3 基础实现进入 VALIDATING。新增 `docs/learning_path.md`、`docs/artifact_catalog.md` 和 `docs/schema/fields.yaml`；日报新增 `Data Lineage Card`、`Score-to-Position Funnel`、`Binding Gate Ladder` 和复核五问；shadow parameter search 报告新增 `Trial Card`。同步更新 README 与 `docs/system_flow.md`。验证通过 `tests/test_daily_scoring.py` 26 passed、`tests/test_shadow_weight_profiles.py` 14 passed、`ruff check src tests` 和 `git diff --check`；`docs validate-freshness` 仍因 11 个既有无关需求文档 stale/missing metadata 失败，本任务新增需求文档已含 `最后更新`。
- 2026-05-16：根据 owner 反馈补充阶段 4，要求面向无金融背景读者说明输入数据到输出数据的计算逻辑、原因和设计思路；范围仍限定为文档学习层，不改变任何 production 计算。
- 2026-05-16：阶段 4 基础实现进入 VALIDATING。新增 `docs/calculation_logic.md`，并从 README、`docs/learning_path.md`、`docs/artifact_catalog.md` 和 `docs/system_flow.md` 建立入口；验证通过 `tests/test_daily_scoring.py` + `tests/test_shadow_weight_profiles.py` 共 40 passed、`ruff check src tests` 和 `git diff --check`；`docs validate-freshness` 仍因 11 个既有无关需求文档 stale/missing metadata 失败，本任务新增/更新文档不在失败清单中。
