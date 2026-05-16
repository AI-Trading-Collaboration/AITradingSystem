# 每日任务展示页面

状态：VALIDATING

最后更新：2026-05-16

关联任务：`OPS-015`

## 背景

当前 `evidence_dashboard_YYYY-MM-DD.html` 更适合作为投资结论和 evidence
下钻入口：首屏能回答当日仓位、置信度、Data Gate、主要限制和复盘摘要。
但 `aits ops daily-run` 实际包含下载、PIT、SEC、估值、日报、参数治理、
市场反馈、pipeline health 和 secret hygiene 等多个子任务。读者如果只看
dashboard，无法一眼确认每个每日任务是否完成、最重要结论是什么、还有哪些
风险需要先排查。

## 目标

新增一个每日任务展示页，作为 daily-run 的每日结论入口：

- 首屏优先汇总关键任务给出的业务结论：当日投资动作/仓位/置信度/Data Gate、
  数据可信度、参数治理、反馈复盘、运行健康和安全扫描。
- 任务执行状态、步骤数量、耗时、run id、visibility cutoff 和详细报告路径后置，
  作为审计和排错入口。
- 明确该页面只读，`production_effect=none`，不改变评分、仓位、回测或执行建议。
- 详细内容先链接到各子任务已有 Markdown / HTML 报告；后续再为重点子任务拆出
  专属网页。

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|1. 总控页面基础版|VALIDATING|新增 `outputs/reports/daily_task_dashboard_YYYY-MM-DD.html/json`，从 `daily_ops_run_metadata` 和同日报告中汇总任务状态、结论、风险和详情路径。|
|2. daily-run 接入|VALIDATING|`aits ops daily-run` 完成后自动生成该页面，并镜像到 canonical run bundle 与 legacy reports。|
|3. 关键结论优先版|VALIDATING|页面首屏改为“关键结论总览”，先汇总投资、数据、参数/反馈、运行健康和安全结论；任务状态表后置。|
|4. Shadow parameter 结论接入|VALIDATING|反馈复盘卡片展示最近一次可用 shadow parameter search 的诊断领先 trial、shadow vs production return、excess、主因、promotion 状态和阻断条件。|
|5. Shadow 参数与结果对比|VALIDATING|反馈复盘卡片展示 production/current 与 shadow candidate 的权重、gate cap 和结果对比表，明确哪些参数改变、结果差异是多少、return 如何计算；Gate cap override 与权重参数分区展示，production/current gate cap 展示回测窗口内实际默认/观测数值，且长文本已拆为数值和说明两行。|
|6. 子任务详情页路线|VALIDATING|每日任务页已提供可点击的子任务下钻入口，链接既有 Markdown/HTML/JSON 报告；后续按 PIT、SEC、valuation、score、feedback、health、secret hygiene 拆分更细网页，并保持“阶段结论、关键对比、重要风险、审计来源”的一致页面结构。|

## 边界

- 不改变 `score-daily`、仓位 gate、approved overlay、prediction ledger、回测或
  production 参数。
- 不保存 stdout/stderr 原文、API key、token 或付费供应商响应正文。
- 页面中的风险来自已生成的 daily-run metadata 和子报告状态；若子报告缺失，
  必须显示为展示层限制，而不是补造结论。

## 子任务页面统一骨架

后续各子任务专属网页不应只是把原始 Markdown 搬进 HTML。默认按同一读者顺序组织：

- 阶段性结论：该子任务今天是否完成、是否可信、是否影响当日投资判断。
- 关键数据对比：与 production/current、前一交易日、上一轮窗口或 policy floor 的差异。
- 重要风险：只列会改变读者判断、需要人工复核或阻断后续流程的限制。
- 审计来源：链接原始 Markdown/JSON/CSV、输入窗口、checksum 或运行 metadata。
- 下钻入口：从每日任务页进入子任务页，再从子任务页进入更细的原始证据。

## 状态记录

- 2026-05-16：新增任务并进入实现。原因：owner 确认需要一个每日任务展示页面，
  汇总各子任务最重要结论和重要风险；详细内容后续在子任务层面继续拆分网页。
- 2026-05-16：进入验证。原因：已实现 `aits reports daily-tasks`、daily-run
  自动生成和 legacy mirror；新增页面/CLI/run bundle 测试通过，并用
  2026-05-15 现有 daily-run metadata 生成 smoke 页面。
- 2026-05-16：从验证回到实现。原因：owner 复核页面后指出“跑各项任务的结果是
  其次”，页面首屏应汇总重要任务的关键结论信息，而不是以执行状态为主。
- 2026-05-16：再次进入验证。原因：已新增结构化 `key_conclusions`，HTML 首屏改为
  “关键结论总览”，运行状态摘要和任务执行明细后置；2026-05-15 页面已重生成。
- 2026-05-16：从验证回到实现。原因：owner 指出反馈复盘应展示 shadow parameter
  search 的关键结论，例如当前回测周期下的诊断最优参数及其相对主线收益差距。
- 2026-05-16：进入验证。原因：反馈复盘已接入最近可用 shadow parameter search
  bundle，2026-05-15 页面显示诊断领先 trial、shadow vs production return、
  excess、主因 cap、promotion 状态和阻断条件。
- 2026-05-16：从验证回到实现。原因：owner 复核指出 shadow parameter 信息仍是
  摘要句，无法看出具体哪些参数改变，需要加入 production vs candidate 对比表。
- 2026-05-16：进入验证。原因：反馈复盘卡片已新增 Shadow 参数对比表，逐项展示
  production/current 与 candidate 的 weight 和 gate cap；2026-05-15 页面已重生成。
- 2026-05-16：从验证回到实现。原因：owner 复核指出当前参数对比表横向挤压、
  换行过多，可读性不足；需要把权重参数与 gate cap override 拆开呈现，并优化
  dashboard 内的表格布局。
- 2026-05-16：进入验证。原因：参数对比表已拆成 Gate cap override 与权重参数
  两个分区，反馈复盘卡片全宽展示；2026-05-15 页面已重生成并通过 targeted
  ruff、pytest、mypy 和 diff 检查。
- 2026-05-16：从验证回到实现。原因：owner 提出其他每日子任务也需要统一考虑
  页面设计，主 dashboard 应附带子任务页面/报告链接，避免读者只能看到路径文本。
- 2026-05-16：进入验证。原因：子任务详情入口已改为卡片式下钻区，已有
  Markdown/HTML/JSON 子报告以相对链接呈现；2026-05-15 页面已重生成。
- 2026-05-16：从验证回到实现。原因：owner 指出 Gate cap override 表中
  production/current 虽然没有静态 override，但仍应展示 production 默认/实际 gate cap
  数值，便于判断 shadow candidate 是放松还是收紧。
- 2026-05-16：进入验证。原因：Gate cap override 表已从 shadow search manifest
  的 `decision_snapshot_path` 读取回测窗口内 production `position_gates`，展示实际
  cap 数值或区间；2026-05-15 页面已重生成。
- 2026-05-16：从验证回到实现。原因：owner 复核发现 Gate cap override 表中
  production/current 长文本与 shadow override 列重叠，需要拆行和调整列宽。
- 2026-05-16：进入验证。原因：production/current 单元格已拆成“实际 cap 数值”
  和“无静态 override/快照覆盖说明”两行，Gate cap override 表列宽已调整；
  2026-05-15 页面已重生成。
- 2026-05-16：从验证回到实现。原因：owner 指出参数对比表缺少结果对比，
  且页面需要解释 return 的具体计算口径。
- 2026-05-16：进入验证。原因：反馈复盘 Shadow 区块已新增结果对比表，
  展示 Total return、Max drawdown、Turnover、Beat rate 和样本覆盖，并在页面与
  JSON 中披露 return 计算口径；2026-05-15 页面已重生成。
