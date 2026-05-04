# UI-001 证据下钻型仪表盘

状态：BASELINE_DONE

最后更新：2026-05-04

关联任务：`UI-001`、`REPORT-001`、`REPORT-002`、`REPORT-003`、`DOC-001`、`COGNITION-001`

## 背景

`REPORT-001` 已提供 evidence bundle，`REPORT-002/003` 已把日报重排为结论卡、变化原因树和证据下钻顺序，`DOC-001` 已输出结论使用等级。`UI-001` 第一阶段不再等待完整交互产品，而是做一个本地、只读、简约的静态 dashboard，把现有报告结论和实际输入数据连起来。

项目 owner 明确第一版主要使用者是自己，优先级如下：

- 先看报告结论和真实输入数据之间的联系。
- 明确论证逻辑：结论、支持证据、输入数据、质量门禁、限制条件和可改变判断的条件必须在同一页面可追踪。
- 视觉风格保持简约，不做营销式或装饰性页面。

## 第一阶段范围

- 新增 `aits reports dashboard`，从日报 Markdown、日报 evidence bundle、decision snapshot 和可选 belief_state 生成 `outputs/reports/evidence_dashboard_YYYY-MM-DD.html`。
- 页面只读展示，不改变评分、`position_gate`、回测仓位、执行建议或规则治理状态。
- 页面必须声明 `production_effect=none`，并说明 Markdown 日报和 trace bundle 仍是审计源。
- 必须保留三类读者模式：
  - 快速读者：结论卡、执行动作、最大限制、改变判断条件。
  - 投资复核者：评分组件、仓位 gate、thesis/risk/valuation 状态和仓位边界。
  - 系统审计者：claim/evidence/dataset/quality refs、trace lookup 命令、输入路径、row count、checksum 和质量报告。
- 当日报、trace bundle 或 decision snapshot 缺失时停止生成；belief_state 缺失时可以降级，但必须在页面中显示限制。

## 输入与输出

- 输入：`outputs/reports/daily_score_YYYY-MM-DD.md`
- 输入：`outputs/reports/evidence/daily_score_YYYY-MM-DD_trace.json`
- 输入：`data/processed/decision_snapshots/decision_snapshot_YYYY-MM-DD.json`
- 可选输入：`data/processed/belief_state/belief_state_YYYY-MM-DD.json`
- 输出：`outputs/reports/evidence_dashboard_YYYY-MM-DD.html`

## 验收标准

- Dashboard 顶部显示评估日期、状态、市场阶段、结论主 claim、置信度、最终仓位、执行动作和数据质量状态。
- 每个核心 claim 可以看到对应 evidence、dataset、quality refs，以及对应的输入路径和质量报告。
- 论证链必须按“结论 -> 证据 -> 输入数据 -> 质量门禁 -> 限制/闸门 -> 可改变判断条件”呈现。
- 页面支持三类读者模式，且无需启动后台服务即可浏览。
- 输出必须包含 trace lookup 命令，便于回到原始 evidence bundle 反查。
- 简约样式优先：清晰表格、紧凑布局、无装饰性视觉元素。
- 新增测试覆盖 HTML 内容和 CLI 输出；更新 `docs/system_flow.md`。

## 不在第一阶段实现

- 不做实时数据刷新、账户连接、通知推送或权限系统。
- 不新增投资结论、评分规则或自动交易动作。
- 不引入 Streamlit/Plotly 服务作为默认路径；后续若需要更复杂交互，再基于第一阶段数据契约扩展。

## 状态记录

- 2026-05-04：进入第一阶段实现。范围按 owner 偏好收窄为本地静态 evidence-first dashboard，先服务“结论与实际输入数据的联系”和“论证逻辑”。
- 2026-05-04：第一阶段达到 `BASELINE_DONE`。新增 `aits reports dashboard`，从日报 Markdown、evidence bundle、decision snapshot 和可选 belief_state 生成 `outputs/reports/evidence_dashboard_YYYY-MM-DD.html`；页面包含结论总览、论证链、三类读者模式、claim 到输入映射、dataset/quality refs 和 trace lookup 命令；`production_effect=none`，不改变评分、仓位、回测或执行建议。
- 剩余缺口：跨日报/上期对比仍主要依赖日报变化原因树和周/月报；reader mode 尚不可配置；未提供长期运行的交互式服务、浏览器内筛选、图表缩放或权限控制。
