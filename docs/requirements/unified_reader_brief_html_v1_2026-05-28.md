# Unified Reader Brief HTML v1

最后更新：2026-06-09

关联任务：`REPORT-053`

## 背景

`REPORT-047` 已完成只读 Reader Brief 基础版，`REPORT-048` 至 `REPORT-052`
补齐了 calculation explainers、score change attribution、research governance、
report registry 和 documentation contract。下一阶段应把这些治理 artifact
产品化为真正的每日阅读首页，而不是让读者继续在多个 dashboard 和 Markdown
报告之间拼上下文。

本任务在现有 `aits reports reader-brief` 基础上增强 HTML / JSON，不新建第二套
Reader Brief。

## 目标

生成统一读者入口：

- `outputs/reports/reader_brief_YYYY-MM-DD.html`
- `outputs/reports/reader_brief_YYYY-MM-DD.json`

读者每天先读 Reader Brief，再按需要打开详细报告。页面顺序必须遵循：

1. 先给结论。
2. 再给证据。
3. 再给计算逻辑。
4. 最后给详细链接。

## CLI

- `aits reports reader-brief --date YYYY-MM-DD`
- `aits reports reader-brief --as-of YYYY-MM-DD`
- `aits reports reader-brief --latest`

`--latest` 从默认 decision snapshot 目录选择最新 signal-date snapshot。`--latest`
和显式 `--date/--as-of` 不能同时使用。

## 必要区块

|区块|状态|说明|
|---|---|---|
|Executive Summary|IN_SCOPE|展示 market regime、核心结论、score/rank/action 变化、`production_effect` 和人工复核数量。|
|Market Situation|IN_SCOPE|读取现有 daily artifacts；缺 benchmark/sector/ticker 明细时必须显示 LIMITED，而不是补造市场结论。|
|Score & Decision Funnel|IN_SCOPE|展示 raw/component/overall/gate/final position 路径；关键数字用 `<details>` 展开 formula、source artifact、PIT、freshness、common misread 和 `production_effect`。|
|Backtest / Outcome Status|IN_SCOPE|优先从 research governance/report index 读取 latest backtest、robustness、parameter replay 和 outcome tracking 状态。|
|Shadow / Weight Iteration Status|IN_SCOPE|展示 observe-only、SEC PIT shadow monitor、weight candidate/promotion 状态，确认 `production_effect=none`。|
|Data Quality & PIT Safety|IN_SCOPE|展示 data gate、market/feature/SEC quality、PIT/degraded/stale 状态。|
|Task Cadence Calendar|IN_SCOPE|按 daily、weekly、bi-weekly、monthly、ad hoc research 分组 report index，展示 latest run、status、artifact、owner action。|
|Manual Review Queue|IN_SCOPE|汇总 snapshot manual review、data gate blocking、stale/missing reports、research governance manual review、documentation contract issues。|
|Report Navigation|IN_SCOPE|链接 evidence dashboard、daily task dashboard、SEC PIT/report governance/documentation contract/artifact catalog 等详细报告。|

## 边界

- Reader Brief 必须只读，不运行 scoring、backtest、SEC PIT、shadow、weight 或 docs
  上游命令。
- 缺少可选 artifact 时降级为 `MISSING` / `LIMITED` / `PASS_WITH_WARNINGS`。
- 每个 production-sensitive 区块必须显示 `production_effect`。
- HTML 应是可读报告，不是 JSON 表格倾倒；复杂计算逻辑必须默认折叠。
- 输出仍为中文；ticker、schema 字段、status code 和 artifact 名保持 English。

## 验收标准

- JSON 包含 `task_cadence_calendar`、`manual_review_queue`、`report_navigation` 和
  calculation detail metadata。
- HTML 包含 Executive Summary、Score & Decision Funnel `<details>`、Task Cadence
  Calendar、Manual Review Queue 和 Report Navigation。
- CLI 支持 `--date` 和 `--latest`。
- 测试覆盖 JSON schema、HTML generation、missing artifact degraded mode、read-only
  source links、`production_effect` display、manual review queue 和 cadence calendar。

## 进展记录

- 2026-05-28：新增 REPORT-053 并进入 IN_PROGRESS。实现方向限定为在现有
  `reader_brief` 上做 HTML v1 产品化增强，不新建平行报告入口。
- 2026-05-28：实现完成并进入 VALIDATING。`reader_brief` 新增 `--date` alias、
  `--latest` snapshot 解析、documentation contract 输入、Executive Summary、
  Task Cadence Calendar、Report Navigation、documentation contract summary、report
  freshness / research governance / docs contract manual review items，以及 Score &
  Decision Funnel `<details>` 展开解释；验证覆盖目标测试、ruff 和 black。
- 2026-06-09：从 `VALIDATING` 改为 `DONE`。latest 真实 artifact 复核通过：
  `aits reports reader-brief --date 2026-06-05` 与 `--latest` 均生成
  `reader_brief_2026-06-05.html/json`，状态 `OK`、warnings=0、
  `production_effect=none`；JSON 字段级复核确认 `task_cadence_calendar`
  覆盖 5 个 cadence groups / 166 个 registry reports，`manual_review_queue`
  items=53，`report_navigation` items=142 / groups=4，documentation contract
  status=`PASS`，calculation explainers source=`AVAILABLE`。HTML 包含 Executive
  Summary、Market Situation、Score &amp; Decision Funnel、Task Cadence Calendar、
  Manual Review Queue 和 Report Navigation，`<details>` count=424，并保留
  `production_effect` 展示和详细 artifact links。专项测试、Reader Brief
  quality、documentation contract 和 docs freshness 均通过。
