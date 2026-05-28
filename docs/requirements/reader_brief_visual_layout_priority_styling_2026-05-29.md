# Reader Brief Visual Layout & Priority Styling

最后更新：2026-05-29

关联任务：`REPORT-057`

## 背景

`REPORT-056` 已补齐 research governance summary，Reader Brief 现在可以覆盖市场、
今日结论、score change、计算逻辑、position gate、data/PIT、manual review、report
freshness、backtest/shadow/SEC PIT/weight governance 和 `production_effect` 边界。

当前剩余问题主要是 HTML 阅读形态：信息结构完整，但页面仍偏表格报告，不利于每日
快速扫描。

## 目标

只增强现有 `outputs/reports/reader_brief_YYYY-MM-DD.html` 的可读性和视觉优先级。
不新增数据源，不改变 JSON 业务语义，不修改 scoring、backtest、production weights、
shadow weights、score configs、promotion gate 或 trading decisions。

## 范围

- 继续使用现有 `aits reports reader-brief --date/--latest`。
- Reader Brief 保持只读，只消费既有 artifact。
- HTML 首屏应像每日投资简报：先展示结论、仓位、约束、市场变化、人工复核和安全边界。
- 明细、路径、source artifacts、PIT policy、common misread 和 navigation source 继续放在
  `<details>` 中，保留审计能力。

## 必要布局改进

1. 顶部 summary cards：
   - final action
   - final AI position
   - binding gate
   - market movement
   - manual review count
   - `production_effect`
2. status badges：
   - `OK`
   - `PASS_WITH_WARNINGS`
   - `LIMITED_READER_CONTEXT`
   - `BLOCKED_BY_MISSING_ARTIFACTS`
   - `NOT_PROMOTABLE`
   - `production_effect=none`
3. Market Situation：
   - SPY / QQQ / SMH / SOXX / VIX / DGS10 以紧凑 cards 展示可用项。
   - 1D / 5D / 20D 清晰展示。
   - 保留详细表格作为 fallback / audit 明细。
4. Score & Decision Funnel：
   - 展示 `score -> raw position -> confidence adjustment -> gate cap -> final position`。
   - 高亮 binding gate。
5. Manual Review Queue：
   - critical / warning / info 分组着色。
   - `recommended_next_action` 要突出显示。
6. Missing / Limited Artifact Impact：
   - `impact_level` 以 badge 展示。
   - blocking / important / optional / info 分区。
7. 审计明细保持可折叠：
   - full paths
   - source artifacts
   - PIT policy
   - common misread

## 验收标准

- Reader Brief HTML 包含 top summary cards。
- status badge 覆盖主要状态和 `production_effect=none`。
- binding gate 在 funnel / gate ladder 中被高亮。
- Market Situation 有 proxy cards，并保留详细表格。
- Manual Review Queue 按 severity 可扫描，recommended action 突出。
- Missing / Limited Artifact Impact 按 impact level 分区并显示 badges。
- `<details>` 审计信息仍可用。
- HTML 生成稳定、确定性输出；相关测试通过。

## 进展记录

- 2026-05-29：新增并进入 `IN_PROGRESS`。原因：REPORT-056 后 Reader Brief 内容闭环已成型，
  下一步不新增数据，只改善 HTML 每日阅读扫描效率。
- 2026-05-29：进入 `VALIDATING`。已完成首屏 summary cards、status badges、
  SPY/QQQ/SMH/SOXX/VIX/DGS10 market cards、Score -> raw position -> confidence
  adjustment -> gate cap -> final position funnel flow、binding gate 高亮、manual
  review severity 分组、Missing / Limited Artifact Impact impact 分区，并保留
  `<details>` 审计信息。验证覆盖 Reader Brief 目标测试、全量 pytest、ruff、black、
  `reader-brief --latest` 和 `validate-reader-brief --latest`。
