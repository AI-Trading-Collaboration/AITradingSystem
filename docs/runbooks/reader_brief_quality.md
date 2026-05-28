# Reader Brief Quality Runbook

最后更新：2026-05-28

## 目的

`aits reports validate-reader-brief` 用于校验既有 Reader Brief 是否满足读者入口质量要求。
它只读取 `reader_brief_YYYY-MM-DD.json`，不生成或重跑任何上游报告。

## 手动运行

```powershell
python -m ai_trading_system.cli reports reader-brief --latest
python -m ai_trading_system.cli reports validate-reader-brief --latest
```

指定日期：

```powershell
python -m ai_trading_system.cli reports reader-brief --date 2026-05-27
python -m ai_trading_system.cli reports validate-reader-brief --date 2026-05-27
```

## 输出

- `outputs/reports/reader_brief_YYYY-MM-DD.html`
- `outputs/reports/reader_brief_YYYY-MM-DD.json`
- `outputs/reports/reader_brief_quality_YYYY-MM-DD.json`
- `outputs/reports/reader_brief_quality_YYYY-MM-DD.md`

## 状态解释

- `OK`：Reader Brief 具备 narrative、impact layer、manual review grouping、
  contribution summary、market panel 状态、grouped navigation 和
  `production_effect=none`。
- `PASS_WITH_WARNINGS`：核心质量检查通过，但仍有 warning 需要读者留意。
- `LIMITED_READER_CONTEXT`：Reader Brief 可读，但缺失重要上下文，例如 score change
  attribution、research governance summary、runtime report index 或必需阅读报告。
- `FAILED`：Reader Brief JSON 缺少核心质量字段，或 production boundary 不满足
  `production_effect=none`。

## 安全边界

该命令固定 `production_effect=none`，不运行 scoring、backtest、shadow、SEC PIT、
weight iteration、docs contract 上游，也不生成交易指令。`LIMITED_READER_CONTEXT`
是阅读上下文受限，不是自动投资动作。
