# 报告可追溯性需求

状态：DONE

最后更新：2026-05-04

关联任务：`REPORT-001`

## 背景

日报、回测报告和输入审计报告已经声明数据质量、输入来源、评分模块和市场阶段，但核心结论仍主要停留在自然语言和表格层。为了把投资解释转成可审计对象，最终报告需要稳定引用 claim、evidence、dataset、quality 和 run manifest，使读者可以从报告结论反查到输入上下文、数据快照和质量门禁。

## 第一版范围

本任务第一版覆盖两个最终报告：

- `aits score-daily` 生成的 `daily_score_YYYY-MM-DD.md`。
- `aits backtest` 生成的 `backtest_YYYY-MM-DD_YYYY-MM-DD.md`。

第一版不重写所有上游校验报告，但必须把它们纳入 evidence bundle 的 dataset、quality 或 run artifact 引用。

## 设计

每个最终报告旁生成一个 JSON evidence bundle：

- 日报默认路径：`outputs/reports/evidence/daily_score_YYYY-MM-DD_trace.json`。
- 回测默认路径：`outputs/backtests/evidence/backtest_YYYY-MM-DD_YYYY-MM-DD_trace.json`。

Bundle 顶层对象：

|字段|含义|
|---|---|
|`schema_version`|trace bundle schema 版本，第一版为 `1`|
|`report_id`|稳定报告 ID，例如 `daily_score:2026-04-30`|
|`report_type`|`daily_score` 或 `backtest`|
|`generated_at`|bundle 生成时间|
|`report_path`|最终 Markdown 报告路径|
|`run_manifest`|命令、日期窗口、市场阶段、配置和输出 artifact|
|`quality_refs`|质量门禁引用|
|`dataset_refs`|数据集、配置和快照引用|
|`evidence_cards`|核心证据卡|
|`claims`|核心结论及其 evidence/dataset/quality 引用|

## 核心 claim

日报第一版至少包含：

- `daily_score:<as_of>:overall_position`
- `daily_score:<as_of>:data_quality`
- `daily_score:<as_of>:component_scores`

回测第一版至少包含：

- `backtest:<start>:<end>:performance`
- `backtest:<start>:<end>:data_quality`
- `backtest:<start>:<end>:input_coverage`

Claim 必须包含：

- `claim_id`
- `statement`
- `report_section`
- `evidence_ids`
- `dataset_ids`
- `quality_ids`

## 数据集和证据要求

Dataset ref 必须尽量记录：

- `dataset_id`
- `label`
- `path`
- `dataset_type`
- `row_count`
- `checksum_sha256`
- `provider`
- `endpoint`
- `request_params`
- `downloaded_at`
- `date_range`

如果某字段无法从现有 manifest 推导，必须显式置空或省略，不能伪造。

Evidence card 必须记录：

- `evidence_id`
- `summary`
- `signal_ids`
- `ticker_ids`
- `date_window`
- `dataset_ids`
- `quality_ids`
- `config_ids`
- `artifact_paths`

## 反查入口

新增 CLI：

```powershell
aits trace lookup --bundle-path outputs/reports/evidence/daily_score_2026-04-30_trace.json --id daily_score:2026-04-30:overall_position
```

输出中文摘要，并保留原始 id、类型、关联证据、数据集和质量门禁引用。

## 验收标准

- 日报和回测报告都包含“可追溯引用”章节，列出 trace bundle、核心 claims、datasets、quality refs 和 lookup 命令。
- Evidence bundle JSON 能从核心 claim 追到 evidence、dataset、quality 和 run manifest。
- Bundle 记录 AI regime 和实际请求日期窗口；日报没有显式 regime 时必须声明使用配置默认 regime 作为解释口径。
- Dataset refs 能记录已知的 provider、endpoint、request parameters、row count 和 checksum；缺失字段不能伪造。
- `aits trace lookup` 能按 claim/evidence/dataset/quality/run id 反查上下文。
- 更新 `docs/system_flow.md`，补充测试。

## 状态记录

- 2026-05-04：创建需求文档，开始实现第一版日报/回测 evidence bundle 和 lookup CLI。
- 2026-05-04：第一版完成。`score-daily` 和 `backtest` 会生成 evidence bundle，Markdown 报告包含可追溯引用章节，`aits trace lookup` 可按 claim/evidence/dataset/quality/run id 反查上下文；`python -m pytest -q` 通过 203 项测试。
