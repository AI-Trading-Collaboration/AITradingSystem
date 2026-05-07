# OPS-005 日报前置数据依赖编排

任务 ID：`OPS-005`

## 背景

2026-05-07 每日 `score-daily` 运行在 SEC 基本面指标 CSV 缺失处 fail closed：
`data/processed/sec_fundamentals_2026-05-07.csv` 不存在，预期观测 72，覆盖 0。
根因不是 `score-daily` 门禁错误，而是每日前置链路只规划了市场下载、PIT 抓取和日报评分，
没有在日报前生成当日 SEC metrics。

## 设计结论

- `score-daily` 继续只负责校验 SEC metrics CSV 并构建 SEC 特征，不在评分命令里偷偷下载或抽取基本面。
- 每日运行编排必须在 `score-daily` 前显式刷新 SEC companyfacts 并运行 `extract-sec-metrics`。
- 估值模块同样读取可审计快照；如果日报要反映当日估值和预期输入，运行编排必须在 `score-daily` 前运行 `valuation fetch-fmp`。
- FMP PIT 抓取仍可在日常调度中非阻断失败，但失败必须进入报告或 pipeline health；SEC metrics 与估值快照刷新失败会阻断日报。

## 阶段拆解

|阶段|状态|目标|验收标准|
|---|---|---|---|
|1. 依赖审计|DONE|列出 `score-daily` 依赖但不由 `score-daily` 自动生成的输入|确认市场/宏观缓存已有 `download-data` 步骤；SEC companyfacts/metrics 和 FMP 估值快照缺少日报前置步骤；官方政策来源/OpenAI 预审已在 `score-daily` 内部执行|
|2. Daily plan 接入|DONE|在 `aits ops daily-plan` 中加入 SEC companyfacts、SEC metrics 和 FMP 估值刷新步骤|计划顺序显示新增步骤、环境变量、artifact、阻断关系；缺少 `SEC_USER_AGENT` 或 `FMP_API_KEY` 时显示 `BLOCKED_ENV`|
|3. 验证和运行观察|VALIDATING|用测试和下一次真实日报验证新编排不会遗漏前置输入|`tests/test_ops_daily.py` 覆盖新增步骤；下一次默认日报不再因当日 SEC metrics 文件缺失而停止|

## 当前仍未自动处理的输入边界

- TSMC IR 季度输入：已有 `merge-tsm-ir-sec-metrics` 可把已导入的 TSM IR 季度指标合入 SEC-style CSV，但发现和导入最新 TSMC Management Report 需要官方 URL/PDF 或 manifest；默认日报目前可在缺少 TSM quarterly 时以 SEC 覆盖 warning 继续，不应伪造季度数据。
- 风险事件正式发生记录和复核声明：OpenAI 预审只写 `llm_extracted / pending_review` 队列，不写正式 occurrence；每日有效复核仍需要 owner 运营纪律。
- 组合持仓、交易 thesis、catalyst calendar：这些是本地手工或审计输入，`score-daily` 会校验和报告限制，但不应自动生成。
- SEC accession archive：用于追溯和审计增强，不是当前 `score-daily` 的阻断性输入；仍由 `DATA-004` 独立推进。

## 进展记录

- 2026-05-07：创建任务，原因：真实每日运行暴露 `score-daily` 依赖当日 SEC metrics，但现有每日编排没有生成该 CSV。
- 2026-05-07：Daily plan 接入完成。新增默认步骤：`sec_companyfacts`、`sec_metrics`、`sec_metrics_validation` 和 `valuation_snapshots`；`score-daily` 仍保留内部重复门禁。验证：`ruff check src tests` 通过，`pytest -q tests/test_ops_daily.py` 6 passed，`pytest -q tests/test_sec_metrics.py tests/test_valuation_sources.py` 36 passed，完整 `pytest -q` 408 passed；`git diff --check` 只有既有 CRLF 提示。
