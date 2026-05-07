# PIT 日常抓取韧性修复

## 背景

2026-05-06 日常 automation 执行 `aits pit-snapshots fetch-fmp-forward` 时，
命令约 46 秒后返回 exit code 1，但 stdout/stderr 没有诊断内容，且没有生成
当日 FMP PIT 抓取报告、PIT validation 报告或 normalized CSV。后续只读诊断
确认 `FMP_API_KEY` 存在，默认 core_watchlist 为
`MSFT, GOOG, TSM, INTC, AMD, NVDA`，同一 fetch 函数和临时目录中的正式 `aits`
入口可成功抓取并校验。

## 设计边界

- 不把供应商失败、权限失败、空结果、写入失败或 PIT 校验失败静默平滑为成功。
- 不补写缺跑日期，也不把未通过校验的快照标记为 strict PIT。
- PIT 抓取失败可以成为运行健康告警和日报限制，但不应自动阻断 `score-daily`
  自身的数据质量门禁、SEC 校验、估值校验、风险事件校验和 rule card 校验。
- `score-daily` 若读取不到当日可用 PIT 样本，应继续沿用现有估值/PIT 覆盖
  降级说明和 pipeline health 告警，而不是把失败快照纳入 scoring。

## 实施步骤

|步骤|状态|验收标准|
|---|---|---|
|1. 抓取失败诊断|DONE|`fetch-fmp-forward` 对未捕获异常生成脱敏中文失败报告或至少打印脱敏错误摘要，避免静默 exit。|
|2. 非阻断运行开关|DONE|新增显式非阻断模式；失败时返回 0 并声明后续只能继续自己的质量门禁，不能使用失败 PIT。|
|3. 日常计划接入|DONE|`aits ops daily-plan` 的 PIT 步骤使用非阻断模式，并在报告中说明 PIT 失败转为 health/alert 限制。|
|4. 文档和测试|DONE|README、系统流图和测试覆盖失败诊断、非阻断模式和日常计划文案。|

## 进展记录

- 2026-05-06：新增任务。owner 要求修复 `fetch-fmp-forward` 静默失败，并且该入口层问题不要阻塞后续日报流程。
- 2026-05-06：实现完成。新增失败报告构造、未捕获异常脱敏诊断、`--continue-on-failure`、daily-plan 接入、README/系统流图说明和测试。真实运行中 PIT 抓取通过，`score-daily` 继续执行并生成 `PASS_WITH_LIMITATIONS` 日报；pipeline health 为 `PASS`。
