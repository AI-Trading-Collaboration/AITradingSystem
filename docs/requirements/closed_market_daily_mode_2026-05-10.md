# 休市日每日运行模式

最后更新：2026-05-10

## 背景

2026-05-10 是周日。每日自动化按默认 `as-of today` 运行
`aits ops daily-run` 时，第一步 `download-data --end 2026-05-10`
退出码为 1，未写入新的 `download_manifest.csv`，后续 PIT、SEC、
valuation、`score-daily`、ops health 和 secret scan 均未运行。

休市日没有新的美股交易日行情是预期情况，但现有 daily-run 没有显式区分
交易日与休市日，导致两个风险：

- 把“没有周末行情”误看作普通下载失败；
- 在没有新收盘价时仍生成新的 `daily_score`、decision snapshot 或执行动作，
  造成投资解释上的伪更新。

## 设计决策

- 默认每日运行使用 U.S. equity market session 判断。第一版覆盖周末和 NYSE
  常规整日休市日，不处理盘中提前收盘，也不尝试预测临时特别休市。
- 休市日运行必须在 `daily-plan` 与 `daily-run` 报告中声明：
  market session、休市原因、上一交易日和交易日历来源。
- 休市日默认不运行 `score-daily`，因此不生成新的日报评分、decision snapshot、
  evidence bundle、prediction ledger 行或执行动作。
- 休市日的 `download-data` 只用于补齐上一交易日缓存。如果主价格和第二行情源
  已覆盖上一交易日，则跳过下载；如果缺失，则以 `--end <上一交易日>` 运行，
  不用休市日作为行情截止日期。
- 休市日仍可运行官方政策/地缘来源抓取、FMP forward-only PIT、SEC
  companyfacts/metrics、FMP valuation snapshots、ops health 和 secret scan。
- `ops health` 在休市日不得要求当日 data quality、features、scores 或
  daily score report 存在；它仍检查市场缓存存在性、PIT 健康和安全扫描输出。

## 阶段拆解

|阶段|状态|实现范围|验收标准|
|---|---|---|---|
|1. 交易日历基础|DONE|新增交易日历模块，覆盖周末、NYSE 常规整日休市日和上一交易日计算|单元测试覆盖 2026-05-08 交易日、2026-05-10 周日、2026-07-03 Independence Day observed、2026-11-26 Thanksgiving|
|2. daily-plan/daily-run 接入|DONE|休市日自动调整步骤：缓存已覆盖时跳过 download-data，添加官方政策来源抓取，跳过 score-daily，health 使用休市模式|计划报告显示 CLOSED_MARKET、上一交易日和 skip reason；执行报告不把跳过评分解释为失败|
|3. ops health 休市语义|DONE|`aits ops health --non-trading-day` 不要求当日评分产物|缺少休市日 daily_score 不产生 FAIL；PIT 和基础缓存仍检查|
|4. 文档与验证|DONE|更新 README、system_flow、task register 和测试|目标测试、Ruff 和真实/近真实 CLI smoke 通过|

## 开放边界

- 第一版不处理 U.S. 市场提前收盘日，因为提前收盘仍有收盘价，不应自动跳过评分。
- 第一版不处理临时特别休市。若发生特别休市，应通过后续配置或 task register
  项登记处理。
- 休市日 OpenAI 风险事件预审不在第一版单独执行；官方来源候选会生成
  pending-review CSV，正式评分仍要求交易日 `score-daily` 或人工复核导入。
- 交易日历依据 NYSE 官方 Holidays & Trading Hours 页面列出的 2026-2028
  常规整日休市日和交易时段；其中 New Year's Day 落在周六时不向前一周五补休。
  提前收盘日仍视为交易日，因为仍会形成当日收盘价。
  参考：https://www.nyse.com/trade/hours-calendars

## 进展记录

- 2026-05-10：新增任务 `OPS-006` 并开始实现。原因：owner 决策采用显式
  closed-market mode，避免周末硬跑市场下载和日报评分。
- 2026-05-10：实现完成。新增 `trading_calendar`，`daily-plan/daily-run`
  自动识别 `CLOSED_MARKET`，缓存已覆盖上一交易日时跳过 `download-data`，
  休市日抓取官方政策来源、保留 PIT/SEC/valuation/health/secret scan，
  并跳过 `score-daily`；`ops health --non-trading-day` 不要求当日评分产物。
  验证：`ruff check src tests` 通过，`pytest -q` 424 passed，2026-05-10
  daily-plan CLI smoke 输出 `READY_WITH_SKIPS` / `CLOSED_MARKET` / 上一交易日
  `2026-05-08`。
- 2026-05-10：按 owner 要求重新验证交易日和休市日流程。先备份并移出
  2026-05-10 旧 daily ops 计划/执行报告，真实运行
  `aits ops daily-run --as-of 2026-05-10`，结果 `PASS_WITH_SKIPS`：
  `download_data` 与 `score_daily` 显式 SKIPPED，其余官方政策来源、PIT、
  SEC、valuation、`ops health --non-trading-day` 和 secret scan 均 PASS。
  交易日完整真实重跑没有执行，因为当前日期无法重新生成 2026-05-08 当时可见的
  forward-only PIT/valuation/官方来源；改用干净临时目录和 fake runner 验证
  `2026-05-08` 交易日编排路径，结果 9/9 步 PASS、无 SKIPPED，命令顺序为
  download-data、PIT、SEC companyfacts、SEC metrics、SEC validation、valuation、
  score-daily、ops health、secret scan。该验证同时修复 PIT produced_paths
  未跟随 `project_root` 的隔离测试问题。复验：`ruff check src tests` 通过，
  `pytest -q` 424 passed。
- 2026-05-10：补充 2026-05-08 归档输入回放验证。5/8 PIT raw payload、
  normalized CSV 和 manifest 记录可用：manifest 中 36 条 5/8 快照均为
  `PASS`，raw payload 无缺失，normalized CSV 4975 行。直接回放会失败：
  当前工作区已有 5/9、5/10 valuation snapshots，按 5/8 as-of 校验会触发
  12 个 `valuation_date_in_future` 错误；同时 PIT manifest 已包含 5/10
  记录，`ops health --as-of 2026-05-08` 会因 latest available_time 晚于
  as-of 失败。按“归档输入回放”边界临时隔离 5/9/5/10 valuation YAML、
  将 PIT manifest 限制到 5/8 可见窗口后，执行
  `aits ops daily-run --as-of 2026-05-08 --skip-download-data
  --skip-pit-snapshots --skip-sec-fundamentals --skip-valuation-snapshots
  --skip-risk-event-openai-precheck`，结果 `PASS_WITH_SKIPS`，`score_daily`、
  `ops health` 和 secret scan 均 PASS。结论：PIT 备份可以用于模拟 5/8
  输入，但必须冻结整套时点输入窗口，不能只替换 PIT 文件。
