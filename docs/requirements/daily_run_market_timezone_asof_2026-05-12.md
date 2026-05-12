# OPS-011 daily-run 美股市场时区 as-of 解析

任务 ID：`OPS-011`

## 背景

2026-05-12 自动化在日本时间早晨运行 `aits ops daily-run`。CLI 默认
`as_of` 使用本机 `date.today()`，得到 `2026-05-12`；输入可见性预检查使用
UTC 日期作为运行日期，在 UTC 仍为 `2026-05-11` 时把该 as-of 判为未来日。

这暴露出更大的生产语义问题：`daily-run` 是 U.S. equity market 日报入口，
默认日期和可见性边界不应取决于本机时区或 UTC 零点，而应按
`America/New_York` 的美股交易日和收盘后可见窗口解析。

## 目标

- 未显式传入 `--as-of` 时，`daily-plan` 和 `daily-run` 默认选择最新已完成的
  U.S. equity trading day。
- 常规交易日美东 16:30 之后，默认 as-of 为当日；16:30 前为上一交易日。
- 周末和 NYSE 常规整日休市日，默认 as-of 为上一交易日。
- `daily-run` 输入可见性预检查使用同一生产 as-of 解析逻辑，避免本机日期与
  UTC 日期不一致导致误判。
- 显式非交易日 `--as-of` 仍可在 America/New_York 日期到达后运行休市日模式；
  默认 as-of 不自动选择休市日。
- 显式传入未来交易日仍必须 fail closed；显式传入历史 as-of 仍必须提示使用
  `aits ops replay-day --mode cache-only`。

## 非目标

- 不实现盘中提前收盘日的特殊收盘时间。
- 不预测供应商数据实际发布时间；第一版只用常规收盘后 30 分钟缓冲表示生产
  as-of 可见窗口。
- 不改变历史 replay 的 cache-only 输入冻结规则。

## 验收标准

- 测试覆盖日本时区早晨对应美东前一交易日盘后时，默认 as-of 解析为美东已
  完成交易日，而不是本机日期。
- 测试覆盖美东常规交易日 16:30 前后、周末和休市日的默认 as-of。
- `run_daily_ops_plan` 的 visibility check 对交易日使用最新已完成交易日，对
  显式休市日使用当前美东日期，避免破坏休市日健康/风险运行入口。
- README、runbook、system flow 和 task register 说明默认生产日期语义。

## 进展记录

- 2026-05-12：新增任务并进入 `IN_PROGRESS`。原因：owner 指出自动化
  `BLOCKED_VISIBILITY` 不符合直觉，诊断确认默认 as-of 使用本机时区，
  visibility check 使用 UTC 日期，均未按美股市场日语义解析。
- 2026-05-12：实现完成，进入 `VALIDATING`。新增
  `latest_completed_us_equity_trading_day` / `current_us_equity_market_date`，
  `daily-plan` 和 `daily-run` 默认 as-of 改按美东最新已完成交易日解析；
  visibility check 对交易日使用最新已完成交易日，对显式休市日使用当前
  America/New_York 日期。验证：`ruff check src tests` PASS；
  `pytest tests/test_trading_calendar.py tests/test_ops_daily.py` 为 29 passed；
  `aits ops daily-plan` smoke 默认评估日期为 `2026-05-11`；
  `git diff --check` PASS；`pytest --lf -q` 跑完 476 passed。普通
  `pytest -q` 在 Windows 上仍会在既有 backtest CLI 测试附近触发 Python
  进程 access violation，同一 backtest 文件单独运行 23 passed，暂记为本机
  测试环境残余风险。
