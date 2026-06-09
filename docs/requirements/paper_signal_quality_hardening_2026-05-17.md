# TRADING-006A：Paper Signal Quality Hardening

状态：`BASELINE_DONE`

最后更新：2026-06-09

关联任务：`TRADING-006A`

## 背景

TRADING-006 已新增只读 paper signal quality evaluation。当前加固目标不是接真实券商，
也不是扩展订单类型，而是收紧报告语义，避免 paper signal quality 被误读为真实交易、
参数晋级或上线批准。

## 范围

1. 格式加固：
   - 对 `paper_signal_quality.py`、`test_paper_signal_quality.py`、
     `run_paper_signal_quality.py`、`run_paper_trading_replay.py` 和
     `run_paper_trading_from_candidates.py` 执行 Black。
   - 确认文件不是超长单行。
2. 状态语义加固：
   - `evaluation_status` / `quality_status` 只允许：
     `INSUFFICIENT_DATA`、`OBSERVE_ONLY`、`PROMISING_BUT_LIMITED`、
     `LOW_DATA_QUALITY`、`UNRELIABLE`。
   - 禁止：`READY_FOR_LIVE`、`SHOULD_TRADE`、`PROMOTE_TO_PRODUCTION`、
     `APPROVED_FOR_TRADING`。
3. Policy 信息：
   - JSON 顶层必须包含 `policy_id`、`policy_version`、`production_effect=none`
     和 thresholds snapshot。
   - Markdown 必须显示当前 policy version 和关键 thresholds。
4. Evaluation gate explanation：
   - `evaluation_gate` 增加 human-readable explanation。
   - 对每个 `blocked_by` 给出解释原因。
   - Dashboard 继续只展示摘要，不展示完整长表。
5. Daily-independent warning：
   - `daily_independent` / `portfolio_carry_forward=false` 语义必须输出
     `DAILY_INDEPENDENT_ONLY` warning。
   - Markdown 必须说明当前结果不是连续组合收益，不能解释现金占用、持仓结转、
     open order 跨日处理或最大回撤。

## 边界

- 不读取 broker API key。
- 不调用真实 broker。
- 不触发 paper runner。
- 不触发 paper replay。
- 不改变 production position recommendation。
- 不影响参数晋级。
- 不把 paper PnL 当成上线依据。

## 验收标准

- `python -m pytest tests/trading_engine`
- `python -m pytest tests/test_daily_task_dashboard.py`
- `python -m pytest`
- `python -m ruff check src tests scripts`
- `python -m black --check src tests scripts`

## 状态记录

- 2026-05-17：新增并进入实现。原因：owner 要求加固 TRADING-006 的状态语义、
  policy 暴露、evaluation gate 解释、daily-independent warning 和测试边界。
- 2026-05-17：实现完成，进入验证。目标文件已执行 Black 且不是单行；报告新增
  allowed status 语义、顶层 policy id/version/threshold snapshot、evaluation gate
  explanation / reason explanations、`DAILY_INDEPENDENT_ONLY` warning 和 Markdown
  policy/threshold 展示；dashboard 仍只读展示摘要；测试覆盖 policy thresholds、
  forbidden status、daily-independent warning、dashboard 边界和不触发 runner/replay/broker。
- 2026-05-17：验证结果：`python -m pytest tests/trading_engine`、
  `python -m pytest tests/test_daily_task_dashboard.py`、`python -m pytest` 和
  `python -m ruff check src tests scripts` 通过；本次目标/改动文件 Black check 通过。
  全仓 `python -m black --check src tests scripts` 在当前环境下仍报告 125 个既有无关
  文件 would reformat，本轮未为通过该命令重排无关文件。
- 2026-06-09：从 `VALIDATING` 改为 `BASELINE_DONE`。原因：本轮 isolated smoke
  和字段级复核确认 allowed status、policy snapshot、evaluation gate explanation、
  `DAILY_INDEPENDENT_ONLY` / `PAPER_ONLY_SIMULATION` warning、dashboard 只读摘要和
  `production_effect=none` 安全边界均保持；forbidden live / promotion 语义只出现在测试
  禁止清单中。目标 paper signal quality + dashboard pytest 25 passed，Ruff 和触达文件
  Black check 通过。剩余全仓 Black baseline 已拆分为 `DEV-001`，不再阻塞本任务基线归档。
