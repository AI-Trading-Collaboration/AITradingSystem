# TRADING-018A：Daily Weight Adjustment Scheduler Dry Run

最后更新：2026-05-18

关联任务：`TRADING-018A`

## 背景

`TRADING-015`、`TRADING-016`、`TRADING-017` 和 `TRADING-018` 已完成。当前
`scripts/run_daily_weight_adjustment.py` 已能生成
`outputs/reports/daily_weight_adjustment_summary_YYYY-MM-DD.json/md`。

本任务只为每日定时运行增加 dry-run 封装和验证。当前阶段不是自动修改 production
权重，不写 approved profile，不触发 IBKR、PaperBroker、replay、controlled fill、
lifecycle 或 comparison。

## 范围

1. 新增 `scripts/run_daily_weight_adjustment_scheduler_dry_run.py`。
2. 封装同日 `scripts/run_daily_weight_adjustment.py` summary pipeline。
3. 新增 `outputs/reports/daily_weight_adjustment_scheduler_dry_run_YYYY-MM-DD.json`。
4. 新增 `outputs/reports/daily_weight_adjustment_scheduler_dry_run_YYYY-MM-DD.md`。
5. 新增 Windows Task Scheduler runbook。
6. 更新系统流图和产物目录。
7. 新增 scheduler dry-run 测试。

## Scheduler Summary Schema

`daily_weight_adjustment_scheduler_dry_run` 至少包含：

- `as_of`
- `started_at`
- `completed_at`
- `duration_seconds`
- `dry_run_status`
- `production_effect`
- `manual_review_only`
- `invoked_command`
- `generated_artifacts`
- `missing_artifacts`
- `pipeline_status`
- `candidate_count`
- `promotion_gate_status`
- `ready_for_manual_review_count`
- `blocked_count`
- `warnings`
- `safety_checks`

## Dry-run 语义

- `mode=dry_run`
- `production_effect=none`
- `manual_review_only=true`
- 允许生成或刷新同日 daily weight adjustment summary。
- 允许生成 scheduler dry-run 自身 JSON/Markdown。
- 不修改 production profile。
- 不写 approved profile。
- 不触发 IBKR。
- 不触发 PaperBroker 下单。
- 不触发 replay / controlled fill / lifecycle / comparison。
- 只验证 pipeline 可被本地定时任务安全调用。

## 安全检查

dry-run report 必须记录：

- `production_profile_write_attempted=false`
- `approved_profile_write_attempted=false`
- `ibkr_order_path_called=false`
- `paperbroker_order_path_called=false`
- `replay_runner_called=false`
- `dashboard_write_only_summary=true`
- `forbidden_terms_absent=true`

## 缺失输入处理

若上游 015/016/017 artifact 缺失，scheduler dry-run 仍可完成封装验证，但必须：

- `dry_run_status=LIMITED`
- `pipeline_status=LIMITED`
- `missing_artifacts` 列出缺失的上游 JSON 或 Markdown。
- 不补造 improvement。
- 不把缺失输入解释为可人工复核结论。

## Runbook 要求

新增 `docs/runbooks/daily_weight_adjustment_scheduler_dry_run.md`，说明：

- 如何本地手动运行 dry-run。
- 如何用 Windows Task Scheduler 每日运行。
- 推荐运行时间。
- 如何设置项目目录。
- 如何激活 `.venv`。
- 如何写日志文件。
- 如何检查 output artifacts。
- 如何确认 GitHub repo 不被自动 commit。
- 如何处理失败状态。
- 如何连续运行 3～7 天观察稳定性。
- GitHub Actions schedule 暂不建议直接启用，以及原因。

## 验收

- `python -m pytest tests/trading_engine/test_daily_weight_adjustment_scheduler_dry_run.py`
- `python -m pytest tests/trading_engine`
- `python -m pytest tests/test_daily_task_dashboard.py`
- `python -m pytest`
- `python -m ruff check scripts src tests`
- `python -m black --check scripts src tests`
- push 后确认 GitHub Actions 通过。

## 状态记录

- 2026-05-18：新增并进入 IN_PROGRESS。原因：owner 要求在已有 daily weight
  adjustment summary pipeline 之后增加 scheduler dry-run 封装，先验证本地 Windows
  Task Scheduler 调用边界；当前阶段禁止自动修改 production profile、写 approved
  profile、触发 IBKR / PaperBroker / replay runner、controlled fill、lifecycle /
  comparison 或任何交易。
- 2026-05-18：从 IN_PROGRESS 改为 VALIDATING。已新增
  `scripts/run_daily_weight_adjustment_scheduler_dry_run.py`、
  `daily_weight_adjustment_scheduler_dry_run_YYYY-MM-DD.json/md` schema、
  显式 safety checks、Windows Task Scheduler runbook、GitHub Actions schedule 暂不启用说明、
  系统流图 / 产物目录和测试；验证通过目标 pytest、`tests/trading_engine`、
  `tests/test_daily_task_dashboard.py`、全量 pytest、ruff 和 black check。
