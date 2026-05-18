# TRADING-018：Daily Scheduled Weight Adjustment Job

最后更新：2026-05-18

关联任务：`TRADING-018`

## 背景

`TRADING-015` 已完成 observe-only weight adjustment candidate generator。
`TRADING-016` 已完成 weight candidate evaluation。
`TRADING-017` 已完成 manual-review-only weight promotion gate。

本任务把 015/016/017 串入每日权重调节评估 summary。当前阶段不是自动调参、
不是定时任务启用、不是 production promotion，也不触发任何交易或 replay runner。

## 范围

1. 新增 `scripts/run_daily_weight_adjustment.py`。
2. 新增 `outputs/reports/daily_weight_adjustment_summary_YYYY-MM-DD.json` 和 `.md`。
3. 默认 `mode=observe_only`、`production_effect=none`。
4. 只读取同日既有 artifact：
   - `weight_adjustment_candidates_YYYY-MM-DD.json/md`
   - `weight_candidate_evaluation_YYYY-MM-DD.json/md`
   - `weight_promotion_gate_YYYY-MM-DD.json/md`
5. Daily task dashboard 新增 Daily Weight Adjustment Summary 轻量卡片，只读读取
   summary JSON，不触发 pipeline 重跑。
6. 新增 `docs/runbooks/daily_weight_adjustment_job.md`，说明手动运行和未来调度
   接入方式；当前不自动启用定时任务。

## Summary Schema

`daily_weight_adjustment_summary` 至少包含：

- `as_of`
- `production_effect`
- `mode`
- `candidate_status`
- `evaluation_status`
- `promotion_gate_status`
- `candidate_count`
- `evaluable_candidate_count`
- `ready_for_manual_review_count`
- `blocked_count`
- `top_candidate_id`
- `main_blocked_by`
- `warnings`
- `source_artifacts`
- `missing_artifacts`
- `required_manual_review_items`
- `recommendation`

## 缺失输入处理

缺任一上游 JSON 或 Markdown artifact 时：

- 不补造结论；
- 不生成虚假的 improvement；
- summary 标记 `LIMITED` / `INSUFFICIENT_DATA`；
- `missing_artifacts` 明确列出缺失文件；
- summary 的 `promotion_gate_status` 不得输出 `READY_FOR_MANUAL_REVIEW`；
- dashboard 只展示缺失状态，不触发上游 runner。

## 安全边界

本任务禁止：

- 自动修改 `config/weights/weight_profile_current.yaml` 或任何 production profile；
- 写入 approved profile；
- 调用 IBKR order path；
- 调用 `PaperBroker` 下单路径；
- 调用 replay runner；
- 调用 controlled fill / lifecycle / comparison 脚本；
- 改变 dashboard 主投资结论。

所有输出必须固定 `production_effect=none`。报告输出不得出现
`AUTO_PROMOTE`、`PROMOTE_TO_PRODUCTION`、`READY_FOR_LIVE`、`SHOULD_TRADE`、
`APPROVED_FOR_TRADING`。

## 阶段拆解

1. 登记任务和需求文档，固定 observe-only / manual-review-only 边界。
2. 实现 daily summary builder 和脚本，只读取 015/016/017 产物。
3. 接入 daily task dashboard 轻量卡片，只读读取 summary JSON。
4. 更新系统流图、产物目录和 runbook。
5. 增加 pipeline 和 dashboard 测试，覆盖缺失输入、安全边界和 forbidden terms。
6. 运行目标测试、全量测试、ruff、black，提交并 push 后确认 GitHub Actions。

## 验收

- `python -m pytest tests/trading_engine/test_daily_weight_adjustment_pipeline.py`
- `python -m pytest tests/trading_engine`
- `python -m pytest tests/test_daily_task_dashboard.py`
- `python -m pytest`
- `python -m ruff check scripts src tests`
- `python -m black --check scripts src tests`
- push 后确认 GitHub Actions 通过。

## 状态记录

- 2026-05-18：新增并进入 IN_PROGRESS。原因：owner 要求把 TRADING-015/016/017
  串成每日只读权重调节评估流程；当前阶段禁止自动修改 production profile、写入
  approved profile、触发 IBKR / PaperBroker / replay runner、controlled fill、
  lifecycle / comparison 脚本或任何交易。
- 2026-05-18：从 IN_PROGRESS 改为 VALIDATING。已新增
  `scripts/run_daily_weight_adjustment.py`、`daily_weight_adjustment_summary_YYYY-MM-DD.json/md`
  生成器、缺失输入 `LIMITED` / `INSUFFICIENT_DATA` 降级、安全边界、dashboard
  Daily Weight Adjustment Summary 只读卡片、runbook、系统流图 / 产物目录和测试；
  验证通过目标 pytest、`tests/trading_engine`、`tests/test_daily_task_dashboard.py`、
  全量 pytest、ruff 和 black check。
