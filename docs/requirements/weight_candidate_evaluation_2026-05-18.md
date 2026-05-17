# TRADING-016：Weight Candidate Evaluation

最后更新：2026-05-18

关联任务：`TRADING-016`

## 背景

`TRADING-015` 已生成 observe-only 的 `weight_adjustment_candidates_YYYY-MM-DD.json/md`。
本任务新增候选权重评估层，只读取现有候选、paper signal quality、shadow impact
和可选 replay / profile metadata，不重跑历史任务，不修改 production profile。

## 范围

1. 新增 `outputs/reports/weight_candidate_evaluation_YYYY-MM-DD.json` 和 `.md`。
2. 固定 `production_effect=none`、`evaluation_mode=observe_only`。
3. 支持 7d / 14d / 30d 窗口，输出 candidate count、evaluable / blocked /
   insufficient / low-quality 统计、continuous replay availability、synthetic snapshot
   ratio、OHLC 覆盖、reconciliation PASS、paper / shadow 状态和可用的 replay delta。
4. 每个 candidate 输出 source / target profile、parameter_changes、
   required_validations、evaluation_status、blocked_by、warnings、scorecard 和
   recommendation。
5. Daily task dashboard 只读展示 Weight Candidate Evaluation 轻量卡片，不触发评估重跑。

## 状态与语义

Candidate 级 `evaluation_status` 只允许：

- `INSUFFICIENT_DATA`
- `OBSERVE_ONLY`
- `CANDIDATE_PROMISING_BUT_LIMITED`
- `NO_CLEAR_IMPROVEMENT`
- `CANDIDATE_UNRELIABLE`
- `LOW_DATA_QUALITY`

禁止任何自动晋级、生产 promotion、实盘交易或批准语义。即使 candidate 看起来更好，
也只能进入 manual review。

## Gate

以下情况 candidate 必须保持 blocked：

- candidate 本身 blocked；
- `manual_approval_required`；
- `insufficient_sample`；
- `low_data_quality`；
- `synthetic_snapshot_ratio_too_high`；
- `continuous_replay_missing`；
- `shadow_impact_insufficient`；
- `paper_signal_quality_unreliable`；
- `reconciliation_unreliable`；
- `max_drawdown_worse`；
- `exposure_worse`；
- `concentration_worse`。

## 验收

- `python -m pytest tests/trading_engine/test_weight_candidate_evaluation.py`
- `python -m pytest tests/trading_engine`
- `python -m pytest tests/test_daily_task_dashboard.py`
- `python -m pytest`
- `python -m ruff check scripts src tests`
- `python -m black --check scripts src tests`

## 状态记录

- 2026-05-18：新增并进入 IN_PROGRESS。原因：owner 要求在 TRADING-015 后新增只读候选权重评估层，当前阶段不自动调参、不 promotion、不触发交易、不影响 daily dashboard 主结论。
- 2026-05-18：从 IN_PROGRESS 改为 VALIDATING。已新增
  `config/weight_candidate_evaluation_policy.yaml`、
  `scripts/run_weight_candidate_evaluation.py`、
  `outputs/reports/weight_candidate_evaluation_YYYY-MM-DD.json/md` 生成器、
  7/14/30 日窗口、candidate scorecard、保守 evaluation gate、dashboard 只读卡片、
  系统流图 / 产物目录和测试；验证通过目标 pytest、`tests/trading_engine`、
  `tests/test_daily_task_dashboard.py`、全量 pytest、ruff 和 black check。
