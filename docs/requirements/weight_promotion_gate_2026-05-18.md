# TRADING-017：Weight Promotion Gate

最后更新：2026-05-18

关联任务：`TRADING-017`

## 背景

`TRADING-015` 已生成 observe-only 的 weight adjustment candidates。
`TRADING-016` 已生成只读 candidate evaluation，并且 CI 通过。
本任务新增候选权重 promotion gate，用于判断 candidate 是否可以进入人工复核。
本阶段仍然不自动修改 production profile，不自动晋级，不触发任何交易或 replay。

## 范围

1. 新增 `outputs/reports/weight_promotion_gate_YYYY-MM-DD.json` 和 `.md`。
2. 固定 `production_effect=none`、`gate_mode=manual_review_only`。
3. 只读取既有 artifact：同日 weight adjustment candidates、weight candidate
   evaluation、shadow parameter impact、paper signal quality、可选 paper trading replay、
   可选 daily decision summary 和可选 governance/profile metadata。
4. 每个 candidate 输出 `candidate_id`、`candidate_evaluation_status`、
   `promotion_gate_status`、`blocked_by`、`warnings`、`improvement_summary`、
   `risk_delta_summary`、`data_quality_summary`、`required_manual_review_items` 和
   `recommendation`。
5. Daily task dashboard 新增 Weight Promotion Gate 轻量卡片，只读展示已有 JSON，
   不触发 gate 重跑或下游 runner。

## 状态与语义

Candidate 级和顶层 `promotion_gate_status` 只允许：

- `INSUFFICIENT_DATA`
- `OBSERVE_ONLY`
- `BLOCKED`
- `NO_CLEAR_IMPROVEMENT`
- `CANDIDATE_PROMISING_BUT_LIMITED`
- `READY_FOR_MANUAL_REVIEW`

禁止任何自动晋级、上线就绪、交易批准或 production approval 语义。

## Gate

以下情况必须 blocked：

- candidate evaluation 不存在；
- candidate 本身 blocked；
- `manual_approval_required` 缺失；
- sample_count 或 filled_count 不足；
- synthetic snapshot ratio 过高；
- historical OHLC coverage 过低；
- reconciliation PASS ratio 不足；
- continuous replay 缺失；
- max drawdown、exposure 或 concentration 恶化；
- shadow impact insufficient / unreliable；
- paper signal quality unreliable；
- data gate BLOCK；
- major risk event warning。

只有样本量、数据质量、continuous replay、risk deltas、稳定改善信号和 warning
全部达标，并且仍保留 `manual_approval_required` 时，才允许
`READY_FOR_MANUAL_REVIEW`。该状态不是 production promotion，也不代表 live readiness。

## 验收

- `python -m pytest tests/trading_engine/test_weight_promotion_gate.py`
- `python -m pytest tests/trading_engine`
- `python -m pytest tests/test_daily_task_dashboard.py`
- `python -m pytest`
- `python -m ruff check scripts src tests`
- `python -m black --check scripts src tests`
- push 后确认 GitHub Actions 通过。

## 状态记录

- 2026-05-18：新增并进入 IN_PROGRESS。原因：owner 要求在 TRADING-015/016 后新增
  manual-review-only promotion gate；当前阶段禁止自动修改 production profile、写入 approved
  profile、触发 IBKR / PaperBroker / replay runner 或任何交易。
- 2026-05-18：从 IN_PROGRESS 改为 VALIDATING。已新增
  `config/weight_promotion_gate_policy.yaml`、`scripts/run_weight_promotion_gate.py`、
  `outputs/reports/weight_promotion_gate_YYYY-MM-DD.json/md` 生成器、保守 promotion gate、
  dashboard Weight Promotion Gate 只读卡片、系统流图 / 产物目录和测试；验证通过目标
  pytest、`tests/trading_engine`、`tests/test_daily_task_dashboard.py`、全量 pytest、ruff
  和 black check。
