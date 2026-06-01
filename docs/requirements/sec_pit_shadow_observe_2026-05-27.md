# TRADING-044: SEC PIT Observe-Only Shadow Lane

最后更新：2026-05-27

## 背景

TRADING-039 至 TRADING-043 已完成 SEC reconstructed PIT backfill、cognitive evaluation、baseline comparison、real-run diagnostics 和 candidate review。最新真实 review 将 `capex_intensity` 标记为 `READY_FOR_MANUAL_REVIEW`，但同时暴露 mixed regime dependency 和 ticker-level 差异。

本任务只实现隔离 observe-only shadow lane。它记录如果 `capex_intensity` 进入评分会怎样影响 score 和 rank，但不得影响 production weights、production actions、active shadow weights 或非 SEC shadow lane。

## 决策

- lane: `sec_pit_capex_intensity_observe_only`
- lane status: `observe_only`
- candidate: `capex_intensity`
- manual decision: `APPROVE_OBSERVE_ONLY_SHADOW`
- initial observe-only weight: `-0.025`
- maximum allowed initial absolute weight: `0.050`
- PIT grade policy: `B_RECONSTRUCTED_SEC_FILING_PIT`
- production effect: `none`
- manual review required: `true`

## 阶段拆解

|阶段|状态|验收标准|
|---|---|---|
|1. 配置与任务登记|DONE|新增 `config/sec_pit_shadow_observe.yaml`，记录 lane、安全阈值、monitoring bucket 和 rollback 条件。|
|2. 核心 observe-only builder|DONE|新增只读模块，读取 TRADING-040/041/042/043 artifacts、feature panel 和 baseline score，生成 summary、shadow scores、rank shift、bucket comparison、monitoring plan 和 safety audit。|
|3. CLI|DONE|新增 `aits sec-pit shadow-observe`，支持显式路径参数和 `--latest` 自动发现。|
|4. Dashboard|DONE|daily task dashboard 新增 `SEC PIT Observe-Only Shadow Lane` 只读卡片，只读取 summary artifact，不运行 builder。|
|5. 文档与测试|DONE|新增 runbook、artifact catalog、system flow、learning path 和相关 runbook 更新；新增专项测试覆盖 CLI、schema、安全降级、determinism、dashboard 只读和 config 不变。|
|6. 验证收尾|DONE|目标 pytest、dashboard pytest、相关 SEC PIT 回归、全量 pytest、ruff 和触达 Python Black check 通过。|

## 安全边界

- 不写 `config/weights/weight_profile_current.yaml`。
- 不写 `config/weights/shadow_weight_profiles.yaml`。
- 不写 active shadow state 或非 SEC shadow lane。
- 不写 production action、order intent、prediction ledger 或 approved overlay。
- 输出目录必须隔离在 `outputs/sec_pit_shadow_observe`。
- 每个相关输出行必须包含 `manual_review_required=true` 和 `production_effect=none`。
- 安全检查 critical failure 时只写 safety audit 和 degraded summary，不生成 shadow score/rank/bucket/monitoring 长表。

## 输入

- `outputs/sec_pit_candidate_review/*`
- `outputs/sec_pit_evaluation/*`
- `outputs/sec_pit_baseline_comparison/*`
- `outputs/sec_pit_diagnostics/*`
- `data/processed/sec_edgar/sec_pit_feature_panel.csv` 或显式 `--feature-panel`
- `data/processed/scores_daily.csv` 或显式 `--baseline-score-path`
- `config/sec_pit_shadow_observe.yaml`

## 输出

- `outputs/sec_pit_shadow_observe/sec_pit_shadow_observe_summary_YYYY-MM-DD.json`
- `outputs/sec_pit_shadow_observe/sec_pit_shadow_observe_summary_YYYY-MM-DD.md`
- `outputs/sec_pit_shadow_observe/sec_pit_shadow_scores_YYYY-MM-DD.csv`
- `outputs/sec_pit_shadow_observe/sec_pit_shadow_rank_shift_YYYY-MM-DD.csv`
- `outputs/sec_pit_shadow_observe/sec_pit_shadow_bucket_comparison_YYYY-MM-DD.csv`
- `outputs/sec_pit_shadow_observe/sec_pit_shadow_monitoring_plan_YYYY-MM-DD.csv`
- `outputs/sec_pit_shadow_observe/sec_pit_shadow_safety_audit_YYYY-MM-DD.csv`

## 状态记录

- 2026-05-27：新增并进入 `IN_PROGRESS`。原因：owner 已给出 `APPROVE_OBSERVE_ONLY_SHADOW`，但该 candidate 仍不得 promotion，只能进入隔离观察 lane。
- 2026-05-27：改为 `DONE`。实现已完成 `aits sec-pit shadow-observe` / `--latest`、隔离输出、只读 dashboard、文档和测试；真实 `--latest --candidate-feature capex_intensity` 生成 2026-05-26 artifacts，`shadow_status=LIMITED_BASELINE_MISSING`，原因是 baseline score 覆盖不完整，safety audit 全部通过，`monitoring_status=ROLLBACK_TRIGGERED` 用于暴露观察期风险且 `production_effect=none`。

## 验证记录

- `python -m pytest tests/trading_engine/test_sec_pit_shadow_observe.py -q`：10 passed。
- `python -m pytest tests/test_daily_task_dashboard.py -q`：21 passed。
- `python -m pytest tests/trading_engine/test_sec_pit_candidate_review.py -q`：10 passed。
- `python -m pytest tests/trading_engine/test_sec_pit_real_run_diagnostics.py -q`：10 passed。
- `python -m pytest -q`：1308 passed, 1 warning。
- `python -m ruff check config src tests scripts docs`：passed。
- `python -m black --check` for touched Python files：passed。
