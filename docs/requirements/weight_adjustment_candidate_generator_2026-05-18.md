# TRADING-015：Weight Adjustment Candidate Generator

最后更新：2026-06-09

关联任务：`TRADING-015`

## 背景

当前系统已经有 daily decision summary、paper signal quality、shadow parameter
impact、continuous replay、parameter governance 和 shadow profile 观察层。TRADING-015
的目标不是自动修改 production 权重，也不是启动定时任务，而是在每日报告目录新增一个
observe-only 权重调节候选生成层，帮助后续人工复核。

## 范围

1. 新增候选产物：
   - `outputs/reports/weight_adjustment_candidates_YYYY-MM-DD.json`
   - `outputs/reports/weight_adjustment_candidates_YYYY-MM-DD.md`
   - 顶层固定 `mode=observe_only`、`production_effect=none`。
2. 输入只读读取：
   - `daily_decision_summary_YYYY-MM-DD.json`
   - `paper_signal_quality_YYYY-MM-DD.json`
   - `shadow_parameter_impact_YYYY-MM-DD.json`
   - 可选已有 `paper_trading_replay_START_END.json`
   - `config/parameter_governance.yaml`
   - `config/weights/weight_profile_current.yaml`
   - 可选已有 `config/weights/shadow_weight_profiles.yaml`
3. 候选 schema 至少包含：
   - `candidate_id`
   - `generated_at`
   - `source_profile`
   - `target_profile`
   - `parameter_changes`
   - `reason_codes`
   - `expected_effect`
   - `risk_notes`
   - `blocked_by`
   - `required_validations`
   - `production_effect=none`
4. 第一版生成策略：
   - 只允许小幅权重调整，向 existing shadow profile 做单日上限内的小步移动。
   - 单日最大变化幅度由 `config/weight_adjustment_candidate_policy.yaml` 管理。
   - 权重总和必须保持 1.0 附近，不能产生总权重失衡。
   - 只生成权重候选，不删除或放宽 core risk gate。
   - 不绕过 `aits validate-data`。
   - 不根据单日 paper PnL 调高权重。
5. Candidate gate：
   - 以下情况必须 blocked：`insufficient_sample`、`low_data_quality`、
     `synthetic_snapshot_ratio_too_high`、`continuous_replay_missing`、
     `shadow_impact_insufficient`、`paper_signal_quality_unreliable`、
     `manual_approval_required`。
   - 缺少关键输入时输出 `LIMITED` / blocked candidate，不补造结论。
6. Daily dashboard：
   - 新增 Weight Adjustment Candidate 轻量卡片。
   - 只展示 `candidate_count`、`top_candidate_id`、`gate_status`、main `blocked_by`、
     report link 和 `production_effect=none`。
   - dashboard 只读读取 JSON，不触发候选生成、调参应用、replay、broker 或交易。

## 边界

- 不修改 `config/weights/weight_profile_current.yaml`。
- 不写 approved overlay、正式 prediction ledger 或 production decision snapshot。
- 不改变 daily dashboard 主结论。
- 不触发 paper trading runner、continuous replay、真实 broker 或交易。
- 不把 shadow impact、paper signal quality 或 paper PnL 解释为 production 证据。

## 验收标准

- `python -m pytest tests/trading_engine/test_weight_adjustment_candidates.py`
- `python -m pytest tests/trading_engine`
- `python -m pytest tests/test_daily_task_dashboard.py`
- `python -m pytest`
- `python -m ruff check scripts src tests`
- `python -m black --check scripts src tests`

## 实施步骤

1. 新增 `config/weight_adjustment_candidate_policy.yaml`。
2. 新增 report 生成模块和 standalone script。
3. 扩展 daily task dashboard 的只读卡片。
4. 更新系统流图、产物目录和任务登记。
5. 增加 trading_engine 与 dashboard 测试。
6. 运行验收命令，记录结果。

## 状态记录

- 2026-05-18：新增并进入实现。范围固定为 observe-only 候选生成层，
  `production_effect=none`，不修改 production 权重、不影响 dashboard 主结论、不触发交易。
- 2026-05-18：实现完成并进入验证。已新增
  `config/weight_adjustment_candidate_policy.yaml`、
  `scripts/run_weight_adjustment_candidates.py`、
  `outputs/reports/weight_adjustment_candidates_YYYY-MM-DD.json/md` 生成器、
  conservative candidate gate、单日权重变化上限、dashboard Weight Adjustment Candidate
  只读卡片、系统流图、产物目录和测试。验证通过 `python -m pytest
  tests/trading_engine/test_weight_adjustment_candidates.py`、`python -m pytest
  tests/trading_engine`、`python -m pytest tests/test_daily_task_dashboard.py`、
  `python -m pytest`、`python -m ruff check scripts src tests` 和
  `python -m black --check scripts src tests`。
- 2026-06-09：`TRADING-015` 从 VALIDATING 归档为 DONE。归档复核确认候选生成层仍为
  observe-only，后续 `TRADING-016` / `TRADING-017` / `TRADING-018` 已在同一链路上
  消费其产物，`TRADING-018A` / `TRADING-018B` 继续承担 scheduler dry-run 和 shadow
  iteration 观察；本任务不再把真实 daily/paper/shadow 输入观察作为自身收口前置。
  本轮先运行 `python -m ai_trading_system.cli validate-data`，数据质量状态
  `PASS_WITH_WARNINGS`、错误数 0；默认缺输入 smoke 使用
  `python scripts/run_weight_adjustment_candidates.py --date 2026-05-18 --reports-dir
  outputs/reports/trading015_check` 输出 `gate_status=LIMITED`、candidate_count 1、
  `top_candidate_id=weight_adjustment_candidate:2026-05-18:limited_input`、
  `main_blocked_by=missing_daily_decision_summary`、`production_effect=none`。字段级复核
  确认 `market_regime=ai_after_chatgpt`、candidate blocked、candidate
  `production_effect=none`、blocked_by 包含缺 daily summary / paper signal quality /
  shadow impact / continuous replay / manual approval，且 safety boundary 固定
  `writes_production_profile=false`、`runs_replay=false`、`calls_real_broker=false`、
  `triggers_trade=false`。验证通过
  `python -m pytest tests/trading_engine/test_weight_adjustment_candidates.py
  tests/test_daily_task_dashboard.py -q`（29 passed）、scoped safety scan 和归档前代码基线
  GitHub Actions `CI` run `27177416285` success。
