# TRADING-018C：Production vs Shadow Weight Comparison Report

最后更新：2026-05-19

关联任务：`TRADING-018C`

## 背景

`TRADING-018B` 已维护独立的 `current_shadow_weights.json`。下一步需要把同一批当日评分输入在
production 权重和 shadow 权重下离线重算一次 score，并把 score、decision、risk flags 和
contribution breakdown 差异写成只读报告。

本阶段只实现：

- `shadow learn -> offline comparison`

本阶段不实现：

- broker runner；
- replay runner；
- production profile 修改；
- approved profile 写入；
- shadow promotion；
- 自动交易或 dashboard 主投资结论变更。

## 范围

1. 新增 `scripts/run_daily_shadow_vs_production_comparison.py`。
2. 新增 comparison artifact 目录：
   - `data/derived/weight_iterations/comparison/daily_shadow_vs_production_YYYY-MM-DD.json`
   - `data/derived/weight_iterations/comparison/daily_shadow_vs_production_YYYY-MM-DD.md`
3. 读取 production weight/profile snapshot、`current_shadow_weights.json`、同日评分 artifact、
   可选 backtest/feedback artifact 摘要、TRADING-018B candidate/result。
4. 使用同一组 `decision_snapshot.scores.components` 组件分，分别按 production 和 shadow
   权重计算 weighted score。
5. 输出 production / shadow 的 score、decision、risk flags、top contributors 和完整
   contribution breakdown。
6. 输出 score delta、decision_changed、score_band_changed、主要差异原因和安全边界。
7. Daily task dashboard 新增 `Shadow vs Production Comparison` 只读卡片。
8. 更新 system flow 和 artifact catalog。

## 输入

默认读取：

- `config/weights/weight_profile_current.yaml`
- `data/derived/weight_iterations/shadow/current_shadow_weights.json`
- `data/derived/weight_iterations/shadow/candidates/shadow_weight_candidate_YYYY-MM-DD.json`
- `data/processed/decision_snapshots/decision_snapshot_YYYY-MM-DD.json`
- `outputs/reports/daily_decision_summary_YYYY-MM-DD.json`
- `outputs/reports/daily_weight_adjustment_summary_YYYY-MM-DD.json`
- `outputs/reports/weight_candidate_evaluation_YYYY-MM-DD.json`
- `outputs/reports/weight_promotion_gate_YYYY-MM-DD.json`

可选读取已有 backtest / feedback 摘要，但第一版不主动运行任何 runner。

## 输出 Schema 边界

所有 JSON/Markdown 输出必须固定：

```json
{
  "mode": "offline_comparison",
  "production_effect": "none",
  "manual_review_only": true
}
```

报告必须显式记录：

- `production_effect=none`
- `manual_review_only=true`
- `writes_production_profile=false`
- `runs_broker_runner=false`
- `runs_replay_runner=false`
- `promotes_shadow_to_production=false`

## 评分语义

- 组件分来自同一个 `decision_snapshot.scores.components`，不重新抓取数据、不重新生成特征。
- production 权重来自 production profile snapshot。
- shadow 权重来自 `current_shadow_weights.json`。
- 两组权重必须覆盖同一组 scoring components；缺失或多余组件进入 `INSUFFICIENT_DATA` /
  `LIMITED`，不得静默映射或补造组件。
- decision band 使用 `config/scoring_rules.yaml` 的 `position_bands`。
- 非 score-model risk flags 来自当日 production decision snapshot；018C 不重新评估风险事件、
  估值、thesis、组合限制或置信度 gate。

## 安全边界

本任务禁止：

- 修改 `config/weights/weight_profile_current.yaml`；
- 写 approved profile；
- 自动 promotion shadow 到 production；
- 调用 IBKR、PaperBroker、paper runner 或 replay runner；
- 改变 daily dashboard 主投资结论；
- 把 shadow 更优写成可交易建议。

## 阶段拆解

|阶段|状态|验收|
|---|---|---|
|1. 登记和需求文档|DONE|task register 指向本文，本文记录范围、输入、输出和安全边界。|
|2. 核心 comparison builder|DONE|同一组件分下输出 production/shadow score、decision、risk 和 attribution。|
|3. CLI 脚本|DONE|`python scripts/run_daily_shadow_vs_production_comparison.py --date YYYY-MM-DD` 可运行。|
|4. Dashboard 只读卡片|DONE|展示 score_delta、decision_changed、main reason、report link，不重跑 pipeline。|
|5. 文档更新|DONE|system flow、artifact catalog 更新。|
|6. 测试验证|DONE|目标 pytest、dashboard 测试、ruff、black 通过。|

## 验收命令

```powershell
python -m pytest tests/trading_engine/test_daily_shadow_vs_production_comparison.py
python -m pytest tests/test_daily_task_dashboard.py
python -m pytest tests/trading_engine
python -m pytest -q
python -m ruff check scripts src tests
python -m black --check scripts src tests
```

## 状态记录

- 2026-05-19：新增并进入 `IN_PROGRESS`。原因：owner 要求在 TRADING-018B 后新增
  production vs shadow 离线比较；当前阶段只允许比较，不允许采用、promotion、production
  profile 修改、broker/replay runner 或交易触发。
- 2026-05-19：从 `IN_PROGRESS` 改为 `VALIDATING`。原因：已新增 comparison builder、
  standalone script、JSON/Markdown 输出、dashboard 只读卡片、system flow / artifact catalog
  更新和测试；验证通过 018C 目标 pytest、dashboard pytest、`tests/trading_engine`、全量
  pytest、ruff 和 black check。
- 2026-05-19：从 `VALIDATING` 改为 `DONE`。原因：最终收尾验证通过手动 CLI smoke
  （生成 comparison JSON/Markdown）、dashboard 只读读取 smoke（删除源输入后仍读取最新
  comparison artifact）、目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest、
  ruff 和 black check；安全边界保持 `production_effect=none`、`manual_review_only=true`、
  无 broker runner、无 replay runner、无 production profile 写入、无 promotion。
