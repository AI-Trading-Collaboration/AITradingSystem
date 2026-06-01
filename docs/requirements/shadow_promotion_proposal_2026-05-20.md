# TRADING-018D：Manual Shadow-to-Production Promotion Proposal Gate

最后更新：2026-05-20

关联任务：`TRADING-018D`

状态：`DONE`

## 背景

TRADING-018B 已维护 `current_shadow_weights.json`，TRADING-018C 已输出单日
shadow vs production comparison，TRADING-018C2 已输出多日 review。当前仍缺少一个
明确的 manual promotion proposal gate，用于把这些只读证据汇总成一份人工审核材料。

本任务不执行 promotion，也不允许修改 production profile。

## 范围

新增离线、只读、安全的 proposal pipeline：

- 新增 `scripts/run_shadow_promotion_proposal.py`。
- 新增核心模块 `src/ai_trading_system/trading_engine/shadow_promotion_proposal.py`。
- 默认读取：
  - `data/derived/weight_iterations/shadow/current_shadow_weights.json`
  - `config/weights/weight_profile_current.yaml`
  - 最新 `data/derived/weight_iterations/comparison/reviews/shadow_vs_production_review_*.json`
  - 最近 N 日 `data/derived/weight_iterations/comparison/daily_shadow_vs_production_*.json`
  - 最近 TRADING-018B candidate/history artifact
- 输出：
  - `data/derived/weight_iterations/promotion/proposals/shadow_promotion_proposal_YYYY-MM-DD.json`
  - `data/derived/weight_iterations/promotion/proposals/shadow_promotion_proposal_YYYY-MM-DD.md`
  - `data/derived/weight_iterations/promotion/logs/shadow_promotion_proposal_run_YYYY-MM-DD.json`
  - `data/derived/weight_iterations/promotion/logs/shadow_promotion_proposal_run_YYYY-MM-DD.md`
- Daily task dashboard 新增 `Shadow Promotion Proposal` 只读卡片，只读取 proposal artifact。

## 安全边界

所有输出必须固定：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "promotion_executed": false,
  "safe_for_production": false
}
```

禁止：

- 修改 `config/weights/weight_profile_current.yaml`。
- 写 production weights 或 approved profile。
- 自动 promotion。
- 调用 broker、paper runner 或 replay runner。
- 触发任何交易执行。
- Dashboard 重新运行 018B、018C、018C2、promotion proposal、scoring、broker 或 replay。

## Proposal Decision Policy

允许的 `proposal_decision`：

- `PROPOSE_FOR_MANUAL_REVIEW`
- `CONTINUE_OBSERVATION`
- `INSUFFICIENT_HISTORY`
- `INSUFFICIENT_DATA`
- `SAFETY_BLOCKED`
- `REJECT_SHADOW`
- `ERROR`

只有同时满足以下条件时，才允许 `PROPOSE_FOR_MANUAL_REVIEW`：

- latest TRADING-018C2 review 存在且 `review_decision=SHADOW_LOOKS_BETTER`。
- `available_comparison_days >= 5`。
- `insufficient_data_days = 0`。
- `safety_blocked_days = 0`。
- `average_score_delta > 0`。
- `shadow_risk_flag_delta_total <= 0`。
- `decision_difference_count` 不超过 proposal policy 阈值。
- production weight keys 与 shadow weight keys 完全一致。
- shadow weights sum 约等于 1.0。
- `production_effect=none` 且 `manual_review_only=true`。

如果 production/shadow weight keys 不一致，必须输出 `INSUFFICIENT_DATA` 并披露
`missing_in_shadow` 与 `missing_in_production`；禁止静默 alias mapping。

## 阶段拆解

|阶段|状态|验收|
|---|---|---|
|1. 登记和需求文档|DONE|task register 指向本文，本文记录范围、输入、输出和安全边界。|
|2. 核心 proposal builder|DONE|读取 shadow、production、018C2、018C 和 018B artifacts，输出 proposal decision。|
|3. CLI 脚本|DONE|`python scripts/run_shadow_promotion_proposal.py --date YYYY-MM-DD` 可运行。|
|4. Dashboard 只读卡片|DONE|展示 latest proposal decision、安全字段和 Markdown path，不重跑 pipeline。|
|5. 文档更新|DONE|system flow、artifact catalog、runbook 更新。|
|6. 测试验证|DONE|目标 pytest、dashboard 测试、ruff、black 通过。|

## 验收命令

```powershell
python -m pytest tests/trading_engine/test_shadow_promotion_proposal.py -q
python -m pytest tests/test_daily_task_dashboard.py -q
python -m pytest tests/trading_engine -q
python -m pytest -q
python -m ruff check scripts src tests
python -m black --check scripts src tests
```

## 进展记录

- 2026-05-20：新增并进入 `IN_PROGRESS`。Owner 要求在 TRADING-018B/018C/018C2 后
  新增 manual shadow-to-production promotion proposal gate；本阶段只生成人工审核 proposal，
  禁止自动 apply、production profile 写入、broker/replay/交易触发。
- 2026-05-20：从 `IN_PROGRESS` 改为 `VALIDATING`。已实现 policy manifest、
  核心 proposal builder、standalone script、JSON/Markdown proposal 与 run log、
  dashboard 只读卡片、runbook、system flow / artifact catalog 更新和测试。验证通过：
  `python -m pytest tests/trading_engine/test_shadow_promotion_proposal.py -q`、
  `python -m pytest tests/test_daily_task_dashboard.py -q`、
  `python -m pytest tests/trading_engine -q`、`python -m pytest -q`、
  `python -m ruff check scripts src tests` 和
  `python -m black --check scripts src tests`。
- 2026-05-20：从 `VALIDATING` 改为 `DONE`。最终收尾验证通过：手动运行
  `python scripts/run_shadow_promotion_proposal.py --date 2026-05-20 --lookback-days 7`
  生成 proposal JSON/Markdown；当前本地没有 current shadow / latest review artifacts，
  因此 proposal decision 为 `INSUFFICIENT_DATA` 且 `available_comparison_days=0`；安全边界保持
  `production_effect="none"`、`manual_review_only=true`、`promotion_executed=false`、
  `safe_for_production=false`；dashboard smoke 只读读取 proposal artifact，import guard、
  proposal mtime 和上游 shadow/comparison artifact 列表确认未重跑 018B/018C/018C2/018D。
  收尾验证通过目标 pytest、dashboard pytest、`tests/trading_engine`、全量 pytest、ruff 和
  black check。
