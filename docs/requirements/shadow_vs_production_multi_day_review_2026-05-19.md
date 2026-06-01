# TRADING-018C2：Multi-day Shadow vs Production Review

最后更新：2026-05-20

关联任务：`TRADING-018C2`

状态：`DONE`

## 背景

TRADING-018B 已维护 shadow-only weight state，TRADING-018C 已输出单日
production vs shadow comparison。当前缺少多日观察层，无法从最近 N 天的
018C comparison artifacts 中判断 shadow 权重是否稳定改善、是否带来额外风险，
以及是否存在过度敏感的 decision 差异。

## 范围

新增离线、只读、安全的 multi-day review pipeline：

- 新增 `scripts/run_shadow_vs_production_multi_day_review.py`。
- 新增核心 builder `daily_shadow_vs_production_multi_day_review.py`。
- 默认读取 `data/derived/weight_iterations/comparison/` 下最近 7 个日历日的
  `daily_shadow_vs_production_YYYY-MM-DD.json`。
- 输出：
  - `data/derived/weight_iterations/comparison/reviews/shadow_vs_production_review_YYYY-MM-DD.json`
  - `data/derived/weight_iterations/comparison/reviews/shadow_vs_production_review_YYYY-MM-DD.md`
- Daily task dashboard 新增只读 `Shadow vs Production Multi-day Review` 卡片，
  只读取 latest review artifact，不重新运行 pipeline。

## 安全边界

- `production_effect="none"`。
- `manual_review_only=true`。
- 不写 `config/weights/weight_profile_current.yaml`。
- 不写 approved profile。
- 不做 promotion。
- 不运行 broker、paper runner 或 replay runner。
- Dashboard 不触发 scoring、comparison、review、broker、paper 或 replay runner。

## Review Policy

本阶段 policy 是 pilot review baseline，只用于人工观察，不产生 promotion
readiness。阈值需要在进入 TRADING-018D manual promotion gate 前重新复核。

- 最少可比较天数：3 天。
- safety blocked 天数达到 2 天及以上时，review decision 为 `SAFETY_BLOCKED`。
- shadow 平均 `score_delta` 大于 0，且风险 flag 没有净增加，且 decision
  difference 比例不超过 50%，则输出 `SHADOW_LOOKS_BETTER`。
- shadow 平均 `score_delta` 小于 0，或风险 flag 有净增加，则输出
  `SHADOW_LOOKS_WORSE`。
- 其他情况继续观察。

允许的 `review_decision`：

- `CONTINUE_OBSERVATION`
- `SHADOW_LOOKS_BETTER`
- `SHADOW_LOOKS_WORSE`
- `INSUFFICIENT_HISTORY`
- `SAFETY_BLOCKED`
- `ERROR`

## 验收标准

1. comparison artifacts 不足 3 天时输出 `INSUFFICIENT_HISTORY`。
2. 多天 shadow score 更好且风险没有增加时输出 `SHADOW_LOOKS_BETTER`。
3. 多天 shadow score 更差时输出 `SHADOW_LOOKS_WORSE`。
4. safety blocked 天数过多时输出 `SAFETY_BLOCKED`。
5. 缺失部分日期 artifact 时仍生成 review，并记录 `missing_comparison_days`。
6. 输出永远保持 `production_effect="none"`。
7. 输出永远保持 `manual_review_only=true`。
8. 运行不会写 production profile。
9. Dashboard 只读读取 review artifact，不重跑 pipeline。

## 进展记录

- 2026-05-19：新增并进入 `IN_PROGRESS`。Owner 要求在 TRADING-018C 单日
  comparison 后新增多日 review evidence 层；本阶段只允许人工观察，不允许
  promotion 或 production 权重修改。
- 2026-05-19：从 `IN_PROGRESS` 改为 `VALIDATING`。已实现 multi-day review
  builder、standalone script、JSON/Markdown 输出、dashboard 只读卡片、runbook、
  系统流图、产物目录和测试。验证通过：
  `python -m pytest tests/trading_engine/test_shadow_vs_production_multi_day_review.py -q`、
  `python -m pytest tests/test_daily_task_dashboard.py -q`、
  `python -m pytest tests/trading_engine -q`、`python -m pytest -q`、
  `python -m ruff check scripts src tests` 和
  `python -m black --check scripts src tests`。
- 2026-05-20：从 `VALIDATING` 改为 `DONE`。最终收尾验证通过：手动运行
  `scripts/run_shadow_vs_production_multi_day_review.py --date 2026-05-19 --lookback-days 7`
  生成 review JSON/Markdown；当前本地没有可用 comparison artifacts，因此 review decision
  为 `INSUFFICIENT_HISTORY` 且 `available_comparison_days=0`；dashboard 只读读取该
  review artifact，未改写 review mtime，也未生成 comparison artifact；安全边界保持
  `production_effect="none"`、`manual_review_only=true`、promotion readiness false，
  pipeline contract 不运行 scoring/comparison/broker/paper/replay，不写 production 或
  approved profile。
