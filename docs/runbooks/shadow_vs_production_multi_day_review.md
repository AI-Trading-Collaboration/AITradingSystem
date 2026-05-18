# Shadow vs Production Multi-day Review Runbook

适用任务：`TRADING-018C2`

## 目的

从最近 N 天既有 `daily_shadow_vs_production_YYYY-MM-DD.json` artifacts 中生成
多日 shadow vs production 观察报告。该报告只提供人工复核证据，不允许 promotion
或 production 权重修改。

## 输入

默认读取：

```text
data/derived/weight_iterations/comparison/daily_shadow_vs_production_YYYY-MM-DD.json
```

默认窗口为 7 个日历日，截至 `--date`。

## 手动运行

```bash
python scripts/run_shadow_vs_production_multi_day_review.py --date YYYY-MM-DD
```

可选参数：

```bash
python scripts/run_shadow_vs_production_multi_day_review.py \
  --date YYYY-MM-DD \
  --lookback-days 7 \
  --data-root data
```

## 输出

```text
data/derived/weight_iterations/comparison/reviews/
  shadow_vs_production_review_YYYY-MM-DD.json
  shadow_vs_production_review_YYYY-MM-DD.md
```

JSON 顶层固定包含：

- `production_effect="none"`
- `manual_review_only=true`
- `promotion_readiness.ready=false`
- `audit.safe_for_production=false`

## Review Decision

允许值：

- `CONTINUE_OBSERVATION`
- `SHADOW_LOOKS_BETTER`
- `SHADOW_LOOKS_WORSE`
- `INSUFFICIENT_HISTORY`
- `SAFETY_BLOCKED`
- `ERROR`

当前 pilot policy：

- 可比较 comparison 天数少于 3 天：`INSUFFICIENT_HISTORY`
- safety blocked 天数达到 2 天及以上：`SAFETY_BLOCKED`
- 平均 `score_delta` 为正、风险 flag 没有净增加、decision difference 不过频：
  `SHADOW_LOOKS_BETTER`
- 平均 `score_delta` 为负或风险 flag 净增加：`SHADOW_LOOKS_WORSE`
- 其他情况：`CONTINUE_OBSERVATION`

这些阈值只用于 TRADING-018C2 人工观察，不是 promotion gate。

## 安全边界

本脚本只读取既有 comparison artifacts，并只写 review JSON/Markdown。它不会：

- 运行 scoring pipeline
- 运行 TRADING-018C comparison pipeline
- 写 production profile
- 写 approved profile
- promotion shadow weights
- 运行 broker、paper runner 或 replay runner
- 改变 daily dashboard 主投资结论

## Dashboard

Daily task dashboard 的 `Shadow vs Production Multi-day Review` 卡片只读取 latest
review artifact，展示：

- latest `review_decision`
- `lookback_days`
- `available_comparison_days`
- `average_score_delta`
- `decision_difference_count`
- `promotion_readiness.ready`
- `production_effect`
- `manual_review_only`
- latest review Markdown path

Dashboard 不重跑 pipeline；缺 review artifact 时只显示 `MISSING`。
