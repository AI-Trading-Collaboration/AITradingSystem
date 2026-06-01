# TRADING-040 SEC PIT Cognitive Evaluation Loop

最后更新：2026-05-26

关联任务：`TRADING-040`

状态：`VALIDATING`

## 背景

`TRADING-039` 已完成 SEC EDGAR reconstructed filing-time PIT backfill。系统可以生成
`data/processed/sec_edgar/sec_pit_feature_panel.csv`，并在 backtest / score-daily 中作为可选
SEC PIT 基本面输入。TRADING-040 将该数据底座推进到可审计的认知评估闭环：

- 回测标签匹配；
- feature effectiveness 分析；
- signal attribution；
- shadow candidate weight 建议。

本任务严格 report-only / observe-only，不修改 production score 权重、approved overlay、
broker、portfolio、TRADING-039 artifact 或历史缓存。

## 输入

- SEC PIT feature panel：默认 `data/processed/sec_edgar/sec_pit_feature_panel.csv`，CLI 可用
  `--feature-panel` 覆盖；
- SEC universe：默认 `config/sec_companies.yaml`，可用 `--tickers` 覆盖；
- market / macro cache：复用 `data/raw/prices_daily.csv` 和 `data/raw/rates_daily.csv`；
- benchmark：默认 `QQQ`；
- evaluation policy：`config/sec_pit_evaluation.yaml`。

## PIT Safety Policy

主评估只允许进入满足以下条件的行：

```text
available_time <= decision_time
```

如果输入缺少 `available_time`，该行必须排除；如果 `available_time` 晚于 `decision_time`，
该行必须排除并计入 `pit_violation_count`。不得使用 `period_end <= decision_time` 替代可见性
条件。

SEC reconstructed PIT 等级固定披露为：

```text
B_RECONSTRUCTED_SEC_FILING_PIT
```

缺少 `accession_number`、`accepted_datetime`、`filed_date` 或 `raw_sha256` 的行会降低
`pit_quality_score`，并阻止进入 `PROMOTE_TO_SHADOW`。

## Evaluation Method

每个 `decision_date` / `feature_id` / `metric_id` 做横截面 winsorize p01 / p99 和 z-score。
当某日有效 ticker 数低于 `min_valid_ticker_count` 时，不计算该日 IC。

核心指标：

- `ic_1d` / `ic_5d` / `ic_20d` / `ic_60d`；
- `rank_ic_20d`；
- `hit_rate_20d`；
- top 20% minus bottom 20% forward return spread；
- top quantile 20d forward max drawdown；
- coverage、stability、PIT quality。

推荐值只允许：

```text
PROMOTE_TO_SHADOW
KEEP_RESEARCH_ONLY
DOWNWEIGHT
EXCLUDE_INSUFFICIENT_DATA
```

所有 shadow weight 输出必须包含：

```text
manual_review_required = true
production_effect = none
```

## CLI

```bash
aits sec-pit evaluate \
  --start 2023-01-01 \
  --end 2026-05-26 \
  --feature-panel data/processed/sec_edgar/sec_pit_feature_panel.csv \
  --universe config/sec_companies.yaml \
  --benchmark QQQ \
  --output-dir outputs/sec_pit_evaluation
```

也可通过 `scripts/run_sec_pit_evaluation.py` 运行同一核心代码路径。

## Artifacts

按 evaluation `--end` 日期命名：

```text
outputs/sec_pit_evaluation/sec_pit_evaluation_summary_YYYY-MM-DD.json
outputs/sec_pit_evaluation/sec_pit_evaluation_summary_YYYY-MM-DD.md
outputs/sec_pit_evaluation/sec_pit_feature_effectiveness_YYYY-MM-DD.csv
outputs/sec_pit_evaluation/sec_pit_signal_attribution_YYYY-MM-DD.csv
outputs/sec_pit_evaluation/sec_pit_shadow_candidate_weights_YYYY-MM-DD.csv
outputs/sec_pit_evaluation/run.log
```

Dashboard 只读读取 `sec_pit_evaluation_summary_*.json`，不运行 evaluation、不读取 market data、
不修改权重。

## Acceptance Criteria

1. `aits sec-pit evaluate` 可生成 JSON / Markdown / 3 个 CSV artifact。
2. 数据质量门禁调用 `validate_data_cache`，失败时停止成功评价并返回非零 CLI exit code。
3. `available_time > decision_time` 和缺少 `available_time` 的行不得进入主评估。
4. provenance 缺口会降低 `pit_quality_score`，不得被 promoted。
5. `feature_effectiveness`、`signal_attribution` 和 `shadow_candidate_weights` schema 稳定。
6. Shadow candidate weights 固定 `manual_review_required=true`、`production_effect=none`。
7. Markdown summary 展示 Metadata、Data Coverage、Feature Effectiveness、Shadow Candidate
   Weights、PIT Safety Checks、Interpretation 和 Manual Review Checklist。
8. Daily task dashboard 新增 SEC PIT Evaluation Summary 只读卡片。
9. 样本不足时 recommendation 必须为 `EXCLUDE_INSUFFICIENT_DATA` 或 `KEEP_RESEARCH_ONLY`。
10. 重复运行的 CSV 输出保持 deterministic。

## Progress Log

- 2026-05-26：新增需求文档和 task register 项；状态为 `IN_PROGRESS`。
- 2026-05-26：基础版进入 `VALIDATING`，但 artifact 命名和 schema 仍为
  feature contribution / shadow candidate 旧口径。
- 2026-05-26：按 owner 最新规格重构为 summary/effectiveness/attribution/shadow weight
  artifact，新增 dashboard 只读卡片、`config/sec_pit_evaluation.yaml` 和专项测试；目标测试通过
  `tests/trading_engine/test_sec_pit_evaluation.py`。
- 2026-05-26：验证通过 `tests/trading_engine/test_sec_pit_evaluation.py`、
  `tests/test_daily_task_dashboard.py`、全量 `python -m pytest -q`（1269 passed, 1 warning）
  和全量 `python -m ruff check config src tests scripts docs`；本次触达 Python 文件 black
  check 通过。全仓 `python -m black --check config src tests scripts docs` 仍仅被既有
  `tests/test_market_data.py` baseline 阻断。
