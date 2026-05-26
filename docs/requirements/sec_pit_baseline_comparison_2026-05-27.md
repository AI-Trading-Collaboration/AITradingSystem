# TRADING-041: SEC PIT Baseline Comparison & Decision Impact Report

## 背景

TRADING-039 已建立 SEC EDGAR reconstructed filing-time PIT 基本面数据底座，
TRADING-040 已建立 SEC PIT cognitive evaluation loop，输出 feature effectiveness、
signal attribution 和 shadow candidate weights。

当前缺口是 decision-level 对比：TRADING-040 能说明某个 SEC PIT feature 是否有
离线解释力，但还不能回答它相对 price-only baseline 和现有 `score-daily` baseline
是否改善排序、回撤规避和最终人工复核判断。

## 目标

新增只读比较流水线：

```bash
aits sec-pit compare-baseline \
  --start 2023-01-01 \
  --end 2026-05-26 \
  --sec-pit-evaluation-dir outputs/sec_pit_evaluation \
  --baseline-score-dir outputs/daily_score \
  --benchmark QQQ \
  --output-dir outputs/sec_pit_baseline_comparison
```

流水线比较：

- price-only baseline；
- existing score-daily baseline；
- SEC PIT enhanced evaluation output。

## 输出

- `outputs/sec_pit_baseline_comparison/sec_pit_baseline_comparison_summary_YYYY-MM-DD.json`
- `outputs/sec_pit_baseline_comparison/sec_pit_baseline_comparison_summary_YYYY-MM-DD.md`
- `outputs/sec_pit_baseline_comparison/sec_pit_decision_impact_YYYY-MM-DD.csv`
- `outputs/sec_pit_baseline_comparison/sec_pit_rank_shift_YYYY-MM-DD.csv`
- `outputs/sec_pit_baseline_comparison/sec_pit_incremental_alpha_YYYY-MM-DD.csv`

## 实施拆解

1. 增加 `sec_pit_baseline_comparison` 核心模块，负责 artifact discovery、schema
   normalization、degraded mode、排序对齐、bucket 统计和 Markdown/JSON/CSV 输出。
2. 扩展 `aits sec-pit compare-baseline` CLI，默认输出到
   `outputs/sec_pit_baseline_comparison`。
3. 扩展 daily task dashboard，只读读取 latest comparison summary，不运行比较、不读取市场
   数据、不修改 production artifact。
4. 增加专项测试覆盖 schema、missing baseline、missing SEC PIT evaluation、
   insufficient overlap、safety fields、dashboard read-only 和 deterministic output。
5. 同步 `docs/artifact_catalog.md`、`docs/system_flow.md`、`docs/learning_path.md`、
   `docs/runbooks/sec_pit_evaluation.md`、`docs/runbooks/sec_pit_baseline_comparison.md`、
   `config/data_sources.yaml` 和 `config/feature_availability.yaml`。

## 状态与安全边界

- 状态枚举：`OK`、`LIMITED_BASELINE_MISSING`、
  `LIMITED_SEC_PIT_EVALUATION_MISSING`、`INSUFFICIENT_OVERLAP`、
  `FAILED_VALIDATION`。
- 缺少 baseline 或 SEC PIT evaluation artifact 时，默认生成 LIMITED report；
  `--strict` 时失败。
- 所有 decision impact 行必须固定：
  `manual_review_required=true`、`production_effect=none`。
- 不得修改 production weights、production actions、approved overlay、prediction ledger 或
  score-daily 输出。
- 不得纳入 `available_time > decision_date` 的 SEC PIT attribution 行。
- 报告必须披露 `B_RECONSTRUCTED_SEC_FILING_PIT` 限制、样本限制、baseline 缺失限制和
  market regime 限制。

## 验收标准

- `aits sec-pit compare-baseline` 可生成 JSON、Markdown 和三份 CSV artifact。
- 缺失输入默认降级输出，`--strict` 才失败。
- summary JSON schema 稳定，Markdown 包含 Metadata、Executive Summary、
  Incremental Alpha、Decision Impact、Feature Drivers、PIT Safety、Limitations 和
  Manual Review Checklist。
- dashboard 卡片只读读取 comparison summary，并展示 latest comparison date、status、
  decision count、action changed count、material rank shift count、incremental alpha 20d、
  drawdown improvement 20d、top promoted tickers 和 top downgraded tickers。
- 专项测试和 dashboard 测试通过；全量验证执行或记录已有无关阻塞。

## 进展记录

- 2026-05-27：新增需求文档和任务登记，状态 `IN_PROGRESS`。
- 2026-05-27：实现完成并归档为 `DONE`。验证通过专项 pytest、dashboard pytest、全量
  pytest、全量 Ruff 和触达 Python 文件 Black check；所有输出保持 `production_effect=none`
  和 `manual_review_required=true`。
