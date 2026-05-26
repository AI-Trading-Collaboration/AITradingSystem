# TRADING-040 SEC PIT Cognitive Evaluation Loop

关联任务：`TRADING-040`

状态：`VALIDATING`

## 背景

`TRADING-039` 已完成 SEC EDGAR reconstructed filing-time PIT backfill，系统可以生成
`data/processed/sec_edgar/sec_pit_feature_panel.csv` 并在回测和每日评分中作为可选
SEC PIT 基本面输入。当前缺口是数据底座之后的认知评估闭环：系统还不能稳定回答
SEC PIT feature 是否提升策略判断、哪些 feature 具备解释力、哪些 feature 应进入
shadow weight iteration、哪些 feature 只能保持 research-only 或排除。

本任务建立 evaluation/report-only 闭环。它不修改 production scoring rules、weight
profile、portfolio、broker、TRADING-039 backfill artifact 或历史缓存。

## 默认市场阶段

默认解释窗口使用 `ai_after_chatgpt`：

- anchor event：ChatGPT public launch on 2022-11-30；
- default backtest start：2022-12-01；
- 报告必须展示实际请求日期范围和所选 market regime；
- 如果请求起点早于 regime 起点，报告必须说明 pre-regime 数据只能作为压力/对比样本。

## CLI

目标命令：

```bash
aits sec-pit evaluate \
  --start 2023-01-01 \
  --end 2026-05-26 \
  --feature-panel data/processed/sec_pit/sec_pit_feature_panel.csv \
  --universe config/sec_companies.yaml \
  --benchmark QQQ \
  --output-dir outputs/sec_pit_evaluation
```

实现时允许默认路径指向 TRADING-039 当前目录：
`data/processed/sec_edgar/sec_pit_feature_panel.csv`。示例中的
`data/processed/sec_pit/...` 作为显式输入路径保留兼容。

## Evaluation Scope

闭环输入：

- SEC PIT feature panel；
- SEC company universe；
- benchmark ticker；
- cached market prices and macro rates；
- `config/market_regimes.yaml`；
- `config/sec_pit_evaluation_policy.yaml`。

输出：

- `sec_pit_evaluation_<start>_<end>.json`；
- `sec_pit_evaluation_<start>_<end>.md`；
- `sec_pit_feature_contributions_<start>_<end>.csv`；
- `sec_pit_shadow_candidates_<start>_<end>.csv`；
- `run.log`。

## Data Quality Gate

`aits sec-pit evaluate` 必须在读取缓存价格和宏观数据后运行与
`aits validate-data` 相同的 `validate_data_cache` 代码路径，写出数据质量报告，并在
`FAIL` 时停止。Markdown 和 JSON 必须展示 data quality status 和报告路径。

## PIT Safety

评估必须检查：

- `decision_date` 在请求窗口内；
- `max_input_available_time_utc` 不晚于 `decision_date`；
- feature panel 必需字段完整；
- 覆盖率按 expected ticker-day 分母计算；
- 输出明确标识 `backtest_data_grade=B_RECONSTRUCTED_SEC_FILING_PIT`、不是 strict vendor
  archive、`production_effect=none`。

## Policy And Thresholds

解释性阈值必须来自 `config/sec_pit_evaluation_policy.yaml`，并在报告中展示 policy
version/status。首版为 `pilot_baseline`，用于筛选 shadow 观察候选，不得作为 production
权重晋级证据。

## Feature Evaluation

每个 `feature_id` 至少输出：

- observation count；
- ticker coverage；
- decision-day coverage；
- future-return matched row count；
- mean daily rank IC versus benchmark excess return；
- IC sign stability by month；
- top-minus-bottom forward excess return spread；
- PIT safety issue count；
- classification：`SHADOW_CANDIDATE`、`RESEARCH_ONLY` 或 `EXCLUDED`；
- classification reason；
- recommended action。

## Acceptance Criteria

1. `aits sec-pit evaluate` 支持目标参数并生成 JSON/Markdown/CSV/run log。
2. 数据质量门禁失败时命令返回非零退出码，不生成成功评价结论。
3. PIT future availability 泄漏被拦截并在报告中标为 safety blocked。
4. 报告用中文展示 market regime、实际日期、PIT 安全性、覆盖、signal contribution、shadow
   candidate、research-only/excluded 和限制。
5. 解释阈值来自 policy config 或命名常量，不出现未说明的投资解释硬编码阈值。
6. `docs/system_flow.md` 和 `docs/artifact_catalog.md` 同步新增 evaluation loop。
7. 专项测试覆盖核心分类、PIT 泄漏、数据质量门禁和 CLI artifact 输出。

## Progress Log

- 2026-05-26：新增需求文档和 task register 项；状态为 `IN_PROGRESS`。
- 2026-05-26：完成核心实现并进入 `VALIDATING`：新增
  `src/ai_trading_system/fundamentals/sec_pit_evaluation.py`、`aits sec-pit evaluate`、
  `config/sec_pit_evaluation_policy.yaml`、evaluation JSON/Markdown、feature contribution
  CSV、shadow candidate CSV、run log、system flow 和 artifact catalog 更新。
- 2026-05-26：验证通过 `tests/test_sec_pit_evaluation.py`、`tests/test_sec_pit_backfill.py`、
  `tests/test_backtest.py`、全量 `python -m pytest -q`（1262 passed, 1 warning）和全量
  `python -m ruff check`。
