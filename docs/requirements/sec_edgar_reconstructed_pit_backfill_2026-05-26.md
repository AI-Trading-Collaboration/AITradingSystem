# TRADING-039 SEC EDGAR Reconstructed PIT Backfill

关联任务：`TRADING-039`

## 背景

`TRADING-035` 已被 Notification Delivery Audit Summary 占用，且当前登记表已使用
`TRADING-036`、`TRADING-037`、`TRADING-038`。本任务按 owner 要求顺延为
`TRADING-039`。

系统已有 forward-only PIT snapshot 纪律，但 2023 至今的基本面回测仍缺少可审计的
filing-time reconstructed PIT 输入。本任务用 SEC EDGAR 当前可下载的 submissions
metadata 与 companyfacts XBRL facts，重建保守的日频 as-of 基本面层。任意
decision date 只能读取 `available_for_signal_date <= decision_date` 的 filing/fact/feature。

## 等级和边界

TRADING-039 交付的数据默认标记为
`B_RECONSTRUCTED_SEC_FILING_PIT`。它可用于方向性回测、认知模型迭代、shadow
评分和输入可见性审计，但不得声明为商业供应商级 strict PIT archive。

安全边界固定为：

- `production_effect=none`
- `backtest_data_grade=B_RECONSTRUCTED_SEC_FILING_PIT`
- `strict_vendor_archive=false`
- `external_side_effects=false`
- `broker_access_required=false`
- `paid_vendor_required=false`
- `retroactive_strict_pit=false`
- `manual_review_required_for_grade_upgrade=true`

本任务不做实盘交易，不调用 broker，不读取真实券商 API key，不改变 paper trading
下单路径，不自动批准规则或权重。

## 目标

1. 从 SEC EDGAR 免费公开接口下载并缓存目标 universe 的 submissions 与 companyfacts
   raw JSON，并写入 manifest、row count 和 checksum。
2. 生成 filing timeline：ticker、CIK、accession、form、filing/report date、
   acceptance datetime、available time、primary document、XBRL flags、source URL、
   raw checksum、PIT grade 和 confidence。
3. 将 companyfacts 展平成 fact long 表，并通过 `accn/accession` join 到 filing
   timeline，得到 fact-level `available_time_utc` 和 `available_for_signal_date`。
4. 基于 `config/fundamental_metrics.yaml` 与 `config/fundamental_features.yaml` 生成
   selected metrics、PIT intervals、daily metric panel 和 feature panel。
5. 在 loader、validation、backtest/score 接入中强制使用
   `available_for_signal_date <= decision_date`，禁止只按 fiscal period end 或
   filing period 合并。
6. 输出 JSON/CSV/Markdown validation artifacts、manifest、coverage report 和
   as-of leakage report。
7. 接入 CLI、feature availability catalog、data source catalog、system flow、
   artifact catalog、requirements index 和测试体系。

## SEC fair access

请求必须设置合规 `User-Agent`，默认 `max_requests_per_second=5`，低于 SEC 公布的
10 requests/second 上限；遇到 `429` / `403` 使用指数退避。请求和响应必须经由现有
`external_request_cache`，缓存 metadata 不保存 User-Agent 原文、API key、token 或
Cookie。

## 初始范围

默认 universe 来自 `config/sec_companies.yaml` 的 active companies。第一版 metric panel
只纳入 `10-K`、`10-K/A`、`10-Q`、`10-Q/A`、`20-F`、`20-F/A`、`40-F`、`40-F/A`。
`8-K` 只进入 filing timeline，不进入 metric panel。`6-K` 默认为
`C_CURRENT_HISTORY_APPROX`，除非未来单独建立可审计规则。

## 产物

```text
data/raw/sec_edgar/submissions/{ticker}_{cik}_submissions.json
data/raw/sec_edgar/companyfacts/{ticker}_{cik}_companyfacts.json
data/raw/sec_edgar/manifest/sec_edgar_raw_manifest.csv

data/processed/sec_edgar/filing_timeline.csv
data/processed/sec_edgar/xbrl_facts_long.csv
data/processed/sec_edgar/mapped_metrics_long.csv
data/processed/sec_edgar/fundamental_pit_intervals.csv
data/processed/sec_edgar/fundamental_pit_daily_panel.csv
data/processed/sec_edgar/sec_pit_feature_panel.csv
data/processed/sec_edgar/sec_pit_coverage_summary.csv

outputs/reports/sec_pit_backfill/sec_pit_backfill_YYYY-MM-DD.md
outputs/reports/sec_pit_backfill/sec_pit_leakage_check_YYYY-MM-DD.md
outputs/reports/sec_pit_backfill/sec_pit_coverage_YYYY-MM-DD.md
outputs/reports/sec_pit_backfill/sec_pit_validation_YYYY-MM-DD.json
outputs/reports/sec_pit_backfill/run.log
```

## CLI

```powershell
aits sec-pit fetch-raw --ticker NVDA --ticker MSFT --ticker AMD --use-cache
aits sec-pit build-filing-timeline --from 2023-01-01 --to 2026-05-25
aits sec-pit build-facts --to 2026-05-25
aits sec-pit build-metrics --to 2026-05-25
aits sec-pit build-panel --from 2023-01-01 --to 2026-05-25
aits sec-pit validate --as-of 2026-05-25
aits sec-pit backfill --from 2023-01-01 --to 2026-05-25 --full-pipeline
```

Exit codes:

- `0`: success or business-level warning with artifacts written
- `1`: fatal exception, unreadable config, impossible schema, write failure
- `2`: validation FAIL

## 验证要求

- 原始 payload 存在且 checksum 匹配 manifest。
- 每个 B-grade fact 必须有 accession 且 join 到 filing timeline。
- feature panel 不得包含 `available_for_signal_date > decision_date`。
- selected metric 不得使用 whitelist 之外的 source form。
- ratio feature 不得在没有 PIT FX source 时混用不同 currency/unit。
- 同一 ticker/metric/period_type/period_end 不得存在 overlapping active intervals。
- stale feature warning：quarterly 超过 180 天，annual 超过 540 天。
- amendment/restatement 只在自身 `available_for_signal_date` 起影响后续 decision date。
- coverage 默认 warning 阈值 70%，core ticker error 阈值 40%。

## 阶段

| 阶段 | 状态 | 验收标准 |
|---|---|---|
| 1. 需求和配置 | DONE | 新增本需求文档、`config/sec_pit_backfill.yaml`、requirements index、task register、data source catalog。 |
| 2. Raw fetch | DONE | 下载 submissions/companyfacts，走 external request cache，写 manifest 和 checksums，可用 mock 测试。 |
| 3. Timeline + facts | DONE | 生成 filing timeline 与 facts long，accn join、fallback/downgrade 规则正确。 |
| 4. Metric mapping | DONE | 按 `fundamental_metrics.yaml` 生成 selected metrics，保留候选和 restatement lineage。 |
| 5. PIT intervals + panel | DONE | 生成 intervals、daily panel、features，强制 `available_for_signal_date <= decision_date`。 |
| 6. Validation/report | DONE | 输出 coverage/leakage/report，validation FAIL 可阻断下游。 |
| 7. Backtest/score 接入 | DONE | fundamentals source 可使用新 PIT panel；报告展示 PIT grade 与 coverage。 |
| 8. 文档和测试 | DONE | 更新 docs/schema、artifact catalog、system flow、learning path；目标测试、全量 pytest、ruff、black、diff check 通过或记录既有无关阻断。 |

## 状态记录

- 2026-05-26：新增并进入 `IN_PROGRESS`。原因：owner 提供外部开发文档，要求完成
  SEC EDGAR reconstructed PIT backfill 的开发、测试和修复闭环；`TRADING-035` 至
  `TRADING-038` 已占用，因此顺延为 `TRADING-039`。
- 2026-05-26：完成第一轮实现：新增 `sec_pit_backfill`、filing timeline、XBRL
  facts long、metric mapping、PIT intervals/daily panel/feature panel、validation
  artifacts、`aits sec-pit` CLI、policy config、data source catalog、feature
  availability、system flow、artifact catalog、schema 和 learning path 更新；进入测试与修复。
- 2026-05-26：从 `IN_PROGRESS` 改为 `DONE`。原因：完成 score-daily/backtest 可选
  `sec_pit_feature_panel` 接入、全链路 mocked SEC JSON CLI 测试、PIT 行为专项测试和
  既有 SEC/FMP/PIT/backtest 回归；最终验证通过全量 `python -m pytest -q`
  （1258 passed, 1 warning）、全量 `python -m ruff check` 和本次触达文件
  `python -m black --check ...`。
