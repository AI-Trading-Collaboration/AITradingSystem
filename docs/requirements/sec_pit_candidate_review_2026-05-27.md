# TRADING-043: SEC PIT Shadow Candidate Review & Evidence Pack

最后更新：2026-05-27

## 背景

TRADING-039 建立 SEC EDGAR reconstructed PIT backfill，TRADING-040 建立 SEC PIT
cognitive evaluation，TRADING-041 建立 SEC PIT baseline comparison，TRADING-042 /
TRADING-042A 修复真实运行诊断和 remediation 问题。

2026-05-26 真实端到端 SEC PIT run remediation 后显示：

- `diagnostics_status=OK`
- downstream missing provenance = 0
- coverage ratio > 1 features = 0
- duplicate observations = 0
- `drawdown_label_coverage=0.9444`
- `max_drawdown_forward_20d` range = [-0.0975, 0.0]
- `capex_intensity` 进入 manual-review shadow candidate 状态
- `production_effect=none`
- production weights 和 active shadow weights 均未修改

下一步不是自动 promotion，而是为 `capex_intensity` 等 SEC PIT shadow candidate
生成可人工复核的 evidence pack，判断是否适合进入 observe-only shadow iteration。

## 目标

新增 `aits sec-pit review-candidates`，只读读取 TRADING-040/041/042 artifacts，
生成 candidate evidence pack、by-ticker / by-period 拆解、baseline overlap 分析和
observe-only shadow proposal。该流水线必须清楚区分 evidence 和 recommendation，
且不得自动修改 production weights 或 active shadow weights。

## 阶段拆解

|阶段|优先级|内容|验收标准|
|---|---|---|---|
|1|P0|核心 review artifact resolver 与 degraded mode|支持显式 `--start/--end` artifact 目录和 `--latest`；缺 evaluation/comparison/diagnostics 时输出 schema 完整的 LIMITED / INSUFFICIENT report。|
|2|P0|Candidate evidence 与 proposal|输出 candidate evidence CSV 和 shadow proposal CSV；所有行固定 `manual_review_required=true`、`review_required=true`、`production_effect=none`；`suggested_observe_only_weight <= max_allowed_initial_weight`；不写 production 或 active shadow config。|
|3|P1|By-ticker / by-period / baseline overlap|基于 TRADING-040 attribution 和 TRADING-041 decision impact 生成 ticker concentration、period stability 和 baseline redundancy evidence；baseline 字段缺失时降级为 `LIMITED_BASELINE_FIELDS_MISSING`。|
|4|P1|Summary JSON/Markdown 与 CLI|输出 summary JSON/Markdown、evidence CSV、by ticker/period/overlap/proposal CSV；Markdown 覆盖 metadata、executive summary、evidence、ticker、period、overlap、proposal、recommendation 和 manual review checklist。|
|5|P2|Dashboard 和文档|daily task dashboard 新增只读 `SEC PIT Candidate Review` 卡片；更新 artifact catalog、system flow、learning path 和 SEC PIT runbooks。|

## 输出产物

- `outputs/sec_pit_candidate_review/sec_pit_candidate_review_summary_YYYY-MM-DD.json`
- `outputs/sec_pit_candidate_review/sec_pit_candidate_review_summary_YYYY-MM-DD.md`
- `outputs/sec_pit_candidate_review/sec_pit_candidate_evidence_YYYY-MM-DD.csv`
- `outputs/sec_pit_candidate_review/sec_pit_candidate_by_ticker_YYYY-MM-DD.csv`
- `outputs/sec_pit_candidate_review/sec_pit_candidate_by_period_YYYY-MM-DD.csv`
- `outputs/sec_pit_candidate_review/sec_pit_candidate_overlap_with_baseline_YYYY-MM-DD.csv`
- `outputs/sec_pit_candidate_review/sec_pit_candidate_shadow_proposal_YYYY-MM-DD.csv`

## 安全边界

- 只读读取 TRADING-040/041/042 artifacts，不下载数据、不运行 evaluation/comparison/diagnostics。
- 不写 `config/weights/*`、scoring config、production weights、approved overlay 或 active shadow weights。
- 所有 review/proposal 输出固定 `production_effect=none`。
- 所有 candidate evidence 固定 `manual_review_required=true`。
- Shadow proposal 只表达人工复核建议；不得输出可自动执行的 production 或 shadow weight mutation。
- Dashboard 只读读取 latest summary JSON，不运行 review pipeline。

## 验证命令

目标验证：

```bash
python -m pytest tests/trading_engine/test_sec_pit_candidate_review.py -q
python -m pytest tests/test_daily_task_dashboard.py -q
python -m pytest tests/trading_engine/test_sec_pit_real_run_diagnostics.py -q
python -m ruff check config src tests scripts docs
python -m black --check <touched-python-files>
```

若时间允许，再运行全量：

```bash
python -m pytest -q
```

全仓 Black 仍可能被既有 `tests/test_market_data.py` 格式基线阻断；本任务不修 unrelated
formatting baseline。

## 进展记录

- 2026-05-27：新增并进入 `IN_PROGRESS`，原因：真实 SEC PIT remediation 后已有
  `capex_intensity` manual-review shadow candidate，但缺少可人工复核的 evidence pack
  来判断是否进入 observe-only shadow iteration。
- 2026-05-27：改为 `DONE`。已完成 `aits sec-pit review-candidates` / `--latest`、
  candidate evidence / by-ticker / by-period / baseline overlap / shadow proposal CSV、
  summary JSON/Markdown、dashboard 只读卡片、runbook、artifact catalog、system flow、
  learning path 和专项测试。真实 `--latest --candidate-feature capex_intensity` 已生成
  `outputs/sec_pit_candidate_review/sec_pit_candidate_review_summary_2026-05-26.*`，状态
  `OK`，`capex_intensity` 为 `READY_FOR_MANUAL_REVIEW`，`production_effect=none`，
  未修改 production 或 active shadow weights。

## 收尾验证

- `python -m pytest tests/trading_engine/test_sec_pit_candidate_review.py -q`：10 passed
- `python -m pytest tests/test_daily_task_dashboard.py -q`：21 passed
- `python -m pytest tests/trading_engine/test_sec_pit_real_run_diagnostics.py -q`：10 passed
- `python -m pytest -q`：1298 passed, 1 warning
- `python -m ruff check config src tests scripts docs`：passed
- `python -m black --check <touched-python-files>`：passed
