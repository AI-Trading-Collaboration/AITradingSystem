# TRADING-111 to 113 Real Research Evidence Closure

最后更新：2026-06-06

## 状态

`VALIDATING`

## 背景

TRADING-102 to 110 已经让 `small_real` 真实参数研究链路跑通，但证据仍不完整：

- data quality 为 `PASS_WITH_WARNINGS`，主要 warning 是 `prices_download_manifest_checksum_missing`。
- candidate attribution 因缺少 real evaluator daily weight path 仍为 `INCOMPLETE`。
- overfit 仍可能是 `REVIEW_REQUIRED`。
- promotion pack 已 fail closed，但还没有完整消费 window / weight path / data provenance 证据。

本阶段不改变 production 权重、不触发 broker action、不生成 `production_candidate`。目标是补齐真实研究证据闭环，让候选是否具备晋级证据可以被审计。

## 范围

|任务|目标|状态|
|---|---|---|
|TRADING-111|Backtest Window Coverage & Artifact Date Range Integrity|VALIDATING|
|TRADING-112|Real Evaluator Daily Weight Path Export|VALIDATING|
|TRADING-113|Price Cache Download Manifest Repair & Data Provenance Closure|VALIDATING|

## 设计原则

- 默认市场 regime 是 `ai_after_chatgpt`，anchor event 为 2022-11-30，默认 backtest start 为 2022-12-01。
- `configured_backtest_start` 表示配置要求的研究起点；`requested_start/end` 表示本次命令请求窗口；`actual_evaluation_start/end` 表示 artifact 实际有结果的窗口。
- artifact 缺少 `actual_evaluation_start/end`、`requested_date_range`、daily weight path、price cache checksum provenance 或 attribution source path 时，不得 silent pass。
- 无法从现有 evaluator 获得的中间权重字段不得伪造；允许先导出 `minimal` daily weight path，并在 metadata 中标记 `PARTIAL`。
- 数据 provenance 若只能由现有 cache 重建，必须标记 `RECONSTRUCTED_MANIFEST`，不得伪装成原始下载事件。
- `BACKTEST_WINDOW_INCOMPLETE`、`MISSING_DAILY_WEIGHT_PATH`、`DATA_PROVENANCE_INCOMPLETE`、`ATTRIBUTION_INCOMPLETE`、`OVERFIT_REVIEW_REQUIRED` 均阻断 `promote_candidate` 和 `production_candidate`。

## 阶段拆解

|阶段|交付|验收标准|状态|
|---|---|---|---|
|A|任务登记与需求文档|task register 指向本文；本文记录范围、证据字段、gate、CLI、测试和状态迁移|DONE|
|B|Data provenance|新增 inspect / repair / validate；修复 checksum-missing 或降级为 reconstructed warning；data audit 和 promotion pack 可消费 provenance|DONE|
|C|Window audit|新增 window-audit run/report/inspect/validate；关键 artifacts 暴露 backtest_window；window incomplete 阻断 promotion|DONE|
|D|Weight path export|real evaluator 写出 `daily_weights.csv`、events、turnover、metadata；新增 weight-path validate/report；candidate attribution 消费路径|DONE|
|E|Promotion evidence gate|promotion pack 消费 data/window/weight/attribution/overfit evidence；缺证据最多 `review_required` 或 `incomplete`|DONE|
|F|文档与验证|README、operations runbook、system flow、report registry、artifact catalog、Reader Brief 同步；focused tests、ruff、compileall、diff check PASS|DONE|

## CLI 合同

新增：

- `aits etf dynamic-v3-rescue data-provenance inspect-price-cache`
- `aits etf dynamic-v3-rescue data-provenance repair-price-manifest --mode reconstruct-from-cache`
- `aits etf dynamic-v3-rescue data-provenance validate`
- `aits etf dynamic-v3-rescue window-audit run --as-of <date> --end <date>`
- `aits etf dynamic-v3-rescue window-audit report --latest`
- `aits etf dynamic-v3-rescue window-audit inspect-artifact --artifact-path <path>`
- `aits etf dynamic-v3-rescue validate-window-audit --audit-id <window_audit_id>`
- `aits etf dynamic-v3-rescue weight-path validate --evaluation-id <evaluation_id>`
- `aits etf dynamic-v3-rescue weight-path report --evaluation-id <evaluation_id>`

## Artifact 合同

新增目录：

- `reports/etf_portfolio/dynamic_v3_rescue/data_provenance/`
- `reports/etf_portfolio/dynamic_v3_rescue/window_audit/<window_audit_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/sweeps/<sweep_id>/real_evaluation/<candidate_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/promotion/<candidate_id>/<promotion_id>/`

关键文件：

- `price_cache_provenance_report.json`
- `price_cache_provenance_report.md`
- `window_audit_manifest.json`
- `artifact_window_inventory.jsonl`
- `window_mismatch_report.json`
- `insufficient_data_report.json`
- `window_audit_report.md`
- `daily_weights.csv`
- `rebalance_events.csv`
- `constraint_events.csv`
- `rescue_events.csv`
- `turnover_path.csv`
- `weight_path_metadata.json`
- `evidence_summary.json`

## 验收测试

Focused tests 至少覆盖：

- `configured_backtest_start = 2022-12-01` 被正确读取。
- `actual_evaluation_start = 2025-05-28` 时标记 `INCOMPLETE` 或 `INSUFFICIENT_DATA`。
- `requested_date_range` 缺失不能 PASS。
- `INSUFFICIENT_DATA` / window incomplete 阻断 `promote_candidate`。
- real evaluator 生成且可校验 `daily_weights.csv`。
- missing daily weight path 导致 attribution `INCOMPLETE`。
- minimal daily weight path 导致 attribution `PARTIAL`。
- price cache sha256 可计算，manifest mismatch 可发现。
- reconstructed manifest 不伪造 original download。
- promotion pack 消费 data/window/weight evidence。

必须运行：

```bash
python -m pytest tests/test_etf_dynamic_v3_parameter_research.py -q
python -m ruff check src tests
python -m compileall -q src tests
git diff --check
```

尽量运行：

```bash
python -m pytest tests -q
```

## 状态记录

- 2026-06-06：新增并进入 `IN_PROGRESS`。本阶段优先实现 P0 纵切：CLI 可运行、artifact 可审计、缺证据 fail closed、focused tests PASS。
- 2026-06-06：实现完成并进入 `VALIDATING`。新增 data provenance / window audit /
  weight path CLI 与 artifact，real evaluator candidate artifact 输出 backtest window 和
  daily weight path，candidate attribution 支持 `PARTIAL` / `COMPLETE`，promotion pack
  写出 `evidence_summary.json` 并在 window、weight path、attribution、provenance 或
  overfit evidence 不完整时阻断 `promote_candidate`。验证通过 ruff、compileall、
  `git diff --check`、dynamic-v3 parameter research focused tests、Reader Brief focused
  tests 和全量 pytest。
