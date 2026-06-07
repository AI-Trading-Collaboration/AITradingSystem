# TRADING-111 to 113 Real Research Evidence Closure

最后更新：2026-06-07

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
- `aits etf dynamic-v3-rescue artifacts repair-latest`

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
- `reports/etf_portfolio/dynamic_v3_rescue/latest/latest_*.json`

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
- 默认 latest pointer 指向 canonical dynamic-v3 artifact root；测试临时目录或 canonical 根外 target 不能通过 `artifacts validate`；`artifacts repair-latest` 可从 canonical root 重建 pointers。

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
- 2026-06-07：TRADING-111 / TRADING-113 从 `VALIDATING` 改回 `IN_PROGRESS`。复验
  当前本机 evidence 状态时，`data-provenance validate` 返回 `FAIL`，`inspect-price-cache`
  显示 `download_manifest_status=AVAILABLE` 但 `prices_checksum_in_manifest=false`；同时
  `window-audit report --latest` 的 latest 指针指向缺失 `window_audit_manifest.json` 的
  audit id 并产生 traceback。修复目标是让 provenance repair/validate 闭合到
  `PASS` 或可解释 `PASS_WITH_WARNINGS`，并让 window audit latest 指针缺失目标时
  fail closed 且输出可审计错误，不得 silent pass 或 traceback。
- 2026-06-07：本机 evidence closure 修复推进。执行
  `data-provenance repair-price-manifest --mode reconstruct-from-cache` 后，
  `data-provenance validate` 为 `PASS_WITH_WARNINGS` 且 `failed_check_count=0`，
  `prices_checksum_in_manifest=true`，provenance 明确为 `RECONSTRUCTED_MANIFEST`。
  新增 latest pointer canonical-root hardening 和 `artifacts repair-latest`；测试临时
  output_dir 不再更新全局 latest pointer，canonical 根外 latest pointer 会 fail closed。
  当前 `artifacts repair-latest` 重建 15 个 canonical pointers，`artifacts validate`
  和 forced schedule observe gate 均 PASS。`window-audit run --as-of 2022-12-01
  --end 2026-06-05` 生成 canonical audit `2eface90ec36f7bf`，artifact validation PASS；
  audit 自身状态仍为 `FAIL`，原因是现有 artifact 集合有 170 个 promotion-blocking
  window evidence 缺口，该结果会继续阻断 promotion，不被视为实现失败。
- 2026-06-07：修复完成并回到 `VALIDATING`。补充验证通过
  `pytest tests/test_etf_dynamic_v3_parameter_research.py -q`（16 passed）、
  `pytest tests/test_scheduled_tasks.py tests/test_ops_daily.py tests/test_cli_direct.py
  tests/test_etf_dynamic_v3_parameter_research.py -q`（72 passed）、`ruff check`、
  `black --check`、`compileall -q src`、`aits docs validate-freshness`、
  `aits docs report-contract --latest` 和 `git diff --check`。剩余限制是当前 window
  audit 真实结果 `status=FAIL` / `promotion_blocking_count=170`，需要 owner/系统决定是否
  重新跑完整窗口 real sweep / weight tuning 或接受继续阻断 promotion。
- 2026-06-07：重新进入 `IN_PROGRESS`。复验当前 latest promotion pack 时发现历史
  pack 缺 `evidence_summary.json`；按当前代码重新生成 pack 后结构校验 PASS，但
  `evidence_summary.backtest_window_status=MISSING`，未消费 canonical latest window
  audit `2eface90ec36f7bf` 的 `status=FAIL` / `promotion_blocking_count=170`。本轮修复
  目标是让 promotion pack 显式读取 latest window audit evidence，并把 audit fail /
  blocking findings 纳入 `BACKTEST_WINDOW_INCOMPLETE`，避免人工 promotion review 看到
  “window missing” 或误以为 window gate 未执行。
- 2026-06-07：修复完成并回到 `VALIDATING`。`promotion pack` 新增
  `--window-audit-dir`，默认读取 canonical `latest_window_audit`，测试/临时目录读取最新
  window audit 子目录；`evidence_summary.json` 和 `linked_artifacts.json` 现在披露
  `window_audit_id`、manifest/report path、`backtest_window_status` 和
  `promotion_blocking_count`，audit `FAIL` / `INCOMPLETE` / missing 会写入
  `BACKTEST_WINDOW_INCOMPLETE`。当前本机重新生成 promotion pack
  `4cb5deda6f6a9853` 后 `validate-promotion-pack` PASS，summary 正确显示
  `backtest_window_status=FAIL`、`window_audit_id=2eface90ec36f7bf`、
  `promotion_blocking_count=170`，并继续阻断 `promote_candidate`。验证通过
  `pytest tests/test_etf_dynamic_v3_parameter_research.py`（16 passed）、
  scheduler/daily/direct/dynamic-v3 组合测试（72 passed）、ruff、black、compileall、
  docs freshness、docs report contract 和 `git diff --check`。
