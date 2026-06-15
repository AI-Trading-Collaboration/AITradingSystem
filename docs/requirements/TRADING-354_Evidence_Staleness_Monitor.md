# TRADING-354 Evidence Staleness Monitor

最后更新：2026-06-16

## 1. 背景

TRADING-349 已建立 candidate decision ledger，但 ledger 只能记录当时的候选决策状态。
如果上游 price data、market panel、signal evidence、stress backfill、A/B review、owner
review、paper-shadow daily observation、paper-shadow drift monitor 或 paper-shadow weekly
review 已经陈旧，后续 candidate decision 仍可能被误读为当前可用证据。

## 2. 目标

1. 新增 evidence timestamp extraction。
2. 用 policy YAML 定义 price data、market panel、signal artifact、stress backfill result、
   A/B review、owner review、paper-shadow daily observation、paper-shadow drift monitor 和
   paper-shadow weekly review 的 freshness rules。
3. 输出 FRESH、ACCEPTABLE、STALE、BLOCKING severity。
4. 新增 validate CLI。
5. 新增 report CLI 和 Reader Brief summary。
6. 新增 focused tests。

## 3. 非目标

- 不刷新 price cache。
- 不运行 market panel 上游。
- 不重跑 signal / stress / A/B / owner review artifacts。
- 不修改 candidate decision ledger。
- 不生成 official target weights、order ticket、broker action 或 production mutation。

## 4. Freshness Policy

Freshness thresholds must live in
`config/etf_portfolio/dynamic_v3_rescue/evidence_staleness_policy_v1.yaml` with owner,
version/status, rationale, intended effect, validation evidence, and review condition.
The monitor code only applies that policy; it must not introduce hidden threshold literals
that affect investment interpretation.

## 5. Artifact Contract

Runtime root:

- `reports/etf_portfolio/dynamic_v3_rescue/evidence_staleness_monitor/<monitor_id>/`

Artifacts:

- `evidence_staleness_manifest.json`
- `evidence_staleness_report.json`
- `evidence_staleness_findings.jsonl`
- `evidence_staleness_report.md`
- `reader_brief_section.md`
- `evidence_staleness_validation.json/md`

Expected summary fields:

- `requested_as_of`
- `freshness_reference_date`
- `latest_complete_market_date`
- `market_calendar_status`
- `evidence_freshness_status`
- `stale_artifacts`
- `blocking_artifacts`
- `missing_artifacts`
- `next_refresh_action`
- `safe_to_continue_shadow`
- `safety_boundary_status`

Expected per-source finding fields:

- `source_id`
- `timestamp_basis`
- `source_timestamp`
- `freshness_reference_date`
- `age_days`
- `severity`
- `missing`
- `stale_reason`

## 6. Safety Boundary

All outputs are read-only and fixed to:

- `production_effect=none`
- `manual_review_only=true`
- `evidence_staleness_monitor_only=true`
- `data_downloaded_by_monitor=false`
- `pipelines_executed_by_monitor=false`
- `not_official_target_weights=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `production_state_mutated=false`
- `automatic_candidate_promotion=false`
- `auto_apply=false`

## 7. 验收标准

- `evidence-staleness-monitor run/report` 可运行。
- `validate-evidence-staleness-monitor` 返回 PASS。
- Reader Brief 显示 freshness status、stale artifacts、blocking artifacts、missing artifacts、
  safe-to-continue-shadow、safety boundary status 和 next action。
- README、operations runbook、system flow、artifact catalog、report registry、requirements 和
  task register 同步更新。
- focused pytest、contract-validation suite、ruff、compileall、git diff check、documentation
  contract、report index 和 Reader Brief quality 通过。

## 8. 进展记录

- 2026-06-15：新增任务并进入 IN_PROGRESS；本阶段只实现只读 staleness monitor，不刷新数据、不重跑上游、不修改 ledger 或 production state。
- 2026-06-15：实现完成并转入 VALIDATING；真实链路生成 monitor
  `evidence-staleness-monitor_031403275837d5b6`，`evidence_freshness_status=ACCEPTABLE`，
  `stale_artifacts=none`，`blocking_artifacts=none`，
  `next_refresh_action=continue_with_manual_freshness_note`，validator `status=PASS` /
  failed=0。Reader Brief JSON 已显示 staleness fields；future-dated evidence regression 已覆盖；
  focused pytest 6 passed，contract-validation suite 23 passed / 19.08s，documentation contract
  PASS，report index `PASS_WITH_WARNINGS` 仅保留既有 missing/stale visibility，Reader Brief OK，
  Reader Brief quality OK。安全边界保持 read-only / no data refresh / no upstream rerun /
  no official target / no broker / no production。
- 2026-06-16：补齐附件验收范围并转 DONE；monitor 现在把 paper-shadow daily observation、
  paper-shadow drift monitor 和 paper-shadow weekly review 纳入 policy-governed freshness
  findings，输出 `missing_artifacts`、`safe_to_continue_shadow` 和
  `safety_boundary_status`，并在缺失 weekly review 时 fail-closed 为 `BLOCKING`。真实只读
  run 生成 `evidence-staleness-monitor_39cc2b41171c8b6d`，当前
  `evidence_freshness_status=BLOCKING`、`blocking_artifacts=paper_shadow_weekly_review`、
  `missing_artifacts=paper_shadow_weekly_review`、`safe_to_continue_shadow=false`，
  `validate-evidence-staleness-monitor` 返回 PASS / failed=0。验证通过
  `python -m pytest tests/test_evidence_staleness_monitor.py tests/test_etf_dynamic_v3_parameter_research.py tests/test_documentation_contract.py tests/test_report_index.py -q`
  36 passed、`python -m ruff check src tests`、`python -m compileall src` 和
  `git diff --check`。
- 2026-06-16：TRADING-354B 更新 market-data freshness 口径；`price_data` 和
  `market_panel_data` 现在使用 `freshness_reference_date` 计算 age，并在 report /
  Reader Brief / CLI summary 中披露 `requested_as_of`、`latest_complete_market_date`、
  `market_calendar_status` 和 per-source `stale_reason`。该变更不放宽 threshold，不增加
  waiver，不刷新数据，也不运行 market panel 或 research 上游。
