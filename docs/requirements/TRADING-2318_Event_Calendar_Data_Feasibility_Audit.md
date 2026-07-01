# TRADING-2318 Event Calendar Data Feasibility Audit

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

Owner post-2302 roadmap 把 event calendar gating 放在 regime diagnostics 之后。
TRADING-2318 只审计事件日历数据源是否具备 PIT / known-at timestamp，不能直接生成
gating signal、return predictor、no-add runtime、manual-review runtime、post-event
confirmation runtime、paper-shadow、production 或 broker action。

审计对象来自 owner roadmap：

- FOMC
- CPI
- PCE
- NFP / payrolls
- NVDA earnings
- MSFT / GOOGL / AMZN / META earnings
- TSM monthly revenue
- major semiconductor earnings window

## 目标

新增 CLI：

```bash
aits research trends event-calendar-data-feasibility-audit
```

输出 event calendar source feasibility、PIT timestamp requirements、gating use-case
route、manual review trigger contract 和 safety boundary。

## 输入

默认读取：

```text
config/research/event_calendar_feasibility_policy.yaml
config/research/event_calendar_definitions.yaml
docs/requirements/TRADING-2301_First_Layer_New_Candidate_Family_Research_Backlog_And_Feasibility_Audit.md
```

本任务是 static source feasibility audit，不读取 cached market / macro prices，不下载
event calendar rows，不调用外部 API。后续 TRADING-2319 / TRADING-2320 一旦读取事件
rows、prices、features、scoring 或 report inputs，必须执行相应 source schema gate 和
`aits validate-data` 或同源 validation code path。

## 产物

- `event_calendar_data_feasibility_summary.json`
- `event_calendar_source_inventory.json`
- `event_calendar_source_inventory.csv`
- `event_calendar_known_at_requirement_matrix.json`
- `event_calendar_known_at_requirement_matrix.csv`
- `event_calendar_gating_use_case_matrix.json`
- `event_calendar_gating_use_case_matrix.csv`
- `event_calendar_manual_review_trigger_contract.json`
- `event_calendar_validation_route.json`
- `event_calendar_validation_route.csv`
- `event_calendar_safety_boundary.json`
- `docs/research/event_calendar_data_feasibility_audit.md`

## 实施边界

1. Source feasibility only。
   - 只审计 provider class、endpoint/file requirement、known-at / available-time 字段、
     PIT blocker 和可用性等级。
   - 不下载真实事件日历，不声称 source entitlement，不生成 event date rows。

2. Gating use cases。
   - 只定义 `pre_event_no_add`、`post_event_confirmation_window`、
     `manual_review_trigger`、`earnings_cluster_risk` 的 required source conditions。
   - 不输出可执行 gating signal，不改变任何仓位建议。

3. Known-at discipline。
   - 每个 source 必须说明 `event_time`、`source_published_time`、`known_at`、
     `available_at`、`download_timestamp`、row count 和 checksum requirement。
   - 缺 known-at timestamp 的 source 只能是 `DIAGNOSTIC_ONLY_SOURCE_AUDIT_REQUIRED`。

4. 安全边界。
   - `research_only=true`
   - `static_feasibility_audit=true`
   - `event_rows_downloaded=false`
   - `gating_signal_generated=false`
   - `event_outcome_prediction_allowed=false`
   - `candidate_artifact_generated=false`
   - `actual_path_validation_executed=false`
   - `promotion_allowed=false`
   - `paper_shadow_allowed=false`
   - `production_allowed=false`
   - `broker_action=none`

## 验收标准

- CLI implemented: `aits research trends event-calendar-data-feasibility-audit`。
- Summary 披露 selected market regime=`ai_after_chatgpt`、data quality status、
  event family counts、PIT-ready/source-blocked counts 和 safety flags。
- Source inventory 覆盖 FOMC、CPI、PCE、NFP、NVDA earnings、AI mega-cap earnings、
  TSM monthly revenue 和 major semiconductor earnings window。
- Known-at requirement matrix 明确每类事件进入 TRADING-2319 前需要的 timestamp /
  manifest / checksum / schema validation。
- Gating use-case matrix 不允许输出 promotion-ready、paper-shadow-ready、
  production-ready 或 broker-ready 状态。
- `docs/system_flow.md`、`docs/artifact_catalog.md` 和 `config/report_registry.yaml`
  同步登记 2318 data flow 和 safety boundary。

## 进展记录

- 2026-07-01: 根据 owner post-2302 roadmap 和 TRADING-2317 allowed next step 新增并
  进入 `IN_PROGRESS`。当前 worktree 有两个无关 research 文档未提交改动，本任务必须
  selective staging，不能混入无关改动。
- 2026-07-01: 实现完成并进入 `VALIDATING`。真实 run status=
  `EVENT_CALENDAR_FEASIBILITY_AUDIT_READY_SOURCE_AUDIT_ONLY`，
  data_quality_status=`NOT_APPLICABLE_STATIC_FEASIBILITY_AUDIT`，
  event_source_count=8，source_audit_required_count=8，pit_ready_source_count=0，
  source_blocked_count=4，gating_use_case_count=4，validation_route_row_count=12。
  本命令不读取 cached market / macro data，因此不运行 `aits validate-data` 作为门禁。
- 2026-07-01: 验证通过 Ruff、compileall、TRADING-2318 focused parallel pytest
  7 passed、regime/event adjacent focused parallel pytest 26 passed、docs/registry
  focused parallel pytest 27 passed、真实 CLI run、`contract-validation` 193 passed。
  Runtime artifact:
  `outputs/validation_runtime/contract-validation_20260701T070030Z/test_runtime_summary.json`。
