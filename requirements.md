# Requirements Index

最后更新：2026-06-16

本文件记录当前新增需求文档入口。详细任务登记仍以 `docs/task_register.md` 为准。

## TRADING-383

- 需求文档：`docs/requirements/TRADING-383_Canonical_Paper_Shadow_Health_Rerun.md`
- 任务：Canonical Paper Shadow Health Rerun
- 状态：`DONE`
- 需求：The system shall aggregate latest paper-shadow data, signal, fallback, cache, daily, drift, weekly, evidence staleness, readiness, and refresh-audit inputs into a single read-only canonical health report.
- 安全边界：只读 paper-shadow health aggregation；不刷新数据、不补造 artifact、不运行上游、不写 official target weights、不修改 shadow/paper/production state、不触发 broker/order。
- 主要输入：latest price data、market panel data、signal input completeness report、daily paper-shadow artifact、drift monitor artifact、weekly review artifact、evidence staleness monitor artifact、shadow continuation readiness report、fallback policy report、cache catalog report、data refresh audit report。
- 主要输出：`reports/etf_portfolio/dynamic_v3_rescue/paper_shadow_health/<health_id>/paper_shadow_health_report.json`、`.md`、validation JSON/Markdown、Reader Brief section，以及 downstream canonical health summary。

## TRADING-371

- 需求文档：`docs/requirements/TRADING-371_Signal_Input_Completeness_Monitor.md`
- 任务：Signal Input Completeness Monitor
- 状态：`DONE`
- 需求：The system shall check required signal inputs for missing files, staleness, schema/version compatibility, empty series, market coverage, and required feature columns before paper-shadow daily observation, weekly review, evidence staleness interpretation, and shadow continuation readiness.
- 安全边界：只读 signal input completeness guard；不刷新数据、不补造 signal 或 feature artifact、不运行上游、不写 official target weights、不修改 shadow/paper/production state、不触发 broker/order。
- 主要输入：`config/etf_portfolio/dynamic_v3_rescue/signal_input_completeness_v1.yaml`、`data/etf_portfolio/signals.csv`、`data/etf_portfolio/features.csv`、`data/processed/features_daily.csv`、latest signal snapshot artifact。
- 主要输出：`reports/etf_portfolio/dynamic_v3_rescue/signal_input_completeness/<monitor_id>/signal_input_completeness_report.json`、`.md`、findings JSONL、validation JSON/Markdown、Reader Brief section，以及 downstream paper-shadow/staleness/readiness summary。

## TRADING-368

- 需求文档：`docs/requirements/TRADING-368_Data_Source_Fallback_Policy.md`
- 任务：Data Source Fallback Policy
- 状态：`DONE`
- 需求：The system shall make primary/fallback source status explicit through a governed policy artifact, and fail closed when no valid source is available for cached-data-dependent interpretation.
- 安全边界：只读 fallback governance；不刷新数据、不补造 cache、不自动切换 provider、不绕过 `aits validate-data`、不写 official target weights、不修改 shadow/paper/production state、不触发 broker/order。
- 主要输入：`config/data_source_fallback_policy.yaml`、`config/data_sources.yaml`、已配置 cache paths 和显式 fallback used metadata。
- 主要输出：`reports/data_governance/data_source_fallback_policy/<report_id>/data_source_fallback_policy.json`、`.md`、validation JSON/Markdown、Reader Brief section，以及 downstream data refresh audit、PIT manifest、evidence staleness monitor、shadow continuation readiness 和 Reader Brief summary。

## TRADING-369

- 需求文档：`docs/requirements/TRADING-369_Checksum_and_Cache_Catalog.md`
- 任务：Checksum and Cache Catalog
- 状态：`DONE`
- 需求：The system shall maintain a read-only cache catalog for price, market panel, and macro data caches, including checksums, dimensions, freshness metadata, source lineage, and refresh audit linkage before cached-data-dependent interpretation.
- 安全边界：只读 cache governance；不刷新数据、不修复 cache、不补造缺失缓存、不绕过 `aits validate-data`、不写 official target weights、不修改 shadow/paper/production state、不触发 broker/order。
- 主要输入：`config/data_sources.yaml`、`config/cache_catalog.yaml`、现有 price / market panel / macro cache、latest data refresh audit、latest validate-data audit sidecar。
- 主要输出：`reports/data_governance/cache_catalog/<catalog_id>/cache_catalog.json`、`.md`、validation JSON/Markdown、Reader Brief section，以及 downstream data refresh audit、PIT manifest、evidence staleness monitor、shadow continuation readiness 和 Reader Brief summary。

## TRADING-039

- 需求文档：`docs/requirements/sec_edgar_reconstructed_pit_backfill_2026-05-26.md`
- 任务：SEC EDGAR Reconstructed PIT Backfill
- 状态：`IN_PROGRESS`
- 需求：The system shall reconstruct a conservative SEC filing-time PIT fundamentals layer from submissions metadata and companyfacts facts, and enforce `available_for_signal_date <= decision_date` for metrics, features, backtests, and validation.
- 安全边界：`production_effect=none`；输出数据等级固定为 `B_RECONSTRUCTED_SEC_FILING_PIT`，不得伪装成 strict vendor archive；不执行交易、不调用 broker、不读取真实券商 API key、不改变 paper trading 下单路径。
- 主要输入：`config/sec_companies.yaml`、`config/fundamental_metrics.yaml`、`config/fundamental_features.yaml`、SEC submissions API、SEC companyfacts API。
- 主要输出：`data/raw/sec_edgar/manifest/sec_edgar_raw_manifest.csv`、`data/processed/sec_edgar/sec_pit_feature_panel.csv`、`outputs/reports/sec_pit_backfill/sec_pit_backfill_YYYY-MM-DD.md`、coverage/leakage/validation reports。

## TRADING-038

- 需求文档：`docs/requirements/retry_execution_dry_run_2026-05-26.md`
- 任务：Manual Approval Record / Retry Execution Dry Run
- 状态：`VALIDATING`
- 需求：The system shall support manual retry approval records and generate retry execution dry-run reports without executing retries, sending notifications, or mutating production state.
- 安全边界：只读 manual approval record + retry execution dry-run，不执行 retry，不发送通知，不修改 approval record/state、delivery state、TRADING-035/036/037 artifact 或 production 参数，不运行 TRADING-037 generator 或任何上游 notification pipeline。
- 主要输入：`inputs/manual_retry_approvals/manual_retry_approval_YYYY-MM-DD.json`。
- 主要输出：`outputs/retry_execution_dry_run/retry_execution_dry_run_YYYY-MM-DD.json`、`.md`、`.log`。

## TRADING-037

- 需求文档：`docs/requirements/retry_candidate_queue_2026-05-26.md`
- 任务：Retry Candidate Queue / Manual Approval Gate
- 状态：`VALIDATING`
- 需求：The system shall generate a read-only retry candidate queue from notification delivery failure classification results and require manual approval before any retry execution is allowed.
- 安全边界：只读 retry candidate queue，不执行 retry，不发送通知，不修改 delivery state、approval state、TRADING-035/036 artifact 或 production 参数，不运行 TRADING-036 generator 或任何上游 notification pipeline。
- 主要输出：`outputs/retry_candidate_queue/retry_candidate_queue_YYYY-MM-DD.json`、`.md`、`.log`。

## TRADING-036

- 需求文档：`docs/requirements/notification_delivery_failure_classification_2026-05-26.md`
- 任务：Notification Delivery Failure Classification / Retry Readiness
- 状态：`VALIDATING`
- 需求：The system shall classify notification delivery audit failures into actionable categories and produce retry readiness guidance without sending notifications or mutating production state.
- 安全边界：只读 failure classification，不发送通知，不创建或修改 Gmail draft，不调用 webhook，不自动 retry，不修改 delivery state 或 production 参数，不运行 TRADING-035 或任何上游 notification pipeline。
- 主要输出：`outputs/notification_delivery_failure_classification/notification_delivery_failure_classification_YYYY-MM-DD.json`、`.md`、`.log`。

## TRADING-035

- 需求文档：`docs/requirements/notification_delivery_audit_summary_2026-05-25.md`
- 任务：Notification Delivery Audit Summary
- 状态：`VALIDATING`
- 安全边界：只读 delivery audit summary，不发送通知，不创建或修改 Gmail draft，不调用 webhook，不运行 TRADING-030/031/034 或任何上游/发送/交易流水线。
- 主要输出：`data/derived/operator_briefs/notifications/delivery_audit/notification_delivery_audit_summary_YYYY-MM-DD.json`、`.md`、`logs/notification_delivery_audit_summary_run_YYYY-MM-DD.json`、`.md`。

## TRADING-034

- 需求文档：`docs/requirements/operator_brief_notification_draft_dispatch_2026-05-25.md`
- 任务：Operator Brief Notification Draft Dispatch
- 状态：`DONE`
- 安全边界：只读 draft dispatch，不发送通知，不创建 Gmail draft，不调用 Gmail API / SMTP / webhook，不访问外部网络，不读取 `.env` secrets，不读取真实邮箱，不自动审批，不修改 TRADING-032 / TRADING-033 artifact 或 approval marker。
- 主要输出：`data/derived/operator_briefs/notifications/draft_dispatch/operator_brief_notification_draft_dispatch_YYYY-MM-DD.json`、`.md`、`latest.json`、`latest.md`、`run.log`。

## TRADING-033

- 需求文档：`docs/requirements/operator_brief_notification_approval_gate_2026-05-24.md`
- 任务：Operator Brief Notification Approval Gate
- 状态：`VALIDATING`
- 安全边界：只读 approval gate，不发送通知，不创建 Gmail draft，不调用 SMTP / webhook，不访问外部网络，不读取 `.env` secrets，不自动审批，不修改 TRADING-032 artifact 或 production 配置。
- 主要输出：`data/derived/operator_briefs/notifications/approval_gate/operator_brief_notification_approval_gate_YYYY-MM-DD.json`、`.md`、`latest.json`、`latest.md`、`run.log`。

## TRADING-032

- 需求文档：`docs/requirements/operator_brief_notification_dispatch_preview_2026-05-24.md`
- 任务：Operator Brief Notification Dry-run Dispatch Preview
- 状态：`VALIDATING`
- 安全边界：只读 dry-run preview，不发送通知，不创建 Gmail draft，不调用 webhook，不访问外部网络，不读取 `.env` secrets，不修改 scheduler 或 production 配置。
- 主要输出：`data/derived/operator_briefs/notifications/dispatch_preview/operator_brief_notification_dispatch_preview_YYYY-MM-DD.json`、`.md`、`latest.json`、`latest.md`、`run.log`。
