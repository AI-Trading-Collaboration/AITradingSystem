# TRADING-074 Portfolio Operations Runbook and Scheduler Review

## 背景

TRADING-062 至 TRADING-073 已建立 ETF allocation baseline、credibility validation、experiment packs、forward simulation、weekly review、decision journal、parameter review、dual-track weight calibration、AI confirmation / attribution 和 satellite replacement / attribution。

TRADING-074 的目标不是新增交易策略，而是把这些模块组织成可审计、可 dry-run、可验证的运营 workflow，回答：

```text
Can the full ETF portfolio research system be operated reliably on a daily / weekly / monthly schedule?
```

## 安全边界

所有 TRADING-074 输出必须固定：

```text
observe_only=true
candidate_only=true
production_effect=none
broker_action=none
manual_review_required=true
```

禁止事项：

```text
broker execution
production weight mutation
automatic promotion
automatic candidate rejection without manual review
cloud scheduler deployment requiring secrets
paid data provider dependency
```

## 阶段拆解

|阶段|标题|状态|验收摘要|
|---|---|---|---|
|TRADING-074A|Operations Schedule Spec|DONE|新增 `config/etf_portfolio/operations_schedule.yaml`，并用 loader/validator 校验 cadence、步骤、依赖、expected outputs 和安全字段。|
|TRADING-074B|Daily Pipeline Command Graph|DONE|从 schedule spec 构建 daily dependency-aware command graph，可 dry-run，不执行命令。|
|TRADING-074C|Weekly Pipeline Command Graph|DONE|从 schedule spec 构建 weekly dependency-aware command graph，保留 external daily dependencies，并标记人工复核 checkpoint。|
|TRADING-074D|Monthly / Biweekly Review Pipeline|DONE|从 schedule spec 构建 biweekly / monthly dependency-aware command graph，确保 heavy historical search 不进入 daily 默认链路。|
|TRADING-074E|Artifact Freshness and Dependency Checks|DONE|新增只读 artifact freshness / dependency checker，校验 required / optional artifacts 的 freshness、missing 和 dependency status。|
|TRADING-074F|Failure Severity and Blocking Policy|DONE|新增只读 failure policy evaluator，把 artifact freshness issue 映射为 severity、blocking effect 和 manual-review action。|
|TRADING-074G|Owner Review Checklist|DONE|新增只读 owner checklist builder，提供 daily / weekly / monthly / incident review template 并绑定 failure policy events。|
|TRADING-074H|Scheduler Dry-Run / Simulation|DONE|新增 `aits etf ops dry-run --cadence ...` 和 dry-run report，只规划不执行、不写 production state。|
|TRADING-074I|Operations Report Generator|DONE|新增 `aits etf ops report --cadence ...`，生成 JSON / Markdown operations health report。|
|TRADING-074J|Reader Brief Operations Health Section|DONE|Reader Brief 只读展示 operations health，链接 detailed ops report。|
|TRADING-074K|Operations Validation Gate|READY|新增 `aits etf ops validate`，fail-closed 校验 A-J 完整性和安全边界。|

## 设计约束

- `docs/operations/operations_runbook.md` 是周期性任务入口；任何 scheduler / cadence 解释必须与该 runbook 保持一致。
- daily scheduler trigger 仍是统一外部入口；TRADING-074 不把 weekly / monthly 任务散落成新的外部 scheduler entries。
- `aits validate-data` 或同一路径 data quality gate 必须在 cached-data-dependent workflow 前可见。
- default market regime 为 `ai_after_chatgpt`，默认结论窗口从 2022-12-01 开始。
- runtime artifacts 继续写入 ignored `reports/`、`outputs/` 或 `data/` 目录；确定性 fixtures 放入 `tests/fixtures/etf_portfolio/`。

## 进展记录

- 2026-06-03: TRADING-074 新增并进入 IN_PROGRESS。当前实现范围从 TRADING-074A 开始，先建立 config-driven operations schedule spec 和 focused validation tests。
- 2026-06-03: TRADING-074A 完成。新增 `config/etf_portfolio/operations_schedule.yaml`、`src/ai_trading_system/etf_portfolio/operations.py` 和 `tests/test_etf_operations.py`，覆盖 schedule load、cadence sections、required daily nodes、unique step IDs、non-empty commands、dependency references、required expected outputs、safety field requirement、unsafe `production_effect` fail-closed 和 weight search not daily。
- 2026-06-03: TRADING-074B 完成。新增 `build_daily_operations_command_graph`、daily graph node/payload schema、required daily node validation、topological ordering、optional attribution node skipping、cycle detection、dry-run-only flags 和 safety propagation；专项测试覆盖 graph build、required fields、topological order、optional skip、required skip refusal、missing required node、cycle detection 和 safety fields。
- 2026-06-03: TRADING-074C 完成。新增 `build_weekly_operations_command_graph`、cadence-aware graph builder、weekly required node validation、weekly manual-review checkpoint propagation、external daily dependency disclosure、optional parameter review skip handling、cycle detection 和 dry-run-only safety fields；专项测试覆盖 weekly graph build、topological order、external daily inputs、manual review flags、optional parameter review、missing weekly review blocking、cycle detection 和 safety fields。
- 2026-06-03: TRADING-074D 完成。新增 `build_biweekly_operations_command_graph` 和 `build_monthly_operations_command_graph`，覆盖 biweekly attribution scorecard / weight evidence graph、monthly data quality audit / bounded weight search / parameter governance graph、cross-cadence external dependency disclosure、slow runtime classification、manual-review propagation、missing required node fail-closed 和 cycle detection；专项测试确认 heavy historical weight search 只存在于 monthly graph，不进入 daily / weekly / biweekly 默认 graph。
- 2026-06-03: TRADING-074E 完成。新增 `check_operations_artifact_freshness`、artifact freshness report schema、artifact metadata records、JSON / text / filename 日期解析、`{run_id}` dynamic path glob resolution、required stale/missing/unknown blocking、optional missing warning 和 dependency chain blocking propagation；专项测试覆盖 fresh artifact pass、stale required block、missing required block、missing optional warning、generated_at/as_of_date parsing、dependency chain status 和 dynamic run-id artifact resolution。
- 2026-06-03: TRADING-074F 完成。新增 `evaluate_operations_failure_policy` 和 `etf_operations_failure_policy_v1` 只读报告，把 freshness report 中的 missing / stale / unknown / dependency-blocked artifacts 映射为 `info` / `warning` / `error` / `critical` severity、`continue` / `continue_with_warning` / `skip_optional_step` / `block_dependent_steps` / `fail_pipeline` / `manual_review_required` policy action、pipeline/dependent-step blocking、manual review requirement、recommended action 和稳定 summary；专项测试覆盖 critical validation gate fail_pipeline、optional missing warning、required stale block_dependent_steps、manual review required event 和 stable serialization。
- 2026-06-03: TRADING-074G 完成。新增 `build_operations_owner_review_checklist` 和 `etf_operations_owner_review_checklist_v1` 只读 checklist schema，从 `manual_review_steps` 选择 daily / weekly / monthly / incident owner review step，并把 safety boundary、cadence gate、failure policy summary、blocking / warning / manual-review events 和 owner signoff 固化为稳定 checklist items；专项测试覆盖 daily owner review step、blocking validation event、optional warning event、weekly manual-review event、monthly template、incident template stable serialization 和 cadence mismatch fail-closed。
- 2026-06-03: TRADING-074H 完成。新增 `build_operations_scheduler_dry_run`、`etf_operations_scheduler_dry_run_v1` 只读报告、`write_operations_scheduler_dry_run`、`aits etf ops dry-run --cadence ... --as-of ...` CLI 和 direct dispatcher 路由；dry-run 组合 command graph、artifact freshness、failure policy 和 owner checklist，输出 dry_run_id、planned_steps、execution_order、skipped_optional_steps、blocking_failures、warnings、expected_outputs 和 safety，并固定 `dry_run_only=true`、`commands_executed=false`、`production_state_mutated=false`。专项测试覆盖 daily/weekly/monthly/biweekly dry-run、dependency order、missing required artifact blocker、optional missing warning、optional skip、stable serialization 和 CLI JSON 写入。
- 2026-06-03: TRADING-074I 完成。新增 `build_operations_health_report`、`etf_operations_health_report_v1` 报告 schema、`render_operations_health_report_markdown`、`write_operations_health_report`、`aits etf ops report --cadence ... --as-of ...` CLI 和 direct dispatcher 路由；report 复用 command graph、freshness checker、failure policy、owner checklist 和 scheduler dry-run，生成 JSON / Markdown operations health report，显式展示 safety banner、run metadata、pipeline schedule、command graph summary、artifact freshness summary、dependency status、failures / warnings、owner review checklist、expected next run 和 source artifacts，并固定 `commands_executed=false`、`production_state_mutated=false`。专项测试覆盖 daily/weekly/monthly report、optional warning、JSON / Markdown writer、Markdown stability 和 CLI JSON/Markdown 写入。
- 2026-06-03: TRADING-074J 完成。新增 Reader Brief `Operations Health` 区块和 `etf_operations_health_report` registry entry；Reader Brief 只读 report index 指向的 latest `etf_operations_health_report_v1`，展示 cadence/status、blocking failures、warnings、stale/missing artifacts、next owner review、safety posture、detailed report link、`production_effect=none` 和 `broker_action=none`。缺失 operations health report 时显示 section-level `MISSING`，不运行上游 ops report CLI、不补造 health 状态。专项测试覆盖 Reader Brief payload/HTML/CLI、pass/warning/blocked 状态、stale/missing artifacts、missing graceful path 和 default registry visibility。
