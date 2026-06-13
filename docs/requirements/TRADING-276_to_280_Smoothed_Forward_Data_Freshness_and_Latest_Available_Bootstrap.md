# TRADING-276 to 280 Smoothed Forward Data Freshness and Latest-Available Bootstrap

最后更新：2026-06-14

## 背景

TRADING-271～275 已完成 smoothed daily emission、outcome due scan、outcome update、forward classification 和 weekly runner。当前 2026-06-20 requested due scan / weekly runner 在本地 cache 状态下被 `validate-data` 正确 fail-closed：prices cache latest 为 2026-06-12，FRED rates latest 为 2026-06-11，blocking errors 为 `prices_stale` 与 `rates_stale`。

本阶段不把 fail-close 当作代码错误处理。目标是让系统明确解释 requested as-of 是否被当前数据支持，给出安全的 `latest_valid_as_of` observation fallback，并在数据刷新后提供可审计 retry runner。

## 范围

|任务|名称|状态|验收重点|
|---|---|---|---|
|TRADING-276|Smoothed Forward Data Freshness Preflight|DONE|输出 data freshness snapshot、runnable command matrix、blocked reason matrix，并能校验。|
|TRADING-277|Latest-Available Daily Emission Fallback|DONE|只允许使用 preflight 的 `latest_valid_as_of` 运行 daily emission；不得运行 due scan 或 outcome update。|
|TRADING-278|Blocked Due / Weekly Run Explanation Pack|DONE|把 stale data blocked run 转为 owner-readable explanation 和 safe next action。|
|TRADING-279|Source Refresh & Rerun Plan|DONE|只生成 source refresh requirements 和 rerun commands，不直接刷新外部数据。|
|TRADING-280|Smoothed Bootstrap Retry Runner|DONE|先运行 preflight；blocked 时停止 outcome update；ready 时串联 full smoothed forward evidence chain。|

## 设计决策

- `latest_available` 只用于 smoothed daily observation emission，不用于伪造成熟 outcome。
- `latest_valid_as_of` 必须来自 preflight artifact，不允许 CLI 手动猜测 fallback date。
- due scan、outcome update、weekly runner 仍必须依赖同一份 cached data quality gate，并在 `FAIL` 时停止。
- blocked state 是业务状态，不是成功伪装；报告必须说明 stale source、blocked command、safe next action 和解除条件。
- retry runner 的 `COMPLETED` 只在 preflight ready 且 full chain 成功时出现；只能 latest-available emission 时标记 `PARTIAL`；stale/future/data-quality fail 时标记 `BLOCKED`。
- 所有新增 artifact 固定输出 `research_target_only=true`、`paper_shadow_only=true`、`not_official_target_weights=true`、`broker_action_allowed=false`、`broker_action_taken=false`、`order_ticket_generated=false`、`auto_apply=false`、`production_effect=none`。

## 新增 CLI

```bash
aits etf dynamic-v3-rescue smoothed-data-preflight run --requested-as-of 2026-06-20
aits etf dynamic-v3-rescue smoothed-data-preflight run --requested-week-ending 2026-06-20
aits etf dynamic-v3-rescue smoothed-data-preflight report --latest
aits etf dynamic-v3-rescue validate-smoothed-data-preflight --preflight-id <preflight_id>

aits etf dynamic-v3-rescue smoothed-latest-emission run --preflight-id <preflight_id>
aits etf dynamic-v3-rescue smoothed-latest-emission report --latest
aits etf dynamic-v3-rescue validate-smoothed-latest-emission --latest-emission-id <latest_emission_id>

aits etf dynamic-v3-rescue smoothed-blocked-explain run --preflight-id <preflight_id>
aits etf dynamic-v3-rescue smoothed-blocked-explain report --latest
aits etf dynamic-v3-rescue validate-smoothed-blocked-explain --explain-id <explain_id>

aits etf dynamic-v3-rescue smoothed-refresh-plan run --preflight-id <preflight_id> --explain-id <explain_id>
aits etf dynamic-v3-rescue smoothed-refresh-plan report --latest
aits etf dynamic-v3-rescue validate-smoothed-refresh-plan --refresh-plan-id <refresh_plan_id>

aits etf dynamic-v3-rescue smoothed-bootstrap-retry run --requested-as-of 2026-06-20
aits etf dynamic-v3-rescue smoothed-bootstrap-retry run --requested-week-ending 2026-06-20
aits etf dynamic-v3-rescue smoothed-bootstrap-retry report --latest
aits etf dynamic-v3-rescue validate-smoothed-bootstrap-retry --retry-id <retry_id>
```

## Artifact Contract

新增 artifact families：

- `reports/etf_portfolio/dynamic_v3_rescue/smoothed_data_preflight/<preflight_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/smoothed_latest_emission/<latest_emission_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/smoothed_blocked_explain/<explain_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/smoothed_refresh_plan/<refresh_plan_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/smoothed_bootstrap_retry/<retry_id>/`

Each family writes a manifest JSON, machine-readable detail JSON files, Markdown report, and `reader_brief_section.md` where applicable. Report text is Chinese by default and keeps identifiers such as `prices_stale`, `rates_stale`, `latest_valid_as_of`, `broker_action_allowed` in English for audit precision.

## Acceptance Criteria

- Preflight can identify `prices_stale`, `rates_stale`, missing price source, data quality fail, and latest valid date.
- Runnable command matrix distinguishes requested blocked commands from latest-available daily emission.
- Latest emission fallback records requested/resolved as-of, does not use future data, and sets `due_scan_allowed=false`, `outcome_update_allowed=false`.
- Blocked explanation pack is owner-readable and names stale sources, blocked commands, fallback scope, and safe next action.
- Refresh plan names stale sources, current latest dates, required-through date, and rerun command order without refreshing external data.
- Retry runner fail-closes when preflight is blocked and does not run outcome update; when ready it can chain emission, due scan, update, classification, progress, dashboard, monitor, switch readiness, and owner renewal.
- New validators, focused tests, ruff, compileall, git diff check, dynamic-v3 validation, and artifact family validation pass or failures are documented with exact blocker.

## Progress Notes

- 2026-06-14: 任务创建并进入 IN_PROGRESS。当前 best solution 是保留 validate-data fail-close，并新增 preflight/explain/refresh/retry artifacts；blocked source freshness 必须通过真实 cache refresh 解除，不允许用 temporary workaround 绕过。
- 2026-06-14: 实现完成并转入 VALIDATING。新增五个 CLI family、validators、report registry entries、Reader Brief `Dynamic Rescue Smoothed Freshness Bootstrap`、artifact catalog、system flow、README 和 operations runbook 更新。真实 2026-06-20 requested run 输出 preflight `smoothed-data-preflight_d32351fbfce79cb7`，`freshness_status=BLOCKED_STALE_DATA`，`latest_valid_as_of=2026-06-11`，`blocking_errors=prices_stale,rates_stale`；latest-available fallback 输出 `smoothed-latest-emission_821012c486311d75`，`resolved_as_of=2026-06-11`，`emitted_event_count=1`，`outcome_update_allowed=false`，`future_data_used=false`；blocked explain 输出 `smoothed-blocked-explain_cb22419b78a4874d`，blocked_command_count=4；refresh plan 输出 `smoothed-refresh-plan_3b86b3a42eceecec`，stale_source_count=3，`external_refresh_executed=false`；retry 输出 `smoothed-bootstrap-retry_6c9cae2022a8dc52`，`retry_status=BLOCKED`，updated_windows=0，`can_execute_switch=false`。验证通过 dynamic-v3 validation、五个新增 artifact validators、family artifact validation、documentation contract、focused pytest、ruff、compileall、git diff check 和 full pytest `2439 passed, 640 warnings`。
