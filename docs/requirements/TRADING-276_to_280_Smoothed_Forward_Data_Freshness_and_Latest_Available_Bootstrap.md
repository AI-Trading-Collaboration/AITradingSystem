# TRADING-276 to 280 Smoothed Forward Data Freshness and Latest-Available Bootstrap

最后更新：2026-07-15

## 状态

BASELINE_DONE（ARCH-004G2.4CT=`COMPLETE_G2_4_CONTINUES`；等待真实数据刷新与 forward/PIT/DQ/cost/holdout 证据）

## 背景

TRADING-271～275 已完成 smoothed daily emission、outcome due scan、outcome update、forward classification 和 weekly runner。当前 2026-06-20 requested due scan / weekly runner 在本地 cache 状态下被 `validate-data` 正确 fail-closed：prices cache latest 为 2026-06-12，FRED rates latest 为 2026-06-11，blocking errors 为 `prices_stale` 与 `rates_stale`。

本阶段不把 fail-close 当作代码错误处理。目标是让系统明确解释 requested as-of 是否被当前数据支持，给出安全的 `latest_valid_as_of` observation fallback，并在数据刷新后提供可审计 retry runner。

## 范围

|任务|名称|状态|验收重点|
|---|---|---|---|
|TRADING-276|Smoothed Forward Data Freshness Preflight|BASELINE_DONE_G2_4CT|输出可从 frozen source slice 重算的 data freshness snapshot、runnable command matrix、blocked reason matrix，并能重验 live cache/DQ/cutoff。|
|TRADING-277|Latest-Available Daily Emission Fallback|BASELINE_DONE_G2_4CT|只允许使用 validated preflight 的 `latest_valid_as_of` 运行 hardened daily emission；不得运行 due scan 或 outcome update。|
|TRADING-278|Blocked Due / Weekly Run Explanation Pack|BASELINE_DONE_G2_4CT|只消费 validated Preflight，把 source-derived blocked commands 转为 owner-readable explanation 和 safe next action。|
|TRADING-279|Source Refresh & Rerun Plan|BASELINE_DONE_G2_4CT|要求 Preflight/Explain exact lineage，只生成 source refresh requirements 和 rerun commands，不直接刷新外部数据。|
|TRADING-280|Smoothed Bootstrap Retry Runner|BASELINE_DONE_G2_4CT|先运行并冻结 validated preflight；blocked 时停止 outcome update；ready 时串联并验证 hardened full smoothed forward evidence chain。|

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

## ARCH-004G2.4CT canonical exit contract（2026-07-15）

旧 2026-06-14 baseline 只证明当时的功能路径可运行。当前审计确认：五类 producer 均未写入
input snapshot；Latest/Explain/Refresh 直接读取 materialized JSON 而不先调用上游 validator；
Preflight validator 不重跑 live cache/DQ，不校验 source checksum/cutoff；Retry validator 不验证其
Preflight/Latest/Weekly artifact，也不能从 source 重建 steps/summary/report。五类 validator 都不能
发现 Markdown/Reader Brief tamper。该 baseline 不能继续作为 ARCH-004 canonical 证据。

本 slice 的完成边界固定如下：

1. 15 callbacks 从 legacy CLI root 迁入独立 canonical freshness interface；五类业务实现迁入
   `dynamic_v3_system_target_smoothed_freshness.py`，legacy domain 仅保留 lazy compatibility
   wrappers；CLI path/options/default/help/exit/tree hash 不漂移。
2. 五类 artifact 分别新增
   `smoothed_data_preflight_input_snapshot.v2`、
   `smoothed_latest_emission_input_snapshot.v2`、
   `smoothed_blocked_explain_input_snapshot.v2`、
   `smoothed_refresh_plan_input_snapshot.v2` 与
   `smoothed_bootstrap_retry_input_snapshot.v2`。Snapshot 只冻结 consumer 实际读取的 bounded
   source views、content commitments、semantic cutoff、DQ result 和 lineage，不递归复制上游
   snapshot 正文。
3. Preflight 在正式写件前运行与 `aits validate-data` 相同路径；冻结 prices/rates/required
   secondary source 的明确 paths、SHA-256、row/date coverage、expected identities、DQ issues 和
   requested cutoff。`latest_valid_as_of` 必须只由 validator-backed required source coverage 求最小
   值；missing、duplicate、non-finite、future、stale 和 invalid schema 显式 fail closed。Model Target
   只作可用性披露时也必须采用 semantic unique、cutoff 前且 validator PASS 的 source，不再吞掉
   invalid/ambiguous latest 为 null。
4. Preflight validator 重跑 live DQ/source commitments，并从 snapshot 重建 freshness、command/
   blocker matrices 和全部 JSON/Markdown/Reader Brief；source/output 任一 byte 漂移均 FAIL。
5. Latest Emission 写前先验证 exact Preflight，resolved date 只能为其
   `min(requested_date, latest_valid_as_of)`；调用 G2.4CS hardened Daily Emission，并验证 exact output。
   结果必须保持 `fallback_scope=daily_emission_only`、due/update=false、future=false；当前
   candidate=null 时 emitted=0/`NOT_REGISTERED`，不得复活旧固定3d event。
6. Blocked Explain 写前验证 exact Preflight；所有 command/reason/explanation/action 均由 frozen
   Preflight views 确定性生成。若 Preflight 没有 blocked command，必须明确输出0条/not-applicable，
   不得伪造 blocker。
7. Refresh Plan 写前同时验证 exact Preflight 与 Explain，要求 Explain 的 source_preflight_id 和
   blocked command set 完全匹配；requirements/rerun steps 从同一 requested date 与 source coverage
   重建。只允许 planning，`external_refresh_executed=false`。
8. Retry 每次先生成并验证一个 exact Preflight；READY 路径调用 G2.4CS hardened Weekly runner并
   验证 exact nine-stage artifact，LATEST_AVAILABLE_ONLY 路径只调用 validated Latest Emission，
   BLOCKED 路径所有下游步骤显式 SKIPPED。Snapshot 冻结 caller-specified Binding/Switch/Owner ids
   与产生的 exact artifact commitments；不得从目录选择替代 lineage。
9. Retry summary/steps/artifacts 必须逐项从 validated child artifact 重建；blocked 不得出现
   outcome-update PASS，latest-only 不得出现 due/update/classification，所有路径固定
   `can_execute_switch=false`。缺失/duplicate/future/cross-lineage/tamper/non-finite 一律 FAIL。
10. 所有五类 validator 逐 byte 重建 manifest、business JSON、Markdown、Reader Brief；完成
    focused/CLI/architecture/contract、Ruff、compileall、manifests/deprecation/source hashes。单 slice
    完成仍为 `COMPLETE_G2_4_CONTINUES`，不触发 ARCH-005 handoff、不进入 G2.5，
    `production_effect=none`。

## Progress Notes

- 2026-07-15: G2.4CT=`COMPLETE_G2_4_CONTINUES`。正式 focused/architecture/contract/full
  分别为 `125/281/203/6,018 passed`；full runtime artifact 为
  `outputs/validation_runtime/full_20260714T195603Z/test_runtime_summary.json`，耗时
  `2,514.64s`。首轮 full 的四项 post-refresh lineage 失败已修复并在完整门禁中闭合；generated
  manifests=`926 modules / 1,124 tests / 858 writers / 0 violations`，CLI tree/hash保持不变。
  当前 null candidate 仍为 `0/NOT_REGISTERED`，后续真实数据/研究证据依赖保留；整个 G2.4 继续，
  不触发 ARCH-005 handoff、不进入 G2.5，`production_effect=none`。

- 2026-07-15: G2.4CT implementation 已完成并进入 `VALIDATING`。15 个 callback 已迁至
  `interfaces/cli/etf_portfolio/dynamic_v3_system_target_smoothed_freshness.py`，五类 producer/
  validator 已迁至独立 canonical domain；legacy root CLI 删除469行，`15,842 -> 15,373`，legacy
  domain 删除旧业务实现并仅留15个 lazy wrapper，`14,895 -> 13,978`。五类 v2 snapshot、strict
  Model Target semantic selection、live DQ/source replay、Preflight→Latest/Explain→Refresh 和
  Preflight→Weekly/Latest Retry exact lineage、全部 JSON/Markdown/Reader Brief byte rebuild 已落地。
  READY Explain 的 zero-blocker、cross-preflight rejection、raw cache/source/output/weekly-child tamper
  fail-closed 与 current null-candidate `0/NOT_REGISTERED` 均有测试；implementation+CLI combined
  focused=`125 passed / 72.87s`，generated manifests=`926 modules / 1,124 tests / 858 writers / 0
  violations`。验证会话缓存复用 nested validator，未减少任何 gate；正式 architecture/contract/full
  结果待本 slice closeout 记录。单 slice 仍不触发 handoff、不进入 G2.5，`production_effect=none`。

- 2026-07-15: 首轮 full `6014 passed / 4 failed` 定位到 TRADING-281 downstream
  `post-refresh-validation` 未继承新 Preflight 的 strict Model Target lineage。修复后
  source-refresh 必须先验证 exact refresh-plan，并把 `source_preflight_id/model_target_dir` 写入请求；
  post-refresh 只能使用该冻结 lineage，显式不同目录必须 FAIL。`test_smoothed_data_refresh.py`
  现为 `5 passed`，不再回退默认 Model Target 目录。

- 2026-07-15: G2.4CT contract freeze并进入 `IN_PROGRESS`。范围固定为 TRADING-276～280 的
  Preflight/Latest/Explain/Refresh/Retry 共15 callbacks迁独立 canonical interface/domain。旧链无
  input snapshot、无上游 live validation、吞掉 invalid Model Target latest、validators 只做浅层
  file/shape/safety 检查且不能重建报告。Exit 改为五类 bounded v2 snapshots、same validate-data
  path、semantic cutoff/source commitments、exact Preflight→Latest/Explain→Refresh 与
  Preflight→Weekly/Latest Retry lineage、current null-candidate preservation 和全部 views byte
  rebuild；继续 G2.4，不进入 G2.5，`production_effect=none`。

- 2026-06-14: 任务创建并进入 IN_PROGRESS。当前 best solution 是保留 validate-data fail-close，并新增 preflight/explain/refresh/retry artifacts；blocked source freshness 必须通过真实 cache refresh 解除，不允许用 temporary workaround 绕过。
- 2026-06-14: 实现完成并转入 VALIDATING。新增五个 CLI family、validators、report registry entries、Reader Brief `Dynamic Rescue Smoothed Freshness Bootstrap`、artifact catalog、system flow、README 和 operations runbook 更新。真实 2026-06-20 requested run 输出 preflight `smoothed-data-preflight_d32351fbfce79cb7`，`freshness_status=BLOCKED_STALE_DATA`，`latest_valid_as_of=2026-06-11`，`blocking_errors=prices_stale,rates_stale`；latest-available fallback 输出 `smoothed-latest-emission_821012c486311d75`，`resolved_as_of=2026-06-11`，`emitted_event_count=1`，`outcome_update_allowed=false`，`future_data_used=false`；blocked explain 输出 `smoothed-blocked-explain_cb22419b78a4874d`，blocked_command_count=4；refresh plan 输出 `smoothed-refresh-plan_3b86b3a42eceecec`，stale_source_count=3，`external_refresh_executed=false`；retry 输出 `smoothed-bootstrap-retry_6c9cae2022a8dc52`，`retry_status=BLOCKED`，updated_windows=0，`can_execute_switch=false`。验证通过 dynamic-v3 validation、五个新增 artifact validators、family artifact validation、documentation contract、focused pytest、ruff、compileall、git diff check 和 full pytest `2439 passed, 640 warnings`。
