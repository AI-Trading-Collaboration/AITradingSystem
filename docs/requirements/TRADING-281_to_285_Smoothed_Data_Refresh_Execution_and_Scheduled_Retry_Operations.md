# TRADING-281 to 285 Smoothed Data Refresh Execution and Scheduled Retry Operations

最后更新：2026-07-15

## 状态

BASELINE_DONE（ARCH-004G2.4CU=`COMPLETE_G2_4_CONTINUES`；canonical migration与正式门禁已闭合；真实 provider 复验依赖保留）

## 背景

TRADING-276～280 已把 2026-06-20 requested smoothed forward run 的 stale data
blocker 转成可解释的 preflight、latest-available emission、blocked explain、refresh
plan 和 bootstrap retry artifact。当前本地 cache 仍未覆盖 requested date：
`prices_daily` / `prices_marketstack_daily` latest 为 2026-06-12，`rates_daily`
latest 为 2026-06-11，因此 full retry 必须继续 fail-closed。

本阶段把 source refresh plan 升级为受控 refresh execution、post-refresh
validation、retry resume、sample growth dashboard 和 owner data readiness pack。真实
外部刷新必须显式执行，不允许默认刷新、不允许绕过 `aits validate-data`，也不允许把
stale data 伪造成可更新 outcome。

## 范围

|任务|名称|状态|验收重点|
|---|---|---|---|
|TRADING-281|Price / Rate Cache Refresh Executor|BASELINE_DONE_G2_4CU|`smoothed-source-refresh plan/execute/report` 和 validator 可运行；默认 dry-run，真实刷新必须显式 `--execute-refresh`，并记录 before/after/latest/checksum audit。|
|TRADING-282|Post-refresh Freshness Validation|BASELINE_DONE_G2_4CU|`smoothed-post-refresh-validate run/report` 重新运行 validate-data 等价门禁与 smoothed preflight，输出 `retry_decision`。|
|TRADING-283|Smoothed Retry Resume Runner|BASELINE_DONE_G2_4CU|`smoothed-retry-resume run/report` 只在 `RETRY_READY` 时 resume；未 ready 时 blocked，不更新 outcome。|
|TRADING-284|Forward Sample Growth Dashboard|BASELINE_DONE_G2_4CU|`smoothed-sample-growth build/report` 输出 before/after/delta 和 target-level progress。|
|TRADING-285|Owner Data Readiness Status Pack|BASELINE_DONE_G2_4CU|`smoothed-data-readiness pack/report` 给 owner 当前状态、推荐动作、progress 和 no broker/no production 边界。|

## 设计决策

- Source refresh executor 默认只生成 dry-run artifact；`execute` 子命令也必须显式传入
  `--execute-refresh`。
- 执行器只允许刷新 local market/rate cache，不得读取或修改 broker、portfolio、
  official target weights、`position_advisory_v1.yaml` 或 production state。
- Refresh 后的 retry 资格由 post-refresh validation 决定。`validate-data` FAIL、
  stale source 未解除或 partial refresh 未覆盖 requested date 时，retry resume 必须停止。
- Retry resume 不直接触发 switch；`can_execute_switch=false` 是本阶段固定安全边界。
- Sample growth 使用 retry resume artifact 中记录的 before/after 计数，不从报告文字反推。
- Owner readiness pack 是人工运营状态包，不是 approval、order ticket 或 production
  target weight 变更。

## 新增 CLI

```bash
aits etf dynamic-v3-rescue smoothed-source-refresh plan --refresh-plan-id <refresh_plan_id>
aits etf dynamic-v3-rescue smoothed-source-refresh execute --refresh-plan-id <refresh_plan_id> --execute-refresh
aits etf dynamic-v3-rescue smoothed-source-refresh report --latest
aits etf dynamic-v3-rescue validate-smoothed-source-refresh --refresh-execution-id <refresh_execution_id>

aits etf dynamic-v3-rescue smoothed-post-refresh-validate run --refresh-execution-id <refresh_execution_id>
aits etf dynamic-v3-rescue smoothed-post-refresh-validate report --latest
aits etf dynamic-v3-rescue validate-smoothed-post-refresh --post-refresh-id <post_refresh_id>

aits etf dynamic-v3-rescue smoothed-retry-resume run --post-refresh-id <post_refresh_id>
aits etf dynamic-v3-rescue smoothed-retry-resume report --latest
aits etf dynamic-v3-rescue validate-smoothed-retry-resume --resume-id <resume_id>

aits etf dynamic-v3-rescue smoothed-sample-growth build --resume-id <resume_id>
aits etf dynamic-v3-rescue smoothed-sample-growth report --latest
aits etf dynamic-v3-rescue validate-smoothed-sample-growth --growth-id <growth_id>

aits etf dynamic-v3-rescue smoothed-data-readiness pack --refresh-execution-id <refresh_execution_id> --post-refresh-id <post_refresh_id> --resume-id <resume_id> --growth-id <growth_id>
aits etf dynamic-v3-rescue smoothed-data-readiness report --latest
aits etf dynamic-v3-rescue validate-smoothed-data-readiness --readiness-id <readiness_id>
```

## Artifact Contract

新增 artifact families：

- `reports/etf_portfolio/dynamic_v3_rescue/smoothed_source_refresh/<refresh_execution_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/smoothed_post_refresh_validation/<post_refresh_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/smoothed_retry_resume/<resume_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/smoothed_sample_growth/<growth_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/smoothed_data_readiness/<readiness_id>/`

Each family writes a manifest JSON, machine-readable detail JSON files, Markdown
report, and `reader_brief_section.md` where applicable. Report text is Chinese by
default while preserving identifiers such as `RETRY_READY`, `BLOCKED_STALE_DATA`,
`prices_daily`, `rates_daily`, `broker_action_allowed` and `production_effect`.

## Acceptance Criteria

- Source refresh dry-run and explicit execute modes produce readable request,
  results and audit artifacts.
- Execute mode requires `--execute-refresh`; missing execute flag cannot mutate cache.
- Post-refresh validation reruns the same cached data quality/preflight code path and
  stops retry when stale/data-quality blockers remain.
- Retry resume is blocked unless post-refresh `retry_decision=RETRY_READY`; ready
  runs reuse smoothed bootstrap retry and preserve `can_execute_switch=false`.
- Sample growth dashboard exposes before/after/delta and progress toward the
  existing smoothed confirmation sample requirements.
- Owner readiness pack exposes current status, recommended owner action, source
  status, retry status, sample growth status and no broker/no production boundary.
- README、operations runbook、system flow、report registry、artifact catalog、Reader
  Brief、focused tests、ruff、compileall、git diff check、dynamic-v3 validation、
  artifact family validation and documentation contract pass or exact blockers are
  documented.

## ARCH-004G2.4CU canonical exit contract（2026-07-15）

1. 16 个 CLI callback（Source Refresh 4 个，其余四族各3个）迁至独立 canonical interface；
   legacy root 不再定义 callback，command path/options/default/help/exit 语义与 CLI tree hash 不变。
2. 15 个 producer/report/validator 业务入口迁至独立 canonical domain；legacy system-target 仅保留
   lazy compatibility wrappers，不复制业务实现。
3. 五族分别冻结 `smoothed_source_refresh_input_snapshot.v2`、
   `smoothed_post_refresh_validation_input_snapshot.v2`、`smoothed_retry_resume_input_snapshot.v2`、
   `smoothed_sample_growth_input_snapshot.v2`、`smoothed_data_readiness_input_snapshot.v2`。
4. Source Refresh 写件前必须验证 exact Refresh Plan，冻结 config、requested cache paths、明确
   `execute_refresh` 授权、before/after file commitments 与结果/audit；dry-run 必须证明 before=after，
   execute validator 只验证冻结授权与真实 after commitments，不重放有副作用的 provider refresh。
5. Post-refresh 必须绑定 validated Source Refresh 与本次生成的 validated Preflight，严格保持
   Refresh Plan→Preflight 的 `source_preflight_id/model_target_dir/requested_as_of` lineage；禁止默认目录
   回退、future-as-of 或跨 lineage 拼接。
6. Retry Resume 只消费 validated Post-refresh；`RETRY_READY` 时绑定 validated Bootstrap Retry child，
   blocked 时不得生成 child 或运行 outcome update。Sample Growth 只从 validated Resume 的冻结计数
   计算；Data Readiness 要求 Refresh/Post/Resume/Growth exact chain 且所有 child validator PASS。
7. 五族 validator 必须 live replay 可重验 sources/commitments，并从 snapshot 逐 byte 重建全部 JSON、
   Markdown 与 Reader Brief；任一 source/output/lineage/authorization tamper 必须 FAIL。
8. 保留 `--execute-refresh` 显式授权、same validate-data path、真实 provider blocker、
   `can_execute_switch=false`、无 broker/order/official weights/production effect；不得为通过测试执行真实
   provider refresh 或伪造 fresh cache。
9. 接入 validation-session PASS-only content-fingerprint reuse；不得减少 focused/architecture/contract/
   full gate。完成 callback migration matrix、manifests/deprecation/source hashes 后，本 slice 仍只能
   `COMPLETE_G2_4_CONTINUES`，不触发 ARCH-005 handoff、不进入 G2.5。

## Progress Notes

- 2026-07-15: G2.4CU=`COMPLETE_G2_4_CONTINUES`，任务转`BASELINE_DONE`等待真实 provider、
  network/credential 与 2026-06-20 后数据刷新复验。正式 focused/architecture/contract/full=
  `123/282/203/6,023 passed`，full=`1,939.34s`；generated=`928 modules / 1,125 tests /
  858 writers / 0 violations`，CLI tree/hash不变。完整验证相对 G2.4CT 的`2,514.64s`单次缩短
  约22.9%，但不宣称稳定收益；长尾仍为weekly confirmation=`1,229.71s`、dashboard=
  `824.66s`、rule queue=`726.05s`，继续由P1 runtime治理项按content-fingerprint DAG去重、
  时长/内存感知分片和active-node heartbeat推进，不减少任何gate。整个G2.4继续，不触发
  ARCH-005 handoff、不进入G2.5，`production_effect=none`。
- 2026-07-15: G2.4CU canonical implementation完成并转 `VALIDATING`。16 个 Typer callback 已迁至
  `interfaces/cli/etf_portfolio/dynamic_v3_system_target_smoothed_refresh.py`，15 个业务入口已迁至
  `etf_portfolio/dynamic_v3_system_target_smoothed_refresh.py`；legacy CLI 删除569行/16 functions，
  legacy domain删除1,027行并只保留15个lazy wrappers。CLI tree仍为41 root / 291 groups /
  993 leaves / 0 duplicates，tree hash保持
  `d4744f3ec1bbbfc05d10246f7969b3f9174e4cfebc9bec9d8b39a472e83bc6f3`。
  五族v2 snapshot、all-view byte rebuild、provider validator non-replay、dry-run before=after、blocked
  no-child、exact Bootstrap child revalidation与跨链readiness拒绝已由hardening tests证明；原业务/CLI
  focused当前为 `123 passed`（119 CLI+业务合同、4 hardening）。同一顶层validator复用PASS-only
  content-fingerprint validation session；更广的confirmation DAG与duration-aware shard优化继续由
  `ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE`治理，不缩减任何gate。下一步为
  architecture/contract/full、manifests/deprecation/source hashes closeout；单slice仍不触发handoff或G2.5。
- 2026-07-15: G2.4CU contract freeze并进入 `IN_PROGRESS`。审计确认16个 callback与15个业务入口仍在
  legacy root/domain；旧五族没有bounded input snapshot，validators主要检查文件/ID/shape/safety，
  不能逐byte重建报告或证明 exact Refresh→Post→Resume→Growth→Readiness lineage。Source Refresh
  execute含显式外部写作用，canonical validator必须验证冻结before/after授权与commitments，不能以
  validator重放provider副作用。真实provider/network依赖继续保留，`production_effect=none`。
- 2026-06-14: 任务创建并进入 IN_PROGRESS。当前 best solution 是实现受控执行器和
  post-refresh retry resume 闭环；真实外部 refresh 依赖 provider credentials /
  network availability，若环境缺失必须记录为 blocker，不得伪造 fresh cache。
- 2026-06-14: 实现完成并转入 VALIDATING。新增受控 source refresh config、
  source refresh / post-refresh validation / retry resume / sample growth / data
  readiness artifact writers、CLI、validators、Reader Brief、registry/catalog/runbook/system
  flow/README 和 focused tests。默认真实 artifact 链路基于 latest refresh plan
  `smoothed-refresh-plan_3b86b3a42eceecec` 生成：
  source refresh `smoothed-source-refresh_9461739423d0afda` 为 `DRY_RUN_ONLY`、
  post-refresh `smoothed-post-refresh_0155c93dc54dc748` 为 `retry_decision=STILL_BLOCKED`
  且 `validate_data_status=FAIL`、retry resume
  `smoothed-retry-resume_c3ba271115fc6c3a` 为 `BLOCKED`、sample growth
  `smoothed-sample-growth_e34995a67ee1221f` 为 `INSUFFICIENT_DATA`、owner readiness
  `smoothed-data-readiness_5ad0bfce2e99d2bc` 为 `current_status=REFRESH_REQUIRED`
  / `recommended_owner_action=run_refresh`。原因：requested_as_of=2026-06-20 仍是当前
  环境日期之后的未来 requested date，且 cache latest 仍不足；未执行真实 provider refresh，
  避免对未来日期伪造 fresh cache。五个新增 artifact validators、dynamic-v3 root
  validation、documentation contract、focused pytest、相邻 freshness/bootstrap pytest、ruff、
  compileall、git diff check 和 full pytest `2444 passed, 640 warnings` 已通过；后续只有在
  2026-06-20 或之后且 provider/network 可用时，才能显式 `--execute-refresh` 后重新跑
  post-refresh validation 与 retry resume。
