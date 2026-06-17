# TRADING-420 to TRADING-428 Targeted Recovery After TRADING-408~419

最后更新：2026-06-17

- owner: system
- status: BLOCKED_OWNER_INPUT
- linked task: TRADING-420_to_428_TARGETED_RECOVERY_AFTER_408_419
- review cadence: manual recovery governance review after each hard-stop boundary

## 背景

TRADING-408 到 TRADING-419 已完成并提交推送。最新恢复治理状态仍为
`POST_RECOVERY_BLOCKED`：

- remaining blockers: 9
- remaining warnings: 10
- report index unwaived warnings: 9
- normal paper-shadow may resume: false
- observation clock: OBSERVATION_NOT_STARTED
- extended shadow remains forbidden: true
- live trading remains forbidden: true

本批次继续执行 owner 附件中的 targeted recovery tasks：先提交推送既有批次，再
生成精确 blocker/warning inventory，恢复真实 ETF signal inputs，处理 decision snapshot
lifecycle，清理 report index unwaived warnings，建立 cost/benchmark metric source map，
仅 materialize 已证明可派生的 metrics，最后在真实修复后重跑 recovery chain。只有在
normal paper-shadow blocker 真实清除时，才准备 owner signoff packet；不得自动追加 owner
decision。

## 安全边界

- 不削弱 policy、gate、validation 或 blocker taxonomy。
- 不补造 signal、feature、cost、benchmark、baseline、decision snapshot 或 observation data。
- 不创建 official target weights、broker integration、order ticket、production mutation、
  automatic position control、extended shadow 或 live trading approval。
- 如果 signal inputs 仍 blocked，停止 normal paper-shadow resumption 和 promotion rerun。
- 如果 decision snapshot not due 或 blocked，输出 lifecycle status，不伪造 snapshot。
- 如果 report index warnings 是真实 metadata defect，优先修复；只有 legacy-only 且具备
  owner、expiry、linked task 和不隐藏安全风险时才允许 explicit waiver。
- 如果 cost/benchmark metrics 不可派生，保持 insufficient / partial，不 invent metric。

## 分阶段任务

|Task|Status|Acceptance criteria|
|---|---|---|
|TRADING-420 commit-and-push-recovery-triage-batch|DONE|TRADING-408~419 已提交并推送到 `origin/main`；提交 hash `550dd98a`；未尝试解决 remaining blockers。|
|TRADING-421 exact-blocker-and-warning-inventory|VALIDATING|已输出 `exact_blocker_warning_inventory_2026-06-17.json/md` 和 validation；逐条列出 9 blockers、10 recovery warnings 与 9 report-index warnings 的 source、dependency、category、next action、waiver/repair type 和 normal/extended/live boundary；focused tests 2 passed。|
|TRADING-422 restore-etf-feature-matrix-and-signal-series-for-real|VALIDATING|已按 canonical order 重跑 `validate-data`、`etf data validate`、`etf features build --end latest`、`signals build-snapshot --latest`、`etf signals generate --date latest`、`reports signal-snapshot --latest`、signal completeness/recovery；feature/signal artifact 推进到 2026-06-16，blocking=0，hard_stop=false，状态 `SIGNAL_INPUTS_RESTORED_WITH_WARNINGS`。|
|TRADING-423 same-day-decision-snapshot-lifecycle-fix|VALIDATING|未伪造 snapshot；通过 `score-daily --as-of 2026-06-17 --prediction-production-effect none` 真实生成 due decision snapshot；lifecycle policy 转为 `SNAPSHOT_AVAILABLE`，validation PASS；同日 quality gate 转为 `PASS_WITH_WARNINGS`。|
|TRADING-424 clear-report-index-unwaived-nine|VALIDATING|通过真实刷新 data freshness、market panel、score attribution、dashboard、research governance summary 和 artifact lineage artifacts，将 2026-06-17 report index unwaived warnings 从 9 降到 0；最终 `PASS_WITH_EXPLICIT_WAIVERS`，无新增 silent waiver。|
|TRADING-425 cost-and-benchmark-metric-source-map|VALIDATING|新增 metric source map CLI/report/validation；真实 artifact `metric-source-map_287f37a56b0abd0a` 为 `METRIC_SOURCE_MAP_READY`，candidate metrics=6、baseline metrics=5、derivable_now=11/11、validation PASS；未 materialize 未证明 metric。|
|TRADING-426 materialize-cost-and-benchmark-metrics|VALIDATING|已基于 TRADING-425 READY source map materialize 可派生 metrics：cost materialization `cost-metrics-materialization_8136a76f9fe276dd` 为 `COST_INPUTS_AVAILABLE`，cost sensitivity `NOT_MEANINGFUL_UNDER_COSTS`；benchmark materialization `benchmark-baseline-metrics-materialization_f25e132a629a92a1` 为 `BASELINE_METRICS_AVAILABLE`，baseline control `CANDIDATE_UNDERPERFORMS_BASELINES`；validation 均 PASS，promotion 仍保持 blocked。|
|TRADING-427 rerun-recovery-chain-after-real-fixes|VALIDATING|已重跑 readiness/health、normal resumption gate、recovery evidence、monthly review、promotion board、normal/extended observation clocks、extended protocol、roadmap、post-recovery pack、research governance recovery pack 和 triage/ledger/cleanup；最终 recovery governance `RECOVERY_GOVERNANCE_BLOCKED`，remaining blockers=8、warnings=9，normal=false、extended/live forbidden=true。|
|TRADING-428 normal-paper-shadow-owner-signoff-packet|BLOCKED|未生成 signoff packet；TRADING-427 未清除 normal-shadow blockers，normal gate 为 `RESUME_NORMAL_SHADOW_BLOCKED`，owner action 仍为 `hold`，需 owner 明确记录 `continue_normal_shadow` 后才可准备 normal-only signoff。|

## 进展记录

|Date|Status|Notes|
|---|---|---|
|2026-06-17|IN_PROGRESS|TRADING-420 已完成：focused pytest、ruff、compileall、docs freshness、documentation contract、task-register consistency、validate-data、report index、git diff check 通过；latest report quality gate PASS_WITH_WARNINGS；2026-06-17 同日 quality gate 因缺同日 decision snapshot fail-closed；提交 `550dd98a` 已推送 `origin/main`。准备实现 TRADING-421。|
|2026-06-17|VALIDATING|TRADING-421 实现并生成真实 artifact：`EXACT_INVENTORY_BLOCKED`、blockers=9、warnings=10、report-index warnings=9、normal paper-shadow=false、extended/live forbidden=true；validation `PASS_WITH_WARNINGS`，focused pytest 2 passed，scoped ruff/compileall passed。该 inventory 只读展开 source/dependency/repair/boundary，不修复 blocker、不创建 waiver、不补造 snapshot/metrics/signal。|
|2026-06-17|VALIDATING|TRADING-422 已通过 intended pipeline 重新生成 signal inputs：`etf_feature_matrix:2026-06-16:dae1c63502c1`、`etf_signal_series:2026-06-16:107d24331e83`、signal completeness monitor `signal-input-completeness_172dfeae0ce29c54`；recovery `signal-input-recovery_44fd18f00827b38b` 和 completeness recovery `signal-input-completeness-recovery_4cddf282d5c1742a` 均为 `SIGNAL_INPUTS_RESTORED_WITH_WARNINGS`、blocking=0、hard_stop=false。唯一 warning 是 latest signal snapshot `LIMITED`，不是 feature/signal missing、stale、schema 或 coverage blocker。|
|2026-06-17|VALIDATING|TRADING-423 已用真实 `score-daily` 生成 `data/processed/decision_snapshots/decision_snapshot_2026-06-17.json`；decision snapshot lifecycle policy 由 missing blocker 转为 `SNAPSHOT_AVAILABLE`，validation PASS；Reader Brief 和 report quality gate 已刷新，quality gate 为 `PASS_WITH_WARNINGS`。|
|2026-06-17|VALIDATING|TRADING-424 已通过真实报告刷新清理 report index unwaived warnings：data freshness、market panel、score-change attribution、evidence dashboard、research governance summary、artifact lineage graph/validation 均重跑；最终 report index `PASS_WITH_EXPLICIT_WAIVERS`、unwaived=0。market data freshness 仍真实披露 `MISSING` / latest manifest date 2026-06-15，未用 silent waiver 隐藏。|
|2026-06-17|VALIDATING|TRADING-425 已实现并生成 source map：`metric-source-map_287f37a56b0abd0a`、`METRIC_SOURCE_MAP_READY`、candidate_metric_count=6、baseline_metric_count=5、derivable_now_count=11、missing_metric_count=0、validation PASS。新增 focused pytest 3 passed、scoped ruff/compileall passed；该层只做来源证明，不 materialize cost/benchmark metrics、不刷新数据、不运行 optimization/backtest、不触发 broker/order。|
|2026-06-17|VALIDATING|TRADING-426 已按 source map 证明结果重跑 materialization：cost metrics `COST_INPUTS_AVAILABLE`，cost review `NOT_MEANINGFUL_UNDER_COSTS`；benchmark metrics `BASELINE_METRICS_AVAILABLE`，benchmark control `CANDIDATE_UNDERPERFORMS_BASELINES`、0/5 baselines outperformed。所有 validation PASS，research-only / no broker / no official target / `production_effect=none`；candidate 仍应回到 research，不能 promotion。|
|2026-06-17|VALIDATING|TRADING-427 已完成恢复链重跑：readiness/health recovery `MANUAL_REVIEW_REQUIRED`、normal resumption gate `RESUME_NORMAL_SHADOW_BLOCKED`（owner_action=`hold`）、recovery evidence `RECOVERY_EVIDENCE_COMPLETE` 但 blockers=2、monthly review `MONTHLY_REVIEW_BLOCKED`、promotion board `HOLD_FOR_MORE_DATA`、normal clock `OBSERVATION_NOT_STARTED`、extended clock `OBSERVATION_PERIOD_UNMET`、extended protocol `EXTENDED_SHADOW_BLOCKED`、roadmap `ROADMAP_BLOCKED`、post-recovery pack `POST_RECOVERY_BLOCKED`、research governance recovery pack `RECOVERY_GOVERNANCE_BLOCKED`。最终 remaining blockers=8、warnings=9、report-index unwaived=0、normal paper-shadow=false、extended/live forbidden=true。|
|2026-06-17|BLOCKED|TRADING-428 hard stop：由于 TRADING-427 未清除 normal-shadow blockers，且 owner action 仍为 hold，未生成 normal paper-shadow owner signoff packet，未追加 owner decision。|
