# TRADING-408 to TRADING-419 Blocker Resolution Batch

最后更新：2026-06-17

- owner: system
- status: VALIDATING
- linked task: TRADING-408_to_419_RECOVERY_BLOCKER_RESOLUTION_BATCH
- review cadence: manual recovery governance review after every blocker-resolution rerun

## 背景

TRADING-401 到 TRADING-407 已完成 recovery triage batch，最新恢复治理状态仍为
`RECOVERY_GOVERNANCE_BLOCKED`：

- remaining blockers: 9
- remaining warnings: 10
- normal paper-shadow may resume: false
- extended shadow remains forbidden: true
- live trading remains forbidden: true

本批次按附件要求继续处理 TRADING-408 到 TRADING-419。范围限定为恢复治理、
信号输入、readiness/health、cost/benchmark evidence、owner hold 记录、report index
warning cleanup、requirements freshness、normal paper-shadow resumption owner gate、
normal observation clock 和 final post-recovery governance pack。

## 安全边界

- 不削弱 policy、gate、validation 或 blocker taxonomy。
- 不创建 official target weights。
- 不创建 broker integration、order ticket 或 live trading action。
- 不修改 production state、candidate state 或 paper-shadow account state。
- 不使用 silent waivers。
- 不补造 signal、cost、benchmark、baseline 或 observation data。
- 如果 signal inputs 仍 blocking，停止 normal paper-shadow resumption / promotion rerun。
- 如果 cost 或 benchmark metrics 仍 insufficient 或 underperform，promotion 和 extended shadow
  继续 blocked。
- 如果 owner decision 只有 `hold`，normal paper-shadow 仍需要 owner 手工再签署
  `continue_normal_shadow` 才能恢复。

## 分阶段任务

|Task|Status|Acceptance criteria|
|---|---|---|
|TRADING-408 remaining-blocker-resolution-ledger|VALIDATING|已生成 `remaining_blocker_resolution_ledger_2026-06-17.json/md` 和 validation；ledger 覆盖 9 blockers / 10 recovery warnings，并保留 normal/extended/live boundary。|
|TRADING-409 restore-etf-signal-inputs-from-pipeline|VALIDATING|`signal-input-completeness-recovery` 与 `signal-input-recovery` 均输出 `SIGNAL_INPUTS_RESTORED_WITH_WARNINGS`，validation PASS；未伪造 feature/signal 文件。|
|TRADING-410 refresh-readiness-and-health-after-signal-restore|VALIDATING|`readiness-health-recovery` 输出 `MANUAL_REVIEW_REQUIRED`，validation PASS；normal observation 仍需 owner gate。|
|TRADING-411 materialize-cost-sensitivity-inputs|VALIDATING|`cost-sensitivity-metrics-materialization` 输出 `COST_INPUTS_AVAILABLE`，但 cost review 为 `NOT_MEANINGFUL_UNDER_COSTS`，promotion 继续 blocked。|
|TRADING-412 materialize-benchmark-baseline-inputs|VALIDATING|`benchmark-baseline-metrics-materialization` 输出 `BASELINE_METRICS_AVAILABLE`，benchmark control 为 `CANDIDATE_UNDERPERFORMS_BASELINES`，promotion 继续 blocked。|
|TRADING-413 append-owner-hold-decision-for-recovery|VALIDATING|已追加 `TRADING-413_owner_hold_2026-06-17` 到 append-only owner decision audit log，validation PASS；该 hold 不授权 resumption/promotion/extended/live。|
|TRADING-414 resolve-report-index-unwaived-warnings|VALIDATING|`report_index_warning_cleanup_2026-06-17` 输出 `remaining_unwaived_count=9`、`silent_waiver_count=0`；未用 silent waiver 隐藏真实 blocker。|
|TRADING-415 repair-requirements-freshness-metadata|VALIDATING|已修复 10 个 requirement docs 的 `最后更新` metadata；`docs validate-freshness` 为 PASS / issues=0。|
|TRADING-416 rerun-recovery-pack-after-blocker-fixes|VALIDATING|已重跑 recovery pack：`RECOVERY_GOVERNANCE_BLOCKED`、remaining blockers=9、remaining warnings=10、normal=false、extended/live forbidden=true。|
|TRADING-417 normal-paper-shadow-resumption-owner-gate|VALIDATING|已以 owner action `hold` 和 manual review completed 重跑 gate，结果 `RESUME_NORMAL_SHADOW_BLOCKED`；normal paper-shadow 不恢复。|
|TRADING-418 observation-clock-bootstrap|VALIDATING|`normal_paper_shadow_observation_clock_2026-06-17` 输出 `OBSERVATION_NOT_STARTED`，required trading days=20；extended shadow 继续 forbidden。|
|TRADING-419 post-recovery-governance-pack|VALIDATING|`post_recovery_governance_pack_2026-06-17` 输出 `POST_RECOVERY_BLOCKED`；summary 保留 latest owner action=`hold`、remaining blockers=9、warnings=10、normal=false、extended/live forbidden=true。|

## 当前实施计划

1. 新增只读 report modules：remaining blocker resolution ledger、report index warning cleanup、
   normal observation clock bootstrap、post-recovery governance pack。
2. 复用既有 TRADING-385 到 TRADING-390、TRADING-388、TRADING-400、TRADING-401 到 TRADING-407
   modules 作为 source artifacts，不复制核心 gate logic。
3. 对 TRADING-413 使用新的唯一 `decision_id` 追加 owner hold decision，保留既有
   `TRADING-392_owner_hold_2026-06-17` 记录。
4. 对 TRADING-414，若 warning 是真实 stale artifact 或 data artifact lag，只输出 cleanup
   evidence 和 owner/code/data action，不创建 waiver。
5. 对 TRADING-418，normal observation clock 必须从 normal resumption gate 的
   `normal_paper_shadow_may_resume=true` 后开始计数；当前 gate blocked 时输出
   `OBSERVATION_NOT_STARTED`。

## 进展记录

|Date|Status|Notes|
|---|---|---|
|2026-06-17|IN_PROGRESS|新增批次需求文档和 task-register 行；准备实现只读 ledger / cleanup / observation clock / post-recovery pack，并复用既有恢复链命令生成最新 artifacts。|
|2026-06-17|VALIDATING|实现并生成 TRADING-408/414/418/419 新 report modules、CLI、registry entries、Reader Brief-compatible payloads 和 focused tests；真实恢复链已按 409~417 顺序重跑，最终 post-recovery 状态为 `POST_RECOVERY_BLOCKED`，无 policy weakening、无 silent waiver、无 fake data、无 official target weights、无 broker/order/production mutation。验证已通过 focused pytest 60 passed、ruff、compileall、docs freshness、documentation contract、task-register consistency、latest Reader Brief quality、data quality gate；2026-06-17 Reader Brief/report quality gate 因缺少同日 `decision_snapshot_2026-06-17.json` 保持 fail-closed，未补造 snapshot。|
