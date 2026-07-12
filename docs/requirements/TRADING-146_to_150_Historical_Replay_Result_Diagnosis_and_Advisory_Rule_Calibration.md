# TRADING-146_to_150 Historical Replay Result Diagnosis and Advisory Rule Calibration

最后更新：2026-06-09

## 状态

- 任务登记：`docs/task_register.md` 中 `TRADING-146_to_150`
- 当前状态：VALIDATING
- 下一责任方：系统实现 + 项目 owner 人工复核
- 编号说明：`TRADING-146` 单项 PIT safety hardening 已归档；本阶段按 owner 附件要求保留 `TRADING-146_to_150` 作为聚合任务编号，避免复用单项 ID。
- 安全边界：no broker API、no automatic order、no production candidate、no owner auto approval、no official target weight mutation、no production config auto calibration、no automatic policy apply。

## 背景

`TRADING-141_to_145` 已完成 historical replay workflow，并生成 replay inventory、historical replay、backfilled outcome、historical paper simulation 和 replay performance review。但 latest artifacts 仍显示 `PARTIAL` / `PENDING` / `INSUFFICIENT_DATA`，这说明系统已经能重放链路，但还不能把结果转成可解释的 replay performance conclusion。

本阶段目标不是继续扩展平台，而是读取现有 artifacts，回答为什么 replay 仍然 pending、哪些 outcome 可修复、不同 variant 表现如何、advisory rule 是否只应提出人工审批建议，以及这些发现如何接回 forward tracking。

## 阶段拆解

1. `TRADING-146` Replay Coverage Diagnosis
   - 新增 `replay-diagnosis run/report` 和 `validate-replay-diagnosis`
   - 输出 `replay_coverage_breakdown.json`、`replay_pending_reason_summary.json`、`replay_artifact_health_matrix.jsonl` 和 diagnosis report
   - 解释 PIT_SAFE / PIT_WARNING / PIT_UNSAFE、AVAILABLE / PENDING / INSUFFICIENT_DATA、review pending 原因和是否可以进入 variant comparison

2. `TRADING-147` Backfilled Outcome Availability Repair
   - 新增 `backfill-repair run/report` 和 `validate-backfill-repair`
   - 只对已存在于 current price cache 且 outcome window 已到期的历史 window 做 calendar / date alignment / price cache lookup 修复
   - 不覆写原始 backfill artifact；无法修复时保持 `PENDING` 或 `INSUFFICIENT_DATA`

3. `TRADING-148` Variant Performance Comparison
   - 新增 `variant-comparison run/report` 和 `validate-variant-comparison`
   - 对 `no_trade`、`consensus_target`、`limited_adjustment`、`owner_decision`、`paper_action` 输出 window metrics、pairwise comparison 和 ranking summary
   - 样本不足时必须标记 `INSUFFICIENT_DATA`

4. `TRADING-149` Historical Replay Rule Calibration
   - 新增 `rule-calibration run/report` 和 `validate-rule-calibration`
   - 基于 comparison 生成 `advisory_rule_diagnostics.json`、`proposed_policy_adjustments.json` 和 `calibration_safety_checks.json`
   - 只输出 proposals，不自动修改 `position_advisory_v1.yaml`

5. `TRADING-150` Replay-to-Forward Tracking Bridge
   - 新增 `replay-forward-bridge run/report` 和 `validate-replay-forward-bridge`
   - 输出 `forward_tracking_focus.json`、`weekly_review_updates.json`、bridge report 和 Reader Brief section
   - 把 historical replay 发现转成未来 forward outcome 应重点观察的问题

## 设计原则

- Backfill repair 只能使用 replay `as_of` 之后、当前 price cache 中已经存在的历史价格计算 outcome；这些价格不得进入 replay decision input。
- 如果 window end date 晚于可用价格日期或当前 evaluation date，保持 `PENDING`。
- 如果价格缺失、权重为空、价格无效或 return path 不完整，保持 `INSUFFICIENT_DATA`。
- Rule calibration 必须 `auto_apply=false`、`requires_owner_approval=true`、`production_effect=none`、`broker_action_allowed=false`。
- Reader Brief 和 report registry 只读 latest artifacts；缺失时显示 missing，不补跑上游。

## 验收标准

- 五个新增 run/report/validate CLI 均可运行。
- Diagnosis 能清晰分类 pending reasons。
- Backfill repair 输出 availability delta，且每个 repair action 的 `future_data_used_in_decision=false`。
- Variant comparison 输出 pairwise comparison、rank summary，并在样本不足时 fail closed。
- Rule calibration 只输出 proposals，`auto_apply=false`、`owner_approval_required=true`。
- Replay-to-forward bridge 生成 Reader Brief section，不触发 production、promotion、broker 或 policy mutation。
- README、operations runbook、system flow、report registry、artifact catalog、task register、requirements 和 Reader Brief 同步。
- Focused tests、ruff、compileall、git diff check、dynamic-v3 root validation 和 dynamic-v3 family artifact validation 通过。

## 进展记录

- 2026-07-12：ARCH-004G2.4AZ对Rule Calibration执行source-derived/evidence-eligibility hardening并迁移CLI。Run在output前要求Variant Comparison技术校验PASS和时间有序，冻结完整comparison及reviewed calibration/target policies。只有source status=PASS且confidence=PILOT_ELIGIBLE才产生manual-only方向性proposal；当前单event fixture正确输出INSUFFICIENT_DATA、proposal_count=0和`require_more_forward_data` non-policy action。Validator重算全部views并检测source/snapshot/policy/output tamper；不自动应用proposal，无policy/config/portfolio/production/broker effect。
- 2026-07-12：ARCH-004G2.4AY对Variant Comparison执行source-derived/same-cohort hardening并迁移CLI。Comparison要求full-PASS backfill及optional same-lineage repair，冻结source/canonical rows/reviewed policy；missing保持null，pairwise使用paired distinct events，ranking固定5d same-event common cohort并执行3 event/10 window/3 pair pilot floor。当前单event fixture正确输出best=MISSING/confidence=INSUFFICIENT_DATA。Validator重算全部views并检测source/snapshot/policy/output tamper；不运行calibration，无policy/portfolio/production/broker effect。
- 2026-07-12：ARCH-004G2.4AX完成Backfill Repair source-derived hardening与CLI迁移。Repair现在在任何output前要求Backfilled Outcome、Replay Diagnosis、Historical Replay full PASS、同链lineage/time ordering及cached DQ PASS，冻结三源bundle、reviewed cost和cutoff price rows；只对原PENDING/INSUFFICIENT按AT fixed-share/cost/null语义重算，原AVAILABLE保持immutable，availability delta显式使用`event_variant_window`单位。Validator重验live source/DQ并重算actions/rows/delta/manifest/Markdown，source/snapshot/output tamper FAIL；不覆写原backfill、不自动运行comparison/calibration，无policy/portfolio/production/broker effect。
- 2026-07-12：G2.4AW完成Replay Diagnosis hardening与CLI迁移；focused 83、architecture 232、contract 203全部PASS。当前fixture虽有部分AVAILABLE windows，但仅1个独立replay event且simulation不足，因此comparison readiness保持false；诊断不把单window误作方向性证据。同步修复Backfill Repair调用AT outcome helper时遗漏cost_rate的回归。G2.4继续，尚未触发ARCH-005 handoff。
- 2026-07-12：ARCH-004G2.4AW对Replay Diagnosis执行source-derived hardening并迁移CLI。Diagnosis现在要求五类source full PASS和完整lineage/time ordering，冻结全部files/content/checksums；coverage、pending reasons与health matrix显式区分event/window/state/chain单位，无reason不再注入blocking unknown，comparison readiness继承AV reviewed evidence gate。Validator重算全部views并检测source/snapshot/output tamper；不运行repair/comparison/calibration，无policy/portfolio/production/broker effect。
- 2026-06-09：新增任务登记和需求文档，进入实现阶段。当前 implementation plan 是复用 `dynamic_v3_historical_replay.py` 现有 artifact helper 和 safety contract，新增 diagnosis/repair/comparison/calibration/bridge artifact，不覆写 TRADING-141_to_145 原始 outputs。
- 2026-06-09：完成 diagnosis / repair / comparison / calibration / bridge 实现、CLI、report registry、Reader Brief、artifact catalog、system flow、operations runbook、README 和 focused tests。真实链路基于 inventory `4cd60c43a04b2288`、replay `0f407af36295acf9`、backfill `b9bc15e81c38dade`、sim `daf5c9deef4601a9`、review `9d0ee2c74043c904` 生成 diagnosis `29bbc2d31984cd07`、repair `3b75fb568c6c1f9b`、comparison `5f9206ca138fe754`、calibration `61055d6b9750da78`、bridge `2a8da46099be3c69`；状态转入 VALIDATING，等待 owner 复核 replay conclusion 和 calibration proposal。当前已知限制：repaired outcome 只覆盖 10 个 1d windows，5/10/20d limited_adjustment vs no_trade 仍为 `INSUFFICIENT_DATA`，bridge 因此要求继续积累 forward data。
