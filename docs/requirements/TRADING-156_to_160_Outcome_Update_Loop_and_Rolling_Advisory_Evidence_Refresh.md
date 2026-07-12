# TRADING-156 to TRADING-160: Outcome Update Loop and Rolling Advisory Evidence Refresh

最后更新：2026-06-10

## 背景

TRADING-151_to_155 已经能扫描 forward outcome due windows，并在真实链路中识别
`update_ready_count=1`。当前限制是系统只生成了 `update_ready_list.json`，尚未提供
人工可读 review pack、安全 update audit、下游 evidence refresh、跨轮 trend 和每周
decision pack。

本阶段目标是在不触发 broker、不进入 production、不自动改 policy 的前提下，把已到期
且通过复核的 outcome windows 从 `PENDING` 安全推进到 `AVAILABLE`，并刷新
outcome dashboard、focused evaluation、consensus risk、weekly review 和 Reader Brief。

## 子任务

|ID|范围|状态|验收标准|
|---|---|---|---|
|TRADING-156|Outcome Update Ready Review Pack|VALIDATING|`outcome-update-review run/report` 和 `validate-outcome-update-review` 可运行；所有 due window 都有 `review_status`；`future_data_used_in_decision=false`；报告回答 ready count、price missing、future leakage、expected delta、affected artifacts 和是否建议执行 safe update。|
|TRADING-157|Safe Outcome Update Runner|VALIDATING|`outcome-update run/report` 和 `validate-outcome-update` 可运行；只更新 `READY_TO_UPDATE` windows；生成 updated/skipped audit 和 before/after status delta；不更新 NOT_DUE、PRICE_MISSING 或 BLOCKED windows。canonical `2026-06-14` due artifact 仍需到该日期后复跑，当前 future-as-of 防护正确阻止真实 mutation。|
|TRADING-158|Rolling Evidence Refresh|VALIDATING|`rolling-evidence-refresh run/report` 和 `validate-rolling-evidence-refresh` 可运行；只接受validated、COMMITTED且未消费的显式Outcome Update；以PREPARED/COMMITTED/ROLLED_BACK事务刷新 outcome dashboard、limited-vs-notrade、consensus-risk、owner-attribution、shadow-aging、weekly-advisory-review 和 Reader Brief section；所有实际下游validator必须PASS，输出full source snapshot与transaction-scoped evidence delta。|
|TRADING-159|Advisory Evidence Trend Report|VALIDATING|`evidence-trend run/report` 和 `validate-evidence-trend` 可运行；只消费validated COMMITTED refresh，冻结full bundles/policy并从post-Dashboard full state构建null-preserving timeseries；历史不足时标记 `INSUFFICIENT_HISTORY`。|
|TRADING-160|Weekly Forward Outcome Decision Pack|VALIDATING|`forward-outcome-decision run/report` 和 `validate-forward-outcome-decision` 可运行；样本不足时 `rule_calibration_readiness=NOT_READY`，不得建议自动调规则；`broker_action_allowed=false`、`production_effect=none`。|

## CLI

```bash
aits etf dynamic-v3-rescue outcome-update-review run --due-id <due_id>
aits etf dynamic-v3-rescue outcome-update-review report --latest
aits etf dynamic-v3-rescue validate-outcome-update-review --review-id <update_review_id>

aits etf dynamic-v3-rescue outcome-update run --update-review-id <update_review_id>
aits etf dynamic-v3-rescue outcome-update report --latest
aits etf dynamic-v3-rescue validate-outcome-update --update-id <outcome_update_id>

aits etf dynamic-v3-rescue rolling-evidence-refresh run --outcome-update-id <outcome_update_id>
aits etf dynamic-v3-rescue rolling-evidence-refresh report --latest
aits etf dynamic-v3-rescue validate-rolling-evidence-refresh --refresh-id <refresh_id>

aits etf dynamic-v3-rescue evidence-trend run
aits etf dynamic-v3-rescue evidence-trend report --latest
aits etf dynamic-v3-rescue validate-evidence-trend --trend-id <trend_id>

aits etf dynamic-v3-rescue forward-outcome-decision run --week-ending YYYY-MM-DD
aits etf dynamic-v3-rescue forward-outcome-decision report --latest
aits etf dynamic-v3-rescue validate-forward-outcome-decision --decision-id <decision_id>
```

## Artifacts

```text
reports/etf_portfolio/dynamic_v3_rescue/outcome_update_review/<update_review_id>/
reports/etf_portfolio/dynamic_v3_rescue/outcome_update/<outcome_update_id>/
reports/etf_portfolio/dynamic_v3_rescue/rolling_evidence_refresh/<refresh_id>/
reports/etf_portfolio/dynamic_v3_rescue/evidence_trend/<trend_id>/
reports/etf_portfolio/dynamic_v3_rescue/forward_outcome_decision/<decision_id>/
```

## 设计决策

1. update-ready review 需要人工复核，因为 due scan 只说明价格窗口已到期；执行更新前还必须明确价格是否完整、future data 是否仅用于 outcome measurement、更新会影响哪些下游报告，以及是否存在需要 owner 判断的异常。
2. outcome update 只能通过 review pack 的 `READY_TO_UPDATE` 行执行。NOT_DUE、PRICE_MISSING、NEEDS_REVIEW 或 BLOCKED 行必须进入 skipped audit，不能由 runner 静默处理。
3. future price 只能用于到期后 outcome measurement，不得作为原始 daily advisory decision input。review 和 update artifacts 都必须写明 `future_data_used_in_decision=false`。
4. rolling evidence refresh 只刷新 evidence artifacts，不修改 `position_advisory_v1.yaml`、official target weights、baseline/production state 或 real portfolio。它必须先验证显式Outcome Update并限制single-use；下游按一个可回滚事务编排，任一步骤失败都删除本次新增artifact、恢复六类latest pointer并记录`ROLLED_BACK`。成功时冻结update、validated baseline、post-refresh full bundles与validation evidence；Reader Brief只生成section，不能声称已更新全局Reader Brief。
5. evidence trend 只比较validated COMMITTED多轮 refresh 的可比full-Dashboard state。ROLLED_BACK/legacy/future必须显式排除，invalid COMMITTED/PREPARED阻断；单轮或历史不足时必须输出 `INSUFFICIENT_HISTORY`，不能把早期正收益写成规则调整依据。所有trend/signal/action边界来自reviewed policy，missing metric保持null。
6. weekly forward outcome decision pack 是人工下一步建议包；样本不足或 risk 仍不足时只能建议 `continue_tracking` / `wait_for_more_outcomes` / `do_not_change_policy`。
7. `CASH` 在 outcome return path 中按零收益处理，不要求 price cache 提供 `CASH` 行；这与 due scan 对 `CASH` 不要求价格的规则一致，避免把现金权重误判为 `INSUFFICIENT_DATA`。

## 数据质量与安全边界

- 依赖 cached market data 的 update 仍通过 `update_advisory_outcome` 的数据质量门禁执行。
- 所有 artifacts 固定 `production_effect=none`、`broker_action_allowed=false`、
  `broker_action_taken=false`、`production_candidate_generated=false`、
  `manual_review_required=true`。
- 本阶段不接入 broker API、不自动下单、不生成 automatic production candidate、不自动 owner approval、
  不修改 official target weights、baseline/production state、real portfolio 或 advisory policy config。

## 验收链路

```bash
aits etf dynamic-v3-rescue outcome-due scan --as-of 2026-06-14
aits etf dynamic-v3-rescue outcome-update-review run --due-id <due_id>
aits etf dynamic-v3-rescue outcome-update run --update-review-id <update_review_id>
aits etf dynamic-v3-rescue rolling-evidence-refresh run --outcome-update-id <outcome_update_id>
aits etf dynamic-v3-rescue evidence-trend run
aits etf dynamic-v3-rescue forward-outcome-decision run --week-ending 2026-06-14
```

## 进展记录

- 2026-07-12：ARCH-004G2.4BJ 对 TRADING-159 Evidence Trend 完成实现与验证。3 callback迁canonical；只选择cutoff内unique validated COMMITTED refresh，ROLLED_BACK/legacy/future以full bundle和可重验reason排除，PREPARED/invalid COMMITTED阻断。Timeseries从post-Dashboard full state重算，不比较不同update selected cohort，missing保持null；history/confidence/growth/return/risk/action及冲突precedence由reviewed policy治理。Validator重验selected/excluded live source与policy并重算全部views；focused 357、architecture 245、contract 203 PASS；不运行上游、不改policy/config/portfolio/production/broker。
- 2026-07-12：ARCH-004G2.4BJ 对 TRADING-159 Evidence Trend 完成contract freeze并进入实现：只选择cutoff内unique validated COMMITTED refresh，ROLLED_BACK/legacy/future显式排除，PREPARED/invalid COMMITTED阻断；full bundles/validation/policy进入snapshot。Timeseries从post-Dashboard full state重算，不再比较不同update selected cohort，limited/consensus missing保持null；history/confidence/growth/return/risk/action由reviewed policy治理。Validator重验live refresh/policy并重算全部views；不运行上游、不改policy/config/portfolio/production/broker。
- 2026-07-12：ARCH-004G2.4BI 对 TRADING-158 Rolling Evidence Refresh 完成contract hardening与并行验证：3个CLI callbacks迁至canonical owner；run在任何下游前要求explicit Outcome Update content-derived PASS、id/time/COMMITTED/live-source一致与single-use。六下游以PREPARED transaction编排，失败删除本次新增artifact并恢复latest pointers后记录ROLLED_BACK；成功要求所有实际下游content-derived validator PASS（无shadow shortlist显式SKIPPED），且post-committed update的Dashboard明确不复用已消费、已因live outcome变化而失效的Outcome Due snapshot。Artifact冻结update、validated baseline、post bundles和validation evidence，forward delta限update selected cohort，其他before只来自同transaction捕获的validated baseline；Reader Brief只生成section。Validator重验live update/downstream并重算inventory/delta/manifest/Markdown/Reader Brief/transaction；累计focused 349、architecture 244、contract 203 PASS；不改policy/config/weights/portfolio/production/broker。
- 2026-07-12：ARCH-004G2.4BH 对 TRADING-157 Outcome Update 完成transaction contract hardening与并行验证：3个CLI callbacks迁至canonical owner；run在mutation前要求explicit review PASS、time/identity/live pre-state/single-use，并对全ready batch在isolated copy完成DQ/price/update/post-validator预演。实际批次以PREPARED transaction和rollback backups开始，异常全量恢复并记录ROLLED_BACK，全部post validators PASS才COMMITTED；snapshot冻结review/pre/post bundles，delta限selected cohort。Validator重算review、pre→post append event、updated/skipped/delta/manifest/Markdown并核对live post source；focused 340、architecture 243、contract 203 PASS；不自动refresh、不改policy/portfolio/production/broker。
- 2026-07-12：ARCH-004G2.4BG 对 TRADING-156 Outcome Update Review 完成contract hardening与并行验证：CLI callbacks迁至canonical interface owner；run在任何output前验证显式Outcome Due并冻结完整bundle，强制due id/cutoff/唯一`outcome_id×window_days`；READY只由`DUE + PENDING + can_update + cutoff-visible price`推导，no-future-data由日期边界证明。Validator重验live source并重算matrix/safety/impact/status/manifest/Markdown；合法empty为`INSUFFICIENT_DATA`，duplicate或tamper FAIL。累计focused 333、architecture 242、contract 203 PASS；范围仍为owner review input，不执行outcome update、data refresh或任何policy/portfolio/production/broker变更。
- 2026-06-10：任务登记与需求文档创建，进入实现阶段。下一步新增核心模块扩展、CLI、validators、focused tests、report registry、artifact catalog、system flow、operations runbook、README 和 Reader Brief integration。
- 2026-06-10：baseline 实现完成并进入 VALIDATING。新增 `outcome-update-review`、`outcome-update`、`rolling-evidence-refresh`、`evidence-trend`、`forward-outcome-decision` CLI/report/validator、registry/catalog/docs/Reader Brief integration 和 focused tests；修复 `CASH` 无价格行时 outcome return path 应按零收益处理的问题。
- 2026-06-10：真实链路使用 existing due artifact `9d344a0a7c24b676`（`as_of=2026-06-14`）生成 review `3afeeb97f9be39e9`，ready=1、blocked=0、future_data=false；safe update `aaa8a4781e74934d` 因当前日期仍早于 2026-06-14，被底层 future-as-of 防护保持 updated=0、skipped=4、forward_available 0->0、forward_pending 4->4。该结果不是 workaround；解除条件是在 2026-06-14 不再是未来日期且 cached data quality gate 通过后复跑。
- 2026-06-10：no-update 状态下验证下游命令可运行：rolling refresh `e3fda052b073d586` material_change=false，trend `d3f74685ebf39711` 为 `INSUFFICIENT_HISTORY` / `NO_CHANGE` / `continue_tracking`，forward decision `f2a5bd5b43036fa6` 为 `wait_for_more_outcomes`、`NOT_READY`、next_due_scan=`2026-06-21`；新增 artifact validators、root dynamic-v3 validation、family artifact validation、focused pytest、ruff、compileall 和 git diff check 均通过；full pytest 运行 15 分钟超时，无失败明细。
