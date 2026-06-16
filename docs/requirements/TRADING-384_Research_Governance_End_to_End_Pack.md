# TRADING-384 Research Governance End-to-End Pack

最后更新：2026-06-16

状态：DONE

## 背景

TRADING-362 到 TRADING-382 已建立 task/register、waiver、Reader Brief、safety、owner review、monthly review、promotion board、extended shadow、roadmap 和 lineage governance artifacts。Owner 需要一个最终 end-to-end pack 证明这些治理面是否一致、阻塞项在哪里、下一步应由谁人工复核。

## 范围

- 新增 `aits reports research-governance-end-to-end-pack --as-of YYYY-MM-DD`。
- 新增 `aits reports validate-research-governance-end-to-end-pack --latest`。
- 只读读取同日 report index 和 latest governance artifacts。
- 聚合 task-register consistency、registry waiver inventory、Reader Brief consistency、research safety boundary audit、production boundary static scan、owner review template、owner decision audit log、monthly review pack、paper-shadow promotion board、extended shadow protocol、research roadmap dashboard 和 artifact lineage graph。
- 输出 overall governance status、top blockers、next actions、JSON / Markdown pack、validation artifact 和 Reader Brief section。

## 状态规则

- `GOVERNANCE_BLOCKED`：required source missing、validation fail、safety blocked、production boundary blocking、lineage/task/register consistency fail、monthly review blocked、promotion board hold/reject/return, extended shadow blocked, roadmap blocked, or report index unwaived blocker.
- `GOVERNANCE_MANUAL_REVIEW_REQUIRED`：无 fail-closed blocker，但存在 owner decision empty、warnings、manual review required 或 unresolved non-blocking governance warning。
- `GOVERNANCE_HEALTHY_WITH_WARNINGS`：无 manual-review blocker，但存在 non-blocking warning。
- `GOVERNANCE_HEALTHY`：所有 required sources available，validation pass，且无 blocker/warning。

## 安全边界

- `production_effect=none`。
- 不运行上游 report，不刷新数据，不补造 missing / stale artifacts。
- 不修改 task register、strategy logic、candidate state、paper-shadow state 或 production state。
- 不生成 official target weights、order ticket 或 broker action。
- Owner decision 缺失、paper-shadow blocker、stale data 或 safety warning 必须可见披露。

## 验收标准

- CLI 可生成 `outputs/reports/research_governance_end_to_end_pack_YYYY-MM-DD.json/md`。
- Validation CLI 可生成 `outputs/reports/research_governance_end_to_end_pack_validation_YYYY-MM-DD.json/md`。
- Pack 包含 required source report summary、overall governance status、top blockers、next actions、Reader Brief section 和 safety boundary。
- Validation 对缺 required source、非法 status、unsafe production effect、缺 Reader Brief section 或 state/broker/order mutation flag fail closed。
- Reader Brief 展示 end-to-end status、validation status、source count、blocking count、warning count、top blocker、next action 和 production boundary。
- `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md`、`docs/operations/operations_runbook.md`、`README.md` 和 task register 同步。
- Focused tests 覆盖 aggregation、status classification、validation fail closed、CLI output 和 Reader Brief summary。

## 进展记录

- 2026-06-16：任务新增并进入 `IN_PROGRESS`。本阶段只聚合既有治理 artifacts，不运行上游、不刷新数据、不补造 owner decision / evidence / metrics。
- 2026-06-16：实现完成并归档为 `DONE`。新增 report module、CLI、validation、Reader Brief section、registry/catalog/docs/runbook/system flow 和 focused tests。真实 artifact `outputs/reports/research_governance_end_to_end_pack_2026-06-16.json/md` 输出 `GOVERNANCE_BLOCKED`、source_reports=12、available_sources=12、validation_pass=5、validation_warnings=7、validation_fail=0、blocking_items=4、warning_items=8、manual_review_items=3、top_blocker=`research_monthly_review_pack`；top blockers 为 `research_monthly_review_pack`、`paper_shadow_promotion_board`、`extended_shadow_protocol`、`research_roadmap_dashboard`。Validation 输出 `PASS_WITH_WARNINGS`、checks=7、failed=0、source_blockers=4、source_warnings=8。Report index 输出 `PASS_WITH_EXPLICIT_WAIVERS`、reports=406、missing=20、stale=10、waived=30、unwaived=0；documentation contract PASS；Reader Brief latest 使用 2026-06-15 decision snapshot + 2026-06-16 report index 展示 end-to-end pack section，Reader Brief quality OK / failed=0；focused pytest 41 passed，Ruff PASS，compileall PASS。精确 2026-06-16 Reader Brief generation remains limited by missing local `data/processed/decision_snapshots/decision_snapshot_2026-06-16.json`; no snapshot was fabricated. 该结果只披露既有 governance blockers，不运行上游、不刷新数据、不补造 owner decision / evidence / metrics，不修改 task / strategy / candidate / paper-shadow / production state。
