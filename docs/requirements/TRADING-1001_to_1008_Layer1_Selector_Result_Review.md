# TRADING-1001 to 1008 Layer-1 Selector Result Review

最后更新：2026-06-25

## 背景

TRADING-986～1000 已完成 Layer-1 simple-rule selector research-only historical study。
当前真实运行结论为：

```text
master status = LAYER1_SELECTOR_RESEARCH_ONLY
data_quality = PASS_WITH_WARNINGS
actual research interval = 2022-12-01 ~ 2026-06-23
watchlist = still requires owner review
paper_shadow_allowed = false
production_allowed = false
broker_action = none
```

本批任务不新增 selector，也不扩大策略搜索；目标是审查已有真实结果、披露历史覆盖缺口和 recent-regime 风险，并判断是否只允许 research-only forward-aging dry-run / watchlist review。

## 阶段拆解

|任务|阶段|当前状态|验收标准|
|---|---|---|---|
|TRADING-1001|real result summary|VALIDATING|新增 `layer1_selector_real_result_summary.json/md`，汇总 986～1000 真实结果，输出 top selector、成本后指标、相对 always_equal_risk / always_100_qqq、regret、period/sensitivity/watchlist/owner 状态，并回答 selector 排名和成本后优势。|
|TRADING-1002|history coverage gap audit|VALIDATING|新增 `layer1_selector_history_coverage_gap_audit.json/md`，解释实际区间为何为 2022-12-01 起，检查 Layer-2 fact panel、feature panel、selector inputs、forward outcome maturity、120d window、QQQ/SGOV/TQQQ coverage、policy hash 和能否回补到 2012。|
|TRADING-1003|recent regime risk disclosure|VALIDATING|新增 `layer1_selector_recent_regime_risk_disclosure.json/md`，披露 2023 recovery、2024 AI rally、2025-to-latest、高利率 SGOV carry 和缺失 2018Q4 / 2020 COVID / 完整熊市的风险。|
|TRADING-1004|owner watchlist review|VALIDATING|新增 `layer1_selector_owner_watchlist_review.json/md` 和 `docs/research/layer1_selector_owner_watchlist_review.md`，基于 986～1003 判断是否允许 research-only forward-aging watchlist candidate，保持 manual review required。|
|TRADING-1005|forward-aging dry-run|VALIDATING|新增 `layer1_selector_forward_aging_dry_run.json/md`，只 dry-run 最新 decision date 的 selector output 和 research-only target weights，不写正式 observation、不改 production config、不接 broker。|
|TRADING-1006|watchlist blocker report|VALIDATING|新增 `layer1_selector_watchlist_blocker_report.json/md`，明确 watchlist allowed/candidate、blockers/warnings、history backfill 和最小 forward observations；paper-shadow/production/broker 继续 blocked。|
|TRADING-1007|Reader Brief preview|VALIDATING|新增 `layer1_selector_reader_brief_preview.json/md`，只预览 research-only status、candidate/watchlist/coverage warnings 和 safety boundary，禁止交易建议措辞。|
|TRADING-1008|result review master|VALIDATING|新增 `layer1_selector_result_review_master.json/md` 和 `docs/research/layer1_selector_result_review_master.md`，汇总 1001～1007，回答是否存在成本后 edge、是否 recent-regime-only、是否需要回补、是否只允许 dry-run 和下一阶段最小任务。|

## 新增 CLI

```bash
aits research strategies layer1-selector-real-result-summary
aits research strategies layer1-selector-history-coverage-gap-audit
aits research strategies layer1-selector-recent-regime-risk-disclosure
aits research strategies layer1-selector-owner-watchlist-review
aits research strategies layer1-selector-forward-aging-dry-run
aits research strategies layer1-selector-watchlist-blocker-report
aits research strategies layer1-selector-reader-brief-preview
aits research strategies layer1-selector-result-review-master
```

## Guardrails

- Formal selectable components 仍只允许 `equal_risk_qqq_sgov` 与 `100_qqq`。
- `qqq_50_sgov_50` / `qqq_60_sgov_40` 只能作为 reference-only。
- QQQ-plus growth、TQQQ-heavy、tail-risk fallback、LEAPS、Wheel 和 Options 继续排除。
- 所有 cached-data dependent 命令必须走同源 `validate-data` 质量门禁路径，并在输出中披露 data quality。
- 本批不自动接入正式 Reader Brief；只生成安全 preview。
- 本批不得写正式 forward-aging observation，不得进入 paper-shadow、production 或 broker。
- 所有 outputs 固定 `paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`、`manual_review_required=true`。

## 进展记录

- 2026-06-25: 新增需求文档并进入 `IN_PROGRESS`。实现范围为 TRADING-1001～1008 Layer-1 simple-rule selector 结果审查、历史覆盖审计、recent-regime 风险披露、owner watchlist review、dry-run、blocker report、Reader Brief preview 和 master review；不新增 selector、不进入 paper-shadow、production 或 broker。
- 2026-06-25: 实现完成并转入 `VALIDATING`。真实 master CLI 输出 `LAYER1_SELECTOR_DRY_RUN_ONLY`，top selector=`trend_200dma_selector`，cost-after edge exists vs `always_equal_risk` 但不优于 `always_100_qqq`；history coverage=`RECENT_REGIME_ONLY_WARNING`，recent regime risk=`RECENT_REGIME_RISK_MATERIAL`，watchlist recommendation=`KEEP_SELECTOR_RESEARCH_ONLY`，blocker=`TOO_MUCH_TURNOVER`，dry-run=`LAYER1_SELECTOR_FORWARD_DRY_RUN_WARN` / `observation_written=false`；所有 safety fields 仍 false/none。
- 2026-06-25: 验证通过并保持 `VALIDATING`，等待 owner 复核是否接受 turnover blocker、recent-regime-only warning 和 history backfill requirement。验证包括 Ruff、compileall、focused Layer-1 / Layer-2 pytest、task/register/report/documentation pytest、真实 master CLI 和 `git diff --check`。
