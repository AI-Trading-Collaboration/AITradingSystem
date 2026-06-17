# TRADING-394 Monthly Review Recovery Rerun

最后更新：2026-06-17

## 背景

TRADING-365 建立了 research monthly review pack。TRADING-385~393 已恢复 signal
inputs、重跑 readiness/health、补齐 cost/benchmark evidence、生成 recovery evidence pack、
记录 owner hold，并实例化 owner review template v2。现在需要重新运行 monthly review pack，
确认 blocker 是否真实清除，或继续 fail closed。

## 范围

- 使用 `outputs/reports/report_index_2026-06-17.json` 作为同日 report discovery input。
- 运行 `aits reports research-monthly-review-pack --as-of 2026-06-17`。
- 运行 `aits reports validate-research-monthly-review-pack --latest`。
- 汇总 monthly status、major blockers、warnings、safety、data governance、owner decision input。
- 重建 report index 和 Reader Brief。

## 安全边界

- 不运行上游 evidence generation。
- 不刷新 market/cache data。
- 不补造 missing artifacts。
- 不新增 waiver，不弱化 blocker。
- 不修改 strategy output、candidate state、paper-shadow state 或 production state。
- 不批准 normal shadow、promotion、extended shadow 或 live trading。
- 不生成 official target weights，不触发 broker action 或 order ticket。

## 验收标准

- Monthly review rerun artifact 和 validation artifact 生成。
- 若 cost/benchmark/data governance 等 blocker 仍存在，status 必须保持
  `MONTHLY_REVIEW_BLOCKED`。
- Owner decision input 不再是 `AUDIT_LOG_EMPTY`；应读取 TRADING-392 的 hold evidence。
- Validation `PASS` 或 `PASS_WITH_WARNINGS` 且 failed checks=0。
- Focused tests、ruff、compileall、documentation contract、report index、Reader Brief quality
  和 git diff check 通过。

## 进展记录

|日期|状态|记录|
|---|---|---|
|2026-06-17|IN_PROGRESS|新增 requirements 和 task-register 行；准备用 latest report index 重跑 monthly review pack。|
|2026-06-17|DONE|更新 monthly source classification，使 cost sensitivity `NOT_MEANINGFUL_*` 和 benchmark baseline `*UNDERPERFORMS*` 作为 monthly blockers；新增 focused regression。真实 rerun `outputs/reports/research_monthly_review_pack_2026-06-17.json/md` 输出 `MONTHLY_REVIEW_BLOCKED`、major_blockers=2、major_warnings=8、owner_decision=`AUDIT_LOG_PASS`、data_governance=`PASS_WITH_WARNINGS`。Validation `outputs/reports/research_monthly_review_pack_validation_2026-06-17.json/md` 输出 `PASS_WITH_WARNINGS`、failed=0、source_blockers=2、source_warnings=8。该 rerun 只读聚合既有 evidence，不授权 normal shadow、promotion、extended shadow、official target、broker/order、live trading 或 production mutation。|
