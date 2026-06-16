# TRADING-365 Research Monthly Review Pack

最后更新：2026-06-16

状态：DONE

## 背景

TRADING-361 已建立 candidate research / paper-shadow artifact lineage graph，TRADING-363、TRADING-364、TRADING-378 已补齐 safety boundary、owner review template 和 owner decision audit log。附件要求继续生成一个适合项目 owner 手工月度复核的 research governance pack，把候选证据、paper-shadow 状态、数据治理、owner decisions 和安全边界放在同一个只读报告中。

## 范围

- 新增 `aits reports research-monthly-review-pack --as-of YYYY-MM-DD`。
- 新增 `aits reports validate-research-monthly-review-pack --latest`。
- 只读读取同日 report index 和既有 source artifacts，不运行上游命令、不刷新数据、不补造缺失 artifact。
- 聚合 candidate ledger、paper-shadow weekly review、evidence staleness monitor、shadow continuation readiness、research safety boundary audit、owner decision audit log、cost sensitivity review、benchmark baseline control、artifact lineage graph，以及 data refresh / fallback / cache catalog / PIT manifest / signal input / paper-shadow health 等 data governance inputs。
- 输出 JSON / Markdown 月度报告、validation JSON / Markdown 和 Reader Brief monthly section。

## 安全边界

- `production_effect=none`。
- 不生成 official target weights。
- 不修改 strategy outputs、candidate state、paper-shadow account/state、production state、config、cache 或历史 artifacts。
- 不触发 broker、order ticket、portfolio mutation、automatic owner approval 或 promotion approval。
- 缺失、stale、blocked、insufficient evidence 和 validation warning 必须显式披露；不得把 missing source 解释成通过。

## 报告结构

- active candidates。
- rejected candidates。
- paper-shadow candidates。
- candidates needing evidence。
- major blockers。
- major warnings。
- safety audit status。
- data governance status。
- owner decision status。
- monthly Reader Brief：Summary、Key Result、Blocking Issues、Warnings、Safety Boundary、Next Action。
- source aggregation table：每个 source family 的 availability、status、artifact path、candidate id、blocking/warning flag 和 next action。

## 验收标准

- CLI 可生成 `outputs/reports/research_monthly_review_pack_YYYY-MM-DD.json/md`。
- Validation CLI 可生成 `outputs/reports/research_monthly_review_pack_validation_YYYY-MM-DD.json/md`，对 unsafe production effect、缺核心 section、缺 source family 或 blocking safety/data issue fail closed。
- Reader Brief 只读展示 latest monthly pack status、candidate counts、blocker/warning counts、safety/data governance status 和 detail link。
- `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md`、`docs/operations/operations_runbook.md` 和 `README.md` 同步。
- Focused tests 覆盖 payload build、validation fail closed、CLI output 和 Reader Brief summary。
- 通过 Ruff、compileall、focused pytest、documentation/report contract、report index、Reader Brief/quality 和相关 safety governance checks。

## 进展记录

- 2026-06-16：任务新增并进入 `IN_PROGRESS`。实现策略确定为只读聚合 report index latest pointers，并在同目录可定位时读取 source report JSON；validation 指针只能证明 source family 可见，不可反推候选投资结论。
- 2026-06-16：实现完成并归档为 `DONE`。真实 pack `outputs/reports/research_monthly_review_pack_2026-06-16.json/md` 输出 `MONTHLY_REVIEW_BLOCKED`，source families=15/15，active=1，paper-shadow=1，needs-evidence=1，major blockers=6，major warnings=6，safety=`SAFETY_PASS_WITH_WARNINGS`，data governance=`BLOCKED`，owner decision=`AUDIT_LOG_EMPTY`。Validation `outputs/reports/research_monthly_review_pack_validation_2026-06-16.json/md` 输出 `PASS_WITH_WARNINGS`、checks=9、failed=0。阻断项来自既有 signal/data/cost/benchmark/readiness/health artifacts，未补造数据、未 append owner decision、未自动 reject 或 promote candidate。
