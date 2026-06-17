# TRADING-400 Research Governance Recovery Pack

## 背景

TRADING-385 到 TRADING-399 已逐项恢复或重跑 signal inputs、readiness/health、
normal paper-shadow gate、cost/benchmark evidence、owner hold、monthly review、
promotion board、observation clock、extended shadow protocol、roadmap dashboard 和
decision snapshot lifecycle policy。需要最终 recovery governance pack 汇总所有最新
结果，明确 remaining blockers/warnings、下一 owner action，以及 normal paper-shadow、
extended shadow、live trading 的边界。

## 目标

- 只读收集 latest outputs：
  - signal input restoration
  - signal completeness recovery
  - readiness and health recovery
  - cost sensitivity metrics
  - benchmark baseline metrics
  - recovery evidence pack
  - owner hold decision
  - monthly review
  - promotion board
  - observation clock
  - extended shadow protocol
  - roadmap dashboard
  - decision snapshot lifecycle
- 生成 final JSON/Markdown、validation artifact 和 Reader Brief section。
- 输出 recovery governance status：
  - `RECOVERY_GOVERNANCE_HEALTHY`
  - `RECOVERY_GOVERNANCE_HEALTHY_WITH_WARNINGS`
  - `RECOVERY_GOVERNANCE_MANUAL_REVIEW_REQUIRED`
  - `RECOVERY_GOVERNANCE_BLOCKED`
- 明确：
  - remaining blockers
  - remaining warnings
  - next owner action
  - whether normal paper-shadow may resume
  - whether extended shadow remains forbidden
  - whether live trading remains forbidden

## 边界

- Pack 只读读取 report index 和既有 artifacts。
- 不运行上游 recovery commands，不刷新数据，不补造缺失 artifact。
- 不 append owner decision，不修改 task/candidate/paper-shadow/production state。
- 不生成 official target weights、order ticket 或 broker action。
- 即使 recovery evidence 好转，也不得批准 live trading；normal paper-shadow 最多只能在
  blocker 全清、owner review 明确允许时标记为可人工复核恢复。

## 实施计划

1. 新增 `src/ai_trading_system/reports/research_governance_recovery_pack.py`。
2. 新增 `aits reports research-governance-recovery-pack` 和
   `aits reports validate-research-governance-recovery-pack`。
3. Reader Brief 从 report index 读取 latest recovery pack 并展示 recovery status、
   blockers/warnings、next owner action 和 paper/extended/live boundaries。
4. 更新 report registry、artifact catalog、README、operations runbook 和
   `docs/system_flow.md`。
5. 添加 focused tests 覆盖 healthy、blocked、manual-review、CLI 和 Reader Brief summary。

## 验收标准

- Recovery pack 和 validation JSON/Markdown 可生成。
- 状态枚举和三类 trading boundary 字段可验证。
- 真实 2026-06-17 pack 汇总 latest 385-399 outputs，预期在 cost/benchmark/monthly/
  promotion/extended/roadmap/decision snapshot blockers 下保持 blocked。
- Reader Brief latest 可展示 recovery pack section。
- Focused tests、ruff、compileall、documentation contract、report index、Reader Brief
  quality、data quality gate 和 git diff check 通过。

## 进度记录

|日期|状态|记录|
|---|---|---|
|2026-06-17|IN_PROGRESS|新增需求文档和任务登记；实现只读 recovery governance pack。|
|2026-06-17|DONE|新增 recovery governance pack module、CLI、validation、Reader Brief section、report registry、artifact catalog、README、operations runbook、system flow 和 focused tests。真实 artifact `outputs/reports/research_governance_recovery_pack_2026-06-17.json/md` 输出 `RECOVERY_GOVERNANCE_BLOCKED`、sources=16/16、remaining_blockers=9、remaining_warnings=10、manual_review_items=1、normal_paper_shadow_may_resume=false、extended_shadow_remains_forbidden=true、live_trading_remains_forbidden=true；validation `PASS_WITH_WARNINGS`、failed=0。该 pack 只读汇总既有 outputs，不运行上游、不补造 artifact、不写 owner decision、不批准 live trading、official target、broker/order 或 production mutation。|
