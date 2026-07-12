# TRADING-151 to TRADING-155: Forward Outcome Accumulation and Replay Sample Expansion

最后更新：2026-07-12

## ARCH-004G2.4BB 进展

- 2026-07-12：Outcome Due完成source-derived hardening与CLI迁移。Scan在任何due output前执行time/as-of、cached DQ和所有Advisory Outcome validators，冻结完整outcome bundles、DQ、price/rate checksums及cutoff price-date availability；duplicate daily×window阻断，validator重算全部scan views并检测tamper。`update-ready`先要求due PASS/live freshness/time/single-use，按outcome显式传DUE `allowed_window_days`，post-update outcome validation PASS并记录execution。NOT_DUE/PRICE_MISSING不更新，无自动后续链或policy/production/broker effect。
- 2026-07-12：Replay Sample Expansion进入G2.4BC source-derived hardening与CLI迁移。Run在任何output前执行range/future、cached DQ、Daily/Owner/Replay Inventory full validators与generated cutoff；重复daily id/as-of、ambiguous latest inventory或跨源sample冲突fail closed。完整source bundles、DQ、price/rate checksums、cutoff price dates和reviewed PIT policy冻结到snapshot；events按`daily_advisory_id|as_of`唯一化，PIT safety与outcome-price evaluability分开。Validator重验live source/DQ/policy并重算inventory/events/summary/manifest/Markdown；不运行replay/backfill或修改policy/portfolio/production/broker。
- 2026-07-12：Replay Sample Expansion的G2.4BC实现与正式验证完成，任务继续保持`VALIDATING`等待真实PIT-safe样本积累。当前validated TARGET_ONLY fixture因缺decision-time current weights，技术source validator PASS但分类仍为`PIT_UNSAFE/INELIGIBLE`，证明不会用结构完整性冒充replay资格。Focused=168、architecture=238、contract=203 PASS；无production/broker effect。
- 2026-07-12：Outcome Dashboard进入G2.4BD source-derived hardening与CLI迁移。Build要求selected Advisory Outcome、Repair/Backfill、Paper Sim、Diagnosis和Outcome Due validators在generated cutoff下PASS，选择唯一latest链并冻结full bundles与reviewed pending-action policy。Matrix按三类明确sample unit计算，pending reason只来自selected frozen sources；validator重算matrix/mode/pending/manifest/Markdown/Reader Brief。无upstream/policy/portfolio/production/broker effect。
- 2026-07-12：Outcome Dashboard的G2.4BD实现与正式验证完成并保持`VALIDATING`。Focused=170、architecture=239、contract=203 PASS；当前fixture只有4个forward PENDING windows，historical/simulation为0、available=0、top reason=future_window_not_reached，证明missing source不会被补成AVAILABLE。无production/broker effect。

## 背景

TRADING-146_to_150 已经能解释 historical replay 的 `PARTIAL`、`PENDING`
和 `INSUFFICIENT_DATA` 原因，并能输出 manual-only advisory calibration proposal。
当前限制不是分析框架缺失，而是 forward outcome 与 replay 样本仍不足：

- available outcome window 数量不足，pending window 仍占主导；
- PIT_SAFE / PIT_WARNING replay events 数量不足；
- limited_adjustment vs no_trade 主要只有短窗口样本；
- consensus_target 暂时表现较好但 recommendation confidence 仍为 LOW。

本阶段目标是在不触发 broker、不进入 production、不自动改 policy 的前提下，
积累到期 outcome、扩大可审计 replay 样本，并把可用性、专项评估和风险审查
汇总成可复核 artifacts。

## 子任务

|ID|范围|状态|验收标准|
|---|---|---|---|
|TRADING-151|Forward Outcome Scheduler & Due Window Detector|VALIDATING|`outcome-due scan/report/update-ready` 和 `validate-outcome-due` 可运行；能区分 `DUE`、`NOT_DUE`、`PRICE_MISSING`、`ALREADY_AVAILABLE`、`INSUFFICIENT_DATA`；`update-ready` 只处理 `can_update=true` 的 outcome。|
|TRADING-152|Replay Sample Expansion from Historical Candidate Artifacts|VALIDATING|`replay-sample-expansion run/report` 和 `validate-replay-sample-expansion` 已迁至canonical CLI并通过正式验证；只接受validated/cutoff-bound sources，输出immutable source snapshot与unique expanded replay events；`PIT_UNSAFE`或缺评价价格默认不进入 replay eligibility；等待真实PIT-safe样本积累。|
|TRADING-153|Outcome Availability Dashboard|VALIDATING|`outcome-dashboard build/report` 和 `validate-outcome-dashboard` 已迁至canonical CLI并通过正式验证；只聚合validated/cutoff-bound unique latest sources，输出immutable snapshot与三类明确sample-unit matrix；Reader Brief只读取frozen projection。|
|TRADING-154|Limited Adjustment vs No Trade Focused Evaluation|VALIDATING|`limited-vs-notrade run/report` 和 `validate-limited-vs-notrade` 可运行；聚合 forward/replay outcome；样本不足时明确 `INSUFFICIENT_DATA`；不自动修改 advisory policy。|
|TRADING-155|Consensus Target Risk Review|VALIDATING|`consensus-risk run/report` 和 `validate-consensus-risk` 可运行；样本不足时不得给 `PASS`；结论只能作为 observation/risk review input。|

## CLI

```bash
aits etf dynamic-v3-rescue outcome-due scan --as-of YYYY-MM-DD
aits etf dynamic-v3-rescue outcome-due report --latest
aits etf dynamic-v3-rescue outcome-due update-ready --due-id <due_id>
aits etf dynamic-v3-rescue validate-outcome-due --due-id <due_id>

aits etf dynamic-v3-rescue replay-sample-expansion run --start 2022-12-01 --end YYYY-MM-DD
aits etf dynamic-v3-rescue replay-sample-expansion report --latest
aits etf dynamic-v3-rescue validate-replay-sample-expansion --expansion-id <expansion_id>

aits etf dynamic-v3-rescue outcome-dashboard build
aits etf dynamic-v3-rescue outcome-dashboard report --latest
aits etf dynamic-v3-rescue validate-outcome-dashboard --dashboard-id <dashboard_id>

aits etf dynamic-v3-rescue limited-vs-notrade run
aits etf dynamic-v3-rescue limited-vs-notrade report --latest
aits etf dynamic-v3-rescue validate-limited-vs-notrade --focus-id <focus_id>

aits etf dynamic-v3-rescue consensus-risk run
aits etf dynamic-v3-rescue consensus-risk report --latest
aits etf dynamic-v3-rescue validate-consensus-risk --risk-id <risk_id>
```

## Artifacts

```text
reports/etf_portfolio/dynamic_v3_rescue/outcome_due/<due_id>/
reports/etf_portfolio/dynamic_v3_rescue/replay_sample_expansion/<expansion_id>/
reports/etf_portfolio/dynamic_v3_rescue/outcome_dashboard/<dashboard_id>/
reports/etf_portfolio/dynamic_v3_rescue/limited_vs_notrade/<focus_id>/
reports/etf_portfolio/dynamic_v3_rescue/consensus_risk/<risk_id>/
```

## 设计决策

1. Outcome due detector 是必要的，因为 forward windows 自然到期后必须有统一扫描、
   price-cache 可见性和 update-ready 清单，而不是人工猜测哪些 outcome 能更新。
2. Replay sample expansion 是必要的，因为当前 replay conclusion 的瓶颈是 PIT-safe
   样本量，不是 rule scoring 逻辑本身。
3. Outcome dashboard 用统一 matrix 汇总 forward、historical replay 和 simulation
   outcome availability，避免在多个 artifact 中分散解释 pending reason。
4. limited_adjustment vs no_trade 是当前重点，因为 replay-forward bridge 已把它列为
   forward tracking focus，但 5/10/20 日窗口仍缺证据。
5. consensus_target 需要单独 risk review，因为它可能因更高风险资产、半导体集中度、
   drawdown 或 turnover 暂时提高收益，不能因少量样本直接成为默认执行规则。
6. 本阶段仍保持 no broker / no production / no auto config apply。所有规则调整只输出
   proposal 或 review input，`position_advisory_v1.yaml` 不自动修改。

## 数据质量与安全边界

- 任何读取 cached price/rate data 的 run command 必须调用 `validate_data_cache`
  等价质量门禁并在 manifest 中记录 `data_quality_status`，测试 fixture 可显式关闭。
- 所有 artifacts 固定 `production_effect=none`、`broker_action_allowed=false`、
  `broker_action_taken=false`、`production_candidate_generated=false`、
  `manual_review_required=true`。
- `outcome-due update-ready` 只复用现有 `advisory-outcome update`，并只针对
  `update_ready_list.json` 中 `can_update=true` 的 outcome；NOT_DUE window 保持 pending。

## 进展记录

- 2026-06-10：任务登记与需求文档创建，进入实现阶段。下一步新增核心模块、CLI、
  validators、focused tests、report registry、artifact catalog、system flow、operations
  runbook、README 和 Reader Brief integration。
- 2026-06-10：实现 `dynamic_v3_outcome_accumulation.py`、CLI、Reader Brief
  outcome dashboard 摘要、report registry、artifact catalog、system flow、operations
  runbook、README 和 focused tests。真实 artifacts 已生成：
  `due_id=9d344a0a7c24b676`、`expansion_id=148142c0e60f0e07`、
  `dashboard_id=b8a1af7ac722bd69`、`focus_id=58b9235fedb7e197`、
  `risk_id=f081e53730e3b300`。新 artifact validations、`aits etf dynamic-v3-rescue
  validate`、`aits etf dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue`、
  focused tests、`ruff check src tests`、`compileall src tests` 和 full `pytest tests -q`
  均通过；状态进入 VALIDATING，等待 owner 复核真实样本解释和后续 outcome accumulation。
