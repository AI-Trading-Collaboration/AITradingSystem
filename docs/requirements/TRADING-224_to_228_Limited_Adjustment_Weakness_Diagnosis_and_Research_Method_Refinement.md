# TRADING-224～228 Limited Adjustment Weakness Diagnosis and Research Method Refinement

最后更新：2026-07-13

## 背景

TRADING-219～223 已把 `limited_adjustment` 从 selection review 推进到 research
method hardening pack，但真实 hardening 结论仍为：

- `candidate_method=limited_adjustment`
- `hardening_decision=REVIEW_REQUIRED`
- `confidence=LOW`
- long-window risk: `RETURN_IMPROVES_RISK_WORSENS`
- rolling consistency: `UNSTABLE`
- data warning impact: `REVIEW_REQUIRED`

本阶段要把未 harden 的原因继续拆解为可审计诊断、归因、repair plan、替代
方案比较和 refined method proposal。该链路仍是 research-only，不是 PIT-safe
production backtest，不允许生成 official target weights、broker order、production
candidate 或自动 owner approval。

## 子任务

### TRADING-224 Limited Adjustment Rolling Instability Diagnosis

新增 `limited-instability run/report` 和 `validate-limited-instability`，读取
paper shadow backfill 与 `limited-consistency` artifact，输出 unstable window
inventory、instability reason summary、rolling failure pattern 和诊断报告。报告必须解释
差窗口是收益、回撤、risk-adjusted、turnover 还是 mixed 问题，并披露是否集中在
pressure regimes 或伴随 weight / exposure jump。

### TRADING-225 Return-Improves-Risk-Worsens Attribution

新增 `limited-risk-attribution run/report` 和 `validate-limited-risk-attribution`，
读取 paper shadow backfill，输出 return contribution、drawdown contribution、
exposure shift attribution 和 risk worsening events。报告必须解释收益改善和风险恶化
是否来自 QQQ / SMH / SOXX 等 risk asset 或 semiconductor exposure，以及是否需要
risk cap 方向。

### TRADING-226 Data Warning Blocking Review / Repair Plan

新增 `data-warning-repair-plan run/report` 和 `validate-data-warning-repair-plan`，
读取 `data-warning-impact` artifact，只生成 repair plan，不自动修复缓存或重跑
backfill。若 warning 明细缺失，必须保留 manual review / unknown 结论，不得静默降级。

### TRADING-227 Alternative Method Candidate Review

新增 `alternative-method-review run/report` 和 `validate-alternative-method-review`，
比较 `limited_adjustment`、`defensive_limited_adjustment`、`static_baseline`、
`consensus_target` 以及 conceptual `risk_capped_limited_adjustment`、
`regime_gated_limited_adjustment`、`lower_turnover_limited_adjustment`、
`cash_buffered_limited_adjustment`。本阶段只提出候选，不实现新 method。

### TRADING-228 Refined Research Method Proposal Pack

新增 `refined-method-proposal run/report` 和 `validate-refined-method-proposal`，
整合 instability、risk attribution、repair plan 和 alternative review，输出下一步建议、
owner checklist、Reader Brief section 和 no broker / no production 安全边界。

## 实施顺序

1. 更新任务登记和本文档，记录 P0 scope 与安全边界。
2. 复用 `dynamic_v3_system_target.py` 的 artifact / latest pointer / validation 模式。
3. 增加五组 run/report payload/validate/render 函数和 CLI。
4. 更新 Reader Brief、report registry、artifact catalog、system flow、operations runbook 和 README。
5. 增加 focused tests。
6. 运行真实链路、focused tests、ruff、compileall、`git diff --check`、dynamic-v3 validation 和 artifact family validation。

## 验收标准

- 五组新增 CLI 可运行并写入 runtime artifacts。
- 所有新增 artifact 固定 `research_target_only=true`、`not_official_target_weights=true`、
  `broker_action_allowed=false`、`broker_action_taken=false`、`order_ticket_generated=false`、
  `production_effect=none`、`auto_apply=false`。
- Instability diagnosis 列出 unstable windows、dominant failure pattern 和 recommended action。
- Risk attribution 输出 return / drawdown / exposure attribution 与 risk worsening source。
- Data warning repair plan 输出 warning repair actions、blocking matrix，且不自动执行 destructive repair。
- Alternative method review 至少输出 `risk_capped_limited_adjustment` 和
  `regime_gated_limited_adjustment`，并明确不自动实现新 method。
- Refined proposal pack 给出 `recommended_next_step`、proposed next methods、confidence、
  owner checklist 和 Reader Brief section。
- README、operations runbook、system flow、report registry、artifact catalog、Reader Brief 和本文档同步。
- Focused tests、ruff、compileall、`git diff --check` 和要求的 validate 命令通过，或记录阻塞原因。

## 状态记录

- 2026-07-13: ARCH-004 G2.4CK contract freeze，状态重新进入 `IN_PROGRESS`。迁移范围固定为
  TRADING-224～228 五组 run/report/validate 共 15 callback，canonical domain/interface 为
  `dynamic_v3_system_target_refinement.py`；TRADING-229～233 及后续 risk-capped 实现保持独立
  slice。旧实现审计发现 producer 未执行上游 content validator、未冻结输入快照，Instability、
  Alternative 与 Refined 可组合跨 Backfill/跨时间 artifact；Risk Attribution 直接读取可变 cache，
  没有 cache/DQ commitment，且缺失收益会经 `fillna(0)` 进入归因；五类 validator 均不能从输入
  逐字节重建输出。CK 要求正式写件前完成上游 PASS、timezone cutoff、exact Backfill/Selection
  lineage 与 chronology；五类 `*.v2` bounded input snapshot 固定 source bundles、validation、
  reviewed `method_refinement_v1` policy，以及 Risk 链的 live DQ/cache commitment；missing/non-finite
  保持 null/UNKNOWN，不以 0 补造结论；validator 重验 live source/policy/cache/DQ 并逐字节重算
  JSON/JSONL/Markdown。所有阈值已从 legacy 常量迁入 reviewed pilot policy，数值保持不变，故本
  slice 不调策略，只使解释规则可审计。固定 research/manual-only、no official/no auto/no order/
  no broker、`production_effect=none`；单 slice 完成不触发 phase-level ARCH-005 handoff。
- 2026-07-13: CK canonical implementation与slice validation完成，状态为
  `COMPLETE_G2_4_CONTINUES`；单slice完成不触发phase handoff。15个callback已离开legacy
  CLI root，canonical interface/domain为`dynamic_v3_system_target_refinement.py`，legacy domain仅
  保留lazy compatibility wrappers。五类snapshot与content-derived validator已落地；Risk Attribution
  改为Backfill snapshot内共同finite价格、`pct_change(fill_method=None)`，首日/缺失收益不再补0；
  conceptual method metrics固定null/UNKNOWN。当前contract fixture区间为`2022-12-01..2024-02-29`：
  unstable windows=49，drawdown-worse=48，recommendation=`consider_risk_cap`；return相对static
  正贡献前三为`SOXX,QQQ,SMH`，最大回撤负贡献前三为`QQQ,SMH,SOXX`；risk/semiconductor/cash
  exposure相对static为`+0.055631/+0.045658/-0.041546`，risk source=`mixed`；20日risk events=268，
  Data Warning repair结果仍`UNKNOWN`；最终Proposal=`IMPLEMENT_RISK_CAPPED_RESEARCH_METHOD/LOW`。
  这些是source-backed contract fixture结果，只证明可复算性，不是策略有效性或生产建议。完整focused
  正向/Reader Brief/五类output-policy-cache drift负向=`7 passed`；slice+CLI/deprecation=`110 passed`；
  architecture-fitness=`272 passed`（`outputs/validation_runtime/architecture-fitness_20260713T225921Z/`）；
  contract-validation=`203 passed`（`outputs/validation_runtime/contract-validation_20260713T230116Z/`）。Generated
  manifests=`907 modules / 1,114 tests / 858 direct writers / 0 violations`；CLI runtime tree保持
  `41 roots/291 groups/993 leaves/0 duplicate/tree_sha256=d4744f3e...`。首轮CLI仅有3个预期source
  hash/行数freshness失败、architecture首轮仅有1个前slice source-hash supersession失败，均未掩盖行为
  回归；刷新合法baseline/freshness后复验PASS。Ruff、compileall通过，phase-level full validation仍留给整个
  G2.4 exit gate统一执行。

- 2026-06-12: 新增任务文档并进入 `IN_PROGRESS`，原因：owner 要求完成 TRADING-224～228
  limited adjustment weakness diagnosis 与 refined research method proposal pack。
- 2026-06-12: baseline 实现完成并转入 `VALIDATING`。真实链路产物：
  `limited-instability_122c078b28088f98`、`limited-risk-attribution_19631ef9ad2582a3`、
  `data-warning-repair-plan_2f308dceca003a5f`、
  `alternative-method-review_0e4d3cbfbbf14a95`、
  `refined-method-proposal_2e31f6cb2590ce5d`。
- 2026-06-12: 真实诊断结论为 unstable windows=129、
  `dominant_failure_regime=sideways_choppy`、`recommendation=consider_risk_cap`；
  `risk_worsening_source=higher_semiconductor_exposure`，top return contributors 为
  `SOXX,SPY,SMH`，top drawdown contributors 为 `QQQ,SMH,SOXX`；data warning repair
  plan 保持 `overall_data_warning_status=REVIEW_REQUIRED`、
  `hardening_allowed_after_repair=UNKNOWN` 且 `auto_repair_executed=false`；refined
  proposal 为 `IMPLEMENT_RISK_CAPPED_RESEARCH_METHOD`，候选下一步 method 为
  `risk_capped_limited_adjustment` 和 `regime_gated_limited_adjustment`，confidence=`LOW`。
- 2026-06-12: 验证已通过 `validate-data` (`PASS_WITH_WARNINGS`)、五个新增
  report/validate CLI、`aits etf dynamic-v3-rescue validate`、`artifacts repair-latest`、
  `artifacts validate --family dynamic_v3_rescue`、report index、Reader Brief
  2026-06-12、Reader Brief quality、focused pytest `9 passed`、ruff、compileall、
  `git diff --check` 和 full pytest `2384 passed, 640 warnings`。
- 2026-06-12: 限制仍有效：本阶段没有实现 `risk_capped_limited_adjustment` 或
  `regime_gated_limited_adjustment`，没有修复 data cache，没有修改
  `model_target_portfolio_v1.yaml` / `position_advisory_v1.yaml`，没有写 official target
  weights、paper/real portfolio、baseline/production state、order ticket 或 broker action。
