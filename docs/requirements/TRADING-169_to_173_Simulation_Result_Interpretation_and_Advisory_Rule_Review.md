# TRADING-169 to TRADING-173 Simulation Result Interpretation and Advisory Rule Review

最后更新：2026-07-13

## 1. 背景

TRADING-161 to TRADING-168 已生成 backtest simulation advisory evaluation。最新真实链路包括：

- outcome_id：`57c2eb4e71c3320d`
- calibration_id：`ad10060ace0a51b7`
- bridge_id：`cd97afd8defa8d49`
- outcome_mode：`BACKTEST_SIMULATION`
- pit_safety_status：`SIMULATION_NOT_PIT`
- report label：`BACKTEST_SIMULATION_NOT_PIT`

当前 simulation 只能作为研究证据，不能作为 PIT evidence、forward evidence 或 production evidence。active variants 在 5/10/20 trading-day windows 有收益增强迹象，但收益增强伴随更深 drawdown；`defensive_limited_adjustment` 虽然 overall best，但没有在 `tech_drawdown` / `risk_off` 中稳定证明防守有效。

## 2. 阶段目标

本阶段目标是把 TRADING-161 to TRADING-168 的 raw metrics 转成可读、可审查、可追踪的规则评估包：

|ID|名称|状态|验收重点|
|---|---|---|---|
|TRADING-169|Variant Result Interpretation Pack|VALIDATING|解释每个 variant 的 role、return profile、risk profile、recommended usage，并明确 `BACKTEST_SIMULATION_NOT_PIT`。|
|TRADING-170|Risk-Return Tradeoff Review for Active Variants|VALIDATING|比较 active variants 相对 `no_trade` 的收益提升、drawdown worsening、turnover 代价和 risk-return status。|
|TRADING-171|Regime-specific Defensive Validation|VALIDATING|验证 `defensive_limited_adjustment` 在 pressure regimes 中是否真的 defensive，不因 overall best 自动判定有效。|
|TRADING-172|Advisory Rule Proposal Review|VALIDATING|只生成 calibration proposals 的人工 review 包，`auto_apply=false`，`owner_approval_required=true`。|
|TRADING-173|Forward Confirmation Plan Update|VALIDATING|把 proposal review 转成 forward tracking targets、trigger conditions 和 failure conditions。|

## 3. 安全边界

- 不修改 `config/etf_portfolio/dynamic_v3_rescue/position_advisory_v1.yaml`。
- 不生成 production candidate。
- 不触发 broker API、自动下单、paper/real portfolio mutation 或 official target weights mutation。
- 不把 `BACKTEST_SIMULATION` 伪装为 PIT-safe replay 或 forward evidence。
- 所有 artifacts 必须写入 `broker_action_allowed=false`、`broker_action_taken=false`、`auto_policy_apply=false`、`production_effect=none`。

## 4. 设计决策

1. `limited_adjustment` 解释为 medium-horizon active tilt，而不是防守策略。它在 5/10/20d 有收益增强迹象，但 1d 弱于 no_trade 且 20d max drawdown 更深。
2. `consensus_target` 只作为 upper-bound reference。它代表更完整跟随 simulated consensus 的上界，不适合作为默认执行方案。
3. `defensive_limited_adjustment` 必须做 regime-specific validation。overall best 不能证明压力窗口下防守有效。
4. risk-return review 必须把 return improvement 和 risk improvement 分开，不把收益改善自动解释为规则更优。
5. proposal review 只决定是否进入 observation / owner review / more data，不自动修改配置。
6. forward confirmation plan 是后续 rule calibration 的进入条件，不是 production approval。

## 5. Artifact 计划

新增 runtime artifact roots：

```text
reports/etf_portfolio/dynamic_v3_rescue/sim_interpretation/<interpretation_id>/
reports/etf_portfolio/dynamic_v3_rescue/sim_risk_return/<risk_return_id>/
reports/etf_portfolio/dynamic_v3_rescue/sim_defensive_validation/<defensive_validation_id>/
reports/etf_portfolio/dynamic_v3_rescue/advisory_proposal_review/<proposal_review_id>/
reports/etf_portfolio/dynamic_v3_rescue/forward_confirmation_plan/<confirmation_plan_id>/
```

## 6. 验收命令

```bash
aits etf dynamic-v3-rescue sim-interpretation run --outcome-id 57c2eb4e71c3320d --calibration-id ad10060ace0a51b7 --bridge-id cd97afd8defa8d49
aits etf dynamic-v3-rescue sim-risk-return run --outcome-id 57c2eb4e71c3320d
aits etf dynamic-v3-rescue sim-defensive-validation run --outcome-id 57c2eb4e71c3320d
aits etf dynamic-v3-rescue advisory-proposal-review run --interpretation-id <interpretation_id> --risk-return-id <risk_return_id> --defensive-validation-id <defensive_validation_id> --calibration-id ad10060ace0a51b7
aits etf dynamic-v3-rescue forward-confirmation-plan run --proposal-review-id <proposal_review_id> --bridge-id cd97afd8defa8d49
```

对应 validate 命令必须 PASS，focused tests、ruff、compileall、git diff check、dynamic-v3 root validation 和 dynamic-v3 family artifact validation 必须通过或记录明确阻塞。

## 7. 进展记录

- 2026-07-13：ARCH-004G2.4BX `COMPLETE`。TRADING-173的3 callback迁canonical；Proposal Review/Forward Bridge validated/cutoff/same-Calibration、`forward_confirmation_plan_input_snapshot.v2` full bundles/validations、reviewed semantic policy、source-only target projection、exact Bridge criteria inheritance、empty/unmatched INSUFFICIENT与全view/live-source byte validator通过529 focused、259 architecture-fitness、203 contract-validation。Fixture只由真实proposal解锁1个limited target，不补造defensive/consensus/threshold。G2.4继续，Confirmation Targets注册仍为独立slice。
- 2026-07-13：ARCH-004G2.4BX 对TRADING-173冻结Forward Confirmation Plan迁移合同并进入`IN_PROGRESS`。3 callback迁canonical；producer要求Proposal Review/Forward Bridge PASS/time/same-Calibration lineage，冻结full bundles/validations+reviewed semantic policy。Plan只投影Bridge真实targets并由真实review proposals解锁，events/windows/numeric criteria逐值继承Bridge；空/无匹配为INSUFFICIENT_DATA，不补造consensus target/threshold。Validator重验live sources/policy并逐字节重算全views；Confirmation Targets注册及后续链保持独立slice。
- 2026-07-13：ARCH-004G2.4BW `COMPLETE`。TRADING-172的3 callback迁canonical；四source validated/cutoff/same-Outcome、reviewed proposal policy、`advisory_proposal_review_input_snapshot.v2` full bundles/validations、no fabricated proposal/confidence与全view/live-source byte validator通过514 focused、258 architecture-fitness、203 contract-validation。G2.4继续，Forward Confirmation仍为独立slice。
- 2026-07-13：ARCH-004G2.4BW 对TRADING-172冻结Advisory Proposal Review迁移合同并进入`IN_PROGRESS`。3 callback迁canonical；producer要求Interpretation/Risk/Defensive/Calibration四source PASS/time/same-Outcome lineage，冻结full bundles/validations+reviewed proposal policy。不得伪造proposal/confidence；空proposal显式INSUFFICIENT_DATA；decision/conditions只来自policy。Validator重验live sources/policy并逐字节重算全部views；Forward Confirmation保持独立slice。
- 2026-07-13：ARCH-004G2.4BV `COMPLETE`。TRADING-171的3 callback迁canonical；validated/cutoff Outcome、reviewed defensive policy、`sim_defensive_validation_input_snapshot.v2` full bundle/validation/policy、same-regime/event/window AVAILABLE finite pairs、paired units、common-cohort ranking、null/INSUFFICIENT语义与全view/live-source byte validator通过500 focused、257 architecture-fitness、203 contract-validation。G2.4继续，Proposal及后续链仍为独立slice。
- 2026-07-13：ARCH-004G2.4BV 对TRADING-171冻结Simulation Defensive Validation迁移合同并进入`IN_PROGRESS`。3 callback迁canonical；producer要求Outcome PASS/time cutoff并冻结full bundle/validation+reviewed defensive policy。Matrix只用same-regime/event/window AVAILABLE finite defensive/no_trade pairs，披露paired units，missing为null/INSUFFICIENT_DATA；pressure regimes/windows/sample floor/return-drawdown boundaries均由policy治理。Validator重验live Outcome/policy并逐字节重算全部views；Proposal及后续链保持独立slice。
- 2026-07-13：ARCH-004G2.4BU `COMPLETE`。TRADING-170的3 callback迁canonical；validated/cutoff Outcome、`sim_risk_return_input_snapshot.v2` full bundle/validation、same-event 20d AVAILABLE finite pairs、paired event/window counts、null/INSUFFICIENT ratio语义与全view/live-source byte validator通过486 focused、256 architecture-fitness、203 contract-validation。G2.4继续，Defensive及后续链仍为独立slice。
- 2026-07-13：ARCH-004G2.4BU 对TRADING-170冻结Simulation Risk-Return迁移合同并进入`IN_PROGRESS`。3 callback迁canonical；producer要求Outcome content-derived PASS/time cutoff并冻结full bundle/validation。Tradeoff只用same-event+20d AVAILABLE finite variant/no_trade pairs，披露paired units，missing/无分母为null/INSUFFICIENT_DATA。Validator重验live Outcome并逐字节重算snapshot/CSV/summary/manifest/Markdown；Defensive及后续链保持独立slice。
- 2026-07-13：ARCH-004G2.4BT `COMPLETE`。TRADING-169的3 callback迁canonical；validated/cutoff/same-lineage Outcome/Calibration/Bridge、`sim_interpretation_input_snapshot.v2` full bundle/validation、paired finite/null-preserving matrix、evidence-derived findings/confidence与全view/live-source byte validator通过475 focused、255 architecture-fitness、203 contract-validation。G2.4继续，Risk Return及后续链仍为独立slice。
- 2026-07-13：ARCH-004G2.4BT 对TRADING-169冻结Simulation Interpretation迁移合同并进入`IN_PROGRESS`。3 callback迁canonical；producer要求Outcome/Calibration/Forward Bridge三source content-derived PASS/time cutoff/same-Outcome lineage，冻结三个full bundle/validation。Matrix/findings只用AVAILABLE finite same-cohort metrics，missing保持null/INSUFFICIENT_DATA，confidence从真实可用证据推导；Forward Bridge仅`TRACKING_PLAN_ONLY`，不表示forward success。Validator重验live sources并逐字节重算全views；Risk Return及后续链保持独立slice。
- 2026-06-10：新增需求文档和 task register 入口，状态为 IN_PROGRESS；开始实现 TRADING-169 to TRADING-173 的 generation/report/validation CLI、artifacts、Reader Brief integration 和 focused tests。
- 2026-06-10：baseline 实现完成并进入 VALIDATING。真实 artifact IDs：interpretation `a629c036f1ea3129`、risk_return `c61b1b9ca357cba1`、defensive_validation `b79486b62042b702`、proposal_review `f5dc442131f3740c`、confirmation_plan `808e55a74ca6951f`。
- 2026-06-10：关键结论保持 manual review only：`defensive_limited_adjustment_status=PARTIALLY_DEFENSIVE`，proposal review `auto_apply=false`、`owner_approval_required=true`，confirmation plan 输出 `limited_adjustment_vs_no_trade`、`defensive_limited_adjustment_drawdown`、`consensus_target_risk` 三个 forward targets。
- 2026-06-10：验证通过五个新增 validate CLI、五个 report CLI、Dynamic v3 root validation、Dynamic v3 family artifact validation、report index、Reader Brief、Reader Brief quality、focused 5 tests、相关 34-test subset、ruff、compileall 和 `git diff --check`。全量 pytest 已尝试 15 分钟超时，未返回失败用例；使用相关测试集补强覆盖本阶段变更面。
