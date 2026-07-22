# TRADING-161 to TRADING-168: Backtest Simulation Advisory Evaluation

最后更新：2026-07-13

## 背景

TRADING-156_to_160 已把 forward advisory outcome update / rolling evidence
refresh 串成安全闭环，但 forward window 仍受真实日期限制。为在
`ai_after_chatgpt` regime 内更快评估当前 shadow shortlist 和 manual advisory 规则，
本阶段新增明确标记为 `BACKTEST_SIMULATION` 的历史模拟链路。

该链路不是 PIT replay，也不是生产证据。所有 artifacts 必须显式写入
`outcome_mode=BACKTEST_SIMULATION`、`pit_safety_status=SIMULATION_NOT_PIT`、
`not_for_production=true`、`broker_action_allowed=false`、
`broker_action_taken=false`、`auto_policy_apply=false` 和
`production_effect=none`。

## 子任务

|ID|范围|状态|验收标准|
|---|---|---|---|
|TRADING-161|Backtest Simulation Config and Advisory Event Generator|VALIDATING|`backtest-sim config-validate`、`event-generate/report` 和 `validate-backtest-sim-events` 可运行；生成前 strict config/time/DQ fail closed，`backtest_sim_event_input_snapshot.v2` 冻结 config、shortlist、position policy、candidate bundle、price/rate cutoff rows、schedule 与 DQ evidence；validator 重验 live sources 并重算全部 views。事件只作为 `BACKTEST_SIMULATION_NOT_PIT` observations，且不自动运行后续链。|
|TRADING-162|Simulated Advisory Variants|VALIDATING|`variants-generate/report` 和 `validate-backtest-sim-variants` 可运行；只消费validated/cutoff-safe event bundle，`backtest_sim_variant_input_snapshot.v2`冻结event/config/validation；exact enabled variants、state continuity、weights/delta/turnover/limits与全部views可重算。生成五类research-only variants，不自动运行outcome/paper。|
|TRADING-163|Simulated Outcome Windows|VALIDATING|`backtest-sim outcome-run/report` 和 `validate-backtest-sim-outcome` 可运行；只消费validated/cutoff-safe variant bundle并在output前通过DQ；`backtest_sim_outcome_input_snapshot.v2`冻结variant/event/config/validation、price/rate full-file与cutoff rows、DQ evidence；按 1/5/10/20 trading-day windows输出AVAILABLE/PENDING/INSUFFICIENT_DATA，unknown metrics为null，全部views逐字节可重算。|
|TRADING-164|Historical Paper Portfolio v2|VALIDATING|`backtest-sim paper-run/report` 和 `validate-backtest-sim-paper` 可运行；只消费validated/cutoff-safe variant bundle并在output前通过DQ；`backtest_sim_paper_input_snapshot.v2`冻结variant/event/config/validation、price/rate full-file与cutoff rows、DQ evidence；selected/no_trade state、ledger、performance与全部views逐字节可重算，unknown metrics为null；return明确为gross-before-costs/no cost model。|
|TRADING-165|Regime-Specific Simulation Review|VALIDATING|`backtest-sim regime-review/report` 和 `validate-backtest-sim-regime` 可运行；只消费validated/cutoff-safe outcome bundle，`backtest_sim_regime_input_snapshot.v2`冻结full outcome/validation；按known regime×variant汇总AVAILABLE finite event/window evidence，missing metrics为null，全部views逐字节可重算。|
|TRADING-166|Overfit and Sensitivity Diagnostics|VALIDATING|`backtest-sim sensitivity-run/report` 和 `validate-backtest-sim-sensitivity` 可运行；输出 threshold、shortlist、adjustment limit、event frequency sensitivity 与 overfit warning summary；高风险时不得给 strong calibration。|
|TRADING-167|Simulation Calibration Pack|VALIDATING|`backtest-sim calibration-pack/report` 和 `validate-backtest-sim-calibration` 可运行；只生成 owner review proposal，不自动改 `position_advisory_v1.yaml` 或 production policy。|
|TRADING-168|Simulation-to-Forward Bridge|VALIDATING|`backtest-sim forward-bridge/report` 和 `validate-backtest-sim-forward-bridge` 可运行；生成 forward confirmation targets、weekly review questions 和 Reader Brief section。|

## CLI

```bash
aits etf dynamic-v3-rescue backtest-sim config-validate --config config/etf_portfolio/dynamic_v3_rescue/backtest_simulation_advisory_v1.yaml
aits etf dynamic-v3-rescue backtest-sim event-generate --config config/etf_portfolio/dynamic_v3_rescue/backtest_simulation_advisory_v1.yaml
aits etf dynamic-v3-rescue backtest-sim event-report --latest
aits etf dynamic-v3-rescue validate-backtest-sim-events --event-set-id <event_set_id>

aits etf dynamic-v3-rescue backtest-sim variants-generate --event-set-id <event_set_id>
aits etf dynamic-v3-rescue backtest-sim variants-report --latest
aits etf dynamic-v3-rescue validate-backtest-sim-variants --variant-set-id <variant_set_id>

aits etf dynamic-v3-rescue backtest-sim outcome-run --variant-set-id <variant_set_id>
aits etf dynamic-v3-rescue backtest-sim outcome-report --latest
aits etf dynamic-v3-rescue validate-backtest-sim-outcome --sim-outcome-id <sim_outcome_id>

aits etf dynamic-v3-rescue backtest-sim paper-run --variant-set-id <variant_set_id>
aits etf dynamic-v3-rescue backtest-sim paper-report --latest
aits etf dynamic-v3-rescue validate-backtest-sim-paper --sim-paper-id <sim_paper_id>

aits etf dynamic-v3-rescue backtest-sim regime-review --sim-outcome-id <sim_outcome_id>
aits etf dynamic-v3-rescue backtest-sim sensitivity-run --sim-outcome-id <sim_outcome_id>
aits etf dynamic-v3-rescue backtest-sim calibration-pack --sim-outcome-id <sim_outcome_id> --sim-paper-id <sim_paper_id> --regime-review-id <regime_review_id> --sensitivity-id <sensitivity_id>
aits etf dynamic-v3-rescue backtest-sim forward-bridge --calibration-pack-id <calibration_pack_id>
```

## Artifacts

```text
reports/etf_portfolio/dynamic_v3_rescue/backtest_sim_events/<event_set_id>/
reports/etf_portfolio/dynamic_v3_rescue/backtest_sim_variants/<variant_set_id>/
reports/etf_portfolio/dynamic_v3_rescue/backtest_sim_outcome/<sim_outcome_id>/
reports/etf_portfolio/dynamic_v3_rescue/backtest_sim_paper/<sim_paper_id>/
reports/etf_portfolio/dynamic_v3_rescue/backtest_sim_regime/<regime_review_id>/
reports/etf_portfolio/dynamic_v3_rescue/backtest_sim_sensitivity/<sensitivity_id>/
reports/etf_portfolio/dynamic_v3_rescue/backtest_sim_calibration/<calibration_pack_id>/
reports/etf_portfolio/dynamic_v3_rescue/backtest_sim_forward_bridge/<bridge_id>/
```

## 设计决策

1. 模拟事件默认从 2022-12-01 开始，延续 AI-after-ChatGPT regime，pre-2022 只允许作为配置外对照或 warm-up。
2. 该链路复用当前 shadow shortlist 输出和 manual advisory 调仓限制，但把历史日期上的信号重放标为 simulation observation，不声明 point-in-time safety。
3. outcome 计算只接受validated且不晚于generated cutoff的variant bundle，并在创建目录前执行与 `aits validate-data` 相同的数据质量门禁；失败时不留partial artifact。Full variant/event/config/validation、cache full-file/cutoff rows与DQ evidence冻结到`backtest_sim_outcome_input_snapshot.v2`，validator重验live source并逐字节重算全部views；PENDING/INSUFFICIENT未知指标保持null，不得伪装为0或参与best ranking。
4. variant、paper、regime、sensitivity、calibration 和 bridge artifacts 均固定 no broker / no production / no auto policy mutation。
5. Paper portfolio只披露`GROSS_BEFORE_COSTS`，因为当前simulation config未治理transaction/slippage cost model；不得将其解释为net performance。后续若引入cost必须先进入reviewed config/policy并通过同源重算validator。
6. 所有影响投资解释的阈值来自 `backtest_simulation_advisory_v1.yaml`；代码中的数值只允许作为 schema/格式/空样本默认值。
7. sensitivity 的高风险或样本不足状态必须阻止 strong calibration proposal，只允许进入 owner review / forward confirmation。

## 验收链路

```bash
aits etf dynamic-v3-rescue backtest-sim config-validate
aits etf dynamic-v3-rescue backtest-sim event-generate
aits etf dynamic-v3-rescue backtest-sim variants-generate --event-set-id <event_set_id>
aits etf dynamic-v3-rescue backtest-sim outcome-run --variant-set-id <variant_set_id>
aits etf dynamic-v3-rescue backtest-sim paper-run --variant-set-id <variant_set_id>
aits etf dynamic-v3-rescue backtest-sim regime-review --sim-outcome-id <sim_outcome_id>
aits etf dynamic-v3-rescue backtest-sim sensitivity-run --sim-outcome-id <sim_outcome_id>
aits etf dynamic-v3-rescue backtest-sim calibration-pack --sim-outcome-id <sim_outcome_id> --sim-paper-id <sim_paper_id> --regime-review-id <regime_review_id> --sensitivity-id <sensitivity_id>
aits etf dynamic-v3-rescue backtest-sim forward-bridge --calibration-pack-id <calibration_pack_id>
```

## 进展记录

- 2026-07-13：ARCH-004G2.4BS `COMPLETE`。TRADING-168的3 callback迁canonical；validated/cutoff Calibration、`backtest_sim_forward_bridge_input_snapshot.v2` full bundle/validation/lineage/policy binding、strict no-fallback events/windows/criteria、tracking-only semantics与全view/live-source byte validator通过464 focused、254 architecture-fitness、203 contract-validation。G2.4继续，后续simulation interpretation链仍为独立slice。
- 2026-07-13：ARCH-004G2.4BS 对 TRADING-168冻结Forward Bridge迁移合同并进入`IN_PROGRESS`：3 callback迁canonical；producer在output前要求Calibration content-derived PASS与timezone-aware cutoff，冻结Calibration full bundle/validation/lineage、reviewed forward policy及governance metadata到`backtest_sim_forward_bridge_input_snapshot.v2`。Events/windows/win-rate/return/drawdown criteria只从frozen policy取值，missing/invalid fail closed不使fallback。Targets固定`TRACKING_REQUIRED`与`TRACKING_PLAN_ONLY`语义，不声明forward success/production candidate。Validator重验live Calibration并逐字节重算全部JSON/Markdown/Reader Brief；只生成manual observation plan，不运行后续链或改policy/config/portfolio/production/order/broker。
- 2026-07-13：ARCH-004G2.4BR `COMPLETE`。TRADING-167的3 callback迁canonical；validated/cutoff Outcome/Paper/Regime/Sensitivity、`backtest_sim_calibration_input_snapshot.v2` full bundles/validations与cross-lineage、finite/missing-null evidence、LOW_RISK-only positive proposal及全view/live-source validator通过451 focused、253 architecture-fitness、203 contract-validation。G2.4继续，Forward Bridge及后续链仍为独立slice。
- 2026-07-13：ARCH-004G2.4BR 对 TRADING-167冻结Calibration Pack迁移合同并进入`IN_PROGRESS`：3 callback迁canonical；producer在output前要求Outcome/Paper/Regime/Sensitivity四source content-derived PASS/time cutoff，冻结四份full bundle/validation并验证同一Outcome/variant/event lineage。Evidence仅保留finite metrics，missing为null；positive keep-rule proposal仅Sensitivity LOW_RISK且metric finite/positive时允许，其他状态只保留forward-confirmation/manual-review。Validator重验live sources并逐字节重算snapshot、evidence、proposals、limitations、manifest、Markdown和Reader Brief；tamper FAIL。Forward Bridge及后续链保持独立slice。
- 2026-07-13：ARCH-004G2.4BQ `COMPLETE`。TRADING-166的3 callback迁canonical；validated/cutoff Outcome source、`backtest_sim_sensitivity_input_snapshot.v2` full lineage binding、四类exact grids、AVAILABLE finite/missing null、event/window/result/excluded单位、missing dispersion exclusion、LOW_RISK-only strong calibration与全view/live-source validator通过437 focused、252 architecture-fitness、203 contract-validation。G2.4继续，Calibration及后续链仍为独立slice。
- 2026-07-13：ARCH-004G2.4BQ 对 TRADING-166冻结Sensitivity迁移合同并进入`IN_PROGRESS`：3 callback迁canonical；producer在output前要求Outcome content-derived PASS/time cutoff并冻结full outcome/validation bundle，event/config/price/DQ不得另开mutable lineage。Frequency/adjustment/shortlist/threshold grids exact/unique；只纳入AVAILABLE finite evidence，missing metrics为null并披露excluded counts，threshold缺dispersion不得静默通过；event/window/result row单位分开，strong calibration仅LOW_RISK允许且不自动apply。Validator重验live Outcome并逐字节重算snapshot、四类diagnostics、warnings、manifest和Markdown；tamper FAIL。Calibration及后续链保持独立slice。
- 2026-07-13：ARCH-004G2.4BP `COMPLETE`。TRADING-165的3 callback迁canonical；validated/cutoff Outcome source、`backtest_sim_regime_input_snapshot.v2` full outcome/validation binding、regime×variant exact coverage、event/window count分离、AVAILABLE finite aggregation、missing null与全view/live-source validator通过423 focused、251 architecture-fitness、203 contract-validation。G2.4继续，Sensitivity及后续链仍为独立slice。
- 2026-07-13：ARCH-004G2.4BP 对 TRADING-165完成实现主体并进入closeout验证：3 callback迁canonical；producer在output前要求Outcome content-derived PASS/time cutoff并冻结full outcome/validation bundle；regime×variant metrics明确event/window counts，只纳入AVAILABLE finite rows，missing metrics保持null且不参与best ranking。Validator重验live outcome并逐字节重算snapshot/inventory/metrics/summary/manifest/Markdown；source/snapshot/output tamper FAIL。11项regime focused通过，等待architecture/contract/manifests closeout。
- 2026-07-12：ARCH-004G2.4BO `COMPLETE`。TRADING-164的3 callback迁canonical；validated/cutoff variant source、pre-output DQ、`backtest_sim_paper_input_snapshot.v2` full lineage/cache/DQ binding、selected/no_trade state/ledger/performance重算、unknown null与gross-before-costs披露通过411 focused、250 architecture-fitness、203 contract-validation。G2.4继续，Regime及后续链仍为独立slice。
- 2026-07-12：ARCH-004G2.4BO 对 TRADING-164完成实现主体并进入closeout验证：3 callback迁canonical；producer在output前要求variant content-derived PASS/time cutoff/DQ并冻结full variant/event/config/validation/cache-cutoff/DQ snapshot；selected/no_trade state、ledger与performance可重算，无READY/不可计算metrics保持null/INSUFFICIENT_DATA；gross-before-costs/no-cost-model显式披露。Validator重验live variant/cache/DQ并逐字节重算snapshot/history/ledger/summary/manifest/Markdown；source/snapshot/output tamper FAIL。12项paper focused与52项完整simulation下游链验证通过，等待architecture/contract/manifests closeout。
- 2026-07-12：ARCH-004G2.4BN `COMPLETE`。TRADING-163的3 callback迁canonical；validated/cutoff variant source、pre-output DQ、`backtest_sim_outcome_input_snapshot.v2` full lineage/cache/DQ binding、unknown null semantics及逐字节全view/live-source validator通过398 focused、249 architecture-fitness、203 contract-validation。G2.4继续，Paper及后续链仍为独立slice。
- 2026-07-12：ARCH-004G2.4BN 对 TRADING-163完成实现主体并进入closeout验证：3 callback迁canonical；producer在output前要求variant content-derived PASS/time cutoff/DQ并冻结full variant/event/config/validation/cache-cutoff/DQ snapshot；AVAILABLE只允许finite metrics，PENDING/INSUFFICIENT unknown metrics保持null且不参与summary/ranking。Validator重验live variant/cache/DQ并逐字节重算snapshot/windows/summary/manifest/Markdown；source/snapshot/output tamper FAIL。11项outcome focused与41项完整simulation下游链验证通过，等待architecture/contract/manifests closeout。
- 2026-07-12：ARCH-004G2.4BM `COMPLETE`。TRADING-162的3 callback迁canonical；validated/cutoff event source、full event/config/validation snapshot、exact variant/state/weights/delta/turnover/limit invariants和逐字节重算validator通过387 focused、248 architecture-fitness、203 contract-validation。G2.4继续，Outcome及后续链仍为独立slice。
- 2026-07-12：ARCH-004G2.4BM 对 TRADING-162完成contract freeze和实现主体：3 callback迁canonical；producer在output前要求event content-derived PASS/time cutoff并冻结full event/config/validation snapshot；validator重验live event并逐字节重算snapshot/weights/ledger/manifest/Markdown。20项event/variant focused与89项CLI/variant组合验证通过，等待architecture/contract/manifests closeout。
- 2026-07-12：ARCH-004G2.4BL `COMPLETE`。TRADING-161 的4 callback已由canonical CLI owner承接；strict governed config、pre-output time/range/DQ、`backtest_sim_event_input_snapshot.v2` governed source/cache cutoff/DQ binding和content-derived live-source validator通过377 focused、247 architecture-fitness、203 contract-validation。G2.4继续，Variants及后续simulation链仍是独立slice，不自动触发。
- 2026-07-12：ARCH-004G2.4BL 对 TRADING-161 Backtest Simulation Event Foundation完成实现并进入验证：4 callback迁canonical；strict config覆盖empty path、unique schedule/variant/window、finite limits/threshold与shortlist/position/cache source schema。Event generation在任何output前执行timezone/range/DQ，冻结config、shortlist raw source、position policy raw source、candidate bundle、DQ evidence、price/rate full-file与cutoff rows、event schedule；异常零partial artifact，合法empty schedule显式`INSUFFICIENT_DATA`。Validator重验live inputs/DQ并重算events/manifest/Markdown，source/snapshot/output tamper FAIL。固定`BACKTEST_SIMULATION_NOT_PIT`，不运行variants/outcomes、不改policy/config/portfolio/production/broker。
- 2026-06-10：任务登记与需求文档创建，进入实现阶段。下一步完成 CLI wiring、validators、focused tests、README、operations runbook、system flow、report registry、artifact catalog 和真实链路验证。
- 2026-06-10：baseline 实现完成并进入 VALIDATING。新增 backtest simulation config、核心模块、`backtest-sim` CLI、8 个 root validators、report registry、artifact catalog、README、operations runbook、system flow、Reader Brief sections 和 9 个 focused tests。
- 2026-06-10：真实链路在 `shadow_shortlist_id=4378b3ed3fc1be41` 上跑通：events `9ef0fd84c5dd09b6`（185 events、184 READY、data_quality=`PASS_WITH_WARNINGS`）、variants `b236c12cd59aa032`（925 rows、920 READY）、outcome `57c2eb4e71c3320d`（available=3650、pending=50、best_variant=`defensive_limited_adjustment`、data_quality=`PASS_WITH_WARNINGS`）、paper `1c1381cb384fda0e`（variant=`limited_adjustment`、total_return=1.668184、max_drawdown=-0.206274）、regime `67f693aabde09890`、sensitivity `203afb23ef0b387c`（LOW_RISK）、calibration `ad10060ace0a51b7`（REVIEW_ONLY、auto_apply=false）和 bridge `cd97afd8defa8d49`（continue_forward_tracking、target_count=2）。所有 `validate-backtest-sim-*` 已 PASS；simulation 仍固定 no broker / no production / no auto policy apply。
- 2026-06-10：最终本地验证通过：focused pytest 9 passed、`ruff check src tests`、`compileall src tests`、`git diff --check`、YAML parse、`dynamic-v3-rescue validate`、`dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue`、`backtest-sim config-validate`、8 个 `validate-backtest-sim-*` 和全量 pytest 2306 passed。
