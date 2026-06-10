# TRADING-161 to TRADING-168: Backtest Simulation Advisory Evaluation

最后更新：2026-06-10

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
|TRADING-161|Backtest Simulation Config and Advisory Event Generator|VALIDATING|新增 `config/etf_portfolio/dynamic_v3_rescue/backtest_simulation_advisory_v1.yaml`；`backtest-sim config-validate`、`backtest-sim event-generate/report` 和 `validate-backtest-sim-events` 可运行；事件只使用当前 shadow shortlist/advisory 规则生成模拟 advisory observations，并披露非 PIT 限制。|
|TRADING-162|Simulated Advisory Variants|VALIDATING|`backtest-sim variants-generate/report` 和 `validate-backtest-sim-variants` 可运行；生成 `no_trade`、`consensus_target`、`limited_adjustment`、`defensive_limited_adjustment` 和 `equal_weight_shadow_candidates`；调仓限制来自配置。|
|TRADING-163|Simulated Outcome Windows|VALIDATING|`backtest-sim outcome-run/report` 和 `validate-backtest-sim-outcome` 可运行；按 1/5/10/20 trading-day windows 输出 AVAILABLE/PENDING/INSUFFICIENT_DATA；缓存数据必须先通过质量门禁。|
|TRADING-164|Historical Paper Portfolio v2|VALIDATING|`backtest-sim paper-run/report` 和 `validate-backtest-sim-paper` 可运行；重建历史 paper-only state、ledger 和 performance summary；不触发 broker 或真实 portfolio mutation。|
|TRADING-165|Regime-Specific Simulation Review|VALIDATING|`backtest-sim regime-review/report` 和 `validate-backtest-sim-regime` 可运行；按 `ai_trend`、`tech_drawdown`、`semiconductor_pullback`、`risk_off` 等 regime 汇总表现和样本覆盖。|
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
3. outcome 计算使用 cached market data，并在运行时先执行与 `aits validate-data` 相同的数据质量门禁；失败时停止。
4. variant、paper、regime、sensitivity、calibration 和 bridge artifacts 均固定 no broker / no production / no auto policy mutation。
5. 所有影响投资解释的阈值来自 `backtest_simulation_advisory_v1.yaml`；代码中的数值只允许作为 schema/格式/空样本默认值。
6. sensitivity 的高风险或样本不足状态必须阻止 strong calibration proposal，只允许进入 owner review / forward confirmation。

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

- 2026-06-10：任务登记与需求文档创建，进入实现阶段。下一步完成 CLI wiring、validators、focused tests、README、operations runbook、system flow、report registry、artifact catalog 和真实链路验证。
- 2026-06-10：baseline 实现完成并进入 VALIDATING。新增 backtest simulation config、核心模块、`backtest-sim` CLI、8 个 root validators、report registry、artifact catalog、README、operations runbook、system flow、Reader Brief sections 和 9 个 focused tests。
- 2026-06-10：真实链路在 `shadow_shortlist_id=4378b3ed3fc1be41` 上跑通：events `9ef0fd84c5dd09b6`（185 events、184 READY、data_quality=`PASS_WITH_WARNINGS`）、variants `b236c12cd59aa032`（925 rows、920 READY）、outcome `57c2eb4e71c3320d`（available=3650、pending=50、best_variant=`defensive_limited_adjustment`、data_quality=`PASS_WITH_WARNINGS`）、paper `1c1381cb384fda0e`（variant=`limited_adjustment`、total_return=1.668184、max_drawdown=-0.206274）、regime `67f693aabde09890`、sensitivity `203afb23ef0b387c`（LOW_RISK）、calibration `ad10060ace0a51b7`（REVIEW_ONLY、auto_apply=false）和 bridge `cd97afd8defa8d49`（continue_forward_tracking、target_count=2）。所有 `validate-backtest-sim-*` 已 PASS；simulation 仍固定 no broker / no production / no auto policy apply。
- 2026-06-10：最终本地验证通过：focused pytest 9 passed、`ruff check src tests`、`compileall src tests`、`git diff --check`、YAML parse、`dynamic-v3-rescue validate`、`dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue`、`backtest-sim config-validate`、8 个 `validate-backtest-sim-*` 和全量 pytest 2306 passed。
