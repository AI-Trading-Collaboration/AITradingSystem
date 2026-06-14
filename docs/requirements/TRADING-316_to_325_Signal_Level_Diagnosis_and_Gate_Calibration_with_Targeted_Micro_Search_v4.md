# TRADING-316 to TRADING-325 Signal-Level Diagnosis and Gate Calibration with Targeted Micro Search v4

最后更新：2026-06-14

## 状态

`VALIDATING`

Owner 要求完成附件中的 TRADING-316～325。本阶段承接 TRADING-306～315 的 no-promotion 结论，不继续无差别扩大参数空间，而是先判断 gate、scorecard、signal、consensus 与当前 AI regime 区间是否解释了没有 promotion candidate。

## 背景

TRADING-306～315 已完成 targeted v3 诊断链路。真实输出显示：

- no-promotion review `no-promotion-review_52f5d7e23bda2288`：promoted=0，gate_assessment=`TOO_STRICT`。
- targeted v3 matrix `targeted-search-v3_599f424a15c6114a`：87 variants。
- targeted v3 backfill `targeted-v3-backfill_e7682602c7655f64`：87/87 completed，data quality=`PASS_WITH_WARNINGS`，range=`2022-12-01`～`2026-06-10`。
- promotion threshold sensitivity `promotion-threshold-sensitivity_64c3f26d9962f3e7`：base / relaxed-only 均无 promoted candidates。
- candidate promotion v2 `candidate-promotion-v2_feab730f0e665860`：decision=`RUN_ANOTHER_TARGETED_SEARCH`，promoted=0。
- next formal-or-search plan `next-formal-or-search-plan_8e375e22540acb95`：decision=`CONTINUE_SEARCH_PLAN`，recommended_next_action=`run_smaller_v4_or_signal_level_diagnosis`。

## Safety Boundary

- `research_screening_only=true`
- `experiment_only=true`
- `not_official_target_weights=true`
- `paper_shadow_only=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `auto_apply=false`
- `production_effect=none`

本阶段不得调用 broker、导入 broker data、生成 order ticket、写 official / production target weights、自动 owner approval、自动修改真实仓位、自动切换 paper shadow primary candidate，或修改 `config/etf_portfolio/dynamic_v3_rescue/position_advisory_v1.yaml`。

## Stage Breakdown

|Task|状态|Scope|验收标准|
|---|---|---|---|
|TRADING-316|VALIDATING|Promotion gate calibration review|`gate-calibration-review run/report` 与 `validate-gate-calibration-review` 可运行；输出 gate strictness、component impact、diagnostic relaxed gate；`can_change_official_gate=false`。|
|TRADING-317|VALIDATING|Scorecard component attribution|`scorecard-attribution run/report` 与 validator 可运行；输出 87 rejected variants 的 component distribution、variant matrix 和 family weakness。|
|TRADING-318|VALIDATING|Signal-level instability diagnosis|`signal-instability-diagnosis run/report` 与 validator 可运行；输出 method stability、flip events、regime mismatch events 和 signal-level fix 判断。|
|TRADING-319|VALIDATING|Candidate consensus quality review|`consensus-quality-review run/report` 与 validator 可运行；输出 dispersion summary、ensemble method quality 和 consensus failure reason。|
|TRADING-320|VALIDATING|Near-miss micro search v4 design|`micro-search-v4-design run/report` 与 validator 可运行；生成 20～40 个 v4 variants，每个有 rationale、安全边界和 failure-mode link。|
|TRADING-321|VALIDATING|Micro search v4 backfill|`micro-search-v4-backfill run/report` 与 validator 可运行；完成或明确 partial，输出 performance/regime/stability/signal metrics。|
|TRADING-322|VALIDATING|Gate-calibrated candidate review|`gate-calibrated-review run/report` 与 validator 可运行；official / diagnostic gate 双轨输出，diagnostic gate 不改正式 gate。|
|TRADING-323|VALIDATING|Signal vs parameter failure attribution|`signal-vs-parameter-attribution run/report` 与 validator 可运行；明确 failure_source、confidence、recommended research shift。|
|TRADING-324|VALIDATING|Next research direction decision pack|`next-research-direction run/report` 与 validator 可运行；生成 next decision、next task plan 和 safety boundary。|
|TRADING-325|VALIDATING|Owner research roadmap update|`owner-research-roadmap update/report` 与 validator 可运行；输出 owner summary、checklist、Reader Brief section。|

## Design Decisions

- Gate calibration 只做 diagnostic review，不修改 official gate policy；relaxed gate 输出只可用于 manual review evidence。
- Scorecard attribution 使用现有 v3 backfill / scorecard rows，不新增外部数据源。
- Signal diagnosis 与 consensus review 使用已有 stability / churn / regime / performance metrics 生成可审计 proxy events；若真实逐日信号明细不足，报告必须标注 `INSUFFICIENT_DATA` 或 `MIXED`，不得伪造精细事件。
- v4 micro search 限制为 20～40 variants，聚焦 cash buffer、smoothing、median/top-k consensus、dispersion gate、高分歧 hold/reduce tilt、sideways hold/fast restore。
- v4 backfill 复用 targeted v3 backfill 的 cached data quality gate 与 scorecard path，输出 market regime 和 actual date range。
- 最终 decision pack 必须明确下一步继续 micro search、转向 signal feature diagnosis、candidate quality filter、gate policy review 或 defer。

## Progress Notes

- 2026-06-14: 新增需求文档并进入 `IN_PROGRESS`。实现范围为 TRADING-316～325 全链路、CLI、artifact validators、Reader Brief/report registry/artifact catalog/system flow/runbook/README/task register 更新，以及 focused tests 和 validators。
- 2026-06-14: 实现完成并转入 `VALIDATING`。真实链路输出：gate calibration `gate-calibration-review_0c1045f356f29212`=`REASONABLE` 且 `official_gate_changed=false`；scorecard attribution `scorecard-attribution_00cdfec4c0c6bc30` 覆盖 87 rejected variants，dominant weak components=`signal_churn_score,drawdown_score,regime_score`；signal diagnosis `signal-instability-diagnosis_97ba36dababddfbf` 显示 `requires_signal_level_fix=True` / dominant issue=`signal_churn`；consensus review `consensus-quality-review_ce9fe98ab5c7fea9`=`no_consensus_specific_failure`；v4 design `micro-search-v4-design_aa5cc80ffd2afac4`=24 variants；v4 backfill `micro-search-v4-backfill_2fc70fadfedf5c3f`=`PASS` / `data_quality_status=PASS_WITH_WARNINGS` / range=`2022-12-01`～`2026-06-10` / `latest_valid_as_of=2026-06-12` / completed=24；gate-calibrated review `gate-calibrated-review_154df0d96b37e42a` official promoted=0 / diagnostic promoted=3；signal-vs-parameter attribution `signal-vs-parameter-attribution_bc3faf0785ac1616` failure_source=`SIGNAL_QUALITY` / confidence=`MEDIUM`；next direction `next-research-direction_9ccc4c115ffd44e7`=`SHIFT_TO_SIGNAL_FEATURE_DIAGNOSIS`；owner roadmap `owner-research-roadmap_5c464b4cd4238a49` recommends `continue_forward_confirmation_and_start_signal_diagnosis`。10 个新增 validators、dynamic-v3 root validation、artifact family validation、documentation contract、Reader Brief/quality、focused pytest `10 passed`、ruff、compileall、git diff check 和 full pytest `2477 passed, 640 warnings` 均通过。数据质量限制：v4 backfill 质量门禁为 `PASS_WITH_WARNINGS`，唯一 warning 是当前 price cache sha256 未出现在 download manifest；Marketstack adjusted-close / OHLC / secondary-source 差异均以信息级记录，不改写价格缓存或回测真值。仍保持 no official target / no broker / no production。
