# TRADING-665 to 685 Indicator To Signal Research Framework V1

最后更新：2026-06-20

## 背景

附件要求在继续 B2/B3/B4 或单一指标深度 backfill 前，先建立日报指标、约束和 heuristic 的统一研究控制面。核心风险是：未经验证的上游约束可能长期限制下游 signal 表达，导致后续研究误把“被遮蔽后的边际效果”解释为独立信号强弱。

本任务覆盖 TRADING-665～685，目标是先完成可审计登记、覆盖审计、依赖图、trace contract、mapping/gate 控制面和 valuation/crowding pilot 覆盖审计。该阶段不放宽任何参数，不修改日报实际权重逻辑，不生成 paper-shadow/live/broker/order/official weights。

## 安全边界

- `research_only=true`
- `manual_review_only=true`
- `official_target_weights=false`
- `paper_shadow_activation=false`
- `extended_shadow_allowed=false`
- `live_trading_allowed=false`
- `broker_effect=none`
- `order_effect=none`
- `production_effect=none`

## 阶段拆解

|阶段|任务|状态|验收标准|
|---|---|---|---|
|A|TRADING-665 ontology/schema/validator|VALIDATING|Raw Data、Feature、Indicator、Signal Mapping、Signal、Constraint、Allocation Intent、Research Weight、Outcome 类型可校验；影响权重对象必须有 role/type/mapping 或明确 blocker。|
|A|TRADING-666 daily indicator inventory|VALIDATING|当前日报评分模块、position gates、confidence/data-quality/manual guardrails 均进入 inventory；输出 JSON/Markdown。|
|A|TRADING-667 dependency/dominance graph|VALIDATING|生成 Feature -> Indicator -> Signal Mapping -> Signal -> Constraint/Modifier -> Allocation Intent -> Research Weight -> Outcome 图；支持 CAPS/MASKS/CONDITIONS 等边；无 trace 时 dominance 不伪造数值。|
|A|TRADING-668 multi-stage weight trace contract|VALIDATING|定义 raw_signal_target、pre/post constraint、post execution research weight 和 per-module delta attribution schema。|
|B|TRADING-669 role/target registry|VALIDATING|每个 indicator 绑定 role、target family 和评价指标，禁止全用未来收益评价。|
|B|TRADING-670 PIT/leakage gate|VALIDATING|对每个 indicator 记录 PIT、staleness、revision、lookahead、survivorship gate 状态；未通过不得进入 mapping research。|
|B|TRADING-671 mapping contract/version registry|VALIDATING|mapping_version、family、方向、参数、output range 和 owner/rationale 可审计。|
|B|TRADING-672 mapping candidate library|VALIDATING|生成 M0 informational、M1 linear、M2 quantile、M3 threshold、M4 soft penalty、M5 hard cap、M6 context-aware hypothesis cards；不默认 hard cap。|
|B|TRADING-673 mapping-free diagnostics|VALIDATING|diagnose 命令只输出分布/覆盖/稳定/冗余/forward-association 待办，不生成 weight。|
|B|TRADING-674 calibration/stability baseline|VALIDATING|报告 calibration、monotonicity、trigger rate、state persistence、parameter/window/asset stability 的 required evidence 状态。|
|C|TRADING-675 conditional/incremental effect|VALIDATING|输出 unconditional/conditional/incremental/residualized effect 的 evidence contract，默认标记 `ASSOCIATIONAL_NOT_CAUSAL`。|
|C|TRADING-676 masking/dominance audit|VALIDATING|支持从 multi-stage trace JSON 计算 masking ratio；无 trace 时输出 trace-required limitation；最小日报 trace 已可使 valuation/crowding -> trend pair 不再只输出 `TRACE_DATA_REQUIRED`。|
|C|TRADING-677 factorial planner|VALIDATING|对高影响未验证上游约束和下游 signal 自动生成 Base/A/B/A+B 计划。|
|C|TRADING-678 portfolio transfer attribution|VALIDATING|定义 signal -> raw target -> constrained target -> executed research weight transfer loss contract。|
|D|TRADING-679 trial ledger/multiple testing|VALIDATING|定义 trial family、parameter/window ledger、DSR/PBO/Reality Check/SPA adapter 接口状态。|
|D|TRADING-680 time-series holdout service|VALIDATING|定义 development/diagnostic/holdout、walk-forward、purge/embargo、contamination tracking contract。|
|D|TRADING-681 research gate|VALIDATING|gate 输出附件要求的 taxonomy，并对被高影响未验证上游约束遮蔽的下游结论条件化。|
|E|TRADING-682 campaign adapter|VALIDATING|声明 generic indicator research adapter contract，先以 research-only/audit-only 方式接入 Campaign adapter registry。|
|E|TRADING-683 CLI/status/Reader Brief|VALIDATING|`aits research indicators inventory|coverage|coverage-gap|graph|diagnose|mapping-plan|masking|gate|masking-casebook|ablation-validation|historical-trace-validation|gate-availability-audit|component-historical-trace|backtest-trace-bridge|validation-pack|validation-pack-stability` 可运行。|
|E|TRADING-684 valuation/crowding pilot|VALIDATING|迁移当前 valuation/crowding 指标，输出 `VALUATION_CROWDING_UNTESTED_HIGH_IMPACT` 或 coverage-known 结论；不改参数。|
|E|TRADING-685 validation pack|VALIDATING|汇总 ontology、inventory、graph、trace、mapping、diagnostics、masking/factorial、trial/holdout、adapter、pilot 和 safety boundary。|
|F|historical sample expansion / gate root-cause|VALIDATING|gate audit 输出 date+asset blocked cases、explicit lineage manifest proof、root-cause class、repairability、event window tags、asset universe/date/case counts；目标 audited_date_count>=30、component_validation_trace_eligible_count>=20，full advisory 只在生产等价 lineage proof 完整时成立。|

## 当前实现策略

1. 新增 `config/research/indicator_research_registry.yaml` 作为指标、mapping、dependency、trace 和 gate policy 的 source configuration。
2. 新增 `src/ai_trading_system/indicator_research.py` 提供 schema validation、inventory、coverage、graph、trace contract、日报 multi-stage weight trace、coverage gap、diagnostics、mapping plan、masking audit、casebook、ablation、historical trace validation、gate availability audit、component-level historical trace、backtest trace bridge、gate、validation pack 和 rerun stability builders。
3. 在 `aits research indicators ...` 下接入 CLI，默认写出 `outputs/research_indicators/` 下的 JSON/Markdown artifacts。
4. `aits score-daily` 在既有 `DailyScoreReport` 生成后写出只读 `daily_indicator_weight_trace_YYYY-MM-DD.json/md`，覆盖 raw indicator value、normalized indicator score、mapped signal contribution、pre/post constraint signal weight 和 final advisory/portfolio-facing weight；不回写评分输入、不改变 production weight logic。
5. 默认 valuation/crowding pilot 只做 coverage/hit attribution/masking audit contract；没有 multi-stage trace 时必须输出 `TRACE_DATA_REQUIRED` limitation。
6. 同步 `docs/system_flow.md`、`docs/artifact_catalog.md`、Campaign adapter registry 和测试。
7. 本轮继续扩展 historical trace diagnostics：gate availability audit 必须按 blocked date+asset 输出 `blocked_gate`、`missing_or_late_feature`、`feature_available_time`、`decision_time`、`reason_class` 和 `can_be_repaired_without_relaxing_production_gate`；historical replay / lineage manifest 必须显式记录 `source_artifact_path`、`generated_at`、`as_of_date`、`decision_time`、`config_hash`、`input_snapshot_hash`、`trace_contract_version` 和 `production_equivalent`，不得用目录名推断 full advisory equivalence。
8. 本轮扩样目标覆盖 date range、event window 与 asset universe 参数；报告必须同时披露 `date_count`、`asset_count`、`case_count`，并把 partial/component/backtest bridge 样本固定标记为 `promotion_gate_allowed=false`、`allowed_uses=[diagnostic, ablation, sensitivity_analysis]`、`trace_source` 和 `confidence`。

## 开放限制

- 当前 expanded historical multi-stage trace 覆盖 2026-05-20～2026-06-18 的 22 个 full advisory equivalent 日期（352 trace rows），gate availability audit 覆盖 2026-04-24～2026-06-18 的 40 个 audited dates、8 个资产、320 date+asset cases，其中 full advisory trace eligible=22、component validation trace eligible=28、partial component-only=6。2026-05-18/19 以及更早 fail-closed 日期仍因 SEC PIT feature available_time 晚于 decision_time 或缺 replay/gate lineage 不能进入 full advisory equivalence；不得把 masking ratio 或 ablation 差异解释为独立因果结论。
- 当前不会运行大规模 indicator search，也不会访问 untouched holdout。
- 当前不会把 valuation/crowding hard cap 变更为 soft cap、context-aware cap 或任何新参数。

## 进展记录

- 2026-06-20：新增任务与需求文档，进入 `IN_PROGRESS`。本轮先实现 research-only/audit-only 控制面与 valuation/crowding pilot 覆盖审计。
- 2026-06-20：实现完成并进入 `VALIDATING`。新增 `config/research/indicator_research_registry.yaml`、`src/ai_trading_system/indicator_research.py`、`aits research indicators ...` CLI、Research Campaign validation-only adapter contract、valuation/crowding pilot audit、validation pack、system flow/artifact catalog 同步和 focused tests。实际 validation pack 输出 `INDICATOR_TO_SIGNAL_RESEARCH_FRAMEWORK_V1_READY_WITH_LIMITATIONS`，原因是当前没有真实 multi-stage weight trace，dominance/masking 只能输出 `TRACE_DATA_REQUIRED` 而不能计算 realized impact。
- 2026-06-20：继续 `VALIDATING`。Owner 要求运行全量 `pytest -q`、检查 validation-pack rerun 稳定性、接入最小日报 multi-stage weight trace、重跑 dominance/masking diagnostics，并生成 coverage gap report。首次全量 `pytest -q` 在 collection 阶段失败 4 errors：`tests/test_validation_tier_script.py`、`tests/trading_engine/test_daily_weight_adjustment_scheduler_dry_run.py`、`tests/trading_engine/test_ibkr_paper_order_lifecycle.py`、`tests/trading_engine/test_ibkr_paper_readonly.py` 导入 `scripts.*` 失败，原因是当前 pytest pythonpath 未包含 repo root；该问题属于测试运行配置，不涉及 paper-shadow/live/broker/order/official weights 或生产权重逻辑。
- 2026-06-20：继续 `VALIDATING`。修正 pytest import path 后，全量 `pytest -q` 第一次执行到 100%，结果 `2952 passed, 1 failed, 429 warnings`；失败为 Python 3.11 测试环境缺少项目 dev optional dependency `pyarrow>=13.0.0`，导致 `string[pyarrow]` 测试无法构造。安装 Python 3.11 的 `pyarrow 24.0.0` 后，目标失败单测通过，第二次全量 `pytest -q` 输出 `2953 passed, 639 warnings in 1691.75s`。
- 2026-06-20：完成最小日报 multi-stage weight trace 接入。隔离运行 `score-daily --as-of 2026-06-18 --skip-risk-event-openai-precheck --sec-fundamental-source sec_pit_feature_panel`，所有输出写入 `outputs/research_indicators/daily_trace_validation/`，prediction ledger 使用 `candidate_id=indicator_trace_validation` 且 `production_effect=none`。`daily_indicator_weight_trace_2026-06-18.json` 状态 `PASS`，row_count=16，component_row_count=6，constraint_row_count=7，missing_trace_field_record_count=0。
- 2026-06-20：用该 trace 重跑 diagnostics。`indicator_masking_and_dominance_audit_trend_strength_indicator` 状态 `PASS`，trace_required_pair_count=0，valuation/crowding -> trend masking_ratio=0.80，masking_status=`HIGH_MASKING`，conclusion_status=`B_EFFECT_MASKED_BY_A`；`valuation_crowding_pilot_audit` 输出 `VALUATION_CROWDING_RESEARCH_COVERAGE_KNOWN`，仍保留 coverage_status=`HIGH_IMPACT_UNVALIDATED` 且 parameter_mutation=false。
- 2026-06-20：生成 coverage gap report 和 validation stability。`daily_indicator_coverage_gap_report` 状态 `PASS_WITH_WARNINGS`，registered_indicator_count=11，unregistered_daily_indicator_count=0，registered_incomplete_indicator_count=1（`data_quality_gate_indicator` 缺 mapping spec），high_impact_unvalidated_count=1（`valuation_crowding_indicator`）。`validation-pack --trace-path ...` 输出 `INDICATOR_TO_SIGNAL_RESEARCH_FRAMEWORK_V1_READY_WITH_LIMITATIONS`，artifacts=20；`validation-pack-stability --trace-path ...` 输出 `PASS`，stable=true，artifact_count=20。
- 2026-06-20：继续 `VALIDATING` 后续诊断。Owner 要求只补 registry/research metadata 中的 `data_quality_gate_indicator` mapping spec，不改变日报权重逻辑；启动 valuation/crowding high-impact pilot validation report；把 masking diagnostics 扩展为 casebook；增加 baseline / no valuation-crowding masking / capped masking 三种只读 ablation；把 multi-stage trace 扩展到 historical replay/backtest validation；validation pack stability 继续检查 artifact count、trace fields、coverage gap、HIGH_IMPACT_UNVALIDATED 清单和 masking diagnostics repeatability；补 focused tests。所有新增输出仍固定 `production_effect=none`，不得提升任何指标到 official/paper-shadow 权重影响层。
- 2026-06-20：完成后续诊断实现并重跑 artifacts。补齐 `data_quality_gate_indicator` 的 `data_quality_gate_v1` mapping spec，明确为 immutable fail-closed hard block，不是可调 penalty/confidence modifier；`daily_indicator_coverage_gap_report` 变为 registered=11、unregistered=0、incomplete=0、HIGH_IMPACT_UNVALIDATED=1。新增 `valuation_crowding_pilot_validation_report`、`indicator_masking_casebook_valuation_crowding_trend`、`valuation_crowding_ablation_validation` 和 `historical_multi_stage_weight_trace_validation`；validation pack artifact_count=23，stability `PASS`、stable=true，trace_fields_complete=true、coverage_gap_unregistered=true、high_impact_unvalidated=true、masking diagnostics/casebook repeatable=true。用 PIT sliced price/rate inputs 生成 2026-05-29、2026-06-05、2026-06-12 historical score-daily traces，并与 2026-06-18 trace 合并为 `outputs/research_indicators/historical_trace_validation/historical_multi_stage_weight_trace_2026-05-29_to_2026-06-18.json`（64 rows、4 dates、missing_trace_field_record_count=0）；casebook case_count=4，ablation scenarios=3，historical validation 因 date_count=4<20 保持 `PASS_WITH_WARNINGS`。更早 2026-04-24、2026-05-01、2026-05-08、2026-05-15 replay 被 `sec_edgar_reconstructed_pit_features` available_time 晚于 decision_time 的 feature availability gate 阻断，未绕过。
- 2026-06-20：继续 historical trace expansion。Owner 要求新增 gate availability audit，拆分 `full_advisory_trace_eligible` 与 `component_validation_trace_eligible`，在不放宽 production data_quality_gate 的前提下为 valuation/crowding -> trend masking 生成 component-level historical trace，从已有 backtest / historical simulation / advisory outcome artifacts 导出 backtest trace bridge，并支持 date range、event window、asset universe 参数。所有非 full advisory trace 必须标记 `trace_source`、`confidence`、`promotion_gate_allowed=false`，只允许用于 diagnostic / ablation / sensitivity analysis。
- 2026-06-20：完成 historical trace expansion 实现并重跑 artifacts。新增 `historical_trace_gate_availability_audit`、`component_level_historical_trace`、`backtest_trace_bridge`，并为 casebook、ablation、historical validation、validation pack/stability 增加 `start_date`、`end_date`、`event_window_start`、`event_window_end`、`asset_universe` 参数。实际 gate audit：audited_date_count=8、full_advisory_trace_eligible_count=3、component_validation_trace_eligible_count=7、not_full_eligible_count=5；component trace：trace_row_count=12、date_count=4、eligible_dates=3、component_eligible_dates=7、partial_component_only_count=4；backtest trace bridge：bridge_record_count=4、source_artifact_count=3。validation pack `INDICATOR_TO_SIGNAL_RESEARCH_FRAMEWORK_V1_READY_WITH_LIMITATIONS`、artifacts=26；validation-pack-stability `PASS`、stable=true，新增 gate/sample/component/bridge repeatability 字段均为 true。验证通过 focused tests 20 passed、Ruff、compileall、task-register consistency run/validate、`git diff --check` 和全量 `pytest -q` 2961 passed、640 warnings、1805.89s。所有 component/bridge/non-full advisory records 均标记 `trace_source`、`confidence`、`promotion_gate_allowed=false`，用途限制为 diagnostic / ablation / sensitivity analysis；未修改 production weight logic。
- 2026-06-20：继续 `VALIDATING` 样本扩张与 gate root-cause analysis。Owner 要求先提交并推送已完成 historical trace expansion（commit `TRADING-665-685 add historical trace expansion and eligibility audit`），随后补 blocked date root-cause 分类、explicit historical replay/lineage manifest、>=30 date audit、event window sampling、QQQ/SPY/SMH/MSFT/GOOGL asset universe 扩样、casebook/ablation sample quality breakdown，并继续禁止 promotion gate、paper-shadow/live/broker/order/official weights 和 production weight logic 变更。
- 2026-06-20：完成样本扩张与 gate root-cause analysis 实现并复验。新增 explicit historical replay lineage manifest proof、date+asset root-cause fields、event window coverage、asset universe expansion、casebook/ablation hit-rate 与 sample_quality_breakdown、backtest bridge source lineage；full advisory equivalence 只在 trace、data quality、feature availability 与 `production_equivalent=true` lineage manifest 同时满足时成立。生成 expanded trace `outputs/research_indicators/historical_trace_validation/historical_multi_stage_weight_trace_2026-05-18_to_2026-06-18_expanded.json`：source_trace_count=22、row_count=352、date_count=22。Gate audit：audited_date_count=40、asset_count=8、case_count=320、component_validation_trace_eligible_count=28、full_advisory_trace_eligible_count=22、root_cause_case_count=144。Casebook / ablation / bridge 均扩到 176 cases；validation pack artifact_count=26；validation-pack-stability `PASS`、stable=true。验证通过 focused 并行 pytest 32 passed、Ruff、compile、task-register consistency run/validate、`git diff --check` 和 full 并行 validation tier 2963 passed、643 warnings、146.75s。所有新增 artifacts 继续 `production_effect=none`，未修改 production weight logic 或任何 paper-shadow/live/broker/order/official weights。
