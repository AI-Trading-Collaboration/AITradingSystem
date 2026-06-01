# TRADING-064 ETF Portfolio Calibration Experiment Pack

最后更新：2026-06-01

## 背景

TRADING-062 已完成 ETF Portfolio Allocation System baseline，TRADING-063 已完成 ETF Portfolio Credibility Validation foundation，并通过 `aits etf credibility validate`。TRADING-064 的目标是在可信 ETF allocation framework 上建立受控、可复现、可治理的参数校准实验包，用于识别值得进入 shadow observation 的候选配置。

本阶段不新增 live broker trading、real-money order generation、automatic production promotion、LLM/news/EDGAR text production weight input、options strategy execution 或 ML model replacement。

## 安全边界

所有 TRADING-064 experiment、报告和 candidate 输出必须固定：

- `observe_only=true`
- `production_effect=none`
- `broker_action=none`
- `manual_review_required=true`

任何 candidate 只能进入 shadow observation 或人工复核，不得自动替换 production baseline，不得写入正式 ETF target weights，不得触发 broker action。

## 实施顺序

|子任务|状态|范围|
|---|---|---|
|TRADING-064A|DONE|Experiment Config Registry|
|TRADING-064B|DONE|Baseline Parameter Grid Definition|
|TRADING-064C|DONE|Batch Backtest Runner|
|TRADING-064D|DONE|Experiment Comparison Report|
|TRADING-064E|DONE|Risk/Return/Turnover Ranking|
|TRADING-064F|DONE|Candidate Selection Gate|
|TRADING-064G|DONE|Shadow Portfolio Enrollment|
|TRADING-064H|DONE|Weekly Experiment Review Report|
|TRADING-064I|DONE|Reader Brief / Reports Index Integration|
|TRADING-064J|DONE|Final Experiment Pack Validation|

## 验收标准

- `config/etf_portfolio/experiments.yaml` 存在并可验证。
- `etf_calibration_v1` experiment pack 存在且只包含受控 first matrix。
- Batch runner 可按 pack 或单个 experiment 运行，输出 manifest 和结果 schema。
- Comparison report 同时展示 baseline、benchmarks、risk、turnover、stability 和 candidate status。
- Ranking policy 明确，不按历史收益单一排序。
- Candidate gate fail closed，unsafe candidate 不能进入 shadow。
- Shadow enrollment 只写 ignored runtime state，保留 manual review 和 observe-only 边界。
- Weekly review 不允许 production promotion。
- Reader Brief / report index 可以发现最新 experiment 状态，并披露 safety status。
- Final validation gate 能验证完整 TRADING-064 基础设施，且不破坏 TRADING-063 credibility gate。

## 状态记录

- 2026-06-01: TRADING-064 新增并进入 IN_PROGRESS。当前从 TRADING-064A 开始，目标是新增受控 experiment config registry、loader/validator、文档和测试；该 registry 只定义允许观察的参数实验，不运行回测、不改变正式 allocation、不产生 broker action。
- 2026-06-01: TRADING-064A 完成。新增 `config/etf_portfolio/experiments.yaml`、`src/ai_trading_system/etf_portfolio/experiments.py` 和 `tests/test_etf_experiments.py`；registry 覆盖 16 个 first-matrix experiment，强制 experiment id 唯一、base weights 合计为 1、base config ref 可解析、override key 受控，以及 `observe_only=true`、`production_effect=none`、`broker_action=none`、`manual_review_required=true`。文档同步到 README、artifact catalog 和 system flow。下一步进入 TRADING-064B `etf_calibration_v1` pack definition。
- 2026-06-01: TRADING-064B 进入实现。目标是新增 `config/etf_portfolio/experiment_packs.yaml`、pack loader/validator 和测试，确保 `etf_calibration_v1` 只引用 registry 中的安全 experiment，不允许重复 experiment、不允许缺 ranking/promotion policy，也不引入 uncontrolled combinatorial search。
- 2026-06-01: TRADING-064B 完成。新增 `etf_calibration_v1` pack，包含 base allocation、regime multiplier、semiconductor cap、rebalance threshold 和 relative strength weight 五个 family 的 16 个受控实验；pack 声明 `risk_adjusted_v1` ranking policy 和 `shadow_only_manual_review` promotion policy，并固定 `observe_only=true`、`production_effect=none`、`broker_action=none`、`manual_review_required=true`。测试覆盖 pack load、experiment ref、重复 experiment、unsafe experiment、missing ranking policy 和 missing promotion policy。下一步进入 TRADING-064C batch backtest runner。
- 2026-06-01: TRADING-064C 完成。`aits etf experiments run` 保留旧 `--config` candidate registry 行为，并新增 `--pack/--experiment --start --end` batch backtest path；batch runner 复用 ETF data quality gate 和 backtest engine，按 run directory 写出 `run_manifest.json`、`experiment_results.json`、`benchmark_results.json`、`metrics_summary.json` 和 `diagnostics_summary.json`。单个 experiment failure 进入 diagnostics，不被静默吞掉；unsafe experiment/pack 在运行前 fail closed。测试覆盖单实验运行、pack run、manifest/schema 文件、失败隔离、unsafe blocking 和 CLI smoke。下一步进入 TRADING-064D comparison report。
- 2026-06-01: TRADING-064D 进入实现。目标是在 `aits etf experiments compare --run-id <run_id>` / `--latest` 上读取 064C run output，生成 JSON/Markdown comparison report，包含 run metadata、experiment list、baseline/benchmark context、metrics/risk/turnover/stability/constraint hit summary、warning/rejection summary 和 ranking policy 待实现状态；不得按 return-only 排序。
- 2026-06-01: TRADING-064D 完成。新增 batch run comparison builder、JSON/Markdown renderer 和 CLI `aits etf experiments compare --run-id/--latest`；comparison report 包含 run metadata、experiment list、baseline context、benchmark context、metrics table、risk table、turnover/stability table、constraint hit summary、warning summary 和 `PENDING_TRADING_064E_RISK_ADJUSTED_V1` ranking 状态。缺 baseline 或失败 experiment 的指标保持 null 并输出 reason，不按 total return 排名。测试覆盖 run output load、baseline/benchmark context、Markdown/JSON 输出、missing metrics null reason、schema stability 和 CLI latest smoke。下一步进入 TRADING-064E ranking policy。
- 2026-06-01: TRADING-064E 进入实现。目标是把 `risk_adjusted_v1` ranking policy 的 component weights、阈值、component scale 和 hard rejection rules 放入 `config/etf_portfolio/experiment_packs.yaml`，并在 comparison report 上计算 candidate score、component scores、hard rejection flags 和 ranking reasons；high-return 但 unstable/unsafe/missing benchmark 的 candidate 必须被拒绝。
- 2026-06-01: TRADING-064E 完成。`risk_adjusted_v1` policy 已在 `experiment_packs.yaml` 中治理 component weights、component scales、turnover/drawdown thresholds 和 hard rejection rules；comparison report 对 pack run 自动应用 ranking，输出 `candidate_score`、benchmark excess、drawdown reduction、risk-adjusted return、turnover penalty、stability component score、hard rejection flags 和 ranking reasons。测试覆盖 component score、drawdown reduction 改善排序、excessive turnover hard rejection、missing benchmark rejection、unsafe production effect rejection、determinism 和 report ranking policy application。下一步进入 TRADING-064F candidate selection gate。
- 2026-06-01: TRADING-064F 进入实现。目标是实现 shadow-only candidate selection gate，消费 `risk_adjusted_v1` ranked candidates 并输出 `eligible_for_shadow` / `rejected` / `needs_more_data` / `blocked`，固定 `production_promotion_allowed=false`，不得让 ranking 直接变成 production change。
- 2026-06-01: TRADING-064F 完成。`shadow_only_manual_review` promotion policy 已在 `experiment_packs.yaml` 中治理 `min_candidate_score`、blocked hard rejection、rejected hard rejection、`shadow_observation_allowed=true` 和 `production_promotion_allowed=false`；新增 `aits etf experiments select-candidates --run-id/--latest`，输出 `candidate_selection_report.json/md`。Candidate gate 消费 ranked comparison，按 policy 生成 `eligible_for_shadow`、`needs_more_data`、`rejected` 或 `blocked`，缺 ranking、missing benchmark、credibility failure、unsafe production effect 或 missing manual review 均 fail closed，不写 production target weights、不触发 broker action。测试覆盖 eligible shadow、score too low、missing benchmark blocked、high turnover rejected、missing ranking blocked、JSON/Markdown 输出和 CLI smoke。下一步进入 TRADING-064G shadow portfolio enrollment。
- 2026-06-01: TRADING-064G 进入实现。目标是新增 observe-only shadow enrollment registry，仅读取 `eligible_for_shadow` candidate selection，写入 ignored runtime state `data/simulation/etf_shadow_candidates.json`；重复登记必须确定性更新，不得写 production ETF target weights 或触发 broker action。
- 2026-06-01: TRADING-064G 完成。新增 `aits etf experiments enroll-shadow --run-id/--latest --candidate <candidate_id>` / `--top N`，从 candidate selection gate 中只登记 `eligible_for_shadow` candidate，并写入 ignored runtime registry `data/simulation/etf_shadow_candidates.json`。每条记录包含 `shadow_id`、`candidate_id`、`experiment_id`、`source_run_id`、`enrolled_at`、`model_version`、`config_hash`、`start_date`、`status`、safety fields 和 evaluation schedule；重复登记同一 `shadow_id` 不追加重复记录，非 eligible 或 unsafe candidate fail closed。测试覆盖 observe-only record、duplicate determinism、unsafe block、noneligible block、runtime path ignored 和 CLI smoke。下一步进入 TRADING-064H weekly experiment review report。
- 2026-06-01: TRADING-064H 完成。新增 `weekly_shadow_review_v1` review policy 和 `aits etf experiments weekly-review --as-of/--latest`；周度报告读取 shadow registry 与 source experiment run metrics，输出 review period、active shadow candidates、candidate/baseline/QQQ forward returns、drawdown、turnover、weight stability、constraint hits、candidate status change、recommended action 和 manual review notes。允许动作限定为 `continue_shadow`、`needs_more_data`、`reject_candidate`、`promote_to_longer_observation`，固定 `production_promotion_allowed=false`，不写 production weights 或 broker action。测试覆盖 shadow candidate load、forward metrics、baseline/benchmark comparison、action assignment、no production promotion、JSON/Markdown 输出和 CLI smoke。下一步进入 TRADING-064I Reader Brief / reports index integration。
- 2026-06-01: TRADING-064I 进入实现。目标是把 experiment run manifest、comparison report、candidate selection report、shadow enrollment registry 和 weekly review report 接入 report registry / Reader Brief，只读展示最新 pack、top candidate、rejected count、active shadow candidates、safety status 和详细报告链接；缺失 artifact 必须优雅降级。
- 2026-06-01: TRADING-064I 完成。`config/report_registry.yaml` 已登记 experiment run manifest、comparison report、candidate selection report、shadow registry 和 weekly review；Reader Brief 新增 `ETF Calibration Experiments` 区块，只读展示最新 `etf_calibration_v1` pack、top candidate、rejected/blocked count、active shadow candidates、weekly review action、detail report 和 `observe_only=true; production_effect=none; broker_action=none` safety status。缺失 experiment artifacts 时显示 `MISSING`，不运行 experiments、backtest、shadow enrollment 或 weekly review。测试覆盖 report registry 条目、Reader Brief payload/HTML safety status 和缺失 artifact 降级。下一步进入 TRADING-064J final experiment pack validation。
- 2026-06-01: TRADING-064J 进入实现。目标是新增 `aits etf experiments validate --pack etf_calibration_v1` final validation gate，聚合 experiment registry、pack registry、batch runner、comparison/ranking、candidate gate、shadow enrollment、weekly review、report registry / Reader Brief 集成、P2/live production-input block 和 safety fields，输出 PASS/FAIL JSON/Markdown，作为 TRADING-064 baseline 完成前的最终门禁。
- 2026-06-01: TRADING-064J 完成。新增 `aits etf experiments validate --pack etf_calibration_v1`，输出 `reports/etf_portfolio/experiments/validation/*_experiment_validation.json/md`，逐项验证 experiment registry、pack、batch runner、comparison report、`risk_adjusted_v1` ranking policy、`shadow_only_manual_review` candidate gate、shadow enrollment runtime safety、`weekly_shadow_review_v1` weekly review、report registry / Reader Brief 可见性、safety fields 和 P2/live production-input block。命令在当前配置下 PASS，固定 `production_effect=none`、`broker_action=none`、`manual_review_required=true`、`production_promotion_allowed=false`；失败场景覆盖 unsafe experiment、missing ranking policy、missing candidate gate 和 unsafe pack production effect。TRADING-064A~J baseline implementation 完成，下一步进入真实 experiment/shadow artifacts 的验证观察和 owner review。
- 2026-06-01: 推送后 GitHub Actions Test 仍失败，但未认证日志只暴露 exit code，无法定位失败用例。CI workflow 改为写出 `pytest-results.xml`，并在失败时把 JUnit failure/error case 转成 GitHub check annotations；该诊断只影响 CI 可观测性，不改变项目运行逻辑、测试断言、报告输出或投资解释。
- 2026-06-01: CI annotations 定位剩余失败为 Linux runner 差异：`Path().glob()` 不支持绝对 glob pattern，导致 report index 扫描 ETF backtest summary 失败；Rich/Typer help 在 Ubuntu runner 的默认宽度/ANSI 输出下裁剪了选项文本，导致两个 help 文本断言误报。已将 report index glob 切换为 `glob.glob`，并让相关 CLI help 测试使用无颜色、固定宽度输出。
