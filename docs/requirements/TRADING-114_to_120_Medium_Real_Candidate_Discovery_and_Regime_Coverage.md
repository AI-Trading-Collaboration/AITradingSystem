# TRADING-114 to TRADING-120 Medium Real Candidate Discovery and Regime Coverage

最后更新：2026-06-08

## 状态

`VALIDATING`

## 背景

TRADING-111 to 113 已经让真实参数研究证据闭环能够 fail closed，但当前仍缺少面向
`medium_real` 的候选发现、regime 覆盖解释、人工观察池和是否值得启动
`overnight_real` 的统一研究结论。没有这一层时，人工 review 只能在 sweep leaderboard、
promotion pack 和多个底层证据 artifact 之间跳转，容易把单一 AI 牛市窗口或缺证据的候选
误读为可推广结论。

本阶段不改变 production 权重、不触发 broker action、不自动 enroll shadow，也不生成
`production_candidate`。目标是把 `medium_real` 真实候选搜索结果整理成可审计、可验证、
可供 owner 复核的研究包，并明确下一步是否继续 `overnight_real`。

## 子任务拆解

|ID|目标|状态|验收|
|---|---|---|---|
|TRADING-114|Candidate Evidence Summary Matrix|VALIDATING|生成候选级 evidence matrix，汇总 data audit、window audit、weight path、candidate attribution、overfit 和 promotion status；缺证据 fail closed。|
|TRADING-115|Medium Real Profile Execution and Validation|VALIDATING|`medium_real` profile 至少尝试 300 个 real candidates，或明确解释参数空间不足、candidate errors 或 owner 停止原因；报告 runtime、artifact size、reject 分布和 next action。|
|TRADING-116|Regime Coverage Gap Report|VALIDATING|以 `ai_after_chatgpt` 为默认 regime，检测 AI bull、drawdown、sideways/high-vol/recovery 和 semiconductor stress windows，披露缺口与 overfit risk。|
|TRADING-117|Candidate Interpretation Pack|VALIDATING|为 top 候选输出参数、证据、turnover/weight path、drawdown protection、regime behavior 和 manual review notes。|
|TRADING-118|Observe-only Candidate Pool|VALIDATING|只把证据完整且无 hard blockers 的候选写入 observe-only pool；默认不写 shadow registry，不触发 enrollment。|
|TRADING-119|Overnight Real Readiness Gate|VALIDATING|评估 projected runtime、artifact size、failure rate、evidence completeness 和 top-candidate stability；不自动启动 `overnight_real`。|
|TRADING-120|Research Decision Report and Next Codex Task|VALIDATING|把 evidence、medium_real、regime、interpretation、observe pool 和 readiness 汇总为 owner-readable decision report，并给出下一步 Codex task。|

## 设计原则

- 默认市场 regime 是 `ai_after_chatgpt`，anchor event 为 2022-11-30，默认研究窗口从
  2022-12-01 开始；报告必须披露实际 requested / resolved date range。
- `medium_real`、`overnight_real` 都是 owner-requested manual research workflow，不是
  daily scheduler entry。daily-run 仍只能调用 lightweight `schedule observe` gate。
- `medium_real` 的结论必须先通过 cached data quality gate 或同等直接校验；候选 metrics
  必须来自 linked `real_dynamic_v3_rescue` artifact，不得使用 tiny fixture proxy。
- Candidate evidence、regime coverage、interpretation pack、observe pool、overnight
  readiness 和 research decision 都固定 `production_effect=none`、`broker_action=none`、
  `manual_review_required=true`。
- 缺 data/window/weight/attribution/overfit evidence 时只能进入 `review_required`、
  `observe_only` 或 blocked 状态，不能进入 automatic promotion。
- `medium_real` 执行优化只能复用同一数据 manifest、同一 date range 和同一 policy hash 下
  不随 sweep candidate 参数变化的 deterministic evidence；不得减少 hard gate、sensitivity、
  overfit、window audit、weight path 或 data quality evidence。任何分阶段/降证据运行模式必须
  另行登记并在报告中显式披露。

## Pilot Threshold Governance

本阶段新增的候选证据分、regime 分类、drawdown protection 容忍度和 overnight readiness
阈值是 `pilot_baseline`，用于先形成可审计闭环，而不是长期投资政策。相关阈值以命名常量
实现，支持测试覆盖和报告披露：

- `EVIDENCE_SCORE_POINTS`、`EVIDENCE_USABLE_SCORE_FLOOR`、`EVIDENCE_REVIEW_SCORE_FLOOR`
- `REGIME_DRAWDOWN_THRESHOLD`、`SEMICONDUCTOR_DRAWDOWN_THRESHOLD`
- `STRONG_TREND_RETURN_THRESHOLD`、`SIDEWAYS_ABS_RETURN_THRESHOLD`
- `HIGH_VOL_ANNUALIZED_THRESHOLD`、`STRONG_RECOVERY_RETURN_THRESHOLD`
- `REGIME_DRAWDOWN_PROTECTION_MAX_DEGRADATION_PP`
- `OVERNIGHT_TARGET_CANDIDATES`、`OVERNIGHT_READY_MAX_HOURS`
- `OVERNIGHT_WARNING_MAX_HOURS`、`OVERNIGHT_READY_MAX_FAILURE_RATE`
- `OVERNIGHT_WARNING_MAX_FAILURE_RATE`、`OVERNIGHT_WARNING_MAX_ARTIFACT_GB`

退出条件：完成至少一轮 `medium_real` 真实运行和 owner 人工复核后，把仍影响投资解释的阈值
迁移到 `config/etf_portfolio/dynamic_v3_rescue/parameter_governance_v1.yaml` 或新的
policy manifest，补充 owner、version/status、rationale、validation evidence 和 review
condition；如果真实样本证明阈值过宽或过窄，新增后续 task 而不是静默调整。

## CLI 合同

新增或扩展：

- `aits etf dynamic-v3-rescue evidence-summary run --sweep-id <sweep_id>`
- `aits etf dynamic-v3-rescue evidence-summary report --latest`
- `aits etf dynamic-v3-rescue validate-evidence-summary --summary-id <summary_id>`
- `aits etf dynamic-v3-rescue medium-real report --sweep-id <sweep_id>`
- `aits etf dynamic-v3-rescue validate-medium-real --sweep-id <sweep_id>`
- `aits etf dynamic-v3-rescue regime-coverage run --sweep-id <sweep_id>`
- `aits etf dynamic-v3-rescue regime-coverage report --latest`
- `aits etf dynamic-v3-rescue validate-regime-coverage --coverage-id <coverage_id>`
- `aits etf dynamic-v3-rescue candidate interpretation-pack --sweep-id <sweep_id> --candidate-id <candidate_id>`
- `aits etf dynamic-v3-rescue candidate interpretation-report --candidate-id <candidate_id>`
- `aits etf dynamic-v3-rescue validate-interpretation-pack --candidate-id <candidate_id>`
- `aits etf dynamic-v3-rescue observe-pool build --sweep-id <sweep_id>`
- `aits etf dynamic-v3-rescue observe-pool report --latest`
- `aits etf dynamic-v3-rescue validate-observe-pool --pool-id <pool_id>`
- `aits etf dynamic-v3-rescue overnight-readiness run --sweep-id <sweep_id>`
- `aits etf dynamic-v3-rescue overnight-readiness report --latest`
- `aits etf dynamic-v3-rescue validate-overnight-readiness --readiness-id <readiness_id>`
- `aits etf dynamic-v3-rescue research-decision run --sweep-id <sweep_id>`
- `aits etf dynamic-v3-rescue research-decision report --latest`
- `aits etf dynamic-v3-rescue validate-research-decision --decision-id <decision_id>`

## Artifact Contract

新增 artifact families：

- `reports/etf_portfolio/dynamic_v3_rescue/evidence_summary/<summary_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/medium_real/<sweep_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/regime_coverage/<coverage_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/interpretation/<candidate_id>/<interpretation_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/observe_pool/<pool_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/overnight_readiness/<readiness_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/research_decision/<decision_id>/`

关键文件：

- `evidence_summary_manifest.json`
- `candidate_evidence_matrix.csv`
- `reader_brief_section.md`
- `medium_real_report.json`
- `medium_real_report.md`
- `regime_coverage_manifest.json`
- `regime_gap_report.json`
- `candidate_regime_matrix.csv`
- `tech_semiconductor_relevance_report.md`
- `interpretation_manifest.json`
- `candidate_interpretation_report.md`
- `observe_pool_manifest.json`
- `observe_pool_candidates.csv`
- `overnight_readiness_report.json`
- `overnight_readiness_report.md`
- `research_decision_manifest.json`
- `research_decision_report.md`

所有 artifacts 必须固定：

- `observe_only=true`
- `candidate_only=true`
- `production_effect=none`
- `broker_action=none`
- `manual_review_required=true`
- `production_candidate_generated=false`

## 验收标准

- `medium_real` profile 配置存在且校验通过，默认 `max_candidates=300`，并要求
  data audit、window audit 和 weight path evidence。
- `validate-medium-real` 对 tiny fixture、缺 real artifact path、candidate 数不足且无解释、
  或 missing leaderboard fail closed。
- Evidence summary 能从 sweep / data provenance / window audit / attribution / overfit
  生成候选证据矩阵，并让缺证据候选不能进入 observe pool。
- Regime coverage 披露 AI bull、drawdown、semiconductor stress、sideways/high-vol/recovery
  覆盖；覆盖不足时 research decision 不得建议直接跑 `overnight_real`。
- Interpretation pack 对 top candidate 披露参数、weight path / turnover source、
  drawdown protection 和 regime behavior；缺 weight path 时明确 incomplete。
- Observe pool 不默认同步 shadow registry；任何 registry sync 必须另行 owner review。
- Overnight readiness 只给出 readiness/status 和 blockers，不启动 `overnight_real`。
- Research decision 把所有 source artifacts 链接起来，给出 next Codex task 和
  `production_effect=none`。
- Reader Brief 只读 latest evidence summary、observe pool 和 research decision，不运行上游。

必须运行：

```bash
python -m pytest tests/test_evidence_summary.py tests/test_medium_real_profile.py tests/test_regime_coverage.py tests/test_candidate_interpretation_pack.py tests/test_observe_pool.py tests/test_overnight_readiness.py tests/test_research_decision.py -q
python -m pytest tests/test_etf_dynamic_v3_parameter_research.py tests/test_reader_brief.py -q
python -m ruff check src tests
python -m compileall -q src tests
git diff --check
```

尽量运行：

```bash
python -m pytest tests -q
```

## 运行记录

- 2026-06-07：新增并进入 `IN_PROGRESS`，按 owner 粘贴的 TRADING-114～120 计划启动
  medium_real 候选发现、regime coverage、interpretation pack、observe pool、overnight
  readiness 和 research decision 工作。
- 2026-06-08：发现 `medium_real` 单 candidate real evaluation 存在重复计算；本轮先实现
  deterministic fixed robustness evidence cache，复用 baseline/v0.2/v0.4 的
  `build_dynamic_robustness_report` 结果，并保留 full v0.3 candidate sensitivity/overfit 证据。
- 2026-06-08：真实 `medium_real` sweep `sweep_20260607T102300Z_ae5ae1d8` 完成
  300/300 candidates、0 failures，全部为 `review_required`；`medium_real` report
  `89353fbd6d2e9e48` validation PASS。Evidence summary `3d98dd79c7ab6b40`
  artifact validation PASS，但业务状态为 FAIL，`usable_for_research_count=0`；
  regime coverage `65bc14b9e740798e` PASS；interpretation pack `291add5a0c664546`
  PASS；observe pool `1201681d0e290627` PASS_WITH_WARNINGS，observe candidates=0；
  overnight readiness `c0755e2c263b0854` 为 READY_WITH_WARNINGS，预计 runtime
  3.902 小时；research decision `81ac692903a4c668` PASS，recommendation 为
  `fix_evidence_gaps`、priority HIGH。Dynamic v3 artifacts validation PASS，仍不生成
  production candidate、shadow enrollment、owner approval 或 broker action。
