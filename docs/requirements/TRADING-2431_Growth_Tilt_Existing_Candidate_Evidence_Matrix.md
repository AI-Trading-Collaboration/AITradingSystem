# TRADING-2431 Growth Tilt Existing Candidate Evidence Matrix

最后更新：2026-07-09

## 状态

- task register id：`TRADING-2431_GROWTH_TILT_EXISTING_CANDIDATE_EVIDENCE_MATRIX`
- status：`DONE`
- owner：系统实现 + 项目 owner 后续复核
- 创建日期：2026-07-09

## 目标

在 TRADING-2430 确认当前没有 paper-shadow promotion candidate 后，把已有
Growth Tilt 相关候选统一整理进 evidence matrix，明确哪些已经 rejected，哪些只
保留 component value，哪些需要后续 PIT/gauntlet 验证，哪些真正达到 promotion
candidate。

本任务只整理已有证据，不重新运行 historical screen、backtest、scoring 或
market-data experiment。

## 覆盖候选

- `defensive_limited_adjustment`
- `lower_turnover_variants`
- `dynamic_valid_until_expiry_strict_v1`
- `dynamic_turnover_budgeted_growth_tilt_v1`
- `equal_risk_growth_tilt_vol_target_variants`
- `growth_tilt_engine_signal_variants`

## 输入

- TRADING-2430 candidate promotion evidence review artifact
- `config/research/equal_risk_growth_tilt_candidate_registry.yaml`
- prior candidate owner-review evidence
- prior component attribution / candidate evidence docs
- report registry
- artifact catalog
- system flow
- research docs

## 输出

- `outputs/research_strategies/growth_tilt_existing_candidate_evidence_matrix/existing_candidate_evidence_matrix_result.json`
- `outputs/research_strategies/growth_tilt_existing_candidate_evidence_matrix/candidate_evidence_matrix.json`
- `outputs/research_strategies/growth_tilt_existing_candidate_evidence_matrix/candidate_status_summary.json`
- `outputs/research_strategies/growth_tilt_existing_candidate_evidence_matrix/candidate_metric_coverage.json`
- `outputs/research_strategies/growth_tilt_existing_candidate_evidence_matrix/no_effect_boundary.json`
- `docs/research/growth_tilt_existing_candidate_evidence_matrix.md`
- `docs/research/growth_tilt_existing_candidate_evidence_matrix_table.md`
- `docs/research/growth_tilt_existing_candidate_status_summary.md`
- `docs/research/growth_tilt_existing_candidate_metric_coverage.md`
- `docs/research/growth_tilt_existing_candidate_no_effect_boundary.md`
- `docs/research/dynamic_strategy_2432_route.md`

## CLI

```bash
aits research strategies growth-tilt-existing-candidate-evidence-matrix --as-of 2026-07-08
```

## 期望状态

```text
GROWTH_TILT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_READY
```

如果必需 prior artifact / registry / docs 缺失，则 fail closed：

```text
GROWTH_TILT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_BLOCKED_BY_EVIDENCE_GAPS
```

## 分类规则

允许的 candidate status：

- `rejected`
- `component_value`
- `needs_pit`
- `promotion_candidate`

TRADING-2431 不能把 engineering readiness 或 no-effect wiring readiness 当作 alpha
evidence。只有已有候选证据明确支持 paper-shadow promotion 时，才允许
`promotion_candidate`。默认真实路径预期仍为 `promotion_candidate_count=0`。

## 安全边界

本任务不得：

- 运行 `aits validate-data`，除非实现读取 fresh cached market/features/signals/outcome/event data
- 读取 fresh cached market data
- 运行 historical screen、backtest、PIT replay、gauntlet、scoring 或 daily report
- 生成新 signal
- 回填真实 outcome
- 生成 trading advice 或 actionable allocation
- 生成 broker order
- 修改实际组合权重
- 启用 paper-shadow、paper-shadow schedule、scheduler、production 或 broker

## Data Quality Gate

默认不运行 `aits validate-data`。原因：TRADING-2431 只读取 prior artifacts/docs、
candidate registry、prior candidate evidence、report registry、artifact catalog 和
system flow，不读取 fresh cached market data，不运行 historical screen、backtest、
PIT replay、candidate gauntlet、scoring、daily report，不生成 feature/signal，不回填
outcome，也不生成交易建议。

如果实现阶段引入 fresh cached market/features/signals/outcome/event data 读取，或
运行 backtest-like / scoring-like comparison，必须重新引入 `aits validate-data` 或
同等代码路径。

## 验收标准

- CLI 可真实运行并输出 deterministic READY / BLOCKED 状态。
- evidence matrix 覆盖 6 个既有候选集合。
- 输出每个候选的 status、status rationale、metric coverage、known blockers、
  component value / PIT / promotion 判断。
- 默认真实 run 不产生 promotion candidate。
- 明确 next route：`TRADING-2432_Growth_Tilt_Candidate_Gauntlet_Harness`。
- paper-shadow、schedule、production、broker、automatic execution 均保持 disabled。
- report registry、artifact catalog、system flow、task register 与 completed archive
  一致。
- focused tests、Ruff、compileall、docs freshness、documentation contract、task
  register consistency、contract validation 和 diff check 通过。

## 验证计划

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_existing_candidate_evidence_matrix.py
aits research strategies growth-tilt-existing-candidate-evidence-matrix --as-of 2026-07-08
aits docs validate-freshness
aits docs report-contract --latest
aits reports task-register-consistency run
aits reports task-register-consistency validate --latest
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

## 进展记录

- 2026-07-09：根据 owner 2426-2440 roadmap 新增，进入 `IN_PROGRESS`。
- 2026-07-09：实现完成并归档 `DONE`。真实 CLI run 输出
  `GROWTH_TILT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_READY`，
  source_2430_ready=true，candidate_registry_ready=true，
  prior_candidate_evidence_ready=true，component_value_evidence_ready=true，
  existing_candidate_evidence_matrix_ready=true，candidate_count=6，
  component_value_count=4，needs_pit_count=2，promotion_candidate_count=0，
  promotion_candidate_found=false，metric_coverage_partial_count=6，
  market_data_experiment_run=false，historical_screen_run=false，pit_replay_run=false，
  candidate_gauntlet_run=false，paper_shadow_enabled=false，production_enabled=false，
  broker_enabled=false，next route 指向
  `TRADING-2432_Growth_Tilt_Candidate_Gauntlet_Harness`。
  本任务未运行 `aits validate-data`，因为只读取 prior artifacts/docs、candidate
  registry、prior candidate evidence、component value matrix、registry、catalog 和
  system flow，不读取 fresh cached market/outcome data、不运行 historical screen /
  PIT replay / candidate gauntlet / backtest / scoring / daily report、不生成 feature /
  signal 或交易建议。
