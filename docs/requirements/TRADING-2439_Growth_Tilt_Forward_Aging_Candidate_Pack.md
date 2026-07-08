# TRADING-2439 Growth Tilt Forward Aging Candidate Pack

最后更新：2026-07-09

## 状态

- task register id：`TRADING-2439_GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK`
- status：`DONE`
- owner：系统实现 + 项目 owner 后续复核
- 创建日期：2026-07-09

## 目标

在 TRADING-2438 top-3 candidate PIT replay 之后，为通过 PIT replay 的候选生成
forward aging candidate pack。当前 authoritative 2438 artifact 并未 READY，而是
`GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP`；因此 2439
必须 fail closed，不得把 top-3 selection、prior artifact readiness 或 blocked replay
evidence 伪装为 forward aging eligibility。

## 输入

- TRADING-2438 top-3 candidate PIT replay result
- TRADING-2438 PIT replay evidence artifact
- TRADING-2438 blocker summary
- data quality gate status carried from 2438 or rerun by the command
- report registry
- artifact catalog
- system flow
- research docs

## 输出

- `outputs/research_strategies/growth_tilt_forward_aging_candidate_pack/forward_aging_candidate_pack_result.json`
- `outputs/research_strategies/growth_tilt_forward_aging_candidate_pack/forward_aging_candidate_pack.json`
- `outputs/research_strategies/growth_tilt_forward_aging_candidate_pack/candidate_tracking_artifact.json`
- `outputs/research_strategies/growth_tilt_forward_aging_candidate_pack/forward_observation_contract.json`
- `outputs/research_strategies/growth_tilt_forward_aging_candidate_pack/no_effect_boundary.json`
- `docs/research/growth_tilt_forward_aging_candidate_pack.md`
- `docs/research/growth_tilt_forward_aging_candidate_pack_details.md`
- `docs/research/growth_tilt_forward_aging_candidate_tracking.md`
- `docs/research/growth_tilt_forward_observation_contract.md`
- `docs/research/growth_tilt_forward_aging_no_effect_boundary.md`
- blocked route doc `docs/research/dynamic_strategy_2439_blocked_route.md` unless PIT replay later succeeds

## CLI

```bash
aits research strategies growth-tilt-forward-aging-candidate-pack --as-of 2026-07-08
```

## 期望 READY 状态

```text
GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_READY
```

## 当前 Fail-Closed 状态

```text
GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_BLOCKED_BY_PIT_REPLAY_GATE
```

## 安全边界

本任务不得生成真实 forward aging observation，不得生成交易建议，不得启用
paper-shadow / schedule / production / broker，不得写组合权重。没有 PIT replay pass
时，`forward_aging_candidate_count` 必须为 0，next route 必须回到
`TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation`。

## 验收标准

- CLI 可真实运行并输出 deterministic READY / BLOCKED 状态。
- 明确 `forward_aging_candidate_count`、`observation_horizons`、PIT replay source
  status、data-quality status 和 next route。
- 当前 2438 blocked 时不得生成 forward aging candidates，不得进入 2440 READY route。
- paper-shadow、schedule、production、broker、automatic execution 均保持 disabled。
- focused tests、Ruff、compileall、docs freshness、documentation contract、task
  register consistency、contract validation 和 diff check 通过。

## 验证计划

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_forward_aging_candidate_pack.py
aits validate-data --as-of 2026-07-08
aits research strategies growth-tilt-forward-aging-candidate-pack --as-of 2026-07-08
aits docs validate-freshness
aits docs report-contract --latest
aits reports task-register-consistency run
aits reports task-register-consistency validate --latest
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

## 进展记录

- 2026-07-09：根据 owner 2426-2440 roadmap 新增，进入 `IN_PROGRESS`；当前 2438 artifact blocked by replay engine/input specs gap，2439 必须 fail-closed，不得生成 forward aging candidates 或 route 到 2440 promotion review。
- 2026-07-09：实现完成并进入 `DONE`。新增 forward aging candidate pack builder、CLI、candidate pack / candidate tracking / forward observation contract / no-effect boundary artifacts、research docs、registry、catalog、system flow 和 focused tests；真实 run 先执行 `aits validate-data --as-of 2026-07-08`，数据质量状态=`PASS_WITH_WARNINGS`、error=0、warning=2、info=12，随后 CLI 输出 `GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_BLOCKED_BY_PIT_REPLAY_GATE`。由于 2438 仍为 `GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP`，本任务固定 `source_2438_ready=false`、`forward_aging_candidate_count=0`、`valid_until_outcome_capture_ready=false`、`forward_aging_observation_started=false`、`forward_aging_observation_written=false`、`candidate_tracking_started=false`，next route=`TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation`。本任务未生成 forward aging candidates，未进入 2440；paper-shadow / schedule / production / broker / automatic execution / trading advice 全部 disabled / false / none。
