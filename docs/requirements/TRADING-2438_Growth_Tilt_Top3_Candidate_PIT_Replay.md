# TRADING-2438 Growth Tilt Top-3 Candidate PIT Replay

最后更新：2026-07-09

## 状态

- task register id：`TRADING-2438_GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY`
- status：`DONE`
- owner：系统实现 + 项目 owner 后续复核
- 创建日期：2026-07-09

## 目标

在 TRADING-2437 regime slice attribution review READY 后，从 2431-2437 的候选证据
中选择最多 3 个 PIT candidates，执行更严格的 candidate-specific PIT replay。2438 是从
contract-level 快速研究进入严格验证的分水岭；如果缺少 Growth Tilt candidate PIT replay
engine、candidate source manifest、as-of / valid-until specs 或 outcome linkage specs，必须
fail closed，不得把 prior-artifact readiness 伪装成 PIT replay pass。

## 输入

- TRADING-2437 regime slice attribution review result
- TRADING-2433 false risk-off / missed-upside batch screen result
- TRADING-2433 candidate-set config
- TRADING-2431 existing candidate evidence matrix
- data quality gate report from `aits validate-data` or same validation code path
- report registry
- artifact catalog
- system flow
- research docs

## 输出

- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay/top3_candidate_pit_replay_result.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay/top3_candidate_selection.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay/pit_replay_evidence.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay/pit_replay_blocker_summary.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay/no_effect_boundary.json`
- `docs/research/growth_tilt_top3_candidate_pit_replay.md`
- `docs/research/growth_tilt_top3_candidate_selection.md`
- `docs/research/growth_tilt_top3_candidate_pit_replay_evidence.md`
- `docs/research/growth_tilt_top3_candidate_pit_replay_blocker_summary.md`
- `docs/research/growth_tilt_top3_candidate_pit_replay_no_effect_boundary.md`
- blocked route doc or `docs/research/dynamic_strategy_2439_route.md` if replay succeeds

## CLI

```bash
aits research strategies growth-tilt-top3-candidate-pit-replay --as-of 2026-07-08
```

## 期望 READY 状态

```text
GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_READY
```

## Fail-Closed 状态

```text
GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP
```

## 安全边界

本任务必须运行 `aits validate-data` 或同源数据质量门。即使数据质量门通过，缺少
candidate-specific PIT replay engine 或 replay input specs 时仍必须 blocked。不得启用
paper-shadow，不得生成 trading advice，不得触发 broker，不得直接晋级 production，不得写组合权重。

## 验收标准

- CLI 可真实运行并输出 deterministic READY / BLOCKED 状态。
- 明确 `pit_candidates_selected`、`pit_candidates_tested`、`pit_replay_pass_count`、
  `pit_replay_fail_count`、`promotion_review_candidate_count`。
- 明确 data-quality gate status、source traceability、as-of boundary、valid-until boundary、
  outcome linkage readiness 和 replay engine availability。
- 缺 replay engine/input specs 时不得 silent pass，必须 blocked 并 route 到 remediation。
- paper-shadow、schedule、production、broker、automatic execution 均保持 disabled。
- focused tests、Ruff、compileall、docs freshness、documentation contract、task
  register consistency、contract validation 和 diff check 通过。

## 验证计划

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_top3_candidate_pit_replay.py
aits validate-data --as-of 2026-07-08
aits research strategies growth-tilt-top3-candidate-pit-replay --as-of 2026-07-08
aits docs validate-freshness
aits docs report-contract --latest
aits reports task-register-consistency run
aits reports task-register-consistency validate --latest
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

## 进展记录

- 2026-07-09：根据 owner 2426-2440 roadmap 新增，进入 `IN_PROGRESS`；初查未发现 Growth Tilt candidate-specific PIT replay engine 或 replay input specs，当前实现必须 fail-closed 记录 blocker。
- 2026-07-09：实现完成并进入 `DONE`。新增 top-3 candidate PIT replay builder、CLI、top-3 candidate selection / PIT replay evidence / PIT replay blocker summary / no-effect boundary artifacts、research docs、registry、catalog、system flow 和 focused tests；真实 run 先执行 `aits validate-data --as-of 2026-07-08`，数据质量状态=`PASS_WITH_WARNINGS`、error=0、warning=2、info=12，随后 CLI 输出 `GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP`。源证据和 candidate set 均 ready，但 `candidate_pit_replay_engine_available=false` 且 replay input / source traceability / as-of / valid-until / outcome linkage specs 全部未就绪，因此 `pit_candidates_selected=3`、`pit_candidates_tested=0`、`pit_replay_pass_count=0`、`pit_replay_fail_count=0`、`pit_replay_blocked_count=3`、`promotion_review_candidate_count=0`，next route=`TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation`。本任务未解除 blocker，未进入 2439；paper-shadow / schedule / production / broker / automatic execution / trading advice 全部 disabled / false / none。
