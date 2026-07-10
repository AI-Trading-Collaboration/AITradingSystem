# TRADING-2438M Growth Tilt Post-Runtime Candidate PIT Replay Blocker Resolution

最后更新：2026-07-10

## Context

- task register id: `TRADING-2438M_GROWTH_TILT_POST_RUNTIME_CANDIDATE_PIT_REPLAY_BLOCKER_RESOLUTION`
- owner route source: TRADING-2438L
- default as-of: `2026-07-08`
- market regime: `ai_after_chatgpt`
- status: `BLOCKED_OWNER_INPUT`（精确根因解析子阶段已实现；resolution 仍 BLOCKED）
- safety boundary: validation-only / candidate-only / fail-closed

TRADING-2438L 的真实结果为
`GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_RUNTIME_REMEDIATION_BLOCKED`，
且 pass/fail/blocked=`0/0/3`。TRADING-2438M 必须区分 runtime contract、runtime
invocation、raw output、metric computation、metric normalization、threshold
evaluation 和 candidate outcome resolution，不能再用最终 null 字段替代 stage-level
根因。

## Root-Cause Stage Audit

2026-07-10 在实现前完成了对 TRADING-2433、2438、2438B、2438K、2438L
源码和真实 artifacts 的逐阶段审计：

|阶段|真实证据|结论|
|---|---|---|
|candidate selection|2438 top-3 identity 与顺序稳定|PASS|
|runtime contract|2438K 输出 executor/adapter/smoke-check shell|PASS，但只证明 contract 存在|
|runtime input hydration|2433 candidate config 只有 candidate id、family、research question 和 rationale，没有参数化 candidate implementation；所有 `threshold_value` 为 null|BLOCKED|
|replay runner invocation|2438B `engine_entrypoint` 指向 blocker-closure builder；2438L builder 只回读 2438K/2438D artifacts，没有调用 compute-plane runner|NOT_STARTED|
|raw replay output|没有 candidate/baseline equity path、daily state/output 或 raw observation artifact|NOT_STARTED|
|metric computation|2438K `metric_summary` 六项全部为 null；仓库中没有为三个 candidate 注册的 runtime calculator invocation|NOT_STARTED|
|metric normalization|没有 raw metric，不能进入 normalization|NOT_STARTED|
|threshold spec resolution|2432/2433 明确要求 future governed policy，threshold values 全部为 null|BLOCKED|
|threshold evaluation|2438K 只有 `pass_fail_threshold_evaluator_shell` 引用，没有 runtime evaluation producer/output|NOT_STARTED|
|persistence/reload|2438K/L artifacts 成功持久化和回读，但其内容是 contract/readiness，不是 compute output|PASS（仅治理 artifact）|

首个 required failed stage 是 `RUNTIME_INPUT_HYDRATED`。当前不能从 candidate 名称或
rationale 推断策略参数，也不能自行设定投资阈值；否则会违反 heuristic governance、
PIT 审计和 no-silent-workaround 要求。第二个独立 owner/policy blocker 是 governed
threshold spec 缺失。

## Scope And Stages

本任务按以下顺序推进：

1. 固化 candidate identity、source artifact hash、as-of、run id 和 schema lineage。
2. 为每个 candidate 生成统一 stage trace：`CANDIDATE_SELECTED`、
   `RUNTIME_CONTRACT_RESOLVED`、`RUNTIME_INPUT_HYDRATED`、
   `REPLAY_RUNNER_INVOKED`、`RAW_REPLAY_OUTPUT_EMITTED`、
   `METRIC_DEPENDENCIES_RESOLVED`、`RUNTIME_METRICS_COMPUTED`、
   `RUNTIME_METRICS_NORMALIZED`、`THRESHOLD_SPECS_RESOLVED`、
   `THRESHOLDS_EVALUATED`、`CANDIDATE_OUTCOME_RESOLVED`、
   `ARTIFACT_PERSISTED` 和 `ARTIFACT_RELOADED`。
3. 对每个 required metric 输出 finite runtime-computed value 或精确 blocker record。
4. 对每个 required threshold 输出 PASS/FAIL/BLOCKED runtime evaluation；静态合同不
   得计为 runtime output。
5. 统一解析 candidate outcome：`BLOCKED > FAIL > PASS`。
6. 输出 primary resolution rollup 和 supporting stage/metric/threshold/blocker/provenance
   artifacts。
7. 若真实 candidate spec、compute-plane 或 threshold policy 仍缺失，保持
   `..._BLOCKED`，并派生 narrowly-scoped owner/policy remediation route；不得把本阶段
   标记为 blocker resolution complete。

## Blockers And Dependencies

- `CANDIDATE_RUNTIME_INPUT_CONTRACT_MISSING`：三个 candidate 没有参数化 executable
  spec，无法把研究假设 hydrate 成真实 replay input。下一责任方：项目 owner 与策略
  研究 owner 定义/批准候选参数及适用现有 engine 的 mapping。
- `CANDIDATE_RUNTIME_REPLAY_RUNNER_NOT_INVOKED`：当前所谓 engine 是 contract builder，
  不是 compute-plane replay runner。下一责任方：系统实现，在 executable spec 获批后
  绑定现有 backtest/replay producer，或登记单独受审 compute-plane requirement。
- `CANDIDATE_RUNTIME_THRESHOLD_SPEC_MISSING`：候选与 gauntlet config 的 threshold values
  明确为 null。下一责任方：项目 owner / policy owner 提供符合 heuristic governance 的
  reviewed threshold policy、版本、理由、验证证据和 review/expiry 条件。

禁止使用默认 0、历史无关 run、fixture value、rationale-derived parameter、静态
threshold contract 或 threshold value 本身代替真实 runtime output。

## Outputs

- `outputs/research_strategies/growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution/growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution.json`
- `outputs/research_strategies/growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution/growth_tilt_candidate_runtime_stage_trace.json`
- `outputs/research_strategies/growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution/growth_tilt_candidate_runtime_metric_materialization.json`
- `outputs/research_strategies/growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution/growth_tilt_candidate_runtime_threshold_evaluations.json`
- `outputs/research_strategies/growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution/growth_tilt_candidate_runtime_blocker_matrix.json`
- `outputs/research_strategies/growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution/growth_tilt_candidate_runtime_provenance.json`
- `docs/research/growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution.md`

Supporting diagnostics 只登记为 SUPPORTING / DEBUG；resolution rollup 为 PRIMARY。

## CLI

```bash
aits research strategies growth-tilt-post-runtime-candidate-pit-replay-blocker-resolution --as-of 2026-07-08 --strict
```

`--strict` 对 source artifact 缺失、schema mismatch、candidate identity drift、unsafe
safety field 和 unknown metric/threshold 返回非零；候选真实 FAIL 或精确诊断后的
BLOCKED 仍是合法研究结果，不把 CLI 工程状态伪装为失败。

## Acceptance Criteria

- top-3 candidate identity、顺序和 lineage 与 2438L 完全一致。
- 每个 candidate 都有完整 stage trace，最终 blocker 来自第一个未通过 required stage。
- 每个 required metric 都有 finite runtime value 或包含 field path、expected、observed、
  source artifact 和 recommended repair 的 blocker。
- 每个 threshold 都有 PASS/FAIL/BLOCKED 结果或精确 spec/evaluator blocker。
- runtime executable 不隐含 metric computed；rechecked=true 不隐含 threshold evaluated。
- null/NaN/Inf 均保持 BLOCKED，calculator 真实产生的 0.0 才合法。
- source hash、schema、as-of、producer/calculator/evaluator provenance 可追溯。
- next route 由真实 pass/fail/blocked 和 precise blocker taxonomy 派生。
- 所有 safety fields 保持 observe-only/candidate-only、production_effect=none、
  paper-shadow/production weights/broker action disabled。
- focused parallel pytest、shared regression（若修改 shared compute plane）、ruff、
  compileall、data quality gate、真实 CLI、docs/registry/catalog/task-register/contract
  validation 和 `git diff --check` 通过。

只有 `null_runtime_metric_count=0`、`missing_threshold_evaluation_count=0`、
`blocked_count=0` 时，才可把 blocker resolution 标记为完整完成。否则本轮只能完成
“精确根因解析子阶段”，任务保持 BLOCKED/BASELINE_DONE 并保留 owner/policy 依赖。

## Progress Notes

- 2026-07-10: 根据 2438L 真实 route 与 owner 开发计划进入 `IN_PROGRESS`；完成
  stage-by-stage 根因审计，确认并非 metric serialize/load 丢值，而是 candidate
  executable spec、真实 replay invocation/raw output 和 governed threshold spec 从未存在。
  开始实现 fail-closed stage trace 与 precise blocker resolution artifacts。
- 2026-07-10: 精确根因解析子阶段实现并转为 `BLOCKED_OWNER_INPUT`，blocker resolution
  不得标记完成。30 项 focused/anti-shortcut parallel pytest 通过；真实执行
  `aits validate-data --as-of 2026-07-08` 得到 `PASS_WITH_WARNINGS`、error=0、
  warning=2、info=12；随后 strict CLI 成功生成全部 artifacts，status=
  `GROWTH_TILT_POST_RUNTIME_CANDIDATE_PIT_REPLAY_BLOCKER_RESOLUTION_BLOCKED`，
  runtime invoked=`0/3`，computed/null/invalid metrics=`0/18/0`，completed/missing
  threshold evaluations=`0/3`，pass/fail/blocked=`0/0/3`，unresolved blockers=33，
  3 个 candidate 的 first failed stage 均为 `RUNTIME_INPUT_HYDRATED`，next route=
  `TRADING-2438M1_Growth_Tilt_Candidate_Runtime_Spec_And_Threshold_Policy_Approval`。
  当前系统没有伪造 metric/threshold output，paper-shadow / production / broker / weight
  mutation 均为 false/none。
