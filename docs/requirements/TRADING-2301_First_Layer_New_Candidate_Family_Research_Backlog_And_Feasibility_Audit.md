# TRADING-2301 First-Layer New Candidate Family Research Backlog and Feasibility Audit

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

Owner 附件 `G:/Download/first_layer_new_candidate_family_research_prioritization.md` 总结了 TRADING-2283～2293 的第一层候选研究闭环：

- `volatility_regime_scope_narrowed_risk_cap_v1` 是唯一保留的 risk-cap-only forward observe candidate。
- `baseline_plus_trend_structure_scope_narrowed_confirmation_v1` 当前形态为 reject recommended。
- `risk_appetite_refined_confidence_v1` 当前形态 archive。

因此，新研究不能继续围绕失败的 `trend_structure` / `risk_appetite` 做小幅调参，应进入新的 candidate family 设计；主线继续推进 risk-cap forward observe，支线建立新 candidate family backlog 和 feasibility audit。

附件中的 `TRADING-2293B`～`TRADING-2298B` 不直接进入任务登记，因为当前 task-register consistency 要求 stable numeric prefix。本文采用合法任务 ID：

|合法 ID|附件 alias|主题|
|---|---|---|
|`TRADING-2294`|TRADING-2294 mainline|Risk-cap evidence accumulation extension plan|
|`TRADING-2301`|`TRADING-2293B`|First-layer new candidate family backlog and feasibility audit|
|`TRADING-2302`|`TRADING-2294B`|Breadth participation data feasibility and candidate spec|
|`TRADING-2303`|post-2302 roadmap|Current constituents proxy diagnostics only|
|`TRADING-2304`|post-2302 roadmap|Breadth proxy signal concept selection|
|`TRADING-2305`|post-2302 roadmap|Breadth proxy candidate generator POC|
|`TRADING-2306`|post-2302 roadmap|Breadth proxy diagnostics validation / data-source decision|
|`TRADING-2307`|`TRADING-2295B`|AI semiconductor leadership candidate family feasibility audit|
|`TRADING-2311`|`TRADING-2296B`|Liquidity rates pressure data feasibility audit|
|`TRADING-2315`|`TRADING-2297B`|Regime state machine design audit|
|`TRADING-2318`|`TRADING-2298B`|Event calendar data feasibility audit|
|`TRADING-2321`|execution mechanics note|Risk-cap cooldown / decay design|

## 目标

新增 CLI：

```bash
aits research trends first-layer-new-candidate-family-prioritization
```

该命令把 owner 附件固化为 research-only artifacts：

- prioritization summary；
- candidate family score matrix；
- data feasibility matrix；
- implementation route / task backlog；
- standard validation path；
- deferred current-form matrix；
- safety boundary；
- owner review note；
- Markdown research docs。

所有输出必须强制：

```yaml
promotion_allowed: false
paper_shadow_allowed: false
production_allowed: false
broker_action: none
candidate_generation_allowed: false
actual_path_validation_executed: false
forward_observe_runtime_started: false
```

## 实施拆解

1. Research-only prioritization model。
   - 编码附件评分维度、权重、family 排序和推荐下一步。
   - 输出 `P0` mainline、`P1` new family、`P2` diagnostic/gating/execution 和 `P3` deferred current-form rows。
   - 所有阈值/权重作为附件评分框架的 research prioritization metadata，不作为 trading heuristic。

2. Feasibility and risk matrices。
   - 对 breadth / participation、AI / semiconductor leadership、liquidity / rates pressure、regime state machine、event calendar gating 和 execution mechanics 生成 PIT/data feasibility、validation clarity、overfit risk、implementation tractability 和 recommended input audit。
   - 对 `trend_structure` / current `risk_appetite` 生成 deferred current-form matrix。

3. Task route。
   - 将附件 alias 和 post-2302 roadmap 映射到合法且不冲突的 task ids。
   - 明确 first next new family 是 breadth / participation，第二是 AI / semiconductor leadership，第三是 liquidity / rates pressure。
   - 保留 risk-cap forward observe mainline，不把新 family 直接接入 generator、validator 或 report runtime。

4. 文档和 registry。
   - 更新 `docs/research/first_layer_new_candidate_family_prioritization.md`。
   - 更新 `docs/research/first_layer_new_candidate_family_task_backlog.md`。
   - 更新 `docs/research/first_layer_new_candidate_family_safety_boundary.md`。
   - 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md` 和 `docs/system_flow.md`。

5. 验证。
   - focused parallel pytest 覆盖 score matrix、task alias mapping、safety boundary、CLI output 和 docs。
   - 运行 Ruff、compileall、docs freshness、report contract 或 registry-focused checks、task-register consistency 和 `git diff --check`。

## 验收标准

- CLI implemented: `aits research trends first-layer-new-candidate-family-prioritization`。
- 生成 JSON/CSV/Markdown artifacts，并可复现附件中的排序：risk-cap mainline P0，breadth / participation 和 AI / semiconductor leadership 为 P1，liquidity / rates pressure 为 P1/P2，regime / event / execution 为辅助层，current trend/risk_appetite 形态暂缓。
- 任务登记记录 `TRADING-2301`～`TRADING-2307`、`TRADING-2311`、`TRADING-2315`、`TRADING-2318`、`TRADING-2321`，并保留附件 legacy alias。
- 所有 outputs 固定 promotion、paper-shadow、production、broker false/none。
- 不生成新的 candidate-bound executable artifacts，不执行 actual-path validation，不启动 forward observe runtime。

## 进展记录

- 2026-07-01: 根据 owner 附件新增并进入 `IN_PROGRESS`。当前 worktree 已存在 TRADING-1087 / ops / data download 相关未提交改动；本任务必须 selective staging，不能混入无关改动。
- 2026-07-01: 实现完成并转入 `VALIDATING`。新增 `aits research trends first-layer-new-candidate-family-prioritization`、deterministic prioritization builder、report registry / artifact catalog / system flow 更新和 focused tests。真实 run 输出 status=`FIRST_LAYER_NEW_CANDIDATE_FAMILY_BACKLOG_READY_PROMOTION_BLOCKED`，next new family=`TRADING-2302_BREADTH_PARTICIPATION_DATA_FEASIBILITY_AND_CANDIDATE_SPEC`，next mainline=`TRADING-2294_EVIDENCE_ACCUMULATION_EXTENSION_PLAN`，data quality status=`NOT_APPLICABLE_STATIC_OWNER_BRIEF`；所有 outputs 固定 promotion / paper-shadow / production / broker false / none，且 `candidate_generation_allowed=false`、`actual_path_validation_executed=false`、`forward_observe_runtime_started=false`。验证通过 focused parallel pytest 12 passed、Ruff、compileall、docs freshness 480 docs / 0 issues、documentation contract PASS、contract-validation 193 passed、task-register consistency run/validate PASS 和 `git diff --check`。
- 2026-07-01: 根据 post-2302 roadmap 修正后续任务编号，避免 TRADING-2303 与 breadth current-constituents diagnostics 冲突。AI leadership 改为 `TRADING-2307`，liquidity/rates 改为 `TRADING-2311`，regime 改为 `TRADING-2315`，event 改为 `TRADING-2318`，execution mechanics 改为 `TRADING-2321`；TRADING-2303～2306 保留给 breadth proxy diagnostics / selection / POC / validation。
